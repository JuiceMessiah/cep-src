"""
Bridge core: engine-agnostic logic between the CEP (Flink, via Redpanda) and the
PAIS (Camunda 8 / Zeebe and DCR Graphs). Modelled on the course example
(POESS-example: BPMN/deployer "communication" + DCR/microservice), with Kafka
topics instead of Siddhi HTTP callbacks.

  CEP -> PAIS : an alert on `alerts` triggers the actions configured for its type
                in mapping.json (start a BPMN process, publish a BPMN message,
                create a DCR case and/or execute a DCR event).
  PAIS -> CEP : process instance started, every configured Zeebe job (service task)
                and every DCR event execution produce an envelope on `pais.events`.
  Logging     : every step is also sent to the logging service (XES per process).

Time: PAIS events carry `event_time` = the event time of the CEP context they belong
to (the alert that started/updated the case), so a Flink job joining pais.events with
weather/fire events stays consistent in live AND compressed replays. Wall-clock time
goes into `timestamp` (used for the XES log).

DCR watch: events PEOPLE execute in the DCR portal don't come through the bridge, so
open cases are polled (mapping "dcr_watch") and newly executed events are reported too.
DCR events are named by their LABEL in pais.events and XES (ids change between graph copies).

Camunda log (mapping "camunda_log"): the XES log for BPMN processes is read from Camunda
itself (completed element instances, real end times, lane = org:resource), because user
tasks, DMN tasks and buffered messages never pass through the bridge. While it's on, the
bridge writes no XES entries of its own for Zeebe (pais.events for Flink are unchanged).

Duplicate alerts (same alert_id: two Flink jobs, a restarted job) are processed once.

No engine clients imported here: Zeebe and DCR clients are injected (testable offline);
dcr's pure XML helpers are fine.
"""
import asyncio
import json
import os
import re
import time
from collections import OrderedDict
from datetime import datetime, timezone

from dcr import enabled as dcr_enabled, executed as dcr_executed

REQUIRED_ALERT_FIELDS = ("alert_id", "type", "case_key", "event_time")


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def now_ms():
    return int(time.time() * 1000)


def envelope(event, case_key, case_id, engine, process, *, payload=None, error="",
             role=None, event_time=None, timestamp=None):
    """The PAIS event format (topic pais.events and logging service). Same fields as
    the course example's callback (event, case_id, timestamp, role, payload, error)
    plus case_key/engine/process/event_time. payload is an object, not a JSON string."""
    return {
        "event": str(event),
        "case_key": str(case_key) if case_key is not None else "",
        "case_id": str(case_id) if case_id is not None else "",
        "engine": engine,
        "process": process,
        "role": role or engine,
        "timestamp": timestamp or now_iso(),
        "event_time": int(event_time) if event_time is not None else now_ms(),
        "payload": payload or {},
        "error": error or "",
    }


def _iso(ts):
    """Camunda date (Z, +0000 or +00:00) -> the bridge's ISO format, so XES traces sort correctly."""
    t = str(ts).strip()
    t = re.sub(r"Z$", "+00:00", t)
    t = re.sub(r"([+-]\d\d)(\d\d)$", r"\1:\2", t)
    try:
        return datetime.fromisoformat(t).astimezone(timezone.utc).isoformat(timespec="milliseconds")
    except ValueError:
        return str(ts)


class Bridge:
    def __init__(self, mapping, *, zeebe=None, dcr=None, emit=None, log=None, printer=print,
                 state_file=None, camunda=None, element_meta=None):
        self.mapping = mapping
        self.zeebe = zeebe          # async start_process(pid, vars) -> key ; async publish_message(name, key, vars)
        self.dcr = dcr              # sync create_case() -> id ; execute(case, event) -> enabled ; enabled_events(case)
        self.emit = emit or (lambda env: None)   # -> pais.events
        self.log = log or (lambda env: None)     # -> logging service
        self.print = printer
        self.state_file = state_file
        self.cases = _CaseMap(self._load_state(), self._save_state)   # (engine, process, case_key) -> case id
        self.dcr_process = f"dcr:{getattr(dcr, 'graph_id', 'graph')}"
        self._dcr_seen = {}        # DCR case id -> executed event ids at the last look (watch baseline)
        self._case_time = {}       # case id -> event time of the alert that last touched it
        self._dcr_watch_off = False
        self._dcr_labels = {}      # DCR event id -> label, from every events listing we saw
        self.camunda = camunda             # element_instances(key) / instance_state(key), sync
        self.element_meta = element_meta or {}   # bpmn_meta.load(): process -> element -> name/tag/lane
        self._state_dir = os.path.dirname(state_file) if state_file else None
        self._alerts_seen = OrderedDict((a, None) for a in self._load_json("alerts_seen.json", []))
        self._clog = self._load_json("camunda_log.json", {})

    # ---------------- persisted case map (survives bridge restarts) ----------------
    def _load_state(self):
        if not self.state_file or not os.path.exists(self.state_file):
            return {}
        try:
            with open(self.state_file, encoding="utf-8") as fh:
                raw = json.load(fh)
            return {tuple(k.split("|", 2)): v for k, v in raw.items()}
        except (OSError, ValueError) as e:
            self.print(f"state file unreadable, starting empty: {e}")
            return {}

    def _save_state(self, data):
        if not self.state_file:
            return
        tmp = f"{self.state_file}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"|".join(k): v for k, v in data.items()}, fh)
            os.replace(tmp, self.state_file)          # atomic
        except OSError as e:
            self.print(f"cannot save state: {e}")

    def _load_json(self, name, default):
        if not self._state_dir:
            return default
        path = os.path.join(self._state_dir, name)
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except FileNotFoundError:
            return default
        except (OSError, ValueError) as e:
            self.print(f"{name} unreadable, starting empty: {e}")
            return default

    def _save_json(self, name, data):
        if not self._state_dir:
            return
        path = os.path.join(self._state_dir, name)
        try:
            with open(f"{path}.tmp", "w", encoding="utf-8") as fh:
                json.dump(data, fh)
            os.replace(f"{path}.tmp", path)
        except OSError as e:
            self.print(f"cannot save {name}: {e}")

    # ---------------- helpers ----------------
    def camunda_log_on(self):
        return bool(self.mapping.get("camunda_log")) and self.camunda is not None

    def engine_enabled(self, engine):
        return {"zeebe": self.zeebe is not None, "dcr": self.dcr is not None}.get(engine, True)

    def skipped_actions(self):
        """(alert type, engine) pairs that will be skipped because the engine is disabled."""
        return sorted({(t, a.get("engine")) for t, acts in self.mapping.get("on_alert", {}).items()
                       for a in acts if not self.engine_enabled(a.get("engine"))})

    def _out(self, env, *, emit=True, log=True):
        if emit:
            try:
                self.emit(env)
            except Exception as e:  # never lose the log because the topic is down
                self.print(f"emit failed: {type(e).__name__}: {e}")
        if log:
            try:
                self.log(env)
            except Exception as e:
                self.print(f"log failed: {type(e).__name__}: {e}")

    @staticmethod
    def render(template, alert):
        """'{case_key}|{details.unit_id}' -> 'inc-...|amb-3'. Missing field = error, not a silent
        wrong key (a wrong correlation key means the message goes nowhere)."""
        def look(m):
            cur = alert
            for part in m.group(1).split("."):
                if not isinstance(cur, dict) or part not in cur:
                    raise KeyError(f"correlation_key template field {m.group(1)!r} missing in alert")
                cur = cur[part]
            return str(cur)
        return re.sub(r"\{([A-Za-z0-9_.]+)\}", look, template)

    @staticmethod
    def zeebe_variables(alert):
        return {
            "caseKey": alert["case_key"],
            "alertType": alert["type"],
            "alertId": alert["alert_id"],
            "severity": alert.get("severity", ""),
            "cepEventTime": int(alert["event_time"]),
            "alert": alert,
        }

    # ---------------- CEP -> PAIS ----------------
    async def handle_alert(self, alert):
        missing = [f for f in REQUIRED_ALERT_FIELDS if f not in alert]
        if missing:
            self._out(envelope("InvalidAlert", alert.get("case_key"), None, "cep", "bridge",
                               payload={"alert": alert}, error=f"missing fields: {missing}"), emit=False)
            return
        aid = str(alert["alert_id"])
        if aid in self._alerts_seen:
            self.print(f"duplicate alert {aid} skipped (already processed)")
            return
        self._alerts_seen[aid] = None
        while len(self._alerts_seen) > 5000:
            self._alerts_seen.popitem(last=False)
        self._save_json("alerts_seen.json", list(self._alerts_seen))
        actions = self.mapping.get("on_alert", {}).get(alert["type"])
        if not actions:
            self._out(envelope("UnmappedAlert", alert["case_key"], None, "cep", "bridge",
                               payload={"alert": alert}, error=f"no mapping for alert type {alert['type']!r}",
                               event_time=alert["event_time"]), emit=False)
            return
        for action in actions:
            if not self.engine_enabled(action.get("engine")):
                continue    # disabled engine: skip quietly (reported once at startup), no failure noise
            try:
                await self._run(action, alert)
            except Exception as e:
                engine = action.get("engine", "?")
                process = action.get("process_id") or (self.dcr_process if engine == "dcr" else "?")
                self._out(envelope(f"{action.get('action', '?')}Failed", alert["case_key"],
                                   self.cases.get((engine, process, alert["case_key"])), engine, process,
                                   payload={"action": action, "alert_id": alert["alert_id"]},
                                   error=f"{type(e).__name__}: {e}", event_time=alert["event_time"]))
                self.print(f"[{engine}] {action.get('action')} for {alert['case_key']} failed: {e}")

    async def _run(self, action, alert):
        engine, kind = action.get("engine"), action.get("action")
        ck, et = alert["case_key"], alert["event_time"]
        # Activity name for the CEP trigger in this engine's XES trace. "" = don't log it
        # (e.g. for DCR, when the trigger isn't an activity of the graph).
        log_activity = action.get("log_activity", alert["type"])
        if engine == "zeebe" and self.camunda_log_on():
            log_activity = ""      # Camunda's own record of the element is logged instead (right time)

        if engine == "zeebe":
            if self.zeebe is None:
                raise RuntimeError("Zeebe disabled (ZEEBE_ENABLED=false)")
            variables = self.zeebe_variables(alert)
            if kind == "start_process":
                pid = action["process_id"]
                key = await self.zeebe.start_process(pid, variables)
                self.cases[("zeebe", pid, ck)] = key
                if log_activity:
                    self._out(envelope(log_activity, ck, key, "cep", pid, role="cep",
                                       payload={"alert_id": alert["alert_id"]}, event_time=et), emit=False)
                self._out(envelope("ProcessStarted", ck, key, "camunda", pid,
                                   payload={"process_id": pid, "alert_id": alert["alert_id"]}, event_time=et),
                          log=False)
            elif kind == "publish_message":
                msg = action["message"]
                corr = self.render(action.get("correlation_key", "{case_key}"), alert)
                await self.zeebe.publish_message(msg, corr, variables,
                                                 ttl_ms=int(float(action.get("ttl_s", 60)) * 1000))
                pid = action.get("process_id", "")
                if log_activity:
                    self._out(envelope(log_activity, ck, self.cases.get(("zeebe", pid, ck)), "cep",
                                       pid or "camunda", role="cep",
                                       payload={"message": msg, "correlation_key": corr,
                                                "alert_id": alert["alert_id"]},
                                       event_time=et), emit=False)
            elif kind == "correlate_message":
                # REST correlation: returns the instance key, also for a message START event,
                # so the case map and the XES trace are complete from the first event.
                msg, pid = action["message"], action.get("process_id", "")
                corr = self.render(action.get("correlation_key", "{case_key}"), alert)
                known = self.cases.get(("zeebe", pid, ck))
                key = await self.zeebe.correlate_message(msg, corr, variables)
                if key and pid:
                    self.cases[("zeebe", pid, ck)] = key
                if log_activity:
                    self._out(envelope(log_activity, ck, key, "cep", pid or "camunda", role="cep",
                                       payload={"message": msg, "correlation_key": corr,
                                                "alert_id": alert["alert_id"]}, event_time=et), emit=False)
                self._out(envelope("ProcessStarted" if known is None else "MessageCorrelated", ck, key,
                                   "camunda", pid or "camunda",
                                   payload={"process_id": pid, "message": msg, "alert_id": alert["alert_id"]},
                                   event_time=et), log=False)
            else:
                raise ValueError(f"unknown zeebe action {kind!r}")

        elif engine == "dcr":
            if self.dcr is None:
                raise RuntimeError("DCR disabled (DCR_ENABLED=false or credentials missing)")
            key = ("dcr", self.dcr_process, ck)
            if kind == "start_case" or (kind == "execute" and key not in self.cases
                                        and action.get("create_if_missing", True)):
                case = await self._dcr(self.dcr.create_case)
                self.cases[key] = case
                self._dcr_seen[case] = set()     # fresh case: nothing executed yet (watch baseline)
                self._case_time[case] = et
                if log_activity:
                    self._out(envelope(log_activity, ck, case, "cep", self.dcr_process, role="cep",
                                       payload={"alert_id": alert["alert_id"]}, event_time=et), emit=False)
                self._out(envelope("CaseCreated", ck, case, "dcr", self.dcr_process,
                                   payload={"alert_id": alert["alert_id"]}, event_time=et), log=False)
            if kind in ("start_case", "execute") and action.get("event_id"):
                case = self.cases.get(key)
                if case is None:
                    raise RuntimeError(f"no DCR case for {ck!r} and create_if_missing is false")
                ev = action["event_id"]
                self._case_time[case] = et
                try:
                    events = await self._dcr(self.dcr.execute, case, ev)
                except Exception as e:
                    # A rejected event is a compliance signal, not just an error: log + emit it.
                    self._out(envelope(self.dcr_label(ev), ck, case, "dcr", self.dcr_process,
                                       payload={"alert_id": alert["alert_id"], "event_id": ev, "rejected": True},
                                       error=str(e), event_time=et))
                    return
                self._learn_labels(events)
                if case in self._dcr_seen:
                    self._dcr_seen[case].add(ev)   # our own execution: the watcher must not re-report it
                self._out(envelope(self.dcr_label(ev), ck, case, "dcr", self.dcr_process,
                                   payload={"alert_id": alert["alert_id"], "event_id": ev,
                                            "enabledEvents": dcr_enabled(events)},
                                   event_time=et))
            elif kind not in ("start_case", "execute"):
                raise ValueError(f"unknown dcr action {kind!r}")
        else:
            raise ValueError(f"unknown engine {engine!r}")

    async def _dcr(self, fn, *args):
        return await asyncio.to_thread(fn, *args)

    def _learn_labels(self, events):
        for e in events or []:
            lbl = e.get("label") or e.get("description")
            if e.get("id") and lbl:
                self._dcr_labels[e["id"]] = lbl

    def dcr_label(self, event_id):
        """What a DCR event is called in pais.events and XES: its label (as the model shows it),
        the id only if the API never told us a label."""
        return self._dcr_labels.get(event_id, event_id)

    # ---------------- PAIS -> CEP (DCR: events executed by people in the portal) ----------------
    async def dcr_watch_once(self):
        """Poll every known DCR case once; report events that became executed since the last look
        and that the bridge didn't execute itself. The first look at a case (e.g. after a bridge
        restart) only sets the baseline. Limitation: the API shows an executed FLAG, so a second
        execution of the same event is invisible. Returns the number of reported events."""
        cfg = self.mapping.get("dcr_watch")
        if not cfg or self.dcr is None or self._dcr_watch_off:
            return 0
        wanted = set(cfg.get("events") or [])          # empty = report every event
        n = 0
        for (engine, _proc, ck), case in list(self.cases.items()):
            if engine != "dcr":
                continue
            try:
                events = await self._dcr(self.dcr.events, case)
            except Exception as e:
                self.print(f"[dcr_watch] case {case}: {e}")
                continue
            now_exec = dcr_executed(events)
            if now_exec is None:
                self.print("[dcr_watch] DCR API returns no 'executed' attribute: watch DISABLED. "
                           "Run tools/dcr_probe.py to see what it does return.")
                self._dcr_watch_off = True
                return n
            self._learn_labels(events)
            before = self._dcr_seen.get(case)
            self._dcr_seen[case] = set(now_exec) if before is None else before | now_exec
            if before is None:
                continue
            for ev in sorted(now_exec - before):
                label = self.dcr_label(ev)
                if wanted and ev not in wanted and label not in wanted:
                    continue
                self._out(envelope(label, ck, case, "dcr", self.dcr_process, role="dcr-user",
                                   payload={"source": "dcr-portal", "event_id": ev,
                                            "enabledEvents": dcr_enabled(events)},
                                   event_time=self._case_time.get(case)))
                n += 1
        return n

    # ---------------- XES for BPMN processes, read from Camunda ----------------
    LOG_TAGS = ("userTask", "serviceTask", "sendTask", "receiveTask", "businessRuleTask", "scriptTask",
                "task", "manualTask", "callActivity", "startEvent", "boundaryEvent",
                "intermediateCatchEvent", "intermediateThrowEvent")
    FINAL_POLLS = 3   # polls after an instance ended, so late exporter records still make it

    async def camunda_log_once(self):
        """Log newly completed activities of every known BPMN process instance, in end-time order,
        with Camunda's end time as the XES timestamp and the element's lane as org:resource.
        First run ever (no camunda_log.json): instances that already exist are only marked as
        seen, not logged (their history was logged the old way). Returns the number logged."""
        cfg = self.mapping.get("camunda_log")
        if not self.camunda_log_on():
            return 0
        tags = set(cfg.get("tags") or self.LOG_TAGS) if isinstance(cfg, dict) else set(self.LOG_TAGS)
        st = self._clog
        baseline = not st.get("_baseline_done")
        n = 0
        for (engine, pid, ck), key in list(self.cases.items()):
            if engine != "zeebe":
                continue
            rec = st.setdefault(str(key), {"seen": [], "final": 0})
            if baseline:
                rec["final"] = self.FINAL_POLLS        # pre-existing: never log
                continue
            if rec["final"] >= self.FINAL_POLLS:
                continue
            try:
                state = await asyncio.to_thread(self.camunda.instance_state, key)
                items = await asyncio.to_thread(self.camunda.element_instances, key)
            except Exception as e:
                self.print(f"[camunda_log] instance {key}: {e}")
                continue
            seen = set(rec["seen"])
            meta = self.element_meta.get(pid, {})
            done = [i for i in items
                    if i.get("state") == "COMPLETED" and i.get("endDate")
                    and str(i.get("elementInstanceKey")) not in seen]
            for i in sorted(done, key=lambda i: (_iso(i["endDate"]), str(i.get("elementInstanceKey")))):
                seen.add(str(i.get("elementInstanceKey")))
                m = meta.get(i.get("elementId"), {})
                name = m.get("name") or i.get("elementName") or ""
                if not name or m.get("tag", "") not in tags:
                    continue        # gateways, subprocess bodies, unnamed events
                self._out(envelope(name, ck, key, "camunda", pid, role=m.get("lane") or "camunda",
                                   payload={"element_id": i.get("elementId"), "bpmn_type": m.get("tag"),
                                            "source": "camunda"},
                                   timestamp=_iso(i["endDate"])), emit=False)
                n += 1
            rec["seen"] = sorted(seen)
            if state in ("COMPLETED", "TERMINATED", "CANCELED"):
                rec["final"] += 1
        if baseline:
            st["_baseline_done"] = True
        self._save_json("camunda_log.json", st)
        return n

    # ---------------- PAIS -> CEP (Zeebe service tasks) ----------------
    def task_types(self):
        return dict(self.mapping.get("zeebe_tasks", {}))

    def on_zeebe_job(self, task_type, variables, process_instance_key, bpmn_process_id,
                     headers=None, element_id=None):
        """Called for every activated job of a configured task type. Returns the
        variables to set on completion (from the mapping's `set`, may be empty).

        Activity name: from the task header named by `activity_from_header` (generic
        `cep-event` type: one worker for every send element), else the fixed `activity`,
        else the element id. A missing header still completes the job (a stuck process is
        worse) but the envelope carries an error so it shows up in logs."""
        cfg = self.task_types().get(task_type, {})
        headers = headers or {}
        error = ""
        hdr = cfg.get("activity_from_header")
        if hdr and headers.get(hdr):
            activity = headers[hdr]
        else:
            activity = cfg.get("activity") or element_id or task_type
            if hdr:
                error = f"task header {hdr!r} missing on element {element_id!r}"
        ck = variables.get("caseKey", "")
        payload = {k: v for k, v in variables.items() if k != "alert"} | {"task_type": task_type}
        if element_id:
            payload["element_id"] = element_id
        self._out(envelope(activity, ck, process_instance_key, "camunda", bpmn_process_id,
                           payload=payload, error=error, event_time=variables.get("cepEventTime")),
                  emit=cfg.get("emit", True), log=not self.camunda_log_on() or bool(error))
        return dict(cfg.get("set", {}))


def load_mapping(path):
    with open(path, encoding="utf-8") as fh:
        m = json.load(fh)
    for t, actions in m.get("on_alert", {}).items():
        if not isinstance(actions, list):
            raise ValueError(f"on_alert.{t} must be a list of actions")
        for a in actions:
            if a.get("engine") == "zeebe" and a.get("action") == "start_process" and not a.get("process_id"):
                raise ValueError(f"on_alert.{t}: start_process needs process_id")
            if a.get("engine") == "zeebe" and a.get("action") in ("publish_message", "correlate_message") \
                    and not a.get("message"):
                raise ValueError(f"on_alert.{t}: {a['action']} needs message")
            if a.get("engine") == "zeebe" and a.get("action") not in (
                    "start_process", "publish_message", "correlate_message"):
                raise ValueError(f"on_alert.{t}: unknown zeebe action {a.get('action')!r}")
    return m


class _CaseMap(dict):
    """dict that persists itself on every write."""

    def __init__(self, initial, saver):
        super().__init__(initial)
        self._saver = saver

    def __setitem__(self, key, value):
        super().__setitem__(key, str(value))
        self._saver(dict(self))
