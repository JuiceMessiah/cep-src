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

No engine imports here: Zeebe and DCR clients are injected (testable offline).
"""
import json
import time
from datetime import datetime, timezone

REQUIRED_ALERT_FIELDS = ("alert_id", "type", "case_key", "event_time")


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def now_ms():
    return int(time.time() * 1000)


def envelope(event, case_key, case_id, engine, process, *, payload=None, error="",
             role=None, event_time=None):
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
        "timestamp": now_iso(),
        "event_time": int(event_time) if event_time is not None else now_ms(),
        "payload": payload or {},
        "error": error or "",
    }


class Bridge:
    def __init__(self, mapping, *, zeebe=None, dcr=None, emit=None, log=None, printer=print):
        self.mapping = mapping
        self.zeebe = zeebe          # async start_process(pid, vars) -> key ; async publish_message(name, key, vars)
        self.dcr = dcr              # sync create_case() -> id ; execute(case, event) -> enabled ; enabled_events(case)
        self.emit = emit or (lambda env: None)   # -> pais.events
        self.log = log or (lambda env: None)     # -> logging service
        self.print = printer
        self.cases = {}             # (engine, process, case_key) -> engine case id
        self.dcr_process = f"dcr:{getattr(dcr, 'graph_id', 'graph')}"

    # ---------------- helpers ----------------
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
                await self.zeebe.publish_message(msg, ck, variables)
                pid = action.get("process_id", "")
                if log_activity:
                    self._out(envelope(log_activity, ck, self.cases.get(("zeebe", pid, ck)), "cep",
                                       pid or "camunda", role="cep",
                                       payload={"message": msg, "alert_id": alert["alert_id"]},
                                       event_time=et), emit=False)
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
                try:
                    enabled = await self._dcr(self.dcr.execute, case, ev)
                except Exception as e:
                    # A rejected event is a compliance signal, not just an error: log + emit it.
                    self._out(envelope(ev, ck, case, "dcr", self.dcr_process,
                                       payload={"alert_id": alert["alert_id"], "rejected": True},
                                       error=str(e), event_time=et))
                    return
                self._out(envelope(ev, ck, case, "dcr", self.dcr_process,
                                   payload={"alert_id": alert["alert_id"], "enabledEvents": enabled},
                                   event_time=et))
            elif kind not in ("start_case", "execute"):
                raise ValueError(f"unknown dcr action {kind!r}")
        else:
            raise ValueError(f"unknown engine {engine!r}")

    async def _dcr(self, fn, *args):
        import asyncio
        return await asyncio.to_thread(fn, *args)

    # ---------------- PAIS -> CEP (Zeebe service tasks) ----------------
    def task_types(self):
        return dict(self.mapping.get("zeebe_tasks", {}))

    def on_zeebe_job(self, task_type, variables, process_instance_key, bpmn_process_id):
        """Called for every activated job of a configured task type. Returns the
        variables to set on completion (from the mapping's `set`, may be empty)."""
        cfg = self.task_types().get(task_type, {})
        activity = cfg.get("activity", task_type)
        ck = variables.get("caseKey", "")
        self._out(envelope(activity, ck, process_instance_key, "camunda", bpmn_process_id,
                           payload={k: v for k, v in variables.items() if k != "alert"} | {"task_type": task_type},
                           event_time=variables.get("cepEventTime")),
                  emit=cfg.get("emit", True))
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
            if a.get("engine") == "zeebe" and a.get("action") == "publish_message" and not a.get("message"):
                raise ValueError(f"on_alert.{t}: publish_message needs message")
    return m
