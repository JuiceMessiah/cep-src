"""
Camunda log (core.Bridge.camunda_log_once + bpmn_meta): XES for BPMN processes read from
Camunda's completed element instances. Fake Camunda, real ambulance model metadata.

  python3 tests/test_camunda_log.py
"""
import asyncio
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bridge"))
import bpmn_meta  # noqa: E402
from core import Bridge  # noqa: E402

checks = 0


def check(name, ok, detail=""):
    global checks
    if not ok:
        raise SystemExit(f"FAIL {name}: {detail}")
    checks += 1


class FakeCamunda:
    def __init__(self):
        self.items, self.state, self.calls = {}, {}, 0

    def element_instances(self, key):
        self.calls += 1
        return list(self.items.get(str(key), []))

    def instance_state(self, key):
        return self.state.get(str(key), "ACTIVE")


def item(k, eid, end, state="COMPLETED"):
    return {"elementInstanceKey": k, "elementId": eid, "state": state, "endDate": end, "elementName": "ignored"}


meta = bpmn_meta.load(ROOT / "bpmn")
check("meta lanes", meta["Ambulance_process"]["T_kit"]["lane"] == "Ambulance crew"
      and meta["Ambulance_process"]["T_assess"]["lane"] == "Dispatcher", meta["Ambulance_process"]["T_kit"])

tmp = Path(tempfile.mkdtemp())
state_file = str(tmp / "cases.json")
mapping = {"camunda_log": {"poll_s": 1}, "zeebe_tasks": {"cep-event": {"activity_from_header": "activity"}},
           "on_alert": {"FireAlert": [{"engine": "zeebe", "action": "correlate_message", "message": "FireAlert",
                                       "process_id": "Ambulance_process",
                                       "log_activity": "Major fire alert received"}]}}
logged, emitted = [], []
cam = FakeCamunda()
b = Bridge(mapping, emit=emitted.append, log=logged.append, printer=lambda *a: None,
           state_file=state_file, camunda=cam, element_meta=meta)

# 1. baseline: an instance that exists before the first poll is never logged
b.cases[("zeebe", "Ambulance_process", "inc-old")] = "100"
cam.items["100"] = [item("1", "T_assess", "2026-11-25T20:00:00.000Z")]
check("baseline logs nothing", asyncio.run(b.camunda_log_once()) == 0 and not logged)


# 2. a new incident: CEP trigger is NOT logged by the bridge (Camunda's record is), job event goes to Flink only
class Z:
    async def correlate_message(self, name, key, v):
        return "200"


b.zeebe = Z()
asyncio.run(b.handle_alert({"alert_id": "a1", "type": "FireAlert", "case_key": "inc-new", "event_time": 5}))
check("no CEP-trigger XES entry", not [e for e in logged if e["event"] == "Major fire alert received"], logged)
b.on_zeebe_job("cep-event", {"caseKey": "inc-new", "cepEventTime": 5}, "200", "Ambulance_process",
               headers={"activity": "Activate MCI protocol"}, element_id="T_mci")
check("job: pais yes, XES no", [e["event"] for e in emitted if e["event"] == "Activate MCI protocol"]
      and not [e for e in logged if e["event"] == "Activate MCI protocol"], (emitted, logged))
b.on_zeebe_job("cep-event", {"caseKey": "inc-new"}, "200", "Ambulance_process", headers={}, element_id="T_x")
check("job with error still logged", [e for e in logged if e["error"]], logged)
logged.clear()

# Camunda's view of instance 200 (out of order, mixed date formats, gateways, MI inner instances, an active one)
cam.items["200"] = [
    item("11", "S1", "2026-11-25T21:00:00.000Z"),
    item("13", "G1", "2026-11-25T21:01:00.100+0000"),                 # gateway: skipped
    item("12", "T_assess", "2026-11-25T21:01:00.000+00:00"),
    item("14", "T_mci", "2026-11-25T22:01:05.000+01:00"),              # = 21:01:05Z
    item("15", "T_kit", "2026-11-25T21:03:00.000Z"),                   # amb-1
    item("16", "T_kit", "2026-11-25T21:02:00.000Z"),                   # amb-2, earlier
    item("17", "T_drive", None, state="ACTIVE"),                       # not done yet
    item("18", "SP", "2026-11-25T21:04:00.000Z"),                      # subprocess: skipped
]
n = asyncio.run(b.camunda_log_once())
names = [e["event"] for e in logged]
check("logged count", n == 5, (n, names))
check("end-time order + model names", names == ["Major fire alert received", "Assess incident",
                                                 "Activate MCI protocol", "Verify burn kit", "Verify burn kit"], names)
check("lanes as resource", [e["role"] for e in logged] == ["Dispatcher", "Dispatcher", "Dispatcher",
                                                            "Ambulance crew", "Ambulance crew"], logged)
check("timestamps normalised", logged[2]["timestamp"] == "2026-11-25T21:01:05.000+00:00", logged[2]["timestamp"])
check("trace keys", all(e["case_id"] == "200" and e["case_key"] == "inc-new" and e["process"] == "Ambulance_process"
                        for e in logged), logged[0])
check("not emitted to Flink", not [e for e in emitted if e.get("payload", {}).get("source") == "camunda"])

# 3. next poll: only what's new
logged.clear()
cam.items["200"][6] = item("17", "T_drive", "2026-11-25T21:05:00.000Z")
check("incremental", asyncio.run(b.camunda_log_once()) == 1 and [e["event"] for e in logged] == ["Drive to scene"])

# 4. finished instance: polled FINAL_POLLS more times, then left alone (late exporter records still caught)
cam.state["200"] = "COMPLETED"
logged.clear()
asyncio.run(b.camunda_log_once())
cam.items["200"].append(item("19", "T_close", "2026-11-25T21:30:00.000Z"))   # exported late
asyncio.run(b.camunda_log_once())
check("late record caught", [e["event"] for e in logged] == ["Close incident and log response times"], logged)
asyncio.run(b.camunda_log_once())
calls = cam.calls
asyncio.run(b.camunda_log_once())
check("stops polling", cam.calls == calls, (calls, cam.calls))

# 5. restart: same state dir -> nothing logged twice, duplicate alert still recognised
logged.clear()
b2 = Bridge(mapping, emit=emitted.append, log=logged.append, printer=lambda *a: None,
            state_file=state_file, camunda=cam, element_meta=meta)
b2.cases[("zeebe", "Ambulance_process", "inc-new")] = "200"
cam.state["200"] = "ACTIVE"
check("restart: no re-log", asyncio.run(b2.camunda_log_once()) == 0 and not logged, logged)
check("restart: alert dedupe persisted", "a1" in b2._alerts_seen)
check("state file", json.loads((tmp / "camunda_log.json").read_text()).get("_baseline_done") is True)

# 6. off switch: without camunda_log the bridge logs CEP triggers itself again
logged.clear()
b3 = Bridge({k: v for k, v in mapping.items() if k != "camunda_log"}, zeebe=Z(), log=logged.append,
            printer=lambda *a: None)
asyncio.run(b3.handle_alert({"alert_id": "a9", "type": "FireAlert", "case_key": "inc-x", "event_time": 5}))
check("off: CEP trigger logged", [e["event"] for e in logged] == ["Major fire alert received"], logged)

print(f"OK: {checks} checks")
