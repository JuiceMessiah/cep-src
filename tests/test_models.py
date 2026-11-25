"""
Static checks for the models in bpmn/ and their wiring in bridge/mapping.json.
Catches the things Camunda would reject at deployment (or that would silently break
the integration) without a running engine:

  - service/send tasks: exactly one zeebe:taskDefinition, type handled in mapping.json
  - cep-event tasks: header "activity" == element name (it becomes the XES activity)
  - user tasks: Camunda user tasks (job-worker user tasks are invisible in Tasklist 8.8)
  - business rule tasks: calledDecision whose decision id exists in a bpmn/*.dmn
  - exclusive gateways: every outgoing flow has a condition or is the default;
    conditions only on flows that leave a gateway
  - message events/receive tasks: messageRef with a name; non-start ones a correlation key
  - multi-instance: zeebe:loopCharacteristics with inputCollection
  - mapping.json: process ids, message names and log activities exist in the models
  - no "--" inside XML comments (Maven/XML parsers reject it)

  python3 tests/test_models.py
"""
import glob
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
B = "{http://www.omg.org/spec/BPMN/20100524/MODEL}"
Z = "{http://camunda.org/schema/zeebe/1.0}"
DMN = "{https://www.omg.org/spec/DMN/20191111/MODEL/}"

errors = []


def err(where, msg):
    errors.append(f"{where}: {msg}")


# ---------- XML comments ----------
for f in [*glob.glob(str(ROOT / "bpmn" / "*.*")), str(ROOT / "flink-job" / "pom.xml")]:
    if f.endswith((".bpmn", ".dmn", ".xml")):
        for c in re.findall(r"<!--(.*?)-->", Path(f).read_text(encoding="utf-8"), re.S):
            if "--" in c:
                err(Path(f).name, "'--' inside an XML comment")

# ---------- DMN decisions ----------
decisions = set()
for f in glob.glob(str(ROOT / "bpmn" / "*.dmn")):
    for d in ET.parse(f).getroot().iter(DMN + "decision"):
        decisions.add(d.get("id"))

mapping = json.loads((ROOT / "bridge" / "mapping.json").read_text())
task_types = set(mapping.get("zeebe_tasks", {}))

# ---------- BPMN ----------
processes = {}   # process id -> {"names": set, "messages": set, "file": str}
for f in sorted(glob.glob(str(ROOT / "bpmn" / "*.bpmn"))):
    fname = Path(f).name
    root = ET.parse(f).getroot()
    messages = {m.get("id"): m for m in root.iter(B + "message")}

    def ext(el, tag):
        x = el.find(B + "extensionElements")
        return [] if x is None else x.findall(Z + tag)

    for proc in root.iter(B + "process"):
        if proc.get("isExecutable") != "true":
            continue
        pid = proc.get("id")
        names, used_msgs = set(), set()
        for el in proc.iter():
            if el.get("name"):
                names.add(el.get("name"))
        here = f"{fname}:{pid}"

        for tag in ("serviceTask", "sendTask"):
            for el in proc.iter(B + tag):
                td = ext(el, "taskDefinition")
                if len(td) != 1:
                    err(here, f"{tag} {el.get('name')!r} needs exactly one zeebe:taskDefinition")
                    continue
                t = td[0].get("type")
                if t not in task_types:
                    err(here, f"{tag} {el.get('name')!r}: job type {t!r} has no worker in mapping.json")
                act_hdr = mapping["zeebe_tasks"].get(t, {}).get("activity_from_header")
                if act_hdr:
                    hdrs = {h.get("key"): h.get("value")
                            for th in ext(el, "taskHeaders") for h in th.findall(Z + "header")}
                    if hdrs.get(act_hdr) != el.get("name"):
                        err(here, f"{tag} {el.get('name')!r}: header {act_hdr!r} = {hdrs.get(act_hdr)!r}, "
                                  "must equal the element name")

        for el in proc.iter(B + "userTask"):
            if not ext(el, "userTask"):
                err(here, f"userTask {el.get('name')!r} is job-worker based; make it a Camunda user task")

        for el in proc.iter(B + "businessRuleTask"):
            cd = ext(el, "calledDecision")
            if len(cd) != 1:
                err(here, f"businessRuleTask {el.get('name')!r} needs zeebe:calledDecision")
            elif cd[0].get("decisionId") not in decisions:
                err(here, f"businessRuleTask {el.get('name')!r}: decision {cd[0].get('decisionId')!r} not in bpmn/*.dmn")
            elif not cd[0].get("resultVariable"):
                err(here, f"businessRuleTask {el.get('name')!r}: resultVariable missing")

        flows = {fl.get("id"): fl for fl in proc.iter(B + "sequenceFlow")}
        for gw in proc.iter(B + "exclusiveGateway"):
            outs = [fl for fl in flows.values() if fl.get("sourceRef") == gw.get("id")]
            if len(outs) > 1:
                for fl in outs:
                    if fl.get("id") != gw.get("default") and fl.find(B + "conditionExpression") is None:
                        err(here, f"gateway {gw.get('name') or gw.get('id')!r}: flow {fl.get('id')} "
                                  "has no condition and isn't the default")

        gateways = {g.get("id") for t in ("exclusiveGateway", "inclusiveGateway") for g in proc.iter(B + t)}
        for fl in flows.values():
            if fl.find(B + "conditionExpression") is not None and fl.get("sourceRef") not in gateways:
                err(here, f"flow {fl.get('id')} has a condition but doesn't leave a gateway (Camunda rejects it)")

        def check_msg(ref, what, needs_key):
            m = messages.get(ref)
            if m is None or not m.get("name"):
                err(here, f"{what}: messageRef {ref!r} missing or message has no name")
                return
            used_msgs.add(m.get("name"))
            if needs_key and not ext(m, "subscription"):
                err(here, f"{what}: message {m.get('name')!r} needs a zeebe:subscription correlationKey")

        parent = {c: p for p in proc.iter() for c in p}
        for med in proc.iter(B + "messageEventDefinition"):
            ev = parent[med]
            check_msg(med.get("messageRef"), f"{ev.tag.split('}')[1]} {ev.get('name')!r}",
                      needs_key=ev.tag != B + "startEvent")
        for rt in proc.iter(B + "receiveTask"):
            check_msg(rt.get("messageRef"), f"receiveTask {rt.get('name')!r}", needs_key=True)

        for mi in proc.iter(B + "multiInstanceLoopCharacteristics"):
            lc = ext(mi, "loopCharacteristics")
            if len(lc) != 1 or not lc[0].get("inputCollection"):
                err(here, f"multi-instance {parent[mi].get('name')!r} needs zeebe:loopCharacteristics inputCollection")

        processes[pid] = {"names": names, "messages": used_msgs, "file": fname}

# ---------- mapping.json against the models ----------
for alert_type, actions in mapping.get("on_alert", {}).items():
    for a in actions:
        if a.get("engine") != "zeebe":
            continue
        where = f"mapping.json on_alert.{alert_type}"
        p = processes.get(a.get("process_id"))
        if p is None:
            err(where, f"process_id {a.get('process_id')!r} is not an executable process in bpmn/")
            continue
        if a.get("message") and a["message"] not in p["messages"]:
            err(where, f"message {a['message']!r} isn't used in {p['file']}")
        la = a.get("log_activity")
        if la and la not in p["names"]:
            err(where, f"log_activity {la!r} isn't an element name in {p['file']} (conformance check would miss it)")
all_names = set().union(*(p["names"] for p in processes.values())) if processes else set()
for t, cfg in mapping.get("zeebe_tasks", {}).items():
    if cfg.get("activity") and cfg["activity"] not in all_names:
        err(f"mapping.json zeebe_tasks.{t}", f"activity {cfg['activity']!r} isn't an element name in any model")

if errors:
    print("\n".join(errors))
    sys.exit(f"\n{len(errors)} model problem(s)")
print(f"models OK: {sorted(processes)}; decisions {sorted(decisions)}")
