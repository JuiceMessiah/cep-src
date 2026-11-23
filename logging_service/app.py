"""
Logging service: collects PAIS/CEP events and writes XES event logs for
conformance checking. Based on the course example (POESS-example
logging_service), with three changes:

  * one XES file PER PROCESS (BPMN process id / DCR graph), because conformance
    is checked per model; plus one combined file
  * events are appended to a JSONL file and kept in memory (no re-reading the
    whole store on every request)
  * accepts the bridge envelope (case_key/engine/process, payload as object)
    as well as the example's format (order_id, payload as JSON string)

POST /log      one event (JSON)
GET  /events   all events of this run (JSON)
GET  /health
Files: /logs/<run>_events.jsonl, /XES/<run>_<process>.xes, /XES/<run>_all.xes
"""
import json
import os
import re
import threading
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request

LOG_DIR = Path(os.getenv("LOG_DIR", "/logs"))
XES_DIR = Path(os.getenv("XES_DIR", "/XES"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
XES_DIR.mkdir(parents=True, exist_ok=True)

RUN_ID = datetime.now(timezone.utc).strftime("%y%m%d_%H%M%S")
EVENT_STORE = LOG_DIR / f"{RUN_ID}_events.jsonl"

app = Flask(__name__)
_events = []
_lock = threading.Lock()


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _maybe_json(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return {"raw": value} if value else {}
    return value if value is not None else {}


def normalize(data):
    p = data.get("event") if isinstance(data.get("event"), dict) else data
    p = _maybe_json(p)
    case_key = p.get("case_key") or p.get("order_id") or ""
    return {
        "event": str(p.get("event") or p.get("activity") or p.get("name") or "unknown"),
        "case_id": str(p.get("case_id") or p.get("case") or "UNKNOWN_CASE"),
        "case_key": str(case_key),
        "engine": str(p.get("engine") or ""),
        "process": str(p.get("process") or "unknown"),
        "role": str(p.get("role") or p.get("user") or p.get("actor") or "system"),
        "timestamp": str(p.get("timestamp") or utc_now_iso()),
        "event_time": p.get("event_time"),
        "error": str(p.get("error") or ""),
        "payload": _maybe_json(p.get("payload")),
    }


def _safe(name):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:80] or "unknown"


def build_xes(events, path):
    root = ET.Element("log", {"xes.version": "2.0", "xes.features": "nested-attributes",
                              "openxes.version": "1.0RC7"})
    for name, prefix, uri in (("Concept", "concept", "http://www.xes-standard.org/concept.xesext"),
                              ("Time", "time", "http://www.xes-standard.org/time.xesext"),
                              ("Organizational", "org", "http://www.xes-standard.org/org.xesext")):
        ET.SubElement(root, "extension", {"name": name, "prefix": prefix, "uri": uri})
    g = ET.SubElement(root, "global", {"scope": "trace"})
    ET.SubElement(g, "string", {"key": "concept:name", "value": "name"})
    g = ET.SubElement(root, "global", {"scope": "event"})
    ET.SubElement(g, "string", {"key": "concept:name", "value": "name"})
    ET.SubElement(g, "date", {"key": "time:timestamp", "value": "1970-01-01T00:00:00.000+00:00"})
    ET.SubElement(g, "string", {"key": "org:resource", "value": "resource"})
    ET.SubElement(root, "classifier", {"name": "Activity", "keys": "concept:name"})

    traces = {}
    for e in events:
        traces.setdefault((e["process"], e["case_id"]), []).append(e)
    for (process, case_id), evs in traces.items():
        tr = ET.SubElement(root, "trace")
        ET.SubElement(tr, "string", {"key": "concept:name", "value": case_id})
        ET.SubElement(tr, "string", {"key": "process", "value": process})
        if evs[0]["case_key"]:
            ET.SubElement(tr, "string", {"key": "case_key", "value": evs[0]["case_key"]})
        for e in sorted(evs, key=lambda x: x["timestamp"]):
            ev = ET.SubElement(tr, "event")
            ET.SubElement(ev, "string", {"key": "concept:name", "value": e["event"]})
            ET.SubElement(ev, "date", {"key": "time:timestamp", "value": e["timestamp"]})
            ET.SubElement(ev, "string", {"key": "org:resource", "value": e["role"]})
            for k in ("engine", "error"):
                if e[k]:
                    ET.SubElement(ev, "string", {"key": k, "value": e[k]})
            if e["event_time"] is not None:
                ET.SubElement(ev, "string", {"key": "event_time", "value": str(e["event_time"])})
            payload = e["payload"]
            if isinstance(payload, dict):
                for k, v in payload.items():
                    if v is not None:
                        val = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                        ET.SubElement(ev, "string", {"key": f"payload:{k}", "value": val})
    ET.indent(root)
    tmp = path.with_suffix(".tmp")
    ET.ElementTree(root).write(tmp, encoding="utf-8", xml_declaration=True)
    tmp.replace(path)          # atomic: a reader never sees a half-written file


def write_outputs(event):
    with open(EVENT_STORE, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(event) + "\n")
    build_xes([e for e in _events if e["process"] == event["process"]],
              XES_DIR / f"{RUN_ID}_{_safe(event['process'])}.xes")
    build_xes(_events, XES_DIR / f"{RUN_ID}_all.xes")


@app.post("/log")
def log_event():
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or not data:
        return jsonify({"status": "invalid", "message": "JSON object required"}), 400
    event = normalize(data)
    with _lock:
        _events.append(event)
        write_outputs(event)
    flag = f" ERROR={event['error']}" if event["error"] else ""
    print(f"[XES] {event['process']} case={event['case_id']} {event['event']} ({event['role']}){flag}",
          flush=True)
    return jsonify({"status": "logged"}), 200


@app.get("/events")
def events():
    with _lock:
        return jsonify(list(_events))


@app.get("/health")
def health():
    return jsonify({"status": "ok", "events": len(_events), "run": RUN_ID})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5010")))
