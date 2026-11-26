#!/usr/bin/env python3
"""
send_pais.py: put one PAIS event on `pais.events` by hand, as if a process engine had
reported it. Tests the CEP's process patterns before the real source exists, above all
CapacityRedirect, which needs DCR's "Emergency order" before the DCR graph is online.
Stdlib only. Goes to pais.events only: Flink sees it, the XES log does not.

  python3 tools/send_pais.py "Emergency order" inc-c529_691-20250813T0310
  python3 tools/send_pais.py "Receive emergency delivery" inc-c529_691-20250813T0310
  python3 tools/send_pais.py "Send EMS pre-alert" inc-... --engine camunda --unit amb-1

  --proxy http://localhost:18082   (default; compose.demo.yaml publishes the proxy on localhost)
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


def main():
    ap = argparse.ArgumentParser(description="Send one PAIS event to pais.events.")
    ap.add_argument("event", help='activity as the CEP sees it, e.g. "Emergency order" (DCR events by label)')
    ap.add_argument("case_key", help="incident key, e.g. inc-c529_691-20250813T0310 (must start with inc-)")
    ap.add_argument("--engine", default="dcr", choices=["dcr", "camunda"])
    ap.add_argument("--unit", help="unitId in the payload (BPMN per-ambulance events)")
    ap.add_argument("--event-time", type=int, help="epoch ms (default: now)")
    ap.add_argument("--proxy", default="http://localhost:18082")
    ap.add_argument("--topic", default="pais.events")
    a = ap.parse_args()
    if not a.case_key.startswith("inc-"):
        sys.exit("case_key must be an incident key (inc-...): the CEP ignores everything else")

    payload = {"source": "send_pais.py"}
    if a.unit:
        payload["unitId"] = a.unit
    now = int(time.time() * 1000)
    ev = {
        "event": a.event,
        "case_key": a.case_key,
        "case_id": "manual",
        "engine": a.engine,
        "process": "dcr:manual" if a.engine == "dcr" else "manual",
        "role": "send_pais.py",
        "event_time": a.event_time or now,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "payload": payload,
        "error": "",
    }
    body = json.dumps({"records": [{"key": a.case_key, "value": ev}]}).encode()
    req = urllib.request.Request(f"{a.proxy}/topics/{a.topic}", data=body, method="POST",
                                 headers={"Content-Type": "application/vnd.kafka.json.v2+json",
                                          "Accept": "application/vnd.kafka.v2+json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"sent {a.event!r} for {a.case_key}: HTTP {r.status} {r.read().decode()[:120]}")
    except urllib.error.URLError as e:
        sys.exit(f"cannot send to {a.proxy}: {e}")


if __name__ == "__main__":
    main()
