#!/usr/bin/env python3
"""
send_alert.py: put one CEP alert on the `alerts` topic by hand, to test the
bridge + Camunda/DCR before the Flink job exists. Stdlib only.

  python3 tools/send_alert.py SmokeTest c3_4
  python3 tools/send_alert.py SmokeTestUpdate c3_4
  python3 tools/send_alert.py FireSpreadRisk c3_4 --severity high --detail frp=42.5 --detail wind_ms=11.2

  --proxy http://localhost:18082   (default; compose.demo.yaml publishes the proxy on localhost)
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid


def main():
    ap = argparse.ArgumentParser(description="Send one alert to the alerts topic.")
    ap.add_argument("type", help="alert type, e.g. SmokeTest (must exist in bridge/mapping.json)")
    ap.add_argument("case_key", help="business key, e.g. a grid cell like c3_4")
    ap.add_argument("--severity", default="medium")
    ap.add_argument("--detail", action="append", default=[], help="key=value, repeatable")
    ap.add_argument("--event-time", type=int, help="epoch ms (default: now)")
    ap.add_argument("--proxy", default="http://localhost:18082")
    ap.add_argument("--topic", default="alerts")
    a = ap.parse_args()

    details = {}
    for kv in a.detail:
        k, _, v = kv.partition("=")
        try:
            details[k] = float(v)
        except ValueError:
            details[k] = v
    now = int(time.time() * 1000)
    alert = {
        "alert_id": str(uuid.uuid4()),
        "type": a.type,
        "case_key": a.case_key,
        "cell": a.case_key,
        "severity": a.severity,
        "event_time": a.event_time or now,
        "detected_time": now,
        "details": details,
        "source": "send_alert.py",
    }
    body = json.dumps({"records": [{"key": a.case_key, "value": alert}]}).encode()
    req = urllib.request.Request(f"{a.proxy}/topics/{a.topic}", data=body, method="POST",
                                 headers={"Content-Type": "application/vnd.kafka.json.v2+json",
                                          "Accept": "application/vnd.kafka.v2+json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"sent {a.type} for {a.case_key}: HTTP {r.status} {r.read().decode()[:120]}")
    except urllib.error.URLError as e:
        sys.exit(f"cannot send to {a.proxy}: {e}")


if __name__ == "__main__":
    main()
