#!/usr/bin/env python3
"""
replay.py: publish a scenario (from backfill.py) to Redpanda, time-compressed.

Events keep their ORIGINAL event_time (unless --rebase-now), so an event-time
Flink job produces the same alerts as it would have in real time. Only the
wall-clock pacing is compressed. ingest_time is set at publish time.

Streams go to "<stream>.<suffix>" (default firms.replay / weather.replay), so
live topics stay clean. Point the Flink job at the replay topics via its args.

After the last event, one heartbeat per stream is sent --heartbeat-h hours later
(event time) so watermarks pass the end of the scenario and the job's last
windows/patterns fire. The job must assign timestamps/watermarks FIRST and only
then filter out type == "heartbeat".

Usage:
  python3 replay.py scenarios/x.jsonl --duration 120          # whole scenario in ~2 min
  python3 replay.py scenarios/x.jsonl --speed 3600            # 1 h of event time per second
  python3 replay.py scenarios/x.jsonl --dry-run               # print the plan, send nothing
  --proxy http://localhost:18082   from the laptop host (compose.demo.yaml)
  --proxy http://redpanda:8082     inside either compose stack (default)

Stdlib only.
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import datetime, timezone


def fmt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def produce(proxy, topic, records):
    """Same wire format as poller.py: Kafka REST v2 JSON via the Redpanda HTTP proxy."""
    for i in range(0, len(records), 500):
        chunk = records[i:i + 500]
        body = json.dumps({"records": [{"key": k, "value": v} for k, v in chunk]}).encode()
        req = urllib.request.Request(
            f"{proxy}/topics/{urllib.parse.quote(topic)}",
            data=body, method="POST",
            headers={"Content-Type": "application/vnd.kafka.json.v2+json",
                     "Accept": "application/vnd.kafka.v2+json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                resp = json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            raise SystemExit(f"produce to {topic}: HTTP {e.code} (topic missing? see runbook Phase 4)") from None
        except urllib.error.URLError as e:
            raise SystemExit(f"produce to {topic}: cannot reach proxy {proxy}: {e.reason}") from None
        errs = [o for o in resp.get("offsets", []) if o.get("error_code")]
        if errs:
            raise SystemExit(f"produce to {topic}: {len(errs)} record errors, first={errs[0]}")


def load(path):
    meta, records = {}, []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if "meta" in obj:
                meta = obj["meta"]
            else:
                records.append(obj)
    if not records:
        raise SystemExit(f"{path}: no events")
    records.sort(key=lambda r: r["value"]["event_time"])
    return meta, records


def main():
    ap = argparse.ArgumentParser(description="Replay a CEP scenario into Redpanda.")
    ap.add_argument("scenario")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--duration", type=float, help="wall-clock seconds for the whole scenario")
    g.add_argument("--speed", type=float, help="event-time seconds per wall-clock second")
    ap.add_argument("--proxy", default="http://redpanda:8082")
    ap.add_argument("--suffix", default="replay", help="topic suffix (default: replay)")
    ap.add_argument("--rebase-now", action="store_true",
                    help="shift event times so the scenario starts now (relative timing kept)")
    ap.add_argument("--heartbeat-h", type=float, default=6.0,
                    help="heartbeat this many event-time hours after the last event (0 = off)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    meta, records = load(a.scenario)
    t0 = records[0]["value"]["event_time"]
    tn = records[-1]["value"]["event_time"]
    span_s = max((tn - t0) / 1000, 1.0)

    if a.rebase_now:
        shift = int(time.time() * 1000) - t0
        for r in records:
            r["value"]["event_time"] += shift
        t0, tn = t0 + shift, tn + shift

    if a.speed:
        speed = a.speed
    else:
        speed = span_s / (a.duration if a.duration else 120.0)
    wall = span_s / speed

    streams = sorted({r["stream"] for r in records})
    topics = {s: f"{s}.{a.suffix}" for s in streams}
    print(f"scenario {meta.get('name', a.scenario)}: {len(records)} events, "
          f"{fmt(t0)} -> {fmt(tn)} UTC ({span_s / 3600:.1f} h)", flush=True)
    print(f"speed x{speed:,.0f}: ~{wall:.0f} s wall clock; topics {', '.join(topics.values())}", flush=True)
    if a.dry_run:
        return

    # group events by event_time so simultaneous events go out in one batch per topic
    groups = OrderedDict()
    for r in records:
        groups.setdefault(r["value"]["event_time"], []).append(r)

    start_wall = time.monotonic()
    sent, last_report = 0, 0.0
    for et, batch in groups.items():
        due = start_wall + (et - t0) / 1000 / speed
        delay = due - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        now_ms = int(time.time() * 1000)
        per_topic = {}
        for r in batch:
            v = dict(r["value"])
            v["ingest_time"] = now_ms
            per_topic.setdefault(topics[r["stream"]], []).append((r["key"], v))
        for topic, recs in per_topic.items():
            produce(a.proxy, topic, recs)
        sent += len(batch)
        if time.monotonic() - last_report > 5 or sent == len(records):
            print(f"  {fmt(et)}  {sent}/{len(records)} sent", flush=True)
            last_report = time.monotonic()

    if a.heartbeat_h > 0:
        hb_time = tn + int(a.heartbeat_h * 3_600_000)
        now_ms = int(time.time() * 1000)
        for topic in topics.values():
            produce(a.proxy, topic, [("heartbeat", {"type": "heartbeat", "event_time": hb_time,
                                                    "ingest_time": now_ms, "replay": True})])
        print(f"heartbeat at {fmt(hb_time)} UTC sent to {', '.join(topics.values())}", flush=True)
    print("done", flush=True)


if __name__ == "__main__":
    main()
