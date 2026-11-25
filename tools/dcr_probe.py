#!/usr/bin/env python3
"""
dcr_probe.py: what does the DCR Graphs API actually return for a case's events?

The bridge's DCR watch needs an `executed` attribute on <event> tags. The course example
only proves `enabled` exists. Run this ONCE with the real graph + account and read the
attribute names it prints. Prints attribute NAMES, counts and event ids/labels only;
credentials never appear (they're in the Authorization header only).

  env DCR_GRAPH_ID=... DCR_USERNAME=... DCR_PASSWORD=... python3 tools/dcr_probe.py           # newest case
  env ... python3 tools/dcr_probe.py --create                                                 # make a fresh case

On the NAS, with the credentials in bridge.env and nothing typed on the command line:
  docker run --rm --env-file /mnt/fast/apps/stacks/apache-flink-cep/bridge.env --user 568:568 \\
    -v /mnt/fast/apps/cep-src/bridge/dcr.py:/app/dcr.py:ro -v /mnt/fast/apps/cep-src/tools/dcr_probe.py:/app/p.py:ro \\
    python:3.12-slim python /app/p.py
"""
import argparse
import os
import re
import sys
from collections import Counter
from pathlib import Path

for d in (Path(__file__).resolve().parent.parent / "bridge", Path(__file__).resolve().parent):
    sys.path.insert(0, str(d))
from dcr import DcrClient, enabled, executed, parse_events  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--create", action="store_true", help="create a new case (sim) and probe that")
a = ap.parse_args()

c = DcrClient(os.environ.get("DCR_GRAPH_ID"), os.environ.get("DCR_USERNAME"), os.environ.get("DCR_PASSWORD"))
if a.create:
    case = c.create_case()
else:
    status, xml = c._call("GET", "/sims?filter=all")
    ids = re.findall(r'<trace[^>]*id="(\d+)"', xml)
    if status != 200 or not ids:
        sys.exit(f"no cases found (HTTP {status}); run with --create")
    case = ids[-1]
status, xml = c._call("GET", f"/sims/{case}/events")
evs = parse_events(xml)
print(f"graph {c.graph_id}, case {case}: HTTP {status}, {len(evs)} <event> tags")
print("attribute names (count):", dict(Counter(k for e in evs for k in e)))
for e in evs:
    print(f"  {e.get('id', '?'):30} label={e.get('label', '')!r:40} "
          f"enabled={e.get('enabled')} executed={e.get('executed')} pending={e.get('pending')}")
ex = executed(evs)
print("\nenabled:", enabled(evs))
print("executed:", sorted(ex) if ex is not None else "NO 'executed' ATTRIBUTE -> bridge dcr_watch can't work as built")
