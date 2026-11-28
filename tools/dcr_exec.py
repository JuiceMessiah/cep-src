#!/usr/bin/env python3
"""
dcr_exec.py: execute a DCR activity in the case the BRIDGE created for an incident, as a
person would in the portal. dcrgraphs.net's web UI can only start new simulations, not continue
the cases the bridge makes through the API, so this is how to work those cases by hand.
The graph still decides (not enabled = refused), and the bridge's DCR watch reports the
execution like any portal event (role dcr-user) -> pais.events + XES. Stdlib only.

  python3 tools/dcr_exec.py                                              # cases + enabled activities
  python3 tools/dcr_exec.py "Assess emergency demand" inc-c528_690-20250813T0310
  python3 tools/dcr_exec.py "Emergency order" inc-c528_690-20250813T0310

Reads the DCR login from bridge/.env (or the environment) and the incident -> case map from
the running bridge container (`docker exec <bridge> cat /state/cases.json`).
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bridge"))
from dcr import DcrClient, DcrError, containers, parse_events  # noqa: E402


def load_env(path):
    if path.exists():
        for line in path.read_text().splitlines():
            key, sep, value = line.strip().partition("=")
            if sep and not key.startswith("#"):
                os.environ.setdefault(key, value)


def bridge_cases(container, graph_id):
    """incident key -> DCR case id, for the cases the bridge created in this graph."""
    try:
        raw = subprocess.run(["docker", "exec", container, "cat", "/state/cases.json"],
                             capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError) as e:
        sys.exit(f"cannot read the bridge's case map from container {container!r}: {e}")
    prefix = f"dcr|dcr:{graph_id}|"
    return {k[len(prefix):]: v for k, v in json.loads(raw or "{}").items() if k.startswith(prefix)}


def activities(c, case):
    status, xml = c._call("GET", f"/sims/{case}/events")
    if status != 200:
        raise DcrError(f"events: HTTP {status}")
    events = parse_events(xml)
    nests = containers(events)
    return [e for e in events if e.get("id") not in nests]


def main():
    ap = argparse.ArgumentParser(description="Execute a DCR activity in the bridge's case for an incident.")
    ap.add_argument("activity", nargs="?", help='label as the graph shows it, e.g. "Emergency order"')
    ap.add_argument("case_key", nargs="?", help="incident key, e.g. inc-c528_690-20250813T0310")
    ap.add_argument("--container", default="cep-demo-bridge-1")
    a = ap.parse_args()

    load_env(ROOT / "bridge" / ".env")
    graph = os.environ.get("DCR_GRAPH_ID")
    c = DcrClient(graph, os.environ.get("DCR_USERNAME"), os.environ.get("DCR_PASSWORD"))
    cases = bridge_cases(a.container, graph)
    if not cases:
        sys.exit(f"the bridge has no DCR cases in graph {graph} yet (BurnSupplyEmergency starts one)")

    if not a.activity:
        for key, case in cases.items():
            enabled = [e.get("label") for e in activities(c, case) if e.get("enabled") == "true"]
            print(f"{key}  case {case}\n  enabled: {', '.join(enabled)}")
        return
    if a.case_key not in cases:
        sys.exit(f"no DCR case for {a.case_key!r}; known: {', '.join(cases)}")
    case = cases[a.case_key]
    acts = activities(c, case)
    match = [e for e in acts if (e.get("label") or "").lower() == a.activity.lower()]
    if not match:
        sys.exit(f"no activity {a.activity!r} in case {case} (excluded or unknown)")
    if match[0].get("enabled") != "true":
        sys.exit(f"{match[0]['label']!r} is not enabled in case {case}: the graph refuses it now")
    try:
        c.execute(case, match[0]["id"])
    except DcrError as e:
        sys.exit(str(e))
    print(f"executed {match[0]['label']!r} in case {case} ({a.case_key}); "
          "the bridge reports it within its poll interval (5 s)")


if __name__ == "__main__":
    main()
