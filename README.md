# CEP backend: wildfire/weather events → Flink → Camunda 8 + DCR

Backend for the 02268 group project: external event sources (NASA FIRMS, OpenWeather), Redpanda, Apache Flink (CEP), and the bridge that connects the CEP to the process engines in **both directions**, plus XES logging for conformance checking.

- **[CONTRACT.md](CONTRACT.md)**: topics, event formats, and what the modelling team delivers. Read this first.
- Architecture modelled on the course example (`POESS-example`), with Flink instead of Siddhi.

## Layout

| Path | What | Runs on |
|---|---|---|
| `poller.py` | live OpenWeather + FIRMS → `weather.raw` / `firms.raw`, daily FWI → `fwi.raw` | server, laptop (`--profile live`) |
| `fwi.py` | Canadian Fire Weather Index (Van Wagner & Pickett 1985), used by poller and backfill | |
| `backfill.py` | builds a scenario file from a **real past fire** (FIRMS + historical weather + FWI) | laptop with internet + own API keys |
| `replay.py` | replays a scenario into `*.replay` topics, time-compressed, original event times | server, laptop |
| `bridge/` | alerts → Zeebe/DCR; Zeebe jobs + DCR results → `pais.events`; everything → logging | laptop (next to Camunda) |
| `bridge/mapping.json` | **which alert does what in which model**: filled in by the modelling team | |
| `logging_service/` | XES log per process (based on the course example) | laptop |
| `bpmn/` | BPMN files; every `*.bpmn` here is deployed by the bridge at start | |
| `tools/send_alert.py` | send one alert by hand: test the process side without a Flink job | laptop |
| `tests/` | `test_offline.py` (end-to-end, fake Redpanda/DCR/Zeebe), `test_lifecycle.py`, `test_fwi.py` | anywhere with Python + Flask |
| `compose.demo.yaml` | **everything on one laptop** (dev + screencast) | laptop |
| `compose.yaml` | **server** stack: everything in `compose.demo.yaml` plus the live poller, hardened (internal network, digests, 568) | NAS (Arcane), see `runbook-cep-lab.md` |

## Laptop quickstart

Needs Docker and about 8 GB free RAM (16 GB laptop recommended).

```bash
cp bridge/.env.template bridge/.env                 # only needed for DCR
docker compose -f compose.demo.yaml up -d --build
docker compose -f compose.demo.yaml logs -f bridge  # wait for "deployed cep-smoke-test.bpmn"
```

Test the CEP → Camunda → CEP path without any Flink job:

```bash
python3 tools/send_alert.py SmokeTest c3_4          # starts cep_smoke_test, worker reports "Acknowledge incident"
python3 tools/send_alert.py SmokeTestUpdate c3_4    # message FireUpdate → "Close incident" → done
```

Then check:

- **Operate** http://localhost:8088 (demo/demo): instance for `c3_4`, completed
- **pais.events**: `docker compose -f compose.demo.yaml exec redpanda rpk topic consume pais.events -n 5`
- **XES**: `./XES/<run>_cep_smoke_test.xes`, or http://localhost:5010/events

DCR: set `DCR_ENABLED=true` and the credentials in `bridge/.env`, and create a test graph with events `ReportFire` and `Escalate` (or change `mapping.json`).

Replay a recorded fire into the CEP:

```bash
docker compose -f compose.demo.yaml run --rm replayer /scenarios/<name>.jsonl --duration 90
```

UIs: Flink http://localhost:8081 · Operate http://localhost:8088/operate · Tasklist http://localhost:8088/tasklist (demo/demo) · logging http://localhost:5010/events. Everything binds to `127.0.0.1`.

## Tests

```bash
python3 tests/test_offline.py      # needs Python 3.10+ and Flask
python3 tests/test_lifecycle.py
python3 tests/test_fwi.py          # stdlib only
```

## Status (2026-11-23)

| Part | State |
|---|---|
| Server: Flink 2.2.1, Redpanda, poller | running on the NAS |
| Server: Camunda 8.8, bridge, logging | compose ready (`compose.yaml`), not yet deployed |
| `backfill.py`, `replay.py` | tested offline (fake APIs); real API endpoints not yet run |
| Bridge, logging service | tested offline end to end; **not yet run against real Zeebe 8.8 / dcrgraphs.net** |
| Flink CEP job(s) | not started: patterns depend on what the processes need |
| Real BPMN/DCR models | modelling team; plug in via `bpmn/` + `mapping.json` |

## Secrets

API keys (`.env`) and DCR credentials (`bridge/.env`) never go into git. `.gitignore` covers them.
