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
| `flink-job/` | **the CEP job**: the four patterns (logic in plain Java + Flink wiring); see its README | NAS / laptop Flink |
| `bridge/` | alerts → Zeebe/DCR; Zeebe jobs + DCR results → `pais.events`; everything → logging | laptop (next to Camunda) |
| `bridge/mapping.json` | **which alert does what in which model**: filled in by the modelling team | |
| `logging_service/` | XES log per process (based on the course example) | laptop |
| `bpmn/` | BPMN files; every `*.bpmn` here is deployed by the bridge at start | |
| `tools/send_alert.py` | send one alert by hand: test the process side without a Flink job | laptop |
| `tools/send_pais.py` | send one PAIS event by hand (e.g. DCR "Emergency order"): test the process patterns before the real source exists | NAS / laptop |
| `tools/dcr_probe.py` | shows which attributes the DCR API returns for a case's events (needed by the DCR watch) | anywhere with the DCR account |
| `tests/` | `test_offline.py` (end-to-end, fake Redpanda/DCR/Zeebe), `test_lifecycle.py`, `test_rest.py` (Camunda REST client), `test_fwi.py`, `test_models.py` (BPMN/DMN/mapping static checks), `test_camunda_log.py` (XES from Camunda) | anywhere with Python + Flask |
| `compose.demo.yaml` | **everything on one laptop** (dev + screencast) | laptop |
| `demo.sh` | one command per demo step: `up`, `replay`, `status`, `pais`, `clean` | laptop |
| `compose.yaml` | **server** stack: everything in `compose.demo.yaml` plus the live poller, hardened (internal network, digests, 568) | NAS (Arcane), see `runbook-cep-lab.md` |

## Laptop quickstart (the demo)

Needs Docker and about 8 GB free RAM (16 GB laptop recommended). Linux, macOS, WSL2 or Git Bash.

```bash
./demo.sh up        # builds the Flink jar + images, starts everything, waits for Camunda, submits the CEP job
./demo.sh replay    # the Ourense fire (Aug 2025) in ~120 s -> 5 FireAlert + 5 SceneCleared
./demo.sh status    # Flink jobs, CEP alerts so far, XES files
./demo.sh clean     # clean slate before the next take
```

Then work the process in **Tasklist** http://localhost:8088/tasklist (demo/demo): pick an incident with `alert.details.danger_class` ≥ 4 for the MCI path (→ `BurnSupplyEmergency`). To show `CapacityRedirect` without the DCR graph: bring an ambulance to *Transport patient*, then

```bash
./demo.sh pais "Emergency order" <caseKey>     # fake DCR event; 2 min later the ambulance is redirected
```

UIs: Flink http://localhost:8081 · Operate http://localhost:8088/operate · Tasklist http://localhost:8088/tasklist (demo/demo) · logging http://localhost:5010/events. XES files in `./XES`. Everything binds to `127.0.0.1`.

Plumbing test without Flink: `python3 tools/send_alert.py SmokeTest c3_4`, then `… SmokeTestUpdate c3_4` (smoke-test process in Operate). DCR: set `DCR_ENABLED=true` and the credentials in `bridge/.env` (from `bridge/.env.template`), event id in `bridge/mapping.json`.

## Tests

```bash
python3 tests/test_offline.py      # needs Python 3.10+ and Flask
python3 tests/test_lifecycle.py
python3 tests/test_rest.py
python3 tests/test_models.py
python3 tests/test_camunda_log.py
python3 tests/test_fwi.py          # stdlib only
```

## Status (2026-11-26)

| Part | State |
|---|---|
| Event sources: OpenWeather, FIRMS, computed FWI | live on the NAS |
| Replay: Ourense fire Aug 2025 (`scenarios/`) | 5 FireAlert + 5 SceneCleared, as predicted |
| Flink CEP job: all four patterns | run end to end on the NAS (`CapacityRedirect` with a fake DCR event) |
| Camunda 8.8: ambulance BPMN + 3 DMN | deployed and worked through Tasklist |
| XES: BPMN from Camunda (complete, ordered, lanes) | verified; processdoctor.dtu.dk upload still to try |
| DCR Graphs (dcrgraphs.net) | waiting for the graph + API account |
| Laptop demo (`demo.sh`) | new, first run at the dry run |

## Secrets

API keys (`.env`) and DCR credentials (`bridge/.env`) never go into git. `.gitignore` covers them.
