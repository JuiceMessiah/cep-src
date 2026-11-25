# cep-src: CEP backend for DTU 02268 (process-oriented, event-driven systems)

Group project, hand-in **2026-12-15 23:59** (report + ≤3 min screencast + source.zip + slides).
This repo is the backend: event sources → Redpanda → Flink CEP → bridge → Camunda 8.8 (BPMN) / DCR Graphs, with XES logs for conformance checking.

**Read `CONTRACT.md` before changing behaviour.** It is the agreement with the modelling team (topics, event formats, incident keys, the four CEP patterns, what the models must contain). If behaviour changes, CONTRACT.md changes in the same commit.

## Layout

| Path | What | Language |
|---|---|---|
| `poller.py`, `fwi.py` | OpenWeather + NASA FIRMS → `weather.raw` / `firms.raw`; computed Fire Weather Index → `fwi.raw` | Python, stdlib only |
| `backfill.py`, `replay.py`, `scenarios/` | build a replay scenario from a real past fire; replay it time-compressed to `*.replay` | Python, stdlib only |
| `flink-job/` | the CEP job. `cep/core` = pattern logic, plain Java, no Flink. `cep/job` = Flink wiring | Java 17, Flink 2.2.1 |
| `bridge/` | alerts → Zeebe/DCR; Zeebe jobs + DCR events → `pais.events`; mapping in `bridge/mapping.json` | Python 3.12 + pyzeebe 3.0.4 |
| `logging_service/` | XES log per process | Python + Flask |
| `bpmn/` | models deployed by the bridge (`cep-smoke-test.*` = plumbing test + template) | BPMN / DMN |
| `compose.demo.yaml` | everything on one laptop, bound to 127.0.0.1 | |
| `compose.yaml` | the server stack (deployed by hand on the owner's NAS, not from here) | |

## Tests: run before every commit

```bash
python3 tests/test_models.py && python3 tests/test_camunda_log.py && python3 tests/test_fwi.py && python3 tests/test_rest.py && python3 tests/test_offline.py && python3 tests/test_lifecycle.py
sh flink-job/test-core.sh
```

`test_offline.py` needs Flask. Everything else is stdlib / plain JDK 17+.
Build the Flink jar: see `flink-job/README.md` (Maven in a container works; no local Maven needed).

## Rules

- **No secrets in this repo, ever** (public). Keys live in `.env` / `bridge.env` / `camunda.env`, which are gitignored; only `*.env.template` is committed. Never print key values; check them by length/shape only. A leaked key gets revoked, not just deleted.
- **XML comments must not contain `--`** (pom.xml, BPMN, DMN): Maven refuses to parse them.
- **After any change in `bpmn/` or `bridge/mapping.json`, run `tests/test_models.py`**: it catches what Camunda would reject at deployment and mapping entries that point at messages/names not in the model.
- Python services are stdlib-only where they already are (poller, fwi, backfill, replay): keep it that way.
- Grid cells are global 0.25° ids `c{row}_{col}` (origin 90S/180W); incident key `inc-<cell>-<yyyyMMdd'T'HHmm>`. Must stay identical in poller, backfill and the Flink job.
- Environment patterns run on **event time** (replays must behave like live data); process patterns on **wall clock**. Don't mix.
- Activity names in logs = element names in the models, literally (conformance checking).
- Work on a branch, open a PR; `main` is what the server pulls. Don't touch the server from here: deployment is manual and done by the owner.
