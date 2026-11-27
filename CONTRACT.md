# Interface contract: CEP backend ↔ process models

This file is the agreement between the **backend team** (event sources, Redpanda, Flink CEP, bridge, logging) and the **modelling team** (BPMN in Camunda 8, DCR graphs). If something here changes, both sides change together.

## 1. Architecture

```
OpenWeather ─┐                ┌─ weather.raw ─┐
FIRMS ───────┼─► poller ──────┼─ firms.raw ───┤                         ┌─► Zeebe (Camunda 8): start / message
Open-Meteo ──┘  (fwi.py)      └─ fwi.raw ─────┤                         │
scenario ─────► replayer ──── *.replay ───────┴─► Flink CEP ─► alerts ─► bridge ─┤
                                                    ▲                            └─► DCR Graphs: case / execute
                                                    └──── pais.events ◄── bridge ◄── Zeebe jobs, DCR results + polling
                                                                            │
                                                                            └─► logging service ─► XES (one file per process)
```

The CEP sits **between** the two processes: the ambulance process (BPMN) and the equipment purchasing (DCR) never talk to each other directly. Every interaction between them goes through a CEP pattern (§4). Same structure as the course example (`POESS-example/BPMN`): the **bridge** plays the role of the example's `communication` (Camunda) and `microservice` (DCR) services; CEP and bridge talk through Redpanda topics instead of Siddhi HTTP calls.

## 2. Topics

| Topic | Producer | Consumer | Content |
|---|---|---|---|
| `weather.raw`, `firms.raw`, `fwi.raw` | poller (live APIs) | Flink | weather observations, fire detections, daily fire weather index |
| `weather.replay`, `firms.replay`, `fwi.replay` | replayer (scenario files) | Flink | same format, from a recorded real fire |
| `alerts` | **Flink** | **bridge** | CEP findings that trigger process behaviour |
| `pais.events` | **bridge** | **Flink** | what happened in the processes (both engines) |

## 3. Keys, events and time

### 3.0 Grid cell and incident key

- **Cell**: global 0.25° lat/lon grid (≈ 28 × 16 km in Denmark, ≈ 28 × 22 km in Iberia), id `c<row>_<col>`, row = ⌊(lat+90)/0.25⌋, col = ⌊(lon+180)/0.25⌋. FIRMS detections are points (VIIRS pixel centres, ~375 m); they get the cell they fall in. Weather and FWI are fetched for the cell centre. Same ids live and in replays.
- **Incident key = `case_key` = `caseKey`** in the processes: `inc-<cell>-<YYYYMMDDTHHMM>`, the cell and the UTC event time of the first detection of the incident, e.g. `inc-c512_688-20250812T1430`. Created by the `FireAlert` pattern; every later alert about that fire carries the same key. A new fire in the same cell after `SceneCleared` is a new incident.

### 3.1 Source events (poller / replayer → Flink)

All times are **epoch milliseconds, UTC**; `event_time` is when it happened (observation, satellite overpass, noon of the FWI day), not when we fetched it.

```json
// weather.*   one per cell per poll (live) / per hour (replay)
{"type":"weather","cell":"c512_688","lat":38.125,"lon":-7.875,"event_time":1786000000000,
 "temp_c":31.2,"humidity_pct":18,"wind_ms":7.5,"wind_deg":310,"gust_ms":11.0,"rain_1h_mm":0.0, ...}

// firms.*     one per satellite detection (all FIRMS CSV columns are kept)
{"type":"fire","cell":"c512_688","latitude":38.1,"longitude":-7.9,"event_time":1786000000000,
 "frp":42.5,"confidence":"n","bright_ti4":340.1,"daynight":"D","source":"VIIRS_NOAA20_NRT", ...}

// fwi.*       one per cell per day, valid from 12:00 local standard time until the next one
{"type":"fwi","cell":"c512_688","lat":38.125,"lon":-7.875,"event_time":1786010400000,"day":"2026-08-12",
 "fwi":27.4,"danger_class":3,"danger_rating":{"value":3,"description":"High"},
 "components":{"ffmc":91.2,"dmc":48.0,"dc":512.3,"isi":11.8,"bui":74.9,"dsr":9.1},
 "inputs":{"temp_c":31.2,"rh_pct":18,"wind_kmh":27,"rain_24h_mm":0.0,"rain_hours":24},
 "inputs_source":"live","source":"computed"}
```

Danger classes (EFFIS, as in the OpenWeather FWI API): 0 Very low < 5.2 ≤ 1 Low < 11.2 ≤ **2 Moderate** < 21.3 ≤ 3 High < 38.0 ≤ 4 Very high < 50.0 ≤ 5 Extreme. `fwi` events are computed by `fwi.py` (Canadian FWI System, Van Wagner & Pickett 1985; verified against the `cffdrs` reference implementation). If OpenWeather grants FWI API access, the poller maps its response onto this exact format (`source: "openweather"`) and nothing downstream changes. Replays may contain one `carried_over: true` event at the scenario start: yesterday's value, still in force.

### 3.2 Alert (Flink → bridge, topic `alerts`)

```json
{
  "alert_id": "uuid",                       // required, unique
  "type": "FireAlert",                      // required, one of §4, must exist in bridge/mapping.json
  "case_key": "inc-c512_688-20250812T1430", // required, the incident key (§3.0)
  "event_time": 1786000000000,              // required, event time of the CEP finding
  "severity": "low | medium | high",
  "cell": "c512_688",
  "detected_time": 1786000004000,           // wall clock when Flink emitted it
  "details": { "detections": 5, "frp_max": 88.1, "danger_class": 3, "fwi": 27.4 }
}
```

### 3.3 PAIS event (bridge → Flink on `pais.events`, and → logging service)

Same fields as the course example's callback, plus `case_key`, `engine`, `process`, `event_time`:

```json
{
  "event": "Acknowledge incident",    // activity name (= name in the model)
  "case_key": "inc-c512_688-20250812T1430",
  "case_id": "2251799813685249",      // Zeebe process instance key / DCR sim id
  "engine": "camunda | dcr | cep",
  "process": "cep_smoke_test",        // BPMN process id, or "dcr:<graph id>"
  "role": "camunda",                  // XES org:resource
  "timestamp": "2026-11-23T15:17:25.123+00:00",   // wall clock (XES)
  "event_time": 1786000000000,        // event time of the CEP context (see 3.4)
  "payload": { "enabledEvents": ["Escalate"] },
  "error": ""                         // non-empty = failed or rejected (e.g. DCR event not enabled)
}
```

Events the bridge produces on its own: `ProcessStarted` (Camunda), `CaseCreated` (DCR), `<action>Failed`. A DCR event that the graph doesn't allow comes back with `payload.rejected = true` and an `error`: a compliance signal, not a crash. **DCR events are named by their label** (as the graph shows it, e.g. "Emergency order"); the id is in `payload.event_id`.

**Alerts are processed once:** the bridge remembers `alert_id`s (persisted in `bridge-state/alerts_seen.json`), so the same finding from two Flink jobs or a restarted job only acts once.

### 3.3b The XES log

| Process | Source of the XES entries |
|---|---|
| BPMN (Camunda) | **Camunda itself** (`camunda_log` in `mapping.json`): every completed task and named catch/start/boundary event, with Camunda's **end time** as `time:timestamp` and the element's **lane** as `org:resource`. Names come from the model file. User tasks, DMN tasks and buffered messages are only visible this way, and the order is the real one. The bridge writes no Zeebe entries of its own while this is on. |
| DCR | the bridge: events it executes + events people execute in the portal (DCR watch), named by label. CEP triggers that aren't graph activities are kept out with `log_activity: ""`. |

Both are written by the logging service to `XES/<run>_<process>.xes`. The Camunda log reads exported data, so entries appear a few seconds after the fact. Instances that existed before the Camunda log was first switched on are not logged retroactively (clean slate before a demo run anyway).

### 3.4 Time: two clocks, on purpose

- **Environment patterns** (`FireAlert`, `SceneCleared`) run on **event time** (`event_time`, watermarks). That's what makes a replay of an August 2025 fire, played 2,000× faster, produce the same alerts as it would have live.
- **Process patterns** (`BurnSupplyEmergency`, `CapacityRedirect`) react to what *people* do in Tasklist and the DCR portal, in real time. Their windows and deadlines are **wall-clock** (`timestamp` of the PAIS event), like the DCR graph's own `PT24H` deadline. For the demo the deadlines are shortened by parameter (e.g. 24 h → 2 min).
- **The clocks don't mix, and that shapes the patterns.** A replay runs the fire (event time) through in about two wall-clock minutes; people need far longer to work the process. So process patterns don't depend on environment state that a replay races through: `CapacityRedirect` is *not* cancelled by `SceneCleared` (patients on the road still need a burn-capable destination after the fire is out); its relevance is "units en route", tracked by the pre-alert / unit-available events.
- PAIS events still carry `event_time` = the event time of the alert that last touched the case (process variable `cepEventTime`), so Flink can join them with the incident's environment state.

## 4. CEP patterns

Parameters in *italics* are Flink job arguments; the values are the defaults (`flink-job/README.md`). The DCR event names are job arguments too: **labels** ("Emergency order", "Receive emergency delivery"), not ids. Only one running job may have `--process-patterns` on (the default); a second one (e.g. live next to a replay) runs with `--process-patterns false`.

| Alert type | Pattern | Inputs | Triggers |
|---|---|---|---|
| **`FireAlert`** | ≥ *3* FIRMS detections (confidence not `l`) in one cell within *6 h*, **while** that cell's current FWI `danger_class` ≥ *2* (Moderate). Once per incident; creates the incident key. | firms + fwi | BPMN `Ambulance_process`: message start **Major fire alert received** |
| **`SceneCleared`** | Active incident, and **no** detection in its cell or the 8 neighbouring cells for *12 h* (absence pattern, event-time timer; replays end with a heartbeat so it can fire). Ends the incident. | firms (absence) | BPMN: message **Await scene cleared** |
| **`BurnSupplyEmergency`** | PAIS `Activate MCI protocol` for an incident **and** (that cell's `danger_class` ≥ *4* **or** ≥ *3* × `Send EMS pre-alert` for the incident within *60 min* wall clock). Once per incident. | pais.events + fwi | DCR purchasing: new case + **Declare Emergency Procurement** |
| **`CapacityRedirect`** | DCR `Emergency order` for an incident **not followed by** `Receive emergency delivery` within *2 min* wall clock (graph deadline PT24H, shortened for the demo). One alert **per unit en route** (`Send EMS pre-alert` seen, `Report unit available` not yet). | pais.events (both engines) | BPMN: message **Destination changed** for that unit |

`FireAlert` and `SceneCleared` are the environment → process direction; `BurnSupplyEmergency` (BPMN → CEP → DCR) and `CapacityRedirect` (DCR → CEP → BPMN) are the process → process direction through the CEP.

## 5. What the modelling team delivers

### 5.1 BPMN `bpmn/ambulance_fire_response.bpmn` (process `Ambulance_process`, Camunda 8.8)

The modellers' model with Camunda execution settings added (layout and names unchanged). Everything in `bpmn/` (`.bpmn` + `.dmn`) is deployed by the bridge at start, in one atomic deployment. `tests/test_models.py` checks offline what Camunda would reject: run it after every model change.

**Envelopes = the events the process exchanges with the CEP.**

| Element | BPMN type | Setting | Bridge does |
|---|---|---|---|
| Major fire alert received | message start event | message `FireAlert` | `FireAlert` alert → REST correlation; one running instance per incident (Zeebe enforces this) |
| Await scene cleared | receive task | message `SceneCleared`, key `=caseKey` | `SceneCleared` alert → publish, **buffered 24 h** (`ttl_s` 86400): the task only opens after all missions end, the alert usually comes earlier |
| Destination changed | interrupting message boundary on *Transport patient*, inside the per-unit subprocess | message `DestinationChanged`, key **`=caseKey + "\|" + unitId`** | `CapacityRedirect` alert → one message per unit, `ttl_s` 0 (only a unit that is transporting right now can be redirected) |
| Activate MCI protocol, Request units from neighbouring region, Notify dispatcher of delay, Send EMS pre-alert, Report unit available | send task | job type `cep-event`, task header `activity` = element name | completes the job, reports the activity on `pais.events` + XES, with `caseKey` and `unitId` |

Why `|unitId` on "Destination changed": Zeebe correlates a message to **only one** subscription per process, *even within one instance* ([Camunda docs, Messages](https://docs.camunda.io/docs/components/concepts/messages/)). With one subscription per ambulance, a plain `=caseKey` would redirect one random ambulance.

**Decisions and data (rule-based; group decision 2026-11-25):**

| Element | Setting | Result |
|---|---|---|
| Mass casualty incident? | `=alert.details.danger_class >= 4 or alert.details.detections >= 10`, else "No" (default) | MCI path (→ `BurnSupplyEmergency` input) |
| Determine units needed | DMN `determine_units` (`bpmn/determine-units.dmn`) → `unitsNeeded` | 2 if danger class ≥ 4, else 1 (kept small: each unit = 7 user tasks in the demo) |
| Check unit availability | job type `check-unit-availability`, **simulated** by the bridge (`set: {unitsAvailable: 1}`, logged, not sent to Flink) | so 2 units needed → "Request units from neighbouring region" |
| Enough units? | "No" = `=unitsAvailable < unitsNeeded`, "Yes" default | |
| Ambulance mission (one per unit) | `inputCollection = =for i in 1..unitsNeeded return "amb-" + string(i)`, `inputElement = unitId` | `amb-1`, `amb-2`, … per incident |
| Select destination hospital | DMN `select_destination` (`bpmn/select-destination.dmn`) → `destination` | "Regional burn centre" if danger class ≥ 3, else "Nearest emergency department" (fictional) |
| More patients at scene? | "Yes" = `=is defined(morePatients) and morePatients = true`, "No" default | set `morePatients = true` when completing *Clean, decontaminate and restock* to loop |
| All user tasks | Camunda user tasks (`zeebe:userTask`): job-worker user tasks are invisible in Tasklist 8.8 | completed by hand in Tasklist (`:8088/tasklist`) |

Variables in every instance started or touched by the CEP: `caseKey`, `alertType`, `alertId`, `severity`, `cepEventTime`, `alert` (the whole alert: `alert.details.danger_class`, `.detections`, `.frp_max`, …).

**Re-running a scenario reuses the incident keys** (they're deterministic): wipe Camunda first (runbook clean slate), or the old instances and buffered `SceneCleared` messages interfere.

### 5.2 DCR graph: emergency equipment purchasing (dcrgraphs.net)

| Item | Value |
|---|---|
| Graph ID | **2017014** ("Purchasing of specialized equipment and instruments"; `DCR_GRAPH_ID` in `bridge/.env`) |
| Event **id** for the bridge, **labels** for everything else | the bridge *executes* by id (`mapping.json`: **`A1`** = *Declare emergency procurement*; `tools/dcr_probe.py` prints the ids); pais.events, XES and the Flink job use the **labels** **Emergency order** and **Receive emergency delivery** (so recreating the graph doesn't break the CEP) |
| Case per incident | `BurnSupplyEmergency` creates one case per incident key and executes *Declare Emergency Procurement* |
| What comes back | the bridge reports events it executes itself, **and polls open cases** for events people execute in the DCR portal (needed for `CapacityRedirect`) |
| Account | API credentials in `bridge.env` on the server / demo laptop, **never in git** |

## 6. `bridge/mapping.json`

The only file that connects alert types to models. No Python changes needed for new models. Current entries:

```json
{
  "on_alert": {
    "FireAlert": [
      { "engine": "zeebe", "action": "correlate_message", "message": "FireAlert",
        "process_id": "Ambulance_process", "log_activity": "Major fire alert received" }
    ],
    "SceneCleared": [
      { "engine": "zeebe", "action": "publish_message", "message": "SceneCleared", "ttl_s": 86400,
        "process_id": "Ambulance_process", "log_activity": "Await scene cleared" }
    ],
    "CapacityRedirect": [
      { "engine": "zeebe", "action": "publish_message", "message": "DestinationChanged",
        "correlation_key": "{case_key}|{details.unit_id}", "ttl_s": 0,
        "process_id": "Ambulance_process", "log_activity": "Destination changed" }
    ],
    "BurnSupplyEmergency": [
      { "engine": "dcr", "action": "start_case", "event_id": "A1",
        "log_activity": "" }
    ]
  },
  "zeebe_tasks": {
    "cep-event": { "activity_from_header": "activity", "variables": ["unitId"], "emit": true },
    "check-unit-availability": { "activity": "Check unit availability", "emit": false,
                                 "set": { "unitsAvailable": 1 } }
  },
  "dcr_watch": { "poll_s": 5, "events": [] },
  "camunda_log": { "poll_s": 5 }
}
```

The `SmokeTest`/`SmokeTestUpdate` entries (smoke-test process) stay for plumbing tests.

| Field | Meaning |
|---|---|
| `start_process` | start a new BPMN instance (none start event) |
| `correlate_message` | message **start** or catch event via Camunda REST `/v2/messages/correlation`: returns the instance key, so the trace is complete from the first event |
| `publish_message` | send a message (REST `/v2/messages/publication`); `correlation_key` template, default `{case_key}`; `ttl_s` how long Zeebe buffers it (0 = drop if nobody waits) |
| `start_case` / `execute` | DCR: new sim (+ optional `event_id`) / execute `event_id` in this incident's case |
| `log_activity` | name of the CEP trigger in the XES trace; `""` = don't log (not an activity of that model) |
| `zeebe_tasks.<type>` | worker per job type; `activity` fixed, or `activity_from_header` read from the task header (missing header: the job still completes, the event carries an `error`) |
| `variables` | process variables fetched by that worker and reported in the PAIS event payload, on top of `caseKey`, `alertType`, `alertId`, `severity`, `cepEventTime` |
| `emit` | `false` = only log, don't send to Flink |
| `set` | variables set on the process when the job completes |
| `camunda_log` | write the XES log for BPMN processes from Camunda's completed element instances every `poll_s` (§3.3b); optional `tags` = BPMN element types to log (default: all task types + start/boundary/intermediate events) |
| `dcr_watch` | poll open DCR cases every `poll_s` and report events newly executed by people in the portal (`role: dcr-user`, `payload.source: dcr-portal`); `events` = only these ids or labels, `[]` = all. Sees an executed **flag**: a second execution of the same event is invisible. Nests/subprocesses (events another event names as `parent`) are skipped: the API marks them executed with their first child |

## 7. Conventions

- **Activity names in logs = names in the models.** Conformance checking compares them literally, so `activity` headers and `log_activity` use the exact element names.
- BPMN correlation keys: `=caseKey`, except per-unit messages (`=caseKey + "|" + unitId`).
- Message names in PascalCase, same as the alert type that sends them where possible.
- The smoke-test process (`bpmn/cep-smoke-test.bpmn` + `.dmn`, alert types `SmokeTest`/`SmokeTestUpdate`) only tests the plumbing; it is not part of the project models. It is also the **template**: message start, a `cep-event` task with an `activity` header, a business rule task calling a DMN decision, a message catch with `=caseKey`.

## 8. Status (what exists vs what's planned)

| Piece | Status |
|---|---|
| Poller (OpenWeather, FIRMS), global 0.25° grid | ✅ built |
| FWI computation (`fwi.py`), live + in scenarios, verified against `cffdrs` | ✅ built, `tests/test_fwi.py` |
| Backfill / replay with `fwi.replay` | ✅ built |
| Bridge: `start_process`, `publish_message`, DCR `start_case`/`execute`, fixed-activity workers, XES | ✅ built, running on the server |
| Bridge: `cep-event` header workers, `correlate_message` (REST), `correlation_key` template + `ttl_s`, `.bpmn`+`.dmn`+`.form` deployment (REST, atomic) | ✅ built, offline tests; smoke test v2 exercises all of it on the server |
| Bridge: `dcr_watch` polling | ✅ built + tested against a fake DCR API and the real graph 2017014 (`executed` is in the events XML; nests skipped) |
| Flink job for the four patterns (`flink-job/`) | ✅ logic built + tested (`test-core.sh`); Flink wiring compiles against stand-ins, first real build on the NAS pending |
| DCR graph 2017014 + event id `A1` + API account; `BurnSupplyEmergency` → case + *Declare emergency procurement* run live through the bridge | ✅ (2026-11-27) |
| BPMN with the conventions above, DMN tables | modelling team |
