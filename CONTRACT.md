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

Events the bridge produces on its own: `ProcessStarted` (Camunda), `CaseCreated` (DCR), `<action>Failed`. A DCR event that the graph doesn't allow comes back with `payload.rejected = true` and an `error`: a compliance signal, not a crash.

### 3.4 Time: two clocks, on purpose

- **Environment patterns** (`FireAlert`, `SceneCleared`) run on **event time** (`event_time`, watermarks). That's what makes a replay of an August 2025 fire, played 2,000× faster, produce the same alerts as it would have live.
- **Process patterns** (`BurnSupplyEmergency`, `CapacityRedirect`) react to what *people* do in Tasklist and the DCR portal, in real time. Their windows and deadlines are **wall-clock** (`timestamp` of the PAIS event), like the DCR graph's own `PT24H` deadline. For the demo the deadlines are shortened by parameter (e.g. 24 h → 2 min).
- PAIS events still carry `event_time` = the event time of the alert that last touched the case (process variable `cepEventTime`), so Flink can join them with the incident's environment state.

## 4. CEP patterns

Parameters in *italics* are Flink job arguments; the values are the defaults (`flink-job/README.md`). DCR event ids are job arguments too, since the graph doesn't exist yet.

| Alert type | Pattern | Inputs | Triggers |
|---|---|---|---|
| **`FireAlert`** | ≥ *3* FIRMS detections (confidence not `l`) in one cell within *6 h*, **while** that cell's current FWI `danger_class` ≥ *2* (Moderate). Once per incident; creates the incident key. | firms + fwi | BPMN `ambulance_fire_response`: message start **Mass fire alert received** |
| **`SceneCleared`** | Active incident, and **no** detection in its cell or the 8 neighbouring cells for *12 h* (absence pattern, event-time timer; replays end with a heartbeat so it can fire). Ends the incident. | firms (absence) | BPMN: message **Await scene cleared** |
| **`BurnSupplyEmergency`** | PAIS `Activate MCI protocol` for an incident **and** (that cell's `danger_class` ≥ *4* **or** ≥ *3* × `Send EMS pre-alert` for the incident within *60 min* wall clock). Once per incident. | pais.events + fwi | DCR purchasing: new case + **Declare Emergency Procurement** |
| **`CapacityRedirect`** | DCR `Emergency order` for an incident **not followed by** `Receive emergency delivery` within *2 min* wall clock (graph deadline PT24H, shortened for the demo), **while** the incident is active. One alert **per unit en route** (`Send EMS pre-alert` seen, `Report unit available` not yet). | pais.events (both engines) | BPMN: message **Destination changed** for that unit |

`FireAlert` and `SceneCleared` are the environment → process direction; `BurnSupplyEmergency` (BPMN → CEP → DCR) and `CapacityRedirect` (DCR → CEP → BPMN) are the process → process direction through the CEP.

## 5. What the modelling team delivers

### 5.1 BPMN `ambulance_fire_response` (Camunda 8.8)

Put the `.bpmn` (and any `.dmn`) in `bpmn/`: the bridge deploys everything there at start. Process **ID** (not the name) `ambulance_fire_response`, `isExecutable="true"`.

**Envelopes = the events the process exchanges with the CEP.**

| Element | BPMN type | Modellers set | Bridge does |
|---|---|---|---|
| Mass fire alert received | **message start event** | message name `FireAlert`, correlation key `=caseKey` | correlates `FireAlert` alerts; one running instance per incident (Zeebe enforces this) |
| Await scene cleared | message catch / receive task | message `SceneCleared`, key `=caseKey` | correlates `SceneCleared` alerts |
| Destination changed | message catch (boundary or intermediate) **inside the per-unit subprocess** | message `DestinationChanged`, key **`=caseKey + "|" + unitId`** | publishes one message per unit from `CapacityRedirect` |
| Activate MCI protocol, Request units from neighbouring region, Notify dispatcher of delay, Send EMS pre-alert, Report unit available | **send task** (or message throw event) | job **type `cep-event`**, task header **`activity`** = the element name (Modeler: *Headers* panel, key `activity`) | completes the job and reports the activity on `pais.events` and in the XES log, with `caseKey` and the variables listed in `mapping.json` (`unitId`, …) |

Why `|unitId` on "Destination changed": Zeebe correlates a message to **only one** subscription per process, *even within one instance* ([Camunda docs, Messages](https://docs.camunda.io/docs/components/concepts/messages/)). With one subscription per ambulance in a multi-instance subprocess, a plain `=caseKey` would redirect one random ambulance. So: the multi-instance subprocess needs an input element variable **`unitId`** (e.g. `inputCollection = =units`, `inputElement = unitId`), which "Send EMS pre-alert" / "Report unit available" then report automatically (`unitId` is in the `cep-event` variables list), so the CEP knows which units are en route. Need another variable reported (e.g. `patients`)? Tell the backend team; it's one entry in `mapping.json`.

**Other tasks block the process until something completes them:**

- **Business rule tasks** (Determine units needed, Select destination hospital): a DMN decision (FEEL decision table) deployed alongside: `.dmn` file in `bpmn/`, task property *Called decision* = the decision ID, *Result variable* e.g. `unitsNeeded`. Input variables available: `severity`, `alert` (whole alert, so `alert.details.danger_class`, `alert.details.frp_max`, …), plus whatever earlier tasks set.
- **User tasks**: completed by hand in Tasklist (`http://<host>:8088/tasklist`). Keep the forms trivial; every click is screencast time.
- **Service tasks** (Check unit availability): need a job worker. Simplest: make it a DMN/script task, or tell the backend team the type and what it should return.

Variables in every instance started or touched by the CEP: `caseKey`, `alertType`, `alertId`, `severity`, `cepEventTime`, `alert`.

### 5.2 DCR graph: emergency equipment purchasing (dcrgraphs.net)

| Item | Value |
|---|---|
| Graph ID | *from the DCR modeller* |
| Event **IDs** (not labels) | the IDs behind **Declare Emergency Procurement**, **Emergency order**, **Receive emergency delivery** (these three are what the CEP touches/watches) |
| Case per incident | `BurnSupplyEmergency` creates one case per incident key and executes *Declare Emergency Procurement* |
| What comes back | the bridge reports events it executes itself, **and polls open cases** for events people execute in the DCR portal (needed for `CapacityRedirect`) |
| Account | API credentials in `bridge.env` on the server / demo laptop, **never in git** |

## 6. `bridge/mapping.json`

The only file that connects alert types to models. No Python changes needed for new models. Target state once the models exist (DCR event IDs to be filled in):

```json
{
  "on_alert": {
    "FireAlert": [
      { "engine": "zeebe", "action": "correlate_message", "message": "FireAlert",
        "process_id": "ambulance_fire_response", "log_activity": "Mass fire alert received" }
    ],
    "SceneCleared": [
      { "engine": "zeebe", "action": "publish_message", "message": "SceneCleared",
        "process_id": "ambulance_fire_response", "log_activity": "Await scene cleared" }
    ],
    "BurnSupplyEmergency": [
      { "engine": "dcr", "action": "start_case", "event_id": "<DeclareEmergencyProcurement id>",
        "log_activity": "Declare Emergency Procurement" }
    ],
    "CapacityRedirect": [
      { "engine": "zeebe", "action": "publish_message", "message": "DestinationChanged",
        "correlation_key": "{case_key}|{details.unit_id}", "ttl_s": 0,
        "process_id": "ambulance_fire_response", "log_activity": "Destination changed" }
    ]
  },
  "zeebe_tasks": {
    "cep-event": { "activity_from_header": "activity", "variables": ["unitId"], "emit": true }
  },
  "dcr_watch": { "poll_s": 5, "events": ["<EmergencyOrder id>", "<ReceiveEmergencyDelivery id>"] }
}
```

| Field | Meaning |
|---|---|
| `start_process` | start a new BPMN instance (none start event) |
| `correlate_message` | message **start** or catch event via Camunda REST `/v2/messages/correlation`: returns the instance key, so the trace is complete from the first event |
| `publish_message` | send a message (gRPC publish); `correlation_key` template, default `{case_key}`; `ttl_s` how long Zeebe buffers it (0 = drop if nobody waits) |
| `start_case` / `execute` | DCR: new sim (+ optional `event_id`) / execute `event_id` in this incident's case |
| `log_activity` | name of the CEP trigger in the XES trace; `""` = don't log (not an activity of that model) |
| `zeebe_tasks.<type>` | worker per job type; `activity` fixed, or `activity_from_header` read from the task header (missing header: the job still completes, the event carries an `error`) |
| `variables` | process variables fetched by that worker and reported in the PAIS event payload, on top of `caseKey`, `alertType`, `alertId`, `severity`, `cepEventTime` |
| `emit` | `false` = only log, don't send to Flink |
| `set` | variables set on the process when the job completes |
| `dcr_watch` | poll open DCR cases every `poll_s` and report events newly executed by people in the portal (`role: dcr-user`, `payload.source: dcr-portal`); `events` = only these ids, `[]` = all. Sees an executed **flag**: a second execution of the same event is invisible |

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
| Bridge: `dcr_watch` polling | ✅ built + tested against a fake DCR API; needs `executed` in the real API's events XML: check with `tools/dcr_probe.py` once credentials exist |
| Flink job for the four patterns (`flink-job/`) | ✅ logic built + tested (`test-core.sh`); Flink wiring compiles against stand-ins, first real build on the NAS pending |
| BPMN with the conventions above, DMN tables, DCR graph + IDs + API account | modelling team |
