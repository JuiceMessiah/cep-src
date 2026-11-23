# Interface contract: CEP backend ↔ process models

This file is the agreement between the **backend team** (event sources, Redpanda, Flink CEP, bridge, logging) and the **modelling team** (BPMN in Camunda 8, DCR graphs). If something here changes, both sides change together.

## 1. Architecture

```
OpenWeather ─┐                                            ┌─► Zeebe (Camunda 8): start process / publish message
FIRMS ───────┼─► poller ─┐                                │
scenario ────┘  replayer ┴─► Redpanda ─► Flink CEP ─► alerts ─► bridge ─┤
                                              ▲                         └─► DCR Graphs: create case / execute event
                                              └──── pais.events ◄── bridge ◄── Zeebe service tasks, DCR results
                                                                       │
                                                                       └─► logging service ─► XES (one file per process)
```

Same structure as the course example (`POESS-example/BPMN`): our **bridge** plays the role of the example's `communication` service (Camunda) and `microservice` (DCR). The difference: CEP and bridge talk through Redpanda topics instead of Siddhi HTTP calls.

## 2. Topics

| Topic | Producer | Consumer | Content |
|---|---|---|---|
| `weather.raw`, `firms.raw` | poller (live APIs) | Flink | weather observations, fire detections |
| `weather.replay`, `firms.replay` | replayer (scenario files) | Flink | same format, from a recorded real fire |
| `alerts` | **Flink** | **bridge** | CEP findings that should trigger process behaviour |
| `pais.events` | **bridge** | **Flink** | what happened in the processes (both engines) |

## 3. Event formats

All times are **epoch milliseconds, UTC**. The join key everywhere is `case_key`: today a grid cell id such as `c3_4` (one incident per cell). If the group wants a different business key (incident id, region, …), it changes here first.

### 3.1 Alert (Flink → bridge, topic `alerts`)

```json
{
  "alert_id": "uuid",                 // required, unique
  "type": "FireSpreadRisk",           // required, must exist in bridge/mapping.json
  "case_key": "c3_4",                 // required, becomes caseKey in the process
  "event_time": 1786000000000,        // required, event time of the CEP finding
  "severity": "low | medium | high",
  "cell": "c3_4",
  "detected_time": 1786000004000,     // wall clock when Flink emitted it
  "details": { "frp": 42.5, "wind_ms": 11.2, "humidity_pct": 18 }
}
```

### 3.2 PAIS event (bridge → Flink on `pais.events`, and → logging service)

Same fields as the course example's callback, plus `case_key`, `engine`, `process`, `event_time`:

```json
{
  "event": "Acknowledge incident",    // activity name (= name in the model)
  "case_key": "c3_4",
  "case_id": "2251799813685249",      // Zeebe process instance key / DCR sim id
  "engine": "camunda | dcr | cep",
  "process": "cep_smoke_test",        // BPMN process id, or "dcr:<graph id>"
  "role": "camunda",                  // XES org:resource
  "timestamp": "2026-11-23T15:17:25.123+00:00",   // wall clock (XES)
  "event_time": 1786000000000,        // event time of the CEP context (see 3.3)
  "payload": { "enabledEvents": ["Escalate"] },
  "error": ""                         // non-empty = failed or rejected (e.g. DCR event not enabled)
}
```

Events the bridge produces on its own: `ProcessStarted` (Camunda), `CaseCreated` (DCR), `<action>Failed`. A DCR event that the graph doesn't allow comes back with `payload.rejected = true` and an `error`: a compliance signal, not a crash.

### 3.3 Time

PAIS events carry the **event time of the alert that started or last updated the case** (process variable `cepEventTime`). That keeps a Flink job joining `pais.events` with fire/weather events consistent, even when a scenario is replayed 2000× faster than real time. Wall-clock time is in `timestamp` and is what the XES log uses.

## 4. What the modelling team delivers

### Per BPMN process (Camunda 8.8, Zeebe)

| Item | Example | Rule |
|---|---|---|
| `.bpmn` file | `bpmn/wildfire_response.bpmn` | Put it in `bpmn/`: the bridge deploys every file there at start |
| Process ID | `wildfire_response` | Process **ID**, not the name; `isExecutable="true"` |
| Which alert type **starts** it | `FireSpreadRisk` | Start event: a plain none start event is enough |
| Messages the CEP sends into it | `FireUpdate` | Message name + correlation key **`=caseKey`** (always) |
| Service tasks that report back to the CEP | type `notify-cep`, name `Acknowledge incident` | Task **type** (kebab-case) + the activity name for the log |
| Variables the tasks may need | `severity`, `alert.details.frp` | Available in every instance: `caseKey`, `alertType`, `alertId`, `severity`, `cepEventTime`, `alert` (whole alert) |

Every service task type needs a worker, or the process stops there. The bridge registers a worker for each type listed in `mapping.json → zeebe_tasks`. Tasks that do something else (e.g. a human/user task) don't need an entry.

### Per DCR graph (dcrgraphs.net)

| Item | Example |
|---|---|
| Graph ID | `2012481` |
| Event **IDs** (not labels) the CEP executes | `ReportFire`, `Escalate` |
| Which alert type creates a case / executes which event | `FireSpreadRisk` → new case + `ReportFire` |
| Account the bridge uses | credentials in `bridge/.env` on the demo laptop, **never in git** |

## 5. `bridge/mapping.json`

The only file that connects alert types to models. The modelling team fills it in; no Python changes needed.

```json
{
  "on_alert": {
    "FireSpreadRisk": [
      { "engine": "zeebe", "action": "start_process", "process_id": "wildfire_response",
        "log_activity": "Fire risk detected" },
      { "engine": "dcr", "action": "start_case", "event_id": "ReportFire", "log_activity": "" }
    ],
    "FireStillActive": [
      { "engine": "zeebe", "action": "publish_message", "message": "FireUpdate",
        "process_id": "wildfire_response", "log_activity": "Fire update received" },
      { "engine": "dcr", "action": "execute", "event_id": "Escalate", "create_if_missing": false }
    ]
  },
  "zeebe_tasks": {
    "notify-cep": { "activity": "Acknowledge incident", "emit": true },
    "close-incident": { "activity": "Close incident", "set": { "closedBy": "bridge" } }
  }
}
```

| Field | Meaning |
|---|---|
| `start_process` | start a new BPMN instance (Zeebe), variables as in §4 |
| `publish_message` | send a message, correlated by `caseKey` |
| `start_case` / `execute` | DCR: new sim (+ optional `event_id`) / execute `event_id` in the case for this `case_key` |
| `log_activity` | name of the CEP trigger in the XES trace; `""` = don't log it (when it isn't an activity of that model) |
| `zeebe_tasks.<type>.activity` | activity name written to `pais.events` and the XES log: **use the task name from the model** so conformance checking matches |
| `emit` | `false` = only log, don't send to Flink |
| `set` | variables set on the process when the job completes |

## 6. Conventions

- **Activity names in logs = names in the models.** Conformance checking compares them literally.
- Correlation key for every BPMN message: `=caseKey`.
- Task types in kebab-case, unique across all processes.
- One alert type may trigger several processes; one process may be triggered by several alert types.
- The smoke-test process (`bpmn/cep-smoke-test.bpmn`, alert types `SmokeTest`/`SmokeTestUpdate`) is for testing the plumbing only. It isn't one of the six project processes.
