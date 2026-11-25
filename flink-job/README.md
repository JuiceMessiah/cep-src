# cep-job: the Flink CEP job

The four patterns from `CONTRACT.md` §4 in one job, two branches:

| Branch | Input | Time | Keyed by | Emits on `alerts` |
|---|---|---|---|---|
| environment | `firms.*` + `fwi.*` | event time (watermarks) | grid cell | `FireAlert`, `SceneCleared` |
| process | `alerts` (own FireAlert/SceneCleared) + `pais.events` | wall clock | incident key | `BurnSupplyEmergency`, `CapacityRedirect` |

| Path | What |
|---|---|
| `src/main/java/cep/core/` | the pattern logic: plain Java, no Flink, tested by `test-core.sh` |
| `src/main/java/cep/job/` | Flink wiring: Kafka sources/sink, watermarks, keyed state + timers |

Design notes for the report:

- **Event-time ordering:** FIRMS NRT data arrives hours late and unordered. The environment branch buffers records per cell until the watermark passes them, then applies them in timestamp order. Records already behind the watermark are applied immediately (best effort, not dropped).
- **FWI is state, not an event stream:** `fwi.*` is always read from the beginning, so a fresh job knows every cell's danger class; FireAlert uses the class in force at the detection's event time.
- **Absence patterns are timers:** `SceneCleared` = event-time timer at last activity + 12 h (detections in the 8 neighbouring cells count as activity); `CapacityRedirect` = processing-time timer at order + timeout, cancelled by the delivery.
- **Idle topics:** `--idle-s` stops a quiet topic (FWI: once a day) from freezing the watermark. Replays end with a heartbeat 13 h after the last event so `SceneCleared` can fire.
- **Deterministic alert ids** (`type:case_key[:unit]`): a restart that re-emits a finding produces the same id.

## Test the logic (any JDK 17+, no network)

```bash
sh test-core.sh                 # OK: 42 checks
```

## Build (NAS, as apps, nothing installed on the host)

```bash
cd /mnt/fast/apps/cep-src/flink-job
docker run --rm -u 568:568 -e HOME=/tmp -e MAVEN_CONFIG=/tmp/.m2 -v "$PWD":/w -w /w maven:3.9-eclipse-temurin-17 \
  sh -c 'sh test-core.sh && mvn -B -q -Dmaven.repo.local=/w/.m2repo package'
ls -l target/cep-job-1.0.jar
```

First build downloads Maven plugins + dependencies into `.m2repo/` (gitignored, a few minutes); later builds are fast.

## Run

```bash
# stream it in (not docker cp): cp keeps the host owner/mode (568, 660) and Flink's own user can't read it
docker exec -i flink-jobmanager sh -c 'cat > /tmp/cep-job.jar' < target/cep-job-1.0.jar
docker exec flink-jobmanager /opt/flink/bin/flink run -d /tmp/cep-job.jar --source replay --group cep-job-r1
docker exec flink-jobmanager /opt/flink/bin/flink list
docker logs -f flink-taskmanager 2>&1 | grep 'CEP '          # every emitted alert is logged
```

The `WARNING: Unknown module: jdk.compiler` lines from the Flink CLI are harmless. Start the job **before** the replay (FIRMS is read from `latest`). Cancel with `flink cancel <job id>`. A new `--group` = a fresh start.

| Argument | Default | Meaning |
|---|---|---|
| `--source` | `live` | `live` = `*.raw`, `replay` = `*.replay` |
| `--group` | `cep-job` | consumer group prefix |
| `--firms-start` | `latest` | `earliest` re-reads the whole FIRMS topic |
| `--out-of-order-min` | 60 | watermark slack |
| `--idle-s` | 60 | idle-topic timeout |
| `--min-detections`, `--window-h`, `--min-danger`, `--clear-h` | 3, 6, 2, 12 | FireAlert / SceneCleared |
| `--burn-danger`, `--pre-alert-count`, `--pre-alert-window-min` | 4, 3, 60 | BurnSupplyEmergency |
| `--delivery-timeout-s` | 120 | CapacityRedirect (graph: 24 h, shortened for the demo) |
| `--dcr-emergency-order`, `--dcr-delivery` | `EmergencyOrder`, `ReceiveEmergencyDelivery` | the DCR event **ids** (fill in once the graph exists) |
