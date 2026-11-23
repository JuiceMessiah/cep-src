"""
cep-bridge: connects the CEP (Flink, via Redpanda topics) with the PAIS
(Camunda 8 / Zeebe and DCR Graphs) in both directions.

  alerts (topic)  --> bridge --> Zeebe: start process / publish message
                             --> DCR:   create case / execute event
  Zeebe jobs, DCR results --> bridge --> pais.events (topic)  --> Flink
  everything                         --> logging service (XES per process)

What happens for which alert type, and which BPMN service tasks report back,
is configured in mapping.json, not in code. Modellers fill that file in.

Environment (see .env.template):
  PANDAPROXY_URL   http://redpanda:8082
  ALERTS_TOPIC     alerts            PAIS_TOPIC   pais.events
  CONSUMER_GROUP   cep-bridge        MAPPING_FILE /config/mapping.json
  ZEEBE_ENABLED    true              ZEEBE_ADDRESS zeebe:26500
  BPMN_DIR         /bpmn             (every *.bpmn there is deployed at start)
  DCR_ENABLED      false             DCR_GRAPH_ID / DCR_USERNAME / DCR_PASSWORD
  LOGGING_URL      http://logging-service:5010/log   (empty = off)
"""
import asyncio
import glob
import json
import os
import signal
import sys
import urllib.error
import urllib.request

from core import Bridge, load_mapping
from pandaproxy import Consumer, ProxyError, produce, wait_for_proxy

PROXY = os.getenv("PANDAPROXY_URL", "http://redpanda:8082")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
PAIS_TOPIC = os.getenv("PAIS_TOPIC", "pais.events")
GROUP = os.getenv("CONSUMER_GROUP", "cep-bridge")
MAPPING_FILE = os.getenv("MAPPING_FILE", "/config/mapping.json")
ZEEBE_ENABLED = os.getenv("ZEEBE_ENABLED", "true").lower() == "true"
ZEEBE_ADDRESS = os.getenv("ZEEBE_ADDRESS", "zeebe:26500")
BPMN_DIR = os.getenv("BPMN_DIR", "/bpmn")
DCR_ENABLED = os.getenv("DCR_ENABLED", "false").lower() == "true"
LOGGING_URL = os.getenv("LOGGING_URL", "http://logging-service:5010/log")
STATE_FILE = os.getenv("STATE_FILE", "/state/cases.json")   # case_key -> instance ids; empty = in memory only

BASE_VARS = ["caseKey", "alertType", "alertId", "severity", "cepEventTime"]


def log(msg):
    print(f"[bridge] {msg}", flush=True)


def emit_pais(env):
    produce(PROXY, PAIS_TOPIC, [(env.get("case_key") or "none", env)])


def post_log(env):
    if not LOGGING_URL:
        return
    req = urllib.request.Request(LOGGING_URL, data=json.dumps(env).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except (urllib.error.URLError, OSError) as e:
        log(f"logging service unreachable: {e}")


class ZeebeAdapter:
    """Thin wrapper so core.py never imports pyzeebe."""

    def __init__(self, client):
        self.client = client

    async def start_process(self, process_id, variables):
        resp = await self.client.run_process(process_id, variables=variables)
        return getattr(resp, "process_instance_key", resp)   # pyzeebe versions differ

    async def publish_message(self, name, correlation_key, variables):
        await self.client.publish_message(name, str(correlation_key), variables)


async def setup_zeebe(bridge):
    import grpc                                   # only needed when Zeebe is enabled
    from pyzeebe import Job, ZeebeClient, ZeebeWorker

    channel = grpc.aio.insecure_channel(ZEEBE_ADDRESS)
    client = ZeebeClient(grpc_channel=channel)
    worker = ZeebeWorker(grpc_channel=channel)

    for task_type, cfg in bridge.task_types().items():
        def make(tt):
            async def handler(job: Job, **variables):
                merged = {**(job.variables or {}), **variables}
                return bridge.on_zeebe_job(tt, merged, job.process_instance_key, job.bpmn_process_id)
            return handler
        worker.task(task_type=task_type,
                    variables_to_fetch=BASE_VARS + list(cfg.get("variables", [])))(make(task_type))
        log(f"zeebe worker registered for task type {task_type!r}")

    files = sorted(glob.glob(os.path.join(BPMN_DIR, "*.bpmn")))
    if not files:
        log(f"no *.bpmn in {BPMN_DIR}: nothing deployed (processes must be deployed some other way)")
    for path in files:
        for attempt in range(1, 31):
            try:
                await client.deploy_process(path)
                log(f"deployed {os.path.basename(path)}")
                break
            except Exception as e:
                if attempt == 30:
                    raise
                log(f"deploy {os.path.basename(path)} attempt {attempt} failed: {e!r}")
                await asyncio.sleep(2)
    return ZeebeAdapter(client), worker


class OffsetStore:
    """Next offset to process per (topic, partition), persisted next to the case map.

    Needed because a NEW Redpanda HTTP-proxy consumer instance starts at auto.offset.reset
    (earliest) instead of the group's committed offset (observed 2026-11-23: committed 4, new
    instance re-read 0-3). The group commit stays (rpk group describe shows lag), but this file
    is what prevents re-processing alerts after a restart. Missing file = start at 0, which is
    right for a fresh stack; to adopt an existing group, seed it: {"alerts|0": <committed offset>}."""

    def __init__(self, path):
        self.path = path
        self.d = {}
        if path and os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as fh:
                    self.d = {k: int(v) for k, v in json.load(fh).items()}
            except (OSError, ValueError) as e:
                log(f"offset file unreadable, starting from 0: {e}")

    def next(self, topic, partition):
        return self.d.get(f"{topic}|{partition}", 0)

    def advance(self, topic, partition, offset):
        k = f"{topic}|{partition}"
        if offset + 1 <= self.d.get(k, 0):
            return
        self.d[k] = offset + 1
        if self.path:
            tmp = f"{self.path}.tmp"
            try:
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(self.d, fh)
                os.replace(tmp, self.path)
            except OSError as e:
                log(f"cannot save offsets: {e}")


async def consume_alerts(bridge, consumer=None, offsets=None):
    consumer = consumer or Consumer(PROXY, GROUP, [ALERTS_TOPIC])
    offsets = offsets or OffsetStore(None)
    log(f"consuming {ALERTS_TOPIC} as group {GROUP}; resume at "
        f"{offsets.d or 'offset 0 (no offset file)'}")
    while True:
        try:
            recs = await asyncio.to_thread(consumer.poll, 1000)
        except ProxyError as e:
            log(f"poll failed: {e}; retry in 5s")
            consumer.name = None
            await asyncio.sleep(5)
            continue
        skipped = 0
        for r in recs:
            if r["offset"] < offsets.next(r["topic"], r["partition"]):
                skipped += 1                       # already processed before a restart
                continue
            v = r.get("value")
            if isinstance(v, dict) and v.get("type") != "heartbeat":
                log(f"alert {v.get('type')} case_key={v.get('case_key')} (offset {r['offset']})")
                await bridge.handle_alert(v)
            offsets.advance(r["topic"], r["partition"], r["offset"])
        if skipped:
            log(f"skipped {skipped} already-processed record(s)")
        if recs:
            try:
                await asyncio.to_thread(consumer.commit, recs)
            except ProxyError as e:
                log(f"offset commit failed (alerts may be re-processed): {e}")
        else:
            await asyncio.sleep(0.2)


async def main():
    mapping = load_mapping(MAPPING_FILE)
    log(f"mapping: alert types {sorted(mapping.get('on_alert', {}))}, "
        f"zeebe tasks {sorted(mapping.get('zeebe_tasks', {}))}")

    dcr = None
    if DCR_ENABLED:
        from dcr import DcrClient
        dcr = DcrClient(os.getenv("DCR_GRAPH_ID"), os.getenv("DCR_USERNAME"), os.getenv("DCR_PASSWORD"))
        log(f"DCR enabled, graph {dcr.graph_id}")

    await asyncio.to_thread(wait_for_proxy, PROXY, 60, 2.0, log)
    state = STATE_FILE if STATE_FILE and os.path.isdir(os.path.dirname(STATE_FILE)) else None
    if STATE_FILE and not state:
        log(f"state dir for {STATE_FILE} missing: case map kept in memory only")
    bridge = Bridge(mapping, dcr=dcr, emit=emit_pais, log=post_log, printer=log, state_file=state)
    log(f"case map: {len(bridge.cases)} known cases" + (f" from {state}" if state else ""))

    # Clean shutdown on SIGTERM (docker stop): delete the proxy-side consumer instance, otherwise a
    # zombie instance stays in the group and can swallow alerts until the proxy expires it.
    consumer = Consumer(PROXY, GROUP, [ALERTS_TOPIC])
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    offsets = OffsetStore(os.path.join(os.path.dirname(state), "offsets.json") if state else None)
    tasks = [consume_alerts(bridge, consumer, offsets)]
    if ZEEBE_ENABLED:
        zeebe, worker = await setup_zeebe(bridge)
        bridge.zeebe = zeebe
        tasks.append(worker.work())
    else:
        log("Zeebe disabled")
    skipped = bridge.skipped_actions()
    if skipped:
        log("engine disabled, these mapping actions are SKIPPED: "
            + ", ".join(f"{t}->{e}" for t, e in skipped))
    running = [asyncio.ensure_future(t) for t in tasks]
    stopper = asyncio.ensure_future(stop.wait())
    done, _ = await asyncio.wait(running + [stopper], return_when=asyncio.FIRST_COMPLETED)
    for t in running:
        t.cancel()
    await asyncio.to_thread(consumer.close)
    log("stopped: consumer instance closed")
    for t in done:
        if t is not stopper and t.exception():
            raise t.exception()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
