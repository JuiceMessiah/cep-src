"""
Offline end-to-end test: fake Redpanda HTTP proxy (produce + consumer API),
fake DCR Graphs API, fake Zeebe client. Runs the real bridge code
(core.py, pandaproxy.py, dcr.py, app.consume_alerts) and the real logging
service (Flask), then checks pais.events and the XES output.

  python3 tests/test_offline.py
"""
import asyncio
import json
import os
import re
import sys
import tempfile
import threading
import urllib.request
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bridge"))
TMP = Path(tempfile.mkdtemp())
os.environ["LOG_DIR"] = str(TMP / "logs")
os.environ["XES_DIR"] = str(TMP / "XES")


# ---------------- fake Redpanda HTTP proxy ----------------
class Proxy:
    def __init__(self):
        self.topics = {"alerts": [], "pais.events": []}
        self.committed = {}        # (group, topic, partition) -> offset
        self.instances = {}        # (group, name) -> {"topics": [...], "pos": {topic: offset}}
        self.lock = threading.Lock()


P = Proxy()


class ProxyHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj=None):
        body = json.dumps(obj).encode() if obj is not None else b""
        self.send_response(code)
        self.send_header("Content-Type", "application/vnd.kafka.v2+json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n)) if n else None

    def do_GET(self):
        if self.path == "/topics":
            return self._send(200, list(P.topics))
        m = re.match(r"/consumers/([^/]+)/instances/([^/]+)/records", self.path)
        if m:
            g, n = m.groups()
            with P.lock:
                inst = P.instances.get((g, n))
                if inst is None:
                    return self._send(404, {"error_code": 40403, "message": "Consumer instance not found"})
                assert self.headers["Accept"] == "application/vnd.kafka.json.v2+json"
                out = []
                for t in inst["topics"]:
                    start = inst["pos"].setdefault(t, P.committed.get((g, t, 0), 0))
                    for off in range(start, len(P.topics[t])):
                        k, v = P.topics[t][off]
                        out.append({"topic": t, "key": k, "value": v, "partition": 0, "offset": off})
                    inst["pos"][t] = len(P.topics[t])
            return self._send(200, out)
        self._send(404, {"message": "not found"})

    def do_POST(self):
        b = self._body()
        m = re.match(r"/topics/([^/]+)$", self.path)
        if m:
            assert self.headers["Content-Type"] == "application/vnd.kafka.json.v2+json"
            t = m.group(1)
            with P.lock:
                offs = []
                for r in b["records"]:
                    P.topics.setdefault(t, []).append((r["key"], r["value"]))
                    offs.append({"partition": 0, "offset": len(P.topics[t]) - 1})
            return self._send(200, {"offsets": offs})
        m = re.match(r"/consumers/([^/]+)$", self.path)
        if m:
            with P.lock:
                P.instances[(m.group(1), b["name"])] = {"topics": [], "pos": {}}
            return self._send(200, {"instance_id": b["name"], "base_uri": "x"})
        m = re.match(r"/consumers/([^/]+)/instances/([^/]+)/subscription", self.path)
        if m:
            with P.lock:
                P.instances[m.groups()]["topics"] = b["topics"]
            return self._send(204)
        m = re.match(r"/consumers/([^/]+)/instances/([^/]+)/offsets", self.path)
        if m:
            g = m.group(1)
            with P.lock:
                for p in b["partitions"]:
                    P.committed[(g, p["topic"], p["partition"])] = p["offset"]
            return self._send(204)
        self._send(404, {"message": "not found"})

    def do_DELETE(self):
        m = re.match(r"/consumers/([^/]+)/instances/([^/]+)$", self.path)
        with P.lock:
            P.instances.pop(m.groups(), None)
        self._send(204)


# ---------------- fake DCR Graphs API ----------------
class Dcr:
    LABELS = {"ReportFire": "Report fire", "Escalate": "Escalate case", "CloseCase": "Close case", "Never": "Never"}
    sims = []
    enabled = {}      # sim -> set(events)
    executed = {}     # sim -> set(events)
    auth_seen = set()


class DcrHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, text=""):
        b = text.encode()
        self.send_response(code)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_POST(self):
        Dcr.auth_seen.add(self.headers.get("Authorization"))
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if re.fullmatch(r"/api/graphs/42/sims", self.path):
            sid = str(1000 + len(Dcr.sims))
            Dcr.sims.append(sid)
            Dcr.enabled[sid] = {"ReportFire"}
            Dcr.executed[sid] = set()
            return self._send(201)
        m = re.fullmatch(r"/api/graphs/42/sims/(\d+)/events/(\w+)", self.path)
        if m:
            sid, ev = m.groups()
            if ev not in Dcr.enabled.get(sid, set()):
                return self._send(400, "<error>event not enabled</error>")
            Dcr.enabled[sid] = {"Escalate", "CloseCase"} if ev == "ReportFire" else {"CloseCase"}
            Dcr.executed[sid].add(ev)
            return self._send(204)
        self._send(404)

    def do_GET(self):
        if self.path.startswith("/api/graphs/42/sims?filter=all"):
            return self._send(200, "<log>" + "".join(f'<trace id="{s}"/>' for s in Dcr.sims) + "</log>")
        m = re.fullmatch(r"/api/graphs/42/sims/(\d+)/events", self.path)
        if m:
            sid = m.group(1)
            ids = sorted(Dcr.enabled.get(sid, set()) | Dcr.executed.get(sid, set()) | {"Never"})
            evs = "".join(f'<event id="{e}" label="{Dcr.LABELS[e]}" enabled="{str(e in Dcr.enabled.get(sid, set())).lower()}" '
                          f'executed="{str(e in Dcr.executed.get(sid, set())).lower()}"/>' for e in ids)
            return self._send(200, f"<events>{evs}</events>")
        self._send(404)


# ---------------- fake Zeebe ----------------
class FakeZeebe:
    """start_process = gRPC; correlate_message / publish_message = REST in the real adapter."""
    def __init__(self):
        self.started, self.messages, self.n = [], [], 2251799813685248

    async def start_process(self, pid, variables):
        self.n += 1
        self.started.append((pid, variables, self.n))
        return self.n

    async def correlate_message(self, name, key, variables):
        assert name == "SmokeStart"          # message start event of cep_smoke_test
        self.n += 1
        self.started.append(("cep_smoke_test", variables, self.n))
        return str(self.n)

    async def publish_message(self, name, key, variables, ttl_ms=None):
        self.messages.append((name, key, variables, ttl_ms))


def serve(handler):
    s = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=s.serve_forever, daemon=True).start()
    return s, f"http://127.0.0.1:{s.server_address[1]}"


def main():
    proxy_srv, proxy_url = serve(ProxyHandler)
    dcr_srv, dcr_url = serve(DcrHandler)

    # real logging service (Flask) on a free port
    import importlib.util
    spec = importlib.util.spec_from_file_location("logsvc", ROOT / "logging_service" / "app.py")
    logsvc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(logsvc)
    from werkzeug.serving import make_server
    ls = make_server("127.0.0.1", 0, logsvc.app, threaded=True)
    threading.Thread(target=ls.serve_forever, daemon=True).start()
    log_url = f"http://127.0.0.1:{ls.server_port}/log"

    os.environ.update({"PANDAPROXY_URL": proxy_url, "LOGGING_URL": log_url,
                       "MAPPING_FILE": str(ROOT / "bridge" / "mapping.json")})
    spec = importlib.util.spec_from_file_location("bridge_app", ROOT / "bridge" / "app.py")
    bridge_app = importlib.util.module_from_spec(spec)    # bridge/app.py, reads env at import
    spec.loader.exec_module(bridge_app)
    from core import Bridge, load_mapping
    from dcr import DcrClient

    mapping = load_mapping(ROOT / "bridge" / "mapping.json")
    dcr = DcrClient("42", "student", "s3cret-pass", api=f"{dcr_url}/api")
    zb = FakeZeebe()
    bridge = Bridge(mapping, zeebe=zb, dcr=dcr, emit=bridge_app.emit_pais, log=bridge_app.post_log,
                    printer=lambda m: print("   bridge:", m))

    def put_alert(t, ck, et):
        body = json.dumps({"records": [{"key": ck, "value": {
            "alert_id": f"a-{t}-{ck}-{et}", "type": t, "case_key": ck, "cell": ck, "severity": "high",
            "event_time": et, "details": {"frp": 42.5}}}]}).encode()
        urllib.request.urlopen(urllib.request.Request(
            f"{proxy_url}/topics/alerts", data=body, method="POST",
            headers={"Content-Type": "application/vnd.kafka.json.v2+json"}))

    T0 = 1786000000000
    put_alert("SmokeTest", "c3_4", T0)
    put_alert("SmokeTest", "c5_1", T0 + 1000)
    put_alert("SmokeTestUpdate", "c3_4", T0 + 3_600_000)
    put_alert("NoSuchType", "c9_9", T0 + 5)
    put_alert("SmokeTest", "c3_4", T0)          # exact duplicate (same alert_id): processed once
    body = json.dumps({"records": [{"key": "heartbeat", "value": {"type": "heartbeat", "event_time": T0}}]})
    urllib.request.urlopen(urllib.request.Request(f"{proxy_url}/topics/alerts", data=body.encode(),
                           method="POST", headers={"Content-Type": "application/vnd.kafka.json.v2+json"}))

    async def run_consumer_briefly():
        task = asyncio.create_task(bridge_app.consume_alerts(bridge))
        await asyncio.sleep(1.5)
        # Zeebe jobs as pyzeebe would deliver them (task type, variables, instance key, process id)
        for pid, variables, key in zb.started:
            out = bridge.on_zeebe_job("cep-event", dict(variables), key, pid,
                                      headers={"activity": "Acknowledge incident"}, element_id="Task_Acknowledge")
            assert out == {}
        # cep-event without the header: job still completes, envelope carries an error
        bridge.on_zeebe_job("cep-event", dict(zb.started[1][1]), zb.started[1][2], "cep_smoke_test",
                            headers={}, element_id="Task_NoHeader")
        out = bridge.on_zeebe_job("close-incident", dict(zb.started[0][1]) | {"priority": "P1"},
                                  zb.started[0][2], "cep_smoke_test")
        assert out == {"closedBy": "bridge"}
        # DCR watch: first look at each case sets the baseline (nothing reported)...
        assert await bridge.dcr_watch_once() == 0
        # ...then a person executes CloseCase in the portal on c3_4's case
        c34 = bridge.cases[("dcr", "dcr:42", "c3_4")]
        Dcr.executed[c34].add("CloseCase")
        assert await bridge.dcr_watch_once() == 1
        assert await bridge.dcr_watch_once() == 0        # reported once, not on every poll
        # second update for c5_1 -> DCR Escalate allowed; then Escalate again on c3_4 -> rejected by DCR
        put_alert("SmokeTestUpdate", "c3_4", T0 + 7_200_000)
        await asyncio.sleep(1.5)
        task.cancel()

    bridge_app.GROUP = "test-group"
    asyncio.run(run_consumer_briefly())

    # ---------------- assertions ----------------
    assert [s[0] for s in zb.started] == ["cep_smoke_test", "cep_smoke_test"], zb.started
    v = zb.started[0][1]
    assert v["caseKey"] == "c3_4" and v["cepEventTime"] == T0 and v["alertType"] == "SmokeTest"
    assert [(m[0], m[1], m[3]) for m in zb.messages] == [("FireUpdate", "c3_4", 60000)] * 2
    assert len(Dcr.sims) == 2 and Dcr.auth_seen and None not in Dcr.auth_seen

    pais = [v for _, v in P.topics["pais.events"]]
    names = [(e["engine"], e["event"], e["case_key"]) for e in pais]
    print("pais.events:", names)
    assert ("camunda", "ProcessStarted", "c3_4") in names
    assert ("dcr", "CaseCreated", "c3_4") in names
    assert ("dcr", "Report fire", "c3_4") in names          # DCR events are named by label
    assert ("camunda", "Acknowledge incident", "c5_1") in names
    assert ("camunda", "Close incident", "c3_4") in names
    assert ("camunda", "Task_NoHeader", "c5_1") in names
    noh = [e for e in pais if e["event"] == "Task_NoHeader"][0]
    assert "activity" in noh["error"], noh
    close = [e for e in pais if e["event"] == "Close incident"][0]
    assert close["payload"]["priority"] == "P1"
    portal = [e for e in pais if e["payload"].get("source") == "dcr-portal"]
    assert [(e["event"], e["case_key"], e["role"]) for e in portal] == [("Close case", "c3_4", "dcr-user")], portal
    assert portal[0]["event_time"] == T0 + 3_600_000          # event time of the alert that last touched the case (first update)
    assert portal[0]["payload"]["event_id"] == "CloseCase"
    esc = [e for e in pais if e["event"] == "Escalate case"]
    assert len(esc) == 2 and esc[0]["error"] == "" and "CloseCase" in esc[0]["payload"]["enabledEvents"]
    assert esc[1]["error"] and esc[1]["payload"]["rejected"] is True        # Escalate not enabled twice
    rf = [e for e in pais if e["event"] == "Report fire" and e["case_key"] == "c3_4"][0]
    assert rf["payload"]["event_id"] == "ReportFire"
    assert rf["event_time"] == T0 and set(rf["payload"]["enabledEvents"]) == {"Escalate", "CloseCase"}
    ack = [e for e in pais if e["event"] == "Acknowledge incident" and e["case_key"] == "c3_4"][0]
    assert ack["event_time"] == T0 and "alert" not in ack["payload"]       # time propagated, no blob
    commits = {k: v for k, v in P.committed.items() if k[0].startswith("test-group-") and k[1] == "alerts"}
    assert list(commits.values()) == [7], P.committed                      # all 7 records committed (incl. the duplicate)
    assert all("s3cret-pass" not in json.dumps(e) for e in pais)

    # logging service + XES
    with urllib.request.urlopen(log_url.replace("/log", "/events")) as r:
        logged = json.load(r)
    assert any(e["event"] == "UnmappedAlert" and e["error"] for e in logged)
    xes = sorted((TMP / "XES").glob("*.xes"))
    print("XES files:", [p.name.split("_", 2)[-1] for p in xes])
    smoke = [p for p in xes if p.name.endswith("_cep_smoke_test.xes")][0]
    root = ET.parse(smoke).getroot()
    traces = root.findall("trace")
    assert len(traces) == 2
    t1 = [t for t in traces if any(s.get("key") == "case_key" and s.get("value") == "c3_4"
                                   for s in t.findall("string"))][0]
    acts = [e.find("string[@key='concept:name']").get("value") for e in t1.findall("event")]
    print("trace c3_4 (BPMN):", acts)
    assert acts[0] == "Fire risk detected" and "Acknowledge incident" in acts and "Close incident" in acts
    assert acts.count("Fire update received") == 2
    dcr_xes = [p for p in xes if p.name.endswith("_dcr_42.xes")][0]
    dacts = [e.find("string[@key='concept:name']").get("value")
             for t in ET.parse(dcr_xes).getroot().findall("trace") for e in t.findall("event")]
    print("DCR traces:", dacts)
    assert "Fire risk detected" not in dacts and dacts.count("Report fire") == 2 and dacts.count("Escalate case") == 2
    assert dacts.count("Close case") == 1
    print("\nALL OFFLINE TESTS PASSED")
    proxy_srv.shutdown(); dcr_srv.shutdown(); ls.shutdown()


if __name__ == "__main__":
    main()
