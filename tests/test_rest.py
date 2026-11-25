"""
Camunda REST client (bridge/camunda_rest.py) against a fake /v2 API, plus correlation-key
templates and mapping validation.

  python3 tests/test_rest.py
"""
import asyncio
import email
import email.policy
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bridge"))
from camunda_rest import CamundaError, CamundaRest  # noqa: E402
from core import Bridge, load_mapping  # noqa: E402

seen = []


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/problem+json" if code >= 400 else "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        ctype = self.headers["Content-Type"]
        seen.append((self.path, ctype, raw))
        if self.path == "/v2/messages/correlation":
            b = json.loads(raw)
            if b["correlationKey"] == "nobody":
                return self._send(404, {"type": "about:blank", "title": "NOT_FOUND", "status": 404,
                                        "detail": "Expected to find subscription for message with name 'X'"})
            return self._send(200, {"messageKey": "11", "processInstanceKey": "2251799813685300",
                                    "tenantId": "<default>"})
        if self.path == "/v2/messages/publication":
            return self._send(200, {"messageKey": "12", "tenantId": "<default>"})
        if self.path == "/v2/deployments":
            msg = email.message_from_bytes(f"Content-Type: {ctype}\r\n\r\n".encode() + raw,
                                           policy=email.policy.HTTP)
            files = [(part.get_param("name", header="content-disposition"), part.get_filename(),
                      len(part.get_payload(decode=True))) for part in msg.iter_parts()]
            seen.append(("files", files, b""))
            if any(f[1].endswith(".broken") for f in files):
                return self._send(400, {"title": "INVALID_ARGUMENT", "detail": "Command rejected: broken model"})
            return self._send(200, {"deploymentKey": "9", "deployments": [
                {"processDefinition": {"processDefinitionId": "cep_smoke_test"}},
                {"decisionDefinition": {"decisionDefinitionId": "smoke_priority"}}]})
        self._send(404, {"title": "nope"})


srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
rest = CamundaRest(f"http://127.0.0.1:{srv.server_port}")

# correlate: body fields + instance key back
assert rest.correlate("SmokeStart", "inc-c1_2-20250812T1430", {"caseKey": "inc"}) == "2251799813685300"
body = json.loads(seen[-1][2])
assert body == {"name": "SmokeStart", "correlationKey": "inc-c1_2-20250812T1430", "variables": {"caseKey": "inc"}}

# correlate with no subscription: CamundaError with Camunda's detail (problem+json), status 404
try:
    rest.correlate("X", "nobody")
    raise SystemExit("expected CamundaError")
except CamundaError as e:
    assert e.status == 404 and "Expected to find subscription" in str(e), e

# publish: explicit timeToLive in ms
assert rest.publish("DestinationChanged", "inc|amb-3", {"a": 1}, ttl_ms=0) == "12"
assert json.loads(seen[-1][2]) == {"name": "DestinationChanged", "correlationKey": "inc|amb-3",
                                   "timeToLive": 0, "variables": {"a": 1}}

# deploy: ONE multipart request, every file as a "resources" part with its file name
files = [str(ROOT / "bpmn" / "cep-smoke-test.bpmn"), str(ROOT / "bpmn" / "cep-smoke-test.dmn")]
resp = rest.deploy(files)
assert resp["deploymentKey"] == "9"
parts = seen[-1][1]
assert [(n, f) for n, f, _ in parts] == [("resources", "cep-smoke-test.bpmn"), ("resources", "cep-smoke-test.dmn")]
assert all(size == Path(p).stat().st_size for (_, _, size), p in zip(parts, files)), parts
assert seen[-2][1].startswith("multipart/form-data; boundary=")

# deploy rejected (400): status visible so app.py can stop retrying
bad = Path(srv.server_address and "/tmp") / "x.broken"
bad.write_text("<nope/>")
try:
    rest.deploy([str(bad)])
    raise SystemExit("expected CamundaError")
except CamundaError as e:
    assert e.status == 400 and "broken model" in str(e)

# unreachable: status 0, no traceback soup
try:
    CamundaRest("http://127.0.0.1:9", timeout=2).publish("x", "y")
    raise SystemExit("expected CamundaError")
except CamundaError as e:
    assert e.status == 0 and "cannot reach" in str(e)
print("1. camunda_rest: ok")

# ---------- correlation-key templates through the bridge ----------
alert = {"alert_id": "a1", "type": "CapacityRedirect", "case_key": "inc-c1_2-20250812T1430",
         "event_time": 5, "details": {"unit_id": "amb-3"}}
assert Bridge.render("{case_key}|{details.unit_id}", alert) == "inc-c1_2-20250812T1430|amb-3"
assert Bridge.render("static", alert) == "static"
try:
    Bridge.render("{details.nope}", alert)
    raise SystemExit("expected KeyError")
except KeyError as e:
    assert "details.nope" in str(e)


class Z:
    def __init__(self):
        self.pub, self.corr = [], []

    async def publish_message(self, name, key, variables, ttl_ms=None):
        self.pub.append((name, key, ttl_ms))

    async def correlate_message(self, name, key, variables):
        self.corr.append((name, key))
        return "777"


mapping = {"on_alert": {
    "CapacityRedirect": [{"engine": "zeebe", "action": "publish_message", "message": "DestinationChanged",
                          "correlation_key": "{case_key}|{details.unit_id}", "ttl_s": 0,
                          "process_id": "ambulance_fire_response", "log_activity": "Destination changed"}],
    "FireAlert": [{"engine": "zeebe", "action": "correlate_message", "message": "FireAlert",
                   "process_id": "ambulance_fire_response", "log_activity": "Mass fire alert received"}]}}
out, logged = [], []      # pais.events / logging service
z = Z()
b = Bridge(mapping, zeebe=z, emit=out.append, log=logged.append, printer=lambda *a: None)
asyncio.run(b.handle_alert(alert))
assert z.pub == [("DestinationChanged", "inc-c1_2-20250812T1430|amb-3", 0)], z.pub
asyncio.run(b.handle_alert(alert | {"details": {}, "alert_id": "a2"}))         # unit_id missing
fail = [e for e in out if e["event"] == "publish_messageFailed"]
assert len(fail) == 1 and "details.unit_id" in fail[0]["error"] and len(z.pub) == 1

asyncio.run(b.handle_alert(alert | {"type": "FireAlert", "alert_id": "a3"}))
asyncio.run(b.handle_alert(alert | {"type": "FireAlert", "alert_id": "a4"}))
assert z.corr == [("FireAlert", "inc-c1_2-20250812T1430")] * 2
assert b.cases[("zeebe", "ambulance_fire_response", "inc-c1_2-20250812T1430")] == "777"
kinds = [e["event"] for e in out if e["engine"] == "camunda"]
assert kinds == ["ProcessStarted", "MessageCorrelated"], kinds
first = [e for e in logged if e["event"] == "Mass fire alert received"][0]
assert first["case_id"] == "777" and first["role"] == "cep"      # trace complete from the first event
print("2. templates, correlate_message: ok")

# ---------- mapping validation ----------
tmp = Path("/tmp/mapping-bad.json")
tmp.write_text(json.dumps({"on_alert": {"X": [{"engine": "zeebe", "action": "correlate_message"}]}}))
try:
    load_mapping(tmp)
    raise SystemExit("expected ValueError")
except ValueError as e:
    assert "needs message" in str(e)
tmp.write_text(json.dumps({"on_alert": {"X": [{"engine": "zeebe", "action": "teleport", "message": "m"}]}}))
try:
    load_mapping(tmp)
    raise SystemExit("expected ValueError")
except ValueError as e:
    assert "unknown zeebe action" in str(e)
load_mapping(ROOT / "bridge" / "mapping.json")
print("3. mapping validation: ok")
srv.shutdown()
print("\nALL REST TESTS PASSED")
