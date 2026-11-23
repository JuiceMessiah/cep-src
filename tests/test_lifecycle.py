"""
Lifecycle tests for the bridge (offline):
  1. case map survives a restart (state file), so CEP triggers after a restart land in the right trace
  2. SIGTERM -> proxy-side consumer instance is deleted (no zombie in the consumer group)
  3. consumer creation falls back if the proxy rejects "auto.commit.enable"

  python3 tests/test_lifecycle.py
"""
import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bridge"))
from core import Bridge, load_mapping          # noqa: E402
from pandaproxy import Consumer                # noqa: E402


# ---------- 1. persisted case map ----------
class Z:
    def __init__(self):
        self.n = 100

    async def start_process(self, pid, v):
        self.n += 1
        return self.n

    async def publish_message(self, *a):
        pass


state = Path(tempfile.mkdtemp()) / "cases.json"
mapping = load_mapping(ROOT / "bridge" / "mapping.json")
logged = []
b1 = Bridge(mapping, zeebe=Z(), log=logged.append, printer=lambda *a: None, state_file=str(state))
asyncio.run(b1.handle_alert({"alert_id": "a", "type": "SmokeTest", "case_key": "c7_2", "event_time": 1}))
assert json.loads(state.read_text()) == {"zeebe|cep_smoke_test|c7_2": "101"}

logged.clear()
b2 = Bridge(mapping, zeebe=Z(), log=logged.append, printer=lambda *a: None, state_file=str(state))  # "restart"
assert b2.cases[("zeebe", "cep_smoke_test", "c7_2")] == "101"
asyncio.run(b2.handle_alert({"alert_id": "b", "type": "SmokeTestUpdate", "case_key": "c7_2", "event_time": 2}))
upd = [e for e in logged if e["event"] == "Fire update received"][0]
assert upd["case_id"] == "101", upd          # before the fix: UNKNOWN_CASE
print("1. case map survives restart: ok")


# ---------- fake proxy for 2 + 3 ----------
calls = []
reject_autocommit = {"on": False}


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj=None):
        body = json.dumps(obj).encode() if obj is not None else b""
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        calls.append(("GET", self.path))
        if self.path == "/topics":
            return self._send(200, ["alerts"])
        time.sleep(0.2)
        return self._send(200, [])

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n)) if n else None
        calls.append(("POST", self.path, body))
        if self.path.startswith("/consumers/") and "/instances/" not in self.path:
            if reject_autocommit["on"] and "auto.commit.enable" in body:
                return self._send(422, {"message": "unknown field"})
            return self._send(200, {"instance_id": body["name"]})
        return self._send(204)

    def do_DELETE(self):
        calls.append(("DELETE", self.path))
        self._send(204)


srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
proxy = f"http://127.0.0.1:{srv.server_address[1]}"

# ---------- 3. fallback ----------
reject_autocommit["on"] = True
c = Consumer(proxy, "g", ["alerts"])
c.start()
creates = [x for x in calls if x[0] == "POST" and x[1] == "/consumers/g"]
assert len(creates) == 2 and "auto.commit.enable" in creates[0][2] and "auto.commit.enable" not in creates[1][2]
reject_autocommit["on"] = False
calls.clear()
print("3. auto.commit.enable fallback: ok")

# ---------- 2. SIGTERM closes the consumer instance ----------
env = dict(os.environ, PANDAPROXY_URL=proxy, ZEEBE_ENABLED="false", LOGGING_URL="",
           MAPPING_FILE=str(ROOT / "bridge" / "mapping.json"), STATE_FILE=str(state),
           PYTHONPATH=str(ROOT / "bridge"))
p = subprocess.Popen([sys.executable, "-u", str(ROOT / "bridge" / "app.py")], env=env,
                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
time.sleep(2.0)
p.send_signal(signal.SIGTERM)
out, _ = p.communicate(timeout=10)
print("   bridge output:", " | ".join(out.strip().splitlines()[-3:]))
deletes = [x for x in calls if x[0] == "DELETE"]
assert p.returncode == 0, (p.returncode, out)
assert len(deletes) == 1 and "/consumers/cep-bridge/instances/bridge-" in deletes[0][1], calls
assert "stopped: consumer instance closed" in out and "1 known cases" in out
print("2. SIGTERM deletes the consumer instance: ok")
srv.shutdown()


# ---------- 4. restart does not re-process alerts (proxy that ignores committed offsets, like Redpanda) ----------
import importlib.util                                                   # noqa: E402
ALERTS = [{"alert_id": f"x{i}", "type": "SmokeTest", "case_key": f"c{i}", "event_time": i} for i in range(4)]


class RP(BaseHTTPRequestHandler):
    """Every new consumer instance starts at offset 0, commits are accepted and ignored."""
    seen = {}

    def log_message(self, *a):
        pass

    def _send(self, code, obj=None):
        body = json.dumps(obj).encode() if obj is not None else b""
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        inst = self.path.split("/instances/")[1].split("/")[0]
        if RP.seen.get(inst):
            time.sleep(0.1)
            return self._send(200, [])
        RP.seen[inst] = True
        return self._send(200, [{"topic": "alerts", "partition": 0, "offset": i, "key": a["case_key"], "value": a}
                                for i, a in enumerate(ALERTS)])

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        return self._send(200 if self.path.endswith("/consumers/cep-bridge") else 204, {})

    def do_DELETE(self):
        self._send(204)


srv2 = ThreadingHTTPServer(("127.0.0.1", 0), RP)
threading.Thread(target=srv2.serve_forever, daemon=True).start()
os.environ.update(PANDAPROXY_URL=f"http://127.0.0.1:{srv2.server_address[1]}", LOGGING_URL="")
spec = importlib.util.spec_from_file_location("bridge_app", ROOT / "bridge" / "app.py")
bapp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bapp)
offs_file = str(Path(tempfile.mkdtemp()) / "offsets.json")


def run_once():
    handled = []

    class B:
        async def handle_alert(self, a):
            handled.append(a["case_key"])

    async def go():
        t = asyncio.create_task(bapp.consume_alerts(B(), bapp.Consumer(bapp.PROXY, "cep-bridge", ["alerts"]),
                                                    bapp.OffsetStore(offs_file)))
        await asyncio.sleep(0.8)
        t.cancel()
    asyncio.run(go())
    return handled


first, second = run_once(), run_once()             # second = "bridge restarted", new proxy instance
assert first == ["c0", "c1", "c2", "c3"], first
assert second == [], second                         # before the fix: c0..c3 again
assert json.loads(Path(offs_file).read_text()) == {"alerts|0": 4}
srv2.shutdown()
print("4. restart does not re-process alerts: ok")
print("\nALL LIFECYCLE TESTS PASSED")
