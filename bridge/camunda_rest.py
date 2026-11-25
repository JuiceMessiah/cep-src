"""
Camunda 8.8 Orchestration Cluster REST API (/v2), stdlib only.

Used for what the gRPC client (pyzeebe 3.0.4) can't do or can't do reliably:
  POST /v2/messages/correlation  correlate a message, get the process instance key back
                                 (works for message START events too; NOT buffered)
  POST /v2/messages/publication  publish a message with an explicit timeToLive (buffered)
  POST /v2/deployments           deploy *.bpmn + *.dmn + *.form atomically (multipart)
  POST /v2/element-instances/search   completed activities of a process instance (XES source)
  GET  /v2/process-instances/{key}    instance state (stop polling finished instances)
  The last two are eventually consistent: they read what the Camunda Exporter has exported.

Auth: none. Both compose files set CAMUNDA_SECURITY_AUTHENTICATION_UNPROTECTEDAPI=true and
the API is only reachable on the internal Docker network.
"""
import json
import os
import urllib.error
import urllib.request
import uuid


class CamundaError(RuntimeError):
    def __init__(self, status, msg):
        super().__init__(f"HTTP {status}: {msg}")
        self.status = status


class CamundaRest:
    def __init__(self, base_url="http://camunda:8080", timeout=30):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def _post(self, path, body, content_type="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        req = urllib.request.Request(f"{self.base}/v2{path}", data=data, method="POST",
                                     headers={"Content-Type": content_type, "Accept": "application/json"})
        return self._send(req)

    def _get(self, path):
        return self._send(urllib.request.Request(f"{self.base}/v2{path}", headers={"Accept": "application/json"}))

    def _send(self, req):
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                raw = r.read()
                return json.loads(raw) if raw.strip() else {}
        except urllib.error.HTTPError as e:
            detail = e.read()[:400].decode("utf-8", "replace")
            try:  # RFC 7807 problem body: {"title":..., "detail":...}
                p = json.loads(detail)
                detail = p.get("detail") or p.get("title") or detail
            except ValueError:
                pass
            raise CamundaError(e.code, detail) from None
        except urllib.error.URLError as e:
            raise CamundaError(0, f"cannot reach Camunda REST at {self.base}: {e.reason}") from None

    def correlate(self, name, correlation_key, variables=None):
        """Returns the key of the first process instance the message correlated with.
        404 = no open subscription (or a message start blocked by an active instance with
        the same correlation key)."""
        resp = self._post("/messages/correlation", {
            "name": name, "correlationKey": str(correlation_key), "variables": variables or {}})
        return str(resp.get("processInstanceKey", ""))

    def publish(self, name, correlation_key, variables=None, ttl_ms=60_000):
        resp = self._post("/messages/publication", {
            "name": name, "correlationKey": str(correlation_key),
            "timeToLive": int(ttl_ms), "variables": variables or {}})
        return str(resp.get("messageKey", ""))

    def deploy(self, paths):
        """All files in ONE deployment (atomic: a BPMN and the DMN it calls go in together)."""
        boundary = f"----cep{uuid.uuid4().hex}"
        parts = []
        for p in paths:
            with open(p, "rb") as fh:
                content = fh.read()
            parts.append(
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"resources\"; "
                f"filename=\"{os.path.basename(p)}\"\r\nContent-Type: application/octet-stream\r\n\r\n".encode()
                + content + b"\r\n")
        body = b"".join(parts) + f"--{boundary}--\r\n".encode()
        return self._post("/deployments", body, f"multipart/form-data; boundary={boundary}")

    def element_instances(self, process_instance_key, limit=1000):
        """All element instances of one process instance (any state). One page is plenty for our
        processes; a warning field is set if Camunda reports more."""
        resp = self._post("/element-instances/search", {
            "filter": {"processInstanceKey": str(process_instance_key)},
            "page": {"limit": limit}})
        items = resp.get("items", [])
        total = (resp.get("page") or {}).get("totalItems")
        if isinstance(total, int) and total > len(items):
            raise CamundaError(0, f"{total} element instances, only {len(items)} fetched (raise limit)")
        return items

    def instance_state(self, process_instance_key):
        """ACTIVE / COMPLETED / TERMINATED (or None if Camunda doesn't know it yet)."""
        try:
            return self._get(f"/process-instances/{process_instance_key}").get("state")
        except CamundaError as e:
            if e.status == 404:          # not exported yet
                return None
            raise
