"""
Minimal Redpanda HTTP proxy (Kafka REST API v2) client. Stdlib only.

Same wire format as poller.py / replay.py for producing, plus a consumer
for the bridge. Consumer offsets are committed explicitly after each batch
(at-least-once from the bridge's point of view).
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

V2 = "application/vnd.kafka.v2+json"
JSON_V2 = "application/vnd.kafka.json.v2+json"


class ProxyError(RuntimeError):
    def __init__(self, status, msg):
        super().__init__(f"HTTP {status}: {msg}")
        self.status = status


def _request(method, url, body=None, content_type=V2, accept=V2, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Accept": accept}
    if data is not None:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return json.loads(raw) if raw.strip() else None
    except urllib.error.HTTPError as e:
        detail = e.read()[:200].decode("utf-8", "replace")
        raise ProxyError(e.code, detail) from None
    except urllib.error.URLError as e:
        raise ProxyError(0, f"cannot reach proxy: {e.reason}") from None


def produce(proxy, topic, records):
    """records: list of (key, value_dict)."""
    for i in range(0, len(records), 500):
        chunk = records[i:i + 500]
        resp = _request(
            "POST", f"{proxy}/topics/{urllib.parse.quote(topic)}",
            {"records": [{"key": k, "value": v} for k, v in chunk]},
            content_type=JSON_V2,
        ) or {}
        errs = [o for o in resp.get("offsets", []) if o.get("error_code")]
        if errs:
            raise ProxyError(500, f"produce to {topic}: {len(errs)} record errors, first={errs[0]}")


class Consumer:
    """One consumer instance in a consumer group. Recreated transparently if the
    proxy expired it (idle instances are removed server-side)."""

    def __init__(self, proxy, group, topics, offset_reset="earliest", ephemeral_group=False):
        """ephemeral_group=True: every (re)start joins a brand-new group "<group>-<random>".
        Redpanda's HTTP proxy keeps old instances of a group alive server-side for minutes after a
        restart; with one partition, the range balancer may hand that partition to such a zombie,
        and alerts silently go nowhere (observed 2026-11-23). A fresh group has no other members.
        Use it together with an own offset store, because a fresh group always starts at
        offset_reset."""
        self.proxy = proxy.rstrip("/")
        self.base_group = group
        self.group = group
        self.topics = list(topics)
        self.offset_reset = offset_reset
        self.ephemeral_group = ephemeral_group
        self.name = None

    @property
    def _base(self):
        return f"{self.proxy}/consumers/{urllib.parse.quote(self.group)}/instances/{self.name}"

    def start(self):
        if self.ephemeral_group:
            self.group = f"{self.base_group}-{uuid.uuid4().hex[:8]}"
        self.name = f"bridge-{uuid.uuid4().hex[:8]}"
        body = {"name": self.name, "format": "json", "auto.offset.reset": self.offset_reset,
                "auto.commit.enable": "false"}      # we commit explicitly after processing
        try:
            _request("POST", f"{self.proxy}/consumers/{urllib.parse.quote(self.group)}", body)
        except ProxyError as e:
            if e.status not in (400, 422):
                raise
            body.pop("auto.commit.enable")           # proxy doesn't know the field: create without it
            _request("POST", f"{self.proxy}/consumers/{urllib.parse.quote(self.group)}", body)
        _request("POST", f"{self._base}/subscription", {"topics": self.topics})

    def poll(self, timeout_ms=1000, max_bytes=1_000_000):
        """Returns a list of {topic, key, value, partition, offset}."""
        if self.name is None:
            self.start()
        try:
            recs = _request("GET", f"{self._base}/records?timeout={timeout_ms}&max_bytes={max_bytes}",
                            accept=JSON_V2, timeout=timeout_ms / 1000 + 30)
        except ProxyError as e:
            if e.status == 404:          # instance expired: rejoin the group, resume from committed offsets
                self.name = None
                return []
            raise
        return recs or []

    def commit(self, records):
        """Commit the next offset after the highest one seen per partition."""
        if not records:
            return
        last = {}
        for r in records:
            k = (r["topic"], r["partition"])
            last[k] = max(last.get(k, -1), r["offset"])
        _request("POST", f"{self._base}/offsets",
                 {"partitions": [{"topic": t, "partition": p, "offset": o + 1} for (t, p), o in last.items()]})

    def close(self):
        if self.name:
            try:
                _request("DELETE", self._base)
            except ProxyError:
                pass
            self.name = None


def wait_for_proxy(proxy, attempts=60, delay=2.0, log=print):
    for i in range(attempts):
        try:
            _request("GET", f"{proxy}/topics")
            return
        except ProxyError as e:
            if i == attempts - 1:
                raise
            log(f"waiting for redpanda proxy ({e})")
            time.sleep(delay)
