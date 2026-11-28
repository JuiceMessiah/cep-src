"""
DCR Graphs REST client (repository.dcrgraphs.net), stdlib only.

Same endpoints as the course example (POESS-example DCR/microservice/dcr_helper.py):
  POST /graphs/{graph}/sims                      create a simulation (= case)
  GET  /graphs/{graph}/sims?filter=all            list sims (to find the new id)
  POST /graphs/{graph}/sims/{sim}/events/{event}  execute an event
  GET  /graphs/{graph}/sims/{sim}/events          enabled/pending events

Difference to the example: create_case() is serialised with a lock. The API
doesn't return the new sim id, so the example takes the LAST id from the list;
two alerts at the same moment would otherwise both get the same case.
Credentials come from the environment and never appear in errors or logs.

events() returns every <event> tag's attributes as a dict. The course example only relies
on `enabled`; the bridge's DCR watcher additionally needs `executed` to notice events people
execute in the portal. tools/dcr_probe.py prints which attribute NAMES the API really returns.
"""
import base64
import re
import threading
import urllib.error
import urllib.request

API = "https://repository.dcrgraphs.net/api"


class DcrError(RuntimeError):
    pass


class DcrClient:
    def __init__(self, graph_id, username, password, api=API, timeout=30):
        if not (graph_id and username and password):
            raise DcrError("DCR_GRAPH_ID, DCR_USERNAME and DCR_PASSWORD must be set")
        self.graph_id = str(graph_id)
        self.api = api.rstrip("/")
        self.timeout = timeout
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        self._headers = {"Authorization": f"Basic {token}", "Accept": "application/xml",
                         "Content-Type": "application/json"}
        self._create_lock = threading.Lock()

    def _call(self, method, path, body=b"{}"):
        req = urllib.request.Request(f"{self.api}/graphs/{self.graph_id}{path}",
                                     data=body if method == "POST" else None,
                                     method=method, headers=self._headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read()[:300].decode("utf-8", "replace")
        except urllib.error.URLError as e:
            raise DcrError(f"cannot reach DCR API: {e.reason}") from None

    def create_case(self):
        with self._create_lock:
            status, text = self._call("POST", "/sims")
            if status not in (200, 201):
                raise DcrError(f"create sim: HTTP {status}")
            status, xml = self._call("GET", "/sims?filter=all")
            if status not in (200, 201):
                raise DcrError(f"list sims: HTTP {status}")
            ids = re.findall(r'<trace[^>]*id="(\d+)"', xml)
            if not ids:
                raise DcrError("create sim: no sim ids in response")
            return ids[-1]

    def events(self, case_id):
        """[{attr: value, ...}] for every event in the case's current marking (attribute
        names lower-cased; values are strings as the API sends them)."""
        status, xml = self._call("GET", f"/sims/{case_id}/events")
        if status not in (200, 201):
            raise DcrError(f"events: HTTP {status}")
        return parse_events(xml)

    def enabled_events(self, case_id):
        return enabled(self.events(case_id))

    def execute(self, case_id, event_id):
        """Execute, then return the case's events (same format as events())."""
        status, _ = self._call("POST", f"/sims/{case_id}/events/{event_id}")
        if status not in (200, 201, 204):
            # typical cause: event not enabled in the current marking (a DCR constraint said no)
            raise DcrError(f"execute {event_id}: HTTP {status} (not enabled?)")
        return self.events(case_id)


_TAG = re.compile(r"<event\b([^>]*)>", re.IGNORECASE)
_ATTR = re.compile(r'([\w:-]+)\s*=\s*"([^"]*)"')


def parse_events(xml):
    xml = xml.replace('\\"', '"')   # the API sometimes returns XML inside a JSON string
    return [{k.lower(): v for k, v in _ATTR.findall(m.group(1))} for m in _TAG.finditer(xml)]


def enabled(events):
    return [e["id"] for e in events if "id" in e and e.get("enabled", "").lower() == "true"]


def containers(events):
    """Ids of nests/subprocesses: events that other events name as their `parent`. The API marks
    them executed as soon as one of their children runs, but they are not activities."""
    return {e["parent"] for e in events if e.get("parent")}


def executed(events):
    """Set of executed event ids, or None if the API doesn't report an executed flag."""
    if not events or not any("executed" in e for e in events):
        return None
    return {e["id"] for e in events if "id" in e and e.get("executed", "").lower() == "true"}
