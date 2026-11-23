#!/usr/bin/env python3
"""
cep-lab poller: OpenWeather (current weather) + NASA FIRMS (active fires)
-> JSON events -> Redpanda via its HTTP proxy (Kafka REST API v2).

Stdlib only, so the image is plain python:3.12-slim pinned by digest.
Secrets come from the environment (env_file), never from this file or git.
NEVER log request URLs: both API keys travel in the URL.

Topics:
  weather.raw  key = grid cell id       value = flat weather event
  firms.raw    key = grid cell id       value = one FIRMS detection row
"""
import csv
import io
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# ---------- config (env) ----------
PROXY = os.environ.get("PANDAPROXY_URL", "http://redpanda:8082")
OWM_KEY = os.environ["OWM_API_KEY"]
FIRMS_KEY = os.environ["FIRMS_MAP_KEY"]

# Area of interest: west,south,east,north (FIRMS order). Pick a region that is
# actually burning (check the FIRMS map first); Denmark will give you ~nothing.
BBOX = [float(x) for x in os.environ.get("BBOX", "-10,35,30,45").split(",")]
GRID_STEP = float(os.environ.get("GRID_STEP_DEG", "5"))  # weather grid + cell ids

OWM_INTERVAL = int(os.environ.get("OWM_INTERVAL_S", "600"))      # 10 min
FIRMS_INTERVAL = int(os.environ.get("FIRMS_INTERVAL_S", "900"))  # 15 min
FIRMS_SOURCES = [s.strip() for s in os.environ.get(
    "FIRMS_SOURCES", "VIIRS_NOAA20_NRT,VIIRS_NOAA21_NRT,VIIRS_SNPP_NRT").split(",") if s.strip()]
FIRMS_DAY_RANGE = int(os.environ.get("FIRMS_DAY_RANGE", "1"))  # each day = 1 transaction
DEDUP_TTL = int(os.environ.get("DEDUP_TTL_S", str(3 * 86400)))
HTTP_TIMEOUT = 30

UA = {"User-Agent": "cep-lab-poller/1.0 (university project)"}


def log(msg):
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}", flush=True)


# ---------- grid ----------
W, S, E, N = BBOX


def cell_id(lat, lon):
    """Stable grid cell id shared by weather and fire events (the CEP join key)."""
    i = math.floor((lon - W) / GRID_STEP)
    j = math.floor((lat - S) / GRID_STEP)
    return f"c{j}_{i}"


def grid_points():
    pts = []
    lat = S + GRID_STEP / 2
    while lat < N:
        lon = W + GRID_STEP / 2
        while lon < E:
            pts.append({"cell": cell_id(lat, lon), "lat": round(lat, 4), "lon": round(lon, 4)})
            lon += GRID_STEP
        lat += GRID_STEP
    return pts


# ---------- HTTP ----------
class RateLimited(Exception):
    pass


def http_get(url):
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        if e.code == 429:
            raise RateLimited() from None
        # Deliberately no URL in the message (it contains the key).
        raise RuntimeError(f"HTTP {e.code}") from None
    except urllib.error.URLError as e:
        raise RuntimeError(f"network error: {e.reason}") from None


def produce(topic, records):
    """records: list of (key, value_dict). Batched POSTs to the Redpanda HTTP proxy."""
    for i in range(0, len(records), 500):
        chunk = records[i:i + 500]
        body = json.dumps({"records": [{"key": k, "value": v} for k, v in chunk]}).encode()
        req = urllib.request.Request(
            f"{PROXY}/topics/{urllib.parse.quote(topic)}",
            data=body,
            method="POST",
            headers={"Content-Type": "application/vnd.kafka.json.v2+json",
                     "Accept": "application/vnd.kafka.v2+json"},
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            resp = json.loads(r.read() or b"{}")
        errs = [o for o in resp.get("offsets", []) if o.get("error_code")]
        if errs:
            raise RuntimeError(f"produce to {topic}: {len(errs)} record errors, first={errs[0]}")


# ---------- dedup ----------
_seen = {}


def first_time(key):
    now = time.time()
    if key in _seen:
        return False
    _seen[key] = now + DEDUP_TTL
    return True


def prune_seen():
    now = time.time()
    for k in [k for k, exp in _seen.items() if exp < now]:
        del _seen[k]


# ---------- OpenWeather ----------
def poll_weather(points):
    out = []
    for p in points:
        q = urllib.parse.urlencode({"lat": p["lat"], "lon": p["lon"], "appid": OWM_KEY, "units": "metric"})
        d = json.loads(http_get(f"https://api.openweathermap.org/data/2.5/weather?{q}"))
        dt = int(d["dt"])  # observation time (unix s) = EVENT time
        if not first_time(f"w|{p['cell']}|{dt}"):
            continue  # same observation as last poll
        main, wind = d.get("main", {}), d.get("wind", {})
        out.append((p["cell"], {
            "type": "weather",
            "cell": p["cell"],
            "lat": p["lat"], "lon": p["lon"],
            "event_time": dt * 1000,
            "ingest_time": int(time.time() * 1000),
            "temp_c": main.get("temp"),
            "humidity_pct": main.get("humidity"),
            "pressure_hpa": main.get("pressure"),
            "wind_ms": wind.get("speed"),
            "wind_deg": wind.get("deg"),
            "gust_ms": wind.get("gust"),
            "clouds_pct": d.get("clouds", {}).get("all"),
            "rain_1h_mm": d.get("rain", {}).get("1h", 0.0),
            "condition": (d.get("weather") or [{}])[0].get("main"),
        }))
        time.sleep(1.1)  # stay far below 60 calls/min
    if out:
        produce("weather.raw", out)
    return len(out)


# ---------- NASA FIRMS ----------
def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return v


def poll_firms():
    area = ",".join(str(x) for x in BBOX)
    total = 0
    for src in FIRMS_SOURCES:
        text = http_get(
            f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{FIRMS_KEY}/{src}/{area}/{FIRMS_DAY_RANGE}")
        if not text.lstrip().startswith("latitude"):
            # FIRMS returns plain-text errors with HTTP 200 (bad key, bad source, quota).
            raise RuntimeError(f"FIRMS {src}: unexpected response: {text[:120]!r}")
        out = []
        for row in csv.DictReader(io.StringIO(text)):
            hhmm = str(row.get("acq_time", "0")).zfill(4)  # HHMM UTC, sometimes unpadded
            ts = datetime.strptime(f"{row['acq_date']} {hhmm}", "%Y-%m-%d %H%M").replace(tzinfo=timezone.utc)
            key = f"f|{row['latitude']}|{row['longitude']}|{row['acq_date']}|{hhmm}|{row.get('satellite')}"
            if not first_time(key):
                continue  # area API returns the whole day again on every poll
            lat, lon = float(row["latitude"]), float(row["longitude"])
            ev = {k: _num(v) for k, v in row.items()}
            ev.update({
                "type": "fire",
                "source": src,
                "cell": cell_id(lat, lon),
                "event_time": int(ts.timestamp() * 1000),
                "ingest_time": int(time.time() * 1000),
            })
            out.append((ev["cell"], ev))
        if out:
            produce("firms.raw", out)
        total += len(out)
    return total


# ---------- main loop ----------
def main():
    points = grid_points()
    per_month = len(points) * (86400 / OWM_INTERVAL) * 30
    log(f"bbox={BBOX} grid={len(points)} cells, OpenWeather ~{per_month:,.0f} calls/month "
        f"(free tier: 1M/month, 60/min); FIRMS {len(FIRMS_SOURCES)} sources x {FIRMS_DAY_RANGE} day(s) "
        f"every {FIRMS_INTERVAL}s")
    if per_month > 900_000:
        log("refusing to start: weather grid would blow the free monthly quota; raise GRID_STEP_DEG")
        sys.exit(1)

    next_w = next_f = 0.0
    backoff = {"weather": 0, "firms": 0}
    while True:
        now = time.time()
        if now >= next_w:
            next_w = run_source("weather", "new observations", lambda: poll_weather(points),
                                OWM_INTERVAL, backoff)
        if now >= next_f:
            next_f = run_source("firms", "new detections", poll_firms, FIRMS_INTERVAL, backoff)
        prune_seen()
        time.sleep(max(1.0, min(next_w, next_f) - time.time()))


def run_source(name, what, fn, interval, backoff):
    """One poll of one source. Returns when it is due next. Each source backs off
    on its own, so a broken key or outage on one API never blocks the other."""
    try:
        n = fn()
        log(f"{name}: {n} {what}")
        backoff[name] = 0
        return time.time() + interval
    except RateLimited:
        backoff[name] = min(max(backoff[name] * 2, 60), 1800)
        log(f"{name}: rate limited (429), backing off {backoff[name]}s")
    except Exception as e:  # keep running; logs tell you if it stays broken
        backoff[name] = min(max(backoff[name] * 2, 30), 900)
        log(f"{name}: error: {type(e).__name__}: {e}; retry in {backoff[name]}s")
    return time.time() + backoff[name]


if __name__ == "__main__":
    main()
