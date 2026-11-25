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
  fwi.raw      key = grid cell id       value = daily Fire Weather Index (see fwi.py)

Grid: GLOBAL, origin 90S/180W, GRID_STEP_DEG (0.25). Cell ids are the same in the
poller and in backfill scenarios, whatever bbox either uses.
FIRMS covers FIRMS_BBOX (cheap, one call per source); weather and FWI only cover
WEATHER_BBOX (one OpenWeather call per cell per poll, so keep it small).
FWI: moisture codes are spun up at start from Open-Meteo history (free, no key),
then updated once a day at local noon from our own OpenWeather observations.
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

import fwi  # fwi.py next to this file

# ---------- config (env) ----------
PROXY = os.environ.get("PANDAPROXY_URL", "http://redpanda:8082")
OWM_KEY = os.environ["OWM_API_KEY"]
FIRMS_KEY = os.environ["FIRMS_MAP_KEY"]

# Areas: west,south,east,north (FIRMS order). Pick a region that is actually
# burning (check the FIRMS map first); Denmark in November gives you ~nothing.
_bbox = os.environ.get("BBOX", "-10,35,30,45")  # legacy single setting, fallback for both
FIRMS_BBOX = [float(x) for x in os.environ.get("FIRMS_BBOX", _bbox).split(",")]
WEATHER_BBOX = [float(x) for x in os.environ.get("WEATHER_BBOX", _bbox).split(",")]
GRID_STEP = float(os.environ.get("GRID_STEP_DEG", "0.25"))  # cell ids (CEP join key)

FWI_ENABLED = os.environ.get("FWI_ENABLED", "true").lower() == "true"
FWI_SPINUP_DAYS = min(int(os.environ.get("FWI_SPINUP_DAYS", "30")), 92)  # Open-Meteo past_days max

OWM_INTERVAL = int(os.environ.get("OWM_INTERVAL_S", "1800"))     # 30 min
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
def cell_id(lat, lon, step=None):
    """Global grid cell id shared by weather, fire and fwi events (the CEP join key).
    Keep identical to backfill.Grid.cell (tests/test_fwi.py checks)."""
    step = step or GRID_STEP
    return f"c{math.floor((lat + 90) / step)}_{math.floor((lon + 180) / step)}"


def cell_center(cell, step=None):
    step = step or GRID_STEP
    j, i = (int(x) for x in cell[1:].split("_"))
    return round(-90 + (j + 0.5) * step, 4), round(-180 + (i + 0.5) * step, 4)


def grid_points(bbox=None, step=None):
    """Centres of every global cell that overlaps the bbox."""
    w, s, e, n = bbox or WEATHER_BBOX
    step = step or GRID_STEP
    j0, j1 = math.floor((s + 90) / step), math.ceil((n + 90) / step)
    i0, i1 = math.floor((w + 180) / step), math.ceil((e + 180) / step)
    pts = []
    for j in range(j0, j1):
        for i in range(i0, i1):
            c = f"c{j}_{i}"
            lat, lon = cell_center(c, step)
            pts.append({"cell": c, "lat": lat, "lon": lon})
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
        rain = d.get("rain", {})
        rain_1h = rain.get("1h", rain["3h"] / 3 if "3h" in rain else 0.0)
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
            "rain_1h_mm": rain_1h,
            "condition": (d.get("weather") or [{}])[0].get("main"),
        }))
        time.sleep(1.1)  # stay far below 60 calls/min
    if out:
        produce("weather.raw", out)
    fwi_from_observations([v for _, v in out])
    return len(out)


# ---------- Fire Weather Index (computed, see fwi.py) ----------
_trackers = {}


def openmeteo_history(points, days):
    """Hourly history (UTC) for many cells, batched. Returns {cell: [(t, temp, rh, wind_kmh, mm)]}."""
    out = {}
    now = time.time()
    for k in range(0, len(points), 25):
        batch = points[k:k + 25]
        q = urllib.parse.urlencode({
            "latitude": ",".join(str(p["lat"]) for p in batch),
            "longitude": ",".join(str(p["lon"]) for p in batch),
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
            "past_days": days, "forecast_days": 1, "wind_speed_unit": "kmh",
            "timezone": "GMT", "timeformat": "unixtime",
        })
        d = json.loads(http_get(f"https://api.open-meteo.com/v1/forecast?{q}"))
        for p, loc in zip(batch, d if isinstance(d, list) else [d]):
            h = loc.get("hourly", {})
            out[p["cell"]] = [
                (t, h["temperature_2m"][i], h["relative_humidity_2m"][i],
                 h["wind_speed_10m"][i], h["precipitation"][i] or 0.0)
                for i, t in enumerate(h.get("time", [])) if t <= now  # history only, no forecast
            ]
        time.sleep(1)
    return out


def fwi_spinup(points):
    if not FWI_ENABLED:
        log("fwi: disabled")
        return
    for p in points:
        try:
            _trackers[p["cell"]] = fwi.Tracker(p["cell"], p["lat"], p["lon"])
        except ValueError as e:
            log(f"fwi: skipping cell: {e}")
    todo = [p for p in points if p["cell"] in _trackers]
    try:
        hist = openmeteo_history(todo, FWI_SPINUP_DAYS)
    except Exception as e:  # FWI still works without it, just from start-up values
        log(f"fwi: spin-up failed ({type(e).__name__}: {e}); starting from standard codes 85/6/15")
        return
    latest = []
    for p in todo:
        tr = _trackers[p["cell"]]
        res = tr.seed(hist.get(p["cell"], []))
        if res:
            latest.append((p["cell"], fwi.event(p["cell"], p["lat"], p["lon"], res[-1], "computed",
                                                spinup_days=tr.spinup_days)))
    if latest:
        try:
            produce("fwi.raw", latest)
        except Exception as e:  # don't crash-loop (and re-download history) over a missing topic
            log(f"fwi: produce failed ({type(e).__name__}: {e}); does topic fwi.raw exist?")
    classes = sorted(v["danger_class"] for _, v in latest)
    log(f"fwi: spun up {len(latest)}/{len(todo)} cells over {FWI_SPINUP_DAYS} days; "
        f"danger classes now: {classes[:1] + classes[-1:] if classes else 'n/a'} (min/max)")


def fwi_from_observations(weather_events):
    out = []
    for w in weather_events:
        tr = _trackers.get(w["cell"])
        if tr is None or w["wind_ms"] is None:
            continue
        for res in tr.observe(w["event_time"] / 1000, w["temp_c"], w["humidity_pct"],
                              w["wind_ms"] * 3.6, w["rain_1h_mm"]):
            out.append((w["cell"], fwi.event(w["cell"], w["lat"], w["lon"], res, "computed")))
    if out:
        try:
            produce("fwi.raw", out)
            log(f"fwi: {len(out)} new daily value(s)")
        except Exception as e:  # weather already went out; don't make that source back off
            log(f"fwi: produce failed, {len(out)} daily value(s) lost ({type(e).__name__}: {e})")


# ---------- NASA FIRMS ----------
def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return v


def poll_firms():
    area = ",".join(str(x) for x in FIRMS_BBOX)
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
    log(f"firms bbox={FIRMS_BBOX}; weather bbox={WEATHER_BBOX} grid={GRID_STEP} deg "
        f"-> {len(points)} cells, OpenWeather ~{per_month:,.0f} calls/month "
        f"(free tier: 1M/month, 60/min); FIRMS {len(FIRMS_SOURCES)} sources x {FIRMS_DAY_RANGE} day(s) "
        f"every {FIRMS_INTERVAL}s")
    if per_month > 900_000:
        log("refusing to start: weather grid would blow the free monthly quota; "
            "shrink WEATHER_BBOX or raise OWM_INTERVAL_S")
        sys.exit(1)
    fwi_spinup(points)

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
