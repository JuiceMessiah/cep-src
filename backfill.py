#!/usr/bin/env python3
"""
backfill.py: build a replayable scenario from REAL historical data.

  NASA FIRMS detections (area API with a date)
+ hourly weather for every grid cell that had a detection (+ optional neighbours)
+ daily Fire Weather Index for the same cells (fwi.py, spun up from Open-Meteo history)
-> scenarios/<name>.jsonl, sorted by event_time.

Event schema is identical to poller.py (plus "replay": true and "provider"),
so a Flink job can't tell replayed from live events except by topic.

Stdlib only. Run it on a laptop with internet access and YOUR OWN keys.
Keys come from the environment and never appear in output or error messages.

Examples (fish-safe; note --bbox=... with '=' because it starts with '-'):

  env FIRMS_MAP_KEY=... python3 backfill.py --check
  env FIRMS_MAP_KEY=... python3 backfill.py --name iberia-aug --bbox=-9.5,37,-6,40 \
      --start 2026-08-10 --days 3 --sources VIIRS_NOAA20_SP,VIIRS_SNPP_SP

Weather providers (--weather):
  openmeteo  free, no key. Archive (reanalysis) API: the most recent days lag behind;
             hours without data are skipped and reported.
  owm        OpenWeather One Call "timemachine". Needs OWM_API_KEY with a One Call
             subscription (card on file; 1,000 free calls/day). One call per cell per hour,
             so it refuses to run above --owm-budget.

GRID: global, origin 90S/180W, size --grid (default 0.25), exactly like poller.py.
Live and scenario cell ids are comparable as long as the grid size matches.

FWI: for each weather cell, hourly Open-Meteo archive data from --fwi-spinup-days before
--start is run through fwi.Tracker; one fwi event per day (at local noon) inside the
scenario window goes into the file (stream "fwi" -> topic fwi.replay).
"""
import argparse
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
from datetime import datetime, timedelta, timezone

import fwi  # fwi.py next to this file

UA = {"User-Agent": "cep-lab-backfill/1.0 (university project)"}
FIRMS = "https://firms.modaps.eosdis.nasa.gov"


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def http_get(url, retries=3):
    """GET with small retry/backoff. Error messages never contain the URL (keys!)."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code in (429, 502, 503, 504) and attempt < retries - 1:
                time.sleep(30 * (attempt + 1))
                continue
            raise RuntimeError(f"HTTP {e.code}") from None
        except urllib.error.URLError as e:
            if attempt < retries - 1:
                time.sleep(5)
                continue
            raise RuntimeError(f"network error: {e.reason}") from None
    raise RuntimeError("unreachable")


class Grid:
    """Same cell math as poller.cell_id: global grid, c{row}_{col}, origin 90S/180W."""

    def __init__(self, step):
        self.step = step

    def cell(self, lat, lon):
        return f"c{math.floor((lat + 90) / self.step)}_{math.floor((lon + 180) / self.step)}"

    def center(self, cell):
        j, i = (int(x) for x in cell[1:].split("_"))
        return round(-90 + (j + 0.5) * self.step, 4), round(-180 + (i + 0.5) * self.step, 4)

    def neighbours(self, cell, radius):
        j, i = (int(x) for x in cell[1:].split("_"))
        return {f"c{j + dj}_{i + di}" for dj in range(-radius, radius + 1) for di in range(-radius, radius + 1)}


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return v


# ---------------- FIRMS ----------------

def firms_check(key):
    text = http_get(f"{FIRMS}/api/data_availability/csv/{key}/ALL")
    print(text)


def fetch_firms(key, sources, bbox, start, days, grid):
    area = ",".join(str(x) for x in bbox)
    events, seen = [], set()
    for src in sources:
        for d in range(days):
            day = (start + timedelta(days=d)).strftime("%Y-%m-%d")
            text = http_get(f"{FIRMS}/api/area/csv/{key}/{src}/{area}/1/{day}")
            if not text.lstrip().startswith("latitude"):
                # FIRMS returns plain-text errors with HTTP 200 (bad key/source/date, quota).
                raise RuntimeError(f"FIRMS {src} {day}: unexpected response: {text[:120]!r}")
            n = 0
            for row in csv.DictReader(io.StringIO(text)):
                hhmm = str(row.get("acq_time", "0")).zfill(4)
                ts = datetime.strptime(f"{row['acq_date']} {hhmm}", "%Y-%m-%d %H%M").replace(tzinfo=timezone.utc)
                dedup = (row["latitude"], row["longitude"], row["acq_date"], hhmm, row.get("satellite"))
                if dedup in seen:
                    continue
                seen.add(dedup)
                lat, lon = float(row["latitude"]), float(row["longitude"])
                ev = {k: _num(v) for k, v in row.items()}
                ev.update({
                    "type": "fire",
                    "source": src,
                    "cell": grid.cell(lat, lon),
                    "event_time": int(ts.timestamp() * 1000),
                    "replay": True,
                })
                events.append(ev)
                n += 1
            log(f"FIRMS {src} {day}: {n} detections")
            time.sleep(1)
    return events


# ---------------- weather ----------------

def weather_event(cell, lat, lon, t_s, provider, **v):
    return {
        "type": "weather",
        "cell": cell,
        "lat": lat, "lon": lon,
        "event_time": int(t_s) * 1000,
        "temp_c": v.get("temp_c"),
        "humidity_pct": v.get("humidity_pct"),
        "pressure_hpa": v.get("pressure_hpa"),
        "wind_ms": v.get("wind_ms"),
        "wind_deg": v.get("wind_deg"),
        "gust_ms": v.get("gust_ms"),
        "clouds_pct": v.get("clouds_pct"),
        "rain_1h_mm": v.get("rain_1h_mm") or 0.0,
        "condition": v.get("condition"),
        "provider": provider,
        "replay": True,
    }


def weather_openmeteo(cells, grid, t_from, t_to):
    events, missing = [], 0
    hourly = ("temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m,"
              "wind_direction_10m,wind_gusts_10m,cloud_cover,precipitation")
    for cell in sorted(cells):
        lat, lon = grid.center(cell)
        q = urllib.parse.urlencode({
            "latitude": lat, "longitude": lon,
            "start_date": t_from.strftime("%Y-%m-%d"), "end_date": t_to.strftime("%Y-%m-%d"),
            "hourly": hourly, "wind_speed_unit": "ms", "timezone": "GMT", "timeformat": "unixtime",
        })
        d = json.loads(http_get(f"https://archive-api.open-meteo.com/v1/archive?{q}"))
        h = d.get("hourly", {})
        lo, hi = t_from.timestamp(), t_to.timestamp()
        for idx, t in enumerate(h.get("time", [])):
            if not (lo <= t < hi):
                continue

            def g(k):
                arr = h.get(k)
                return arr[idx] if arr else None

            if g("temperature_2m") is None:
                missing += 1
                continue
            events.append(weather_event(
                cell, lat, lon, t, "open-meteo",
                temp_c=g("temperature_2m"), humidity_pct=g("relative_humidity_2m"),
                pressure_hpa=g("surface_pressure"), wind_ms=g("wind_speed_10m"),
                wind_deg=g("wind_direction_10m"), gust_ms=g("wind_gusts_10m"),
                clouds_pct=g("cloud_cover"), rain_1h_mm=g("precipitation"),
            ))
        time.sleep(0.5)
    if missing:
        log(f"open-meteo: {missing} cell-hours without data (archive lag?) skipped")
    return events


def fwi_openmeteo(cells, grid, t_from, t_to, spinup_days):
    """Daily FWI events (noon LST) inside [t_from, t_to) for each cell, spun up from history."""
    events, skipped = [], 0
    h_from = t_from - timedelta(days=spinup_days + 1)
    for cell in sorted(cells):
        lat, lon = grid.center(cell)
        try:
            tr = fwi.Tracker(cell, lat, lon)
        except ValueError as e:
            log(f"fwi: {e}")
            skipped += 1
            continue
        q = urllib.parse.urlencode({
            "latitude": lat, "longitude": lon,
            "start_date": h_from.strftime("%Y-%m-%d"), "end_date": t_to.strftime("%Y-%m-%d"),
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
            "wind_speed_unit": "kmh", "timezone": "GMT", "timeformat": "unixtime",
        })
        h = json.loads(http_get(f"https://archive-api.open-meteo.com/v1/archive?{q}")).get("hourly", {})
        rows = [(t, h["temperature_2m"][i], h["relative_humidity_2m"][i], h["wind_speed_10m"][i],
                 h["precipitation"][i] or 0.0) for i, t in enumerate(h.get("time", []))]
        results = tr.seed(rows)
        prior = [r for r in results if r["noon_t"] < t_from.timestamp()]
        before = len(prior)
        if prior:  # the value in force when the scenario starts, stamped at the start
            ev = fwi.event(cell, lat, lon, prior[-1], "computed", spinup_days=before - 1, replay=True)
            ev["event_time"], ev["carried_over"] = int(t_from.timestamp() * 1000), True
            events.append(ev)
        for r in results:
            if t_from.timestamp() <= r["noon_t"] < t_to.timestamp():
                events.append(fwi.event(cell, lat, lon, r, "computed", spinup_days=before, replay=True))
        time.sleep(0.5)
    if skipped:
        log(f"fwi: {skipped} cell(s) skipped (latitude)")
    return events


def weather_owm(cells, grid, t_from, t_to, key, budget):
    hours = int((t_to - t_from).total_seconds() // 3600)
    calls = len(cells) * hours
    if calls > budget:
        sys.exit(f"refusing: {calls} One Call requests > --owm-budget {budget} "
                 f"(free tier: 1,000/day). Fewer cells/days, or --weather openmeteo.")
    log(f"owm: {calls} One Call timemachine requests")
    events = []
    for cell in sorted(cells):
        lat, lon = grid.center(cell)
        for hr in range(hours):
            dt = int((t_from + timedelta(hours=hr)).timestamp())
            q = urllib.parse.urlencode({"lat": lat, "lon": lon, "dt": dt, "appid": key, "units": "metric"})
            d = json.loads(http_get(f"https://api.openweathermap.org/data/3.0/onecall/timemachine?{q}"))
            for it in d.get("data", []):
                events.append(weather_event(
                    cell, lat, lon, it["dt"], "openweather-onecall",
                    temp_c=it.get("temp"), humidity_pct=it.get("humidity"),
                    pressure_hpa=it.get("pressure"), wind_ms=it.get("wind_speed"),
                    wind_deg=it.get("wind_deg"), gust_ms=it.get("wind_gust"),
                    clouds_pct=it.get("clouds"), rain_1h_mm=(it.get("rain") or {}).get("1h"),
                    condition=(it.get("weather") or [{}])[0].get("main"),
                ))
            time.sleep(1.1)  # stay far below 60/min
    return events


# ---------------- main ----------------

def main():
    ap = argparse.ArgumentParser(description="Build a replayable CEP scenario from historical data.")
    ap.add_argument("--check", action="store_true", help="print FIRMS data availability and exit")
    ap.add_argument("--name", help="scenario name -> scenarios/<name>.jsonl")
    ap.add_argument("--bbox", help="west,south,east,north (use --bbox=-9.5,37,-6,40)")
    ap.add_argument("--start", help="first day, YYYY-MM-DD (UTC)")
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--grid", type=float, default=0.25, help="cell size in degrees (default 0.25)")
    ap.add_argument("--sources", default="VIIRS_NOAA20_NRT,VIIRS_SNPP_NRT",
                    help="FIRMS sources; _NRT for recent dates, _SP for older (see --check)")
    ap.add_argument("--neighbours", type=int, default=1,
                    help="also fetch weather for cells within N rings of a fire cell (default 1)")
    ap.add_argument("--lead-hours", type=int, default=12,
                    help="weather starts this many hours before --start (default 12)")
    ap.add_argument("--weather", choices=["openmeteo", "owm", "none"], default="openmeteo")
    ap.add_argument("--owm-budget", type=int, default=900)
    ap.add_argument("--fwi-spinup-days", type=int, default=30,
                    help="days of history before --start used to spin up FWI codes (0 = no FWI)")
    ap.add_argument("--out-dir", default="scenarios")
    a = ap.parse_args()

    firms_key = os.environ.get("FIRMS_MAP_KEY")
    if not firms_key:
        sys.exit("FIRMS_MAP_KEY not set")
    if a.check:
        firms_check(firms_key)
        return
    if not (a.name and a.bbox and a.start):
        sys.exit("--name, --bbox and --start are required (or --check)")

    bbox = [float(x) for x in a.bbox.split(",")]
    if len(bbox) != 4 or bbox[0] >= bbox[2] or bbox[1] >= bbox[3]:
        sys.exit("--bbox must be west,south,east,north")
    grid = Grid(a.grid)
    start = datetime.strptime(a.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=a.days)
    sources = [s.strip() for s in a.sources.split(",") if s.strip()]

    fires = fetch_firms(firms_key, sources, bbox, start, a.days, grid)
    if not fires:
        sys.exit("no FIRMS detections in that bbox/date range: pick another fire (check the FIRMS map)")

    cells = set()
    for c in {f["cell"] for f in fires}:
        cells |= grid.neighbours(c, a.neighbours)
    log(f"{len(fires)} detections in {len({f['cell'] for f in fires})} cells; weather for {len(cells)} cells")

    w_from = start - timedelta(hours=a.lead_hours)
    if a.weather == "openmeteo":
        weather = weather_openmeteo(cells, grid, w_from, end)
    elif a.weather == "owm":
        key = os.environ.get("OWM_API_KEY")
        if not key:
            sys.exit("OWM_API_KEY not set")
        weather = weather_owm(cells, grid, w_from, end, key, a.owm_budget)
    else:
        weather = []

    fwis = fwi_openmeteo(cells, grid, w_from, end, a.fwi_spinup_days) if a.fwi_spinup_days > 0 else []
    if fwis:
        cls = [e["danger_class"] for e in fwis]
        log(f"fwi: {len(fwis)} daily values, danger class {min(cls)}..{max(cls)}")

    records = [{"stream": "firms", "key": f["cell"], "value": f} for f in fires]
    records += [{"stream": "weather", "key": w["cell"], "value": w} for w in weather]
    records += [{"stream": "fwi", "key": e["cell"], "value": e} for e in fwis]
    records.sort(key=lambda r: (r["value"]["event_time"], r["stream"]))

    meta = {
        "name": a.name, "bbox": bbox, "grid_deg": a.grid, "start": a.start, "days": a.days,
        "sources": sources, "weather_provider": a.weather, "neighbours": a.neighbours,
        "lead_hours": a.lead_hours, "fires": len(fires), "weather": len(weather),
        "fwi": len(fwis), "fwi_spinup_days": a.fwi_spinup_days,
        "first_event": records[0]["value"]["event_time"], "last_event": records[-1]["value"]["event_time"],
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    os.makedirs(a.out_dir, exist_ok=True)
    path = os.path.join(a.out_dir, f"{a.name}.jsonl")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"meta": meta}) + "\n")
        for r in records:
            fh.write(json.dumps(r, separators=(",", ":")) + "\n")
    span_h = (meta["last_event"] - meta["first_event"]) / 3_600_000
    log(f"wrote {path}: {len(fires)} fire + {len(weather)} weather + {len(fwis)} fwi events over {span_h:.1f} h")


if __name__ == "__main__":
    main()
