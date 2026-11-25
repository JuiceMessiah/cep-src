"""
FWI tests: equations against the cffdrs reference values, the daily tracker,
the poller wiring (fake Open-Meteo + fake Redpanda) and grid consistency.

  python3 tests/test_fwi.py
"""
import json
import math
import os
import random
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("OWM_API_KEY", "test")
os.environ.setdefault("FIRMS_MAP_KEY", "test")
os.environ["WEATHER_BBOX"] = "9.5,55,10.5,55.5"

import backfill  # noqa: E402
import fwi  # noqa: E402
import poller  # noqa: E402

OK = []


def check(name, cond, detail=""):
    if not cond:
        raise SystemExit(f"FAIL {name} {detail}")
    OK.append(name)


# 1. Reference: cffdrs (R) fwi() on test_fwi rows 1-3, each from start values 85/6/15,
#    standard day-length tables (lat.adjust=FALSE), April. Van Wagner & Pickett (1985) data.
REF = [
    ((17.0, 42, 25, 0.0), dict(ffmc=87.69298, dmc=8.545051, dc=19.014, isi=10.853661,
                               bui=8.490427, fwi=10.096371, dsr=1.6290766)),
    ((20.0, 21, 25, 2.4), dict(ffmc=85.93646, dmc=8.208782, dc=19.554, isi=8.459204,
                               bui=8.186131, fwi=8.021921, dsr=1.0842804)),
    ((8.5, 40, 17, 0.0), dict(ffmc=86.55884, dmc=7.396408, dc=17.484, isi=6.170985,
                              bui=7.370238, fwi=5.704064, dsr=0.5929462)),
]
for n, (inp, want) in enumerate(REF, 1):
    _, got = fwi.step(fwi.START, *inp, 4)
    for k, v in want.items():
        check(f"ref row {n} {k}", abs(got[k] - v) < 1e-5, f"got {got[k]:.6f} want {v}")

# Heavy rain exercises every rain branch (FFMC mo>150, DMC b ranges, DC) without blowing up.
codes = (92.0, 70.0, 400.0)
for rain in (0.6, 3.0, 25.0, 80.0):
    c2, idx = fwi.step(codes, 15.0, 90, 10, rain, 8)
    dry, _ = fwi.step(codes, 15.0, 90, 10, 0.0, 8)
    check(f"rain {rain} lowers FFMC", c2[0] < dry[0] and idx["fwi"] >= 0, (c2, dry))
    check(f"rain {rain} DMC", (c2[1] < dry[1]) == (rain > 1.5), (c2, dry))   # DMC threshold 1.5 mm
    check(f"rain {rain} DC", (c2[2] < dry[2]) == (rain > 2.8), (c2, dry))    # DC threshold 2.8 mm

# 2. Danger classes (EFFIS thresholds; lower bound inclusive).
for v, cls in [(0, 0), (5.19, 0), (5.2, 1), (11.19, 1), (11.2, 2), (21.3, 3), (37.99, 3),
               (38.0, 4), (50.0, 5), (120, 5)]:
    check(f"danger {v}", fwi.danger(v)[0] == cls, fwi.danger(v))

# 3. Noon local standard time from longitude.
d = date(2026, 8, 12)
for lon, hh in [(10.0, 11), (-7.0, 12), (23.5, 10), (-120.0, 20)]:
    t = datetime.fromtimestamp(fwi.noon_utc(d, lon), tz=timezone.utc)
    check(f"noon lon {lon}", t.hour == hh and t.day == 12, t)

# 4. Tracker: seeded history == manual chain of step().
LON, LAT = 10.0, 55.1
t0 = int(datetime(2026, 8, 10, tzinfo=timezone.utc).timestamp())
rows = []
for h in range(5 * 24):  # 5 days hourly, rain 1 mm/h for 6 h on day 3 morning
    t = t0 + h * 3600
    rain = 1.0 if 2 * 24 + 4 <= h < 2 * 24 + 10 else 0.0
    rows.append((t, 20.0 + (h % 24) / 6, 30.0, 15.0, rain))
tr = fwi.Tracker("cX", LAT, LON)
res = tr.seed([r for r in rows if r[0] <= t0 + 4 * 86400 + 9 * 3600])  # up to day 5, 09:00 UTC
check("seed days", [r["day"].day for r in res] == [11, 12, 13], [r["day"] for r in res])
manual = fwi.START
for r in res:
    noon = fwi.noon_utc(r["day"], LON)
    obs = next(x for x in rows if x[0] == noon)
    rain24 = sum(x[4] for x in rows if noon - 86400 < x[0] <= noon)
    manual, idx = fwi.step(manual, obs[1], obs[2], obs[3], rain24, 8)
    check(f"chain {r['day']}", abs(idx["fwi"] - r["indices"]["fwi"]) < 1e-9
          and abs(rain24 - r["inputs"]["rain_24h_mm"]) < 0.01)
check("rain day 12 = 6 mm", res[1]["inputs"]["rain_24h_mm"] == 6.0, res[1]["inputs"])
check("codes carried", tr.codes == manual)

# 5. Live observations take over: hotter readings every 30 min until after noon of day 5.
out = []
t = t0 + 4 * 86400 + 9 * 3600 + 1800
while t <= t0 + 4 * 86400 + 12 * 3600:
    out += tr.observe(t, 30.0, 20.0, 20.0, 0.0)
    t += 1800
check("one live day", len(out) == 1 and out[0]["day"] == date(2026, 8, 14), out)
check("live inputs used", out[0]["src"] == "live" and out[0]["inputs"]["temp_c"] == 30.0, out[0])
check("no double compute", tr.observe(t + 1800, 30.0, 20.0, 20.0, 0.0) == [])

ev = fwi.event("cX", LAT, LON, out[0], "computed")
check("event shape", ev["type"] == "fwi" and ev["event_time"] == fwi.noon_utc(date(2026, 8, 14), LON) * 1000
      and ev["danger_rating"]["value"] == ev["danger_class"] == fwi.danger(ev["fwi"])[0], ev)
json.dumps(ev)  # serialisable

try:
    fwi.Tracker("cS", 12.0, 10.0)
    check("low latitude refused", False)
except ValueError:
    OK.append("low latitude refused")

# 6. Grid: poller and backfill agree, globally; grid_points covers the bbox.
rnd = random.Random(1)
g = backfill.Grid(0.25)
for _ in range(2000):
    lat, lon = rnd.uniform(-89.9, 89.9), rnd.uniform(-179.9, 179.9)
    c = poller.cell_id(lat, lon, 0.25)
    check("grid agree", c == g.cell(lat, lon), (lat, lon))
    clat, clon = g.center(c)
    check("centre inside cell", poller.cell_id(clat, clon, 0.25) == c and poller.cell_center(c, 0.25) == (clat, clon))
pts = poller.grid_points([9.5, 55.0, 10.5, 55.5], 0.25)
check("grid_points count", len(pts) == 4 * 2, len(pts))

# 7. Poller wiring: spin-up from fake Open-Meteo, then live noon via poll path.
now = datetime.now(timezone.utc).timestamp()
produced = []
poller.produce = lambda topic, recs: produced.append((topic, recs))


def fake_get(url):
    assert "open-meteo" in url and "appid" not in url
    n = url.count("%2C") + 1 if "latitude=" in url else 1  # comma-separated coords
    start = int(now // 3600) * 3600 - 10 * 86400
    hourly = {"time": [], "temperature_2m": [], "relative_humidity_2m": [], "wind_speed_10m": [],
              "precipitation": []}
    for h in range(10 * 24 + 24):  # includes "forecast" hours after now, which must be ignored
        hourly["time"].append(start + h * 3600)
        hourly["temperature_2m"].append(25.0)
        hourly["relative_humidity_2m"].append(25.0)
        hourly["wind_speed_10m"].append(18.0)
        hourly["precipitation"].append(None if h % 50 == 0 else 0.0)
    return json.dumps([{"hourly": hourly} for _ in range(n)] if n > 1 else {"hourly": hourly})


poller.http_get = fake_get
poller.time.sleep = lambda s: None
cells = poller.grid_points()
poller.fwi_spinup(cells)
fwi_out = [r for t_, recs in produced if t_ == "fwi.raw" for r in recs]
check("spin-up one value per cell", len(fwi_out) == len(cells) and all(v["spinup_days"] >= 8 for _, v in fwi_out),
      [(k, v.get("spinup_days")) for k, v in fwi_out][:3])
last_noon = max(v["event_time"] for _, v in fwi_out) / 1000
check("spin-up used history only", last_noon <= now)
tr0 = poller._trackers[cells[0]["cell"]]
next_noon = fwi.noon_utc(tr0.last_day.fromordinal(tr0.last_day.toordinal() + 1), cells[0]["lon"])
produced.clear()
poller.fwi_from_observations([{
    "cell": cells[0]["cell"], "lat": cells[0]["lat"], "lon": cells[0]["lon"],
    "event_time": (next_noon + 600) * 1000, "temp_c": 28.0, "humidity_pct": 22, "wind_ms": 5.0,
    "rain_1h_mm": 0.0}])
live = [v for t_, recs in produced if t_ == "fwi.raw" for _, v in recs]
check("live noon event", len(live) == 1 and live[0]["inputs"]["wind_kmh"] == 18.0
      and live[0]["inputs_source"] == "live", live)

print(f"OK: {len(OK)} checks")
