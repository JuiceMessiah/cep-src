"""
fwi.py: Canadian Forest Fire Weather Index (FWI) System, computed from weather.

Equations: Van Wagner, C.E. & Pickett, T.L. (1985). Equations and FORTRAN program
for the Canadian Forest Fire Weather Index System. Can. For. Serv., For. Tech. Rep. 33.
System description: Van Wagner, C.E. (1987). Development and structure of the
Canadian Forest Fire Weather Index System. For. Tech. Rep. 35.
Danger classes: EFFIS thresholds (same classes the OpenWeather FWI API returns).

Daily inputs, observed at 12:00 LOCAL STANDARD TIME:
  temp_c, rh_pct, wind_kmh (10 m), rain_mm (24 h ending at noon).
Moisture codes carry over day to day (FFMC ~2/3 day lag, DMC ~12 days, DC ~52 days),
so a cell needs a spin-up period before its values mean much.

Day-length factors are the standard (Canadian, 46 N) monthly tables, valid for
roughly 30 N and further north. Other latitudes are refused rather than silently wrong.

Stdlib only: imported by poller.py (live) and backfill.py (scenarios).
"""
import math
from datetime import date, datetime, timedelta, timezone

START = (85.0, 6.0, 15.0)  # standard start-up values: FFMC, DMC, DC

LE = (6.5, 7.5, 9.0, 12.8, 13.9, 13.9, 12.4, 10.9, 9.4, 8.0, 7.0, 6.0)   # DMC day length
LF = (-1.6, -1.6, -1.6, 0.9, 3.8, 5.8, 6.4, 5.0, 2.4, 0.4, -1.6, -1.6)  # DC day-length factor

# EFFIS fire danger classes: (lower bound, value, description)
CLASSES = ((50.0, 5, "Extreme"), (38.0, 4, "Very high"), (21.3, 3, "High"),
           (11.2, 2, "Moderate"), (5.2, 1, "Low"), (float("-inf"), 0, "Very low"))

MIN_LAT = 30.0


# ---------------- the equations ----------------

def ffmc(ffmc0, temp, rh, wind, rain):
    """Fine Fuel Moisture Code (eq. 1-10)."""
    mo = 147.2 * (101.0 - ffmc0) / (59.5 + ffmc0)
    if rain > 0.5:
        rf = rain - 0.5
        delta = 42.5 * rf * math.exp(-100.0 / (251.0 - mo)) * (1.0 - math.exp(-6.93 / rf))
        if mo > 150.0:
            delta += 0.0015 * (mo - 150.0) ** 2 * math.sqrt(rf)
        mo = min(mo + delta, 250.0)
    ed = 0.942 * rh ** 0.679 + 11.0 * math.exp((rh - 100.0) / 10.0) \
        + 0.18 * (21.1 - temp) * (1.0 - math.exp(-0.115 * rh))
    if mo > ed:  # drying
        ko = 0.424 * (1.0 - (rh / 100.0) ** 1.7) + 0.0694 * math.sqrt(wind) * (1.0 - (rh / 100.0) ** 8)
        kd = ko * 0.581 * math.exp(0.0365 * temp)
        m = ed + (mo - ed) * 10.0 ** (-kd)
    else:
        ew = 0.618 * rh ** 0.753 + 10.0 * math.exp((rh - 100.0) / 10.0) \
            + 0.18 * (21.1 - temp) * (1.0 - math.exp(-0.115 * rh))
        if mo < ew:  # wetting
            k1 = 0.424 * (1.0 - ((100.0 - rh) / 100.0) ** 1.7) \
                + 0.0694 * math.sqrt(wind) * (1.0 - ((100.0 - rh) / 100.0) ** 8)
            kw = k1 * 0.581 * math.exp(0.0365 * temp)
            m = ew - (ew - mo) * 10.0 ** (-kw)
        else:
            m = mo
    return min(max(59.5 * (250.0 - m) / (147.2 + m), 0.0), 101.0)


def dmc(dmc0, temp, rh, rain, month):
    """Duff Moisture Code (eq. 11-17)."""
    temp = max(temp, -1.1)
    k = 1.894 * (temp + 1.1) * (100.0 - rh) * LE[month - 1] * 1e-6
    pr = dmc0
    if rain > 1.5:
        re = 0.92 * rain - 1.27
        # Van Wagner (1987) form, as in the cffdrs reference implementation. The 1985
        # form exp(5.6348 - P/43.43) is the same curve but differs in the 3rd decimal.
        mo = 20.0 + 280.0 / math.exp(0.023 * dmc0)
        if dmc0 <= 33.0:
            b = 100.0 / (0.5 + 0.3 * dmc0)
        elif dmc0 <= 65.0:
            b = 14.0 - 1.3 * math.log(dmc0)
        else:
            b = 6.2 * math.log(dmc0) - 17.2
        mr = mo + 1000.0 * re / (48.77 + b * re)
        pr = max(43.43 * (5.6348 - math.log(mr - 20.0)), 0.0)  # cffdrs form (not 244.72 - ...)
    return max(pr + 100.0 * k, 0.0)


def dc(dc0, temp, rain, month):
    """Drought Code (eq. 18-23)."""
    temp = max(temp, -2.8)
    v = max(0.36 * (temp + 2.8) + LF[month - 1], 0.0)
    dr = dc0
    if rain > 2.8:
        rd = 0.83 * rain - 1.27
        qr = 800.0 * math.exp(-dc0 / 400.0) + 3.937 * rd
        dr = max(400.0 * math.log(800.0 / qr), 0.0)
    return dr + 0.5 * v


def isi(ffmc_, wind):
    """Initial Spread Index (eq. 24-26)."""
    m = 147.2 * (101.0 - ffmc_) / (59.5 + ffmc_)
    ff = 91.9 * math.exp(-0.1386 * m) * (1.0 + m ** 5.31 / 4.93e7)
    return 0.208 * math.exp(0.05039 * wind) * ff


def bui(dmc_, dc_):
    """Buildup Index (eq. 27a/b)."""
    if dmc_ == 0.0 and dc_ == 0.0:
        return 0.0
    if dmc_ <= 0.4 * dc_:
        u = 0.8 * dmc_ * dc_ / (dmc_ + 0.4 * dc_)
    else:
        u = dmc_ - (1.0 - 0.8 * dc_ / (dmc_ + 0.4 * dc_)) * (0.92 + (0.0114 * dmc_) ** 1.7)
    return max(u, 0.0)


def fwi(isi_, bui_):
    """Fire Weather Index (eq. 28-30)."""
    fd = 0.626 * bui_ ** 0.809 + 2.0 if bui_ <= 80.0 else 1000.0 / (25.0 + 108.64 * math.exp(-0.023 * bui_))
    b = 0.1 * isi_ * fd
    return math.exp(2.72 * (0.434 * math.log(b)) ** 0.647) if b > 1.0 else b


def dsr(fwi_):
    """Daily Severity Rating (eq. 31)."""
    return 0.0272 * fwi_ ** 1.77


def danger(fwi_):
    for lo, value, desc in CLASSES:
        if fwi_ >= lo:
            return value, desc
    raise AssertionError("unreachable")


def step(codes, temp, rh, wind, rain, month):
    """One day. codes = (ffmc, dmc, dc) of yesterday. Returns (new codes, all indices)."""
    rh = min(max(rh, 0.0), 100.0)
    wind, rain = max(wind, 0.0), max(rain, 0.0)
    f = ffmc(codes[0], temp, rh, wind, rain)
    d = dmc(codes[1], temp, rh, rain, month)
    c = dc(codes[2], temp, rain, month)
    i = isi(f, wind)
    u = bui(d, c)
    w = fwi(i, u)
    return (f, d, c), {"ffmc": f, "dmc": d, "dc": c, "isi": i, "bui": u, "fwi": w, "dsr": dsr(w)}


# ---------------- time handling ----------------

def lst_offset_h(lon):
    """Local STANDARD time offset from longitude (15 deg per hour). No DST, by definition."""
    return round(lon / 15.0)


def noon_utc(day, lon):
    """Epoch seconds of 12:00 local standard time on `day` (a date in LST)."""
    return int(datetime(day.year, day.month, day.day, 12, tzinfo=timezone.utc).timestamp()) \
        - lst_offset_h(lon) * 3600


def lst_day(t_s, lon):
    return datetime.fromtimestamp(t_s + lst_offset_h(lon) * 3600, tz=timezone.utc).date()


# ---------------- per-cell daily tracker ----------------

class Tracker:
    """
    Keeps hourly weather buckets for one grid cell and produces one FWI result per
    day at local noon. Feed it hourly history (spin-up) with seed(), then live
    observations with observe(). Hour buckets: epoch seconds rounded to the hour;
    rain in a bucket = precipitation of the hour ending there (Open-Meteo convention;
    for OpenWeather the max 'rain.1h' reading seen in that bucket).
    """
    NOON_TOLERANCE_S = 90 * 60

    def __init__(self, cell, lat, lon):
        if lat < MIN_LAT:
            raise ValueError(f"{cell}: lat {lat} < {MIN_LAT}; day-length tables not valid there")
        self.cell, self.lat, self.lon = cell, lat, lon
        self.codes = START
        self.last_day = None   # last LST date computed
        self.spinup_days = 0
        self.hours = {}        # bucket_t -> dict(temp, rh, wind, rain, src)

    # -- input --
    def _put(self, t_s, temp, rh, wind_kmh, rain_mm, src):
        b = int(round(t_s / 3600.0)) * 3600
        old = self.hours.get(b)
        if old and old["src"] != "seed" and src == "seed":
            return  # live observations beat history for the same hour
        rain = rain_mm or 0.0
        if old and old["src"] == src:
            rain = max(rain, old["rain"])
        self.hours[b] = {"temp": temp, "rh": rh, "wind": wind_kmh, "rain": rain, "src": src}

    def seed(self, rows):
        """rows: iterable of (t_s, temp_c, rh_pct, wind_kmh, precip_mm), hourly. Computes every
        complete day in the history; returns the results (newest last)."""
        for r in rows:
            if None in r[:4]:
                continue
            self._put(r[0], r[1], r[2], r[3], r[4], "seed")
        out = self._advance()
        self.spinup_days = len(out)
        return out

    def observe(self, t_s, temp_c, rh_pct, wind_kmh, rain_1h_mm):
        """Live observation. Returns results for any day whose noon has now passed."""
        if None in (temp_c, rh_pct, wind_kmh):
            return []
        self._put(t_s, temp_c, rh_pct, wind_kmh, rain_1h_mm, "live")
        return self._advance()

    # -- computation --
    def _advance(self):
        if not self.hours:
            return []
        newest = max(self.hours)
        first = lst_day(min(self.hours), self.lon) if self.last_day is None else self.last_day + timedelta(days=1)
        out = []
        day = first
        while True:
            noon = noon_utc(day, self.lon)
            if noon > newest:
                break
            res = self._day(day, noon)
            if res is not None:
                out.append(res)
            day += timedelta(days=1)
        self._prune(newest)
        return out

    def _day(self, day, noon):
        near = [b for b in self.hours if abs(b - noon) <= self.NOON_TOLERANCE_S]
        rain_hours = [b for b in self.hours if noon - 86400 < b <= noon]
        if not near or (self.last_day is None and len(rain_hours) < 20):
            return None  # no noon obs, or the very first day lacks a full 24 h of rain data
        obs = self.hours[min(near, key=lambda b: abs(b - noon))]
        rain24 = sum(self.hours[b]["rain"] for b in rain_hours)
        gap = self.last_day is not None and (day - self.last_day).days > 1
        self.codes, idx = step(self.codes, obs["temp"], obs["rh"], obs["wind"], rain24, day.month)
        self.last_day = day
        return {"day": day, "noon_t": noon, "indices": idx, "src": obs["src"], "gap": gap,
                "inputs": {"temp_c": obs["temp"], "rh_pct": obs["rh"], "wind_kmh": obs["wind"],
                           "rain_24h_mm": round(rain24, 2), "rain_hours": len(rain_hours)}}

    def _prune(self, newest):
        for b in [b for b in self.hours if b < newest - 30 * 3600]:
            del self.hours[b]


def event(cell, lat, lon, res, source, spinup_days=None, replay=False):
    """The fwi event (topic fwi.raw / fwi.replay). Field names mirror the OpenWeather
    FWI API (fwi, danger_rating) so a real API response maps 1:1 if access is granted."""
    idx = res["indices"]
    value, desc = danger(idx["fwi"])
    ev = {
        "type": "fwi",
        "cell": cell, "lat": lat, "lon": lon,
        "event_time": res["noon_t"] * 1000,   # noon LST of the day the value is valid for
        "day": res["day"].isoformat(),
        "fwi": round(idx["fwi"], 2),
        "danger_class": value,
        "danger_rating": {"value": value, "description": desc},
        "components": {k: round(v, 2) for k, v in idx.items() if k != "fwi"},
        "inputs": res["inputs"],
        "inputs_source": res["src"],
        "source": source,
    }
    if spinup_days is not None:
        ev["spinup_days"] = spinup_days
    if res.get("gap"):
        ev["gap_before"] = True
    if replay:
        ev["replay"] = True
    return ev


if __name__ == "__main__":  # quick manual check: python3 fwi.py
    print(step(START, 17.0, 42.0, 25.0, 0.0, 4))
    print(date.today(), danger(15.0))
