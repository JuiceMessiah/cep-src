package cep.core;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Environment patterns for ONE grid cell (CONTRACT.md §4), in EVENT time.
 * Callers must feed events in event-time order (the Flink wrapper sorts them by watermark).
 *
 * FireAlert:    >= minDetections own detections (confidence not low) within window,
 *               while the cell's latest FWI danger class >= minDanger. Opens an incident,
 *               key inc-{cell}-{yyyyMMdd'T'HHmm of the first detection in the window, UTC}.
 * SceneCleared: incident open and no detection in the cell OR its 8 neighbours for clearAfter
 *               (absence pattern: an event-time timer at lastActivity + clearAfter).
 *
 * Plain fields + public no-arg constructor: Flink stores the whole object as keyed state.
 */
public class FireIncidentLogic implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter KEY_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmm").withZone(ZoneOffset.UTC);

    public static final class Params implements Serializable {
        public int minDetections = 3;
        public long windowMs = 6 * 3_600_000L;
        public int minDanger = 2;              // Moderate
        public long clearAfterMs = 12 * 3_600_000L;
    }

    // latest fire weather for this cell
    public int dangerClass = -1;               // -1 = unknown: no FireAlert without FWI
    public double fwi = Double.NaN;
    // own detections inside the window while no incident is open
    public ArrayList<Long> windowTimes = new ArrayList<>();
    public ArrayList<Double> windowFrp = new ArrayList<>();
    // open incident
    public String incidentKey;                 // null = none
    public long incidentStart;
    public long lastActivity;
    public int incidentDetections;
    public double incidentFrpMax;

    public FireIncidentLogic() {}

    public void onFwi(int dangerClass, double fwi) {
        this.dangerClass = dangerClass;
        this.fwi = fwi;
    }

    /** VIIRS: l/n/h; MODIS: 0-100. Low confidence never counts. */
    public static boolean qualifies(Object confidence) {
        if (confidence == null) return true;
        String c = confidence.toString().trim().toLowerCase();
        if (c.equals("l") || c.equals("low")) return false;
        try {
            return Double.parseDouble(c) >= 30;
        } catch (NumberFormatException e) {
            return true;
        }
    }

    /**
     * A detection in this cell (own = true) or in a neighbouring cell (own = false).
     * Returns a FireAlert when this detection opens an incident, else null.
     */
    public Alert onDetection(String cell, long t, double frp, Object confidence, boolean own, Params p) {
        if (incidentKey != null) {                       // incident open: just activity
            lastActivity = Math.max(lastActivity, t);
            if (own && qualifies(confidence)) {
                incidentDetections++;
                incidentFrpMax = Math.max(incidentFrpMax, frp);
            }
            return null;
        }
        if (!own || !qualifies(confidence)) return null; // neighbours don't open incidents here
        windowTimes.add(t);
        windowFrp.add(frp);
        prune(t, p);
        if (windowTimes.size() < p.minDetections || dangerClass < p.minDanger) return null;

        incidentStart = windowTimes.get(0);
        incidentKey = "inc-" + cell + "-" + KEY_TIME.format(Instant.ofEpochMilli(incidentStart));
        lastActivity = t;
        incidentDetections = windowTimes.size();
        double sum = 0, max = 0;
        for (double f : windowFrp) {
            sum += f;
            max = Math.max(max, f);
        }
        incidentFrpMax = max;
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("detections", windowTimes.size());
        d.put("frp_max", round(max));
        d.put("frp_sum", round(sum));
        d.put("danger_class", dangerClass);
        d.put("fwi", round(fwi));
        d.put("first_detection_time", incidentStart);
        windowTimes.clear();
        windowFrp.clear();
        return new Alert("FireAlert", incidentKey, t, dangerClass >= 3 ? "high" : "medium", cell, d);
    }

    /** Event time at which SceneCleared fires if nothing happens before, or null. */
    public Long clearDeadline(Params p) {
        return incidentKey == null ? null : lastActivity + p.clearAfterMs;
    }

    /** Event-time timer. Returns SceneCleared if the incident really went quiet, else null. */
    public Alert onTimer(String cell, long t, Params p) {
        if (incidentKey == null || t < lastActivity + p.clearAfterMs) return null; // stale timer
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("last_activity_time", lastActivity);
        d.put("detections", incidentDetections);
        d.put("frp_max", round(incidentFrpMax));
        d.put("duration_h", round((lastActivity - incidentStart) / 3_600_000.0));
        Alert a = new Alert("SceneCleared", incidentKey, lastActivity + p.clearAfterMs, "low", cell, d);
        incidentKey = null;
        incidentDetections = 0;
        incidentFrpMax = 0;
        return a;
    }

    /** Nothing worth keeping (lets the wrapper clear state): no incident, no pending window. */
    public boolean idle() {
        return incidentKey == null && windowTimes.isEmpty();
    }

    private void prune(long now, Params p) {
        while (!windowTimes.isEmpty() && windowTimes.get(0) <= now - p.windowMs) {
            windowTimes.remove(0);
            windowFrp.remove(0);
        }
    }

    static double round(double v) {
        return Double.isNaN(v) ? v : Math.round(v * 100) / 100.0;
    }
}
