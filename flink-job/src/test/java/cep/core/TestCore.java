package cep.core;

import java.util.List;
import java.util.Map;

/**
 * Tests for the pattern logic, no Flink and no JUnit needed:
 *   flink-job/test-core.sh      (javac + java, any JDK 17+)
 */
public final class TestCore {
    static int checks = 0;
    static final long H = 3_600_000L, MIN = 60_000L;
    static final long T0 = 1_755_000_000_000L;      // 2025-08-12T12:00Z

    static void check(String name, boolean ok, Object detail) {
        if (!ok) throw new AssertionError("FAIL " + name + " " + detail);
        checks++;
    }

    public static void main(String[] args) {
        fireAlert();
        sceneCleared();
        burnSupply();
        capacityRedirect();
        json();
        System.out.println("OK: " + checks + " checks");
    }

    static void fireAlert() {
        var p = new FireIncidentLogic.Params();
        var c = new FireIncidentLogic();
        String cell = "c512_688";
        // no FWI yet: detections alone never alert
        check("no fwi", c.onDetection(cell, T0, 10, "n", true, p) == null, "");
        check("no fwi 2", c.onDetection(cell, T0 + 1 * H, 10, "n", true, p) == null, "");
        check("no fwi 3", c.onDetection(cell, T0 + 2 * H, 10, "n", true, p) == null, "");
        // danger Low: still nothing
        c.onFwi(1, 9.0);
        check("low danger", c.onDetection(cell, T0 + 3 * H, 10, "h", true, p) == null, "");
        // Moderate: next qualifying detection alerts; window (T0-2h, T0+4h] holds T0..T0+4h = 5 detections
        c.onFwi(2, 15.5);
        check("low confidence ignored", c.onDetection(cell, T0 + 4 * H, 99, "l", true, p) == null, "");
        check("neighbour can't open", c.onDetection(cell, T0 + 4 * H, 99, "h", false, p) == null, "");
        Alert a = c.onDetection(cell, T0 + 4 * H, 40, "n", true, p);
        check("alert", a != null && a.type.equals("FireAlert"), a);
        check("key from first detection in window", a.caseKey.equals("inc-c512_688-20250812T1200"), a.caseKey);
        check("details", a.details.get("detections").equals(5) && a.details.get("frp_max").equals(40.0)
                && a.details.get("danger_class").equals(2) && a.severity.equals("medium"), a.details);
        check("alert at event time of the trigger", a.eventTime == T0 + 4 * H, a.eventTime);
        // incident open: no second FireAlert
        check("once per incident", c.onDetection(cell, T0 + 5 * H, 50, "n", true, p) == null, "");
        check("incident counts", c.incidentDetections == 6 && c.incidentFrpMax == 50.0, c.incidentDetections);
        // window rule: 3 detections spread over > 6 h never alert
        var d = new FireIncidentLogic();
        d.onFwi(3, 30);
        d.onDetection(cell, T0, 1, "n", true, p);
        d.onDetection(cell, T0 + 4 * H, 1, "n", true, p);
        check("spread out", d.onDetection(cell, T0 + 7 * H, 1, "n", true, p) == null, d.windowTimes);
        check("then in window", d.onDetection(cell, T0 + 8 * H, 1, "n", true, p) != null, d.windowTimes);
        check("high severity", d.incidentKey.endsWith("20250812T1600"), d.incidentKey);   // T0+4h
        // MODIS numeric confidence
        check("modis 20 low", !FireIncidentLogic.qualifies("20") && FireIncidentLogic.qualifies(80)
                && FireIncidentLogic.qualifies(null), "");
    }

    static void sceneCleared() {
        var p = new FireIncidentLogic.Params();
        var c = new FireIncidentLogic();
        String cell = "c1_1";
        c.onFwi(2, 12);
        for (int k = 0; k < 3; k++) c.onDetection(cell, T0 + k * H, 5, "n", true, p);
        check("open", c.incidentKey != null, "");
        long dl = c.clearDeadline(p);
        check("deadline", dl == T0 + 2 * H + 12 * H, dl);
        // a neighbour keeps the incident alive
        c.onDetection(cell, T0 + 10 * H, 5, "n", false, p);
        check("stale timer ignored", c.onTimer(cell, dl, p) == null, "");
        long dl2 = c.clearDeadline(p);
        check("deadline moved", dl2 == T0 + 22 * H, dl2);
        Alert s = c.onTimer(cell, dl2, p);
        check("cleared", s != null && s.type.equals("SceneCleared") && s.eventTime == dl2, s);
        check("same key", s.caseKey.equals("inc-c1_1-20250812T1200"), s.caseKey);
        check("duration", s.details.get("duration_h").equals(10.0), s.details);
        check("idle after", c.idle() && c.clearDeadline(p) == null, "");
        // fire flares up again later: NEW incident key
        for (int k = 0; k < 3; k++) c.onDetection(cell, T0 + 30 * H + k * MIN, 5, "n", true, p);
        check("new incident", c.incidentKey.equals("inc-c1_1-20250813T1800"), c.incidentKey);
        check("neighbours", Grid.neighbours("c10_20").size() == 8 && Grid.neighbours("c10_20").contains("c9_19")
                && !Grid.neighbours("c10_20").contains("c10_20"), Grid.neighbours("c10_20"));
    }

    static void burnSupply() {
        var p = new ProcessPatternLogic.Params();
        String k = "inc-c1_1-20250812T1200";
        // path 1: danger >= 4
        var a = new ProcessPatternLogic();
        a.onAlert("FireAlert", "c1_1", Map.of("danger_class", 4), T0);
        check("no mci, no burn", a.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-1"), "", T0, 0, p).isEmpty(), "");
        List<Alert> out = a.onPais(k, "Activate MCI protocol", Map.of(), "", T0, 1000, p);
        check("burn by danger", out.size() == 1 && out.get(0).details.get("trigger").equals("danger_class"), out);
        check("once", a.onPais(k, "Activate MCI protocol", Map.of(), "", T0, 2000, p).isEmpty(), "");
        check("alert fields", out.get(0).caseKey.equals(k) && out.get(0).cell.equals("c1_1")
                && out.get(0).eventTime == T0, out.get(0));
        // path 2: danger 2, but 3 pre-alerts within 60 min (wall clock)
        var b = new ProcessPatternLogic();
        b.onAlert("FireAlert", "c1_1", Map.of("danger_class", 2), T0);
        b.onPais(k, "Activate MCI protocol", Map.of(), "", T0, 0, p);
        b.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-1"), "", T0, 0, p);
        b.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-2"), "", T0, 61 * MIN, p);   // first one expires
        check("2 in window", b.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-3"), "", T0, 62 * MIN, p).isEmpty(), b.preAlerts);
        out = b.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-4"), "", T0, 63 * MIN, p);
        check("burn by load", out.size() == 1 && out.get(0).details.get("trigger").equals("pre_alerts"), out);
        // failed/rejected PAIS events don't count
        var c = new ProcessPatternLogic();
        c.onAlert("FireAlert", "c1_1", Map.of("danger_class", 5), T0);
        check("error ignored", c.onPais(k, "Activate MCI protocol", Map.of(), "HTTP 500", T0, 0, p).isEmpty() && !c.mciSeen, "");
    }

    static void capacityRedirect() {
        var p = new ProcessPatternLogic.Params();
        String k = "inc-c2_2-20250812T1200";
        var s = new ProcessPatternLogic();
        s.onAlert("FireAlert", "c2_2", Map.of("danger_class", 3), T0);
        s.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-1"), "", T0, 0, p);
        s.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-2"), "", T0, 0, p);
        s.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-3"), "", T0, 0, p);
        s.onPais(k, "Report unit available", Map.of("unitId", "amb-3"), "", T0, 0, p);   // back at base
        s.onPais(k, "Emergency order", Map.of(), "", T0, 10 * MIN, p);
        check("timer set", s.nextTimer() == 12 * MIN, s.nextTimer());
        check("not yet", s.onTimer(k, 11 * MIN, p).isEmpty() && s.nextTimer() == 12 * MIN, "");
        List<Alert> out = s.onTimer(k, 12 * MIN, p);
        check("one per unit en route", out.size() == 2 && out.get(0).details.get("unit_id").equals("amb-1")
                && out.get(1).details.get("unit_id").equals("amb-2"), out);
        check("ids per unit", out.get(0).alertId().equals("CapacityRedirect:" + k + ":amb-1"), out.get(0).alertId());
        check("timer cleared", s.nextTimer() == -1, "");
        // second order, same units: no duplicate redirects
        s.onPais(k, "Emergency order", Map.of(), "", T0, 20 * MIN, p);
        check("no duplicates", s.onTimer(k, 22 * MIN, p).isEmpty(), "");
        // delivery in time cancels
        var d = new ProcessPatternLogic();
        d.onAlert("FireAlert", "c2_2", Map.of("danger_class", 3), T0);
        d.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-9"), "", T0, 0, p);
        d.onPais(k, "Emergency order", Map.of(), "", T0, 0, p);
        d.onPais(k, "Receive emergency delivery", Map.of(), "", T0, 1 * MIN, p);
        check("delivered in time", d.nextTimer() == -1 && d.onTimer(k, 5 * MIN, p).isEmpty(), "");
        // incident over: no redirect
        var e = new ProcessPatternLogic();
        e.onAlert("FireAlert", "c2_2", Map.of("danger_class", 3), T0);
        e.onPais(k, "Send EMS pre-alert", Map.of("unitId", "amb-9"), "", T0, 0, p);
        e.onPais(k, "Emergency order", Map.of(), "", T0, 0, p);
        e.onAlert("SceneCleared", "c2_2", Map.of(), T0 + 20 * H);
        check("cleared incident", e.onTimer(k, 5 * MIN, p).isEmpty(), "");
    }

    static void json() {
        var a = new Alert("FireAlert", "inc-\"x\"", 5, "high", "c1_1",
                new java.util.LinkedHashMap<>(Map.of("frp_max", 12.5)));
        a.details.put("n", 3);
        a.details.put("nan", Double.NaN);
        a.details.put("list", List.of(1, "b"));
        String j = a.toJson(7);
        check("json", j.equals("{\"alert_id\":\"FireAlert:inc-\\\"x\\\"\",\"type\":\"FireAlert\",\"case_key\":\"inc-\\\"x\\\"\","
                + "\"event_time\":5,\"severity\":\"high\",\"cell\":\"c1_1\",\"detected_time\":7,"
                + "\"details\":{\"frp_max\":12.5,\"n\":3,\"nan\":null,\"list\":[1,\"b\"]}}"), j);
    }
}
