package cep.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Process patterns for ONE incident (case_key), in WALL-CLOCK time (CONTRACT.md §3.4, §4):
 * they react to what people do in Tasklist and the DCR portal.
 *
 * BurnSupplyEmergency: BPMN "Activate MCI protocol" AND (incident danger class >= burnDanger
 *                      OR >= preAlertCount x "Send EMS pre-alert" within preAlertWindow). Once.
 * CapacityRedirect:    DCR "Emergency order" NOT followed by "Receive emergency delivery" within
 *                      deliveryTimeout, while the incident is active -> one alert per unit en route
 *                      (pre-alert seen, "Report unit available" not yet). Absence pattern: a timer.
 *
 * Inputs: our own FireAlert / SceneCleared alerts (incident state + danger class) and pais.events.
 * Event names are parameters: the DCR event ids aren't known yet.
 */
public class ProcessPatternLogic implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final class Params implements Serializable {
        public String mci = "Activate MCI protocol";
        public String preAlert = "Send EMS pre-alert";
        public String unitAvailable = "Report unit available";
        public String emergencyOrder = "EmergencyOrder";             // DCR event id, TBD
        public String delivery = "ReceiveEmergencyDelivery";         // DCR event id, TBD
        public int burnDanger = 4;
        public int preAlertCount = 3;
        public long preAlertWindowMs = 60 * 60_000L;
        public long deliveryTimeoutMs = 2 * 60_000L;                 // demo; the graph says PT24H
    }

    public boolean active;
    public String cell;
    public int dangerClass = -1;
    public long envTime;                       // event time of the CEP context, for emitted alerts
    public boolean mciSeen;
    public boolean burnSent;
    public ArrayList<Long> preAlerts = new ArrayList<>();
    public LinkedHashSet<String> enRoute = new LinkedHashSet<>();
    public LinkedHashSet<String> redirected = new LinkedHashSet<>();
    public long orderDeadline = -1;            // -1 = no open emergency order
    public long orderTime = -1;

    public ProcessPatternLogic() {}

    /** Our own FireAlert / SceneCleared for this incident. */
    public void onAlert(String type, String cell, Map<String, Object> details, long eventTime) {
        if ("FireAlert".equals(type)) {
            active = true;
            this.cell = cell;
            Object dc = details == null ? null : details.get("danger_class");
            if (dc instanceof Number n) dangerClass = n.intValue();
            envTime = Math.max(envTime, eventTime);
        } else if ("SceneCleared".equals(type)) {
            active = false;
            orderDeadline = -1;                // no redirects for a finished incident
        }
    }

    /** One PAIS event for this incident. Failed/rejected events (error set) are ignored. */
    public List<Alert> onPais(String caseKey, String event, Map<String, Object> payload, String error,
                              long eventTime, long now, Params p) {
        List<Alert> out = new ArrayList<>();
        if (error != null && !error.isEmpty() || event == null) return out;
        if (eventTime > 0) envTime = Math.max(envTime, eventTime);
        Object unit = payload == null ? null : payload.get("unitId");

        if (event.equals(p.mci)) {
            mciSeen = true;
        } else if (event.equals(p.preAlert)) {
            preAlerts.add(now);
            if (unit != null) enRoute.add(unit.toString());
        } else if (event.equals(p.unitAvailable)) {
            if (unit != null) enRoute.remove(unit.toString());
        } else if (event.equals(p.emergencyOrder)) {
            orderTime = now;
            orderDeadline = now + p.deliveryTimeoutMs;
        } else if (event.equals(p.delivery)) {
            orderDeadline = -1;
        }
        prunePreAlerts(now, p);
        Alert burn = checkBurn(caseKey, p);
        if (burn != null) out.add(burn);
        return out;
    }

    /** Wall-clock time of the next timer, or -1. */
    public long nextTimer() {
        return orderDeadline;
    }

    public List<Alert> onTimer(String caseKey, long now, Params p) {
        List<Alert> out = new ArrayList<>();
        if (orderDeadline < 0 || now < orderDeadline) return out;    // cancelled or stale timer
        orderDeadline = -1;
        if (!active) return out;
        for (String u : enRoute) {
            if (!redirected.add(u)) continue;                        // one redirect per unit
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("unit_id", u);
            d.put("order_age_s", (now - orderTime) / 1000);
            d.put("units_en_route", enRoute.size());
            out.add(new Alert("CapacityRedirect", caseKey, envTime, "high", cell, d));
        }
        return out;
    }

    private Alert checkBurn(String caseKey, Params p) {
        if (burnSent || !mciSeen) return null;
        boolean byDanger = dangerClass >= p.burnDanger;
        boolean byLoad = preAlerts.size() >= p.preAlertCount;
        if (!byDanger && !byLoad) return null;
        burnSent = true;
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("trigger", byDanger ? "danger_class" : "pre_alerts");
        d.put("danger_class", dangerClass);
        d.put("pre_alerts_window", preAlerts.size());
        return new Alert("BurnSupplyEmergency", caseKey, envTime, "high", cell, d);
    }

    private void prunePreAlerts(long now, Params p) {
        preAlerts.removeIf(t -> t <= now - p.preAlertWindowMs);
    }
}
