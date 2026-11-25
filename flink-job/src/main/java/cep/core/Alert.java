package cep.core;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** One CEP finding for the bridge (topic `alerts`, CONTRACT.md §3.2). */
public final class Alert implements Serializable {
    public final String type;
    public final String caseKey;
    public final long eventTime;
    public final String severity;
    public final String cell;
    public final Map<String, Object> details;

    public Alert(String type, String caseKey, long eventTime, String severity, String cell,
                 Map<String, Object> details) {
        this.type = type;
        this.caseKey = caseKey;
        this.eventTime = eventTime;
        this.severity = severity;
        this.cell = cell;
        this.details = details == null ? new LinkedHashMap<>() : details;
    }

    /** Deterministic id: a Flink restart that re-emits the same finding produces the same id. */
    public String alertId() {
        Object unit = details.get("unit_id");
        return type + ":" + caseKey + (unit != null ? ":" + unit : "");
    }

    public String toJson(long detectedTime) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("alert_id", alertId());
        m.put("type", type);
        m.put("case_key", caseKey);
        m.put("event_time", eventTime);
        m.put("severity", severity);
        m.put("cell", cell);
        m.put("detected_time", detectedTime);
        m.put("details", details);
        return Json.write(m);
    }

    @Override
    public String toString() {
        return type + "[" + caseKey + (details.containsKey("unit_id") ? " " + details.get("unit_id") : "") + "]";
    }
}
