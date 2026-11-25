package cep.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import cep.core.Grid;

/** Event POJOs (public fields + no-arg constructor = Flink POJO types) and JSON parsing. */
public final class Events {
    private Events() {}

    static final ObjectMapper JSON = new ObjectMapper();

    /** fire / fwi / heartbeat, keyed by grid cell. */
    public static class EnvEvent {
        public static final String FIRE = "fire", FWI = "fwi", HEARTBEAT = "heartbeat";
        public String kind;
        public String cell;
        public long t;
        public double frp;
        public String confidence;
        public int dangerClass;
        public double fwi;
        public boolean own = true;   // false = copy of a detection in a neighbouring cell

        public EnvEvent() {}

        EnvEvent copyFor(String otherCell) {
            EnvEvent e = new EnvEvent();
            e.kind = kind;
            e.cell = otherCell;
            e.t = t;
            e.frp = frp;
            e.confidence = confidence;
            e.own = false;
            return e;
        }
    }

    /** our own FireAlert/SceneCleared alerts and PAIS events, keyed by incident (case_key). */
    public static class ProcEvent {
        public static final String ALERT = "alert", PAIS = "pais";
        public String kind;
        public String caseKey;
        public String name;          // alert type or PAIS activity
        public String cell;
        public long eventTime;
        public String error;
        public String unitId;
        public int dangerClass = -1;

        public ProcEvent() {}
    }

    /** firms.* and fwi.* records -> EnvEvent. Unparseable records are dropped (and counted in logs). */
    public static class ParseEnv implements FlatMapFunction<String, EnvEvent> {
        @Override
        public void flatMap(String s, Collector<EnvEvent> out) {
            JsonNode n;
            try {
                n = JSON.readTree(s);
            } catch (Exception e) {
                return;
            }
            long t = n.path("event_time").asLong(-1);
            if (t < 0) return;
            String type = n.path("type").asText("");
            EnvEvent e = new EnvEvent();
            e.t = t;
            switch (type) {
                case "fire" -> {
                    e.kind = EnvEvent.FIRE;
                    e.cell = n.path("cell").asText(null);
                    e.frp = n.path("frp").asDouble(0);
                    JsonNode c = n.get("confidence");
                    e.confidence = c == null || c.isNull() ? null : c.asText();
                }
                case "fwi" -> {
                    e.kind = EnvEvent.FWI;
                    e.cell = n.path("cell").asText(null);
                    e.dangerClass = n.path("danger_class").asInt(-1);
                    e.fwi = n.path("fwi").asDouble(Double.NaN);
                }
                case "heartbeat" -> {
                    e.kind = EnvEvent.HEARTBEAT;   // only moves the watermark
                    e.cell = "";
                }
                default -> {
                    return;
                }
            }
            if (e.cell == null) return;
            out.collect(e);
        }
    }

    /** After watermarks: drop heartbeats, copy each detection to its 8 neighbours (SceneCleared). */
    public static class FanOut implements FlatMapFunction<EnvEvent, EnvEvent> {
        @Override
        public void flatMap(EnvEvent e, Collector<EnvEvent> out) {
            if (EnvEvent.HEARTBEAT.equals(e.kind)) return;
            out.collect(e);
            if (EnvEvent.FIRE.equals(e.kind)) {
                for (String nb : Grid.neighbours(e.cell)) out.collect(e.copyFor(nb));
            }
        }
    }

    /** alerts (only FireAlert/SceneCleared) and pais.events -> ProcEvent, only real incidents. */
    public static class ParseProc implements FlatMapFunction<String, ProcEvent> {
        @Override
        public void flatMap(String s, Collector<ProcEvent> out) {
            JsonNode n;
            try {
                n = JSON.readTree(s);
            } catch (Exception e) {
                return;
            }
            String caseKey = n.path("case_key").asText("");
            if (!caseKey.startsWith("inc-")) return;            // smoke tests etc.
            ProcEvent p = new ProcEvent();
            p.caseKey = caseKey;
            p.eventTime = n.path("event_time").asLong(0);
            if (n.has("alert_id")) {                            // an alert
                p.kind = ProcEvent.ALERT;
                p.name = n.path("type").asText("");
                if (!p.name.equals("FireAlert") && !p.name.equals("SceneCleared")) return;
                p.cell = n.path("cell").asText(null);
                p.dangerClass = n.path("details").path("danger_class").asInt(-1);
            } else {                                            // a PAIS event
                p.kind = ProcEvent.PAIS;
                p.name = n.path("event").asText(null);
                p.error = n.path("error").asText("");
                JsonNode u = n.path("payload").get("unitId");
                p.unitId = u == null || u.isNull() ? null : u.asText();
            }
            out.collect(p);
        }
    }

    public static class ByCell implements KeySelector<EnvEvent, String> {
        @Override
        public String getKey(EnvEvent e) {
            return e.cell;
        }
    }

    public static class ByCase implements KeySelector<ProcEvent, String> {
        @Override
        public String getKey(ProcEvent e) {
            return e.caseKey;
        }
    }
}
