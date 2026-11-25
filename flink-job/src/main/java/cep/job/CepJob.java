package cep.job;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cep.core.FireIncidentLogic;
import cep.core.ProcessPatternLogic;
import cep.job.Events.EnvEvent;
import cep.job.Events.ProcEvent;

/**
 * The CEP job (CONTRACT.md §4). Two branches, one output topic:
 *
 *   firms.* + fwi.*  -> event time, keyed by cell     -> FireAlert, SceneCleared
 *   alerts + pais.*  -> wall clock, keyed by incident  -> BurnSupplyEmergency, CapacityRedirect
 *                                                          (reads its own FireAlert/SceneCleared back)
 *
 * Arguments (--name value), all optional:
 *   --source live|replay          firms.raw/fwi.raw or firms.replay/fwi.replay   (default live)
 *   --bootstrap redpanda:9092     --alerts-topic alerts   --pais-topic pais.events
 *   --group cep-job               consumer group prefix (new value = fresh start)
 *   --firms-start latest|earliest (fwi always reads from the beginning: it is state, not events)
 *   --out-of-order-min 60         watermark slack for late FIRMS data
 *   --idle-s 60                   a topic with no records stops holding back the watermark
 *   --min-detections 3 --window-h 6 --min-danger 2 --clear-h 12
 *   --burn-danger 4 --pre-alert-count 3 --pre-alert-window-min 60 --delivery-timeout-s 120
 *   --dcr-emergency-order "Emergency order" --dcr-delivery "Receive emergency delivery"   (DCR labels)
 *   --process-patterns true       false = only FireAlert/SceneCleared. Use it for a second job next to
 *                                 the one that runs the process patterns (e.g. live next to replay):
 *                                 both read the same alerts/pais topics and would duplicate findings.
 */
public final class CepJob {
    public static void main(String[] argv) throws Exception {
        Map<String, String> a = parse(argv);
        String bootstrap = a.getOrDefault("bootstrap", "redpanda:9092");
        String suffix = a.getOrDefault("source", "live").equals("replay") ? "replay" : "raw";
        String group = a.getOrDefault("group", "cep-job");
        String alertsTopic = a.getOrDefault("alerts-topic", "alerts");
        String paisTopic = a.getOrDefault("pais-topic", "pais.events");

        FireIncidentLogic.Params fp = new FireIncidentLogic.Params();
        fp.minDetections = num(a, "min-detections", fp.minDetections);
        fp.windowMs = (long) (dbl(a, "window-h", 6) * 3_600_000L);
        fp.minDanger = num(a, "min-danger", fp.minDanger);
        fp.clearAfterMs = (long) (dbl(a, "clear-h", 12) * 3_600_000L);

        ProcessPatternLogic.Params pp = new ProcessPatternLogic.Params();
        pp.burnDanger = num(a, "burn-danger", pp.burnDanger);
        pp.preAlertCount = num(a, "pre-alert-count", pp.preAlertCount);
        pp.preAlertWindowMs = (long) (dbl(a, "pre-alert-window-min", 60) * 60_000L);
        pp.deliveryTimeoutMs = (long) (dbl(a, "delivery-timeout-s", 120) * 1000L);
        pp.emergencyOrder = a.getOrDefault("dcr-emergency-order", pp.emergencyOrder);
        pp.delivery = a.getOrDefault("dcr-delivery", pp.delivery);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);                 // one partition per topic, one TaskManager
        env.enableCheckpointing(30_000);       // incident state survives task restarts

        WatermarkStrategy<EnvEvent> wm = WatermarkStrategy
                .<EnvEvent>forBoundedOutOfOrderness(Duration.ofMinutes(num(a, "out-of-order-min", 60)))
                .withTimestampAssigner((SerializableTimestampAssigner<EnvEvent>) (e, ts) -> e.t)
                .withIdleness(Duration.ofSeconds(num(a, "idle-s", 60)));

        OffsetsInitializer firmsStart = "earliest".equals(a.get("firms-start"))
                ? OffsetsInitializer.earliest() : OffsetsInitializer.latest();
        DataStream<EnvEvent> firms = env
                .fromSource(source(bootstrap, "firms." + suffix, group + "-firms", firmsStart),
                        WatermarkStrategy.noWatermarks(), "firms." + suffix)
                .flatMap(new Events.ParseEnv()).name("parse firms")
                .assignTimestampsAndWatermarks(wm).name("watermarks firms");
        DataStream<EnvEvent> fwi = env
                .fromSource(source(bootstrap, "fwi." + suffix, group + "-fwi", OffsetsInitializer.earliest()),
                        WatermarkStrategy.noWatermarks(), "fwi." + suffix)
                .flatMap(new Events.ParseEnv()).name("parse fwi")
                .assignTimestampsAndWatermarks(wm).name("watermarks fwi");

        DataStream<String> envAlerts = firms.union(fwi)
                .flatMap(new Events.FanOut()).name("drop heartbeats, neighbour copies")
                .keyBy(new Events.ByCell())
                .process(new EnvPatternFunction(fp)).name("FireAlert / SceneCleared");

        boolean processPatterns = !"false".equalsIgnoreCase(a.getOrDefault("process-patterns", "true"));
        DataStream<String> out = envAlerts;
        if (processPatterns) {
            DataStream<ProcEvent> proc = env
                    .fromSource(source(bootstrap, alertsTopic, group + "-alerts", OffsetsInitializer.latest()),
                            WatermarkStrategy.noWatermarks(), alertsTopic)
                    .union(env.fromSource(source(bootstrap, paisTopic, group + "-pais", OffsetsInitializer.latest()),
                            WatermarkStrategy.noWatermarks(), paisTopic))
                    .flatMap(new Events.ParseProc()).name("parse alerts + pais");
            DataStream<String> procAlerts = proc
                    .keyBy(new Events.ByCase())
                    .process(new ProcessPatternFunction(pp)).name("BurnSupplyEmergency / CapacityRedirect");
            out = envAlerts.union(procAlerts);
        }

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(alertsTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        out.sinkTo(sink).name(alertsTopic);

        env.execute("cep-job (" + suffix + (processPatterns ? "" : ", env only") + ")");
    }

    private static KafkaSource<String> source(String bootstrap, String topic, String group, OffsetsInitializer start) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(start)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    static Map<String, String> parse(String[] argv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < argv.length; i += 2) {
            if (!argv[i].startsWith("--")) throw new IllegalArgumentException("expected --name value, got " + argv[i]);
            m.put(argv[i].substring(2), argv[i + 1]);
        }
        return m;
    }

    private static int num(Map<String, String> a, String k, int def) {
        return a.containsKey(k) ? Integer.parseInt(a.get(k)) : def;
    }

    private static double dbl(Map<String, String> a, String k, double def) {
        return a.containsKey(k) ? Double.parseDouble(a.get(k)) : def;
    }
}
