package cep.job;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cep.core.Alert;
import cep.core.FireIncidentLogic;
import cep.job.Events.EnvEvent;

/**
 * FireAlert + SceneCleared per cell, in EVENT time. Records are buffered until the watermark
 * passes their timestamp, then fed to FireIncidentLogic in timestamp order (FIRMS NRT data is
 * hours late and not ordered). SceneCleared is an event-time timer at lastActivity + clearAfter,
 * so it fires in replays as soon as the watermark (heartbeat) passes it.
 */
public class EnvPatternFunction extends KeyedProcessFunction<String, EnvEvent, String> {
    private static final Logger LOG = LoggerFactory.getLogger(EnvPatternFunction.class);
    private final FireIncidentLogic.Params p;
    private transient ValueState<FireIncidentLogic> logic;
    private transient ListState<EnvEvent> buffer;

    public EnvPatternFunction(FireIncidentLogic.Params p) {
        this.p = p;
    }

    @Override
    public void open(OpenContext ctx) {
        logic = getRuntimeContext().getState(new ValueStateDescriptor<>("fire-logic", FireIncidentLogic.class));
        buffer = getRuntimeContext().getListState(new ListStateDescriptor<>("buffer", EnvEvent.class));
    }

    @Override
    public void processElement(EnvEvent e, Context ctx, Collector<String> out) throws Exception {
        if (e.t <= ctx.timerService().currentWatermark()) {
            // late (behind the watermark): apply now rather than lose it; order is best effort
            FireIncidentLogic l = load();
            apply(l, e, ctx.getCurrentKey(), out);
            save(l, ctx);
            return;
        }
        buffer.add(e);
        ctx.timerService().registerEventTimeTimer(e.t);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
        String cell = ctx.getCurrentKey();
        List<EnvEvent> due = new ArrayList<>(), keep = new ArrayList<>();
        Iterable<EnvEvent> all = buffer.get();
        if (all != null) {
            for (EnvEvent e : all) (e.t <= ts ? due : keep).add(e);
        }
        buffer.update(keep);
        // timestamp order; at equal times the weather comes first (it describes the conditions)
        due.sort(Comparator.comparingLong((EnvEvent e) -> e.t).thenComparing(e -> !EnvEvent.FWI.equals(e.kind)));

        FireIncidentLogic l = load();
        for (EnvEvent e : due) {
            Long dl = l.clearDeadline(p);
            if (dl != null && dl < e.t) emit(l.onTimer(cell, dl, p), out);   // went quiet before this event
            apply(l, e, cell, out);
        }
        Long dl = l.clearDeadline(p);
        if (dl != null && dl <= ts) emit(l.onTimer(cell, dl, p), out);
        save(l, ctx);
    }

    private void apply(FireIncidentLogic l, EnvEvent e, String cell, Collector<String> out) {
        if (EnvEvent.FWI.equals(e.kind)) {
            l.onFwi(e.dangerClass, e.fwi);
        } else if (EnvEvent.FIRE.equals(e.kind)) {
            emit(l.onDetection(cell, e.t, e.frp, e.confidence, e.own, p), out);
        }
    }

    private void emit(Alert a, Collector<String> out) {
        if (a == null) return;
        LOG.info("CEP {} {} at event time {}", a.type, a.caseKey, a.eventTime);
        out.collect(a.toJson(System.currentTimeMillis()));
    }

    private FireIncidentLogic load() throws Exception {
        FireIncidentLogic l = logic.value();
        return l != null ? l : new FireIncidentLogic();
    }

    private void save(FireIncidentLogic l, Context ctx) throws Exception {
        if (l.idle() && l.dangerClass < 0) {      // nothing known about this cell: don't keep state
            logic.clear();
            return;
        }
        logic.update(l);
        Long dl = l.clearDeadline(p);
        if (dl != null) ctx.timerService().registerEventTimeTimer(dl);   // same ts = deduplicated
    }
}
