package cep.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cep.core.Alert;
import cep.core.ProcessPatternLogic;
import cep.job.Events.ProcEvent;

/**
 * BurnSupplyEmergency + CapacityRedirect per incident, in WALL-CLOCK (processing) time:
 * the inputs are people working in Tasklist and the DCR portal (CONTRACT.md §3.4).
 */
public class ProcessPatternFunction extends KeyedProcessFunction<String, ProcEvent, String> {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessPatternFunction.class);
    private final ProcessPatternLogic.Params p;
    private transient ValueState<ProcessPatternLogic> logic;

    public ProcessPatternFunction(ProcessPatternLogic.Params p) {
        this.p = p;
    }

    @Override
    public void open(OpenContext ctx) {
        logic = getRuntimeContext().getState(new ValueStateDescriptor<>("process-logic", ProcessPatternLogic.class));
    }

    @Override
    public void processElement(ProcEvent e, Context ctx, Collector<String> out) throws Exception {
        ProcessPatternLogic l = logic.value();
        if (l == null) l = new ProcessPatternLogic();
        long now = ctx.timerService().currentProcessingTime();
        if (ProcEvent.ALERT.equals(e.kind)) {
            Map<String, Object> d = new HashMap<>();
            d.put("danger_class", e.dangerClass);
            l.onAlert(e.name, e.cell, d, e.eventTime);
        } else {
            Map<String, Object> payload = new HashMap<>();
            if (e.unitId != null) payload.put("unitId", e.unitId);
            emit(l.onPais(e.caseKey, e.name, payload, e.error, e.eventTime, now, p), out);
        }
        logic.update(l);
        if (l.nextTimer() > 0) ctx.timerService().registerProcessingTimeTimer(l.nextTimer());
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
        ProcessPatternLogic l = logic.value();
        if (l == null) return;
        emit(l.onTimer(ctx.getCurrentKey(), ts, p), out);
        logic.update(l);
        if (l.nextTimer() > 0) ctx.timerService().registerProcessingTimeTimer(l.nextTimer());
    }

    private void emit(List<Alert> alerts, Collector<String> out) {
        for (Alert a : alerts) {
            LOG.info("CEP {} {} {}", a.type, a.caseKey, a.details);
            out.collect(a.toJson(System.currentTimeMillis()));
        }
    }
}
