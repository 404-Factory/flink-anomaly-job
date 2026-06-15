package com.factory.flink.process;

import java.util.Objects;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.factory.flink.domain.dto.SensorViolationEvent;
import com.factory.flink.domain.enums.Severity;
import com.factory.flink.domain.enums.RuleName;

public class TransitionProcessFunction extends
    KeyedProcessFunction<String, SensorViolationEvent, SensorViolationEvent> {

    private transient MapState<RuleName, Severity> lastSeverityState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<RuleName, Severity> descriptor = new MapStateDescriptor<>(
            "last-severity", TypeInformation.of(RuleName.class), TypeInformation.of(Severity.class)
        );

        lastSeverityState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(SensorViolationEvent event,
        KeyedProcessFunction<String, SensorViolationEvent, SensorViolationEvent>.Context ctx,
        Collector<SensorViolationEvent> out) throws Exception {

        Severity lastSeverity = lastSeverityState.get(event.getRuleName());
        Severity currentSeverity = event.getSeverity();

        if (!Objects.equals(lastSeverity, currentSeverity)) {
            lastSeverityState.put(event.getRuleName(), currentSeverity);
            out.collect(event);
        }
    }
}
