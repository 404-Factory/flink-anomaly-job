package com.factory.flink.process;

import com.factory.flink.dto.RuleName;
import com.factory.flink.dto.RuleResult;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.dto.Severity;
import com.factory.flink.process.engine.Rule3Engine;
import com.factory.flink.process.engine.RuleEngine;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class OneMinProcessFunction extends KeyedProcessFunction<String, SensorReadingEvent, SensorViolationEvent> {
    private static final long serialVersionUID = 1L;

    private transient ListState<SensorReadingEvent> sampleState;
    private transient RuleEngine rule3Engine;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<SensorReadingEvent> sampleDescriptor = new ListStateDescriptor<>(
                "sensor-samples-1min",
                TypeInformation.of(SensorReadingEvent.class)
        );
        sampleState = getRuntimeContext().getListState(sampleDescriptor);
        rule3Engine = new Rule3Engine();
    }

    @Override
    public void processElement(SensorReadingEvent newEvent, Context ctx, Collector<SensorViolationEvent> out) throws Exception {
        // 1. Add new record to list state
        sampleState.add(newEvent);

        // 2. Fetch and clean expired samples (1 minute back in event time)
        List<SensorReadingEvent> validSamples = new ArrayList<>();
        long cutoffTime = ctx.timestamp() - (60 * 1000L); // 1 minute

        for (SensorReadingEvent sample : sampleState.get()) {
            if (sample.getMeasuredAtEpochMilli() >= cutoffTime) {
                validSamples.add(sample);
            }
        }

        // Update list state with only valid samples
        sampleState.update(validSamples);

        // Sort samples chronologically
        validSamples.sort(Comparator.comparingLong(SensorReadingEvent::getMeasuredAtEpochMilli));

        Double recipeMin = newEvent.getRecipeMin();
        Double recipeMax = newEvent.getRecipeMax();

        if (validSamples.isEmpty()) {
            return;
        }

        Instant windowStart = Instant.ofEpochMilli(validSamples.get(0).getMeasuredAtEpochMilli());
        Instant windowEnd = Instant.ofEpochMilli(newEvent.getMeasuredAtEpochMilli());

        // Evaluate Nelson Rule 3
        RuleResult r3Result = rule3Engine.evaluate(validSamples, recipeMin, recipeMax);
        if (r3Result != null) {
            if (r3Result.isDetected()) {
                out.collect(SensorViolationEvent.from(newEvent, r3Result, validSamples.size(), windowStart, windowEnd));
            } else {
                out.collect(SensorViolationEvent.builder()
                        .equipmentId(newEvent.getEquipmentId())
                        .sensorId(newEvent.getSensorId())
                        .sensorType(newEvent.getSensorType())
                        .ruleName(RuleName.NELSON_RULE_3)
                        .severity(null)
                        .detectedAt(windowEnd)
                        .windowStart(windowStart)
                        .windowEnd(windowEnd)
                        .sampleCount(validSamples.size())
                        .reason("정상 범위 복구 (NELSON_RULE_3)")
                        .build());
            }
        }
    }
}
