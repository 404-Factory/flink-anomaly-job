package com.factory.flink.process;

import com.factory.flink.domain.dto.RuleResult;
import com.factory.flink.domain.dto.SensorReadingEvent;
import com.factory.flink.domain.dto.SensorViolationEvent;
import com.factory.flink.domain.enums.Severity;
import com.factory.flink.domain.enums.RuleName;
import com.factory.flink.process.engine.BiasEngine;
import com.factory.flink.process.engine.Rule1Engine;
import com.factory.flink.process.engine.RuleEngine;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class FiveMinProcessFunction extends
    KeyedProcessFunction<String, SensorReadingEvent, SensorViolationEvent> {

    private static final long serialVersionUID = 1L;

    private transient ListState<SensorReadingEvent> sampleState;
    private transient ValueState<Integer> batchCountState;
    private transient ValueState<String> lastBatchIdState;

    private transient RuleEngine rule1Engine;
    private transient RuleEngine biasEngine;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<SensorReadingEvent> sampleDescriptor = new ListStateDescriptor<>(
            "sensor-samples-5min",
            TypeInformation.of(SensorReadingEvent.class)
        );
        sampleState = getRuntimeContext().getListState(sampleDescriptor);

        ValueStateDescriptor<Integer> batchCountDescriptor = new ValueStateDescriptor<>(
            "batch-count-5min",
            TypeInformation.of(Integer.class)
        );
        batchCountState = getRuntimeContext().getState(batchCountDescriptor);

        ValueStateDescriptor<String> lastBatchIdDescriptor = new ValueStateDescriptor<>(
            "last-batch-id-5min",
            TypeInformation.of(String.class)
        );
        lastBatchIdState = getRuntimeContext().getState(lastBatchIdDescriptor);

        rule1Engine = new Rule1Engine();
        biasEngine = new BiasEngine();
    }

    @Override
    public void processElement(SensorReadingEvent newEvent, Context ctx,
        Collector<SensorViolationEvent> out) throws Exception {
        // 1. Add new record to list state
        sampleState.add(newEvent);

        // 2. Count unique batches
        String batchId = newEvent.getBatchId();
        if (batchId != null) {
            String lastBatchId = lastBatchIdState.value();
            if (lastBatchId == null || !lastBatchId.equals(batchId)) {
                lastBatchIdState.update(batchId);
                Integer currentCount = batchCountState.value();
                if (currentCount == null) {
                    currentCount = 0;
                }
                currentCount++;
                batchCountState.update(currentCount);
            }
        }

        // 3. Fetch and clean expired samples (5 minutes back in event time)
        List<SensorReadingEvent> validSamples = new ArrayList<>();
        long cutoffTime = ctx.timestamp() - (300 * 1000L); // 5 minutes

        for (SensorReadingEvent sample : sampleState.get()) {
            if (sample.getMeasuredAtEpochMilli() >= cutoffTime) {
                validSamples.add(sample);
            }
        }

        // Update list state with only valid samples
        sampleState.update(validSamples);

        // Sort samples chronologically
        validSamples.sort(Comparator.comparingLong(SensorReadingEvent::getMeasuredAtEpochMilli));

        // 4. Enforce bootstrapping and batch count limit
        Integer batchCount = batchCountState.value();
        if (batchCount == null || batchCount < 5) {
            return;
        }

        Double recipeMin = newEvent.getRecipeMin();
        Double recipeMax = newEvent.getRecipeMax();

        if (validSamples.isEmpty()) {
            return;
        }

        Instant windowStart = Instant.ofEpochMilli(validSamples.get(0).getMeasuredAtEpochMilli());
        Instant windowEnd = Instant.ofEpochMilli(newEvent.getMeasuredAtEpochMilli());

        // Evaluate Nelson Rule 1
        RuleResult r1Result = rule1Engine.evaluate(validSamples, recipeMin, recipeMax);
        if (r1Result != null) {
            if (r1Result.isDetected()) {
                out.collect(
                    SensorViolationEvent.from(newEvent, r1Result, validSamples.size(), windowStart,
                        windowEnd));
            } else {
                out.collect(SensorViolationEvent.builder()
                    .equipmentId(newEvent.getEquipmentId())
                    .sensorId(newEvent.getSensorId())
                    .sensorType(newEvent.getSensorType())
                    .ruleName(RuleName.NELSON_RULE_1)
                    .severity(Severity.NORMAL)
                    .detectedAt(windowEnd)
                    .windowStart(windowStart)
                    .windowEnd(windowEnd)
                    .sampleCount(validSamples.size())
                    .reason("정상 범위 복구 (NELSON_RULE_1)")
                    .build());
            }
        }

        // Evaluate Bias Ratio Rule
        RuleResult biasResult = biasEngine.evaluate(validSamples, recipeMin, recipeMax);
        if (biasResult != null) {
            if (biasResult.isDetected()) {
                out.collect(SensorViolationEvent.from(newEvent, biasResult, validSamples.size(),
                    windowStart, windowEnd));
            } else {
                out.collect(SensorViolationEvent.builder()
                    .equipmentId(newEvent.getEquipmentId())
                    .sensorId(newEvent.getSensorId())
                    .sensorType(newEvent.getSensorType())
                    .ruleName(RuleName.BIAS_RATIO_RULE)
                    .severity(Severity.NORMAL)
                    .detectedAt(windowEnd)
                    .windowStart(windowStart)
                    .windowEnd(windowEnd)
                    .sampleCount(validSamples.size())
                    .reason("정상 범위 복구 (BIAS_RATIO_RULE)")
                    .build());
            }
        }
    }
}
