package com.factory.flink.process;

import com.factory.flink.dto.AnomalyType;
import com.factory.flink.dto.RuleName;
import com.factory.flink.dto.RuleResult;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.dto.Severity;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class AnomalyEvaluationProcessFunction extends KeyedProcessFunction<String, SensorReadingEvent, SensorViolationEvent> {
    private static final long serialVersionUID = 1L;

    // Flink list state to store 5 minutes of samples for each key (equipment:sensorType)
    private transient ListState<SensorReadingEvent> sampleState;
    // Flink value state to remember the last detected severity for state-change deduplication
    private transient ValueState<Severity> lastSeverityState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<SensorReadingEvent> sampleDescriptor = new ListStateDescriptor<>(
                "sensor-samples",
                TypeInformation.of(SensorReadingEvent.class)
        );
        sampleState = getRuntimeContext().getListState(sampleDescriptor);

        org.apache.flink.api.common.state.ValueStateDescriptor<Severity> severityDescriptor = 
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                        "last-severity",
                        TypeInformation.of(Severity.class)
                );
        lastSeverityState = getRuntimeContext().getState(severityDescriptor);
    }

    @Override
    public void processElement(SensorReadingEvent newEvent, Context ctx, Collector<SensorViolationEvent> out) throws Exception {
        // 1. Add new record to list state
        sampleState.add(newEvent);

        // 2. Fetch and retain only the samples within the last 5 minutes (300 seconds)
        List<SensorReadingEvent> validSamples = new ArrayList<>();
        long cutoffTime = ctx.timestamp() - (300 * 1000L); // 5 minutes back in event time

        for (SensorReadingEvent sample : sampleState.get()) {
            if (sample.getMeasuredAtEpochMilli() >= cutoffTime) {
                validSamples.add(sample);
            }
        }

        // Clean up expired state
        sampleState.update(validSamples);

        // Sort chronologically
        validSamples.sort(Comparator.comparingLong(SensorReadingEvent::getMeasuredAtEpochMilli));

        // 3. Filter 1 minute (60 seconds) worth of samples for Nelson Rule 3
        long oneMinuteCutoff = ctx.timestamp() - (60 * 1000L);
        List<SensorReadingEvent> oneMinuteSamples = new ArrayList<>();
        for (SensorReadingEvent sample : validSamples) {
            if (sample.getMeasuredAtEpochMilli() >= oneMinuteCutoff) {
                oneMinuteSamples.add(sample);
            }
        }

        // 4. Run rule evaluations
        evaluateRules(validSamples, oneMinuteSamples, newEvent, out);
    }

    private void evaluateRules(
            List<SensorReadingEvent> fiveMinSamples,
            List<SensorReadingEvent> oneMinSamples,
            SensorReadingEvent currentEvent,
            Collector<SensorViolationEvent> out
    ) throws Exception {
        // Enforce bootstrapping - skip evaluation if we don't have enough history
        if (fiveMinSamples.size() < 240) {
            return;
        }

        Double recipeMin = currentEvent.getRecipeMin();
        Double recipeMax = currentEvent.getRecipeMax();

        RuleResult finalResult = null;
        int activeSampleCount = fiveMinSamples.size();

        // 1. Nelson Rule 1 Evaluation (Recipe limit violations in last 5 minutes)
        RuleResult nelson1Result = evaluateNelsonRule1(fiveMinSamples, recipeMin, recipeMax);
        if (nelson1Result.isDetected()) {
            finalResult = nelson1Result;
        } else {
            // 2. Nelson Rule 3 Evaluation (Continual trend in last 1 minute)
            if (oneMinSamples.size() >= 42) {
                RuleResult nelson3Result = evaluateNelsonRule3(oneMinSamples, recipeMin, recipeMax);
                if (nelson3Result.isDetected()) {
                    finalResult = nelson3Result;
                    activeSampleCount = oneMinSamples.size();
                }
            }

            // 3. Bias Rule Evaluation (Mean shift in last 5 minutes)
            if (finalResult == null) {
                RuleResult biasResult = evaluateBiasRule(fiveMinSamples, recipeMin, recipeMax);
                if (biasResult.isDetected()) {
                    finalResult = biasResult;
                }
            }
        }

        // 4. State Machine & State Change Deduplication
        Severity lastSeverity = lastSeverityState.value();
        Severity currentSeverity = (finalResult != null) ? finalResult.getSeverity() : null;

        if (currentSeverity != lastSeverity) {
            lastSeverityState.update(currentSeverity);

            if (currentSeverity != null) {
                // Transition to an anomalous state: emit violation event
                out.collect(SensorViolationEvent.from(currentEvent, finalResult, activeSampleCount));
            } else {
                // Transition back to normal (recovery): emit recovery event (severity is null)
                SensorViolationEvent recoveryEvent = SensorViolationEvent.builder()
                        .equipmentId(currentEvent.getEquipmentId())
                        .sensorId(currentEvent.getSensorId())
                        .sensorType(currentEvent.getSensorType())
                        .severity(null) // null indicates normal recovery
                        .detectedAt(java.time.Instant.ofEpochMilli(currentEvent.getMeasuredAtEpochMilli()))
                        .reason("정상 범위 복구 (물리적 복구)")
                        .build();
                out.collect(recoveryEvent);
            }
        }
    }

    private RuleResult evaluateNelsonRule1(
            List<SensorReadingEvent> samples,
            Double recipeMin,
            Double recipeMax
    ) {
        if (samples == null || samples.isEmpty()) {
            return RuleResult.normal();
        }

        List<SensorReadingEvent> violatedSamples = new ArrayList<>();
        for (SensorReadingEvent sample : samples) {
            if (sample.getValue() != null && isOutOfRecipeRange(sample.getValue(), recipeMin, recipeMax)) {
                violatedSamples.add(sample);
            }
        }

        if (violatedSamples.isEmpty()) {
            return RuleResult.normal();
        }

        SensorReadingEvent latestViolated = violatedSamples.get(violatedSamples.size() - 1);
        Double value = latestViolated.getValue();
        boolean high = recipeMax != null && value > recipeMax;

        Double referenceValue = high ? recipeMax : recipeMin;
        Double deviation = Math.abs(value - referenceValue);
        Double deviationRate = (referenceValue != null && referenceValue != 0.0)
                ? (deviation / referenceValue) * 100.0
                : 0.0;

        Severity severity = violatedSamples.size() >= 3 ? Severity.CRITICAL : Severity.CAUTION;
        AnomalyType anomalyType = high ? AnomalyType.HIGH : AnomalyType.LOW;

        return RuleResult.detected(
                RuleName.NELSON_RULE_1,
                severity,
                anomalyType,
                value,
                referenceValue,
                deviation,
                deviationRate,
                String.format("최근 5분 내 Recipe 기준 이탈 %d회 발생. 측정값 %.3f (기준 %.3f)",
                        violatedSamples.size(), value, referenceValue)
        );
    }

    private boolean isOutOfRecipeRange(Double value, Double recipeMin, Double recipeMax) {
        if (recipeMin != null && value < recipeMin) return true;
        return recipeMax != null && value > recipeMax;
    }

    private RuleResult evaluateNelsonRule3(
            List<SensorReadingEvent> oneMinSamples,
            Double recipeMin,
            Double recipeMax
    ) {
        if (oneMinSamples.size() < 42) {
            return RuleResult.normal();
        }

        int increaseCount = 0;
        int decreaseCount = 0;
        int totalIntervals = 0;

        for (int i = 1; i < oneMinSamples.size(); i++) {
            Double prevVal = oneMinSamples.get(i - 1).getValue();
            Double curVal = oneMinSamples.get(i).getValue();

            if (prevVal != null && curVal != null) {
                if (curVal > prevVal) {
                    increaseCount++;
                } else if (curVal < prevVal) {
                    decreaseCount++;
                }
                totalIntervals++;
            }
        }

        if (totalIntervals == 0) return RuleResult.normal();

        double increaseRatio = (double) increaseCount / totalIntervals;
        double decreaseRatio = (double) decreaseCount / totalIntervals;

        Double firstValue = oneMinSamples.get(0).getValue();
        Double lastValue = oneMinSamples.get(oneMinSamples.size() - 1).getValue();

        if (firstValue == null || lastValue == null || firstValue == 0.0) {
            return RuleResult.normal();
        }

        double changeRate = Math.abs((lastValue - firstValue) / firstValue) * 100.0;

        boolean trendUp = increaseRatio >= 0.7;
        boolean trendDown = decreaseRatio >= 0.7;

        if ((!trendUp && !trendDown) || changeRate < 5.0) {
            return RuleResult.normal();
        }

        AnomalyType anomalyType = trendUp ? AnomalyType.TREND_UP : AnomalyType.TREND_DOWN;
        Double referenceValue = trendUp ? recipeMax : recipeMin;

        Severity severity = isNearLimit(trendUp, lastValue, recipeMin, recipeMax)
                ? Severity.CRITICAL
                : Severity.CAUTION;

        return RuleResult.detected(
                RuleName.NELSON_RULE_3,
                severity,
                anomalyType,
                lastValue,
                referenceValue,
                Math.abs(lastValue - firstValue),
                changeRate,
                String.format("최근 1분 내 %s 추세 감지. 방향 비율 %.2f, 변화율 %.2f%%",
                        trendUp ? "증가" : "감소", trendUp ? increaseRatio : decreaseRatio, changeRate)
        );
    }

    private boolean isNearLimit(boolean trendUp, Double currentValue, Double recipeMin, Double recipeMax) {
        if (currentValue == null || recipeMin == null || recipeMax == null) return false;
        double range = recipeMax - recipeMin;
        if (range <= 0.0) return false;

        if (trendUp) {
            return (currentValue - recipeMin) / range >= 0.9;
        } else {
            return (recipeMax - currentValue) / range >= 0.9;
        }
    }

    private RuleResult evaluateBiasRule(
            List<SensorReadingEvent> samples,
            Double recipeMin,
            Double recipeMax
    ) {
        if (samples.size() < 240) {
            return RuleResult.normal();
        }

        if (recipeMin == null || recipeMax == null || recipeMax <= recipeMin) {
            return RuleResult.normal();
        }

        double mean = (recipeMin + recipeMax) / 2.0;
        double sigma = (recipeMax - recipeMin) * 0.167; // 6-sigma Cp=1 baseline

        int upper1Count = 0;
        int lower1Count = 0;
        int upper2Count = 0;
        int lower2Count = 0;
        int totalCount = 0;

        for (SensorReadingEvent sample : samples) {
            Double val = sample.getValue();
            if (val != null) {
                if (val > mean + (sigma * 2)) upper2Count++;
                if (val < mean - (sigma * 2)) lower2Count++;
                if (val > mean + sigma) upper1Count++;
                if (val < mean - sigma) lower1Count++;
                totalCount++;
            }
        }

        if (totalCount == 0) return RuleResult.normal();

        double u2Ratio = (double) upper2Count / totalCount;
        double l2Ratio = (double) lower2Count / totalCount;
        double u1Ratio = (double) upper1Count / totalCount;
        double l1Ratio = (double) lower1Count / totalCount;

        if (u2Ratio >= 0.8) return buildBiasResult(Severity.CRITICAL, AnomalyType.BIAS_UP, samples, mean + (sigma * 2), u2Ratio);
        if (l2Ratio >= 0.8) return buildBiasResult(Severity.CRITICAL, AnomalyType.BIAS_DOWN, samples, mean - (sigma * 2), l2Ratio);
        if (u1Ratio >= 0.8) return buildBiasResult(Severity.CAUTION, AnomalyType.BIAS_UP, samples, mean + sigma, u1Ratio);
        if (l1Ratio >= 0.8) return buildBiasResult(Severity.CAUTION, AnomalyType.BIAS_DOWN, samples, mean - sigma, l1Ratio);

        return RuleResult.normal();
    }

    private RuleResult buildBiasResult(
            Severity severity,
            AnomalyType anomalyType,
            List<SensorReadingEvent> samples,
            Double referenceValue,
            double ratio
    ) {
        Double latestValue = samples.get(samples.size() - 1).getValue();
        Double deviation = Math.abs(latestValue - referenceValue);

        return RuleResult.detected(
                RuleName.BIAS_RATIO_RULE,
                severity,
                anomalyType,
                latestValue,
                referenceValue,
                deviation,
                ratio * 100.0,
                String.format("최근 5분 데이터 중 %.2f%%가 공정 중심값에서 한쪽으로 편향됨", ratio * 100.0)
        );
    }
}
