package com.factory.flink.process.engine;

import com.factory.flink.dto.AnomalyType;
import com.factory.flink.dto.RuleName;
import com.factory.flink.dto.RuleResult;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.Severity;
import java.util.ArrayList;
import java.util.List;

public class Rule1Engine implements RuleEngine {

    @Override
    public RuleResult evaluate(List<SensorReadingEvent> samples, Double recipeMin,
        Double recipeMax) {
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
}
