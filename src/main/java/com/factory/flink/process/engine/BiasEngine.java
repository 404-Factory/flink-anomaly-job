package com.factory.flink.process.engine;

import com.factory.flink.dto.AnomalyType;
import com.factory.flink.dto.RuleName;
import com.factory.flink.dto.RuleResult;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.Severity;
import java.util.List;

public class BiasEngine implements RuleEngine {

    @Override
    public RuleResult evaluate(List<SensorReadingEvent> samples, Double recipeMin,
        Double recipeMax) {
        if (samples == null || samples.size() < 240) {
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
