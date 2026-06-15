package com.factory.flink.process.engine;

import com.factory.flink.dto.AnomalyType;
import com.factory.flink.dto.RuleName;
import com.factory.flink.dto.RuleResult;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.Severity;
import java.util.List;

public class Rule3Engine implements RuleEngine {

    @Override
    public RuleResult evaluate(List<SensorReadingEvent> samples, Double recipeMin,
        Double recipeMax) {
        if (samples == null || samples.size() < 42) {
            return RuleResult.normal();
        }

        int increaseCount = 0;
        int decreaseCount = 0;
        int totalIntervals = 0;

        for (int i = 1; i < samples.size(); i++) {
            Double prevVal = samples.get(i - 1).getValue();
            Double curVal = samples.get(i).getValue();

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

        Double firstValue = samples.get(0).getValue();
        Double lastValue = samples.get(samples.size() - 1).getValue();

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
}
