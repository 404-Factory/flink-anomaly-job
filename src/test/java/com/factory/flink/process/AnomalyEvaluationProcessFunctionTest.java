package com.factory.flink.process;

import com.factory.flink.dto.AnomalyType;
import com.factory.flink.dto.RuleName;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.Severity;
import com.factory.flink.dto.RuleResult;
import com.factory.flink.process.engine.Rule1Engine;
import com.factory.flink.process.engine.Rule3Engine;
import com.factory.flink.process.engine.BiasEngine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyEvaluationProcessFunctionTest {

    private static final double RECIPE_MIN = 29.0;
    private static final double RECIPE_MAX = 31.0;
    private static final long BASE_TIME = 1_749_686_400_000L;

    private final Rule1Engine rule1Engine = new Rule1Engine();
    private final Rule3Engine rule3Engine = new Rule3Engine();
    private final BiasEngine biasEngine = new BiasEngine();

    private SensorReadingEvent sample(double value, int i, Double min, Double max) {
        return SensorReadingEvent.builder()
                .equipmentId(1L).sensorId("S1").sensorType("Temperature")
                .value(value).recipeMin(min).recipeMax(max)
                .measuredAtEpochMilli(BASE_TIME + (long) i * 1000)
                .build();
    }

    private List<SensorReadingEvent> flat(int count, double value) {
        List<SensorReadingEvent> s = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            s.add(sample(value, i, RECIPE_MIN, RECIPE_MAX));
        }
        return s;
    }

    private List<SensorReadingEvent> ramp(int count, double start, double end) {
        List<SensorReadingEvent> s = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            double v = start + (end - start) * i / (count - 1);
            s.add(sample(v, i, RECIPE_MIN, RECIPE_MAX));
        }
        return s;
    }

    // ---- Nelson Rule 1 ----
    @Test
    void nelsonRule1HighCritical() throws Exception {
        List<SensorReadingEvent> s = new ArrayList<>(flat(237, 30.0));
        s.addAll(flat(3, 32.0)); // 3 above max -> CRITICAL HIGH
        RuleResult result = rule1Engine.evaluate(s, RECIPE_MIN, RECIPE_MAX);
        assertThat(result.isDetected()).isTrue();
        assertThat(result.getRuleName()).isEqualTo(RuleName.NELSON_RULE_1);
        assertThat(result.getAnomalyType()).isEqualTo(AnomalyType.HIGH);
        assertThat(result.getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void nelsonRule1LowCaution() throws Exception {
        List<SensorReadingEvent> s = new ArrayList<>(flat(238, 30.0));
        s.addAll(flat(2, 27.0)); // 2 below min -> CAUTION LOW
        RuleResult result = rule1Engine.evaluate(s, RECIPE_MIN, RECIPE_MAX);
        assertThat(result.isDetected()).isTrue();
        assertThat(result.getAnomalyType()).isEqualTo(AnomalyType.LOW);
        assertThat(result.getSeverity()).isEqualTo(Severity.CAUTION);
    }

    // ---- Nelson Rule 3 (trend) ----
    @Test
    void nelsonRule3TrendUpCritical() throws Exception {
        List<SensorReadingEvent> one = ramp(50, 29.0, 31.0);   // monotonic up, ends at max -> near-limit CRITICAL
        RuleResult result = rule3Engine.evaluate(one, RECIPE_MIN, RECIPE_MAX);
        assertThat(result.isDetected()).isTrue();
        assertThat(result.getRuleName()).isEqualTo(RuleName.NELSON_RULE_3);
        assertThat(result.getAnomalyType()).isEqualTo(AnomalyType.TREND_UP);
        assertThat(result.getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void nelsonRule3TrendDownCaution() throws Exception {
        List<SensorReadingEvent> one = ramp(50, 30.9, 29.3);   // down >5% change, not near min -> CAUTION
        RuleResult result = rule3Engine.evaluate(one, RECIPE_MIN, RECIPE_MAX);
        assertThat(result.isDetected()).isTrue();
        assertThat(result.getAnomalyType()).isEqualTo(AnomalyType.TREND_DOWN);
        assertThat(result.getSeverity()).isEqualTo(Severity.CAUTION);
    }

    @Test
    void nelsonRule3FlatNoTrend() throws Exception {
        List<SensorReadingEvent> one = flat(50, 30.0);         // flat -> changeRate 0 -> normal
        RuleResult result = rule3Engine.evaluate(one, RECIPE_MIN, RECIPE_MAX);
        assertThat(result.isDetected()).isFalse();
    }

    // ---- Bias rule ----
    @Test
    void biasUpCritical() throws Exception {
        List<SensorReadingEvent> s = flat(240, 30.8);          // > mean+2sigma, within range
        RuleResult result = biasEngine.evaluate(s, RECIPE_MIN, RECIPE_MAX);
        assertThat(result.isDetected()).isTrue();
        assertThat(result.getRuleName()).isEqualTo(RuleName.BIAS_RATIO_RULE);
        assertThat(result.getAnomalyType()).isEqualTo(AnomalyType.BIAS_UP);
        assertThat(result.getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void biasDownCaution() throws Exception {
        List<SensorReadingEvent> s = flat(240, 29.5);          // between mean-sigma and mean-2sigma
        RuleResult result = biasEngine.evaluate(s, RECIPE_MIN, RECIPE_MAX);
        assertThat(result.isDetected()).isTrue();
        assertThat(result.getAnomalyType()).isEqualTo(AnomalyType.BIAS_DOWN);
        assertThat(result.getSeverity()).isEqualTo(Severity.CAUTION);
    }

    // ---- null / edge guards ----
    @Test
    void biasReturnsNormalWhenRecipeMissing() throws Exception {
        List<SensorReadingEvent> s = flat(240, 30.5);
        RuleResult result = biasEngine.evaluate(s, null, null);
        assertThat(result.isDetected()).isFalse();
    }
}
