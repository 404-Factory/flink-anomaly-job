package com.factory.flink.process;

import com.factory.flink.dto.AnomalyType;
import com.factory.flink.dto.RuleName;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.dto.Severity;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyEvaluationProcessFunctionTest {

    private static final double RECIPE_MIN = 29.0;
    private static final double RECIPE_MAX = 31.0;
    private static final long BASE_TIME = 1_749_686_400_000L;

    private List<SensorViolationEvent> violations;
    private Collector<SensorViolationEvent> collector;

    @BeforeEach
    void setUp() {
        violations = new ArrayList<>();
        collector = new Collector<>() {
            @Override public void collect(SensorViolationEvent record) { violations.add(record); }
            @Override public void close() {}
        };
    }

    private List<SensorReadingEvent> buildSamples(int count, double value) {
        List<SensorReadingEvent> samples = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            samples.add(SensorReadingEvent.builder()
                .equipmentId("EQP-001").sensorType("Temperature")
                .value(value).recipeMin(RECIPE_MIN).recipeMax(RECIPE_MAX)
                .measuredAtEpochMilli(BASE_TIME + (long) i * 1000)
                .build());
        }
        return samples;
    }

    @Test
    void NelsonRule1_레시피_초과_샘플이_충분하면_위반이_감지된다() throws Exception {
        List<SensorReadingEvent> samples = new ArrayList<>();
        samples.addAll(buildSamples(237, 30.0));
        samples.addAll(buildSamples(3, 32.0));

        AnomalyEvaluationProcessFunction fn = new AnomalyEvaluationProcessFunction();
        fn.evaluateRules(samples, samples, samples.get(samples.size() - 1), collector);

        assertThat(violations).hasSize(1);
        assertThat(violations.get(0).getRuleName()).isEqualTo(RuleName.NELSON_RULE_1);
        assertThat(violations.get(0).getAnomalyType()).isEqualTo(AnomalyType.HIGH);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void 샘플이_240개_미만이면_룰_평가를_건너뛴다() throws Exception {
        List<SensorReadingEvent> samples = buildSamples(239, 32.0);

        AnomalyEvaluationProcessFunction fn = new AnomalyEvaluationProcessFunction();
        fn.evaluateRules(samples, samples, samples.get(samples.size() - 1), collector);

        assertThat(violations).isEmpty();
    }

    @Test
    void 정상_범위_내_샘플은_위반이_감지되지_않는다() throws Exception {
        List<SensorReadingEvent> samples = buildSamples(240, 30.0);

        AnomalyEvaluationProcessFunction fn = new AnomalyEvaluationProcessFunction();
        fn.evaluateRules(samples, List.of(), samples.get(samples.size() - 1), collector);

        assertThat(violations).isEmpty();
    }
}
