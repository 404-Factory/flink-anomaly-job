package com.factory.flink.process;

import com.factory.flink.dto.AnomalyType;
import com.factory.flink.dto.RuleName;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.dto.Severity;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@code evaluateRules} directly with crafted sample windows to exercise every
 * rule branch (Nelson Rule 1 high/low + critical/caution, Nelson Rule 3 up/down +
 * near-limit, Bias up/down + critical/caution, bootstrapping, null guards).
 */
class AnomalyEvaluationProcessFunctionTest {

    private static final double RECIPE_MIN = 29.0;
    private static final double RECIPE_MAX = 31.0;
    private static final long BASE_TIME = 1_749_686_400_000L;
    private static final long DWELL_MS = 30_000L; // 30s downgrade-only dwell (= 10% of window)

    private List<SensorViolationEvent> violations;
    private Collector<SensorViolationEvent> collector;
    private final AnomalyEvaluationProcessFunction fn = new AnomalyEvaluationProcessFunction(DWELL_MS);

    @BeforeEach
    void setUp() {
        violations = new ArrayList<>();
        collector = new Collector<>() {
            @Override public void collect(SensorViolationEvent record) { violations.add(record); }
            @Override public void close() {}
        };
        // Inject in-memory state so evaluateRules can be driven without a full Flink operator
        // harness. Both start empty (last severity == Normal, no pending downgrade).
        fn.lastSeverityState = inMemorySeverityState();
        fn.downgradePendingSinceState = inMemoryLongState();
    }

    private ValueState<Severity> inMemorySeverityState() {
        return new ValueState<>() {
            private Severity value;
            @Override public Severity value() { return value; }
            @Override public void update(Severity value) { this.value = value; }
            @Override public void clear() { this.value = null; }
        };
    }

    private ValueState<Long> inMemoryLongState() {
        return new ValueState<>() {
            private Long value;
            @Override public Long value() { return value; }
            @Override public void update(Long value) { this.value = value; }
            @Override public void clear() { this.value = null; }
        };
    }

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

    private SensorReadingEvent current(Double min, Double max) {
        return sample(30.0, 999, min, max);
    }

    /** Current ("now") reading at an explicit second offset, to advance the dwell clock. */
    private SensorReadingEvent currentAt(long sec, Double min, Double max) {
        return SensorReadingEvent.builder()
                .equipmentId(1L).sensorId("S1").sensorType("Temperature")
                .value(30.0).recipeMin(min).recipeMax(max)
                .measuredAtEpochMilli(BASE_TIME + sec * 1000)
                .build();
    }

    // ---- bootstrapping ----
    @Test
    void skipsEvaluationBelow240Samples() throws Exception {
        List<SensorReadingEvent> s = flat(239, 32.0);
        fn.evaluateRules(s, s, s.get(s.size() - 1), collector);
        assertThat(violations).isEmpty();
    }

    @Test
    void normalSamplesProduceNoViolation() throws Exception {
        List<SensorReadingEvent> s = flat(240, 30.0);
        fn.evaluateRules(s, List.of(), current(RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).isEmpty();
    }

    // ---- Nelson Rule 1 ----
    @Test
    void nelsonRule1HighCritical() throws Exception {
        List<SensorReadingEvent> s = new ArrayList<>(flat(237, 30.0));
        s.addAll(flat(3, 32.0)); // 3 above max -> CRITICAL HIGH
        fn.evaluateRules(s, s, current(RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0).getRuleName()).isEqualTo(RuleName.NELSON_RULE_1);
        assertThat(violations.get(0).getAnomalyType()).isEqualTo(AnomalyType.HIGH);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void nelsonRule1LowCaution() throws Exception {
        List<SensorReadingEvent> s = new ArrayList<>(flat(238, 30.0));
        s.addAll(flat(2, 27.0)); // 2 below min -> CAUTION LOW
        fn.evaluateRules(s, List.of(), current(RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0).getAnomalyType()).isEqualTo(AnomalyType.LOW);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CAUTION);
    }

    // ---- Nelson Rule 3 (trend) ----
    @Test
    void nelsonRule3TrendUpCritical() throws Exception {
        List<SensorReadingEvent> five = flat(240, 30.0);       // no rule1, no bias
        List<SensorReadingEvent> one = ramp(50, 29.0, 31.0);   // monotonic up, ends at max -> near-limit CRITICAL
        fn.evaluateRules(five, one, current(RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0).getRuleName()).isEqualTo(RuleName.NELSON_RULE_3);
        assertThat(violations.get(0).getAnomalyType()).isEqualTo(AnomalyType.TREND_UP);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void nelsonRule3TrendDownCaution() throws Exception {
        List<SensorReadingEvent> five = flat(240, 30.0);
        List<SensorReadingEvent> one = ramp(50, 30.9, 29.3);   // down >5% change, not near min -> CAUTION
        fn.evaluateRules(five, one, current(RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0).getAnomalyType()).isEqualTo(AnomalyType.TREND_DOWN);
    }

    @Test
    void nelsonRule3FlatNoTrend() throws Exception {
        List<SensorReadingEvent> five = flat(240, 30.0);
        List<SensorReadingEvent> one = flat(50, 30.0);         // flat -> changeRate 0 -> normal
        fn.evaluateRules(five, one, current(RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).isEmpty();
    }

    // ---- Bias rule ----
    @Test
    void biasUpCritical() throws Exception {
        List<SensorReadingEvent> s = flat(240, 30.8);          // > mean+2sigma, within range
        fn.evaluateRules(s, List.of(), current(RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0).getRuleName()).isEqualTo(RuleName.BIAS_RATIO_RULE);
        assertThat(violations.get(0).getAnomalyType()).isEqualTo(AnomalyType.BIAS_UP);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void biasDownCaution() throws Exception {
        List<SensorReadingEvent> s = flat(240, 29.5);          // between mean-sigma and mean-2sigma
        fn.evaluateRules(s, List.of(), current(RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0).getAnomalyType()).isEqualTo(AnomalyType.BIAS_DOWN);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CAUTION);
    }

    // ---- null / edge guards ----
    @Test
    void biasReturnsNormalWhenRecipeMissing() throws Exception {
        List<SensorReadingEvent> s = flat(240, 30.5);
        // recipe null -> rule1 no range check, bias short-circuits to normal
        fn.evaluateRules(s, List.of(), current(null, null), collector);
        assertThat(violations).isEmpty();
    }

    // ---- state machine (req ①) ----
    private List<SensorReadingEvent> criticalWindow() {
        List<SensorReadingEvent> s = new ArrayList<>(flat(237, 30.0));
        s.addAll(flat(3, 32.0)); // 3 above max -> Nelson Rule 1 CRITICAL HIGH
        return s;
    }

    private List<SensorReadingEvent> cautionWindow() {
        List<SensorReadingEvent> s = new ArrayList<>(flat(238, 30.0));
        s.addAll(flat(2, 32.0)); // 2 above max -> Nelson Rule 1 CAUTION HIGH
        return s;
    }

    @Test
    void suppressesRepeatedSameSeverity() throws Exception {
        // Two consecutive evaluations of the same critical window emit only once.
        fn.evaluateRules(criticalWindow(), List.of(), current(RECIPE_MIN, RECIPE_MAX), collector);
        fn.evaluateRules(criticalWindow(), List.of(), current(RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void emitsRecoveryEventOnReturnToNormal() throws Exception {
        // CRITICAL, then normal sustained past the 30s dwell -> recovery confirmed.
        fn.evaluateRules(criticalWindow(), List.of(), currentAt(0, RECIPE_MIN, RECIPE_MAX), collector);
        for (long sec = 1; sec <= 31; sec++) { // 30s of continuous normal triggers at sec=31
            fn.evaluateRules(flat(240, 30.0), List.of(), currentAt(sec, RECIPE_MIN, RECIPE_MAX), collector);
        }

        assertThat(violations).hasSize(2);
        SensorViolationEvent recovery = violations.get(1);
        assertThat(recovery.getSeverity()).isNull(); // null severity == normal recovery
        assertThat(recovery.getReason()).contains("복구");
    }

    @Test
    void populatesWindowBounds() throws Exception {
        fn.evaluateRules(criticalWindow(), List.of(), current(RECIPE_MIN, RECIPE_MAX), collector);
        SensorViolationEvent v = violations.get(0);
        assertThat(v.getWindowEnd()).isEqualTo(v.getDetectedAt());
        assertThat(v.getWindowStart()).isBefore(v.getWindowEnd());
        assertThat(v.getSampleCount()).isEqualTo(240);
    }

    @Test
    void sustainedCriticalEmitsOnceDespiteManyEvaluations() throws Exception {
        // Steady state: 10 consecutive seconds of the same CRITICAL window -> 1 event.
        for (long sec = 0; sec < 10; sec++) {
            fn.evaluateRules(criticalWindow(), List.of(), currentAt(sec, RECIPE_MIN, RECIPE_MAX), collector);
        }
        assertThat(violations).hasSize(1);
    }

    // ---- downgrade-only dwell (flapping mitigation) --------------------------------
    // Escalations fire immediately; de-escalations must hold for the dwell before emitting.

    @Test
    void escalationIsNeverDamped() throws Exception {
        // Normal -> CAUTION -> CRITICAL on consecutive seconds: both fire immediately,
        // no dwell applies to worsening transitions.
        fn.evaluateRules(cautionWindow(), List.of(), currentAt(0, RECIPE_MIN, RECIPE_MAX), collector);
        fn.evaluateRules(criticalWindow(), List.of(), currentAt(1, RECIPE_MIN, RECIPE_MAX), collector);
        assertThat(violations).hasSize(2);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CAUTION);
        assertThat(violations.get(1).getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void dwellCollapsesCautionCriticalFlapping() throws Exception {
        // Value hovers at the CRITICAL threshold: CRITICAL,CAUTION,CRITICAL,CAUTION,... every
        // second. The downgrades never hold for 30s, so the burst collapses to a single event.
        for (long sec = 0; sec < 40; sec++) {
            List<SensorReadingEvent> window = (sec % 2 == 0) ? criticalWindow() : cautionWindow();
            fn.evaluateRules(window, List.of(), currentAt(sec, RECIPE_MIN, RECIPE_MAX), collector);
        }
        assertThat(violations).hasSize(1); // was 40 (one per transition) before the dwell
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void dwellCollapsesNormalBoundaryFlapping() throws Exception {
        // CAUTION,Normal,CAUTION,Normal,... every second: no recovery is ever confirmed,
        // so the backend session is opened once and never spuriously closed.
        for (long sec = 0; sec < 40; sec++) {
            List<SensorReadingEvent> window = (sec % 2 == 0) ? cautionWindow() : flat(240, 30.0);
            fn.evaluateRules(window, List.of(), currentAt(sec, RECIPE_MIN, RECIPE_MAX), collector);
        }
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CAUTION);
        assertThat(violations).noneMatch(v -> v.getSeverity() == null); // no spurious recovery
    }

    @Test
    void genuineDowngradeEmitsAfterDwellHold() throws Exception {
        // CRITICAL, then CAUTION sustained past 30s -> the downgrade is confirmed once.
        fn.evaluateRules(criticalWindow(), List.of(), currentAt(0, RECIPE_MIN, RECIPE_MAX), collector);
        for (long sec = 1; sec <= 31; sec++) {
            fn.evaluateRules(cautionWindow(), List.of(), currentAt(sec, RECIPE_MIN, RECIPE_MAX), collector);
        }
        assertThat(violations).hasSize(2);
        assertThat(violations.get(0).getSeverity()).isEqualTo(Severity.CRITICAL);
        assertThat(violations.get(1).getSeverity()).isEqualTo(Severity.CAUTION);
    }

}
