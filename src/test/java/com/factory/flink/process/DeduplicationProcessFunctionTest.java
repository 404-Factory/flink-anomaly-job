package com.factory.flink.process;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.factory.flink.domain.dto.SensorRecord;

class DeduplicationProcessFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, SensorRecord, SensorRecord> harness;

    private KeyedOneInputStreamOperatorTestHarness<String, SensorRecord, SensorRecord> open(
            DeduplicationProcessFunction fn) throws Exception {
        KeySelector<SensorRecord, String> key = SensorRecord::dedupKey;
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(fn), key, Types.STRING);
        harness.open();
        return harness;
    }

    private static SensorRecord reading(String batch, int seq, String sensor) {
        return SensorRecord.builder()
                .batchId(batch).equipmentId(1L).sequence(seq).sensorId(sensor).value(1.0).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void firstOccurrencePassesThrough() throws Exception {
        open(new DeduplicationProcessFunction(Duration.ofHours(1)));
        harness.processElement(new StreamRecord<>(reading("B", 0, "S1")));
        assertThat(harness.extractOutputValues()).hasSize(1);
    }

    @Test
    void duplicateWithinTtlIsDropped() throws Exception {
        open(new DeduplicationProcessFunction(Duration.ofHours(1)));
        harness.processElement(new StreamRecord<>(reading("B", 0, "S1")));
        harness.processElement(new StreamRecord<>(reading("B", 0, "S1")));
        assertThat(harness.extractOutputValues()).hasSize(1);
    }

    @Test
    void distinctReadingsAllPass() throws Exception {
        open(new DeduplicationProcessFunction(Duration.ofHours(1)));
        harness.processElement(new StreamRecord<>(reading("B", 0, "S1")));
        harness.processElement(new StreamRecord<>(reading("B", 0, "S2")));
        harness.processElement(new StreamRecord<>(reading("B", 1, "S1")));
        List<SensorRecord> out = harness.extractOutputValues();
        assertThat(out).hasSize(3);
    }

    @Test
    void duplicateReappearsAfterTtlExpiry() throws Exception {
        open(new DeduplicationProcessFunction(Duration.ofMillis(50)));
        harness.setStateTtlProcessingTime(0L);
        harness.processElement(new StreamRecord<>(reading("B", 0, "S1")));
        harness.setStateTtlProcessingTime(1_000L); // advance well beyond the 50ms TTL
        harness.processElement(new StreamRecord<>(reading("B", 0, "S1")));
        assertThat(harness.extractOutputValues()).hasSize(2);
    }

    @Test
    void defaultConstructorUsesPositiveTtl() throws Exception {
        open(new DeduplicationProcessFunction());
        harness.processElement(new StreamRecord<>(reading("B", 0, "S1")));
        assertThat(harness.extractOutputValues()).hasSize(1);
    }

    @Test
    void rejectsNonPositiveTtl() {
        assertThatThrownBy(() -> new DeduplicationProcessFunction(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeduplicationProcessFunction(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DeduplicationProcessFunction(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
