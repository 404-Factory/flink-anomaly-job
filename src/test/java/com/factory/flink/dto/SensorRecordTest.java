package com.factory.flink.dto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SensorRecordTest {

    @Test
    void dedupKeyCombinesBatchEquipmentSequenceSensor() {
        SensorRecord r = SensorRecord.builder()
                .batchId("B1").equipmentId(1L).sequence(3).sensorId("S7").build();
        assertThat(r.dedupKey()).isEqualTo("B1|1|3|S7");
    }

    @Test
    void dedupKeyToleratesNullStringFields() {
        SensorRecord r = SensorRecord.builder().sequence(0).build();
        assertThat(r.dedupKey()).isEqualTo("||0|");
    }

    @Test
    void trueDuplicateSharesTheSameDedupKey() {
        // Retry resends the same payload (same batchId) -> same key -> deduped.
        SensorRecord a = SensorRecord.builder()
                .batchId("B").equipmentId(1L).sequence(1).sensorId("S").value(1.0).build();
        SensorRecord b = SensorRecord.builder()
                .batchId("B").equipmentId(1L).sequence(1).sensorId("S").value(1.0).build();
        assertThat(a.dedupKey()).isEqualTo(b.dedupKey());
    }

    @Test
    void differentBatchYieldsDifferentKey() {
        // A later reading is a new batch (new UUID batchId) -> distinct key, never dropped.
        SensorRecord first = SensorRecord.builder()
                .batchId("B-uuid-1").equipmentId(1L).sequence(1).sensorId("S").build();
        SensorRecord later = SensorRecord.builder()
                .batchId("B-uuid-2").equipmentId(1L).sequence(1).sensorId("S").build();
        assertThat(first.dedupKey()).isNotEqualTo(later.dedupKey());
    }
}
