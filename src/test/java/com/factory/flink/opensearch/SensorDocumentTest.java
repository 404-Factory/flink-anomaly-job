package com.factory.flink.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.factory.flink.dto.SensorRecord;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SensorDocumentTest {

    private SensorRecord full() {
        return SensorRecord.builder()
                .batchId("B1").deviceId("D1").equipmentId("EQP-1")
                .createdAtEpochMilli(1000L).intervalSec(5)
                .sequence(2).measuredAtEpochMilli(2000L)
                .measurementStatus("OK").sensorId("S1").sensorType("TEMP")
                .value(21.5).recipeMin(10.0).recipeMax(30.0).unit("C").sensorStatus("OK")
                .build();
    }

    @Test
    void idIsDeterministicEquipmentSensorMeasuredAt() {
        assertThat(SensorDocument.id(full())).isEqualTo("EQP-1_S1_2000");
    }

    @Test
    void identicalReadingProducesSameId() {
        assertThat(SensorDocument.id(full())).isEqualTo(SensorDocument.id(full()));
    }

    @Test
    void idToleratesNullEquipmentOrSensor() {
        SensorRecord r = SensorRecord.builder().measuredAtEpochMilli(7L).build();
        assertThat(SensorDocument.id(r)).isEqualTo("__7");
    }

    @Test
    void sourceMapsAllIndexedFields() {
        Map<String, Object> s = SensorDocument.source(full());
        assertThat(s.get("batchId")).isEqualTo("B1");
        assertThat(s.get("deviceId")).isEqualTo("D1");
        assertThat(s.get("equipmentId")).isEqualTo("EQP-1");
        assertThat(s.get("createdAt")).isEqualTo(1000L);
        assertThat(s.get("intervalSec")).isEqualTo(5);
        assertThat(s.get("sequence")).isEqualTo(2);
        assertThat(s.get("measuredAt")).isEqualTo(2000L);
        assertThat(s.get("status")).isEqualTo("OK");
        assertThat(s.get("sensorId")).isEqualTo("S1");
        assertThat(s.get("sensorType")).isEqualTo("TEMP");
        assertThat(s.get("value")).isEqualTo(21.5);
        assertThat(s.get("recipeMin")).isEqualTo(10.0);
        assertThat(s.get("recipeMax")).isEqualTo(30.0);
        assertThat(s.get("unit")).isEqualTo("C");
    }

    @Test
    void sourceKeepsNullableFieldsAsNull() {
        SensorRecord r = SensorRecord.builder().equipmentId("E").measuredAtEpochMilli(1L).build();
        Map<String, Object> s = SensorDocument.source(r);
        assertThat(s).containsKey("value").containsKey("batchId");
        assertThat(s.get("value")).isNull();
        assertThat(s.get("measuredAt")).isEqualTo(1L);
    }
}
