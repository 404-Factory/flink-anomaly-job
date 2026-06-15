package com.factory.flink.process;

import static org.assertj.core.api.Assertions.assertThat;

import com.factory.flink.domain.dto.SensorReadingEvent;
import com.factory.flink.domain.dto.SensorRecord;
import org.junit.jupiter.api.Test;

class SensorRecordToReadingEventTest {

    @Test
    void projectsFieldsNeededByAnomalyEngine() {
        SensorRecord r = SensorRecord.builder()
                .batchId("B").deviceId("D").equipmentId(1L)
                .sensorId("S1").sensorType("TEMP").value(21.5)
                .recipeMin(10.0).recipeMax(30.0).measuredAtEpochMilli(2000L)
                .unit("C").sensorStatus("OK").build();

        SensorReadingEvent e = new SensorRecordToReadingEvent().map(r);

        assertThat(e.getEquipmentId()).isEqualTo(1L);
        assertThat(e.getSensorId()).isEqualTo("S1");
        assertThat(e.getSensorType()).isEqualTo("TEMP");
        assertThat(e.getValue()).isEqualTo(21.5);
        assertThat(e.getRecipeMin()).isEqualTo(10.0);
        assertThat(e.getRecipeMax()).isEqualTo(30.0);
        assertThat(e.getMeasuredAtEpochMilli()).isEqualTo(2000L);
    }
}
