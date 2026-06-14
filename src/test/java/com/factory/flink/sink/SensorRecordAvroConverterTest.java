package com.factory.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.factory.flink.dto.SensorRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class SensorRecordAvroConverterTest {

    @Test
    void schemaDeclaresTimestampMillisOnTimeColumns() {
        Schema.Field measuredAt = SensorRecordAvroConverter.SCHEMA.getField("measuredAt");
        assertThat(measuredAt.schema().getLogicalType()).isNotNull();
        assertThat(measuredAt.schema().getLogicalType().getName()).isEqualTo("timestamp-millis");
    }

    @Test
    void schemaHasAllFifteenColumns() {
        assertThat(SensorRecordAvroConverter.SCHEMA.getFields()).hasSize(15);
    }

    @Test
    void mapsEveryFieldOntoGenericRecord() {
        SensorRecord r = SensorRecord.builder()
                .batchId("B").deviceId("D").equipmentId("E")
                .createdAtEpochMilli(1000L).intervalSec(5)
                .sequence(2).measuredAtEpochMilli(2000L).measurementStatus("OK")
                .sensorId("S1").sensorType("TEMP").value(21.5)
                .recipeMin(10.0).recipeMax(30.0).unit("C").sensorStatus("OK")
                .build();

        GenericRecord avro = SensorRecordAvroConverter.toAvro(r);

        assertThat(avro.get("batchId")).isEqualTo("B");
        assertThat(avro.get("deviceId")).isEqualTo("D");
        assertThat(avro.get("equipmentId")).isEqualTo("E");
        assertThat(avro.get("createdAt")).isEqualTo(1000L);
        assertThat(avro.get("intervalSec")).isEqualTo(5);
        assertThat(avro.get("sequence")).isEqualTo(2);
        assertThat(avro.get("measuredAt")).isEqualTo(2000L);
        assertThat(avro.get("measurementStatus")).isEqualTo("OK");
        assertThat(avro.get("sensorId")).isEqualTo("S1");
        assertThat(avro.get("sensorType")).isEqualTo("TEMP");
        assertThat(avro.get("value")).isEqualTo(21.5);
        assertThat(avro.get("recipeMin")).isEqualTo(10.0);
        assertThat(avro.get("recipeMax")).isEqualTo(30.0);
        assertThat(avro.get("unit")).isEqualTo("C");
        assertThat(avro.get("sensorStatus")).isEqualTo("OK");
    }

    @Test
    void nullableFieldsBecomeNullNotError() {
        SensorRecord r = SensorRecord.builder()
                .createdAtEpochMilli(1L).measuredAtEpochMilli(2L).build();
        GenericRecord avro = SensorRecordAvroConverter.toAvro(r);
        assertThat(avro.get("batchId")).isNull();
        assertThat(avro.get("value")).isNull();
        assertThat(avro.get("intervalSec")).isNull();
        // mandatory longs still present
        assertThat(avro.get("createdAt")).isEqualTo(1L);
        assertThat(avro.get("measuredAt")).isEqualTo(2L);
    }
}
