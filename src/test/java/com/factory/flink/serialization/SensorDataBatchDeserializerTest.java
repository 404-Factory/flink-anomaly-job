package com.factory.flink.serialization;

import com.factory.flink.dto.SensorDataBatchDto;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class SensorDataBatchDeserializerTest {

    private final SensorDataBatchDeserializer deserializer = new SensorDataBatchDeserializer();

    @Test
    void 배치_메시지가_정상적으로_역직렬화된다() throws Exception {
        byte[] json = """
            {
              "batchId": "batch-001",
              "deviceId": "device-001",
              "equipmentId": "EQP-CLEANING-001",
              "createdAt": "2026-06-12T00:15:00Z",
              "intervalSec": 1,
              "measurements": [
                {
                  "sequence": 1,
                  "measuredAt": "2026-06-12T00:15:01Z",
                  "status": "OK",
                  "sensors": [
                    {
                      "sensorId": "sensor-001",
                      "sensorType": "Chemical Temperature",
                      "value": 31.5,
                      "recipeMin": 29.0,
                      "recipeMax": 31.0
                    }
                  ]
                }
              ]
            }
            """.getBytes();

        SensorDataBatchDto result = deserializer.deserialize(json);

        assertThat(result.getEquipmentId()).isEqualTo("EQP-CLEANING-001");
        assertThat(result.getBatchId()).isEqualTo("batch-001");
        assertThat(result.getCreatedAt()).isEqualTo(Instant.parse("2026-06-12T00:15:00Z"));
        assertThat(result.getMeasurements()).hasSize(1);
        assertThat(result.getMeasurements().get(0).getSensors()).hasSize(1);
        assertThat(result.getMeasurements().get(0).getSensors().get(0).getValue()).isEqualTo(31.5);
    }

    @Test
    void 알수없는_필드가_있어도_역직렬화에_실패하지_않는다() {
        byte[] json = """
            {
              "batchId": "batch-001",
              "equipmentId": "EQP-001",
              "unknownField": "someValue",
              "measurements": []
            }
            """.getBytes();

        assertThatNoException().isThrownBy(() -> deserializer.deserialize(json));
    }
}
