package com.factory.flink.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.factory.flink.domain.dto.SensorDataBatchDto;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class SensorDataBatchDeserializerTest {

    // Real payload shape: numeric "id" is the equipment Long id (-> equipmentId),
    // the String "equipmentId" is unused and must be ignored, deviceId is its own field.
    private static final String VALID_JSON = """
            {
              "batchId": "B1",
              "deviceId": "D1",
              "id": 1,
              "equipmentId": "EQP-001",
              "createdAt": "2026-06-13T00:00:00Z",
              "intervalSec": 5,
              "measurements": [
                {"sequence": 0, "measuredAt": "2026-06-13T00:00:00Z", "status": "OK",
                 "sensors": [{"sensorId": "S1", "sensorType": "TEMP", "value": 21.0,
                              "recipeMin": 10.0, "recipeMax": 30.0, "unit": "C", "status": "OK"}]}
              ]
            }
            """;

    private static final class ListCollector implements Collector<SensorDataBatchDto> {
        final List<SensorDataBatchDto> items = new ArrayList<>();
        @Override public void collect(SensorDataBatchDto r) { items.add(r); }
        @Override public void close() { }
    }

    private byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void parsesValidPayload() throws IOException {
        SensorDataBatchDto dto = new SensorDataBatchDeserializer().deserialize(bytes(VALID_JSON));
        assertThat(dto.getBatchId()).isEqualTo("B1");
        assertThat(dto.getEquipmentId()).isEqualTo(1L);
        assertThat(dto.getMeasurements()).hasSize(1);
        assertThat(dto.getMeasurements().get(0).getSensors().get(0).getSensorType()).isEqualTo("TEMP");
    }

    @Test
    void ignoresUnknownProperties() throws IOException {
        String json = "{\"batchId\":\"B\",\"surprise\":123}";
        SensorDataBatchDto dto = new SensorDataBatchDeserializer().deserialize(bytes(json));
        assertThat(dto.getBatchId()).isEqualTo("B");
    }

    @Test
    void collectorEmitsValidRecord() throws IOException {
        ListCollector out = new ListCollector();
        new SensorDataBatchDeserializer().deserialize(bytes(VALID_JSON), out);
        assertThat(out.items).hasSize(1);
    }

    @Test
    void collectorSkipsPoisonMessageWithoutThrowing() throws IOException {
        ListCollector out = new ListCollector();
        new SensorDataBatchDeserializer().deserialize(bytes("}{ not json"), out);
        assertThat(out.items).isEmpty();
    }

    @Test
    void collectorIgnoresNullAndEmptyMessages() throws IOException {
        ListCollector out = new ListCollector();
        SensorDataBatchDeserializer d = new SensorDataBatchDeserializer();
        d.deserialize(null, out);
        d.deserialize(new byte[0], out);
        assertThat(out.items).isEmpty();
    }

    @Test
    void rawDeserializeThrowsOnInvalidJson() {
        assertThatThrownBy(() -> new SensorDataBatchDeserializer().deserialize(bytes("nope")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void collectorSkipsJsonNullLiteral() throws IOException {
        // Jackson maps the literal `null` to a null DTO -> nothing collected.
        ListCollector out = new ListCollector();
        new SensorDataBatchDeserializer().deserialize(bytes("null"), out);
        assertThat(out.items).isEmpty();
    }

    @Test
    void reusesMapperAcrossCalls() throws IOException {
        // Second call on the same instance exercises the already-initialized path.
        SensorDataBatchDeserializer d = new SensorDataBatchDeserializer();
        assertThat(d.deserialize(bytes(VALID_JSON)).getBatchId()).isEqualTo("B1");
        assertThat(d.deserialize(bytes(VALID_JSON)).getBatchId()).isEqualTo("B1");
    }
}
