package com.factory.flink.serialization;

import com.factory.flink.dto.AnomalyType;
import com.factory.flink.dto.RuleName;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.dto.Severity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SensorViolationEnvelopeSerializerTest {

    private final SensorViolationEnvelopeSerializer serializer = new SensorViolationEnvelopeSerializer();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void envelope_형식으로_정상_직렬화된다() throws Exception {
        SensorViolationEvent event = SensorViolationEvent.builder()
            .equipmentId(1L)
            .sensorId("EQP-01-SENSOR-01")
            .sensorType("Chemical Temperature")
            .ruleName(RuleName.NELSON_RULE_1)
            .anomalyType(AnomalyType.HIGH)
            .severity(Severity.CAUTION)
            .measuredValue(31.118)
            .referenceValue(31.0)
            .detectedAt(Instant.parse("2026-06-12T00:15:26.591Z"))
            .sampleCount(240)
            .build();

        byte[] bytes = serializer.serialize(event);
        JsonNode json = mapper.readTree(bytes);

        // Envelope fields
        assertThat(json.has("idempotencyKey")).isTrue();
        assertThat(json.get("eventType").asText()).isEqualTo("SensorViolation");
        assertThat(json.get("envelopeType").asText()).isEqualTo("domain");
        assertThat(json.get("aggregateType").asText()).isEqualTo("Equipment");
        assertThat(json.get("aggregateId").asText()).isEqualTo("1");
        assertThat(json.has("timestamp")).isTrue();

        // Payload fields
        assertThat(json.has("payload")).isTrue();
        JsonNode payload = json.get("payload");
        assertThat(payload.get("equipmentId").asLong()).isEqualTo(1L);
        assertThat(payload.get("sensorId").asText()).isEqualTo("EQP-01-SENSOR-01");
        assertThat(payload.get("ruleName").asText()).isEqualTo("NELSON_RULE_1");
        assertThat(payload.get("severity").asText()).isEqualTo("CAUTION");
    }
}
