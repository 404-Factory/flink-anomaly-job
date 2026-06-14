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

class SensorViolationSerializerTest {

    private final SensorViolationSerializer serializer = new SensorViolationSerializer();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void envelope_없이_payload_필드가_최상위에_직렬화된다() throws Exception {
        SensorViolationEvent event = SensorViolationEvent.builder()
            .equipmentId("EQP-CLEANING-001")
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

        assertThat(json.has("equipmentId")).isTrue();
        assertThat(json.get("equipmentId").asText()).isEqualTo("EQP-CLEANING-001");
        assertThat(json.get("ruleName").asText()).isEqualTo("NELSON_RULE_1");
        assertThat(json.get("anomalyType").asText()).isEqualTo("HIGH");
        assertThat(json.get("severity").asText()).isEqualTo("CAUTION");

        assertThat(json.has("payload")).isFalse();
        assertThat(json.has("idempotencyKey")).isFalse();
        assertThat(json.has("eventType")).isFalse();
    }

    @Test
    void open_초기화_후_재사용_경로도_직렬화한다() throws Exception {
        SensorViolationEvent event = SensorViolationEvent.builder()
                .equipmentId("EQP-2").sensorType("Pressure").ruleName(RuleName.BIAS_RATIO_RULE)
                .anomalyType(AnomalyType.BIAS_UP).severity(Severity.CRITICAL)
                .measuredValue(5.0).referenceValue(4.0)
                .detectedAt(Instant.parse("2026-06-12T00:00:00Z")).sampleCount(240).build();

        serializer.open(null); // initialize mapper via open()
        byte[] first = serializer.serialize(event);
        byte[] second = serializer.serialize(event); // reuse already-initialized mapper

        assertThat(mapper.readTree(first).get("equipmentId").asText()).isEqualTo("EQP-2");
        assertThat(second).isEqualTo(first);
    }
}
