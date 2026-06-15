package com.factory.flink.serialization;

import com.factory.flink.domain.dto.SensorViolationEvent;
import com.factory.flink.domain.enums.AnomalyType;
import com.factory.flink.domain.enums.RuleName;
import com.factory.flink.domain.enums.Severity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SensorViolationEnvelopeSerializerTest {

    private final SensorViolationEnvelopeSerializer serializer = new SensorViolationEnvelopeSerializer();
    private final ObjectMapper mapper = new ObjectMapper();

    private SensorViolationEvent sampleEvent() {
        return SensorViolationEvent.builder()
                .equipmentId(1L)
                .sensorId("TEMP_01")
                .sensorType("TEMP")
                .ruleName(RuleName.NELSON_RULE_1)
                .anomalyType(AnomalyType.HIGH)
                .severity(Severity.CRITICAL)
                .measuredValue(35.0).referenceValue(30.0)
                .deviation(5.0).deviationRate(16.67)
                .min(10.0).max(30.0)
                .detectedAt(Instant.parse("2026-06-14T00:15:26Z"))
                .windowStart(Instant.parse("2026-06-14T00:10:26Z"))
                .windowEnd(Instant.parse("2026-06-14T00:15:26Z"))
                .sampleCount(240)
                .reason("최근 5분 내 Recipe 기준 이탈")
                .build();
    }

    @Test
    void wrapsViolationUnderPayloadKeyWithMetadata() throws Exception {
        JsonNode json = mapper.readTree(serializer.serialize(sampleEvent()));

        // envelope metadata at root
        assertThat(json.has("idempotencyKey")).isTrue();
        assertThat(json.get("eventType").asText()).isEqualTo("SensorViolation");
        assertThat(json.get("envelopeType").asText()).isEqualTo("domain");
        assertThat(json.get("aggregateType").asText()).isEqualTo("Equipment");
        assertThat(json.get("aggregateId").asText()).isEqualTo("1");
        assertThat(json.has("timestamp")).isTrue();
        // a trace id is always minted for correlation (UUID, like MdcTraceIdProvider fallback)
        assertThat(json.get("traceId").asText()).isNotBlank();

        // violation lives under the payload key (this is what the consumer extracts)
        assertThat(json.has("payload")).isTrue();
        JsonNode payload = json.get("payload");
        assertThat(payload.get("equipmentId").asLong()).isEqualTo(1L);
        assertThat(payload.get("sensorId").asText()).isEqualTo("TEMP_01");
        assertThat(payload.get("severity").asText()).isEqualTo("CRITICAL");
        assertThat(payload.get("ruleName").asText()).isEqualTo("NELSON_RULE_1");
        assertThat(payload.get("sampleCount").asInt()).isEqualTo(240);
        assertThat(payload.has("windowStart")).isTrue();
        assertThat(payload.has("windowEnd")).isTrue();
        // dates are ISO strings, not epoch numbers
        assertThat(payload.get("detectedAt").asText()).startsWith("2026-06-14T00:15:26");
    }

    @Test
    void idempotencyKeyIsDeterministicForSameEvent() throws Exception {
        JsonNode a = mapper.readTree(serializer.serialize(sampleEvent()));
        JsonNode b = mapper.readTree(serializer.serialize(sampleEvent()));
        // Same logical event -> same key, so the consumer can drop redeliveries.
        assertThat(a.get("idempotencyKey").asText()).isEqualTo(b.get("idempotencyKey").asText());
        long ts = Instant.parse("2026-06-14T00:15:26Z").toEpochMilli();
        assertThat(a.get("idempotencyKey").asText()).isEqualTo("1:TEMP:CRITICAL:" + ts);
    }

    @Test
    void recoveryEventSerializesWithNullSeverityAndRecoveryKey() throws Exception {
        SensorViolationEvent recovery = SensorViolationEvent.builder()
                .equipmentId(1L).sensorId("TEMP_01").sensorType("TEMP")
                .severity(null) // recovery
                .detectedAt(Instant.parse("2026-06-14T00:20:00Z"))
                .reason("정상 범위 복구 (물리적 복구)")
                .build();

        JsonNode json = mapper.readTree(serializer.serialize(recovery));
        assertThat(json.get("payload").get("severity").isNull()).isTrue();
        long ts = Instant.parse("2026-06-14T00:20:00Z").toEpochMilli();
        assertThat(json.get("idempotencyKey").asText()).isEqualTo("1:TEMP:RECOVERY:" + ts);
    }
}
