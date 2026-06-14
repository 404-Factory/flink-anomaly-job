package com.factory.flink.serialization;

import com.factory.flink.dto.EventEnvelope;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.dto.SensorViolationPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serializes a violation as an {@link EventEnvelope}&lt;{@link SensorViolationPayload}&gt; JSON:
 * the violation fields live under a {@code "payload"} key, wrapped with dedup/tracing metadata.
 *
 * <p>The consumer extracts the {@code payload} node and indexes/persists it; {@code idempotencyKey}
 * lets it drop duplicate redeliveries.
 */
public class SensorViolationEnvelopeSerializer implements SerializationSchema<SensorViolationEvent> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = buildMapper();
    }

    @Override
    public byte[] serialize(SensorViolationEvent element) {
        if (objectMapper == null) {
            objectMapper = buildMapper();
        }
        try {
            EventEnvelope<SensorViolationPayload> envelope = EventEnvelope.<SensorViolationPayload>builder()
                    .idempotencyKey(idempotencyKey(element))
                    .eventType("SensorViolation")
                    .envelopeType("domain")
                    .aggregateType("Equipment")
                    .aggregateId(element.getEquipmentId())
                    .timestamp(Instant.now())
                    .payload(SensorViolationPayload.from(element))
                    .build();
            return objectMapper.writeValueAsBytes(envelope);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize SensorViolation envelope", e);
        }
    }

    /**
     * Deterministic identity of one state-transition violation, so a consumer can dedup Kafka
     * redeliveries: the same logical event always yields the same key. (The state machine already
     * emits at most one event per transition, so equipment:sensorType:severity:detectedAt is unique.)
     *
     * <p>NOTE: this intentionally replaces the previous {@code UUID.randomUUID()} key, which was
     * unique per call and therefore could not actually prevent duplicates.
     */
    static String idempotencyKey(SensorViolationEvent e) {
        String severity = e.getSeverity() != null ? e.getSeverity().name() : "RECOVERY";
        long detectedAt = e.getDetectedAt() != null ? e.getDetectedAt().toEpochMilli() : 0L;
        return e.getEquipmentId() + ":" + e.getSensorType() + ":" + severity + ":" + detectedAt;
    }

    private static ObjectMapper buildMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
