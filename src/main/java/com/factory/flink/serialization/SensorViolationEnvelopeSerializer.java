package com.factory.flink.serialization;

import com.factory.flink.dto.EventEnvelope;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.dto.SensorViolationPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.UUID;
import org.apache.flink.api.common.serialization.SerializationSchema;

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
            String severityStr = element.getSeverity() != null ? element.getSeverity().name() : "RECOVERY";
            String idempotencyKey = element.getEquipmentId() + ":" + element.getSensorType() + ":" + severityStr + ":" + element.getDetectedAt().toEpochMilli();

            EventEnvelope<SensorViolationPayload> envelope = EventEnvelope.<SensorViolationPayload>builder()
                .idempotencyKey(idempotencyKey)
                .eventType("SensorViolation")
                .envelopeType("domain")
                .aggregateType("Equipment")
                .aggregateId(String.valueOf(element.getEquipmentId()))
                .timestamp(Instant.now())
                .payload(SensorViolationPayload.from(element))
                .build();
            return objectMapper.writeValueAsBytes(envelope);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize SensorViolation envelope", e);
        }
    }

    private static ObjectMapper buildMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
