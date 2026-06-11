package com.factory.flink.dto;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventEnvelope<T> {
    private String idempotencyKey;
    private String eventType;
    private String envelopeType;
    private String aggregateType;
    private String aggregateId;
    private String traceId;
    private Instant timestamp;
    private T payload;
}
