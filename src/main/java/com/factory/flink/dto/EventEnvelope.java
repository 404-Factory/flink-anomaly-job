package com.factory.flink.dto;

import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Generic event envelope: wraps a domain {@code payload} with delivery/tracing metadata.
 *
 * <p>The {@code payload} is nested under a {@code "payload"} key so consumers can extract it
 * deterministically, and {@code idempotencyKey} lets a consumer drop duplicate redeliveries
 * (Kafka at-least-once / job recovery) of the same logical event.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventEnvelope<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private String idempotencyKey;
    private String eventType;
    private String envelopeType;
    private String aggregateType;
    private String aggregateId;
    private String traceId;
    private Instant timestamp;
    private T payload;
}
