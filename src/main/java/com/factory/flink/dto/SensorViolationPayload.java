package com.factory.flink.dto;

import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The violation domain payload carried inside an {@link EventEnvelope}.
 *
 * <p>Mirrors {@link SensorViolationEvent} but flattens the enums to strings for a stable,
 * language-neutral JSON contract with the consumer (SensorViolationConsumer).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorViolationPayload implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long equipmentId;
    private String sensorId;
    private String sensorType;
    private String ruleName;
    private String anomalyType;
    private String severity;
    private Double measuredValue;
    private Double referenceValue;
    private Double deviation;
    private Double deviationRate;
    private Double min;
    private Double max;
    private Instant detectedAt;
    private Instant windowStart;
    private Instant windowEnd;
    private Integer sampleCount;
    private String reason;

    public static SensorViolationPayload from(SensorViolationEvent event) {
        return SensorViolationPayload.builder()
                .equipmentId(event.getEquipmentId())
                .sensorId(event.getSensorId())
                .sensorType(event.getSensorType())
                .ruleName(event.getRuleName() != null ? event.getRuleName().name() : null)
                .anomalyType(event.getAnomalyType() != null ? event.getAnomalyType().name() : null)
                .severity(event.getSeverity() != null ? event.getSeverity().name() : null)
                .measuredValue(event.getMeasuredValue())
                .referenceValue(event.getReferenceValue())
                .deviation(event.getDeviation())
                .deviationRate(event.getDeviationRate())
                .min(event.getMin())
                .max(event.getMax())
                .detectedAt(event.getDetectedAt())
                .windowStart(event.getWindowStart())
                .windowEnd(event.getWindowEnd())
                .sampleCount(event.getSampleCount())
                .reason(event.getReason())
                .build();
    }
}
