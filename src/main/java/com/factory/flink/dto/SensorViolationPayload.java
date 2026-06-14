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
public class SensorViolationPayload {
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
            .sampleCount(event.getSampleCount())
            .reason(event.getReason())
            .build();
    }
}
