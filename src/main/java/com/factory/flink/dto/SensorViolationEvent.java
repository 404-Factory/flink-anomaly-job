package com.factory.flink.dto;

import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SensorViolationEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long equipmentId;
    private String sensorId;
    private String sensorType;
    private RuleName ruleName;
    private AnomalyType anomalyType;
    private Severity severity;
    private Double measuredValue;
    private Double referenceValue;
    private Double deviation;
    private Double deviationRate;
    private Double min;
    private Double max;
    private Instant detectedAt;
    private Integer sampleCount;
    private String reason;

    public static SensorViolationEvent from(SensorReadingEvent reading, RuleResult result, int sampleCount) {
        return SensorViolationEvent.builder()
                .equipmentId(reading.getEquipmentId())
                .sensorId(reading.getSensorId())
                .sensorType(reading.getSensorType())
                .ruleName(result.getRuleName())
                .anomalyType(result.getAnomalyType())
                .severity(result.getSeverity())
                .measuredValue(result.getMeasuredValue())
                .referenceValue(result.getReferenceValue())
                .deviation(result.getDeviation())
                .deviationRate(result.getDeviationRate())
                .min(reading.getRecipeMin())
                .max(reading.getRecipeMax())
                .detectedAt(Instant.ofEpochMilli(reading.getMeasuredAtEpochMilli()))
                .sampleCount(sampleCount)
                .reason(result.getReason())
                .build();
    }
}
