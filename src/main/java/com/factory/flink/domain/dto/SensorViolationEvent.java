package com.factory.flink.domain.dto;

import java.io.Serializable;
import java.time.Instant;

import com.factory.flink.domain.enums.AnomalyType;
import com.factory.flink.domain.enums.RuleName;
import com.factory.flink.domain.enums.Severity;

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
    // Start/end of the sample window the rule was evaluated over (windowEnd == detection time).
    private Instant windowStart;
    private Instant windowEnd;
    private Integer sampleCount;
    private String reason;

    public static SensorViolationEvent from(SensorReadingEvent reading, RuleResult result, int sampleCount,
                                            Instant windowStart, Instant windowEnd) {
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
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .sampleCount(sampleCount)
                .reason(result.getReason())
                .build();
    }
}
