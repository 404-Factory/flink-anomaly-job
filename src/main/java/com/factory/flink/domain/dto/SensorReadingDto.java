package com.factory.flink.domain.dto;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A single sensor reading inside a {@link MeasurementDto}.
 * Mirrors the JSON produced on the {@code fab-semiconductor-001} topic.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorReadingDto implements Serializable {
    private static final long serialVersionUID = 1L;

    private String sensorId;
    private String sensorType;
    private Double value;
    private Double recipeMin;
    private Double recipeMax;
    private String unit;
    private String status;
}
