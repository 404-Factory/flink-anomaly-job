package com.factory.flink.dto;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
