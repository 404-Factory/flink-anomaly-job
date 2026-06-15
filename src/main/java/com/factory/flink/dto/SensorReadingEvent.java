package com.factory.flink.dto;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SensorReadingEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String batchId;
    private Long equipmentId;
    private String sensorId;
    private String sensorType;
    private Double value;
    private Double recipeMin;
    private Double recipeMax;
    private long measuredAtEpochMilli;
}
