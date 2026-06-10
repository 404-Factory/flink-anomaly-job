package com.factory.flink.dto;

import java.io.Serializable;

public record SensorReadingDto(
    String sensorId,
    String sensorType,
    Double value,
    Double recipeMin,
    Double recipeMax,
    String unit
) implements Serializable {
    private static final long serialVersionUID = 1L;
}
