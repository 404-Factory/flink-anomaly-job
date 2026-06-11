package com.factory.flink.dto;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

public record MeasurementDto(
    Integer sequence,
    Instant measuredAt,
    String status,
    List<SensorReadingDto> sensors
) implements Serializable {
    private static final long serialVersionUID = 1L;
}
