package com.factory.flink.dto;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

public record SensorDataBatchDto(
    String batchId,
    String deviceId,
    String equipmentId,
    List<MeasurementDto> measurements,
    Instant createdAt,
    Integer intervalSec
) implements Serializable {
    private static final long serialVersionUID = 1L;
}
