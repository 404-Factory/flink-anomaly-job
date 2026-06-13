package com.factory.flink.dto;

import java.io.Serializable;
import java.util.List;

public record SensorDataBatchDto(
    String batchId,
    String deviceId,
    Long equipmentId,
    List<MeasurementDto> measurements,
    String createdAt,
    Integer intervalSec
) implements Serializable {
    private static final long serialVersionUID = 1L;
}
