package com.factory.flink.dto;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorDataBatchDto implements Serializable {
    private static final long serialVersionUID = 1L;

    private String batchId;
    private String deviceId;
    private String equipmentId;
    private List<MeasurementDto> measurements;
    private Instant createdAt;
    private Integer intervalSec;
}
