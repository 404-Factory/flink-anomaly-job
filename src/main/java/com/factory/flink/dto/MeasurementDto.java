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
public class MeasurementDto implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer sequence;
    private Instant measuredAt;
    private String status;
    private List<SensorReadingDto> sensors;
}
