package com.factory.flink.domain.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Root payload of a Kafka message on {@code fab-semiconductor-001}.
 * A batch groups several measurement cycles for one device/equipment.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorDataBatchDto implements Serializable {
    private static final long serialVersionUID = 1L;

    private String batchId;
    private String deviceId;
    // Incoming JSON has a numeric "id" (the equipment's Long id) plus a separate String
    // "equipmentId" we don't use. Map the Long "id" here; the String "equipmentId" is
    // ignored (FAIL_ON_UNKNOWN_PROPERTIES=false). deviceId binds to its own JSON field.
    @JsonProperty("id")
    private Long equipmentId;
    private List<MeasurementDto> measurements;
    private Instant createdAt;
    private Integer intervalSec;
}
