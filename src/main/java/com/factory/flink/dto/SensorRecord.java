package com.factory.flink.dto;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Flattened, storage-oriented record: exactly one sensor reading.
 *
 * <p>A nested {@link SensorDataBatchDto} is exploded into one {@code SensorRecord}
 * per (measurement, sensor) pair. This columnar-friendly shape is what gets
 * written to Parquet, giving far better compression and query (Athena) cost than
 * the nested source JSON.
 *
 * <p>Timestamps are kept as epoch-millis ({@code long}) so they map cleanly onto
 * Avro/Parquet {@code timestamp-millis} logical types without timezone ambiguity.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    // ---- batch-level (denormalized onto every row) ----
    private String batchId;
    private String deviceId;
    private Long equipmentId;
    private long createdAtEpochMilli;
    private Integer intervalSec;

    // ---- measurement-level ----
    private Integer sequence;
    private long measuredAtEpochMilli;
    private String measurementStatus;

    // ---- sensor-level ----
    private String sensorId; // sensor id: equipment name - sensor name
    private String sensorType; // sensor type: TEMPERATURE SENSOR
    private Double value;
    private Double recipeMin;
    private Double recipeMax;
    private String unit;
    private String sensorStatus;

    /**
     * Deterministic business identity of a reading.
     *
     * <p>{@code batchId} is a fresh UUID per batch (never reused), so
     * {@code (batchId, sequence, sensorId)} already identifies one physical reading
     * uniquely across time — a later, different reading lands in a new batch with a
     * new {@code batchId} and therefore a different key. A true duplicate (Kafka
     * producer retry / source replay) resends the same payload, preserving
     * {@code batchId}, so its key matches and the copy is dropped exactly once.
     */
    public String dedupKey() {
        return n(batchId) + '|' + n(equipmentId) + '|' + sequence + '|' + n(sensorId);
    }

    private static String n(Object o) {
        return o == null ? "" : o.toString();
    }
}
