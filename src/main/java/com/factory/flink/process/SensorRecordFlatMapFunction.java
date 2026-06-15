package com.factory.flink.process;

import java.time.Instant;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.factory.flink.domain.dto.MeasurementDto;
import com.factory.flink.domain.dto.SensorDataBatchDto;
import com.factory.flink.domain.dto.SensorReadingDto;
import com.factory.flink.domain.dto.SensorRecord;

/**
 * Explodes a nested {@link SensorDataBatchDto} into one {@link SensorRecord} per
 * (measurement, sensor) reading.
 *
 * <p>Defensive about partial/malformed data: null collections and individual
 * null elements are skipped instead of throwing, so one bad element never drops
 * the whole batch. When a measurement has no {@code measuredAt}, the batch's
 * {@code createdAt} is used as the event time so the row can still be
 * time-partitioned; a record with no usable timestamp at all is dropped (it
 * could not be placed in a {@code dt=} partition).
 */
public class SensorRecordFlatMapFunction implements FlatMapFunction<SensorDataBatchDto, SensorRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SensorRecordFlatMapFunction.class);

    @Override
    public void flatMap(SensorDataBatchDto batch, Collector<SensorRecord> out) {
        if (batch == null || batch.getMeasurements() == null) {
            return;
        }
        final Instant createdAt = batch.getCreatedAt();
        final long createdAtMs = createdAt != null ? createdAt.toEpochMilli() : 0L;

        for (MeasurementDto measurement : batch.getMeasurements()) {
            if (measurement == null || measurement.getSensors() == null) {
                continue;
            }

            final Instant measuredAt = measurement.getMeasuredAt() != null
                    ? measurement.getMeasuredAt()
                    : createdAt;
            if (measuredAt == null) {
                LOG.warn("Dropping measurement with no timestamp (batchId={})", batch.getBatchId());
                continue;
            }
            final long measuredAtMs = measuredAt.toEpochMilli();

            for (SensorReadingDto sensor : measurement.getSensors()) {
                if (sensor == null) {
                    continue;
                }
                out.collect(SensorRecord.builder()
                        .batchId(batch.getBatchId())
                        .deviceId(batch.getDeviceId())
                        .equipmentId(batch.getEquipmentId())
                        .createdAtEpochMilli(createdAtMs)
                        .intervalSec(batch.getIntervalSec())
                        .sequence(measurement.getSequence())
                        .measuredAtEpochMilli(measuredAtMs)
                        .measurementStatus(measurement.getStatus())
                        .sensorId(sensor.getSensorId())
                        .sensorType(sensor.getSensorType())
                        .value(sensor.getValue())
                        .recipeMin(sensor.getRecipeMin())
                        .recipeMax(sensor.getRecipeMax())
                        .unit(sensor.getUnit())
                        .sensorStatus(sensor.getStatus())
                        .build());
            }
        }
    }
}
