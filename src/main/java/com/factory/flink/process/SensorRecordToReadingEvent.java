package com.factory.flink.process;

import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorRecord;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Adapts the shared {@link SensorRecord} (flattened once) into the
 * {@link SensorReadingEvent} shape the anomaly rule engine consumes.
 *
 * <p>This keeps the flatten step shared across all branches: flatten runs once into
 * {@code SensorRecord} (a superset), and only the anomaly branch pays this cheap
 * projection — no duplicate flattening.
 */
public class SensorRecordToReadingEvent implements MapFunction<SensorRecord, SensorReadingEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public SensorReadingEvent map(SensorRecord r) {
        return SensorReadingEvent.builder()
                .equipmentId(r.getEquipmentId())
                .sensorId(r.getSensorId())
                .sensorType(r.getSensorType())
                .value(r.getValue())
                .recipeMin(r.getRecipeMin())
                .recipeMax(r.getRecipeMax())
                .measuredAtEpochMilli(r.getMeasuredAtEpochMilli())
                .build();
    }
}
