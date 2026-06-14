package com.factory.flink.process;

import com.factory.flink.dto.MeasurementDto;
import com.factory.flink.dto.SensorDataBatchDto;
import com.factory.flink.dto.SensorReadingDto;
import com.factory.flink.dto.SensorReadingEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class SensorDataBatchFlatMapFunction implements FlatMapFunction<SensorDataBatchDto, SensorReadingEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(SensorDataBatchDto batch, Collector<SensorReadingEvent> out) throws Exception {
        if (batch.measurements() == null) {
            return;
        }

        for (MeasurementDto measurement : batch.measurements()) {
            if (measurement.getMeasuredAt() == null || measurement.getSensors() == null) {
                continue;
            }

            long epochMilli = measurement.getMeasuredAt().toEpochMilli();

            for (SensorReadingDto sensor : measurement.getSensors()) {
                SensorReadingEvent event = SensorReadingEvent.builder()
                        .equipmentId(batch.equipmentId())
                        .sensorId(sensor.getSensorId())
                        .sensorType(sensor.getSensorType())
                        .value(sensor.getValue())
                        .recipeMin(sensor.getRecipeMin())
                        .recipeMax(sensor.getRecipeMax())
                        .measuredAtEpochMilli(epochMilli)
                        .build();
                out.collect(event);
            }
        }
    }
}
