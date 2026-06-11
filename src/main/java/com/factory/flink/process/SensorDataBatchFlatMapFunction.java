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
            if (measurement.measuredAt() == null || measurement.sensors() == null) {
                continue;
            }

            long epochMilli = measurement.measuredAt().toEpochMilli();

            for (SensorReadingDto sensor : measurement.sensors()) {
                SensorReadingEvent event = SensorReadingEvent.builder()
                        .equipmentId(batch.equipmentId())
                        .sensorId(sensor.sensorId())
                        .sensorType(sensor.sensorType())
                        .value(sensor.value())
                        .recipeMin(sensor.recipeMin())
                        .recipeMax(sensor.recipeMax())
                        .measuredAtEpochMilli(epochMilli)
                        .build();
                out.collect(event);
            }
        }
    }
}
