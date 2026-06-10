package com.factory.flink.process;

import com.factory.flink.dto.MeasurementDto;
import com.factory.flink.dto.SensorDataBatchDto;
import com.factory.flink.dto.SensorReadingDto;
import com.factory.flink.dto.SensorReadingEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.time.OffsetDateTime;

public class SensorDataBatchFlatMapFunction implements FlatMapFunction<SensorDataBatchDto, SensorReadingEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(SensorDataBatchDto batch, Collector<SensorReadingEvent> out) throws Exception {
        if (batch.measurements() == null) {
            return;
        }

        for (MeasurementDto measurement : batch.measurements()) {
            String measuredAt = measurement.measuredAt();
            long epochMilli;
            try {
                epochMilli = OffsetDateTime.parse(measuredAt).toInstant().toEpochMilli();
            } catch (Exception e) {
                continue;
            }

            if (measurement.sensors() == null) {
                continue;
            }

            for (SensorReadingDto sensor : measurement.sensors()) {
                SensorReadingEvent event = SensorReadingEvent.builder()
                        .equipmentId(batch.equipmentId())
                        .sensorId(sensor.sensorId())
                        .sensorType(sensor.sensorType())
                        .value(sensor.value())
                        .recipeMin(sensor.recipeMin())
                        .recipeMax(sensor.recipeMax())
                        .measuredAt(measuredAt)
                        .measuredAtEpochMilli(epochMilli)
                        .build();
                out.collect(event);
            }
        }
    }
}
