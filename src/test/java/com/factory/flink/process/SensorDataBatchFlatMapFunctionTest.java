package com.factory.flink.process;

import com.factory.flink.dto.MeasurementDto;
import com.factory.flink.dto.SensorDataBatchDto;
import com.factory.flink.dto.SensorReadingDto;
import com.factory.flink.dto.SensorReadingEvent;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SensorDataBatchFlatMapFunctionTest {

    private final SensorDataBatchFlatMapFunction function = new SensorDataBatchFlatMapFunction();

    private List<SensorReadingEvent> collect(SensorDataBatchDto batch) throws Exception {
        List<SensorReadingEvent> results = new ArrayList<>();
        function.flatMap(batch, new Collector<>() {
            @Override public void collect(SensorReadingEvent record) { results.add(record); }
            @Override public void close() {}
        });
        return results;
    }

    @Test
    void 배치의_센서_데이터가_SensorReadingEvent로_변환된다() throws Exception {
        SensorDataBatchDto batch = SensorDataBatchDto.builder()
            .equipmentId("EQP-001")
            .measurements(List.of(
                MeasurementDto.builder()
                    .measuredAt(Instant.parse("2026-06-12T00:15:01Z"))
                    .sensors(List.of(
                        SensorReadingDto.builder()
                            .sensorId("s1").sensorType("Temperature")
                            .value(31.5).recipeMin(29.0).recipeMax(31.0)
                            .build(),
                        SensorReadingDto.builder()
                            .sensorId("s2").sensorType("Pressure")
                            .value(1.2).recipeMin(1.0).recipeMax(1.5)
                            .build()
                    ))
                    .build()
            ))
            .build();

        List<SensorReadingEvent> results = collect(batch);

        assertThat(results).hasSize(2);
        assertThat(results.get(0).getEquipmentId()).isEqualTo("EQP-001");
        assertThat(results.get(0).getSensorType()).isEqualTo("Temperature");
        assertThat(results.get(0).getValue()).isEqualTo(31.5);
        assertThat(results.get(1).getSensorType()).isEqualTo("Pressure");
    }

    @Test
    void measurements가_null이면_결과가_없다() throws Exception {
        SensorDataBatchDto batch = SensorDataBatchDto.builder()
            .equipmentId("EQP-001")
            .measurements(null)
            .build();

        assertThat(collect(batch)).isEmpty();
    }

    @Test
    void measuredAt이_null인_measurement는_스킵된다() throws Exception {
        SensorDataBatchDto batch = SensorDataBatchDto.builder()
            .equipmentId("EQP-001")
            .measurements(List.of(
                MeasurementDto.builder().measuredAt(null)
                    .sensors(List.of(SensorReadingDto.builder().sensorType("Temp").value(30.0).build()))
                    .build()
            ))
            .build();

        assertThat(collect(batch)).isEmpty();
    }
}
