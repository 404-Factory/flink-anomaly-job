package com.factory.flink.process;

import static org.assertj.core.api.Assertions.assertThat;

import com.factory.flink.dto.MeasurementDto;
import com.factory.flink.dto.SensorDataBatchDto;
import com.factory.flink.dto.SensorReadingDto;
import com.factory.flink.dto.SensorRecord;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class SensorRecordFlatMapFunctionTest {

    private final SensorRecordFlatMapFunction fn = new SensorRecordFlatMapFunction();

    /** Minimal in-memory collector. */
    private static final class ListCollector implements Collector<SensorRecord> {
        final List<SensorRecord> items = new ArrayList<>();
        @Override public void collect(SensorRecord r) { items.add(r); }
        @Override public void close() { }
    }

    private SensorReadingDto sensor(String id, Double value) {
        return SensorReadingDto.builder()
                .sensorId(id).sensorType("TEMP").value(value)
                .recipeMin(10.0).recipeMax(30.0).unit("C").status("OK").build();
    }

    @Test
    void explodesEveryMeasurementSensorPair() {
        Instant t = Instant.parse("2026-06-13T00:00:00Z");
        SensorDataBatchDto batch = SensorDataBatchDto.builder()
                .batchId("B1").deviceId("D1").equipmentId("EQP-1")
                .createdAt(t).intervalSec(5)
                .measurements(Arrays.asList(
                        MeasurementDto.builder().sequence(0).measuredAt(t).status("OK")
                                .sensors(Arrays.asList(sensor("S1", 21.0), sensor("S2", 22.0))).build(),
                        MeasurementDto.builder().sequence(1).measuredAt(t.plusSeconds(5)).status("OK")
                                .sensors(Collections.singletonList(sensor("S1", 23.0))).build()))
                .build();

        ListCollector out = new ListCollector();
        fn.flatMap(batch, out);

        assertThat(out.items).hasSize(3);
        SensorRecord first = out.items.get(0);
        assertThat(first.getBatchId()).isEqualTo("B1");
        assertThat(first.getEquipmentId()).isEqualTo("EQP-1");
        assertThat(first.getSensorId()).isEqualTo("S1");
        assertThat(first.getMeasuredAtEpochMilli()).isEqualTo(t.toEpochMilli());
        assertThat(first.getCreatedAtEpochMilli()).isEqualTo(t.toEpochMilli());
        assertThat(first.getIntervalSec()).isEqualTo(5);
    }

    @Test
    void nullBatchEmitsNothing() {
        ListCollector out = new ListCollector();
        fn.flatMap(null, out);
        assertThat(out.items).isEmpty();
    }

    @Test
    void nullMeasurementsEmitsNothing() {
        ListCollector out = new ListCollector();
        fn.flatMap(SensorDataBatchDto.builder().batchId("B").build(), out);
        assertThat(out.items).isEmpty();
    }

    @Test
    void skipsNullMeasurementAndNullSensorElements() {
        Instant t = Instant.parse("2026-06-13T00:00:00Z");
        List<SensorReadingDto> sensors = new ArrayList<>();
        sensors.add(sensor("S1", 1.0));
        sensors.add(null);
        SensorDataBatchDto batch = SensorDataBatchDto.builder()
                .batchId("B").equipmentId("E").createdAt(t)
                .measurements(Arrays.asList(
                        null,
                        MeasurementDto.builder().sequence(0).measuredAt(t).sensors(sensors).build()))
                .build();

        ListCollector out = new ListCollector();
        fn.flatMap(batch, out);
        assertThat(out.items).hasSize(1);
    }

    @Test
    void measurementWithNullSensorsListIsSkipped() {
        Instant t = Instant.parse("2026-06-13T00:00:00Z");
        SensorDataBatchDto batch = SensorDataBatchDto.builder()
                .batchId("B").equipmentId("E").createdAt(t)
                .measurements(Collections.singletonList(
                        MeasurementDto.builder().sequence(0).measuredAt(t).sensors(null).build()))
                .build();
        ListCollector out = new ListCollector();
        fn.flatMap(batch, out);
        assertThat(out.items).isEmpty();
    }

    @Test
    void fallsBackToCreatedAtWhenMeasuredAtMissing() {
        Instant created = Instant.parse("2026-06-13T01:02:03Z");
        SensorDataBatchDto batch = SensorDataBatchDto.builder()
                .batchId("B").equipmentId("E").createdAt(created)
                .measurements(Collections.singletonList(
                        MeasurementDto.builder().sequence(0).measuredAt(null)
                                .sensors(Collections.singletonList(sensor("S1", 1.0))).build()))
                .build();

        ListCollector out = new ListCollector();
        fn.flatMap(batch, out);
        assertThat(out.items).hasSize(1);
        assertThat(out.items.get(0).getMeasuredAtEpochMilli()).isEqualTo(created.toEpochMilli());
    }

    @Test
    void dropsMeasurementWhenNoTimestampAvailable() {
        SensorDataBatchDto batch = SensorDataBatchDto.builder()
                .batchId("B").equipmentId("E").createdAt(null)
                .measurements(Collections.singletonList(
                        MeasurementDto.builder().sequence(0).measuredAt(null)
                                .sensors(Collections.singletonList(sensor("S1", 1.0))).build()))
                .build();

        ListCollector out = new ListCollector();
        fn.flatMap(batch, out);
        assertThat(out.items).isEmpty();
    }
}
