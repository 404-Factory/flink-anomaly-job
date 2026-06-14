package com.factory.flink.perf;

import static org.assertj.core.api.Assertions.assertThat;

import com.factory.flink.dto.MeasurementDto;
import com.factory.flink.dto.SensorDataBatchDto;
import com.factory.flink.dto.SensorReadingDto;
import com.factory.flink.dto.SensorRecord;
import com.factory.flink.opensearch.SensorDocument;
import com.factory.flink.process.SensorRecordFlatMapFunction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

/**
 * Performance (non-functional) test for the CPU hot path shared by every branch:
 * flatten → {@link SensorRecord} → OpenSearch document ({@code id} + {@code source}).
 *
 * <p>Asserts a conservative throughput floor so a regression that, say, makes the
 * mapping allocate excessively will fail CI, while staying non-flaky on slow agents.
 */
class IngestionThroughputTest {

    private static final class Counter implements Collector<SensorRecord> {
        long count;
        @Override public void collect(SensorRecord r) {
            // simulate the per-record downstream work of both heavy branches' shared root:
            // build the OpenSearch id + source for every flattened record.
            SensorDocument.id(r);
            SensorDocument.source(r);
            count++;
        }
        @Override public void close() { }
    }

    private SensorDataBatchDto batch(int sensors, int measurements, Instant t) {
        List<MeasurementDto> ms = new ArrayList<>();
        for (int m = 0; m < measurements; m++) {
            List<SensorReadingDto> ss = new ArrayList<>();
            for (int s = 0; s < sensors; s++) {
                ss.add(SensorReadingDto.builder()
                        .sensorId("S" + s).sensorType("TEMP").value(20.0 + s)
                        .recipeMin(10.0).recipeMax(30.0).unit("C").status("OK").build());
            }
            ms.add(MeasurementDto.builder().sequence(m).measuredAt(t.plusSeconds(m)).status("OK").sensors(ss).build());
        }
        return SensorDataBatchDto.builder()
                .batchId("B").deviceId("D").equipmentId(1L).createdAt(t).intervalSec(1).measurements(ms).build();
    }

    @Test
    void flattenAndDocumentMappingMeetsThroughputFloor() {
        SensorRecordFlatMapFunction flatten = new SensorRecordFlatMapFunction();
        Instant t = Instant.parse("2026-06-13T00:00:00Z");
        SensorDataBatchDto batch = batch(20, 10, t); // 200 readings per batch

        // warm up JIT
        for (int i = 0; i < 50; i++) {
            flatten.flatMap(batch, new Counter());
        }

        int batches = 5_000; // 5000 * 200 = 1,000,000 readings
        Counter counter = new Counter();
        long start = System.nanoTime();
        for (int i = 0; i < batches; i++) {
            flatten.flatMap(batch, counter);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        long records = counter.count;
        double perSec = records / Math.max(1.0, elapsedMs / 1000.0);
        System.out.printf("[perf] flatten+doc: %d records in %d ms = %.0f rec/s%n", records, elapsedMs, perSec);

        assertThat(records).isEqualTo((long) batches * 20 * 10);
        // Conservative floor (single thread, pure CPU). Real CI machines do >1M rec/s.
        assertThat(perSec).isGreaterThan(50_000.0);
    }
}
