package com.factory.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.factory.flink.dto.SensorRecord;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DeviceDateBucketAssignerTest {

    @Test
    void buildsHiveStylePartitionFromEventTime() {
        long ts = Instant.parse("2026-06-13T15:04:05Z").toEpochMilli();
        assertThat(DeviceDateBucketAssigner.bucketId(ts, "EQP-001"))
                .isEqualTo("dt=2026-06-13/device_id=EQP-001");
    }

    @Test
    void usesUtcDateRegardlessOfTimeOfDay() {
        long ts = Instant.parse("2026-06-13T23:59:59Z").toEpochMilli();
        assertThat(DeviceDateBucketAssigner.bucketId(ts, "E")).startsWith("dt=2026-06-13/");
    }

    @Test
    void getBucketIdReadsFromRecord() {
        long ts = Instant.parse("2026-01-02T00:00:00Z").toEpochMilli();
        SensorRecord r = SensorRecord.builder().deviceId("D9").measuredAtEpochMilli(ts).build();
        DeviceDateBucketAssigner assigner = new DeviceDateBucketAssigner();
        assertThat(assigner.getBucketId(r, null))
                .isEqualTo("dt=2026-01-02/device_id=D9");
    }

    @Test
    void sanitizesPathTraversalAttempts() {
        assertThat(DeviceDateBucketAssigner.sanitize("../../etc/passwd"))
                .isEqualTo(".._.._etc_passwd");
        assertThat(DeviceDateBucketAssigner.sanitize("a/b\\c")).isEqualTo("a_b_c");
    }

    @Test
    void blankOrNullEquipmentFallsBackToUnknown() {
        assertThat(DeviceDateBucketAssigner.sanitize(null))
                .isEqualTo(DeviceDateBucketAssigner.UNKNOWN_DEVICE);
        assertThat(DeviceDateBucketAssigner.sanitize("   "))
                .isEqualTo(DeviceDateBucketAssigner.UNKNOWN_DEVICE);
    }

    @Test
    void dotsOnlyTokenFallsBackToUnknown() {
        assertThat(DeviceDateBucketAssigner.sanitize(".."))
                .isEqualTo(DeviceDateBucketAssigner.UNKNOWN_DEVICE);
        assertThat(DeviceDateBucketAssigner.sanitize("."))
                .isEqualTo(DeviceDateBucketAssigner.UNKNOWN_DEVICE);
    }

    @Test
    void keepsSafeCharacters() {
        assertThat(DeviceDateBucketAssigner.sanitize("EQP_001-A.v2")).isEqualTo("EQP_001-A.v2");
    }

    @Test
    void serializerIsProvided() {
        assertThat(new DeviceDateBucketAssigner().getSerializer()).isNotNull();
    }
}
