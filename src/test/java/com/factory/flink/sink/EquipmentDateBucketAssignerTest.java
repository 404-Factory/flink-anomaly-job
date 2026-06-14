package com.factory.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.factory.flink.dto.SensorRecord;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class EquipmentDateBucketAssignerTest {

    @Test
    void buildsHiveStylePartitionFromEventTime() {
        long ts = Instant.parse("2026-06-13T15:04:05Z").toEpochMilli();
        assertThat(EquipmentDateBucketAssigner.bucketId(ts, "EQP-001"))
                .isEqualTo("dt=2026-06-13/equipment_id=EQP-001");
    }

    @Test
    void usesUtcDateRegardlessOfTimeOfDay() {
        long ts = Instant.parse("2026-06-13T23:59:59Z").toEpochMilli();
        assertThat(EquipmentDateBucketAssigner.bucketId(ts, "E")).startsWith("dt=2026-06-13/");
    }

    @Test
    void getBucketIdReadsFromRecord() {
        long ts = Instant.parse("2026-01-02T00:00:00Z").toEpochMilli();
        SensorRecord r = SensorRecord.builder().equipmentId("EQP-9").measuredAtEpochMilli(ts).build();
        EquipmentDateBucketAssigner assigner = new EquipmentDateBucketAssigner();
        assertThat(assigner.getBucketId(r, null))
                .isEqualTo("dt=2026-01-02/equipment_id=EQP-9");
    }

    @Test
    void sanitizesPathTraversalAttempts() {
        assertThat(EquipmentDateBucketAssigner.sanitize("../../etc/passwd"))
                .isEqualTo(".._.._etc_passwd");
        assertThat(EquipmentDateBucketAssigner.sanitize("a/b\\c")).isEqualTo("a_b_c");
    }

    @Test
    void blankOrNullEquipmentFallsBackToUnknown() {
        assertThat(EquipmentDateBucketAssigner.sanitize(null))
                .isEqualTo(EquipmentDateBucketAssigner.UNKNOWN_EQUIPMENT);
        assertThat(EquipmentDateBucketAssigner.sanitize("   "))
                .isEqualTo(EquipmentDateBucketAssigner.UNKNOWN_EQUIPMENT);
    }

    @Test
    void dotsOnlyTokenFallsBackToUnknown() {
        assertThat(EquipmentDateBucketAssigner.sanitize(".."))
                .isEqualTo(EquipmentDateBucketAssigner.UNKNOWN_EQUIPMENT);
        assertThat(EquipmentDateBucketAssigner.sanitize("."))
                .isEqualTo(EquipmentDateBucketAssigner.UNKNOWN_EQUIPMENT);
    }

    @Test
    void keepsSafeCharacters() {
        assertThat(EquipmentDateBucketAssigner.sanitize("EQP_001-A.v2")).isEqualTo("EQP_001-A.v2");
    }

    @Test
    void serializerIsProvided() {
        assertThat(new EquipmentDateBucketAssigner().getSerializer()).isNotNull();
    }
}
