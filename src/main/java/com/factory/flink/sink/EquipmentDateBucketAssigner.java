package com.factory.flink.sink;

import com.factory.flink.dto.SensorRecord;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

/**
 * Buckets records into Hive-style partitions {@code dt=YYYY-MM-DD/device_id=<id>}
 * based on the reading's <b>event time</b> ({@code measuredAt}), not arrival time.
 * Partitioned by {@code deviceId} (a string id), keeping the numeric {@code equipmentId}
 * purely as data rather than a path token.
 *
 * <p>Two payoffs:
 * <ul>
 *   <li><b>Query cost</b> — engines prune whole partitions on {@code dt}/{@code device_id}
 *       predicates, scanning far less S3 data.</li>
 *   <li><b>Late data correctness</b> — a delayed message still lands in the partition
 *       of the day it was measured, so out-of-order arrival never corrupts the layout.</li>
 * </ul>
 *
 * <p>{@code deviceId} is sanitized to a safe partition token to prevent path
 * traversal / injection from untrusted upstream values.
 *
 * <p>The {@code dt} date is derived in <b>UTC</b> — matching the source timestamps
 * ({@code createdAt}/{@code measuredAt} are UTC) and the daily Glue jobs which pick
 * "yesterday" in UTC. Writer and reader must share the same zone, or the batch jobs
 * would target the wrong {@code dt=} partition near midnight.
 */
public class EquipmentDateBucketAssigner implements BucketAssigner<SensorRecord, String> {
    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    static final String UNKNOWN_EQUIPMENT = "unknown";

    @Override
    public String getBucketId(SensorRecord record, Context context) {
        return bucketId(record.getMeasuredAtEpochMilli(), record.getDeviceId());
    }

    /** Pure, testable bucket-path computation. */
    static String bucketId(long measuredAtEpochMilli, String deviceId) {
        String date = DATE_FMT.format(Instant.ofEpochMilli(measuredAtEpochMilli));
        return "dt=" + date + "/device_id=" + sanitize(deviceId);
    }

    /**
     * Reduces a device id to {@code [A-Za-z0-9._-]} so it cannot escape its
     * partition directory ({@code ../}, absolute paths, etc.).
     */
    static String sanitize(String raw) {
        if (raw == null || raw.isBlank()) {
            return UNKNOWN_EQUIPMENT;
        }
        String cleaned = raw.replaceAll("[^A-Za-z0-9._-]", "_");
        // Guard against tokens that are only dots (".", "..") after cleaning.
        return cleaned.chars().anyMatch(c -> c != '.') ? cleaned : UNKNOWN_EQUIPMENT;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
