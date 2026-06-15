package com.factory.flink.sink;

import com.factory.flink.dto.SensorRecord;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

/**
 * Buckets records into {@code YYYY/MM/DD/<device_id>} partitions based on the
 * batch's <b>creation time</b> ({@code createdAt}). This mirrors the s3-consumer
 * layout ({@code YYYY/MM/DD/<equipmentId>/...}), where that service's
 * {@code equipmentId} is this job's {@code deviceId}, so both writers land data
 * under the same path shape.
 *
 * <p>{@code createdAt} is batch-level (denormalized onto every row), so all records
 * exploded from one batch share a date and land in a single partition — matching
 * the 1-minute batch file unit rather than splitting a batch across days when its
 * individual {@code measuredAt} readings straddle midnight.
 *
 * <p>{@code deviceId} is sanitized to a safe partition token to prevent path
 * traversal / injection from untrusted upstream values.
 *
 * <p>The date is derived in <b>UTC</b> — matching the source timestamps
 * ({@code createdAt}/{@code measuredAt} are UTC) and the daily Glue jobs which pick
 * "yesterday" in UTC. Writer and reader must share the same zone, or the batch jobs
 * would target the wrong date partition near midnight.
 */
public class DeviceDateBucketAssigner implements BucketAssigner<SensorRecord, String> {
    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);
    static final String UNKNOWN_DEVICE = "unknown";

    @Override
    public String getBucketId(SensorRecord record, Context context) {
        return bucketId(record.getCreatedAtEpochMilli(), record.getDeviceId());
    }

    /** Pure, testable bucket-path computation. Produces {@code YYYY/MM/DD/<device_id>}. */
    static String bucketId(long createdAtEpochMilli, String deviceId) {
        String date = DATE_FMT.format(Instant.ofEpochMilli(createdAtEpochMilli));
        return date + "/" + sanitize(deviceId);
    }

    /**
     * Reduces a device id to {@code [A-Za-z0-9._-]} so it cannot escape its
     * partition directory ({@code ../}, absolute paths, etc.).
     */
    static String sanitize(String raw) {
        if (raw == null || raw.isBlank()) {
            return UNKNOWN_DEVICE;
        }
        String cleaned = raw.replaceAll("[^A-Za-z0-9._-]", "_");
        // Guard against tokens that are only dots (".", "..") after cleaning.
        return cleaned.chars().anyMatch(c -> c != '.') ? cleaned : UNKNOWN_DEVICE;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
