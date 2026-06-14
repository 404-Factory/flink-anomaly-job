package com.factory.flink.opensearch;

import com.factory.flink.dto.SensorRecord;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps a {@link SensorRecord} to an OpenSearch document: a deterministic {@code _id}
 * and the indexed source map.
 *
 * <p><b>Deduplication:</b> the {@code _id = equipmentId_sensorId_measuredAt} is the
 * physical identity of a reading. Re-delivering the same reading (Kafka retry,
 * checkpoint replay) issues the same {@code _id}, so OpenSearch <b>upserts</b> in
 * place instead of creating a duplicate — idempotent indexing. Combined with
 * at-least-once delivery this yields effectively-once into the index, without a
 * separate dedup operator.
 *
 * <p>Timestamps are emitted as epoch-millis longs; OpenSearch's default {@code date}
 * format ({@code strict_date_optional_time||epoch_millis}) accepts them directly.
 */
public final class SensorDocument {

    private SensorDocument() {
    }

    /** Deterministic document id — same reading always maps to the same id. */
    public static String id(SensorRecord r) {
        return n(r.getEquipmentId()) + '_' + n(r.getSensorId()) + '_' + r.getMeasuredAtEpochMilli();
    }

    /** Indexed source document (field order stable for predictable output/tests). */
    public static Map<String, Object> source(SensorRecord r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("batchId", r.getBatchId());
        m.put("deviceId", r.getDeviceId());
        m.put("equipmentId", r.getEquipmentId());
        m.put("createdAt", r.getCreatedAtEpochMilli());
        m.put("intervalSec", r.getIntervalSec());
        m.put("sequence", r.getSequence());
        m.put("measuredAt", r.getMeasuredAtEpochMilli());
        m.put("status", r.getSensorStatus());
        m.put("sensorId", r.getSensorId());
        m.put("sensorType", r.getSensorType());
        m.put("value", r.getValue());
        m.put("recipeMin", r.getRecipeMin());
        m.put("recipeMax", r.getRecipeMax());
        m.put("unit", r.getUnit());
        return m;
    }

    private static String n(String s) {
        return s == null ? "" : s;
    }
}
