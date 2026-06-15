package com.factory.flink.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.factory.flink.domain.dto.SensorRecord;

/**
 * Converts a {@link SensorRecord} into an Avro {@link GenericRecord} matching
 * {@link #SCHEMA}, which is what the Parquet writer serializes.
 *
 * <p>The schema uses nullable unions for every optional field and the
 * {@code timestamp-millis} logical type for the two time columns, so downstream
 * engines (Athena/Spark) read them as proper timestamps rather than raw longs.
 */
public final class SensorRecordAvroConverter {

    /** Avro schema for one flattened sensor reading. */
    public static final Schema SCHEMA = new Schema.Parser().parse(
            """
            {
              "type": "record",
              "name": "SensorRecord",
              "namespace": "com.factory.flink.avro",
              "fields": [
                {"name": "batchId",           "type": ["null", "string"], "default": null},
                {"name": "deviceId",          "type": ["null", "string"], "default": null},
                {"name": "equipmentId",       "type": ["null", "long"],   "default": null},
                {"name": "createdAt",         "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "intervalSec",       "type": ["null", "int"],    "default": null},
                {"name": "sequence",          "type": ["null", "int"],    "default": null},
                {"name": "measuredAt",        "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "measurementStatus", "type": ["null", "string"], "default": null},
                {"name": "sensorId",          "type": ["null", "string"], "default": null},
                {"name": "sensorType",        "type": ["null", "string"], "default": null},
                {"name": "value",             "type": ["null", "double"], "default": null},
                {"name": "recipeMin",         "type": ["null", "double"], "default": null},
                {"name": "recipeMax",         "type": ["null", "double"], "default": null},
                {"name": "unit",              "type": ["null", "string"], "default": null},
                {"name": "sensorStatus",      "type": ["null", "string"], "default": null}
              ]
            }
            """);

    private SensorRecordAvroConverter() {
    }

    public static GenericRecord toAvro(SensorRecord r) {
        GenericRecord rec = new GenericData.Record(SCHEMA);
        rec.put("batchId", r.getBatchId());
        rec.put("deviceId", r.getDeviceId());
        rec.put("equipmentId", r.getEquipmentId());
        rec.put("createdAt", r.getCreatedAtEpochMilli());
        rec.put("intervalSec", r.getIntervalSec());
        rec.put("sequence", r.getSequence());
        rec.put("measuredAt", r.getMeasuredAtEpochMilli());
        rec.put("measurementStatus", r.getMeasurementStatus());
        rec.put("sensorId", r.getSensorId());
        rec.put("sensorType", r.getSensorType());
        rec.put("value", r.getValue());
        rec.put("recipeMin", r.getRecipeMin());
        rec.put("recipeMax", r.getRecipeMax());
        rec.put("unit", r.getUnit());
        rec.put("sensorStatus", r.getSensorStatus());
        return rec;
    }
}
