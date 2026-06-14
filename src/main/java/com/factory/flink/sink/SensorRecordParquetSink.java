package com.factory.flink.sink;

import com.factory.flink.dto.SensorRecord;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

/**
 * Builds the exactly-once Parquet {@link FileSink} that writes flattened sensor
 * readings to S3 (or any Flink-supported filesystem).
 *
 * <p>Records are written as Snappy-compressed Parquet, partitioned by event date
 * and equipment ({@link EquipmentDateBucketAssigner}). Bulk formats roll on
 * checkpoint ({@link OnCheckpointRollingPolicy}); combined with checkpointing the
 * sink commits files transactionally, so a failover never duplicates or loses a
 * committed part file.
 */
public final class SensorRecordParquetSink {

    private SensorRecordParquetSink() {
    }

    public static FileSink<SensorRecord> create(String outputPath) {
        ParquetWriterFactory<GenericRecord> parquetFactory =
                AvroParquetWriters.forGenericRecord(SensorRecordAvroConverter.SCHEMA);

        // "part" prefix (not "sensor"): one file holds MANY rows across all sensors of
        // a partition — the name must not imply one-sensor-per-file.
        OutputFileConfig fileConfig = OutputFileConfig.builder()
                .withPartPrefix("part")
                .withPartSuffix(".snappy.parquet")
                .build();

        return FileSink
                .forBulkFormat(new Path(outputPath), new SensorRecordBulkWriterFactory(parquetFactory))
                .withBucketAssigner(new EquipmentDateBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(fileConfig)
                .build();
    }

    /**
     * Adapts the Avro/Parquet {@link BulkWriter} to accept {@link SensorRecord}
     * directly, converting each element on the fly. Named (not a lambda) to keep
     * it reliably {@link java.io.Serializable} for Flink operator shipping.
     */
    static final class SensorRecordBulkWriterFactory implements BulkWriter.Factory<SensorRecord> {
        private static final long serialVersionUID = 1L;

        private final ParquetWriterFactory<GenericRecord> delegate;

        SensorRecordBulkWriterFactory(ParquetWriterFactory<GenericRecord> delegate) {
            this.delegate = delegate;
        }

        @Override
        public BulkWriter<SensorRecord> create(org.apache.flink.core.fs.FSDataOutputStream out)
                throws IOException {
            BulkWriter<GenericRecord> inner = delegate.create(out);
            return new BulkWriter<SensorRecord>() {
                @Override
                public void addElement(SensorRecord element) throws IOException {
                    inner.addElement(SensorRecordAvroConverter.toAvro(element));
                }

                @Override
                public void flush() throws IOException {
                    inner.flush();
                }

                @Override
                public void finish() throws IOException {
                    inner.finish();
                }
            };
        }
    }
}
