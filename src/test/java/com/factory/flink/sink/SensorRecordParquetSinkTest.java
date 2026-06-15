package com.factory.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.connector.file.sink.FileSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

class SensorRecordParquetSinkTest {

    @Test
    void buildsParquetFileSink(@TempDir Path tmp) {
        FileSink<?> sink = SensorRecordParquetSink.create(tmp.toUri().toString());
        assertThat(sink).isNotNull();
    }

    @Test
    void prependsS3aSchemeToBareBucketName() {
        assertThat(SensorRecordParquetSink.normalizeS3Path("sensor-data-lake"))
                .isEqualTo("s3a://sensor-data-lake");
    }

    @Test
    void leavesExistingSchemeUntouched() {
        assertThat(SensorRecordParquetSink.normalizeS3Path("s3a://sensor-data-lake"))
                .isEqualTo("s3a://sensor-data-lake");
        assertThat(SensorRecordParquetSink.normalizeS3Path("s3://sensor-data-lake"))
                .isEqualTo("s3://sensor-data-lake");
        assertThat(SensorRecordParquetSink.normalizeS3Path("file:///tmp/out"))
                .isEqualTo("file:///tmp/out");
    }

    @Test
    void trimsWhitespaceBeforeNormalizing() {
        assertThat(SensorRecordParquetSink.normalizeS3Path("  sensor-data-lake  "))
                .isEqualTo("s3a://sensor-data-lake");
    }

    @Test
    void passesBlankThrough() {
        assertThat(SensorRecordParquetSink.normalizeS3Path("")).isEqualTo("");
        assertThat(SensorRecordParquetSink.normalizeS3Path(null)).isNull();
    }
}
