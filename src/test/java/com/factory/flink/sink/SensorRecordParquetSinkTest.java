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
}
