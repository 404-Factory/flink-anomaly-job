package com.factory.flink.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JobConfigTest {

    @Test
    void appliesDefaultsForEmptyEnvironment() {
        JobConfig c = JobConfig.from(Collections.emptyMap());
        assertThat(c.getBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(c.getSourceTopic()).isEqualTo("fab-semiconductor-001");
        assertThat(c.getGroupId()).isEqualTo("flink-anomaly-ingestion");
        assertThat(c.getS3OutputPath()).isEqualTo("s3a://sensor-data-lake/sensor");
        assertThat(c.isStartFromEarliest()).isTrue();
        assertThat(c.getCheckpointIntervalMs()).isEqualTo(60_000L);
        assertThat(c.getDedupTtl()).isEqualTo(Duration.ofHours(6));
        assertThat(c.getOpenSearchHost()).isEqualTo("opensearch");
        assertThat(c.getOpenSearchPort()).isEqualTo(9200);
        assertThat(c.getOpenSearchIndex()).isEqualTo("sensor-realtime");
        assertThat(c.getViolationsTopic()).isEqualTo("sensor-violations");
        assertThat(c.getRolloverIntervalMs()).isEqualTo(Duration.ofMinutes(15).toMillis());
        assertThat(c.getMaxPartSizeBytes()).isEqualTo(128L * 1024 * 1024);
        assertThat(c.getCheckpointDir()).isEmpty(); // blank → keep cluster default
    }

    @Test
    void readsOverridesFromEnvironment() {
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BOOTSTRAP_SERVERS", "broker:9092");
        env.put("KAFKA_SOURCE_TOPIC", "topic-x");
        env.put("KAFKA_GROUP_ID", "g1");
        env.put("S3_OUTPUT_PATH", "s3a://bucket/prefix");
        env.put("START_FROM_EARLIEST", "false");
        env.put("CHECKPOINT_INTERVAL_MS", "30000");
        env.put("DEDUP_TTL_HOURS", "12");
        env.put("ROLLOVER_INTERVAL_MS", "5000");
        env.put("MAX_PART_SIZE_BYTES", "1024");
        env.put("CHECKPOINT_DIR", "s3a://bucket/flink-checkpoints");

        JobConfig c = JobConfig.from(env);
        assertThat(c.getBootstrapServers()).isEqualTo("broker:9092");
        assertThat(c.getSourceTopic()).isEqualTo("topic-x");
        assertThat(c.getGroupId()).isEqualTo("g1");
        assertThat(c.getS3OutputPath()).isEqualTo("s3a://bucket/prefix");
        assertThat(c.isStartFromEarliest()).isFalse();
        assertThat(c.getCheckpointIntervalMs()).isEqualTo(30_000L);
        assertThat(c.getDedupTtl()).isEqualTo(Duration.ofHours(12));
        assertThat(c.getRolloverIntervalMs()).isEqualTo(5000L);
        assertThat(c.getMaxPartSizeBytes()).isEqualTo(1024L);
        assertThat(c.getCheckpointDir()).isEqualTo("s3a://bucket/flink-checkpoints");
    }

    @Test
    void blankValuesFallBackToDefaults() {
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BOOTSTRAP_SERVERS", "   ");
        env.put("KAFKA_SOURCE_TOPIC", "");
        JobConfig c = JobConfig.from(env);
        assertThat(c.getBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(c.getSourceTopic()).isEqualTo("fab-semiconductor-001");
    }

    @Test
    void invalidNumbersFallBackToDefaults() {
        Map<String, String> env = new HashMap<>();
        env.put("CHECKPOINT_INTERVAL_MS", "not-a-number");
        env.put("MAX_PART_SIZE_BYTES", "");
        JobConfig c = JobConfig.from(env);
        assertThat(c.getCheckpointIntervalMs()).isEqualTo(60_000L);
        assertThat(c.getMaxPartSizeBytes()).isEqualTo(128L * 1024 * 1024);
    }

    @Test
    void invalidBooleanParsesAsFalse() {
        // Boolean.parseBoolean treats any non-"true" token as false
        JobConfig c = JobConfig.from(Collections.singletonMap("START_FROM_EARLIEST", "yes"));
        assertThat(c.isStartFromEarliest()).isFalse();
    }

    @Test
    void trimsSurroundingWhitespace() {
        JobConfig c = JobConfig.from(Collections.singletonMap("KAFKA_GROUP_ID", "  grp  "));
        assertThat(c.getGroupId()).isEqualTo("grp");
    }

    @Test
    void fromEnvDoesNotThrow() {
        assertThat(JobConfig.fromEnv()).isNotNull();
    }
}
