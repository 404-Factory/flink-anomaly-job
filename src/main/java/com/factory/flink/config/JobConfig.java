package com.factory.flink.config;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

/**
 * Immutable job configuration resolved from environment variables.
 *
 * <p>Parsing is done against a {@code Map} so it is fully unit-testable without
 * mutating real process environment. Invalid/blank values fall back to defaults
 * (fail-soft) rather than crashing the job at startup.
 *
 * <p>Covers all three branches of the unified job: Kafka source, S3 Parquet sink,
 * OpenSearch realtime sink, and the Kafka violations sink (anomaly branch).
 */
public final class JobConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // ---- source ----
    private final String bootstrapServers;
    private final String sourceTopic;
    private final String groupId;
    private final boolean startFromEarliest;
    private final long checkpointIntervalMs;

    // ---- S3 branch ----
    private final String s3OutputPath;
    private final Duration dedupTtl;
    private final long rolloverIntervalMs;
    private final long maxPartSizeBytes;

    // ---- OpenSearch branch ----
    private final String openSearchHost;
    private final int openSearchPort;
    private final String openSearchScheme;
    private final String openSearchIndex;
    private final String openSearchUsername;
    private final String openSearchPassword;
    private final int openSearchBulkFlushMaxActions;

    // ---- anomaly branch ----
    private final String violationsTopic;
    private final long recoveryDwellMs;

    // Optional durable checkpoint storage path (e.g. s3a://bucket/flink-checkpoints).
    // Injected via env (ExternalSecrets) since flinkConfiguration cannot reference Secrets.
    // Blank → keep the cluster/flinkConfiguration default (e.g. jobmanager storage).
    private final String checkpointDir;

    JobConfig(String bootstrapServers, String sourceTopic, String groupId, boolean startFromEarliest,
              long checkpointIntervalMs, String s3OutputPath, Duration dedupTtl, long rolloverIntervalMs,
              long maxPartSizeBytes, String openSearchHost, int openSearchPort, String openSearchScheme,
              String openSearchIndex, String openSearchUsername, String openSearchPassword,
              int openSearchBulkFlushMaxActions, String violationsTopic, long recoveryDwellMs,
              String checkpointDir) {
        this.bootstrapServers = bootstrapServers;
        this.sourceTopic = sourceTopic;
        this.groupId = groupId;
        this.startFromEarliest = startFromEarliest;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.s3OutputPath = s3OutputPath;
        this.dedupTtl = dedupTtl;
        this.rolloverIntervalMs = rolloverIntervalMs;
        this.maxPartSizeBytes = maxPartSizeBytes;
        this.openSearchHost = openSearchHost;
        this.openSearchPort = openSearchPort;
        this.openSearchScheme = openSearchScheme;
        this.openSearchIndex = openSearchIndex;
        this.openSearchUsername = openSearchUsername;
        this.openSearchPassword = openSearchPassword;
        this.openSearchBulkFlushMaxActions = openSearchBulkFlushMaxActions;
        this.violationsTopic = violationsTopic;
        this.recoveryDwellMs = recoveryDwellMs;
        this.checkpointDir = checkpointDir;
    }

    public static JobConfig fromEnv() {
        return from(System.getenv());
    }

    public static JobConfig from(Map<String, String> env) {
        return new JobConfig(
                str(env, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                str(env, "KAFKA_SOURCE_TOPIC", "fab-semiconductor-001"),
                str(env, "KAFKA_GROUP_ID", "flink-anomaly-ingestion"),
                bool(env, "START_FROM_EARLIEST", true),
                longVal(env, "CHECKPOINT_INTERVAL_MS", 60_000L),
                str(env, "S3_BUCKET_NAME", "s3a://sensor-data-lake/sensor"),
                Duration.ofHours(longVal(env, "DEDUP_TTL_HOURS", 6L)),
                longVal(env, "ROLLOVER_INTERVAL_MS", Duration.ofMinutes(15).toMillis()),
                longVal(env, "MAX_PART_SIZE_BYTES", 128L * 1024 * 1024),
                str(env, "OPENSEARCH_ENDPOINT", "opensearch"),
                intVal(env, "OPENSEARCH_PORT", 9200),
                str(env, "OPENSEARCH_SCHEME", "http"),
                str(env, "OPENSEARCH_INDEX", "sensor-realtime"),
                str(env, "OPENSEARCH_USERNAME", ""),
                str(env, "OPENSEARCH_PASSWORD", ""),
                intVal(env, "OPENSEARCH_BULK_FLUSH_MAX_ACTIONS", 1000),
                str(env, "KAFKA_VIOLATIONS_TOPIC", "sensor-violations"),
                // Recovery dwell = 10% of the 5-min anomaly window (30 seconds)
                longVal(env, "RECOVERY_DWELL_MS", 30_000L),
                str(env, "CHECKPOINT_DIR", ""));
    }

    private static String str(Map<String, String> env, String key, String def) {
        String v = env.get(key);
        return (v != null && !v.isBlank()) ? v.trim() : def;
    }

    private static boolean bool(Map<String, String> env, String key, boolean def) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            return def;
        }
        return Boolean.parseBoolean(v.trim());
    }

    private static long longVal(Map<String, String> env, String key, long def) {
        return parse(env.get(key), def, Long::parseLong);
    }

    private static int intVal(Map<String, String> env, String key, int def) {
        return parse(env.get(key), def, Integer::parseInt);
    }

    private static <T> T parse(String raw, T def, Function<String, T> fn) {
        if (raw == null || raw.isBlank()) {
            return def;
        }
        try {
            return fn.apply(raw.trim());
        } catch (RuntimeException e) {
            return def;
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public String getGroupId() {
        return groupId;
    }

    public boolean isStartFromEarliest() {
        return startFromEarliest;
    }

    public long getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public String getS3OutputPath() {
        return s3OutputPath;
    }

    public Duration getDedupTtl() {
        return dedupTtl;
    }

    public long getRolloverIntervalMs() {
        return rolloverIntervalMs;
    }

    public long getMaxPartSizeBytes() {
        return maxPartSizeBytes;
    }

    public String getOpenSearchHost() {
        return openSearchHost;
    }

    public int getOpenSearchPort() {
        return openSearchPort;
    }

    public String getOpenSearchScheme() {
        return openSearchScheme;
    }

    public String getOpenSearchIndex() {
        return openSearchIndex;
    }

    public String getOpenSearchUsername() {
        return openSearchUsername;
    }

    public String getOpenSearchPassword() {
        return openSearchPassword;
    }

    public int getOpenSearchBulkFlushMaxActions() {
        return openSearchBulkFlushMaxActions;
    }

    public String getViolationsTopic() {
        return violationsTopic;
    }

    public long getRecoveryDwellMs() {
        return recoveryDwellMs;
    }

    public String getCheckpointDir() {
        return checkpointDir;
    }
}
