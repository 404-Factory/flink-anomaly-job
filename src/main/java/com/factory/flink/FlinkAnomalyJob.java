package com.factory.flink;

import com.factory.flink.config.JobConfig;
import com.factory.flink.dto.SensorDataBatchDto;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorRecord;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.opensearch.SensorRecordOpensearchSink;
import com.factory.flink.process.AnomalyEvaluationProcessFunction;
import com.factory.flink.process.DeduplicationProcessFunction;
import com.factory.flink.process.SensorRecordFlatMapFunction;
import com.factory.flink.process.SensorRecordToReadingEvent;
import com.factory.flink.serialization.SensorDataBatchDeserializer;
import com.factory.flink.serialization.SensorViolationEnvelopeSerializer;
import com.factory.flink.sink.SensorRecordParquetSink;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Unified sensor ingestion job. Reads {@code fab-semiconductor-001} <b>once</b>,
 * flattens <b>once</b> into {@link SensorRecord}, then fans the shared stream into
 * three branches placed in separate slot-sharing groups (so Flink distributes them
 * onto different TaskManagers):
 *
 * <pre>
 *   Kafka ─ flatMap(SensorRecord) ─┬─[s3]   keyBy(dedupKey) → dedup → Parquet → S3
 *                                  ├─[os]   → OpenSearch (sensor-realtime, raw)
 *                                  └─[anom] map→ReadingEvent → watermark → anomaly → Kafka(violations)
 * </pre>
 *
 * <p>Sharing the source read + flatten avoids the duplicate work that separate jobs
 * would incur. The trade-off (shared checkpoint/failure domain) is mitigated by
 * defensive per-branch handling and slot-group resource isolation; if hard isolation
 * is later required, the branches split cleanly into separate Application-mode jobs.
 */
public class FlinkAnomalyJob {

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.fromEnv();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpointing(env, cfg);

        KafkaSource<SensorDataBatchDto> source = KafkaSource.<SensorDataBatchDto>builder()
                .setBootstrapServers(cfg.getBootstrapServers())
                .setTopics(cfg.getSourceTopic())
                .setGroupId(cfg.getGroupId())
                .setStartingOffsets(cfg.isStartFromEarliest()
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SensorDataBatchDeserializer())
                .build();

        // ---- shared: read once, flatten once ----
        DataStream<SensorRecord> records = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSensorBatchSource")
                .flatMap(new SensorRecordFlatMapFunction())
                .name("FlattenToSensorRecords");

        // ---- branch 1: S3 Parquet (cold/analytics) ----
        records.keyBy(SensorRecord::dedupKey)
                .process(new DeduplicationProcessFunction(cfg.getDedupTtl()))
                .name("Deduplicate")
                .slotSharingGroup("s3")
                .sinkTo(SensorRecordParquetSink.create(cfg.getS3OutputPath()))
                .name("S3ParquetSink")
                .slotSharingGroup("s3");

        // ---- branch 2: OpenSearch realtime (raw flattened readings) ----
        records.sinkTo(SensorRecordOpensearchSink.create(cfg))
                .name("OpenSearchRealtimeSink")
                .slotSharingGroup("os");

        // ---- branch 3: anomaly detection → Kafka violations ----
        WatermarkStrategy<SensorReadingEvent> wm = WatermarkStrategy
                .<SensorReadingEvent>forBoundedOutOfOrderness(Duration.ofSeconds(70))
                .withTimestampAssigner((e, ts) -> e.getMeasuredAtEpochMilli());

        DataStream<SensorViolationEvent> violations = records
                .map(new SensorRecordToReadingEvent())
                .name("ToReadingEvent")
                .slotSharingGroup("anom")
                .assignTimestampsAndWatermarks(wm)
                .name("EventTimeWatermarks")
                .keyBy(e -> e.getEquipmentId() + ":" + e.getSensorType())
                .process(new AnomalyEvaluationProcessFunction(cfg.getRecoveryDwellMs()))
                .name("AnomalyRuleEngine")
                .slotSharingGroup("anom");

        violations.sinkTo(buildViolationsSink(cfg)).name("KafkaViolationSink").slotSharingGroup("anom");

        env.execute("Flink Anomaly Detection Job");
    }

    private static KafkaSink<SensorViolationEvent> buildViolationsSink(JobConfig cfg) {
        return KafkaSink.<SensorViolationEvent>builder()
                .setBootstrapServers(cfg.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(cfg.getViolationsTopic())
                        .setValueSerializationSchema(new SensorViolationEnvelopeSerializer())
                        .build())
                .build();
    }

    static void configureCheckpointing(StreamExecutionEnvironment env, JobConfig cfg) {
        env.enableCheckpointing(cfg.getCheckpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cp = env.getCheckpointConfig();
        // Durable checkpoint storage (e.g. s3a://bucket/flink-checkpoints) injected via env
        // (ExternalSecrets), since flinkConfiguration cannot reference Secrets. Blank → cluster default.
        if (cfg.getCheckpointDir() != null && !cfg.getCheckpointDir().isBlank()) {
            cp.setCheckpointStorage(cfg.getCheckpointDir());
        }
        cp.setMinPauseBetweenCheckpoints(cfg.getCheckpointIntervalMs() / 2);
        cp.setCheckpointTimeout(cfg.getCheckpointIntervalMs() * 5);
        cp.setMaxConcurrentCheckpoints(1);
        cp.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    private FlinkAnomalyJob() {
    }
}
