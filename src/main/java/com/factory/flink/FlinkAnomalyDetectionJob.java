package com.factory.flink;

import com.factory.flink.dto.SensorDataBatchDto;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.process.AnomalyEvaluationProcessFunction;
import com.factory.flink.process.SensorDataBatchFlatMapFunction;
import com.factory.flink.serialization.SensorDataBatchDeserializer;
import com.factory.flink.serialization.SensorViolationEnvelopeSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.opensearch.sink.OpensearchSink;
import org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Requests;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class FlinkAnomalyDetectionJob {

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        final String sourceTopic      = env("KAFKA_SOURCE_TOPIC",      "fab-semiconductor-001");
        final String sinkTopic        = env("KAFKA_SINK_TOPIC",        "sensor-violations");
        final String groupId          = env("KAFKA_GROUP_ID",          "flink-anomaly-group");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint Configuration (RocksDB State Backend & S3 Checkpoint)
        // Checkpointing is enabled for production fault tolerance
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds

        // 1. Configure Kafka Source to read SensorDataBatchDto JSON messages
        KafkaSource<SensorDataBatchDto> kafkaSource = KafkaSource.<SensorDataBatchDto>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SensorDataBatchDeserializer())
                .build();

        // 2. Read raw batches
        DataStream<SensorDataBatchDto> batchStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaBatchSource"
        );

        DataStream<SensorReadingEvent> flattenedStream = batchStream
                .flatMap(new SensorDataBatchFlatMapFunction())
                .name("SensorBatchFlattener");

        // 4. Assign Event Time and Watermarks
        WatermarkStrategy<SensorReadingEvent> watermarkStrategy = WatermarkStrategy
                .<SensorReadingEvent>forBoundedOutOfOrderness(Duration.ofSeconds(70))
                .withTimestampAssigner((event, timestamp) -> event.getMeasuredAtEpochMilli());

        DataStream<SensorReadingEvent> sensorStream = flattenedStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("EventTimeWatermarking");

        // 5. Optionally Sink raw Flattened sensor readings to OpenSearch (Grafana Real-time graph)
        if ("true".equalsIgnoreCase(System.getenv("OPENSEARCH_SINK_ENABLED"))) {
            String osHost = System.getenv("OPENSEARCH_HOST") != null ? System.getenv("OPENSEARCH_HOST") : "localhost";
            int osPort = System.getenv("OPENSEARCH_PORT") != null ? Integer.parseInt(System.getenv("OPENSEARCH_PORT")) : 9200;
            String osScheme = System.getenv("OPENSEARCH_SCHEME") != null ? System.getenv("OPENSEARCH_SCHEME") : "http";

            OpensearchSink<SensorReadingEvent> opensearchSink = new OpensearchSinkBuilder<SensorReadingEvent>()
                    .setHosts(new HttpHost(osHost, osPort, osScheme))
                    .setEmitter((element, context, indexer) -> {
                        Map<String, Object> json = new HashMap<>();
                        json.put("equipmentId", element.getEquipmentId());
                        json.put("sensorId", element.getSensorId());
                        json.put("sensorType", element.getSensorType());
                        json.put("value", element.getValue());
                        json.put("recipeMin", element.getRecipeMin());
                        json.put("recipeMax", element.getRecipeMax());
                        json.put("measuredAt", java.time.Instant.ofEpochMilli(element.getMeasuredAtEpochMilli()).toString());
                        json.put("measuredAtEpochMilli", element.getMeasuredAtEpochMilli());

                        IndexRequest indexRequest = Requests.indexRequest()
                                .index("sensor-readings")
                                .id(element.getEquipmentId() + "_" + element.getSensorId() + "_" + element.getMeasuredAtEpochMilli())
                                .source(json);
                        indexer.add(indexRequest);
                    })
                    .setBulkFlushMaxActions(1000) // Bulk settings to prevent backpressure
                    .setBulkFlushInterval(1000)
                    .build();

            sensorStream.sinkTo(opensearchSink).name("OpenSearchSensorSink");
        }

        // 6. Optionally Sink raw Flattened sensor readings to S3/Local in Parquet format
        if ("true".equalsIgnoreCase(System.getenv("S3_SINK_ENABLED"))) {
            String avroSchemaJson = "{\n" +
                    "  \"type\": \"record\",\n" +
                    "  \"name\": \"SensorReadingRecord\",\n" +
                    "  \"namespace\": \"com.factory.flink.dto\",\n" +
                    "  \"fields\": [\n" +
                    "    {\"name\": \"equipmentId\", \"type\": [\"long\", \"null\"]},\n" +
                    "    {\"name\": \"sensorId\", \"type\": [\"string\", \"null\"]},\n" +
                    "    {\"name\": \"sensorType\", \"type\": [\"string\", \"null\"]},\n" +
                    "    {\"name\": \"value\", \"type\": [\"double\", \"null\"]},\n" +
                    "    {\"name\": \"recipeMin\", \"type\": [\"double\", \"null\"]},\n" +
                    "    {\"name\": \"recipeMax\", \"type\": [\"double\", \"null\"]},\n" +
                    "    {\"name\": \"measuredAt\", \"type\": [\"string\", \"null\"]},\n" +
                    "    {\"name\": \"measuredAtEpochMilli\", \"type\": \"long\"}\n" +
                    "  ]\n" +
                    "}";
            Schema schema = new Schema.Parser().parse(avroSchemaJson);

            DataStream<GenericRecord> avroStream = sensorStream.map(new MapFunction<SensorReadingEvent, GenericRecord>() {
                private static final long serialVersionUID = 1L;
                @Override
                public GenericRecord map(SensorReadingEvent event) throws Exception {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("equipmentId", event.getEquipmentId());
                    record.put("sensorId", event.getSensorId());
                    record.put("sensorType", event.getSensorType());
                    record.put("value", event.getValue());
                    record.put("recipeMin", event.getRecipeMin());
                    record.put("recipeMax", event.getRecipeMax());
                    record.put("measuredAt", java.time.Instant.ofEpochMilli(event.getMeasuredAtEpochMilli()).toString());
                    record.put("measuredAtEpochMilli", event.getMeasuredAtEpochMilli());
                    return record;
                }
            });

            String s3Path = System.getenv("S3_SINK_PATH") != null ? System.getenv("S3_SINK_PATH") : "./data/parquet";
            FileSink<GenericRecord> fileSink = FileSink
                    .forBulkFormat(new Path(s3Path), ParquetAvroWriters.forGenericRecord(schema))
                    .build();

            avroStream.sinkTo(fileSink).name("ParquetS3Sink");
        }

        // 7. Partition the stream by equipmentId and sensorType, and apply Nelson / Bias rule evaluators
        DataStream<SensorViolationEvent> violationStream = sensorStream
                .keyBy(event -> event.getEquipmentId() + ":" + event.getSensorType())
                .process(new AnomalyEvaluationProcessFunction())
                .name("AnomalyRuleEngine");

        // 8. Configure Kafka Sink to emit violation events to 'sensor-violations' topic
        KafkaSink<SensorViolationEvent> kafkaSink = KafkaSink.<SensorViolationEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(new SensorViolationEnvelopeSerializer())
                                .build()
                )
                .build();

        violationStream.sinkTo(kafkaSink).name("KafkaViolationSink");

        env.execute("Flink Real-time Anomaly Detection Job");
    }
}
