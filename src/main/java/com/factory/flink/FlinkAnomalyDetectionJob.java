package com.factory.flink;

import com.factory.flink.dto.SensorDataBatchDto;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.process.AnomalyEvaluationProcessFunction;
import com.factory.flink.process.SensorDataBatchFlatMapFunction;
import com.factory.flink.serialization.SensorDataBatchDeserializer;
import com.factory.flink.serialization.SensorViolationEnvelopeSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

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

        KafkaSource<SensorDataBatchDto> kafkaSource = KafkaSource.<SensorDataBatchDto>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SensorDataBatchDeserializer())
                .build();

        DataStream<SensorDataBatchDto> batchStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaBatchSource"
        );

        DataStream<SensorReadingEvent> flattenedStream = batchStream
                .flatMap(new SensorDataBatchFlatMapFunction())
                .name("SensorBatchFlattener");

        WatermarkStrategy<SensorReadingEvent> watermarkStrategy = WatermarkStrategy
                .<SensorReadingEvent>forBoundedOutOfOrderness(Duration.ofSeconds(70))
                .withTimestampAssigner((event, timestamp) -> event.getMeasuredAtEpochMilli());

        DataStream<SensorReadingEvent> sensorStream = flattenedStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("EventTimeWatermarking");

        DataStream<SensorViolationEvent> violationStream = sensorStream
                .keyBy(event -> event.getEquipmentId() + ":" + event.getSensorType())
                .process(new AnomalyEvaluationProcessFunction())
                .name("AnomalyRuleEngine");

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
