package com.factory.flink;

import com.factory.flink.dto.SensorDataBatchDto;
import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.process.AnomalyEvaluationProcessFunction;
import com.factory.flink.process.SensorDataBatchFlatMapFunction;
import com.factory.flink.serialization.SensorDataBatchDeserializer;
import com.factory.flink.serialization.SensorViolationSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

public class FlinkAnomalyDetectionJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Configure Kafka Source to read SensorDataBatchDto JSON messages
        KafkaSource<SensorDataBatchDto> kafkaSource = KafkaSource.<SensorDataBatchDto>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("fab-semiconductor-001")
                .setGroupId("flink-anomaly-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SensorDataBatchDeserializer())
                .build();

        // 2. Read raw batches without watermarks first (we will assign them after flatmapping)
        DataStream<SensorDataBatchDto> batchStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaBatchSource"
        );

        // 3. Flatten the 1-minute batch of measurements into individual 1-second sensor reading events
        DataStream<SensorReadingEvent> flattenedStream = batchStream
                .flatMap(new SensorDataBatchFlatMapFunction())
                .name("SensorBatchFlattener");

        // 4. Assign Event Time and Watermarks to the individual sensor reading events
        // Allow up to 70 seconds of out-of-orderness to handle the 1-minute batch arrival lag
        WatermarkStrategy<SensorReadingEvent> watermarkStrategy = WatermarkStrategy
                .<SensorReadingEvent>forBoundedOutOfOrderness(Duration.ofSeconds(70))
                .withTimestampAssigner((event, timestamp) -> event.getMeasuredAtEpochMilli());

        DataStream<SensorReadingEvent> sensorStream = flattenedStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("EventTimeWatermarking");

        // 5. Partition the stream by equipmentId and sensorType, and apply the Nelson / Bias rule evaluators
        DataStream<SensorViolationEvent> violationStream = sensorStream
                .keyBy(event -> event.getEquipmentId() + ":" + event.getSensorType())
                .process(new AnomalyEvaluationProcessFunction())
                .name("AnomalyRuleEngine");

        // 6. Configure Kafka Sink to emit violation events to 'sensor-violations' topic
        KafkaSink<SensorViolationEvent> kafkaSink = KafkaSink.<SensorViolationEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("sensor-violations")
                                .setValueSerializationSchema(new SensorViolationSerializer())
                                .build()
                )
                .build();

        violationStream.sinkTo(kafkaSink).name("KafkaViolationSink");

        // Execute Flink Job
        env.execute("Flink Real-time Anomaly Detection Job");
    }
}
