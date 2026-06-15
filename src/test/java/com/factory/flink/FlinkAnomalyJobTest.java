package com.factory.flink;

import static org.assertj.core.api.Assertions.assertThat;

import com.factory.flink.config.JobConfig;
import java.util.Collections;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class FlinkAnomalyJobTest {

    @Test
    void configuresExactlyOnceCheckpointing() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        JobConfig cfg = JobConfig.from(Collections.singletonMap("CHECKPOINT_INTERVAL_MS", "20000"));

        FlinkAnomalyJob.configureCheckpointing(env, cfg);

        CheckpointConfig cp = env.getCheckpointConfig();
        assertThat(cp.isCheckpointingEnabled()).isTrue();
        assertThat(cp.getCheckpointInterval()).isEqualTo(20000L);
        assertThat(cp.getCheckpointingMode()).isEqualTo(CheckpointingMode.EXACTLY_ONCE);
        assertThat(cp.getMaxConcurrentCheckpoints()).isEqualTo(1);
        assertThat(cp.getMinPauseBetweenCheckpoints()).isEqualTo(10000L);
    }
}
