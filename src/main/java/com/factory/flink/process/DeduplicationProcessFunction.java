package com.factory.flink.process;

import com.factory.flink.dto.SensorRecord;
import java.time.Duration;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Drops duplicate {@link SensorRecord}s. The stream must be keyed by
 * {@link SensorRecord#dedupKey()} so every copy of a logical reading is routed
 * to the same keyed state.
 *
 * <p>The first time a key is seen the record is emitted and a flag is stored;
 * later copies (Kafka producer retries, source replays after a failover) hit the
 * flag and are dropped. The flag is bounded by a {@link StateTtlConfig} TTL so
 * keyed state cannot grow without limit — duplicates are only suppressed within
 * the TTL window, which is the realistic horizon for retries/replays.
 *
 * <p>Combined with checkpointing + the exactly-once file sink, this gives
 * effectively-once delivery to S3 while keeping the dedup state size bounded.
 */
public class DeduplicationProcessFunction
        extends KeyedProcessFunction<String, SensorRecord, SensorRecord> {
    private static final long serialVersionUID = 1L;

    static final Duration DEFAULT_TTL = Duration.ofHours(6);

    private final long ttlMillis;
    private transient ValueState<Boolean> seen;

    public DeduplicationProcessFunction() {
        this(DEFAULT_TTL);
    }

    public DeduplicationProcessFunction(Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("dedup TTL must be positive");
        }
        this.ttlMillis = ttl.toMillis();
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.milliseconds(ttlMillis))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();

        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("dedup-seen", Types.BOOLEAN);
        descriptor.enableTimeToLive(ttlConfig);
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(SensorRecord record, Context ctx, Collector<SensorRecord> out)
            throws Exception {
        if (Boolean.TRUE.equals(seen.value())) {
            return; // duplicate within TTL window — drop
        }
        seen.update(Boolean.TRUE);
        out.collect(record);
    }
}
