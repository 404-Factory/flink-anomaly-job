package com.factory.flink.opensearch;

import com.factory.flink.config.JobConfig;
import com.factory.flink.dto.SensorRecord;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.opensearch.sink.OpensearchSink;
import org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the OpenSearch sink for the realtime branch (raw flattened readings →
 * {@code sensor-realtime}).
 *
 * <p>Reliability:
 * <ul>
 *   <li><b>No loss</b> — {@code AT_LEAST_ONCE}: bulk requests are flushed on
 *       checkpoint before Kafka offsets advance.</li>
 *   <li><b>No duplicates</b> — deterministic {@code _id} upsert
 *       ({@link SensorRecordEmitter}) makes re-delivery idempotent.</li>
 *   <li><b>Poison isolation</b> — a {@link org.apache.flink.connector.opensearch.sink.FailureHandler}
 *       logs and swallows permanent bulk failures (e.g. mapping errors) so one bad
 *       document cannot wedge or crash the whole job.</li>
 * </ul>
 */
public final class SensorRecordOpensearchSink {
    private static final Logger LOG = LoggerFactory.getLogger(SensorRecordOpensearchSink.class);

    private SensorRecordOpensearchSink() {
    }

    public static OpensearchSink<SensorRecord> create(JobConfig cfg) {
        OpensearchSinkBuilder<SensorRecord> builder = new OpensearchSinkBuilder<SensorRecord>()
                .setHosts(new HttpHost(cfg.getOpenSearchHost(), cfg.getOpenSearchPort(), cfg.getOpenSearchScheme()))
                .setEmitter(new SensorRecordEmitter(cfg.getOpenSearchIndex()))
                .setBulkFlushMaxActions(cfg.getOpenSearchBulkFlushMaxActions())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setFailureHandler(failure ->
                        LOG.error("OpenSearch bulk failure — skipping (poison/permanent): {}",
                                failure.getMessage(), failure));

        if (cfg.getOpenSearchUsername() != null && !cfg.getOpenSearchUsername().isBlank()) {
            builder.setConnectionUsername(cfg.getOpenSearchUsername())
                    .setConnectionPassword(cfg.getOpenSearchPassword());
        }
        return builder.build();
    }
}
