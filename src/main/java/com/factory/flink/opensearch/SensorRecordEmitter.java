package com.factory.flink.opensearch;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.opensearch.sink.OpensearchEmitter;
import org.apache.flink.connector.opensearch.sink.RequestIndexer;
import org.opensearch.action.index.IndexRequest;

import com.factory.flink.domain.dto.SensorRecord;

/**
 * Turns each {@link SensorRecord} into an OpenSearch {@link IndexRequest} keyed by
 * the deterministic {@link SensorDocument#id(SensorRecord)} so re-delivered readings
 * upsert (idempotent) rather than duplicate.
 */
public class SensorRecordEmitter implements OpensearchEmitter<SensorRecord> {
    private static final long serialVersionUID = 1L;

    private final String index;

    public SensorRecordEmitter(String index) {
        this.index = index;
    }

    @Override
    public void emit(SensorRecord record, SinkWriter.Context context, RequestIndexer indexer) {
        indexer.add(buildRequest(record));
    }

    /** Exposed for unit testing the request shape (index / id / source). */
    IndexRequest buildRequest(SensorRecord record) {
        return new IndexRequest(index)
                .id(SensorDocument.id(record))
                .source(SensorDocument.source(record));
    }
}
