package com.factory.flink.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.factory.flink.dto.SensorRecord;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.opensearch.sink.RequestIndexer;
import org.junit.jupiter.api.Test;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;

class SensorRecordEmitterTest {

    private SensorRecord reading() {
        return SensorRecord.builder()
                .equipmentId("EQP-1").sensorId("S1").sensorType("TEMP")
                .measuredAtEpochMilli(2000L).value(21.5).build();
    }

    /** Captures requests added by the emitter. */
    private static final class CapturingIndexer implements RequestIndexer {
        final List<DocWriteRequest<?>> requests = new ArrayList<>();
        @Override public void add(DeleteRequest... r) { }
        @Override public void add(UpdateRequest... r) { }
        @Override public void add(IndexRequest... r) {
            for (IndexRequest req : r) {
                requests.add(req);
            }
        }
    }

    @Test
    void buildsIndexRequestWithDeterministicId() {
        IndexRequest req = new SensorRecordEmitter("sensor-realtime").buildRequest(reading());
        assertThat(req.index()).isEqualTo("sensor-realtime");
        assertThat(req.id()).isEqualTo("EQP-1_S1_2000");
        assertThat(req.sourceAsMap()).containsEntry("sensorType", "TEMP");
        assertThat(req.sourceAsMap()).containsEntry("value", 21.5);
    }

    @Test
    void emitAddsExactlyOneRequestToIndexer() {
        CapturingIndexer indexer = new CapturingIndexer();
        new SensorRecordEmitter("idx").emit(reading(), null, indexer);
        assertThat(indexer.requests).hasSize(1);
        assertThat(((IndexRequest) indexer.requests.get(0)).id()).isEqualTo("EQP-1_S1_2000");
    }
}
