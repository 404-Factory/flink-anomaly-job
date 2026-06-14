package com.factory.flink.serialization;

import com.factory.flink.dto.SensorDataBatchDto;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes a {@code fab-semiconductor-001} JSON payload into a
 * {@link SensorDataBatchDto}.
 *
 * <p>Poison messages (non-JSON / schema-incompatible bytes) are <b>logged and
 * skipped</b> rather than thrown, so a single bad record cannot wedge the whole
 * ingestion pipeline. Valid messages are emitted via the collector. This keeps
 * the "no data loss for valid records" guarantee while isolating bad input.
 */
public class SensorDataBatchDeserializer extends AbstractDeserializationSchema<SensorDataBatchDto> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SensorDataBatchDeserializer.class);

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        super.open(context);
        objectMapper = buildMapper();
    }

    @Override
    public SensorDataBatchDto deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = buildMapper();
        }
        return objectMapper.readValue(message, SensorDataBatchDto.class);
    }

    @Override
    public void deserialize(byte[] message, Collector<SensorDataBatchDto> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }
        final SensorDataBatchDto dto;
        try {
            dto = deserialize(message);
        } catch (IOException e) {
            LOG.warn("Skipping poison message ({} bytes): {}", message.length, e.getMessage());
            return;
        }
        if (dto != null) {
            out.collect(dto);
        }
    }

    static ObjectMapper buildMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
}
