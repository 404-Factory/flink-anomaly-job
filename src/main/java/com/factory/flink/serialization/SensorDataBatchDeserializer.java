package com.factory.flink.serialization;

import com.factory.flink.dto.SensorDataBatchDto;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import java.io.IOException;

public class SensorDataBatchDeserializer extends AbstractDeserializationSchema<SensorDataBatchDto> {
    private static final long serialVersionUID = 1L;
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

    private static ObjectMapper buildMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
}
