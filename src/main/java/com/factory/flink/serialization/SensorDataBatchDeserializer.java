package com.factory.flink.serialization;

import com.factory.flink.dto.SensorDataBatchDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import java.io.IOException;

public class SensorDataBatchDeserializer extends AbstractDeserializationSchema<SensorDataBatchDto> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        super.open(context);
        objectMapper = new ObjectMapper();
    }

    @Override
    public SensorDataBatchDto deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.readValue(message, SensorDataBatchDto.class);
    }
}
