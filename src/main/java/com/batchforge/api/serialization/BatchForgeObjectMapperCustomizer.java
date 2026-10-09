package com.batchforge.api.serialization;

import com.batchforge.engine.core.ExecutionStatus;
import com.batchforge.engine.core.StepStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.quarkus.jackson.ObjectMapperCustomizer;
import jakarta.inject.Singleton;

@Singleton
public class BatchForgeObjectMapperCustomizer implements ObjectMapperCustomizer {
    @Override
    public void customize(ObjectMapper objectMapper) {
        SimpleModule module = new SimpleModule("BatchForge");
        module.addSerializer(ExecutionStatus.class, new ExecutionStatusSerializer());
        module.addSerializer(StepStatus.class, new StepStatusSerializer());
        objectMapper.registerModule(module);
    }
}
