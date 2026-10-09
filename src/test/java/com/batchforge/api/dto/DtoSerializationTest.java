package com.batchforge.api.dto;

import com.batchforge.api.serialization.ExecutionStatusSerializer;
import com.batchforge.api.serialization.StepStatusSerializer;
import com.batchforge.engine.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DtoSerializationTest {

    private final ObjectMapper mapper = createMapper();

    private static ObjectMapper createMapper() {
        ObjectMapper om = new ObjectMapper();
        om.findAndRegisterModules();
        SimpleModule mod = new SimpleModule();
        mod.addSerializer(ExecutionStatus.class, new ExecutionStatusSerializer());
        mod.addSerializer(StepStatus.class, new StepStatusSerializer());
        om.registerModule(mod);
        return om;
    }

    @Test
    void jobExecutionDto_serializes_to_json() throws Exception {
        var dto = new JobExecutionDto(UUID.randomUUID(), "test-job", "RUNNING",
            Map.of("key", "value"), Instant.now(), null, 0, null);
        String json = mapper.writeValueAsString(dto);
        assertThat(json).contains("\"status\":\"RUNNING\"");
        assertThat(json).contains("\"jobName\":\"test-job\"");
    }

    @Test
    void executionStatus_serializes_as_string() throws Exception {
        ExecutionStatus status = new ExecutionStatus.Failed("boom");
        String json = mapper.writeValueAsString(status);
        assertThat(json).isEqualTo("\"FAILED\"");
    }

    @Test
    void stepStatus_serializes_as_string() throws Exception {
        StepStatus status = new StepStatus.Retrying(3);
        String json = mapper.writeValueAsString(status);
        assertThat(json).isEqualTo("\"RETRYING\"");
    }

    @Test
    void deadLetterItemDto_round_trips() throws Exception {
        var dto = new DeadLetterItemDto(UUID.randomUUID(), UUID.randomUUID(), "step1",
            "{\"id\":1}", "bad value", null, 3, "PENDING", Instant.now());
        String json = mapper.writeValueAsString(dto);
        var back = mapper.readValue(json, DeadLetterItemDto.class);
        assertThat(back.stepName()).isEqualTo("step1");
        assertThat(back.attemptCount()).isEqualTo(3);
    }
}
