package com.batchforge.api.serialization;

import com.batchforge.engine.core.StepStatus;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class StepStatusSerializer extends JsonSerializer<StepStatus> {
    @Override
    public void serialize(StepStatus value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeString(value.toDbValue());
    }
}
