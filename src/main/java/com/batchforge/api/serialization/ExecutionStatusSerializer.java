package com.batchforge.api.serialization;

import com.batchforge.engine.core.ExecutionStatus;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class ExecutionStatusSerializer extends JsonSerializer<ExecutionStatus> {
    @Override
    public void serialize(ExecutionStatus value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeString(value.toDbValue());
    }
}
