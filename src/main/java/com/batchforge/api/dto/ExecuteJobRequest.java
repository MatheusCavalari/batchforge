package com.batchforge.api.dto;

import java.util.Map;

public record ExecuteJobRequest(Map<String, Object> parameters) {
    public ExecuteJobRequest {
        if (parameters == null) parameters = Map.of();
    }
}
