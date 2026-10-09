package com.batchforge.api.dto;

import java.time.Instant;

public record JobDto(
    String name,
    String description,
    String schedule,
    int stepCount,
    String lastExecutionStatus,
    Instant lastExecutionAt
) {}
