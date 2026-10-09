package com.batchforge.api.dto;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record JobExecutionDto(
    UUID id,
    String jobName,
    String status,
    Map<String, Object> parameters,
    Instant startedAt,
    Instant finishedAt,
    int restartCount,
    String errorMessage
) {}
