package com.batchforge.api.dto;

import java.time.Instant;
import java.util.UUID;

public record StepExecutionDto(
    UUID id,
    String stepName,
    String status,
    String partitionId,
    int chunksProcessed,
    Integer chunksTotal,
    long itemsRead,
    long itemsWritten,
    long itemsFailed,
    Instant startedAt,
    Instant finishedAt,
    String errorMessage
) {}
