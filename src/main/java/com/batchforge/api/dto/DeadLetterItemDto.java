package com.batchforge.api.dto;

import java.time.Instant;
import java.util.UUID;

public record DeadLetterItemDto(
    UUID id,
    UUID stepExecutionId,
    String stepName,
    String itemPayload,
    String errorMessage,
    String stackTrace,
    int attemptCount,
    String status,
    Instant failedAt
) {}
