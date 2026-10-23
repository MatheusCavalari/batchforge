package com.batchforge.ws;

import java.time.Instant;
import java.util.UUID;

public sealed interface ExecutionEvent {

    UUID executionId();
    Instant timestamp();

    record StepStarted(UUID executionId, String step, Instant timestamp) implements ExecutionEvent {}

    record ChunkCompleted(UUID executionId, String step, String partition,
                          long offset, double itemsPerSec, Instant timestamp) implements ExecutionEvent {}

    record ItemFailed(UUID executionId, String step, String itemId,
                      String error, int attempt, Instant timestamp) implements ExecutionEvent {}

    record StepCompleted(UUID executionId, String step, String duration,
                         long itemsProcessed, Instant timestamp) implements ExecutionEvent {}

    record ExecutionCompleted(UUID executionId, String duration,
                              long totalItems, Instant timestamp) implements ExecutionEvent {}
}
