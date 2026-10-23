package com.batchforge.api;

public class ExecutionConflictException extends RuntimeException {
    public ExecutionConflictException(String message) {
        super(message);
    }
}
