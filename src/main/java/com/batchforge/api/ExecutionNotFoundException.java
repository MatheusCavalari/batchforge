package com.batchforge.api;

public class ExecutionNotFoundException extends RuntimeException {
    public ExecutionNotFoundException(String message) {
        super(message);
    }
}
