package com.batchforge.ws;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

@ApplicationScoped
public class ExecutionEventBus {

    private final Map<UUID, List<Consumer<ExecutionEvent>>> subscribers = new ConcurrentHashMap<>();

    public void subscribe(UUID executionId, Consumer<ExecutionEvent> listener) {
        subscribers.compute(executionId, (_, list) -> {
            var result = list != null ? list : new CopyOnWriteArrayList<Consumer<ExecutionEvent>>();
            result.add(listener);
            return result;
        });
    }

    public void unsubscribe(UUID executionId, Consumer<ExecutionEvent> listener) {
        subscribers.computeIfPresent(executionId, (_, listeners) -> {
            listeners.remove(listener);
            return listeners.isEmpty() ? null : listeners;
        });
    }

    public void publish(ExecutionEvent event) {
        var listeners = subscribers.get(event.executionId());
        if (listeners != null) {
            listeners.forEach(l -> {
                try { l.accept(event); } catch (Exception ignored) {}
            });
        }
    }
}
