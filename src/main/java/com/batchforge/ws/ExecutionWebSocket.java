package com.batchforge.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.websockets.next.OnClose;
import io.quarkus.websockets.next.OnOpen;
import io.quarkus.websockets.next.WebSocket;
import io.quarkus.websockets.next.WebSocketConnection;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@WebSocket(path = "/ws/executions/{id}")
public class ExecutionWebSocket {

    @Inject ExecutionEventBus eventBus;
    @Inject ObjectMapper mapper;

    private record Subscription(UUID executionId, Consumer<ExecutionEvent> listener) {}

    // Keyed by connection id so state is safe regardless of endpoint instance scope.
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

    @OnOpen
    public void onOpen(WebSocketConnection connection) {
        UUID executionId;
        try {
            executionId = UUID.fromString(connection.pathParam("id"));
        } catch (IllegalArgumentException e) {
            connection.close().subscribe().with(_ -> {}, _ -> {});
            return;
        }
        Consumer<ExecutionEvent> listener = event -> {
            try {
                connection.sendText(mapper.writeValueAsString(toWireFormat(event))).subscribe().with(_ -> {}, _ -> {});
            } catch (Exception ignored) {}
        };
        subscriptions.put(connection.id(), new Subscription(executionId, listener));
        eventBus.subscribe(executionId, listener);
    }

    @OnClose
    public void onClose(WebSocketConnection connection) {
        var sub = subscriptions.remove(connection.id());
        if (sub != null) {
            eventBus.unsubscribe(sub.executionId(), sub.listener());
        }
    }

    static Map<String, Object> toWireFormat(ExecutionEvent event) {
        String ts = event.timestamp().toString();
        return switch (event) {
            case ExecutionEvent.StepStarted e -> Map.of(
                "type", "STEP_STARTED", "timestamp", ts, "step", e.step());
            case ExecutionEvent.ChunkCompleted e -> {
                var map = new java.util.HashMap<String, Object>();
                map.put("type", "CHUNK_COMPLETED");
                map.put("timestamp", ts);
                map.put("step", e.step());
                map.put("partition", e.partition() != null ? e.partition() : "default");
                map.put("offset", e.offset());
                map.put("itemsPerSec", e.itemsPerSec());
                yield java.util.Collections.unmodifiableMap(map);
            }
            case ExecutionEvent.ItemFailed e -> Map.of(
                "type", "ITEM_FAILED", "timestamp", ts, "step", e.step(),
                "itemId", e.itemId(), "error", String.valueOf(e.error()), "attempt", e.attempt());
            case ExecutionEvent.StepCompleted e -> Map.of(
                "type", "STEP_COMPLETED", "timestamp", ts, "step", e.step(),
                "duration", e.duration(), "itemsProcessed", e.itemsProcessed());
            case ExecutionEvent.ExecutionCompleted e -> Map.of(
                "type", "EXECUTION_COMPLETED", "timestamp", ts, "duration", e.duration(),
                "totalItems", e.totalItems(), "status", e.status());
        };
    }
}
