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
                connection.sendTextAndAwait(mapper.writeValueAsString(toWireFormat(event)));
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

    record WireEvent(String type, String timestamp, Map<String, Object> data) {}

    static WireEvent toWireFormat(ExecutionEvent event) {
        String ts = event.timestamp().toString();
        return switch (event) {
            case ExecutionEvent.StepStarted e ->
                new WireEvent("STEP_STARTED", ts, Map.of("step", e.step()));
            case ExecutionEvent.ChunkCompleted e ->
                new WireEvent("CHUNK_COMPLETED", ts, Map.of("step", e.step(), "partition", e.partition(),
                    "offset", e.offset(), "itemsPerSec", e.itemsPerSec()));
            case ExecutionEvent.ItemFailed e ->
                new WireEvent("ITEM_FAILED", ts, Map.of("step", e.step(), "itemId", e.itemId(),
                    "error", String.valueOf(e.error()), "attempt", e.attempt()));
            case ExecutionEvent.StepCompleted e ->
                new WireEvent("STEP_COMPLETED", ts, Map.of("step", e.step(), "duration", e.duration(),
                    "itemsProcessed", e.itemsProcessed()));
            case ExecutionEvent.ExecutionCompleted e ->
                new WireEvent("EXECUTION_COMPLETED", ts, Map.of("duration", e.duration(),
                    "totalItems", e.totalItems()));
        };
    }
}
