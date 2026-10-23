package com.batchforge.ws;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionEventBusTest {

    @Test
    void subscribe_and_publish() {
        var bus = new ExecutionEventBus();
        var received = new ArrayList<ExecutionEvent>();
        var execId = UUID.randomUUID();

        bus.subscribe(execId, received::add);
        bus.publish(new ExecutionEvent.StepStarted(execId, "step1", Instant.now()));

        assertThat(received).hasSize(1);
        assertThat(received.getFirst()).isInstanceOf(ExecutionEvent.StepStarted.class);
    }

    @Test
    void unsubscribe_stops_delivery() {
        var bus = new ExecutionEventBus();
        var received = new ArrayList<ExecutionEvent>();
        var execId = UUID.randomUUID();

        Consumer<ExecutionEvent> listener = received::add;
        bus.subscribe(execId, listener);
        bus.publish(new ExecutionEvent.StepStarted(execId, "step1", Instant.now()));
        bus.unsubscribe(execId, listener);
        bus.publish(new ExecutionEvent.StepCompleted(execId, "step1", "1s", 100, Instant.now()));

        assertThat(received).hasSize(1);
    }

    @Test
    void publish_to_wrong_execution_does_not_deliver() {
        var bus = new ExecutionEventBus();
        var received = new ArrayList<ExecutionEvent>();

        bus.subscribe(UUID.randomUUID(), received::add);
        bus.publish(new ExecutionEvent.StepStarted(UUID.randomUUID(), "step1", Instant.now()));

        assertThat(received).isEmpty();
    }

    @Test
    void failing_listener_does_not_block_others() {
        var bus = new ExecutionEventBus();
        var received = new ArrayList<ExecutionEvent>();
        var execId = UUID.randomUUID();

        bus.subscribe(execId, _ -> { throw new RuntimeException("boom"); });
        bus.subscribe(execId, received::add);
        bus.publish(new ExecutionEvent.StepStarted(execId, "s", Instant.now()));

        assertThat(received).hasSize(1);
    }
}
