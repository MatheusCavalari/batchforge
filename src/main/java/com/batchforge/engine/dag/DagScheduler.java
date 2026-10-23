package com.batchforge.engine.dag;

import com.batchforge.engine.checkpoint.CheckpointManager;
import com.batchforge.engine.core.*;
import com.batchforge.engine.deadletter.DeadLetterManager;
import com.batchforge.engine.executor.StepExecutor;
import com.batchforge.engine.partition.PartitionManager;
import com.batchforge.engine.repository.*;
import com.batchforge.engine.retry.RetryPolicy;
import com.batchforge.spi.*;
import com.batchforge.ws.ExecutionEvent;
import com.batchforge.ws.ExecutionEventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.concurrent.StructuredTaskScope;

@ApplicationScoped
public class DagScheduler {

    @Inject DagValidator dagValidator;
    @Inject StepExecutionRepository stepExecRepo;
    @Inject JobExecutionRepository jobExecRepo;
    @Inject CheckpointManager checkpointManager;
    @Inject DeadLetterManager deadLetterManager;
    @Inject PartitionManager partitionManager;
    @Inject ReaderWriterFactory readerWriterFactory;
    @Inject ExecutionEventBus eventBus;

    private final Map<UUID, AtomicLong> totalItemsByExec = new ConcurrentHashMap<>();

    private void fire(ExecutionEvent event) {
        if (eventBus != null) eventBus.publish(event);
    }

    public void execute(JobDefinition jobDef, JobExecution jobExec) {
        Instant execStart = Instant.now();
        AtomicLong totalItems = totalItemsByExec.computeIfAbsent(jobExec.id(), _ -> new AtomicLong());
        jobExecRepo.updateStatus(jobExec.id(), new ExecutionStatus.Running());

        try {
            List<List<String>> levels = dagValidator.topologicalSort(jobDef);

            for (List<String> level : levels) {
                if (!executeLevel(level, jobDef, jobExec)) {
                    jobExecRepo.updateStatus(jobExec.id(), new ExecutionStatus.Failed("Step failure in level"));
                    fire(new ExecutionEvent.ExecutionCompleted(jobExec.id(),
                        Duration.between(execStart, Instant.now()).toString(), totalItems.get(), "FAILED", Instant.now()));
                    return;
                }
            }

            jobExecRepo.updateStatus(jobExec.id(), new ExecutionStatus.Completed());
            fire(new ExecutionEvent.ExecutionCompleted(jobExec.id(),
                Duration.between(execStart, Instant.now()).toString(), totalItems.get(), "COMPLETED", Instant.now()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            jobExecRepo.updateStatus(jobExec.id(), new ExecutionStatus.Failed("Interrupted"));
            fire(new ExecutionEvent.ExecutionCompleted(jobExec.id(),
                Duration.between(execStart, Instant.now()).toString(), totalItems.get(), "FAILED", Instant.now()));
        } catch (Exception e) {
            jobExecRepo.updateStatus(jobExec.id(), new ExecutionStatus.Failed(e.getMessage()));
            fire(new ExecutionEvent.ExecutionCompleted(jobExec.id(),
                Duration.between(execStart, Instant.now()).toString(), totalItems.get(), "FAILED", Instant.now()));
        } finally {
            totalItemsByExec.remove(jobExec.id());
        }
    }

    private boolean executeLevel(List<String> stepNames, JobDefinition jobDef, JobExecution jobExec)
            throws InterruptedException {

        if (stepNames.size() == 1) {
            return executeSingleStep(stepNames.getFirst(), jobDef, jobExec);
        }

        try (var scope = StructuredTaskScope.open()) {
            List<StructuredTaskScope.Subtask<Boolean>> subtasks = new ArrayList<>();
            for (String stepName : stepNames) {
                subtasks.add(scope.fork(() -> executeSingleStep(stepName, jobDef, jobExec)));
            }
            scope.join();
            return subtasks.stream().allMatch(st ->
                st.state() == StructuredTaskScope.Subtask.State.SUCCESS && Boolean.TRUE.equals(st.get()));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean executeSingleStep(String stepName, JobDefinition jobDef, JobExecution jobExec) {
        StepDefinition stepDef = jobDef.steps().get(stepName);

        var stepExec = stepExecRepo.save(StepExecution.create(jobExec.id(), stepName, "main"));
        stepExecRepo.updateStatus(stepExec.id(), new StepStatus.StepRunning());
        fire(new ExecutionEvent.StepStarted(jobExec.id(), stepName, Instant.now()));
        Instant stepStart = Instant.now();

        try {
            var ctx = new ExecutionContext(
                jobExec.id(), stepExec.id(), stepName, "main",
                jobExec.parameters(), new java.util.HashMap<String, Object>(stepDef.reader().properties())
            );

            ItemReader reader = readerWriterFactory.createReader(stepDef.reader());
            ItemWriter writer = readerWriterFactory.createWriter(stepDef.writer());
            ItemProcessor processor = readerWriterFactory.createProcessor(stepDef.processorClass());
            RetryPolicy retryPolicy = RetryPolicy.from(stepDef.retry());

            var executor = new StepExecutor(checkpointManager, deadLetterManager, eventBus);
            var result = executor.execute(ctx, reader, processor, writer, stepDef.chunkSize(), retryPolicy);

            stepExecRepo.updateProgress(stepExec.id(),
                result.itemsRead() / Math.max(stepDef.chunkSize(), 1),
                result.itemsRead(), result.itemsWritten(), result.itemsFailed());
            stepExecRepo.updateStatus(stepExec.id(), new StepStatus.StepCompleted());
            var counter = totalItemsByExec.get(jobExec.id());
            if (counter != null) counter.addAndGet(result.itemsWritten());
            fire(new ExecutionEvent.StepCompleted(jobExec.id(), stepName,
                Duration.between(stepStart, Instant.now()).toString(), result.itemsWritten(), Instant.now()));
            return true;
        } catch (Exception e) {
            stepExecRepo.updateStatus(stepExec.id(), new StepStatus.StepFailed(e.getMessage()));
            return stepDef.skipOnFail();
        }
    }
}
