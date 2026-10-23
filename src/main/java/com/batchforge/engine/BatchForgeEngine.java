package com.batchforge.engine;

import com.batchforge.engine.core.ExecutionStatus;
import com.batchforge.engine.core.JobExecution;
import com.batchforge.engine.dag.DagScheduler;
import com.batchforge.engine.registry.JobRegistry;
import com.batchforge.engine.repository.JobExecutionRepository;
import com.batchforge.yaml.JobDefinitionParser;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class BatchForgeEngine {

    private static final Logger LOG = Logger.getLogger(BatchForgeEngine.class);

    @Inject JobRegistry registry;
    @Inject DagScheduler scheduler;
    @Inject JobExecutionRepository jobExecRepo;
    @Inject JobDefinitionParser parser;

    private final Map<UUID, Thread> runningJobs = new ConcurrentHashMap<>();

    @ConfigProperty(name = "batchforge.jobs-dir", defaultValue = "jobs")
    String jobsDir;

    public void start() {
        Path dir = Path.of(jobsDir);
        if (dir.toFile().exists()) {
            registry.loadFromDirectory(dir);
            registry.startWatching(dir);
            LOG.infof("BatchForge started, watching '%s'", dir);
        }
    }

    public void loadJobFile(Path file) throws Exception {
        registry.register(parser.parseFile(file));
    }

    public UUID executeJob(String jobName, Map<String, Object> parameters) {
        var jobDef = registry.getJob(jobName)
            .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobName));

        var execution = jobExecRepo.save(JobExecution.create(jobName, parameters));

        var thread = Thread.ofVirtual().name("job-" + execution.id()).unstarted(() -> {
            try {
                scheduler.execute(jobDef, execution);
            } catch (Exception e) {
                LOG.errorf("Job '%s' execution %s failed: %s", jobName, execution.id(), e.getMessage());
            } finally {
                runningJobs.remove(execution.id());
            }
        });
        runningJobs.put(execution.id(), thread);
        thread.start();

        return execution.id();
    }

    public Optional<JobExecution> getExecution(UUID id) {
        return jobExecRepo.findById(id);
    }

    public void cancelExecution(UUID executionId) {
        var exec = jobExecRepo.findById(executionId)
            .orElseThrow(() -> new com.batchforge.api.ExecutionNotFoundException("Execution not found: " + executionId));
        if (exec.status() instanceof ExecutionStatus.Completed ||
            exec.status() instanceof ExecutionStatus.Failed ||
            exec.status() instanceof ExecutionStatus.Abandoned) {
            throw new com.batchforge.api.ExecutionConflictException(
                "Cannot cancel execution in " + exec.status().toDbValue() + " state");
        }
        var thread = runningJobs.remove(executionId);
        if (thread != null) {
            thread.interrupt();
        }
        jobExecRepo.updateStatus(executionId, new ExecutionStatus.Failed("Cancelled by user"));
    }

    public UUID restartExecution(UUID executionId) {
        var exec = jobExecRepo.findById(executionId)
            .orElseThrow(() -> new com.batchforge.api.ExecutionNotFoundException("Execution not found: " + executionId));
        if (!(exec.status() instanceof ExecutionStatus.Failed) &&
            !(exec.status() instanceof ExecutionStatus.Abandoned)) {
            throw new com.batchforge.api.ExecutionConflictException("Can only restart FAILED or ABANDONED executions");
        }
        return executeJob(exec.jobName(), exec.parameters());
    }
}
