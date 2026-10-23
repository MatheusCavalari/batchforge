package com.batchforge.api;

import com.batchforge.api.dto.ErrorResponse;
import com.batchforge.api.dto.StepExecutionDto;
import com.batchforge.engine.core.StepExecution;
import com.batchforge.engine.metrics.MetricsCollector;
import com.batchforge.engine.metrics.StepMetricsSnapshot;
import com.batchforge.engine.repository.StepExecutionRepository;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Path("/api/executions/{executionId}/steps")
@Produces(MediaType.APPLICATION_JSON)
public class StepResource {

    @Inject StepExecutionRepository stepExecRepo;
    @Inject MetricsCollector metricsCollector;

    @GET
    public List<StepExecutionDto> listSteps(@PathParam("executionId") UUID executionId) {
        return stepExecRepo.findByJobExecutionId(executionId).stream()
            .map(this::toDto)
            .toList();
    }

    @GET
    @Path("/{stepName}/metrics")
    public Response getStepMetrics(
            @PathParam("executionId") UUID executionId,
            @PathParam("stepName") String stepName) {
        return stepExecRepo.findByJobExecutionId(executionId).stream()
            .filter(s -> s.stepName().equals(stepName))
            .findFirst()
            .map(s -> {
                StepMetricsSnapshot snapshot = metricsCollector.snapshot(s.id());
                return Response.ok(Map.of(
                    "stepName", stepName,
                    "itemsPerSecond", snapshot.itemsPerSecond(),
                    "latencyP50Ms", snapshot.latencyP50Ms(),
                    "latencyP95Ms", snapshot.latencyP95Ms(),
                    "latencyP99Ms", snapshot.latencyP99Ms(),
                    "errorCount", snapshot.errorCount()
                )).build();
            })
            .orElse(Response.status(404)
                .entity(new ErrorResponse("Not Found", "Step not found: " + stepName)).build());
    }

    private StepExecutionDto toDto(StepExecution s) {
        return new StepExecutionDto(s.id(), s.stepName(), s.status().toDbValue(),
            s.partitionId(), s.chunksProcessed(), s.chunksTotal(),
            s.itemsRead(), s.itemsWritten(), s.itemsFailed(),
            s.startedAt(), s.finishedAt(), s.errorMessage());
    }
}
