package com.batchforge.api;

import com.batchforge.api.dto.*;
import com.batchforge.engine.BatchForgeEngine;
import com.batchforge.engine.core.JobExecution;
import com.batchforge.engine.core.StepExecution;
import com.batchforge.engine.repository.JobExecutionRepository;
import com.batchforge.engine.repository.StepExecutionRepository;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class ExecutionResource {

    @Inject BatchForgeEngine engine;
    @Inject JobExecutionRepository jobExecRepo;
    @Inject StepExecutionRepository stepExecRepo;

    @POST
    @Path("/jobs/{name}/execute")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response executeJob(@PathParam("name") String name, ExecuteJobRequest request) {
        Map<String, Object> params = request != null ? request.parameters() : Map.of();
        UUID id = engine.executeJob(name, params);
        return Response.created(URI.create("/api/executions/" + id))
            .entity(Map.of("id", id)).build();
    }

    @GET
    @Path("/executions")
    public List<JobExecutionDto> listExecutions(
            @QueryParam("job") String jobFilter,
            @QueryParam("status") String statusFilter,
            @QueryParam("limit") @DefaultValue("50") int limit) {
        return jobExecRepo.findAll(jobFilter, statusFilter, limit).stream()
            .map(this::toDto).toList();
    }

    @GET
    @Path("/executions/{id}")
    public Response getExecution(@PathParam("id") UUID id) {
        return engine.getExecution(id)
            .map(exec -> {
                var steps = stepExecRepo.findByJobExecutionId(id).stream()
                    .map(this::toStepDto).toList();
                return Response.ok(new ExecutionDetailResponse(toDto(exec), steps)).build();
            })
            .orElse(Response.status(404)
                .entity(new ErrorResponse("Not Found", "Execution not found: " + id)).build());
    }

    @POST
    @Path("/executions/{id}/restart")
    public Response restartExecution(@PathParam("id") UUID id) {
        UUID newId = engine.restartExecution(id);
        return Response.ok(Map.of("id", newId)).build();
    }

    @POST
    @Path("/executions/{id}/cancel")
    public Response cancelExecution(@PathParam("id") UUID id) {
        engine.cancelExecution(id);
        return Response.ok(Map.of("status", "cancelled")).build();
    }

    private JobExecutionDto toDto(JobExecution e) {
        return new JobExecutionDto(e.id(), e.jobName(), e.status().toDbValue(),
            e.parameters(), e.startedAt(), e.finishedAt(), e.restartCount(), e.errorMessage());
    }

    private StepExecutionDto toStepDto(StepExecution s) {
        return new StepExecutionDto(s.id(), s.stepName(), s.status().toDbValue(),
            s.partitionId(), s.chunksProcessed(), s.chunksTotal(),
            s.itemsRead(), s.itemsWritten(), s.itemsFailed(),
            s.startedAt(), s.finishedAt(), s.errorMessage());
    }

    public record ExecutionDetailResponse(
        JobExecutionDto execution,
        List<StepExecutionDto> steps
    ) {}
}
