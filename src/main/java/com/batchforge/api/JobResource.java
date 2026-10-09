package com.batchforge.api;

import com.batchforge.api.dto.ErrorResponse;
import com.batchforge.api.dto.JobDto;
import com.batchforge.api.dto.JobExecutionDto;
import com.batchforge.engine.core.JobExecution;
import com.batchforge.engine.registry.JobRegistry;
import com.batchforge.engine.repository.JobExecutionRepository;
import com.batchforge.yaml.JobDefinitionParser;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;

@Path("/api/jobs")
@Produces(MediaType.APPLICATION_JSON)
public class JobResource {

    @Inject JobRegistry registry;
    @Inject JobExecutionRepository jobExecRepo;
    @Inject JobDefinitionParser parser;

    @GET
    public List<JobDto> listJobs() {
        return registry.listJobs().stream().map(job -> {
            var executions = jobExecRepo.findByJobName(job.name(), 1);
            var last = executions.isEmpty() ? null : executions.getFirst();
            return new JobDto(job.name(), job.description(), job.schedule(),
                job.steps().size(),
                last != null ? last.status().toDbValue() : null,
                last != null ? last.startedAt() : null);
        }).toList();
    }

    @GET
    @Path("/{name}")
    public Response getJob(@PathParam("name") String name) {
        return registry.getJob(name)
            .map(job -> {
                var executions = jobExecRepo.findByJobName(name, 10);
                return Response.ok(new JobDetailResponse(
                    job.name(), job.description(), job.schedule(),
                    job.steps().keySet().stream().sorted().toList(),
                    job.steps().entrySet().stream().map(e -> new StepSummary(
                        e.getKey(), e.getValue().reader().type(),
                        e.getValue().writer().type(), e.getValue().chunkSize(),
                        e.getValue().dependsOn()
                    )).toList(),
                    executions.stream().map(this::toExecDto).toList()
                )).build();
            })
            .orElse(notFound(name));
    }

    @PUT
    @Path("/{name}")
    @Consumes(MediaType.TEXT_PLAIN)
    public Response registerJob(@PathParam("name") String name, String yamlBody) {
        var job = parser.parse(yamlBody);
        if (!job.name().equals(name)) {
            throw new IllegalArgumentException(
                "Job name in body '" + job.name() + "' does not match path '" + name + "'");
        }
        registry.register(job);
        if (registry.getJob(name).isEmpty()) {
            throw new IllegalArgumentException("Job '" + name + "' failed DAG validation");
        }
        return Response.ok(new JobDto(job.name(), job.description(), job.schedule(),
            job.steps().size(), null, null)).build();
    }

    @DELETE
    @Path("/{name}")
    public Response deleteJob(@PathParam("name") String name) {
        return registry.remove(name)
            ? Response.noContent().build()
            : notFound(name);
    }

    private static Response notFound(String name) {
        return Response.status(404).entity(new ErrorResponse("Not Found", "Job not found: " + name)).build();
    }

    private JobExecutionDto toExecDto(JobExecution e) {
        return new JobExecutionDto(
            e.id(), e.jobName(), e.status().toDbValue(), e.parameters(),
            e.startedAt(), e.finishedAt(), e.restartCount(), e.errorMessage());
    }

    public record JobDetailResponse(
        String name, String description, String schedule,
        List<String> stepOrder, List<StepSummary> steps,
        List<JobExecutionDto> recentExecutions
    ) {}

    public record StepSummary(
        String name, String readerType, String writerType,
        int chunkSize, List<String> dependsOn
    ) {}
}
