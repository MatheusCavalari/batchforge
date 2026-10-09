# BatchForge REST API + WebSocket Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the BatchForge engine via REST endpoints for job management, execution control, dead letter operations, and real-time WebSocket streaming of execution events.

**Architecture:** Quarkus REST (Jakarta REST) resource classes delegate to engine services. A WebSocket endpoint backed by an in-memory event bus publishes execution lifecycle events. JSON serialization via Jackson with custom serializers for sealed interfaces. REST-Assured integration tests against a live Quarkus test instance.

**Tech Stack:** Java 25 (preview), Quarkus 3.21.0 (quarkus-rest-jackson, quarkus-websockets-next), Jackson, REST-Assured, AssertJ

## Global Constraints

- Java 25 with `--enable-preview` (virtual threads, sealed interfaces, records, pattern matching)
- Quarkus 3.21.0 — use `quarkus-rest-jackson` (Jakarta REST), NOT `quarkus-resteasy`
- Quarkus WebSockets Next (`quarkus-websockets-next`), NOT the legacy `quarkus-websocket` extension
- Do NOT use Mockito — incompatible with Java 25. Use hand-written stubs/fakes
- `StructuredTaskScope.open()` for structured concurrency — `ShutdownOnFailure` was removed in Java 25
- All domain types are records with sealed interfaces (ExecutionStatus, StepStatus)
- Jackson needs custom serialization for sealed interface records (they don't auto-serialize nicely)
- REST-Assured for HTTP integration tests; hand-written fakes for unit tests
- No Co-Authored-By: Claude trailer on commits

---

### Task 1: Jackson Serialization + DTO Layer

**Files:**
- Create: `src/main/java/com/batchforge/api/dto/JobDto.java`
- Create: `src/main/java/com/batchforge/api/dto/JobExecutionDto.java`
- Create: `src/main/java/com/batchforge/api/dto/StepExecutionDto.java`
- Create: `src/main/java/com/batchforge/api/dto/DeadLetterItemDto.java`
- Create: `src/main/java/com/batchforge/api/dto/ExecuteJobRequest.java`
- Create: `src/main/java/com/batchforge/api/dto/ErrorResponse.java`
- Create: `src/main/java/com/batchforge/api/serialization/ExecutionStatusSerializer.java`
- Create: `src/main/java/com/batchforge/api/serialization/StepStatusSerializer.java`
- Create: `src/main/java/com/batchforge/api/serialization/BatchForgeObjectMapperCustomizer.java`
- Create: `src/test/java/com/batchforge/api/dto/DtoSerializationTest.java`

**Interfaces:**
- Consumes: `ExecutionStatus` (sealed interface with `toDbValue()`), `StepStatus` (sealed), `JobExecution`, `StepExecution`, `DeadLetterItem`, `JobDefinition`, `StepDefinition` from `com.batchforge.engine.core`
- Produces: DTO records used by all REST resources in Tasks 2-5. `JobDto(String name, String description, String schedule, int stepCount, String lastExecutionStatus, Instant lastExecutionAt)`. `JobExecutionDto(UUID id, String jobName, String status, Map<String,Object> parameters, Instant startedAt, Instant finishedAt, int restartCount, String errorMessage)`. `StepExecutionDto(UUID id, String stepName, String status, String partitionId, int chunksProcessed, Integer chunksTotal, long itemsRead, long itemsWritten, long itemsFailed, Instant startedAt, Instant finishedAt, String errorMessage)`. `DeadLetterItemDto(UUID id, UUID stepExecutionId, String stepName, String itemPayload, String errorMessage, String stackTrace, int attemptCount, String status, Instant failedAt)`. `ExecuteJobRequest(Map<String,Object> parameters)`. `ErrorResponse(String error, String detail)`.

- [ ] **Step 1: Write the DTO records**

```java
// JobDto.java
package com.batchforge.api.dto;

import java.time.Instant;

public record JobDto(
    String name,
    String description,
    String schedule,
    int stepCount,
    String lastExecutionStatus,
    Instant lastExecutionAt
) {}
```

```java
// JobExecutionDto.java
package com.batchforge.api.dto;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record JobExecutionDto(
    UUID id,
    String jobName,
    String status,
    Map<String, Object> parameters,
    Instant startedAt,
    Instant finishedAt,
    int restartCount,
    String errorMessage
) {}
```

```java
// StepExecutionDto.java
package com.batchforge.api.dto;

import java.time.Instant;
import java.util.UUID;

public record StepExecutionDto(
    UUID id,
    String stepName,
    String status,
    String partitionId,
    int chunksProcessed,
    Integer chunksTotal,
    long itemsRead,
    long itemsWritten,
    long itemsFailed,
    Instant startedAt,
    Instant finishedAt,
    String errorMessage
) {}
```

```java
// DeadLetterItemDto.java
package com.batchforge.api.dto;

import java.time.Instant;
import java.util.UUID;

public record DeadLetterItemDto(
    UUID id,
    UUID stepExecutionId,
    String stepName,
    String itemPayload,
    String errorMessage,
    String stackTrace,
    int attemptCount,
    String status,
    Instant failedAt
) {}
```

```java
// ExecuteJobRequest.java
package com.batchforge.api.dto;

import java.util.Map;

public record ExecuteJobRequest(Map<String, Object> parameters) {
    public ExecuteJobRequest {
        if (parameters == null) parameters = Map.of();
    }
}
```

```java
// ErrorResponse.java
package com.batchforge.api.dto;

public record ErrorResponse(String error, String detail) {}
```

- [ ] **Step 2: Write custom Jackson serializers for sealed interfaces**

`ExecutionStatus` and `StepStatus` are sealed interfaces whose records don't serialize nicely by default (Jackson doesn't know how to pick the type). We serialize them as simple strings using `toDbValue()`.

```java
// ExecutionStatusSerializer.java
package com.batchforge.api.serialization;

import com.batchforge.engine.core.ExecutionStatus;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class ExecutionStatusSerializer extends JsonSerializer<ExecutionStatus> {
    @Override
    public void serialize(ExecutionStatus value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeString(value.toDbValue());
    }
}
```

```java
// StepStatusSerializer.java
package com.batchforge.api.serialization;

import com.batchforge.engine.core.StepStatus;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class StepStatusSerializer extends JsonSerializer<StepStatus> {
    @Override
    public void serialize(StepStatus value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeString(value.toDbValue());
    }
}
```

```java
// BatchForgeObjectMapperCustomizer.java
package com.batchforge.api.serialization;

import com.batchforge.engine.core.ExecutionStatus;
import com.batchforge.engine.core.StepStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.quarkus.jackson.ObjectMapperCustomizer;
import jakarta.inject.Singleton;

@Singleton
public class BatchForgeObjectMapperCustomizer implements ObjectMapperCustomizer {
    @Override
    public void customize(ObjectMapper objectMapper) {
        SimpleModule module = new SimpleModule("BatchForge");
        module.addSerializer(ExecutionStatus.class, new ExecutionStatusSerializer());
        module.addSerializer(StepStatus.class, new StepStatusSerializer());
        objectMapper.registerModule(module);
    }
}
```

- [ ] **Step 3: Write serialization tests**

```java
// DtoSerializationTest.java
package com.batchforge.api.dto;

import com.batchforge.api.serialization.ExecutionStatusSerializer;
import com.batchforge.api.serialization.StepStatusSerializer;
import com.batchforge.engine.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DtoSerializationTest {

    private final ObjectMapper mapper = createMapper();

    private static ObjectMapper createMapper() {
        ObjectMapper om = new ObjectMapper();
        om.findAndRegisterModules();
        SimpleModule mod = new SimpleModule();
        mod.addSerializer(ExecutionStatus.class, new ExecutionStatusSerializer());
        mod.addSerializer(StepStatus.class, new StepStatusSerializer());
        om.registerModule(mod);
        return om;
    }

    @Test
    void jobExecutionDto_serializes_to_json() throws Exception {
        var dto = new JobExecutionDto(UUID.randomUUID(), "test-job", "RUNNING",
            Map.of("key", "value"), Instant.now(), null, 0, null);
        String json = mapper.writeValueAsString(dto);
        assertThat(json).contains("\"status\":\"RUNNING\"");
        assertThat(json).contains("\"jobName\":\"test-job\"");
    }

    @Test
    void executionStatus_serializes_as_string() throws Exception {
        ExecutionStatus status = new ExecutionStatus.Failed("boom");
        String json = mapper.writeValueAsString(status);
        assertThat(json).isEqualTo("\"FAILED\"");
    }

    @Test
    void stepStatus_serializes_as_string() throws Exception {
        StepStatus status = new StepStatus.Retrying(3);
        String json = mapper.writeValueAsString(status);
        assertThat(json).isEqualTo("\"RETRYING\"");
    }

    @Test
    void deadLetterItemDto_round_trips() throws Exception {
        var dto = new DeadLetterItemDto(UUID.randomUUID(), UUID.randomUUID(), "step1",
            "{\"id\":1}", "bad value", null, 3, "PENDING", Instant.now());
        String json = mapper.writeValueAsString(dto);
        var back = mapper.readValue(json, DeadLetterItemDto.class);
        assertThat(back.stepName()).isEqualTo("step1");
        assertThat(back.attemptCount()).isEqualTo(3);
    }
}
```

- [ ] **Step 4: Run tests**

Run: `mvn test -f pom.xml -Dtest=DtoSerializationTest -pl .`
Expected: 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/batchforge/api/ src/test/java/com/batchforge/api/
git commit -m "feat(api): DTO layer and Jackson serializers for sealed interfaces"
```

---

### Task 2: Job Management REST Endpoints

**Files:**
- Create: `src/main/java/com/batchforge/api/JobResource.java`
- Create: `src/main/java/com/batchforge/api/ExceptionMappers.java`
- Create: `src/test/java/com/batchforge/api/JobResourceTest.java`

**Interfaces:**
- Consumes: `JobRegistry.listJobs(): List<JobDefinition>`, `JobRegistry.getJob(String): Optional<JobDefinition>`, `JobRegistry.register(JobDefinition)`, `JobDefinitionParser.parse(String yamlContent): JobDefinition`, `JobExecutionRepository.findByJobName(String, int): List<JobExecution>` from engine layer. DTOs from Task 1.
- Produces: `JobResource` JAX-RS resource: `GET /api/jobs`, `GET /api/jobs/{name}`, `PUT /api/jobs/{name}`, `DELETE /api/jobs/{name}`. `ExceptionMappers` with `@ServerExceptionMapper` for common errors. Used by the dashboard in Sub-Project 4.

- [ ] **Step 1: Write the ExceptionMappers**

```java
// ExceptionMappers.java
package com.batchforge.api;

import com.batchforge.api.dto.ErrorResponse;
import com.batchforge.yaml.JobValidationException;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

public class ExceptionMappers {

    @ServerExceptionMapper
    public Response handleIllegalArgument(IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(new ErrorResponse("Bad Request", e.getMessage())).build();
    }

    @ServerExceptionMapper
    public Response handleJobValidation(JobValidationException e) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(new ErrorResponse("Validation Error", e.getMessage())).build();
    }
}
```

- [ ] **Step 2: Write the JobResource**

```java
// JobResource.java
package com.batchforge.api;

import com.batchforge.api.dto.ErrorResponse;
import com.batchforge.api.dto.JobDto;
import com.batchforge.engine.core.JobExecution;
import com.batchforge.engine.registry.JobRegistry;
import com.batchforge.engine.repository.JobExecutionRepository;
import com.batchforge.yaml.JobDefinitionParser;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Comparator;
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
            .orElse(Response.status(404).entity(new ErrorResponse("Not Found", "Job not found: " + name)).build());
    }

    @PUT
    @Path("/{name}")
    @Consumes(MediaType.TEXT_PLAIN)
    public Response registerJob(@PathParam("name") String name, String yamlBody) {
        var job = parser.parse(yamlBody);
        registry.register(job);
        return Response.ok(new JobDto(job.name(), job.description(), job.schedule(),
            job.steps().size(), null, null)).build();
    }

    @DELETE
    @Path("/{name}")
    public Response deleteJob(@PathParam("name") String name) {
        return registry.getJob(name)
            .map(_ -> {
                registry.remove(name);
                return Response.noContent().build();
            })
            .orElse(Response.status(404).entity(new ErrorResponse("Not Found", "Job not found: " + name)).build());
    }

    private com.batchforge.api.dto.JobExecutionDto toExecDto(JobExecution e) {
        return new com.batchforge.api.dto.JobExecutionDto(
            e.id(), e.jobName(), e.status().toDbValue(), e.parameters(),
            e.startedAt(), e.finishedAt(), e.restartCount(), e.errorMessage());
    }

    public record JobDetailResponse(
        String name, String description, String schedule,
        List<String> stepOrder, List<StepSummary> steps,
        List<com.batchforge.api.dto.JobExecutionDto> recentExecutions
    ) {}

    public record StepSummary(
        String name, String readerType, String writerType,
        int chunkSize, List<String> dependsOn
    ) {}
}
```

- [ ] **Step 3: Add `remove(String)` method to JobRegistry**

The `DELETE /api/jobs/{name}` endpoint needs a `remove` method. Add to `JobRegistry`:

```java
public boolean remove(String name) {
    return jobs.remove(name) != null;
}
```

- [ ] **Step 4: Write integration test**

```java
// JobResourceTest.java
package com.batchforge.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
class JobResourceTest {

    @Test
    void listJobs_returns_empty_initially() {
        given().when().get("/api/jobs")
            .then().statusCode(200).body("$.size()", is(0));
    }

    @Test
    void registerJob_then_get() {
        String yaml = """
            name: test-api-job
            steps:
              step1:
                reader:
                  type: csv
                  properties:
                    path: /tmp/test.csv
                writer:
                  type: file
                  properties:
                    path: /tmp/out.csv
                chunk-size: 100
            """;
        given().contentType("text/plain").body(yaml)
            .when().put("/api/jobs/test-api-job")
            .then().statusCode(200)
            .body("name", equalTo("test-api-job"))
            .body("stepCount", equalTo(1));

        given().when().get("/api/jobs/test-api-job")
            .then().statusCode(200)
            .body("name", equalTo("test-api-job"));
    }

    @Test
    void getJob_not_found_returns_404() {
        given().when().get("/api/jobs/nonexistent")
            .then().statusCode(404);
    }
}
```

Note: This is a `@QuarkusTest` — it starts the full Quarkus app. It requires Postgres and Redis to be available (Testcontainers or Docker Compose). If Docker is unavailable, these tests will be deferred like the other integration tests.

- [ ] **Step 5: Run tests**

Run: `mvn test -f pom.xml -Dtest=JobResourceTest`
Expected: 3 tests PASS (or deferred if Docker unavailable)

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/batchforge/api/ src/test/java/com/batchforge/api/JobResourceTest.java
git commit -m "feat(api): job management REST endpoints (CRUD)"
```

---

### Task 3: Execution Control REST Endpoints

**Files:**
- Create: `src/main/java/com/batchforge/api/ExecutionResource.java`
- Modify: `src/main/java/com/batchforge/engine/BatchForgeEngine.java` (add `cancelExecution`, `restartExecution`)
- Modify: `src/main/java/com/batchforge/engine/repository/JobExecutionRepository.java` (add `findAll` with filters)
- Create: `src/test/java/com/batchforge/api/ExecutionResourceTest.java`

**Interfaces:**
- Consumes: `BatchForgeEngine.executeJob(String, Map): UUID`, `BatchForgeEngine.getExecution(UUID): Optional<JobExecution>`, `JobExecutionRepository.findByJobName(String, int)`, `StepExecutionRepository.findByJobExecutionId(UUID)`, DTOs from Task 1.
- Produces: `ExecutionResource` JAX-RS resource: `POST /api/jobs/{name}/execute`, `GET /api/executions`, `GET /api/executions/{id}`, `POST /api/executions/{id}/restart`, `POST /api/executions/{id}/cancel`. `BatchForgeEngine.cancelExecution(UUID)` and `BatchForgeEngine.restartExecution(UUID)` engine methods. `JobExecutionRepository.findAll(String jobName, String status, int limit)` query method. Used by dashboard in Sub-Project 4.

- [ ] **Step 1: Add query methods to JobExecutionRepository**

Add to `JobExecutionRepository`:

```java
public List<JobExecution> findAll(String jobNameFilter, String statusFilter, int limit) {
    var sql = new StringBuilder("SELECT " + COLS + " FROM job_executions WHERE 1=1");
    var params = new ArrayList<>();
    if (jobNameFilter != null && !jobNameFilter.isBlank()) {
        sql.append(" AND job_name = ?");
        params.add(jobNameFilter);
    }
    if (statusFilter != null && !statusFilter.isBlank()) {
        sql.append(" AND status = ?");
        params.add(statusFilter);
    }
    sql.append(" ORDER BY created_at DESC LIMIT ?");
    params.add(limit);
    try (Connection c = dataSource.getConnection();
         PreparedStatement ps = c.prepareStatement(sql.toString())) {
        for (int i = 0; i < params.size(); i++) {
            Object p = params.get(i);
            if (p instanceof Integer n) ps.setInt(i + 1, n);
            else ps.setString(i + 1, (String) p);
        }
        try (ResultSet rs = ps.executeQuery()) {
            List<JobExecution> out = new ArrayList<>();
            while (rs.next()) out.add(map(rs));
            return out;
        }
    } catch (SQLException | JsonProcessingException ex) {
        throw new RepositoryException("Failed to list executions", ex);
    }
}
```

- [ ] **Step 2: Add cancel and restart to BatchForgeEngine**

Add to `BatchForgeEngine`:

```java
private final Map<UUID, Thread> runningJobs = new ConcurrentHashMap<>();

public void cancelExecution(UUID executionId) {
    var thread = runningJobs.get(executionId);
    if (thread != null) {
        thread.interrupt();
        runningJobs.remove(executionId);
    }
    jobExecRepo.updateStatus(executionId, new ExecutionStatus.Failed("Cancelled by user"));
}

public UUID restartExecution(UUID executionId) {
    var exec = jobExecRepo.findById(executionId)
        .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));
    if (!(exec.status() instanceof ExecutionStatus.Failed) &&
        !(exec.status() instanceof ExecutionStatus.Abandoned)) {
        throw new IllegalStateException("Can only restart FAILED or ABANDONED executions");
    }
    return executeJob(exec.jobName(), exec.parameters());
}
```

Also update `executeJob` to track the thread:

```java
// Inside executeJob, change the Thread.ofVirtual() block to store the thread:
var thread = Thread.ofVirtual().name("job-" + execution.id()).start(() -> {
    try {
        scheduler.execute(jobDef, execution);
    } catch (Exception e) {
        LOG.errorf("Job '%s' execution %s failed: %s", jobName, execution.id(), e.getMessage());
    } finally {
        runningJobs.remove(execution.id());
    }
});
runningJobs.put(execution.id(), thread);
```

- [ ] **Step 3: Write ExecutionResource**

```java
// ExecutionResource.java
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
```

- [ ] **Step 4: Write integration tests**

```java
// ExecutionResourceTest.java
package com.batchforge.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
class ExecutionResourceTest {

    @Test
    void listExecutions_empty() {
        given().when().get("/api/executions")
            .then().statusCode(200).body("$.size()", is(0));
    }

    @Test
    void getExecution_not_found() {
        given().when().get("/api/executions/00000000-0000-0000-0000-000000000000")
            .then().statusCode(404);
    }

    @Test
    void executeJob_not_found_returns_400() {
        given().contentType("application/json").body("{\"parameters\":{}}")
            .when().post("/api/jobs/nonexistent/execute")
            .then().statusCode(400);
    }
}
```

- [ ] **Step 5: Run tests**

Run: `mvn test -f pom.xml -Dtest=ExecutionResourceTest`

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/batchforge/api/ExecutionResource.java src/main/java/com/batchforge/engine/BatchForgeEngine.java src/main/java/com/batchforge/engine/repository/JobExecutionRepository.java src/test/java/com/batchforge/api/ExecutionResourceTest.java
git commit -m "feat(api): execution control endpoints (trigger, list, detail, restart, cancel)"
```

---

### Task 4: Dead Letter + Step Metrics REST Endpoints

**Files:**
- Create: `src/main/java/com/batchforge/api/DeadLetterResource.java`
- Create: `src/main/java/com/batchforge/api/StepResource.java`
- Modify: `src/main/java/com/batchforge/engine/repository/DeadLetterRepository.java` (add `findAll`, `findById`, `delete`)
- Create: `src/test/java/com/batchforge/api/DeadLetterResourceTest.java`

**Interfaces:**
- Consumes: `DeadLetterRepository.findByStepExecution(UUID)`, `DeadLetterRepository.findPending(int)`, `DeadLetterRepository.updateStatus(UUID, String)`, `StepExecutionRepository.findByJobExecutionId(UUID)`, `MetricsCollector.snapshot(UUID)`, DTOs from Task 1.
- Produces: `DeadLetterResource`: `GET /api/dead-letter`, `POST /api/dead-letter/{itemId}/retry`, `POST /api/dead-letter/retry-all`, `DELETE /api/dead-letter/{itemId}`. `StepResource`: `GET /api/executions/{id}/steps`, `GET /api/executions/{id}/steps/{name}/metrics`. Used by dashboard in Sub-Project 4.

- [ ] **Step 1: Add query methods to DeadLetterRepository**

```java
public List<DeadLetterItem> findAll(String stepNameFilter, String statusFilter, int limit) {
    var sql = new StringBuilder("SELECT " + COLS + " FROM dead_letter_items WHERE 1=1");
    var params = new ArrayList<>();
    if (stepNameFilter != null && !stepNameFilter.isBlank()) {
        sql.append(" AND step_name = ?");
        params.add(stepNameFilter);
    }
    if (statusFilter != null && !statusFilter.isBlank()) {
        sql.append(" AND status = ?");
        params.add(statusFilter);
    }
    sql.append(" ORDER BY failed_at DESC LIMIT ?");
    params.add(limit);
    return queryWithParams(sql.toString(), params);
}

public Optional<DeadLetterItem> findById(UUID id) {
    var items = query("SELECT " + COLS + " FROM dead_letter_items WHERE id = ?",
        ps -> ps.setObject(1, id));
    return items.isEmpty() ? Optional.empty() : Optional.of(items.getFirst());
}

@Transactional
public void delete(UUID id) {
    try (Connection c = dataSource.getConnection();
         PreparedStatement ps = c.prepareStatement("DELETE FROM dead_letter_items WHERE id = ?")) {
        ps.setObject(1, id);
        ps.executeUpdate();
    } catch (SQLException e) {
        throw new RepositoryException("Failed to delete dead letter item " + id, e);
    }
}

private List<DeadLetterItem> queryWithParams(String sql, List<Object> params) {
    try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
        for (int i = 0; i < params.size(); i++) {
            Object p = params.get(i);
            if (p instanceof Integer n) ps.setInt(i + 1, n);
            else ps.setString(i + 1, (String) p);
        }
        try (ResultSet rs = ps.executeQuery()) {
            List<DeadLetterItem> out = new ArrayList<>();
            while (rs.next()) {
                out.add(new DeadLetterItem(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),
                    rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6),
                    rs.getInt(7), rs.getString(8), rs.getTimestamp(9).toInstant()));
            }
            return out;
        }
    } catch (SQLException e) {
        throw new RepositoryException("Failed to query dead letter items", e);
    }
}
```

- [ ] **Step 2: Write DeadLetterResource**

```java
// DeadLetterResource.java
package com.batchforge.api;

import com.batchforge.api.dto.DeadLetterItemDto;
import com.batchforge.api.dto.ErrorResponse;
import com.batchforge.engine.core.DeadLetterItem;
import com.batchforge.engine.repository.DeadLetterRepository;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Path("/api/dead-letter")
@Produces(MediaType.APPLICATION_JSON)
public class DeadLetterResource {

    @Inject DeadLetterRepository dlqRepo;

    @GET
    public List<DeadLetterItemDto> list(
            @QueryParam("step") String stepFilter,
            @QueryParam("status") @DefaultValue("PENDING") String statusFilter,
            @QueryParam("limit") @DefaultValue("50") int limit) {
        return dlqRepo.findAll(stepFilter, statusFilter, limit).stream()
            .map(this::toDto).toList();
    }

    @POST
    @Path("/{itemId}/retry")
    public Response retryItem(@PathParam("itemId") UUID itemId) {
        return dlqRepo.findById(itemId)
            .map(item -> {
                dlqRepo.updateStatus(itemId, "RETRYING");
                return Response.ok(Map.of("status", "RETRYING")).build();
            })
            .orElse(Response.status(404)
                .entity(new ErrorResponse("Not Found", "Dead letter item not found")).build());
    }

    @POST
    @Path("/retry-all")
    public Response retryAll(@QueryParam("step") String stepFilter) {
        var items = dlqRepo.findAll(stepFilter, "PENDING", 1000);
        items.forEach(item -> dlqRepo.updateStatus(item.id(), "RETRYING"));
        return Response.ok(Map.of("count", items.size())).build();
    }

    @DELETE
    @Path("/{itemId}")
    public Response deleteItem(@PathParam("itemId") UUID itemId) {
        return dlqRepo.findById(itemId)
            .map(item -> {
                dlqRepo.delete(itemId);
                return Response.noContent().build();
            })
            .orElse(Response.status(404)
                .entity(new ErrorResponse("Not Found", "Dead letter item not found")).build());
    }

    private DeadLetterItemDto toDto(DeadLetterItem d) {
        return new DeadLetterItemDto(d.id(), d.stepExecutionId(), d.stepName(),
            d.itemPayload(), d.errorMessage(), d.stackTrace(), d.attemptCount(),
            d.status(), d.failedAt());
    }
}
```

- [ ] **Step 3: Write StepResource**

```java
// StepResource.java
package com.batchforge.api;

import com.batchforge.api.dto.StepExecutionDto;
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
            .map(s -> new StepExecutionDto(s.id(), s.stepName(), s.status().toDbValue(),
                s.partitionId(), s.chunksProcessed(), s.chunksTotal(),
                s.itemsRead(), s.itemsWritten(), s.itemsFailed(),
                s.startedAt(), s.finishedAt(), s.errorMessage()))
            .toList();
    }

    @GET
    @Path("/{stepName}/metrics")
    public Response getStepMetrics(
            @PathParam("executionId") UUID executionId,
            @PathParam("stepName") String stepName) {
        var steps = stepExecRepo.findByJobExecutionId(executionId);
        var stepExec = steps.stream()
            .filter(s -> s.stepName().equals(stepName))
            .findFirst();
        return stepExec.map(s -> {
            StepMetricsSnapshot snapshot = metricsCollector.snapshot(s.id());
            return Response.ok(Map.of(
                "stepName", stepName,
                "itemsPerSecond", snapshot.itemsPerSecond(),
                "latencyP50Ms", snapshot.latencyP50Ms(),
                "latencyP95Ms", snapshot.latencyP95Ms(),
                "latencyP99Ms", snapshot.latencyP99Ms(),
                "errorCount", snapshot.errorCount()
            )).build();
        }).orElse(Response.status(404).build());
    }
}
```

- [ ] **Step 4: Write tests**

```java
// DeadLetterResourceTest.java
package com.batchforge.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
class DeadLetterResourceTest {

    @Test
    void listDeadLetter_empty() {
        given().when().get("/api/dead-letter")
            .then().statusCode(200).body("$.size()", is(0));
    }

    @Test
    void retryItem_not_found() {
        given().when().post("/api/dead-letter/00000000-0000-0000-0000-000000000000/retry")
            .then().statusCode(404);
    }

    @Test
    void deleteItem_not_found() {
        given().when().delete("/api/dead-letter/00000000-0000-0000-0000-000000000000")
            .then().statusCode(404);
    }
}
```

- [ ] **Step 5: Run tests**

Run: `mvn test -f pom.xml -Dtest=DeadLetterResourceTest`

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/batchforge/api/DeadLetterResource.java src/main/java/com/batchforge/api/StepResource.java src/main/java/com/batchforge/engine/repository/DeadLetterRepository.java src/test/java/com/batchforge/api/DeadLetterResourceTest.java
git commit -m "feat(api): dead letter management and step metrics endpoints"
```

---

### Task 5: DAG Builder Helper Endpoints

**Files:**
- Create: `src/main/java/com/batchforge/api/DagBuilderResource.java`
- Create: `src/test/java/com/batchforge/api/DagBuilderResourceTest.java`

**Interfaces:**
- Consumes: `JobDefinitionParser.parse(String): JobDefinition`, `DagValidator.validate(JobDefinition): List<String>`, `DagValidator.topologicalSort(JobDefinition): List<String>` from engine layer.
- Produces: `DagBuilderResource`: `POST /api/jobs/validate` (validate YAML, return DAG or errors), `GET /api/readers` (list built-in reader types), `GET /api/writers` (list built-in writer types). Used by DAG Builder in Sub-Project 4.

- [ ] **Step 1: Write DagBuilderResource**

```java
// DagBuilderResource.java
package com.batchforge.api;

import com.batchforge.api.dto.ErrorResponse;
import com.batchforge.engine.dag.DagValidator;
import com.batchforge.yaml.JobDefinitionParser;
import com.batchforge.yaml.JobValidationException;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class DagBuilderResource {

    @Inject JobDefinitionParser parser;
    @Inject DagValidator validator;

    @POST
    @Path("/jobs/validate")
    @Consumes(MediaType.TEXT_PLAIN)
    public Response validateYaml(String yamlBody) {
        try {
            var job = parser.parse(yamlBody);
            var errors = validator.validate(job);
            if (!errors.isEmpty()) {
                return Response.status(400)
                    .entity(Map.of("valid", false, "errors", errors)).build();
            }
            var order = validator.topologicalSort(job);
            return Response.ok(Map.of(
                "valid", true,
                "jobName", job.name(),
                "stepCount", job.steps().size(),
                "executionOrder", order
            )).build();
        } catch (JobValidationException e) {
            return Response.status(400)
                .entity(Map.of("valid", false, "errors", List.of(e.getMessage()))).build();
        }
    }

    @GET
    @Path("/readers")
    public List<Map<String, String>> listReaders() {
        return List.of(
            Map.of("type", "jdbc", "description", "Read from SQL database via JDBC query"),
            Map.of("type", "csv", "description", "Read from CSV files"),
            Map.of("type", "jsonlines", "description", "Read from JSON Lines files")
        );
    }

    @GET
    @Path("/writers")
    public List<Map<String, String>> listWriters() {
        return List.of(
            Map.of("type", "jdbc", "description", "Write to SQL database table via JDBC"),
            Map.of("type", "csv", "description", "Write to CSV files"),
            Map.of("type", "file", "description", "Write to flat files")
        );
    }
}
```

- [ ] **Step 2: Write tests**

```java
// DagBuilderResourceTest.java
package com.batchforge.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
class DagBuilderResourceTest {

    @Test
    void validateYaml_valid() {
        String yaml = """
            name: validation-test
            steps:
              step1:
                reader:
                  type: csv
                  properties:
                    path: /tmp/test.csv
                writer:
                  type: file
                  properties:
                    path: /tmp/out.csv
                chunk-size: 100
            """;
        given().contentType("text/plain").body(yaml)
            .when().post("/api/jobs/validate")
            .then().statusCode(200)
            .body("valid", is(true))
            .body("jobName", equalTo("validation-test"))
            .body("executionOrder[0]", equalTo("step1"));
    }

    @Test
    void validateYaml_invalid_cycle() {
        String yaml = """
            name: cycle-test
            steps:
              a:
                reader:
                  type: csv
                  properties:
                    path: /tmp/a.csv
                writer:
                  type: file
                  properties:
                    path: /tmp/a-out.csv
                chunk-size: 100
                depends-on: [b]
              b:
                reader:
                  type: csv
                  properties:
                    path: /tmp/b.csv
                writer:
                  type: file
                  properties:
                    path: /tmp/b-out.csv
                chunk-size: 100
                depends-on: [a]
            """;
        given().contentType("text/plain").body(yaml)
            .when().post("/api/jobs/validate")
            .then().statusCode(400)
            .body("valid", is(false));
    }

    @Test
    void listReaders_returns_builtin_types() {
        given().when().get("/api/readers")
            .then().statusCode(200)
            .body("$.size()", is(3))
            .body("[0].type", equalTo("jdbc"));
    }

    @Test
    void listWriters_returns_builtin_types() {
        given().when().get("/api/writers")
            .then().statusCode(200)
            .body("$.size()", is(3))
            .body("[0].type", equalTo("jdbc"));
    }
}
```

- [ ] **Step 3: Run tests**

Run: `mvn test -f pom.xml -Dtest=DagBuilderResourceTest`

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/batchforge/api/DagBuilderResource.java src/test/java/com/batchforge/api/DagBuilderResourceTest.java
git commit -m "feat(api): DAG builder helper endpoints (validate, list readers/writers)"
```

---

### Task 6: WebSocket Execution Event Stream

**Files:**
- Create: `src/main/java/com/batchforge/ws/ExecutionEventBus.java`
- Create: `src/main/java/com/batchforge/ws/ExecutionEvent.java`
- Create: `src/main/java/com/batchforge/ws/ExecutionWebSocket.java`
- Modify: `src/main/java/com/batchforge/engine/executor/StepExecutor.java` (fire events)
- Modify: `src/main/java/com/batchforge/engine/dag/DagScheduler.java` (fire events)
- Create: `src/test/java/com/batchforge/ws/ExecutionEventBusTest.java`

**Interfaces:**
- Consumes: Quarkus WebSockets Next API (`@WebSocket`, `WebSocketConnection`). Engine classes `StepExecutor` and `DagScheduler` fire events through `ExecutionEventBus`.
- Produces: `ExecutionEventBus` (CDI singleton, in-memory pub/sub). `ExecutionEvent` (sealed interface with event types). `ExecutionWebSocket` at path `/ws/executions/{id}` — clients subscribe by execution ID and receive JSON events. Event types: `STEP_STARTED`, `CHUNK_COMPLETED`, `ITEM_FAILED`, `STEP_COMPLETED`, `EXECUTION_COMPLETED`.

- [ ] **Step 1: Write ExecutionEvent sealed interface**

```java
// ExecutionEvent.java
package com.batchforge.ws;

import java.time.Instant;
import java.util.UUID;

public sealed interface ExecutionEvent {

    UUID executionId();
    Instant timestamp();

    record StepStarted(UUID executionId, String step, Instant timestamp) implements ExecutionEvent {}

    record ChunkCompleted(UUID executionId, String step, String partition,
                          long offset, double itemsPerSec, Instant timestamp) implements ExecutionEvent {}

    record ItemFailed(UUID executionId, String step, String itemId,
                      String error, int attempt, Instant timestamp) implements ExecutionEvent {}

    record StepCompleted(UUID executionId, String step, String duration,
                         long itemsProcessed, Instant timestamp) implements ExecutionEvent {}

    record ExecutionCompleted(UUID executionId, String duration,
                              long totalItems, Instant timestamp) implements ExecutionEvent {}
}
```

- [ ] **Step 2: Write ExecutionEventBus**

```java
// ExecutionEventBus.java
package com.batchforge.ws;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

@ApplicationScoped
public class ExecutionEventBus {

    private final Map<UUID, List<Consumer<ExecutionEvent>>> subscribers = new ConcurrentHashMap<>();

    public void subscribe(UUID executionId, Consumer<ExecutionEvent> listener) {
        subscribers.computeIfAbsent(executionId, _ -> new CopyOnWriteArrayList<>()).add(listener);
    }

    public void unsubscribe(UUID executionId, Consumer<ExecutionEvent> listener) {
        var listeners = subscribers.get(executionId);
        if (listeners != null) {
            listeners.remove(listener);
            if (listeners.isEmpty()) subscribers.remove(executionId);
        }
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
```

- [ ] **Step 3: Write ExecutionWebSocket**

```java
// ExecutionWebSocket.java
package com.batchforge.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.websockets.next.*;
import jakarta.inject.Inject;

import java.util.UUID;
import java.util.function.Consumer;

@WebSocket(path = "/ws/executions/{id}")
public class ExecutionWebSocket {

    @Inject ExecutionEventBus eventBus;
    @Inject ObjectMapper mapper;

    private Consumer<ExecutionEvent> listener;
    private UUID executionId;

    @OnOpen
    public void onOpen(WebSocketConnection connection) {
        executionId = UUID.fromString(connection.pathParam("id"));
        listener = event -> {
            try {
                String json = mapper.writeValueAsString(toWireFormat(event));
                connection.sendTextAndAwait(json);
            } catch (Exception ignored) {}
        };
        eventBus.subscribe(executionId, listener);
    }

    @OnClose
    public void onClose() {
        if (executionId != null && listener != null) {
            eventBus.unsubscribe(executionId, listener);
        }
    }

    private record WireEvent(String type, Object data) {}

    private WireEvent toWireFormat(ExecutionEvent event) {
        return switch (event) {
            case ExecutionEvent.StepStarted e ->
                new WireEvent("STEP_STARTED", java.util.Map.of("step", e.step(), "timestamp", e.timestamp().toString()));
            case ExecutionEvent.ChunkCompleted e ->
                new WireEvent("CHUNK_COMPLETED", java.util.Map.of("step", e.step(), "partition", e.partition(), "offset", e.offset(), "itemsPerSec", e.itemsPerSec()));
            case ExecutionEvent.ItemFailed e ->
                new WireEvent("ITEM_FAILED", java.util.Map.of("step", e.step(), "itemId", e.itemId(), "error", e.error(), "attempt", e.attempt()));
            case ExecutionEvent.StepCompleted e ->
                new WireEvent("STEP_COMPLETED", java.util.Map.of("step", e.step(), "duration", e.duration(), "itemsProcessed", e.itemsProcessed()));
            case ExecutionEvent.ExecutionCompleted e ->
                new WireEvent("EXECUTION_COMPLETED", java.util.Map.of("duration", e.duration(), "totalItems", e.totalItems()));
        };
    }
}
```

- [ ] **Step 4: Integrate event bus into DagScheduler and StepExecutor**

In `DagScheduler`, inject `ExecutionEventBus` and fire `StepStarted`, `StepCompleted`, and `ExecutionCompleted` events at the appropriate points.

In `StepExecutor`, inject `ExecutionEventBus` and fire `ChunkCompleted` after each chunk write and `ItemFailed` when an item goes to dead letter.

The injection uses `@Inject ExecutionEventBus eventBus;` in both classes. Fire events with null-safe check: `if (eventBus != null) eventBus.publish(...)` so existing tests (which don't inject the bus) still work.

- [ ] **Step 5: Write EventBus unit test**

```java
// ExecutionEventBusTest.java
package com.batchforge.ws;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

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

        java.util.function.Consumer<ExecutionEvent> listener = received::add;
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
        var execId1 = UUID.randomUUID();
        var execId2 = UUID.randomUUID();

        bus.subscribe(execId1, received::add);
        bus.publish(new ExecutionEvent.StepStarted(execId2, "step1", Instant.now()));

        assertThat(received).isEmpty();
    }
}
```

- [ ] **Step 6: Run tests**

Run: `mvn test -f pom.xml -Dtest=ExecutionEventBusTest`
Expected: 3 tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/batchforge/ws/ src/test/java/com/batchforge/ws/ src/main/java/com/batchforge/engine/executor/StepExecutor.java src/main/java/com/batchforge/engine/dag/DagScheduler.java
git commit -m "feat(ws): WebSocket execution event stream with event bus integration"
```

---

### Task 7: CORS Configuration + Quarkus Startup Hook + Integration Smoke Test

**Files:**
- Modify: `src/main/resources/application.properties` (CORS config)
- Create: `src/main/java/com/batchforge/engine/StartupListener.java`
- Create: `src/test/java/com/batchforge/api/SmokeTest.java`

**Interfaces:**
- Consumes: `BatchForgeEngine.start()` from engine. Quarkus `@Observes StartupEvent`.
- Produces: Quarkus starts the engine on boot (loads jobs, starts watcher). CORS enabled for dashboard dev server (`localhost:3000`). `SmokeTest` verifies all endpoints are reachable in one `@QuarkusTest`.

- [ ] **Step 1: Create StartupListener**

```java
// StartupListener.java
package com.batchforge.engine;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class StartupListener {

    @Inject BatchForgeEngine engine;

    void onStart(@Observes StartupEvent event) {
        engine.start();
    }
}
```

- [ ] **Step 2: Configure CORS in application.properties**

Append to `application.properties`:

```properties
# CORS for dashboard
quarkus.http.cors=true
quarkus.http.cors.origins=http://localhost:3000,http://localhost:5173
quarkus.http.cors.methods=GET,POST,PUT,DELETE,OPTIONS
quarkus.http.cors.headers=Content-Type,Authorization
```

- [ ] **Step 3: Write smoke test**

```java
// SmokeTest.java
package com.batchforge.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
class SmokeTest {

    @Test
    void all_endpoints_reachable() {
        given().when().get("/api/jobs").then().statusCode(200);
        given().when().get("/api/executions").then().statusCode(200);
        given().when().get("/api/dead-letter").then().statusCode(200);
        given().when().get("/api/readers").then().statusCode(200);
        given().when().get("/api/writers").then().statusCode(200);
    }

    @Test
    void cors_headers_present() {
        given().header("Origin", "http://localhost:3000")
            .when().get("/api/jobs")
            .then().statusCode(200)
            .header("Access-Control-Allow-Origin", "http://localhost:3000");
    }
}
```

- [ ] **Step 4: Run full test suite**

Run: `mvn test -f pom.xml` (excluding Docker-dependent tests)
Expected: All non-Docker tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/batchforge/engine/StartupListener.java src/main/resources/application.properties src/test/java/com/batchforge/api/SmokeTest.java
git commit -m "feat(api): CORS config, startup hook, and integration smoke test"
```
