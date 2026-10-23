package com.batchforge.api;

import com.batchforge.engine.dag.DagValidator;
import com.batchforge.yaml.JobDefinitionParser;
import com.batchforge.yaml.JobValidationException;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.yaml.snakeyaml.error.YAMLException;

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
            // topologicalSort returns parallel levels; flatten into a single execution order
            var order = validator.topologicalSort(job).stream()
                .flatMap(List::stream)
                .toList();
            return Response.ok(Map.of(
                "valid", true,
                "jobName", job.name(),
                "stepCount", job.steps().size(),
                "executionOrder", order
            )).build();
        } catch (JobValidationException | YAMLException e) {
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
