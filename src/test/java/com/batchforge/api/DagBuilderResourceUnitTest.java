package com.batchforge.api;

import com.batchforge.engine.dag.DagValidator;
import com.batchforge.yaml.JobDefinitionParser;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Plain JUnit test (no Quarkus/Docker): wires the real parser and validator directly. */
class DagBuilderResourceUnitTest {

    private DagBuilderResource resource() {
        var r = new DagBuilderResource();
        r.parser = new JobDefinitionParser();
        r.validator = new DagValidator();
        return r;
    }

    @Test
    void validate_linear_chain_returns_flattened_execution_order() {
        String yaml = """
            name: chain
            steps:
              b:
                reader:
                  type: csv
                  properties:
                    path: /tmp/b.csv
                processor: com.batchforge.demo.TransactionNormalizer
                writer:
                  type: file
                  properties:
                    path: /tmp/b-out.csv
                chunk-size: 100
                depends-on: [a]
              a:
                reader:
                  type: csv
                  properties:
                    path: /tmp/a.csv
                processor: com.batchforge.demo.TransactionNormalizer
                writer:
                  type: file
                  properties:
                    path: /tmp/a-out.csv
                chunk-size: 100
            """;
        Response response = resource().validateYaml(yaml);
        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(true, body.get("valid"));
        assertEquals("chain", body.get("jobName"));
        assertEquals(2, body.get("stepCount"));
        assertEquals(List.of("a", "b"), body.get("executionOrder"));
    }

    @Test
    void validate_cycle_returns_400_with_errors() {
        String yaml = """
            name: cycle
            steps:
              a:
                reader:
                  type: csv
                  properties:
                    path: /tmp/a.csv
                processor: com.batchforge.demo.TransactionNormalizer
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
                processor: com.batchforge.demo.TransactionNormalizer
                writer:
                  type: file
                  properties:
                    path: /tmp/b-out.csv
                chunk-size: 100
                depends-on: [a]
            """;
        Response response = resource().validateYaml(yaml);
        assertEquals(400, response.getStatus());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getEntity();
        assertEquals(false, body.get("valid"));
        assertFalse(((List<?>) body.get("errors")).isEmpty());
    }

    @Test
    void validate_malformed_yaml_returns_400() {
        Response response = resource().validateYaml("not: [valid");
        assertEquals(400, response.getStatus());
    }
}
