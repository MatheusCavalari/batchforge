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
                processor: com.batchforge.demo.TransactionNormalizer
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
