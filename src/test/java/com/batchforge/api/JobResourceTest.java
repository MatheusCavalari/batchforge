package com.batchforge.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@QuarkusTest
class JobResourceTest {

    private static String yaml(String name) {
        return """
            name: %s
            steps:
              step1:
                reader:
                  type: csv
                  path: /tmp/test.csv
                processor: com.batchforge.engine.TestPassthroughProcessor
                writer:
                  type: file
                  path: /tmp/out.csv
                chunk-size: 100
            """.formatted(name);
    }

    @Test
    void listJobs_returns_array() {
        given().when().get("/api/jobs")
            .then().statusCode(200).body("$", instanceOf(java.util.List.class));
    }

    @Test
    void registerJob_then_get_then_delete() {
        given().contentType("text/plain").body(yaml("test-api-job"))
            .when().put("/api/jobs/test-api-job")
            .then().statusCode(200)
            .body("name", equalTo("test-api-job"))
            .body("stepCount", equalTo(1));

        given().when().get("/api/jobs/test-api-job")
            .then().statusCode(200)
            .body("name", equalTo("test-api-job"))
            .body("steps[0].readerType", equalTo("csv"));

        given().when().delete("/api/jobs/test-api-job").then().statusCode(204);
        given().when().get("/api/jobs/test-api-job").then().statusCode(404);
    }

    @Test
    void registerJob_name_mismatch_returns_400() {
        given().contentType("text/plain").body(yaml("other-name"))
            .when().put("/api/jobs/different")
            .then().statusCode(400);
    }

    @Test
    void registerJob_invalid_yaml_returns_400() {
        given().contentType("text/plain").body("name: x\n")
            .when().put("/api/jobs/x")
            .then().statusCode(400);
    }

    @Test
    void getJob_not_found_returns_404() {
        given().when().get("/api/jobs/nonexistent")
            .then().statusCode(404);
    }

    @Test
    void deleteJob_not_found_returns_404() {
        given().when().delete("/api/jobs/nonexistent")
            .then().statusCode(404);
    }
}
