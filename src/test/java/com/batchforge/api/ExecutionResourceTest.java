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
