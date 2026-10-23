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
