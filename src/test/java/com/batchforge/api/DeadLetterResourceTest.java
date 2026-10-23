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
