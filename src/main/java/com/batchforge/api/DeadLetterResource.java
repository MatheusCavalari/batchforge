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
