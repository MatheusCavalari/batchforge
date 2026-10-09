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
