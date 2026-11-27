package com.helesto.rest;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class BusinessFailure implements ExceptionMapper<com.helesto.journal.Main.Fault> {
  public Response toResponse(com.helesto.journal.Main.Fault error) {
    return Response.status(com.helesto.journal.Main.errorStatus(error))
        .entity(error.getMessage())
        .build();
  }
}
