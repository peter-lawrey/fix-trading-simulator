package com.helesto.rest;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.QueryParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.helesto.dto.OrderDto;
import com.helesto.service.NewOrderSingleService;
import com.helesto.service.OrderCancelRequestService;
import com.helesto.service.OrderService;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




@Path("/orders")
@Tag(name = "Orders")
@RequestScoped
public class OrdersRest {

        private static final Logger LOG = LoggerFactory.getLogger(OrdersRest.class.getName());

        static final ObjectWriter CREATED_ORDER = new ObjectMapper()
                        .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                        .writerFor(OrderDto.class);
        private static final MediaType JSON_UTF8 = MediaType.valueOf("application/json;charset=utf-8");

        @Inject
        NewOrderSingleService newOrderSingleService;

        @Inject
        OrderCancelRequestService orderCancelRequestService;

        @Inject
        OrderService orderService;

        @POST
        @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
        @Operation(summary = "Create a new order")
        @APIResponse(responseCode = "200", description = "Order created", content = {
                        @Content(mediaType = "application/json", schema = @Schema(implementation = OrderDto.class)) })
        public Response create(OrderDto request) throws Exception {

                OrderDto response = newOrderSingleService.newOrderSingle(request);

                return Response.ok(CREATED_ORDER.writeValueAsBytes(response), JSON_UTF8).build();
        }

        @GET
        @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
        @Operation(summary = "List Orders", description = "List all Orders")
        @APIResponse(responseCode = "200", description = "Orders listed", content = {
                        @Content(mediaType = "application/json", schema = @Schema(implementation = OrderDto[].class)) })
        public Response list(@QueryParam("after") Integer after, @QueryParam("before") Integer before,
                        @QueryParam("order") String order, @QueryParam("limit") Integer limit) throws Exception {

                LOG.debug("OrdersListRest + GET - begin");

                OrderDto[] response = orderService.listOrders(after, before, order, limit);

                return Response.status(Response.Status.OK).entity(response).build();

        }

        @Path("{clOrdID}")
        @GET
        @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
        @Operation(summary = "Get Order by ClOrdID")
        @APIResponse(responseCode = "200", description = "Order", content = {
                        @Content(mediaType = "application/json", schema = @Schema(implementation = OrderDto.class)) })
        public Response getByID(
                        @Parameter(description = "The order id to READ.", required = true) @PathParam("clOrdID") int clOrdID)
                        throws Exception {

                LOG.debug("OrderGetRest + GET BY ID - begin - clOrdID=[" + clOrdID + "]");

                OrderDto response = orderService.gerOrder(clOrdID);

                return Response.status(Response.Status.OK).entity(response).build();

        }

        @Path("{clOrdID}")
        @DELETE
        @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
        @Operation(summary = "Cancel an order")
        @APIResponse(responseCode = "200", description = "Order cancelled", content = {
                        @Content(mediaType = "application/json", schema = @Schema(implementation = OrderDto.class)) })
        public Response cancel(
                        @Parameter(description = "The order id to cancel.", required = true) @PathParam("clOrdID") int clOrdID)
                        throws Exception {

                LOG.debug("Orders + DELETE - clOrdID=[" + clOrdID + "]");

                OrderDto response = orderCancelRequestService.orderCancelRequest(clOrdID);

                return Response.status(Response.Status.OK).entity(response).build();
        }

}
