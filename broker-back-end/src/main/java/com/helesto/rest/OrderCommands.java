package com.helesto.rest;

import com.helesto.dto.OrderDto;
import com.helesto.journal.Main;
import com.helesto.service.NewOrderSingleService;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.websocket.CloseReason;
import javax.websocket.OnMessage;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One command and one private admission response, in message order on each connection. */
@ServerEndpoint("/socket/order-commands")
@ApplicationScoped
public class OrderCommands {
  private static final Logger LOG = LoggerFactory.getLogger(OrderCommands.class);

  @Inject Jsonb json;
  @Inject NewOrderSingleService orders;

  @OnMessage(maxMessageSize = 65536)
  public String submit(String body, Session session) throws IOException {
    OrderDto order;
    try {
      order = SmallOrderReader.decodeCommand(json, body);
    } catch (javax.json.JsonException | javax.json.bind.JsonbException invalid) {
      return invalidOrder();
    }
    if (order == null || order.getSymbol() == null) return invalidOrder();

    try {
      return OrdersRest.CREATED_ORDER.writeValueAsString(orders.newOrderSingle(order));
    } catch (Main.Fault rejected) {
      return new JsonObject()
          .put("status", Main.errorStatus(rejected))
          .put("error", rejected.getMessage())
          .encode();
    } catch (Exception failure) {
      LOG.error("Order admission outcome is unknown", failure);
      session.close(
          new CloseReason(
              CloseReason.CloseCodes.UNEXPECTED_CONDITION, "Order admission outcome is unknown"));
      return null;
    }
  }

  private static String invalidOrder() {
    return "{\"status\":400,\"error\":\"Invalid order JSON\"}";
  }
}
