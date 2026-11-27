package com.helesto.service;

import com.helesto.dto.OrderDto;
import com.helesto.journal.Main;
import javax.enterprise.context.RequestScoped;

@RequestScoped
public class OrderService {
  public OrderDto[] listOrders(Integer after, Integer before, String order, Integer limit) {
    return Main.orders(after, before, order, limit).stream()
        .map(o -> o.mapTo(OrderDto.class))
        .toArray(OrderDto[]::new);
  }

  public OrderDto getOrder(int id) {
    return Main.getOrder(id).mapTo(OrderDto.class);
  }

  public OrderDto cancelOrder(int orderID) throws Exception {
    return com.helesto.journal.Main.change(orderID, "DELETE", new io.vertx.core.json.JsonObject())
        .mapTo(OrderDto.class);
  }

  public OrderDto editOrder(int orderID, char ordStatus, double cumQty) throws Exception {
    var body =
        new io.vertx.core.json.JsonObject()
            .put("ordStatus", String.valueOf(ordStatus))
            .put("cumQty", cumQty);
    return com.helesto.journal.Main.change(orderID, "PATCH", body).mapTo(OrderDto.class);
  }
}
