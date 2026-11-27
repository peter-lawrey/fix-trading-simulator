package com.helesto.service;

import com.helesto.dto.OrderDto;
import com.helesto.journal.Main;
import io.vertx.core.json.JsonObject;
import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class NewOrderSingleService {
  public OrderDto newOrderSingle(OrderDto request) throws Exception {
    JsonObject command =
        new JsonObject()
            .put("side", Character.toString(request.getSide()))
            .put("symbol", request.getSymbol())
            .put("orderQty", request.getOrderQty())
            .put("price", request.getPrice());
    JsonObject state = Main.order(command);
    OrderDto response = new OrderDto();
    response.setClOrdID(state.getInteger("clOrdID"));
    response.setSide(state.getString("side").charAt(0));
    response.setSymbol(state.getString("symbol"));
    response.setOrdStatus(state.getString("ordStatus").charAt(0));
    response.setPrice(state.getDouble("price"));
    response.setOrderQty(state.getDouble("orderQty"));
    response.setCumQty(state.getDouble("cumQty"));
    return response;
  }
}
