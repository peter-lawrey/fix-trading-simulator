package com.helesto.service;

import com.helesto.dto.*;
import com.helesto.journal.Main;
import io.vertx.core.json.JsonObject;
import javax.enterprise.context.RequestScoped;

@RequestScoped
public class OrderCancelRequestService {
  public OrderDto orderCancelRequest(int clOrdID) throws Exception {
    return Main.change(clOrdID, "DELETE", new JsonObject()).mapTo(OrderDto.class);
  }
}
