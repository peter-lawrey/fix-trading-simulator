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

  public OrderDto gerOrder(int id) {
    return Main.getOrder(id).mapTo(OrderDto.class);
  }
}
