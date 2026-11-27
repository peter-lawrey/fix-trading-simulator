package com.helesto.dao;

import java.util.List;
import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import com.helesto.dto.OrderDto;
import com.helesto.model.OrderEntity;
import javax.enterprise.event.Event;

@ApplicationScoped
public class OrderDao {

    @Inject
    EntityManager em;

    @Inject
    Event<OrderDto> committedOrders;

    public List<OrderEntity> listOrders() {

        TypedQuery<OrderEntity> query = em.createNamedQuery("Orders.findAll", OrderEntity.class);

        return query.getResultList();

    }

    public Optional<OrderEntity> readByClOrdID(int clOrdID) {
        return Optional.ofNullable(em.find(OrderEntity.class, clOrdID));
    }

    public void persistOrder(OrderEntity order) {
        em.persist(order);
        committedOrders.fire(new OrderDto(order));
    }

    public void updateOrder(OrderEntity order) {
        em.merge(order);
        committedOrders.fire(new OrderDto(order));
    }

}
