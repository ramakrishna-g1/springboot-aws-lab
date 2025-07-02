package com.springboot.miniecommerce.commonutils.dto;

import com.springboot.miniecommerce.commonutils.constant.OrderStatus;

public class OrderUpdateRequestDTO {

    private Long orderId;
    private OrderStatus orderStatus;

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public OrderStatus getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(OrderStatus orderStatus) {
        this.orderStatus = orderStatus;
    }

    public OrderUpdateRequestDTO(Long orderId, OrderStatus orderStatus) {
        this.orderId = orderId;
        this.orderStatus = orderStatus;
    }

    @Override
    public String toString() {
        return "OrderUpdateRequestDTO{" +
                "orderId=" + orderId +
                ", orderStatus=" + orderStatus +
                '}';
    }
}