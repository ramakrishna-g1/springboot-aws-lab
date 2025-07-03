package com.springboot.miniecommerce.commonutils.dto;

import com.springboot.miniecommerce.commonutils.constant.ModificationReason;
import com.springboot.miniecommerce.commonutils.constant.OrderStatus;

public class OrderUpdateRequestDTO {

    private Long orderId;
    private OrderStatus orderStatus;
    private ModificationReason modificationReason;

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

    public ModificationReason getModificationReason() {
        return modificationReason;
    }

    public void setModificationReason(ModificationReason modificationReason) {
        this.modificationReason = modificationReason;
    }

    public OrderUpdateRequestDTO(Long orderId, OrderStatus orderStatus, ModificationReason modificationReason) {
        this.orderId = orderId;
        this.orderStatus = orderStatus;
        this.modificationReason = modificationReason;
    }

    @Override
    public String toString() {
        return "OrderUpdateRequestDTO{" +
                "orderId=" + orderId +
                ", orderStatus=" + orderStatus +
                ", modificationReason=" + modificationReason +
                '}';
    }
}