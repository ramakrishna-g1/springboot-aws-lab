package com.springboot.miniecommerce.commonutils.dto;

import com.springboot.miniecommerce.commonutils.constant.ModificationReason;
import com.springboot.miniecommerce.commonutils.constant.OrderEnum;

public class OrderUpdateRequestDTO {

    private long orderId;
    private OrderEnum orderEnum;
    private ModificationReason modificationReason;

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public OrderEnum getOrderStatus() {
        return orderEnum;
    }

    public void setOrderStatus(OrderEnum orderEnum) {
        this.orderEnum = orderEnum;
    }

    public ModificationReason getModificationReason() {
        return modificationReason;
    }

    public void setModificationReason(ModificationReason modificationReason) {
        this.modificationReason = modificationReason;
    }

    public OrderUpdateRequestDTO(long orderId, OrderEnum orderEnum, ModificationReason modificationReason) {
        this.orderId = orderId;
        this.orderEnum = orderEnum;
        this.modificationReason = modificationReason;
    }

    @Override
    public String toString() {
        return "OrderUpdateRequestDTO{" +
                "orderId=" + orderId +
                ", orderEnum=" + orderEnum +
                ", modificationReason=" + modificationReason +
                '}';
    }
}