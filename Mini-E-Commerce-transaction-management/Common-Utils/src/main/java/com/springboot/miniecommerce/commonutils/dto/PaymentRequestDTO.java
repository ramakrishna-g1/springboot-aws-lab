package com.springboot.miniecommerce.commonutils.dto;

public class PaymentRequestDTO {

    private Long orderId;
    private Double amount;

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public PaymentRequestDTO(Long orderId, Double amount) {
        this.orderId = orderId;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "PaymentRequestDTO{" +
                "orderId=" + orderId +
                ", amount=" + amount +
                '}';
    }
}