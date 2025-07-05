package com.springboot.miniecommerce.commonutils.dto;

public class PaymentRequestDTO {

    private long orderId;
    private double amount;

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public PaymentRequestDTO(long orderId, double amount) {
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