package com.springboot.miniecommerce.commonutils.dto;

public class PaymentResponseDTO {

    private long paymentId;
    private double totalAmount;

    public long getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(long paymentId) {
        this.paymentId = paymentId;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public PaymentResponseDTO() {
    }

    public PaymentResponseDTO(long paymentId, double totalAmount) {
        this.paymentId = paymentId;
        this.totalAmount = totalAmount;
    }

    @Override
    public String toString() {
        return "PaymentResponseDTO{" +
                "paymentId=" + paymentId +
                ", totalAmount=" + totalAmount +
                '}';
    }
}