package com.springboot.miniecommerce.commonutils.dto;

public class PaymentResponseDTO {

    private Long paymentId;
    private Double totalAmount;

    public Long getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(Long paymentId) {
        this.paymentId = paymentId;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public PaymentResponseDTO() {
    }

    public PaymentResponseDTO(Long paymentId, Double totalAmount) {
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