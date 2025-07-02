package com.springboot.miniecommerce.commonutils.dto;

public class PaymentResponseDTO {

    private Long paymentId;
    private Double totalAmount;
    private String status;
    private String message;

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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "PaymentResponseDTO{" +
                "paymentId=" + paymentId +
                ", totalAmount=" + totalAmount +
                ", status='" + status + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}