package com.springboot.miniecommerce.commonutils.dto;

public class InventoryResponseDTO {

    private long productId;
    private double amount;

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public InventoryResponseDTO(long productId, double amount) {
        this.productId = productId;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "InventoryResponseDTO{" +
                "productId=" + productId +
                ", amount=" + amount +
                '}';
    }
}