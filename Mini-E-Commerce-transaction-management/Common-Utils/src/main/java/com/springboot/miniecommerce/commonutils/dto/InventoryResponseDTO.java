package com.springboot.miniecommerce.commonutils.dto;

public class InventoryResponseDTO {

    private Long productId;
    private Double amount;

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public InventoryResponseDTO(Long productId, Double amount) {
        this.productId = productId;
        this.amount = amount;
    }

    public InventoryResponseDTO() {
    }

    @Override
    public String toString() {
        return "InventoryResponseDTO{" +
                "productId=" + productId +
                ", amount=" + amount +
                '}';
    }
}