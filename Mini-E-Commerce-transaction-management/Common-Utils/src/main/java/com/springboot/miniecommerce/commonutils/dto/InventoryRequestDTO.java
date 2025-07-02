package com.springboot.miniecommerce.commonutils.dto;

public class InventoryRequestDTO {

    private Long productId;
    private Integer quantity;

    public InventoryRequestDTO(Long productId, Integer quantity) {
        this.productId = productId;
        this.quantity = quantity;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "InventoryRequestDTO{" +
                "productId=" + productId +
                ", quantity=" + quantity +
                '}';
    }
}