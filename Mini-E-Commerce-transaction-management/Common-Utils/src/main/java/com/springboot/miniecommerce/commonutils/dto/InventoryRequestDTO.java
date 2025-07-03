package com.springboot.miniecommerce.commonutils.dto;

public class InventoryRequestDTO {

    private Long orderId;
    private Long productId;
    private Integer requestedQuantity;

    public InventoryRequestDTO(Long orderId, Long productId, Integer requestedQuantity) {
        this.orderId = orderId;
        this.productId = productId;
        this.requestedQuantity = requestedQuantity;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getRequestedQuantity() {
        return requestedQuantity;
    }

    public void setRequestedQuantity(Integer requestedQuantity) {
        this.requestedQuantity = requestedQuantity;
    }

    @Override
    public String toString() {
        return "InventoryRequestDTO{" +
                "orderId=" + orderId +
                ", productId=" + productId +
                ", quantity=" + requestedQuantity +
                '}';
    }
}