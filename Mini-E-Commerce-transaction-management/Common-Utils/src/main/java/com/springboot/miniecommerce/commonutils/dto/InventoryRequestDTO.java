package com.springboot.miniecommerce.commonutils.dto;

import com.springboot.miniecommerce.commonutils.constant.ModificationReason;

public class InventoryRequestDTO {

    private long orderId;
    private long productId;
    private int productQuantity;
    private ModificationReason modificationReason;

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public int getProductQuantity() {
        return productQuantity;
    }

    public void setProductQuantity(int productQuantity) {
        this.productQuantity = productQuantity;
    }

    public ModificationReason getModificationReason() {
        return modificationReason;
    }

    public void setModificationReason(ModificationReason modificationReason) {
        this.modificationReason = modificationReason;
    }

    public InventoryRequestDTO(long orderId, long productId, int productQuantity, ModificationReason modificationReason) {
        this.orderId = orderId;
        this.productId = productId;
        this.productQuantity = productQuantity;
        this.modificationReason = modificationReason;
    }

    @Override
    public String toString() {
        return "InventoryRequestDTO{" +
                "orderId=" + orderId +
                ", productId=" + productId +
                ", requestedQuantity=" + productQuantity +
                ", modificationReason=" + modificationReason +
                '}';
    }
}