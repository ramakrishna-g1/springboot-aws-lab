package com.springboot.miniecommerce.commonutils.dto;

import com.springboot.miniecommerce.commonutils.constant.ModificationReason;

public class OrderRequestDTO {
    private long productId;
    private int quantity;
    private ModificationReason modificationReason;

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public ModificationReason getModificationReason() {
        return modificationReason;
    }

    public void setModificationReason(ModificationReason modificationReason) {
        this.modificationReason = modificationReason;
    }

    public OrderRequestDTO(long productId, int quantity, ModificationReason modificationReason) {
        this.productId = productId;
        this.quantity = quantity;
        this.modificationReason = modificationReason;
    }

    @Override
    public String toString() {
        return "OrderRequestDTO{" +
                "productId=" + productId +
                ", quantity=" + quantity +
                ", modificationReason=" + modificationReason +
                '}';
    }
}
