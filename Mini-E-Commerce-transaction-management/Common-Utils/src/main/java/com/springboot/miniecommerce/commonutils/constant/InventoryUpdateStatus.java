package com.springboot.miniecommerce.commonutils.constant;

public enum InventoryUpdateStatus {
    SUCCESS("SUCCESS"),
    FAILED("FAILED");

    private final String text;

    InventoryUpdateStatus(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
