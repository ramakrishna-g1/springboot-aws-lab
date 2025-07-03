package com.springboot.miniecommerce.commonutils.constant;

public enum InventoryEnum {
    //The below codes are for Adding products into Inventory
    NO_PRODUCTS_SUPPLIED("NO_PRODUCTS_SUPPLIED"),
    PRODUCT_ADDED("PRODUCT_ADDED"),

    //The below codes are for Inventory update requests
    SUCCESS("SUCCESS"),
    FAILED("FAILED"),
    INVENTORY_RESPONSE_NULL("INVENTORY_RESPONSE_NULL"),
    PRODUCT_ID_NULL("PRODUCT_ID_NULL"),
    PRODUCT_QUANTITY_NULL("PRODUCT_QUANTITY_NULL"),
    PRODUCT_NOT_FOUND("PRODUCT_NOT_FOUND"),
    INSUFFICIENT_INVENTORY("INSUFFICIENT_INVENTORY");

    private final String text;

    InventoryEnum(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
