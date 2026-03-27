package com.springboot.miniecommerce.commonutils.constant;

public enum OrderEnum {
    //Below are statuses of Order
    INITIATED("INITIATED"),
    CONFIRMED("CONFIRMED"),
    FAILED("FAILED"),
    CANCELLED("CANCELLED"),

    //Below are API response codes
    UPDATED("UPDATED"),
    PRODUCT_ID_NULL("PRODUCT_ID_NULL"),
    INVALID_QUANTITY("INVALID_QUANTITY"),;

    private final String text;

    OrderEnum(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
