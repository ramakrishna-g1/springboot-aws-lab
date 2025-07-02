package com.springboot.miniecommerce.commonutils.constant;

public enum OrderStatus {
    INITIATED("INITIATED"),
    CONFIRMED("CONFIRMED"),
    FAILED("FAILED"),
    CANCELLED("CANCELLED"),
    UPDATED("UPDATED");

    private final String text;

    OrderStatus(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
