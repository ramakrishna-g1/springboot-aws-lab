package com.springboot.miniecommerce.commonutils.constant;

public enum PaymentStatus {
    SUCCESS("SUCCESS"),
    FAILED("FAILED");

    private final String text;

    PaymentStatus(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
