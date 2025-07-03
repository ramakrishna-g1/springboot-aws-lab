package com.springboot.miniecommerce.commonutils.constant;

public enum PaymentEnum {
    //The below Codes are for Payment
    SUCCESS("SUCCESS"),
    FAILED("FAILED"),
    PAYMENT_RESPONSE_NULL("PAYMENT_RESPONSE_NULL");

    private final String text;

    PaymentEnum(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
