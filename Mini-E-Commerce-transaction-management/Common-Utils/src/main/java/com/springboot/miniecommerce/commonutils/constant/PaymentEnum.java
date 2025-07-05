package com.springboot.miniecommerce.commonutils.constant;

public enum PaymentEnum {
    //The below Codes are for Payment
    PAYMENT_SERVICE_UNREACHABLE("PAYMENT_SERVICE_UNREACHABLE"),
    PAYMENT_RESPONSE_NULL("PAYMENT_RESPONSE_NULL"),
    ORDER_ID_NULL("ORDER_ID_NULL"),
    PAYMENT_AMOUNT_NULL("PAYMENT_AMOUNT_NULL"),;

    private final String text;

    PaymentEnum(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
