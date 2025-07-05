package com.springboot.miniecommerce.commonutils.constant;

public enum ModificationReason {
    //For Order MS
    NEW_ORDER_CREATION("NEW_ORDER_CREATION"),
    USER_ORDER_CANCEL("USER_ORDER_CANCEL"),

    //For Inventory MS

    //For Payment MS
    PAYMENT_REFUND("PAYMENT_REFUND"), //for refund
    PAYMENT_SUCCESSFUL("PAYMENT_SUCCESSFUL"),

    //For Compensation
//    COMPENSATE_ORDER_FAILURE("COMPENSATE_ORDER_FAILURE"),
    COMPENSATE_INVENTORY_FAILURE("COMPENSATE_INVENTORY_FAILURE"),
    COMPENSATE_PAYMENT_FAILURE("COMPENSATE_PAYMENT_FAILURE");

    private final String text;

    ModificationReason(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
