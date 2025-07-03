package com.springboot.miniecommerce.commonutils.constant;

public enum ModificationReason {
    //For Order MS
    ORDER_CREATION("ORDER_CREATION"),
    USER_ORDER_CANCEL("USER_ORDER_CANCEL"),

    //For Inventory MS
    INVENTORY_UPDATE("INVENTORY_UPDATE"),

    //For Payment MS
    MAKE_PAYMENT("MAKE_PAYMENT"),
    PAYMENT_UPDATE("PAYMENT_UPDATE"), //for refund
    PAYMENT_SUCCESSFUL("PAYMENT_SUCCESSFUL"),

    //For Compensation
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
