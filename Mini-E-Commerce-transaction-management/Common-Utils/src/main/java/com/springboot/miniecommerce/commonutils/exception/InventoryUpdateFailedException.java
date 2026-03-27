package com.springboot.miniecommerce.commonutils.exception;

public class InventoryUpdateFailedException extends CustomException{

    public InventoryUpdateFailedException(int httpStatusCode, String status, String message) {
        super(httpStatusCode, status, message);
    }
}
