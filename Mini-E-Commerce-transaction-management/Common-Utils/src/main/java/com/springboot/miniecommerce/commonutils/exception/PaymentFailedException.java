package com.springboot.miniecommerce.commonutils.exception;

public class PaymentFailedException extends CustomException{

    public PaymentFailedException(int httpStatusCode, String status, String message) {
        super(httpStatusCode, status, message);
    }
}
