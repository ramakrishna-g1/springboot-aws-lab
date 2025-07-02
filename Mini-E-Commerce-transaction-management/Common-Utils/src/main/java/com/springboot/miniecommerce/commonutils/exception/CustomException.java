package com.springboot.miniecommerce.commonutils.exception;

public class CustomException extends RuntimeException {
    private final int httpStatusCode;
    private final String status;
    private final String message;

    public CustomException(int httpStatusCode, String status, String message) {
        super(message);
        this.httpStatusCode = httpStatusCode;
        this.status = status;
        this.message = message;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    public String getStatus() {
        return status;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
