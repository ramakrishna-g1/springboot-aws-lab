package com.springboot.miniecommerce.commonutils.dto;

public class ErrorDTO {
    private int statusCode;
    private String errorCode;
    private String message;
    private String timeStamp;

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public ErrorDTO(int statusCode, String errorCode, String message, String timeStamp) {
        this.statusCode = statusCode;
        this.errorCode = errorCode;
        this.message = message;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "ErrorDTO{" +
                "statusCode=" + statusCode +
                ", errorCode='" + errorCode + '\'' +
                ", message='" + message + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                '}';
    }
}