package com.spring.aws_s3.constant;

public enum UploadTrackerConstants {

    PENDING("pending"),
    INPROGRESS("inprogress"),
    COMPLETED("completed"),
    FAILED("failed");

    private final String text;

    UploadTrackerConstants(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
