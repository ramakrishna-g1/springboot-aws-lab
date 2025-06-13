package com.spring.aws_s3.util;

import software.amazon.awssdk.transfer.s3.progress.TransferListener;
import software.amazon.awssdk.utils.Logger;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link TransferListener} that logs file name and progress initiation and completion at the {@code INFO} level.
 */
public class UploadProcessListener implements TransferListener {
    private static final Logger log = Logger.loggerFor(UploadProcessListener.class);
    private final String fileName;
    private long startTime;

    private UploadProcessListener(String fileName) {
        this.fileName = fileName;
    }

    public static UploadProcessListener create(String fileName) {
        return new UploadProcessListener(fileName);
    }

    @Override
    public void transferInitiated(Context.TransferInitiated context) {
        startTime = System.currentTimeMillis();
        log.info(() -> "Upload initiated for file: " + fileName + " at: " + new Date());
    }

    @Override
    public void transferComplete(Context.TransferComplete context) {
        log.info(() -> "Upload completed for " + fileName + " in: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime) + " seconds");
    }

    @Override
    public void transferFailed(Context.TransferFailed context) {
        log.warn(() -> "Upload failed for " + fileName + " in: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime) + " seconds", context.exception());
    }

}
 