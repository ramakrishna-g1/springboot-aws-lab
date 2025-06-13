package com.spring.aws_s3.dto;

import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

public class FileUploadDTO {

    private S3TransferManager transferManager;

    private UploadFileRequest fileRequest;

    private UploadDirectoryRequest directoryRequest;

    public S3TransferManager getTransferManager() {
        return transferManager;
    }

    public void setTransferManager(S3TransferManager transferManager) {
        this.transferManager = transferManager;
    }

    public UploadFileRequest getFileRequest() {
        return fileRequest;
    }

    public void setFileRequest(UploadFileRequest fileRequest) {
        this.fileRequest = fileRequest;
    }

    public UploadDirectoryRequest getDirectoryRequest() {
        return directoryRequest;
    }

    public void setDirectoryRequest(UploadDirectoryRequest directoryRequest) {
        this.directoryRequest = directoryRequest;
    }
}
