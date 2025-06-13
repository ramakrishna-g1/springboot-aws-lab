package com.spring.aws_s3.util;

import com.spring.aws_s3.constant.UploadTrackerConstants;
import com.spring.aws_s3.model.UploadStreamTracker;
import com.spring.aws_s3.repository.StreamFilesRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.BlockingInputStreamAsyncRequestBody;
import software.amazon.awssdk.http.Header;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class S3MultipartUploadUtil {

    private final Logger logger = LoggerFactory.getLogger(S3MultipartUploadUtil.class);

    private static final int AWAIT_TIME = 2; // in seconds
    private static final int DEFAULT_THREAD_COUNT = 4;

    private final String s3BucketName;


    private final String filename;

    private final ThreadPoolExecutor executorService;

//    private final S3Client s3Client

    private final S3AsyncClient s3AsyncClient;

    private String uploadId;

    private final StreamFilesRepository streamFilesRepository;

    private final AtomicInteger uploadPartId = new AtomicInteger(0);

    private final List<Future<String>> futuresPartETags = new ArrayList<>();
    List<CompletedPart> completedParts = new ArrayList<>();

    public S3MultipartUploadUtil(String s3BucketName, String filename, S3AsyncClient s3AsyncClient, StreamFilesRepository streamFilesRepository) {
        this.executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(DEFAULT_THREAD_COUNT);
        this.s3BucketName = s3BucketName;
        this.filename = filename;
//        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
        this.streamFilesRepository = streamFilesRepository;

    }


    public void initializeUpload(UploadStreamTracker uploadStreamTracker) throws ExecutionException, InterruptedException {

        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest
                .builder()
                .bucket(s3BucketName)
                .key(filename)
                .metadata(getObjectMetadata(uploadStreamTracker))
                .build();

//        CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest);
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadResponse = s3AsyncClient.createMultipartUpload(createMultipartUploadRequest);

        uploadId = createMultipartUploadResponse.get().uploadId();
    }

    public void uploadPartAsync(ByteArrayInputStream inputStream) {
        submitTaskForUploading(inputStream);
    }

    public void uploadFinalPartAsync(ByteArrayInputStream inputStream, String finalCalculatedEtag, UploadStreamTracker fileTracker) {
        try {
            submitTaskForUploading(inputStream);

            logger.info("Waiting for all the partEtags to arrive");
            List<String> partETags = new ArrayList<>();
            for (Future<String> partETagFuture : futuresPartETags) {
                partETags.add(partETagFuture.get());
            }

            logger.debug("eTag of all the parts is " + Arrays.toString(partETags.toArray()));

            // sort completedParts to resolve error, The list of part is not in ascending order
            logger.info("Sorting completed parts");
            completedParts.sort(Comparator.comparingInt(CompletedPart::partNumber));
            logger.info("Sorted completed parts");

            logger.info(completedParts.toString());

            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload
                    .builder()
                    .parts(completedParts)
                    .build();

            logger.info("going to completeRequest");

            CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest
                    .builder()
                    .bucket(s3BucketName)
                    .key(filename)
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build();

//            CompleteMultipartUploadResponse completeMultipartUploadResponse = s3Client.completeMultipartUpload(completeRequest);

            CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUploadResponse = s3AsyncClient.completeMultipartUpload(completeRequest);


            boolean isChecksumMatched = completeMultipartUploadResponse.get().eTag().replace("\"", "").equals(finalCalculatedEtag);

            logger.info("completedUploadResponse : " + completeMultipartUploadResponse.get().eTag());
            logger.info("fileName: {} with fileID:{} ETag from upload response: {}, Calculated ETag : {}, ETag matches: {}",
                    fileTracker.getPath(), fileTracker.getFileId(), completeMultipartUploadResponse.get().eTag().replace("\"", ""),
                    finalCalculatedEtag, isChecksumMatched);

            if (isChecksumMatched){
                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.COMPLETED.toString(), fileTracker.getId(), fileTracker.getFileId());
                logger.info("Checksum matched hence marking the file as complete");
            }
            else {
                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
                logger.info("Checksum did not match hence marking the file as failed");
            }

        } catch (ExecutionException | InterruptedException e) {
            streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
            logger.error("Error occurred while uploading Final part", e);
//            throw new RuntimeException(e);
        } finally {
            this.shutdownAndAwaitTermination();
        }

    }

    private void shutdownAndAwaitTermination() {
        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(AWAIT_TIME, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.debug("Interrupted while awaiting ThreadPoolExecutor to shutdown");
        }
        this.executorService.shutdownNow();
        logger.info("executor shutdown complete");
    }

    private void submitTaskForUploading(ByteArrayInputStream inputStream) {
        if (uploadId == null || uploadId.isEmpty()) {
            throw new IllegalStateException("Upload Id cannot be null or empty");
        }

        if (s3BucketName == null || s3BucketName.isEmpty()) {
            throw new IllegalStateException("Bucket name cannot be empty");
        }

        if (filename == null || filename.isEmpty()) {
            throw new IllegalStateException("Uploading file name has not been set.");
        }

        submitTaskToExecutorService(() -> {
            int eachPartId = uploadPartId.incrementAndGet();
            UploadPartRequest uploadRequest = UploadPartRequest.builder()
                    .bucket(s3BucketName)
                    .key(filename)
                    .uploadId(uploadId)
                    .partNumber(eachPartId)
                    .contentLength((long) inputStream.available())
//                    .contentMD5("")
                    .build();

            logger.info("Submitting uploadPartId: {} of partSize: {}", eachPartId, inputStream.available());

//            UploadPartResponse uploadPartResponse = s3AsyncClient.uploadPart(uploadRequest, RequestBody.fromInputStream(inputStream, inputStream.available()));

            BlockingInputStreamAsyncRequestBody asyncRequestBody = AsyncRequestBody.forBlockingInputStream((long) inputStream.available());
            CompletableFuture<UploadPartResponse> uploadPartResponse = s3AsyncClient.uploadPart(uploadRequest, asyncRequestBody);
            asyncRequestBody.writeInputStream(inputStream);

            completedParts.add(CompletedPart
                    .builder()
                    .partNumber(eachPartId)
                    .eTag(uploadPartResponse.get().eTag())
                    .build());

            logger.info("Successfully submitted uploadPartId: {}", eachPartId);
            return uploadPartResponse.get().eTag();
        });
    }

    private void submitTaskToExecutorService(Callable<String> callable) {
        Future<String> partETagFuture = this.executorService.submit(callable);
        this.futuresPartETags.add(partETagFuture);
    }

    private Map<String, String> getObjectMetadata(UploadStreamTracker uploadStreamTracker) {
        Map<String, String> map = new HashMap<>();
        map.put(Header.CONTENT_TYPE, uploadStreamTracker.getContentType());
        return map;
    }
}