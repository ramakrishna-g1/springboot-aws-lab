package com.spring.aws_s3.service;

import com.google.gson.Gson;
import com.spring.aws_s3.constant.UploadTrackerConstants;
import com.spring.aws_s3.dto.ResponseDTO;
import com.spring.aws_s3.dto.StreamFileDTO;
import com.spring.aws_s3.model.UploadStreamTracker;
import com.spring.aws_s3.repository.StreamFilesRepository;
import com.spring.aws_s3.util.UploadProcessListener;
import okhttp3.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.BlockingInputStreamAsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.Upload;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class UploadStreamV3Service {

    private final Logger logger = LoggerFactory.getLogger(UploadStreamV3Service.class);

    private final String runIdAPIPlaceHolder = "{runID}";
    private final String fileLimitPerRun = "{fileLimit}";

    @Value("${s3_bucket_name}")
    private String s3BucketName;

    @Value("${bearerToken}")
    private String bearerToken;

    @Value("${preSignedBaseURL}")
    private String preSignedBaseURL;

    @Value("${upload_executor_pool_size:10}")
    private int uploadExecutorPoolSize;

    @Value("${forkJoinPoolSize:10}")
    private int forkJoinPoolSize;

    @Value("${aws_access_key}")
    private String awsAccessKey;

    @Value("${aws_secret_key}")
    private String awsSecretKey;

    @Value("${aws_region}")
    private String aws_region;

    @Value("${maxConcurrency:50}")
    private int maxConcurrency;

    @Value("${targetThroughputInGbps:30.0}")
    private double targetThroughputInGbps;

    @Value("${minimumPartSizeInMB:50}")
    private long minimumPartSizeInMB;

    @Value("${maxRetryFoUploading:3}")
    private int maxRetryForUploading;

    @Value("${retryWaitTime:1000}")
    private int retryWaitTime;

    @Autowired
    private StreamFilesRepository streamFilesRepository;

    @Autowired
    private OkHttpClient okHttpClient;

    private RestTemplate restTemplate = new RestTemplate();

    /**
     * This method will get the list of FileMetaData (StreamFileDTO) from the base URL of API.
     *
     * @return List<StreamFileDTO> it contains the list of the File Metadata provided by API
     */
    public List<UploadStreamTracker> getFileMetadata(String runId) {

        List<StreamFileDTO> responseItems = Collections.emptyList();
        List<UploadStreamTracker> uploadStreamTrackerList = Collections.emptyList();

        logger.info("Invoking BSSH api to fetch file(s) metadata for run: {} to upload", runId);

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(bearerToken);
        HttpEntity<String> requestEntity = new HttpEntity<>(headers);

        ResponseEntity<String> result = restTemplate
                .exchange(preSignedBaseURL.replace(runIdAPIPlaceHolder, runId),
                        HttpMethod.GET, requestEntity, String.class);

        if (result != null && !StringUtils.isEmpty(result.getBody())) {
            Gson gson = new Gson();
            ResponseDTO responseDTO = gson.fromJson(result.getBody(), ResponseDTO.class);
            logger.info("Files Count for runID: {} is :{}", runId, responseDTO.getItems().size());
            responseItems = responseDTO.getItems();
            uploadStreamTrackerList = getStreamFileTrackerList(responseItems, UploadTrackerConstants.INPROGRESS.toString(), runId);
            uploadStreamTrackerList = streamFilesRepository.saveAllAndFlush(uploadStreamTrackerList);
        }

        return uploadStreamTrackerList;
    }

    /**
     * @param runIds
     * @return
     */
    public String uploadStreamToS3(String runIds) {

        List<String> runIdList = List.of(runIds.split(","));
        logger.info("list of runIds: {}", runIdList);
        logger.info("ThroughputInGbps= {}, maxConcurrency= {}, minimumPartSizeInBytes= {}",
                targetThroughputInGbps, maxConcurrency, minimumPartSizeInMB);
        try {
            ForkJoinPool forkJoinPool = new ForkJoinPool(forkJoinPoolSize);
            for (String runID : runIdList) {
                S3TransferManager transferManager = getTransferManager();
                long startTime = System.currentTimeMillis();

                CompletableFuture.runAsync(() -> {
                    try {
                        logger.info("Triggering upload for run:{}", runID);
                        List<UploadStreamTracker> fileTrackerList = getFileMetadata(runID.trim());

                        if (!CollectionUtils.isEmpty(fileTrackerList)) {
                            SimpleDateFormat sdf = new SimpleDateFormat("ddMMM_hh_mm_ss_SSSS");
                            String s3Key = runID + "_" + sdf.format(new Date());
                            AtomicInteger completedFiles = new AtomicInteger(0);
                            logger.info("Folder to copy files: V3_POC3/{}", s3Key);

                            logger.info("fileTracker size for run:{} is: {}", runID, fileTrackerList.size());

                            for (UploadStreamTracker fileTracker : fileTrackerList) {
                                int retriesDone = 0;
                                if (fileTracker.getSize() > 0) {
                                    initiateUploadToS3(fileTracker, transferManager, fileTrackerList, completedFiles, retriesDone, s3Key);
                                } else {
                                    uploadEmptyFileToS3(fileTracker, transferManager, s3Key, completedFiles, fileTrackerList);
                                }
                            }
                        } else {
                            logger.info("No files found to upload in runId:{}", runID);
                        }
                    } catch (Exception e) {
                        logger.error("Exception occurred while initiating the upload for:{}, exception: ", runID, e);
                    }
                }, forkJoinPool);

                logger.info("Upload initiated for run:{} in {} millis!", runID, (System.currentTimeMillis() - startTime));
            }
        } catch (Exception e) {
            logger.error("Exception occurred: {}", e.getMessage(), e);
            streamFilesRepository.updateStatusByStatusAndFileIdIn(UploadTrackerConstants.FAILED.toString(), UploadTrackerConstants.COMPLETED.toString(), runIdList);
        }

        return "Upload triggered for:" + runIds + " successfully";
    }

    private void uploadEmptyFileToS3(UploadStreamTracker fileTracker, S3TransferManager transferManager, String s3Key, AtomicInteger completedFiles, List<UploadStreamTracker> fileTrackerList) {
        Upload upload = transferManager.upload(builder -> builder
                .requestBody(AsyncRequestBody.empty())
                .addTransferListener(UploadProcessListener.create(fileTracker.getPath()))
                .putObjectRequest(req -> req
                        .bucket(s3BucketName)
                        .key("V3_POC3/" + s3Key + "/" + fileTracker.getPath())
                )
                .build());

        upload.completionFuture().whenCompleteAsync((result, exception) -> {
            if (ObjectUtils.isEmpty(exception)) {
                logger.info("Empty File with fileId:{} uploaded to S3", fileTracker.getFileId());
                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.COMPLETED.toString(), fileTracker.getId(), fileTracker.getFileId());
                completedFiles.getAndIncrement();
                logger.info("Completed files: {}, files left: {}", completedFiles.get(), (fileTrackerList.size() - completedFiles.get()));
            } else {
                logger.warn("Exception occurred while uploading empty file:{} exception: ", fileTracker.getPath(), exception);
                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
            }
        });
    }

    /**
     * @param fileTracker
     * @param transferManager
     * @param fileTrackerList
     * @param completedCount
     * @param retriesDone
     * @param s3Key
     */
    public void initiateUploadToS3(UploadStreamTracker fileTracker, S3TransferManager transferManager,
                                   List<UploadStreamTracker> fileTrackerList, AtomicInteger completedCount, int retriesDone,
                                   String s3Key) {
        Response response = null;

        try {
            if (!StringUtils.isEmpty(fileTracker.getHrefContent()) && !StringUtils.isEmpty(fileTracker.getPath())) {
                Request httpRequest = new Request.Builder()
                        .get()
                        .url(fileTracker.getHrefContent())
                        .build();
                logger.debug("Created HTTP request for: {}", fileTracker.getPath());

                BlockingInputStreamAsyncRequestBody body = AsyncRequestBody.forBlockingInputStream(fileTracker.getSize());

                Upload upload = transferManager.upload(builder -> builder
                        .requestBody(body)
                        .addTransferListener(UploadProcessListener.create(fileTracker.getPath()))
                        .putObjectRequest(req -> req
                                .bucket(s3BucketName)
                                .key("V3_POC3/" + s3Key + "/" + fileTracker.getPath())
                        )
                        .build());

                response = okHttpClient.newCall(httpRequest).execute();

                logger.debug("BSSH API response time for fileID:{} is:{} millis", fileTracker.getFileId(), (response.receivedResponseAtMillis() - response.sentRequestAtMillis()));
                if (!ObjectUtils.isEmpty(response) && response.code() == HttpStatus.OK.value()) {
                    if (!ObjectUtils.isEmpty(response.body())) {
                        body.writeInputStream(response.body().byteStream());
                        response.body().close();
                    }
                    response.close();
                    updateStatusAndRetries(fileTracker, transferManager, fileTrackerList, completedCount, retriesDone, s3Key, upload);
                } else {
                    if (!ObjectUtils.isEmpty(response) && !ObjectUtils.isEmpty(response.body())) {
                        response.body().close();
                    }
                    response.close();
                    streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
                    logger.info("Failed to retrieve data from the server for fileID:{}, status code: {}", fileTracker.getFileId(), response.code());
                }
            } else {
                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
                logger.info("Either HrefContent or Path is empty unable to process upload for: {}!", fileTracker.getFileId());
            }
        } catch (IllegalStateException ex) {
            retryStreamUpload(fileTracker, transferManager, fileTrackerList, completedCount, retriesDone, s3Key);
            if (!ObjectUtils.isEmpty(response) && !ObjectUtils.isEmpty(response.body())) {
                response.body().close();
                response.close();
            }
            logger.warn("Exception occurred while uploading file: {}, going for retry. Exception: {}", fileTracker.getPath(), ex.getMessage(), ex);
        } catch (Exception e) {
            logger.error("Exception occurred while uploading file: {}, so updating DB with failed status. Exception:{}", fileTracker.getPath(), e.getClass().getName(), e);
            streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
        } finally {
            if (!ObjectUtils.isEmpty(response)) {
                if (!ObjectUtils.isEmpty(response.body())) {
                    response.body().close();
                }
                response.close();
            }
        }
    }

    /**
     * @param fileTracker
     * @param transferManager
     * @param fileTrackerList
     * @param completedCount
     * @param retriesDone
     * @param s3Key
     * @param upload
     */
    private void updateStatusAndRetries(UploadStreamTracker fileTracker, S3TransferManager transferManager,
                                        List<UploadStreamTracker> fileTrackerList, AtomicInteger completedCount,
                                        int retriesDone, String s3Key, Upload upload) {

        upload.completionFuture().whenCompleteAsync((result, exception) -> {
            if (ObjectUtils.isEmpty(exception)) {
                logger.info("fileName: {} with fileID:{} ETag from upload response: {}, eTag from BSSH metadata: {}, FileSize:{} MB, ETag matches: {}",
                        fileTracker.getPath(), fileTracker.getFileId(),
                        result.response().eTag().replace("\"", ""), fileTracker.getETag().replace("\"", ""), (fileTracker.getSize() / (1024 * 1024)),
                        fileTracker.getETag().replace("\"", "").equals(result.response().eTag().replace("\"", "")));

                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.COMPLETED.toString(), fileTracker.getId(), fileTracker.getFileId());
                completedCount.getAndIncrement();
                logger.info("Completed files: {}, files left: {}", completedCount.get(), (fileTrackerList.size() - completedCount.get()));
            } else {
                logger.warn("Exception occurred while uploading file:{} exception: ", fileTracker.getPath(), exception);
                retryStreamUpload(fileTracker, transferManager, fileTrackerList, completedCount, retriesDone, s3Key);
            }
        });
    }

    /**
     * @param fileTracker
     * @param transferManager
     * @param fileTrackerList
     * @param completedCount
     * @param retriesDone
     * @param s3Key
     */
    private void retryStreamUpload(UploadStreamTracker fileTracker, S3TransferManager transferManager,
                                   List<UploadStreamTracker> fileTrackerList, AtomicInteger completedCount, int retriesDone, String s3Key) {
        try {
            int nextRetry = retriesDone + 1;
            Thread.sleep((long) retryWaitTime * nextRetry);

            if (retriesDone < maxRetryForUploading) {
                streamFilesRepository.updateRetriesDoneByIdAndFileId(nextRetry, fileTracker.getId(), fileTracker.getFileId());

                logger.info("Going to retry file upload:{}", fileTracker.getPath());
                initiateUploadToS3(fileTracker, transferManager, fileTrackerList, completedCount, nextRetry, s3Key);
            } else {
                logger.info("Upload failed, even after all retries for:{}", fileTracker.getPath());
                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
            }
        } catch (Exception e) {
            logger.error("Upload failed, while retrying for:{}, exception:", fileTracker.getPath(), e);
            streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
        }
    }

    /**
     * This method is to populate the UploadTracker List
     *
     * @param streamFiles List of Files that has to be uploaded to S3
     * @param runId
     * @return List<UploadStreamTracker>
     */
    public List<UploadStreamTracker> getStreamFileTrackerList(List<StreamFileDTO> streamFiles, String status, String runId) {

        List<UploadStreamTracker> uploadStreamTrackerList = new ArrayList<>(0);
        if (!CollectionUtils.isEmpty(streamFiles)) {
            streamFiles.forEach(file -> {
                UploadStreamTracker uploadStreamTracker = new UploadStreamTracker();
                uploadStreamTracker.setRunId(runId);
                uploadStreamTracker.setFileId(file.getId());
                uploadStreamTracker.setStatus(status);
                uploadStreamTracker.setRetriesDone(0);
                uploadStreamTracker.setContentType(file.getContentType());
                uploadStreamTracker.setHrefContent(file.getHrefContent());
                uploadStreamTracker.setSize(file.getSize());
                uploadStreamTracker.setETag(file.getETag());
                uploadStreamTracker.setPath(file.getPath());
                uploadStreamTrackerList.add(uploadStreamTracker);
            });
        }
        return uploadStreamTrackerList;
    }

    /**
     * @return S3TransferManager
     */
    public S3TransferManager getTransferManager() {
        //Setting thread Name
        Thread.currentThread().setName("UploadData_V3");

        return S3TransferManager.builder()
                .s3Client(getS3AsyncClient())
                .executor(createExecutorService(uploadExecutorPoolSize, Thread.currentThread().getName() + "_slave"))
                .build();
    }

    public S3AsyncClient getS3AsyncClient() {
        AwsCredentialsProvider awsCredentials = StaticCredentialsProvider
                .create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey));

        S3CrtHttpConfiguration s3CrtHttpConfiguration =
                S3CrtHttpConfiguration.builder()
                        .connectionTimeout(Duration.of(10, ChronoUnit.SECONDS))
                        .build();

        return S3AsyncClient.crtBuilder()
                .region(Region.of(aws_region))
                .credentialsProvider(awsCredentials)
                .minimumPartSizeInBytes(minimumPartSizeInMB * 1024 * 1024)
                .httpConfiguration(s3CrtHttpConfiguration)
                .targetThroughputInGbps(targetThroughputInGbps)
                .maxConcurrency(maxConcurrency)
                .build();
    }

    /**
     * @param threadNumber
     * @param slaveThreadName
     */
    private ThreadPoolExecutor createExecutorService(int threadNumber, String slaveThreadName) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private int threadCount = 1;

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(slaveThreadName + threadCount++);
                return thread;
            }
        };
        return (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNumber, threadFactory);
    }
}