package com.spring.aws_s3.service;

import com.google.gson.Gson;
import com.spring.aws_s3.constant.UploadTrackerConstants;
import com.spring.aws_s3.dto.ResponseDTO;
import com.spring.aws_s3.dto.StreamFileDTO;
import com.spring.aws_s3.model.UploadStreamTracker;
import com.spring.aws_s3.repository.StreamFilesRepository;
import com.spring.aws_s3.util.UploadProcessListener;
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

import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Upload Stream to S3 using {@link S3TransferManager} and {@link S3AsyncClient}.
 * Custom {@link ThreadPoolExecutor} is used to upload configured number of streams in parallel.
 * <br>
 * @implNote Upload status is tracked in DB and configured number of retries are done for failed uploads.
 */
@Service
public class UploadStreamV2Service {

    private final Logger logger = LoggerFactory.getLogger(UploadStreamV2Service.class);

    private final String runIdAPIPlaceHolder = "{runID}";

    @Value("${s3_bucket_name}")
    private String s3BucketName;

    @Value("${bearerToken}")
    private String bearerToken;

    @Value("${baseURL}")
    private String baseURL;

    @Value("${upload_executor_pool_size:10}")
    private int executorThreadSize;

    @Value("${aws_access_key}")
    private String awsAccessKey;

    @Value("${aws_secret_key}")
    private String awsSecretKey;

    @Value("${aws_region}")
    private String aws_region;

    @Value("${corePoolSize}")
    private int corePoolSize;

    @Value("${maximumPoolSize}")
    private int maximumPoolSize;

    @Value("${targetThroughputInGbps}")
    private double targetThroughputInGbps;

    @Value("${maxConcurrency}")
    private int maxConcurrency;

    @Value("${minimumPartSizeInMB:10}")
    private long minimumPartSizeInMB;

    @Value("${maxRetryFoUploading:3}")
    private int maxRetryForUploading;

    @Value("${retryWaitTime:5000}")
    private int retryWaitTime;

    @Autowired
    StreamFilesRepository streamFilesRepository;

    private RestTemplate restTemplate = new RestTemplate();

    /**
     * This method will upload the stream of the file to the S3
     *
     * @return String
     */
    public String uploadStreamV2(String runIds) {

        List<String> runIdList = List.of(runIds.split(","));
        logger.info("list of runIds: {}", runIdList);

        try (S3TransferManager transferManager = getTransferManager()) {
            for (String runID : runIdList) {
                logger.info("Triggering upload for run:{}", runID);
                List<UploadStreamTracker> fileTrackerList = getFileMetadata(runID);

                if (!CollectionUtils.isEmpty(fileTrackerList)) {
                    long startTime = System.currentTimeMillis();

                    AtomicInteger completedFiles = new AtomicInteger(0);
                    logger.info("fileTracker size for run:{} is: {}", runID, fileTrackerList.size());

                    BlockingQueue<Runnable> workingQueue = new LinkedBlockingQueue<>();
                    ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
                            1000L, TimeUnit.MILLISECONDS, workingQueue);

                    //FOR testing multiple runs
                    SimpleDateFormat sdf = new SimpleDateFormat("ddMMM_hh_mm_ss_SSSS");
                    String s3Key = runID + "_" + sdf.format(new Date());
                    logger.info("Folder to copy files: V2_POC3/{}", s3Key);

                    for (UploadStreamTracker fileTracker : fileTrackerList) {
                        CompletableFuture.runAsync(() -> {
                            int retriesDone = 0;
                            initiateUploadToS3(fileTracker, transferManager, fileTrackerList, completedFiles, retriesDone, s3Key);
                        }, executor);
                    }

                    logger.info("Upload initiated for run:{} in {} millis!", runID, (System.currentTimeMillis() - startTime));
                } else {
                    logger.info("No files found to upload in runId:{}", runID);
                }
            }
        } catch (Exception e) {
            logger.error("Exception occurred: {}", e.getMessage(), e);
            streamFilesRepository.updateStatusByStatusAndFileIdLike(UploadTrackerConstants.FAILED.toString(), UploadTrackerConstants.COMPLETED.toString(), runIds);
        }

        return "Upload triggered for:" + runIdList + "  successfully";
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
                                   List<UploadStreamTracker> fileTrackerList, AtomicInteger completedCount, int retriesDone, String s3Key) {
//        HttpURLConnection urlConnection = null;

        try {
            if (!StringUtils.isEmpty(fileTracker.getHrefContent()) && !StringUtils.isEmpty(fileTracker.getPath())) {
                URL targetURL = new URL(fileTracker.getHrefContent());
                HttpURLConnection urlConnection = (HttpURLConnection) targetURL.openConnection();
                urlConnection.setRequestMethod(HttpMethod.GET.toString());
                urlConnection.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken);
                urlConnection.setRequestProperty(HttpHeaders.ACCEPT, MediaType.ALL_VALUE);
                logger.debug("Created HTTP request for: {}", fileTracker.getPath());

                if (urlConnection.getResponseCode() == HttpStatus.OK.value()) {
                    BlockingInputStreamAsyncRequestBody body = AsyncRequestBody.forBlockingInputStream(null);

                    Upload upload = transferManager.upload(builder -> builder
                            .requestBody(body)
                            .addTransferListener(UploadProcessListener.create(fileTracker.getPath()))
                            .putObjectRequest(req -> req.bucket(s3BucketName).key("V2_POC3/" + s3Key + "/" + fileTracker.getPath()))
                            .build());

                    body.writeInputStream(urlConnection.getInputStream());

                    urlConnection.getInputStream().close();
                    urlConnection.disconnect();

                    upload.completionFuture().whenCompleteAsync((result, exception) -> {
                        if (ObjectUtils.isEmpty(exception)) {
                            logger.info("eTag for file: {} with fileID:{} from result: {}, eTag from BSSH metadata: {}, matching with the uploaded data: {}",
                                    fileTracker.getPath(), fileTracker.getFileId(),
                                    result.response().eTag().replace("\"", ""), fileTracker.getETag(),
                                    fileTracker.getETag().equals(result.response().eTag().replace("\"", "")));

                            completedCount.getAndIncrement();
                            logger.info("Completed files: {}, files left: {}", completedCount.get(), (fileTrackerList.size() - completedCount.get()));
                            if (completedCount.get() == fileTrackerList.size()) {
                                transferManager.close();
                            }
                        } else {
                            logger.warn("Exception occurred while uploading file:{}, exception: ", fileTracker.getPath(), exception);
                            retryStreamUpload(fileTracker, transferManager, fileTrackerList, completedCount, retriesDone, s3Key);
                        }
                    });
                } else {
                    streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
                    logger.info("Failed to retrieve data from the server status code: {}", urlConnection.getResponseCode());
                }
            } else {
                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
                logger.info("Either HrefContent or Path is empty unable to process upload for: {}!", fileTracker.getPath());
            }
        } catch (IllegalStateException ex) {
            logger.warn("Exception occurred while uploading file: {}, going for retry. Exception: {}", fileTracker.getPath(), ex.getMessage(), ex);
            retryStreamUpload(fileTracker, transferManager, fileTrackerList, completedCount, retriesDone, s3Key);
        } catch (Exception e) {
            logger.error("Exception occurred while uploading file: {}, updating DB with failed status. Exception:{}", fileTracker.getPath(), e.getClass().getName(), e);
            streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
        }
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
            logger.error("Upload failed, while retrying file: {}, exception:", fileTracker.getPath(), e);
            streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
        }
    }

    /**
     * This method is to populate the UploadTracker List
     *
     * @param streamFiles List of Files that has to be uploaded to S3
     * @return List<UploadStreamTracker>
     */
    public List<UploadStreamTracker> getStreamFileTrackerList(List<StreamFileDTO> streamFiles, String status) {

        List<UploadStreamTracker> uploadStreamTrackerList = new ArrayList<>(0);
        if (!CollectionUtils.isEmpty(streamFiles)) {
            streamFiles.forEach(file -> {
                UploadStreamTracker uploadStreamTracker = new UploadStreamTracker();
                uploadStreamTracker.setFileId(file.getId());
                uploadStreamTracker.setStatus(status);
                uploadStreamTracker.setRetriesDone(0);
                uploadStreamTracker.setContentType(file.getContentType());
                uploadStreamTracker.setHrefContent(file.getHrefContent());
                uploadStreamTracker.setETag(file.getETag());
                uploadStreamTracker.setSize(file.getSize());
                uploadStreamTracker.setPath(file.getPath());
                uploadStreamTrackerList.add(uploadStreamTracker);
            });
        }
        return uploadStreamTrackerList;
    }


    /**
     * This method will get the list of FileMetaData (StreamFileDTO) from the base URL of BSSH API.
     *
     * @return List<StreamFileDTO> it contains the list of the File Metadata provided by BaseSpace API
     */
    public List<UploadStreamTracker> getFileMetadata(String runId) {

        logger.info("targetThroughputInGbps= {}", targetThroughputInGbps);
        logger.info("maxConcurrency= {}", maxConcurrency);
        logger.info("minimumPartSizeInBytes= {}", minimumPartSizeInMB);
        logger.info("corePoolSize= {}", corePoolSize);
        logger.info("maximumPoolSize= {}", maximumPoolSize);

        List<StreamFileDTO> responseItems = Collections.emptyList();
        List<UploadStreamTracker> uploadStreamTrackerList = Collections.emptyList();

        logger.info("Invoking BSSH api to fetch file(s) metadata to upload");

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(bearerToken);
        HttpEntity<String> requestEntity = new HttpEntity<>(headers);

        ResponseEntity<String> result = restTemplate.exchange(baseURL.replace(runIdAPIPlaceHolder, runId),
                HttpMethod.GET, requestEntity, String.class);
        if (result != null && !StringUtils.isEmpty(result.getBody())) {
            Gson gson = new Gson();
            ResponseDTO responseDTO = gson.fromJson(result.getBody(), ResponseDTO.class);
            logger.info("Files Count for runID: {} is :{}", runId, responseDTO.getItems().size());
            responseItems = responseDTO.getItems();
            uploadStreamTrackerList = getStreamFileTrackerList(responseItems, UploadTrackerConstants.INPROGRESS.toString());
            uploadStreamTrackerList = streamFilesRepository.saveAllAndFlush(uploadStreamTrackerList);
        }

        return uploadStreamTrackerList;
    }

    /**
     * @return S3TransferManager
     */
    public S3TransferManager getTransferManager() {
        // Setting thread Name
        Thread.currentThread().setName("UploadData_V2");

        return S3TransferManager.builder()
                .s3Client(getS3AsyncClient())
                .executor(createExecutorService(executorThreadSize, Thread.currentThread().getName() + "_slave"))
                .build();
    }

    /**
     * @return S3AsyncClient
     */
    private S3AsyncClient getS3AsyncClient() {
        AwsCredentialsProvider awsCredentials = StaticCredentialsProvider
                .create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey));

        S3CrtHttpConfiguration s3CrtHttpConfiguration =
                S3CrtHttpConfiguration.builder()
                        .connectionTimeout(Duration.ofSeconds(30))
                        .build();

        return S3AsyncClient.crtBuilder()
                .region(Region.of(aws_region))
                .credentialsProvider(awsCredentials)
                .minimumPartSizeInBytes(minimumPartSizeInMB * 1024 * 1024)
                .targetThroughputInGbps(targetThroughputInGbps)
                .maxConcurrency(maxConcurrency)
                .httpConfiguration(s3CrtHttpConfiguration)
                .build();
    }

    /**
     * @param threadNumber
     * @param slaveThreadName
     * @return ThreadPoolExecutor
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