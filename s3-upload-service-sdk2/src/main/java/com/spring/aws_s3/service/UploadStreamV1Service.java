package com.spring.aws_s3.service;

import com.google.gson.Gson;
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

import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Upload Stream to S3 using {@link S3TransferManager} and {@link S3AsyncClient}.
 * Custom {@link ThreadPoolExecutor} is used to upload configured number of streams in parallel.
 * <br>
 * NOTE: Record tracking in DB is not done for this class.
 */
@Service
public class UploadStreamV1Service {

    private final Logger logger = LoggerFactory.getLogger(UploadStreamV1Service.class);

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

    @Value("${corePoolSize:1}")
    private int corePoolSize;

    @Value("${maximumPoolSize:5}")
    private int maximumPoolSize;

    @Value("${targetThroughputInGbps}")
    private double targetThroughputInGbps;

    @Value("${maxConcurrency}")
    private int maxConcurrency;

    @Value("${minimumPartSizeInMB:10}")
    private long minimumPartSizeInMB;

    private RestTemplate restTemplate = new RestTemplate();

    /**
     * This method will upload the stream of the file to the S3
     *
     * @return String
     */
    public String uploadStreamV1(String runIds) {
        List<String> runIdList = List.of(runIds.split(","));
        logger.info("list of runIds: {}", runIdList);
        String response = null;

        try (S3TransferManager transferManager = getTransferManager()) {
            for (String runId : runIdList) {
                List<StreamFileDTO> streamFileDTOList = getFileMetadata(runId);

                if (!CollectionUtils.isEmpty(streamFileDTOList)) {
                    long startTime = System.currentTimeMillis();

                    AtomicInteger completedFiles = new AtomicInteger(0);
                    final int numberOfFiles = streamFileDTOList.size();

                    logger.info("corePoolSize for executor: {}", corePoolSize);
                    logger.info("maximumPoolSize for executor: {}", maximumPoolSize);
                    BlockingQueue<Runnable> workingQueue = new LinkedBlockingQueue<>();
                    ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
                            1000L, TimeUnit.MILLISECONDS, workingQueue);

                    //FOR testing multiple runs
                    SimpleDateFormat sdf = new SimpleDateFormat("ddMMM_hh_mm_ss_SSSS");
                    String s3Key = runId + "_" + sdf.format(new Date());
                    logger.info("Folder to copy files: V1_POC3/{}", s3Key);

                    for (StreamFileDTO file : streamFileDTOList) {
                        CompletableFuture.runAsync(() -> {
                            try {
                                if (!StringUtils.isEmpty(file.getHrefContent()) || !StringUtils.isEmpty(file.getPath())) {
                                    URL targetURL = new URL(file.getHrefContent());
                                    HttpURLConnection urlConnection = (HttpURLConnection) targetURL.openConnection();
                                    urlConnection.setRequestMethod(HttpMethod.GET.toString());
                                    urlConnection.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken);
                                    urlConnection.setRequestProperty(HttpHeaders.ACCEPT, MediaType.ALL_VALUE);
                                    urlConnection.connect();
                                    logger.debug("Created HTTP request for: {}", file.getPath());

                                    if (urlConnection.getResponseCode() == HttpStatus.OK.value()) {
                                        BlockingInputStreamAsyncRequestBody body = AsyncRequestBody.forBlockingInputStream(file.getSize());

                                        Upload upload = transferManager.upload(builder -> builder
                                                .requestBody(body)
                                                .addTransferListener(UploadProcessListener.create(file.getName()))
                                                .putObjectRequest(req -> req.bucket(s3BucketName).key("V1_POC3/" + s3Key + "/" + file.getPath()))
                                                .build());

                                        body.writeInputStream(urlConnection.getInputStream());

                                        urlConnection.getInputStream().close();
                                        urlConnection.disconnect();

                                        upload.completionFuture().whenCompleteAsync((result, exception) -> {
                                            if (ObjectUtils.isEmpty(exception)) {
                                                completedFiles.getAndIncrement();
                                                logger.info("Completed files: {}, files left: {}", completedFiles.get(), (numberOfFiles - completedFiles.get()));
                                            } else {
                                                logger.warn("Exception occurred while processing the file upload: {}, exceptionMessage:{}", file.getName(), exception.getMessage(), exception);
                                            }
                                        });
                                    } else {
                                        logger.info("Failed to retrieve data from the server status code: {}", urlConnection.getResponseCode());
                                    }
                                } else {
                                    logger.info("Either HrefContent or Path is empty unable to process upload for: {}!", file.getId());
                                }
                            } catch (ConnectException e) {
                                logger.error("Failed to Upload, unable to connect to server file:{}, exception {}", file.getId(), e.getMessage());
                            } catch (Exception e) {
                                logger.error("Exception occurred while uploading fileID: {}, exception:{}", file.getId(), e.getClass().getName(), e);
                            }
                        }, executor);
                    }
                    logger.info("Upload initiated in {}!", (System.currentTimeMillis() - startTime));
                    response = "Upload initiated!";
                } else {
                    response = "No files found to upload";
                }
            }
        } catch (Exception e) {
            logger.error("Exception occurred: {}", e.getMessage(), e);
            response = "Failed to upload data to S3";
        }
        return response;
    }


    /**
     * This method will get the list of FileMetaData (StreamFileDTO) from the base URL of BSSH API.
     *
     * @return List<StreamFileDTO> it contains the list of the File Metadata provided by BaseSpace API
     */
    public List<StreamFileDTO> getFileMetadata(String runId) {

        logger.info("targetThroughputInGbps= {}", targetThroughputInGbps);
        logger.info("maxConcurrency= {}", maxConcurrency);
        logger.info("minimumPartSizeInBytes= {}", minimumPartSizeInMB);
        logger.info("corePoolSize= {}", corePoolSize);
        logger.info("maximumPoolSize= {}", maximumPoolSize);

        List<StreamFileDTO> responseItems = Collections.emptyList();

        logger.info("Invoking BSSH api to fetch file(s) metadata to upload");

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(bearerToken);
        HttpEntity<String> requestEntity = new HttpEntity<>(headers);

        ResponseEntity<String> result = restTemplate.exchange(baseURL.replace(runIdAPIPlaceHolder, runId), HttpMethod.GET, requestEntity, String.class);
        if (result != null && !StringUtils.isEmpty(result.getBody())) {
            Gson gson = new Gson();
            ResponseDTO responseDTO = gson.fromJson(result.getBody(), ResponseDTO.class);
            responseItems = responseDTO.getItems();
            logger.info("Files Count :{}", responseItems.size());
        }

        return responseItems;
    }

    /**
     * @return S3TransferManager
     */
    public S3TransferManager getTransferManager() {
        // Setting thread Name
        Thread.currentThread().setName("UploadData_1");

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
