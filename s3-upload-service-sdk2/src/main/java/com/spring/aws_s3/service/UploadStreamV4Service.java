package com.spring.aws_s3.service;

import com.google.gson.Gson;
import com.spring.aws_s3.constant.UploadTrackerConstants;
import com.spring.aws_s3.dto.ResponseDTO;
import com.spring.aws_s3.dto.StreamFileDTO;
import com.spring.aws_s3.model.UploadStreamTracker;
import com.spring.aws_s3.repository.StreamFilesRepository;
import com.spring.aws_s3.util.S3MultipartUploadUtil;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.Upload;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class UploadStreamV4Service {

    private final Logger logger = LoggerFactory.getLogger(UploadStreamV4Service.class);

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

    @Autowired
    @Qualifier("s3ClientBeanForV4")
    private S3Client s3Client;

    @Autowired
    @Qualifier("s3AsyncClient")
    private S3AsyncClient s3AsyncClient;


    private RestTemplate restTemplate = new RestTemplate();

    /**
     * This method will get the list of FileMetaData (StreamFileDTO) from the base URL of BSSH API.
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

        if (result.hasBody()) {
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
    public String uploadMultiPartStreamToS3(String runIds) {

        List<String> runIdList = List.of(runIds.split(","));
        logger.info("list of runIds: {}", runIdList);
        logger.info("ThroughputInGbps= {}, maxConcurrency= {}, minimumPartSizeInBytes= {}",
                targetThroughputInGbps, maxConcurrency, minimumPartSizeInMB);
        try {
            ForkJoinPool forkJoinPool = new ForkJoinPool(forkJoinPoolSize);
            for (String runID : runIdList) {

                long startTime = System.currentTimeMillis();

                CompletableFuture.runAsync(() -> {
                    try {
                        Thread.currentThread().setName(runID+"Async");
                        logger.info("Triggering upload for run:{}", runID);
                        List<UploadStreamTracker> fileTrackerList = getFileMetadata(runID.trim());

                        if (!CollectionUtils.isEmpty(fileTrackerList)) {
                            SimpleDateFormat sdf = new SimpleDateFormat("ddMMM_hh_mm_ss_SSSS");
                            String s3Key = runID + "_" + sdf.format(new Date());
                            AtomicInteger completedFiles = new AtomicInteger(0);
                            logger.info("Folder to copy files: V3_POC3/{}", s3Key);
                            logger.info("fileTracker List size for run:{} is: {}", runID, fileTrackerList.size());

                            for (UploadStreamTracker fileTracker : fileTrackerList) {
                                initiateUploadToS3(fileTracker, fileTrackerList, completedFiles, 0, s3Key);
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

    /**
     * @param fileTracker
     * @param fileTrackerList
     * @param completedCount
     * @param retriesDone
     * @param s3Key
     */
    public void initiateUploadToS3(UploadStreamTracker fileTracker,
                                   List<UploadStreamTracker> fileTrackerList, AtomicInteger completedCount, int retriesDone,
                                   String s3Key) throws ExecutionException, InterruptedException {

        logger.info("Entered into initiateUploadToS3 method");
        final int UPLOAD_PART_SIZE = (int) (minimumPartSizeInMB * 1024 * 1024); // Part Size should not be less than 5 MB while using MultipartUpload
        logger.info("Using part size : {}", UPLOAD_PART_SIZE);
        S3MultipartUploadUtil s3MultipartUploadUtil = new S3MultipartUploadUtil(s3BucketName, fileTracker.getPath(), s3AsyncClient, streamFilesRepository);
        s3MultipartUploadUtil.initializeUpload(fileTracker);

        Response response = null;

        try {
            Request httpRequest = new Request.Builder()
                    .get()
                    .url(fileTracker.getHrefContent())
                    .build();
            logger.debug("Created HTTP request for: {}", fileTracker.getPath());

            response = okHttpClient.newCall(httpRequest).execute();
            InputStream inputStream = null;
            logger.debug("BSSH API response time for fileID:{} is:{} millis", fileTracker.getFileId(), (response.receivedResponseAtMillis() - response.sentRequestAtMillis()));

            if (!ObjectUtils.isEmpty(response) && response.code() == HttpStatus.OK.value()) {
                if (!ObjectUtils.isEmpty(response.body())) {
                    inputStream = response.body().byteStream();
                } else {
                    logger.warn("Empty response received from BSSH for file fileId : {}", fileTracker.getFileId());
                    // Assign empty stream if throwing exception for empty files
                }
//                response.close();
            } else {
                if (!ObjectUtils.isEmpty(response) && !ObjectUtils.isEmpty(response.body())) {
                    response.body().close();
                }
                response.close();
                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.FAILED.toString(), fileTracker.getId(), fileTracker.getFileId());
                logger.info("Failed to retrieve data from the server for fileID:{}, status code: {}", fileTracker.getFileId(), response.code());
            }


            ArrayList<byte[]> byteChecksum = new ArrayList<>();
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");


            int bytesRead, bytesAdded = 0;
            byte[] dataPart = new byte[UPLOAD_PART_SIZE];
            ByteArrayOutputStream bufferOutputStream = new ByteArrayOutputStream();
            int finalPartByteSize = 0;

            while ((bytesRead = inputStream.read(dataPart, 0, dataPart.length)) != -1) {

                bufferOutputStream.write(dataPart, 0, bytesRead);

                if (bytesAdded < UPLOAD_PART_SIZE) {
                    bytesAdded += bytesRead;
                    continue;
                }
                logger.info("one part upload size is before checksum : " + bufferOutputStream.size());
                messageDigest.update(bufferOutputStream.toByteArray());
                byteChecksum.add(messageDigest.digest());
                messageDigest.reset();

                logger.info("one part upload size is after checksum : " + bufferOutputStream.size());

                s3MultipartUploadUtil.uploadPartAsync(new ByteArrayInputStream(bufferOutputStream.toByteArray()));
                bufferOutputStream.reset();
                bytesAdded = 0;
            }
            logger.info("final part bytes Added value " + bytesAdded);
            messageDigest.update(bufferOutputStream.toByteArray());
            byteChecksum.add(messageDigest.digest());
            messageDigest.reset();
            // upload remaining part of output stream as final part
            // bufferOutputStream size can be less than 5 MB as it is the last part of upload

           String finalCalculatedEtag = "";
            if (byteChecksum.size() > 1) {
                logger.info("Multipart download file detected");
                byte[] concBytes = new byte[byteChecksum.size() * 16];
                int byteIterator = 0;
                for (byte[] bytes : byteChecksum) {
                    System.arraycopy(bytes, 0, concBytes, byteIterator, bytes.length);
                    byteIterator += bytes.length;
                }

                System.out.println(Arrays.toString(concBytes));
                byte[] finalDigestOfDigests = messageDigest.digest(concBytes);
                finalCalculatedEtag = new BigInteger(1, finalDigestOfDigests).toString(16) + "-" + byteChecksum.size();
                logger.info("Final calculated multi part eTag : {}", finalCalculatedEtag);
            } else {
                logger.info("File was downloaded and uploaded and uploaded as single part");
                byte[] digest = messageDigest.digest(byteChecksum.get(0));

//                System.out.println("Custom formatted final hex : " + String.format("%032x", digest));
                finalCalculatedEtag = new BigInteger(1, digest).toString(16) + "-" + byteChecksum.size();
                logger.info("Final calculated single part MD5 is : {}", finalCalculatedEtag);
            }
            s3MultipartUploadUtil.uploadFinalPartAsync(new ByteArrayInputStream(bufferOutputStream.toByteArray()),
                    finalCalculatedEtag, fileTracker);

        } catch (IllegalStateException ex) {
            retryStreamUpload(fileTracker, fileTrackerList, completedCount, retriesDone, s3Key);
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

                {
                    if (fileTracker.getETag().contains("-")) {
                        logger.info("Multiupload filename: {} and eTag from upload response: {}", fileTracker.getPath(), result.response().eTag());
                    }
                }

                streamFilesRepository.updateStatusByIdAndFileId(UploadTrackerConstants.COMPLETED.toString(), fileTracker.getId(), fileTracker.getFileId());
                completedCount.getAndIncrement();
                logger.info("Completed files: {}, files left: {}", completedCount.get(), (fileTrackerList.size() - completedCount.get()));
            } else {
                logger.warn("Exception occurred while uploading file:{} exception: ", fileTracker.getPath(), exception);
                retryStreamUpload(fileTracker, fileTrackerList, completedCount, retriesDone, s3Key);
            }
        });
    }

    /**
     * @param fileTracker
     * @param fileTrackerList
     * @param completedCount
     * @param retriesDone
     * @param s3Key
     */
    private void retryStreamUpload(UploadStreamTracker fileTracker,
                                   List<UploadStreamTracker> fileTrackerList, AtomicInteger completedCount, int retriesDone, String s3Key) {
        try {
            int nextRetry = retriesDone + 1;
            Thread.sleep((long) retryWaitTime * nextRetry);

            if (retriesDone < maxRetryForUploading) {
                streamFilesRepository.updateRetriesDoneByIdAndFileId(nextRetry, fileTracker.getId(), fileTracker.getFileId());

                logger.info("Going to retry file upload:{}", fileTracker.getPath());
                initiateUploadToS3(fileTracker, fileTrackerList, completedCount, nextRetry, s3Key);
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

}