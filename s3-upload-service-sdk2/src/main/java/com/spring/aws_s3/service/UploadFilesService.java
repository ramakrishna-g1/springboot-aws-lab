package com.spring.aws_s3.service;

import com.spring.aws_s3.constant.UploadTrackerConstants;
import com.spring.aws_s3.dto.FileUploadDTO;
import com.spring.aws_s3.model.UploadTracker;
import com.spring.aws_s3.repository.UploadTrackerRepository;
import com.spring.aws_s3.util.UploadProcessListener;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

@Service
public class UploadFilesService {

    Logger logger = LoggerFactory.getLogger(UploadFilesService.class);

    @Value("${retryWaitTime:5000}")
    private int retryWaitTime;

    @Value("${maxRetryFoUploading:3}")
    private int maxRetryFoUploading;

    @Value("${blockedExtensionList:csv,jpg}")
    private List<String> blockedExtensionList;

    @Value("${includeSubDirectoriesFiles:true}")
    private boolean includeSubDirectoriesFiles;

    @Value("${upload_executor_pool_size:10}")
    private int executorThreadSize;

    @Value("${aws_access_key}")
    private String awsAccessKey;

    @Value("${aws_secret_key}")
    private String awsSecretKey;

    @Value("${aws_region}")
    private String aws_region;

    @Value("${s3_bucket_name}")
    private String s3BucketName;

    @Value("${sourceDirectory}")
    private String sourceDirectory;

    @Value("${s3_FileUpload_Key:SDK2FileUpload}")
    private String s3_FileUpload_Key;

    @Value("${targetThroughputInGbps:20}")
    private Double targetThroughputInGbps;

    @Value("${minimumPartSizeInMB:10}")
    private Long minimumPartSizeInMB;

    @Value("${maxConcurrency:50}")
    private int maxConcurrency;

    @Autowired
    private UploadTrackerRepository uploadTrackerRepo;

    public static final int MB = 1024 * 1024;

    /**
     * S3 upload process starts from this method
     *
     * @return message String
     */
    public String uploadFilesToS3() {
        List<UploadTracker> allUploadTrackerList = null;
        String uploadResponse = null;
        S3TransferManager transferManager = null;

        try {
            List<File> allowedFilesToBeUploaded = new ArrayList<>(0);
            populateAllowedFiles(new File(sourceDirectory), allowedFilesToBeUploaded);

            if (!CollectionUtils.isEmpty(allowedFilesToBeUploaded)) {
                logger.info("File count in given directory: " + allowedFilesToBeUploaded.size());

                // Setting thread Name
                Thread.currentThread().setName("UploadData");

                transferManager = getTransferManager();

                FileUploadDTO fileUploadDTO = new FileUploadDTO();
                fileUploadDTO.setTransferManager(transferManager);

                allUploadTrackerList = getUploadTrackerList(allowedFilesToBeUploaded,
                        UploadTrackerConstants.PENDING.toString());
                allUploadTrackerList = uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);
                logger.info("Populated DB with {} records with Pending status", allUploadTrackerList.size());

                prepareFilesForS3Upload(allowedFilesToBeUploaded, fileUploadDTO, allUploadTrackerList);
                uploadResponse = "Upload initiated successfully";

                transferManager.close();
            } else {
                logger.info("No files found to upload, in source directory");
                uploadResponse = "No files found to upload, in source directory";
            }

        } catch (Exception e) {
            logger.info("Exception occurred while uploading files to S3: {}", e.getMessage(), e);
            if (!ObjectUtils.isEmpty(transferManager)) {
                transferManager.close();
            }
            uploadResponse = "Exception occurred, upload to S3 Failed";
        }
        return uploadResponse;
    }

    /**
     * @return S3TransferManager
     */
    public S3TransferManager getTransferManager() {
        return S3TransferManager.builder()
                .s3Client(getS3AsyncClient())
                .executor(createExecutorService(executorThreadSize, Thread.currentThread().getName() + "_slave"))
                .build();
    }

    private S3AsyncClient getS3AsyncClient() {
        AwsCredentialsProvider awsCredentials = StaticCredentialsProvider
                .create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey));

        return S3AsyncClient.crtBuilder()
                .region(Region.of(aws_region))
                .credentialsProvider(awsCredentials)
                .minimumPartSizeInBytes(minimumPartSizeInMB * MB)
                .targetThroughputInGbps(targetThroughputInGbps)
                .maxConcurrency(maxConcurrency)
                .build();
    }

    /**
     * This method is used to load the files from the configured directory on the disk
     *
     * @param directory            The File object of source directory from where files are to be picked
     * @param allowedFilesToUpload List of the allowed files that are to be uploaded
     */
    private void populateAllowedFiles(File directory, List<File> allowedFilesToUpload) {

        File[] listOfFileInDir = directory.listFiles();

        if (listOfFileInDir != null) {
            for (File file : listOfFileInDir) {
                if (file.isDirectory()) {
                    if (includeSubDirectoriesFiles) {
                        logger.debug("found subDirectory: {}, exists: {}, readable: {}, writable: {}", file.getName(), file.exists(), file.canRead(), file.canWrite());
                        populateAllowedFiles(file, allowedFilesToUpload);
                    }
                } else {
                    logger.debug("found file: {}, exists: {}, readable: {}, writable: {}", file.getName(), file.exists(), file.canRead(), file.canWrite());

                    String extension = FilenameUtils.getExtension(file.getName());
                    if (!(file.getName().startsWith(".")) && (ObjectUtils.isEmpty(blockedExtensionList)
                            || !(blockedExtensionList.contains(extension.toLowerCase())))) {
                        allowedFilesToUpload.add(file);
                        logger.debug("allowed file:{}", file.getName());
                    } else {
                        logger.info("blocked file:{}", file.getName());
                    }
                }
            }
        }
    }

    /**
     * This method is to upload the files to the S3 Bucket using batch process
     * depending on number of files to be uploaded. After uploading it will update
     * the status of that file accordingly before and after completion of Upload in
     * DB
     *
     * @param allowedFilesToBeUploaded List of files that are to be uploaded
     * @param fileUploadDTO            For setting the FileUploadRequest object
     * @param allUploadTrackerList     List of all DB trackers, using which we will be updating the DB with InProgress Status
     */
    public void prepareFilesForS3Upload(List<File> allowedFilesToBeUploaded, FileUploadDTO fileUploadDTO,
                                        List<UploadTracker> allUploadTrackerList) {

        try {
            allUploadTrackerList.parallelStream().forEach(tracker ->
                    tracker.setFileStatus(UploadTrackerConstants.INPROGRESS.toString()));
            uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);
            logger.info("updated DB with InProgress status for {} Files", allUploadTrackerList.size());

            long uploadStartTime = System.currentTimeMillis();

            for (File file : allowedFilesToBeUploaded) {
                UploadFileRequest uploadFileRequest = null;
                int retriesDone = 0;

                String parentDirectoryPath;
                if (file.getParentFile().getPath().startsWith("/")) {
                    parentDirectoryPath =
                            file.getParentFile().getPath()
                                    .replaceFirst("/", "");
                } else if (file.getParentFile().getPath().contains("\\")) {
                    parentDirectoryPath =
                            file.getParentFile().getPath()
                                    .replace("\\", "/");
                } else {
                    parentDirectoryPath = file.getParentFile().getPath();
                }

                uploadFileRequest = UploadFileRequest.builder()
                        .putObjectRequest(b -> b.bucket(s3BucketName)
                                .key(s3_FileUpload_Key + "/" + parentDirectoryPath + "/" + file.getName()))
                        .addTransferListener(UploadProcessListener.create(file.getName()))
                        .source(Paths.get(file.getPath()))
                        .build();

                fileUploadDTO.setFileRequest(uploadFileRequest);

                uploadFileToS3AndUpdateStatus(file, fileUploadDTO, allUploadTrackerList, retriesDone);
            }
            logger.info("upload initiated for all files in millis:" + (System.currentTimeMillis() - uploadStartTime));

        } catch (Exception e) {
            logger.info("Exception occurred, while uploading files, stopping further upload: {}", e.getMessage());

            allUploadTrackerList.parallelStream()
                    .filter(tracker ->
                            List.of(UploadTrackerConstants.PENDING.toString(),
                                            UploadTrackerConstants.INPROGRESS.toString())
                                    .contains(tracker.getFileStatus()))
                    .forEach(uploadTracker -> uploadTracker.setFileStatus(UploadTrackerConstants.FAILED.toString())
                    );
            uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);
        }
    }

    /**
     * This method will upload a particular file into S3 Bucket and updated db status to completed or failed,
     * it will also update the retriesDone column in the table accordingly
     *
     * @param file                 The file which is to be uploaded to S3
     * @param fileUploadDTO        This DTO contains object of S3TransferManager and UploadFileRequest
     * @param allUploadTrackerList List with all file trackers, with fileName, current retriesCount and Status
     * @param retriesDone          This is the retryCount
     */
    public void uploadFileToS3AndUpdateStatus(File file, FileUploadDTO fileUploadDTO,
                                              List<UploadTracker> allUploadTrackerList, int retriesDone) {

        S3TransferManager transferManager = fileUploadDTO.getTransferManager();
        UploadFileRequest uploadFileRequest = fileUploadDTO.getFileRequest();

        logger.info("upload is going to start for file: {}", file.getName());
        FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);

        fileUpload.completionFuture().whenCompleteAsync((result, exception) -> {
            logger.info("Progress Snapshot for file:{} " + fileUpload.progress().snapshot().toString(), file.getName());

            if (ObjectUtils.isEmpty(exception)) {
                allUploadTrackerList.parallelStream()
                        .filter(tracker ->
                                tracker.getFileName().equals(file.getName())
                                        && tracker.getFilePath().equals(file.getParent()))
                        .forEach(
                                uploadTracker -> uploadTracker.setFileStatus(UploadTrackerConstants.COMPLETED.toString())
                        );
                uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);
                logger.info("File upload completed and updated status to completed in DB for: {}", file.getName());
            } else {
                try {
                    logger.info("Upload failed, going for retry of: {}, retryNumber: {}", file.getName(), retriesDone + 1);
                    retryLogicForFileUpload(file, fileUploadDTO, allUploadTrackerList, retriesDone);
                } catch (Exception e) {
                    logger.error("Exception occurred while executing retries for file: {} updated Status to failed in DB.", file.getName(), e);
                    allUploadTrackerList.parallelStream()
                            .filter(tracker ->
                                    tracker.getFileName().equals(file.getName())
                                            && tracker.getFilePath().equals(file.getParent()))
                            .forEach(
                                    uploadTracker -> uploadTracker.setFileStatus(UploadTrackerConstants.FAILED.toString())
                            );
                    uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);
                }
            }
        });
    }

    /**
     * This method will retry to upload the file,
     * if maximum threshold of the retry is reached then it will update DB with Failed status.
     *
     * @param file                 This is the file that is getting uploaded to S3
     * @param fileUploadDTO        This DTO is required so that we can call the uploadFileToS3AndUpdateStatus for Retry
     * @param allUploadTrackerList Status and retry will be updated in particular UploadTracker object.
     * @param retriesDone          This is the count of retries done.
     * @throws Exception throws exception if any exception came during processing of this retry logic
     */
    private void retryLogicForFileUpload(File file, FileUploadDTO fileUploadDTO,
                                         List<UploadTracker> allUploadTrackerList, int retriesDone) throws Exception {

        // Retry logic starts here
        if (retriesDone < maxRetryFoUploading) {
            Thread.sleep(retryWaitTime);

            // retriesDone starts from 0, so we add 1 for first retry
            int nextRetry = retriesDone + 1;
            allUploadTrackerList.parallelStream()
                    .filter(tracker ->
                            tracker.getFileName().equals(file.getName())
                                    && tracker.getFilePath().equals(file.getParent()))
                    .forEach(
                            uploadTracker -> uploadTracker.setRetriesDone(nextRetry)
                    );
            uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);
            logger.info("Updated retries count in DB to: {}", nextRetry);

            uploadFileToS3AndUpdateStatus(file, fileUploadDTO, allUploadTrackerList, nextRetry);
        } else {
            logger.info("Uploading files failed even after all retries for file: {}", file.getName());

            allUploadTrackerList.parallelStream()
                    .filter(tracker ->
                            tracker.getFileName().equals(file.getName())
                                    && tracker.getFilePath().equals(file.getParent()))
                    .forEach(
                            uploadTracker -> uploadTracker.setFileStatus(UploadTrackerConstants.FAILED.toString())
                    );
            uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);
            logger.info("Updated File status to Failed in DB for: {}", file.getName());
        }
    }

    /**
     * This method is to populate the UploadTracker List
     *
     * @param allowedFilesToBeUploaded List of Files that has to be uploaded to S3
     * @param status                   Status that you wish set in all the UploadTrackers that will be returned
     * @return List<UploadTracker>
     */
    public List<UploadTracker> getUploadTrackerList(List<File> allowedFilesToBeUploaded, String status) {

        List<UploadTracker> uploadTrackerList = new ArrayList<>(0);
        if (!CollectionUtils.isEmpty(allowedFilesToBeUploaded)) {
            for (File file : allowedFilesToBeUploaded) {
                UploadTracker uploadTracker = new UploadTracker();
                uploadTracker.setFileName(file.getName());
                uploadTracker.setFileStatus(status);
                uploadTracker.setRetriesDone(0);
                uploadTracker.setFilePath(file.getParent());
                uploadTrackerList.add(uploadTracker);
            }
        }
        return uploadTrackerList;
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
