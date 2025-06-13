package com.spring.aws_s3.service;

import com.spring.aws_s3.constant.UploadTrackerConstants;
import com.spring.aws_s3.dto.FileUploadDTO;
import com.spring.aws_s3.model.UploadTracker;
import com.spring.aws_s3.repository.UploadTrackerRepository;
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
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.io.File;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

@Service
public class UploadDirectoryService {

    Logger logger = LoggerFactory.getLogger(UploadDirectoryService.class);

    @Value("${retryWaitTime:5000}")
    private int retryWaitTime;

    @Value("${maxRetryFoUploading:3}")
    private int maxRetryFoUploading;

    @Value("${upload_executor_pool_size:10}")
    private int executorThreadSize;

    @Value("${s3_DirectoryUploadFolder_Key:UploadFilesSDK2}")
    private String s3_DirectoryUploadFolder_Key;

    @Autowired
    private UploadTrackerRepository uploadTrackerRepo;

    @Autowired
    private UploadFilesService uploadFilesService;

    @Value("${aws_access_key}")
    private String awsAccessKey;

    @Value("${aws_secret_key}")
    private String awsSecretKey;

    @Value("${aws_region}")
    private String region;

    @Value("${s3_bucket_name}")
    private String s3BucketName;

    @Value("${sourceDirectory}")
    private String sourceDirectory;

    @Value("${blockedExtensionList:csv,jpg}")
    private List<String> blockedExtensionList;

    @Value("${includeSubDirectoriesFiles:true}")
    private boolean includeSubDirectoriesFiles;

    public static final int KB = 1024;

    public static final int MB = 1024 * KB;

    /**
     * @return Message String
     */
    public String uploadDirectoryToS3() {
        List<UploadTracker> allUploadTrackerList = null;
        String uploadResponse = null;
        S3TransferManager transferManager = null;

        try {
            List<File> allowedFilesToBeUploaded = new ArrayList<>();
            populateAllowedFilesToS3Bucket(new File(sourceDirectory), allowedFilesToBeUploaded);

            logger.info("total files found in given directory: {}", allowedFilesToBeUploaded.size());

            // Setting thread Name
            Thread.currentThread().setName("UploadData");

            transferManager = getTransferManager();
            logger.info("created TransferManager object");

            FileUploadDTO fileUploadDTO = new FileUploadDTO();
            fileUploadDTO.setTransferManager(transferManager);

            allUploadTrackerList = getUploadTrackerList(allowedFilesToBeUploaded,
                    UploadTrackerConstants.PENDING.toString());
            uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);

            transferFilesToS3UsingDirectoryUpload(allowedFilesToBeUploaded, fileUploadDTO, allUploadTrackerList);
            uploadResponse = "Upload initiated successfully";

        } catch (Exception e) {
            logger.error("exception occurred: {}", e.getMessage());
            uploadResponse = "Exception occurred, upload Failed";
        } finally {
            if (!ObjectUtils.isEmpty(transferManager)) {
                transferManager.close();
            }
        }
        return uploadResponse;
    }

    private S3TransferManager getTransferManager() {
        return S3TransferManager.builder()
                .s3Client(getS3AsyncClient())
                .executor(createExecutorService(executorThreadSize, Thread.currentThread().getName() + "_slave"))
                .build();
    }

    private S3AsyncClient getS3AsyncClient() {
        AwsCredentialsProvider awsCredentials = StaticCredentialsProvider
                .create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey));

        return S3AsyncClient.crtBuilder()
                .region(Region.US_WEST_2)
                .credentialsProvider(awsCredentials)
                .minimumPartSizeInBytes(8L * MB)
                .targetThroughputInGbps(20.0)
                .build();
    }

    /**
     * This method will add the files into list, it will filter out the files with blocked extensions as well
     *
     * @param directory            The File object of source directory from where files are to be picked
     * @param allowedFilesToUpload List of the allowed files that are to be uploaded
     */
    private void populateAllowedFilesToS3Bucket(File directory, List<File> allowedFilesToUpload) {
        logger.info("source directory exists: {}, readable: {}, writable: {}", directory.exists(), directory.canRead(), directory.canWrite());
        File[] listOfFileInDir = directory.listFiles();
        logger.info("list of files: {}", Arrays.toString(listOfFileInDir));

        if (listOfFileInDir != null) {
            for (File file : listOfFileInDir) {
                if (file.isDirectory()) {
                    if (includeSubDirectoriesFiles) {
                        logger.info("found subDirectory: {}, exists: {}, readable: {}, writable: {}", file.getName(), file.exists(), file.canRead(), file.canWrite());
                        populateAllowedFilesToS3Bucket(file, allowedFilesToUpload);
                    }
                } else {
                    logger.info("found file: {}, exists: {}, readable: {}, writable: {}", file.getName(), file.exists(), file.canRead(), file.canWrite());

                    String extension = FilenameUtils.getExtension(file.getName());
                    if (!(file.getName().startsWith(".")) && (ObjectUtils.isEmpty(blockedExtensionList)
                            || !(blockedExtensionList.contains(extension.toLowerCase())))) {
                        allowedFilesToUpload.add(file);
                        logger.info("allowed file:{}", file.getName());
                    } else {
                        logger.info("blocked file:{}", file.getName());
                    }
                }
            }
        }
        logger.info("exit populateAllowedFilesToS3Bucket");
    }

    /**
     * This method is to upload the files to the S3 Bucket using DirectoryUpload.
     * After uploading it will update the status of that file accordingly before
     * and after completion of Upload in DB
     *
     * @param allowedFilesToBeUploaded List of files that are to be uploaded
     * @param fileUploadDTO            For setting the FileUploadRequest object to use in upcoming methods
     * @param allUploadTrackerList     List of all DB trackers, using which we will be updating the DB with InProgress Status
     */
    public void transferFilesToS3UsingDirectoryUpload(List<File> allowedFilesToBeUploaded, FileUploadDTO fileUploadDTO,
                                                      List<UploadTracker> allUploadTrackerList) throws Exception {
        logger.info("entered transferFilesToS3UsingDirectoryUpload method");

        if (!CollectionUtils.isEmpty(allowedFilesToBeUploaded)) {
            int retriesDone = 0;

            try {
                allUploadTrackerList.parallelStream().forEach(tracker ->
                        tracker.setFileStatus(UploadTrackerConstants.INPROGRESS.toString()));
                uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);
                logger.info("uploaded DB with InProgress status for all Files");

                long uploadStartTime = System.currentTimeMillis();

                UploadDirectoryRequest uploadDirectoryRequest = UploadDirectoryRequest.builder()
                        .source(Paths.get(sourceDirectory))
                        .bucket(s3BucketName)
                        .s3Prefix(s3_DirectoryUploadFolder_Key + "/DirectoryUploadTest")
                        .build();
                fileUploadDTO.setDirectoryRequest(uploadDirectoryRequest);

                uploadDirectoryToS3AndUpdateStatus(fileUploadDTO, allUploadTrackerList, retriesDone);

                logger.info("upload initiated in millis: {}", (System.currentTimeMillis() - uploadStartTime));
            } catch (Exception e) {
                logger.error("Exception occurred while uploading directory", e);
                throw new Exception();
            }
        }
        logger.info("exit transferFilesToS3UsingDirectoryUpload method");
    }

    /**
     * This method will upload a particular file into S3 Bucket and
     * updates db status to completed or failed, it will also update
     * the retriesDone column in the table based on retries done for failed files.
     *
     * @param fileUploadDTO        To get the S3TransferManager and UploadDirectoryRequest
     * @param allUploadTrackerList List of all UploadTrackers for updating Completed Status in DB for Files
     * @param retriesDone          Count of the retries Done for the File
     * @throws Exception Throws Exception if Upload fails due to any exception
     */
    private void uploadDirectoryToS3AndUpdateStatus(FileUploadDTO fileUploadDTO,
                                                    List<UploadTracker> allUploadTrackerList, int retriesDone) throws Exception {

        S3TransferManager transferManager = fileUploadDTO.getTransferManager();
        UploadDirectoryRequest uploadDirectoryRequest = fileUploadDTO.getDirectoryRequest();

        try {
            logger.info("upload is going to start");
            DirectoryUpload directoryUpload = transferManager.uploadDirectory(uploadDirectoryRequest);

            directoryUpload.completionFuture().whenCompleteAsync((result, exception) -> {
                logger.info("inside whenCompleteAsync");
                if (!ObjectUtils.isEmpty(result.failedTransfers())) {
                    logger.info("There are some failed Transfers, count of failed files: {}", result.failedTransfers().size());
                    Throwable exceptionOfFile1 = result.failedTransfers().get(0).exception();

                    if (exceptionOfFile1 != null) {
                        logger.info("Exception cause: {}", exceptionOfFile1.getMessage());
                        if (exceptionOfFile1 instanceof SocketTimeoutException
                                || exceptionOfFile1 instanceof ConnectException
                                || exceptionOfFile1 instanceof SdkClientException) {
                            logger.info("Not able to connect to server: {}", exceptionOfFile1.getMessage());
                        }
                        retryFailedFiles(result, transferManager, retriesDone);
                    }
                } else {
                    allUploadTrackerList.stream().filter(e -> !e.getFileName().isBlank())
                            .forEach(tracker -> tracker.setFileStatus(UploadTrackerConstants.COMPLETED.toString()));
                    uploadTrackerRepo.saveAllAndFlush(allUploadTrackerList);
                    logger.info("updated completed status for all files in DB");
                }
            });
        } catch (Exception e) {
            logger.error("Uploading Files to s3 failed, e.getMessage: {}", e.getMessage());
            throw new Exception("Uploading Files to s3 failed");
        }
    }

    private void retryFailedFiles(CompletedDirectoryUpload result, S3TransferManager transferManager, int retriesDone) {
        try {

            List<FailedFileUpload> failedFileUploads = result.failedTransfers();
            List<Path> pathList = failedFileUploads.stream()
                    .map(failedFileUpload -> failedFileUpload.request().source())
                    .collect(Collectors.toList());

            logger.info("Going for retry of failed files: {}", pathList.parallelStream().map(Path::getFileName).collect(Collectors.toList()));

            for (Path filePath : pathList) {
                logger.info("Retrying file:{}", filePath.getFileName().toString());
                long uploadStartTime = System.currentTimeMillis();
                int nextRetry = retriesDone + 1;

                UploadTracker uploadTracker = new UploadTracker();
                uploadTracker.setFileName(filePath.getFileName().toString());
                uploadTracker.setRetriesDone(nextRetry);
                int recordsUpdated = uploadTrackerRepo.updateRetriesDoneByFileNameAndFilePath(nextRetry, filePath.getFileName().toString(), filePath.toFile().getParent());
                logger.info("Updated {} retry count for: {}", recordsUpdated, filePath.getFileName().toString());

                try {
                    UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                            .putObjectRequest(b -> b.bucket(s3BucketName).key(s3_DirectoryUploadFolder_Key + "/" + filePath.getFileName()))
                            .addTransferListener(LoggingTransferListener.create())
                            .source(filePath)
                            .build();

                    logger.info("Retry upload is going to start retry number: {}", nextRetry);
                    FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);

                    fileUpload.completionFuture().whenCompleteAsync((singleFileResult, singleFileException) -> {
                        logger.info("Progress Snapshot: " + fileUpload.progress().snapshot().toString());
                        if (ObjectUtils.isEmpty(singleFileException)) {
                            uploadTracker.setFileStatus(UploadTrackerConstants.COMPLETED.toString());
                            int countOfRecordsUpdated = uploadTrackerRepo.updateFileStatusByFileNameAndFilePath(UploadTrackerConstants.COMPLETED.toString(), filePath.getFileName().toString(), filePath.toFile().getParent());
                            logger.info("File upload completed and updated {} status in DB for:{}", countOfRecordsUpdated, filePath.getFileName().toString());
                        } else {
                            try {
                                if (singleFileException instanceof SocketTimeoutException
                                        || singleFileException instanceof ConnectException
                                        || singleFileException instanceof SdkClientException) {
                                    logger.info("Not able to connect to server: {}", singleFileException.getMessage());
                                }
                                logger.info("No further retry, since exception came in single file upload retry ");
                                uploadTracker.setFileStatus(UploadTrackerConstants.FAILED.toString());
                                int countOfRecordsUpdated = uploadTrackerRepo.updateFileStatusByFileNameAndFilePath(UploadTrackerConstants.FAILED.toString(), filePath.getFileName().toString(), filePath.toFile().getParent());
                                logger.info("File upload failed and updated {} status in DB for: {}", countOfRecordsUpdated, filePath.getFileName().toString());
                            } catch (Exception e) {
                                logger.error("Exception occurred: {}", e.getMessage());
                            }
                        }
                    });
                } catch (Exception e) {
                    logger.info("Uploading Files to s3 failed, e.getMessage: {}", e.getMessage());
                    throw new Exception("Uploading Files to s3 failed");
                }
            }
        } catch (Exception e) {
            logger.info("exception occurred while retrying the failed Files of directory upload");
        }
    }

    /**
     * This method is to populate and returns the UploadTracker List for the give list of Files.
     *
     * @param allowedFilesToBeUploaded List of Files that has to be uploaded to S3
     * @param status                   Status that you wish set in all the UploadTrackers that will be returned
     * @return List<UploadTracker>
     */
    public List<UploadTracker> getUploadTrackerList(List<File> allowedFilesToBeUploaded, String status) {
        logger.info("enter getUploadTrackerList method");
        List<UploadTracker> uploadTrackerList = new ArrayList<>(0);
        logger.info("allowedFilesToBeUploaded size: {}", allowedFilesToBeUploaded.size());

        if (!CollectionUtils.isEmpty(allowedFilesToBeUploaded)) {
            try {
                for (File file : allowedFilesToBeUploaded) {
                    UploadTracker uploadTracker = new UploadTracker();

                    // TODO For future use
//                  String checksum = "";
//    				MessageDigest messageDigest = MessageDigest.getInstance(checksumalgorithm);
//    				checksum=generateFileChecksum(messageDigest,file);
//    				uploadTracker.setChecksum(checksum);

                    uploadTracker.setFileName(file.getName());
                    uploadTracker.setFileStatus(status);
                    uploadTracker.setRetriesDone(0);
                    uploadTracker.setFilePath(file.getParent());

                    uploadTrackerList.add(uploadTracker);
                }

            } catch (Exception e) {
                logger.error("exception while populating the uploadTracker List: {}", e.getMessage());
                e.printStackTrace();
            }
        }

        logger.info("exit uploadTrackerFile List count:{}", uploadTrackerList.size());
        return uploadTrackerList;
    }

    /**
     * @param threadNumber
     * @param slaveThreadName
     * @return
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
