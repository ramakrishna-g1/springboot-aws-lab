package com.spring.aws_s3.rest;

import com.spring.aws_s3.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UploadFilesRestAPI {

    @Autowired
    UploadFilesService uploadFilesService;

    @Autowired
    UploadDirectoryService uploadDirectoryService;

    @Autowired
    UploadStreamV1Service uploadStreamV1Service;

    @Autowired
    UploadStreamV2Service uploadStreamV2Service;

    @Autowired
    UploadStreamV3Service uploadStreamV3Service;

    @Autowired
    UploadStreamV4Service uploadStreamV4Service;

    @Autowired
    UploadStreamV5Service uploadStreamV5Service;

    /**
     * @return message String
     */
    @GetMapping("/uploadFilesToS3")
    public String uploadFilesToS3() {
        return uploadFilesService.uploadFilesToS3();
    }

    /**
     * @return message String
     */
    @GetMapping("/uploadDirectoryToS3")
    public String uploadDirectoryToS3() {
        return uploadDirectoryService.uploadDirectoryToS3();
    }

    /**
     * @return message String
     */
    @GetMapping("v1/uploadStreamToS3/{runIds}")
    public String uploadStreamV1(@PathVariable @NonNull String runIds) {
        return uploadStreamV1Service.uploadStreamV1(runIds);
    }

    /**
     * @return message String
     */
    @GetMapping("v2/uploadStreamToS3/{runIds}")
    public String uploadStreamV2(@PathVariable @NonNull String runIds) {
        return uploadStreamV2Service.uploadStreamV2(runIds);
    }

    /**
     * @return message String
     */
    @GetMapping("v3/uploadStreamToS3/{runIds}")
    public String uploadStreamV3(@PathVariable @NonNull String runIds) {
        return uploadStreamV3Service.uploadStreamToS3(runIds);
    }

    @GetMapping("v4/uploadStreamToS3/{runIds}")
    public String uploadStreamV4(@PathVariable @NonNull String runIds) {
        return uploadStreamV4Service.uploadMultiPartStreamToS3(runIds);
    }

    @GetMapping("v5/uploadStreamToS3/{runIds}")
    public String uploadStreamV5(@PathVariable @NonNull String runIds) {
        return uploadStreamV5Service.uploadStreamToS3(runIds);
    }

}

