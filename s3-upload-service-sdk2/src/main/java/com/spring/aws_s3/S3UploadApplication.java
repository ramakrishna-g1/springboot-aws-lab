package com.spring.aws_s3;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@SpringBootApplication
@EnableJpaAuditing
public class S3UploadApplication {

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

    public static void main(String[] args) {
        SpringApplication.run(S3UploadApplication.class, args);
        System.out.println("!.............Application Started Successfully.............!");
    }

    @Bean(name = "s3ClientBeanForV4")
    public S3Client getS3ClientBeanForV4() {
        AwsCredentialsProvider awsCredentialsProvider1 = StaticCredentialsProvider
                .create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey));

        return S3Client.builder()
                .credentialsProvider(awsCredentialsProvider1)
                .region(Region.of(aws_region)).build();
    }

//    @Bean("s3AsyncClient")
//    public S3AsyncClient getS3AsyncClient() {
//        AwsCredentialsProvider awsCredentials = StaticCredentialsProvider
//                .create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey));
//
//        S3CrtHttpConfiguration s3CrtHttpConfiguration =
//                S3CrtHttpConfiguration.builder()
//                        .connectionTimeout(Duration.of(10, ChronoUnit.SECONDS))
//                        .build();
//
//        return S3AsyncClient.crtBuilder()
//                .region(Region.of(aws_region))
//                .credentialsProvider(awsCredentials)
//                .minimumPartSizeInBytes(minimumPartSizeInMB * 1024 * 1024)
//                .httpConfiguration(s3CrtHttpConfiguration)
//                .targetThroughputInGbps(targetThroughputInGbps)
//                .maxConcurrency(maxConcurrency)
//                .build();
//    }


}
