package com.spring.aws_s3.configurations;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Configuration
public class S3AsyncClientConfiguration {

    @Value("${aws_access_key}")
    private String awsAccessKey;

    @Value("${aws_secret_key}")
    private String awsSecretKey;

    @Value("${aws_region}")
    private String aws_region;

    @Value("${maxConcurrency}")
    private int maxConcurrency;

    @Value("${targetThroughputInGbps}")
    private double targetThroughputInGbps;

    @Value("${minimumPartSizeInMB:10}")
    private long minimumPartSizeInMB;

    @Bean
    public S3AsyncClient s3AsyncClient() {
        AwsCredentialsProvider awsCredentials = StaticCredentialsProvider
                .create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey));

        S3CrtHttpConfiguration s3CrtHttpConfiguration=
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
}
