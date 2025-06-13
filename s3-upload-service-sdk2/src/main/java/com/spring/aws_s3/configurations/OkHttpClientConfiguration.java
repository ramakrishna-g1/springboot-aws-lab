package com.spring.aws_s3.configurations;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class OkHttpClientConfiguration {

    @Bean
    public OkHttpClient okHttpClient(){
        ConnectionPool connectionPool =
                new ConnectionPool(10, 5, TimeUnit.MINUTES);

        return new OkHttpClient.Builder()
                .connectionPool(connectionPool)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

}
