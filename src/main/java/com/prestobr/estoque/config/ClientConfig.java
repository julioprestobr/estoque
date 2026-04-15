package com.prestobr.estoque.config;

import com.prestobr.estoque.client.DataLakeClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class ClientConfig {

    @Value("${s3.bucket-name}")
    private String bucketName;

    @Value("${datalake.gold-estoque-produto-base-prefix}")
    private String goldProductStockPrefix;

    // ==================== DATALAKE CLIENT ====================

    @Bean
    public DataLakeClient dataLakeClient(S3Client s3Client) {

        return new DataLakeClient(
                s3Client,
                bucketName,
                goldProductStockPrefix
        );
    }

}