package com.prestobr.estoque.client;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class DataLakeClient {

    private final S3Client s3Client;
    private final String bucketName;

    public DataLakeClient(S3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    // =========================================================================
    // MÉTODOS COMUNS
    // =========================================================================

    public File downloadToTempFile(String s3Key) throws Exception {
        File tempFile = Files.createTempFile("datalake_", ".parquet").toFile();
        tempFile.delete(); // Deleta o arquivo vazio criado pelo createTempFile
        tempFile.deleteOnExit();

        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        s3Client.getObject(getRequest, ResponseTransformer.toFile(tempFile));

        log.debug("Arquivo baixado: {} -> {}", s3Key, tempFile.getAbsolutePath());
        return tempFile;
    }

    // =========================================================================
    // MÉTODOS INTERNOS
    // =========================================================================

    public List<String> findLatestRunParquetKeysFromPrefix(String prefix) {
        List<S3Object> allObjects = listAllObjects(prefix);

        List<S3Object> parquetFiles = allObjects.stream()
                .filter(obj -> obj.key().endsWith(".parquet"))
                .collect(Collectors.toList());

        if (parquetFiles.isEmpty()) {
            log.warn("Nenhum arquivo Parquet encontrado no prefix: {}", prefix);
            return List.of();
        }

        // Padrão: run-YYYYMMDD_HHMMSS
        Pattern runPattern = Pattern.compile("run-(\\d{8}_\\d{6})");

        Map<String, List<S3Object>> filesByRun = parquetFiles.stream()
                .collect(Collectors.groupingBy(obj -> {
                    Matcher matcher = runPattern.matcher(obj.key());
                    return matcher.find() ? matcher.group(1) : "unknown";
                }));

        filesByRun.remove("unknown");

        if (filesByRun.isEmpty()) {
            log.info("Nenhuma run identificada, usando fallback por lastModified");
            return parquetFiles.stream()
                    .sorted(Comparator.comparing(S3Object::lastModified).reversed())
                    .limit(10)
                    .map(S3Object::key)
                    .collect(Collectors.toList());
        }

        String latestRun = filesByRun.keySet().stream()
                .max(Comparator.naturalOrder())
                .orElseThrow();

        log.info("Run mais recente identificada: {}", latestRun);

        return filesByRun.get(latestRun).stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }

    private List<S3Object> listAllObjects(String prefix) {
        List<S3Object> allObjects = new ArrayList<>();

        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();

        ListObjectsV2Response response;
        do {
            response = s3Client.listObjectsV2(listRequest);
            allObjects.addAll(response.contents());
            listRequest = listRequest.toBuilder()
                    .continuationToken(response.nextContinuationToken())
                    .build();
        } while (response.isTruncated());

        return allObjects;
    }

    public String getBucketName() {
        return bucketName;
    }
}