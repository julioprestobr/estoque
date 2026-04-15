package com.prestobr.estoque.service;

import com.prestobr.estoque.client.DataLakeClient;
import com.prestobr.estoque.dto.response.QueryResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class QueryService {

    private final DataLakeClient dataLakeClient;

    @Value("${s3.endpoint-url}")
    private String endpointUrl;

    @Value("${s3.access-key}")
    private String accessKey;

    @Value("${s3.secret-key}")
    private String secretKey;

    @Value("${s3.region}")
    private String region;

    @Value("${s3.bucket-name}")
    private String bucketName;

    private final Map<String, Connection> connectionCache = new ConcurrentHashMap<>();

    public QueryResponse execute(String query, String s3Path) {
        long startTime = System.currentTimeMillis();

        try {
            Connection conn = getOrCreateConnection(s3Path);
            Statement stmt = conn.createStatement();

            String finalQuery = query.replaceAll("(?i)FROM\\s+[\\w\\-]+", "FROM dados");

            log.info("Executing query: {}", finalQuery);

            ResultSet rs = stmt.executeQuery(finalQuery);

            List<String> columns = new ArrayList<>();
            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(meta.getColumnName(i));
            }

            List<Map<String, Object>> data = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (String col : columns) {
                    row.put(col, rs.getObject(col));
                }
                data.add(row);
            }

            long executionTime = System.currentTimeMillis() - startTime;

            return QueryResponse.builder()
                    .columns(columns)
                    .data(data)
                    .totalRecords(data.size())
                    .executedQuery(query)
                    .executionTimeMs(executionTime)
                    .build();

        } catch (SQLException e) {
            connectionCache.remove(s3Path);
            throw new RuntimeException("Error executing query: " + e.getMessage(), e);
        }
    }

    private synchronized Connection getOrCreateConnection(String s3Path) throws SQLException {
        Connection conn = connectionCache.get(s3Path);

        if (conn != null && !conn.isClosed()) {
            log.debug("Using cached connection for: {}", s3Path);
            return conn;
        }

        log.info("Creating new DuckDB connection for: {}", s3Path);

        List<String> latestKeys = dataLakeClient.findLatestRunParquetKeysFromPrefix(s3Path);

        if (latestKeys.isEmpty()) {
            throw new RuntimeException("No parquet files found in: " + s3Path);
        }

        log.info("Found {} files in latest run", latestKeys.size());

        String paths = latestKeys.stream()
                .map(key -> "'s3://" + bucketName + "/" + key + "'")
                .collect(Collectors.joining(", "));

        conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();

        stmt.execute("INSTALL httpfs;");
        stmt.execute("LOAD httpfs;");
        stmt.execute("SET s3_region='" + region + "';");
        stmt.execute("SET s3_access_key_id='" + accessKey + "';");
        stmt.execute("SET s3_secret_access_key='" + secretKey + "';");
        stmt.execute("SET s3_endpoint='" + endpointUrl.replace("https://", "").replace("http://", "") + "';");

        stmt.execute("CREATE TABLE dados AS SELECT * FROM read_parquet([" + paths + "], union_by_name=true);");

        log.info("Data loaded into memory for: {}", s3Path);

        connectionCache.put(s3Path, conn);
        return conn;
    }

    public void clearCache() {
        log.info("Clearing DuckDB connection cache");
        for (Map.Entry<String, Connection> entry : connectionCache.entrySet()) {
            try {
                if (entry.getValue() != null && !entry.getValue().isClosed()) {
                    entry.getValue().close();
                }
            } catch (SQLException e) {
                log.warn("Error closing connection: {}", e.getMessage());
            }
        }
        connectionCache.clear();
    }

    public void warmup(List<String> s3Paths) {
        for (String s3Path : s3Paths) {
            try {
                log.info("Warming up cache for: {}", s3Path);
                getOrCreateConnection(s3Path);
            } catch (Exception e) {
                log.error("Failed to warmup {}: {}", s3Path, e.getMessage());
            }
        }
    }
}