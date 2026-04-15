package com.prestobr.estoque.controller.v1;

import com.prestobr.estoque.dto.request.QueryRequest;
import com.prestobr.estoque.dto.response.QueryResponse;
import com.prestobr.estoque.service.QueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Tag(name = "Query")
public class QueryController {

    private final QueryService queryService;

    @PostMapping("/query")
    @Operation(summary = "Executa consulta SQL em dataset de datalake")
    public QueryResponse query(@Valid @RequestBody QueryRequest request) {
        return queryService.execute(request.getQuery(), request.getS3Path());
    }

    @DeleteMapping("/query/cache")
    @Operation(summary = "Limpa cache DuckDB")
    public ResponseEntity<String> clearCache() {
        queryService.clearCache();
        return ResponseEntity.ok("Cache cleared successfully");
    }

    @PostMapping("/query/warmup")
    @Operation(summary = "Carrega datasets no cache DuckDB")
    public ResponseEntity<String> warmup(@RequestBody List<String> s3Paths) {
        queryService.warmup(s3Paths);
        return ResponseEntity.ok("Warmup completed for " + s3Paths.size() + " datasets");
    }
}