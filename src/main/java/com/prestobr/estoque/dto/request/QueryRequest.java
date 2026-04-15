package com.prestobr.estoque.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;

@Getter
@Schema(description = "Requisição de query dinâmica para o Data Lake")
public class QueryRequest {

    @NotBlank
    private String dataset;

    @NotBlank
    @Schema(description = "Query em linguagem natural ou SQL-like",
            example = "SELECT SUM(valorTitulo) FROM contas WHERE dataVencimento < '2026-04-07'")
    private String query;

    @NotBlank
    private String s3Path;
}