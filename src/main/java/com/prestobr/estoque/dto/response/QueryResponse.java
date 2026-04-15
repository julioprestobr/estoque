package com.prestobr.estoque.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@Builder
@AllArgsConstructor
@Schema(description = "Resposta da query dinâmica")
public class QueryResponse {

    @Schema(description = "Colunas retornadas")
    private List<String> columns;

    @Schema(description = "Dados retornados (lista de mapas chave-valor)")
    private List<Map<String, Object>> data;

    @Schema(description = "Total de registros")
    private int totalRecords;

    @Schema(description = "Query executada")
    private String executedQuery;

    @Schema(description = "Tempo de execução em ms")
    private long executionTimeMs;
}