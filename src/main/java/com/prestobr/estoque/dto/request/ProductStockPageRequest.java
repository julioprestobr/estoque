package com.prestobr.estoque.dto.request;

import java.util.List;

public record ProductStockPageRequest(
        // Paginação
        Integer page,
        Integer size,
        List<String> sort,

        // Filtros
        Integer codEmpresa,
        Integer codProduto,
        String nomeProduto,
        String referencia,
        String codBarras,
        Integer codGrupo,
        Integer codSubgrupo,
        String marca,
        Integer codFornecedor,
        String tipoProduto,
        String situacao,

        // Flags
        Boolean abaixoMinimo,
        Boolean acimaMaximo,
        Boolean estoqueZerado
) {
    public ProductStockPageRequest {
        if (page == null) page = 0;
        if (size == null) size = 20;
    }
}