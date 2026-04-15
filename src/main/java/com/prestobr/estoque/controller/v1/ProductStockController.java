package com.prestobr.estoque.controller.v1;

import com.prestobr.estoque.domain.entity.ProductStock;
import com.prestobr.estoque.dto.request.ProductStockPageRequest;
import com.prestobr.estoque.dto.request.QueryRequest;
import com.prestobr.estoque.dto.response.PageResponse;
import com.prestobr.estoque.dto.response.ProductStockResponse;
import com.prestobr.estoque.dto.response.QueryResponse;
import com.prestobr.estoque.service.ProductStockService;
import com.prestobr.estoque.service.QueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/product-stock")
@RequiredArgsConstructor
@Tag(name = "Estoque de Produtos", description = "Consulta de estoque de produtos do Data Lake")
public class ProductStockController {

    private final ProductStockService productStockService;

    private static final String SORT_DESCRIPTION = "Campos disponíveis para ordenação (sort): dataEmissao, dataVencimento, dataEntrada, dataCadastro, dataAlteracao, valorTitulo, numeroParcela, diasAtraso. Direções disponíveis: asc, desc. Exemplo: [\"dataVencimento,asc\",\"valorTitulo,asc\",\"diasAtraso,desc\"]";

    @PostMapping("/search")
    @Operation(
            summary = "Busca estoque de produtos com filtros",
            description = SORT_DESCRIPTION
    )
    public PageResponse<ProductStockResponse> search(@RequestBody ProductStockPageRequest request) {
        return productStockService.search(request);
    }

    @GetMapping("/empresa/{codEmpresa}/produto/{codProduto}")
    @Operation(summary = "Busca estoque por empresa e produto")
    public ProductStockResponse getByEmpresaAndProduto(

            @Parameter(description = "Código da empresa")
            @PathVariable Integer codEmpresa,

            @Parameter(description = "Código do produto")
            @PathVariable Integer codProduto) {

        return productStockService.getByEmpresaAndProduto(codEmpresa, codProduto);
    }

    @DeleteMapping("/cache")
    @Operation(summary = "Limpa o cache de estoque de produtos")
    public String clearCache() {
        productStockService.clearCache();
        return "Cache limpo";
    }

    // =========================================================================
    // GOLD (AccountPayableEnriched)
    // =========================================================================

    @PostMapping("/enriched/search")
    @Operation(
            summary = "Busca contas a pagar enriquecidas com filtros (Gold)",
            description = SORT_DESCRIPTION
    )
    public PageResponse<ProductStockResponse> enrichedSearch(@RequestBody ProductStockPageRequest request) {
        return productStockService.search(request);
    }

    @PostMapping("/query")
    @Operation(summary = "Executa query dinâmica no Data Lake")
    public QueryResponse query(@RequestBody QueryRequest request) {
        return productStockService.executeQuery(request.getQuery());
    }
}