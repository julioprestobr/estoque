package com.prestobr.estoque.controller.v1;

import com.prestobr.estoque.dto.request.ProductStockPageRequest;
import com.prestobr.estoque.dto.request.QueryRequest;
import com.prestobr.estoque.dto.response.PageResponse;
import com.prestobr.estoque.dto.response.ProductStockResponse;
import com.prestobr.estoque.dto.response.QueryResponse;
import com.prestobr.estoque.service.ProductStockService;
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

    private static final String SORT_DESCRIPTION =
            "Campos disponíveis para ordenação (sort): " +
                    "codEmpresa, nomeEmpresa, codProduto, nomeProduto, " +
                    "qtdEstoque, qtdEstoqueDisponivel, valorEstoque, " +
                    "custoMedio, precoVenda, dataInventario. " +
                    "Direções disponíveis: asc, desc. " +
                    "Exemplo: [\"nomeProduto,asc\",\"qtdEstoque,desc\",\"valorEstoque,desc\"]";

    @PostMapping("/search")
    @Operation(summary = "Busca estoque de produtos com filtros", description = SORT_DESCRIPTION)
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

    @PostMapping("/query")
    @Operation(summary = "Executa query dinâmica no Data Lake")
    public QueryResponse query(@RequestBody QueryRequest request) {
        return productStockService.executeQuery(request.getQuery());
    }
}