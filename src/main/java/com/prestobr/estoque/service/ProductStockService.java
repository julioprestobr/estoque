package com.prestobr.estoque.service;

import com.prestobr.estoque.client.DataLakeClient;
import com.prestobr.estoque.domain.entity.ProductStock;
import com.prestobr.estoque.dto.request.ProductStockPageRequest;
import com.prestobr.estoque.dto.response.PageResponse;
import com.prestobr.estoque.dto.response.Pagination;
import com.prestobr.estoque.dto.response.ProductStockResponse;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.ApplicationContext;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.prestobr.estoque.domain.util.ParquetUtils.*;
@Slf4j
@Service
public class ProductStockService {

    private final DataLakeClient dataLakeClient;
    private final ApplicationContext applicationContext;

    @Value("${datalake.gold-estoque-produto-base-prefix}")
    private String goldProductStockPrefix;

    public ProductStockService(DataLakeClient dataLakeClient, ApplicationContext applicationContext) {
        this.dataLakeClient = dataLakeClient;
        this.applicationContext = applicationContext;
    }

    private ProductStockService self() {
        return applicationContext.getBean(ProductStockService.class);
    }

    // =========================================================================
    // ENDPOINTS PÚBLICOS
    // =========================================================================

        public PageResponse<ProductStockResponse> search(ProductStockPageRequest request) {
        Pageable pageable = buildPageable(request);
        List<ProductStock> filtered = self().loadAll().stream()
                .filter(ps -> matchesFilters(ps, request))
                .collect(Collectors.toList());

        return toPageResponse(toPage(filtered, pageable));
    }

    public ProductStockResponse getByEmpresaAndProduto(Integer codEmpresa, Integer codProduto) {
        return self().loadAll().stream()
                .filter(ps -> codEmpresa.equals(ps.getCodEmpresa()) && codProduto.equals(ps.getCodProduto()))
                .findFirst()
                .map(ProductStockResponse::from)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Estoque não encontrado: empresa=" + codEmpresa + ", produto=" + codProduto
                ));
    }

    @CacheEvict(value = "product-stock", allEntries = true)
    public void clearCache() {
        log.info("Cache de estoque de produtos limpo");
    }

    // =========================================================================
    // CARREGAMENTO DE DADOS
    // =========================================================================

    @Cacheable("product-stock")
    public List<ProductStock> loadAll() {
        List<String> latestRunKeys = dataLakeClient.findLatestRunParquetKeysFromPrefix(goldProductStockPrefix);

        if (latestRunKeys.isEmpty()) {
            log.warn("Nenhum arquivo Parquet encontrado no Data Lake Gold (estoque)");
            return Collections.emptyList();
        }

        log.info("Encontrados {} arquivos Parquet na run mais recente (Gold estoque)", latestRunKeys.size());

        List<ProductStock> all = new ArrayList<>();
        for (String key : latestRunKeys) {
            all.addAll(readParquetFile(key));
        }

        log.info("Total de registros carregados: {}", all.size());
        return all;
    }

    private List<ProductStock> readParquetFile(String s3Key) {
        List<ProductStock> items = new ArrayList<>();
        File tempFile = null;

        try {
            tempFile = dataLakeClient.downloadToTempFile(s3Key);

            Configuration hadoopConf = new Configuration();
            Path parquetPath = new Path(tempFile.getAbsolutePath());

            try (ParquetReader<GenericRecord> reader = AvroParquetReader
                    .<GenericRecord>builder(HadoopInputFile.fromPath(parquetPath, hadoopConf))
                    .build()) {

                GenericRecord record;
                while ((record = reader.read()) != null) {
                    items.add(mapToEntity(record));
                }
            }

            log.debug("Lidos {} registros de {}", items.size(), s3Key);

        } catch (Exception e) {
            log.error("Erro ao ler arquivo Parquet {}: {}", s3Key, e.getMessage(), e);
        } finally {
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
        }

        return items;
    }

    // =========================================================================
    // MAPEAMENTO PARQUET -> ENTITY
    // =========================================================================

    private ProductStock mapToEntity(GenericRecord record) {
        return ProductStock.builder()
                // Chave
                .codEmpresa(getInteger(record, "cod_empresa"))
                .nomeEmpresa(getString(record, "nome_empresa"))
                .codProduto(getInteger(record, "cod_produto"))

                // Produto - Identificação
                .nomeProduto(getString(record, "nome_produto"))
                .nomeResumido(getString(record, "nome_resumido"))
                .referencia(getString(record, "referencia"))
                .codBarras(getString(record, "cod_barras"))
                .codGrupo(getInteger(record, "cod_grupo"))
                .codSubgrupo(getInteger(record, "cod_subgrupo"))
                .marca(getString(record, "marca"))
                .unidadeProduto(getString(record, "unidade_produto"))
                .classificacaoFiscal(getString(record, "classificacao_fiscal"))
                .tipoProduto(getString(record, "tipo_produto"))
                .situacao(getString(record, "situacao"))
                .localizacaoProduto(getString(record, "localizacao_produto"))
                .prateleira(getString(record, "prateleira"))

                // Fornecedor principal
                .codFornecedor(getInteger(record, "cod_fornecedor"))
                .nomeFornecedor(getString(record, "nome_fornecedor"))
                .fantasiaFornecedor(getString(record, "fantasia_fornecedor"))

                // Estoque atual
                .qtdEstoque(getBigDecimal(record, "qtd_estoque"))
                .qtdEstoqueBloqueado(getBigDecimal(record, "qtd_estoque_bloqueado"))
                .qtdEstoqueEntrada(getBigDecimal(record, "qtd_estoque_entrada"))
                .qtdEstoqueSaida(getBigDecimal(record, "qtd_estoque_saida"))
                .qtdEstoquePendente(getBigDecimal(record, "qtd_estoque_pendente"))
                .qtdEstoqueReservado(getBigDecimal(record, "qtd_estoque_reservado"))
                .qtdEstoqueDisponivel(getBigDecimal(record, "qtd_estoque_disponivel"))

                // Estoque fiscal
                .qtdFiscal(getBigDecimal(record, "qtd_fiscal"))
                .qtdFiscalBloqueado(getBigDecimal(record, "qtd_fiscal_bloqueado"))
                .qtdFiscalEntrada(getBigDecimal(record, "qtd_fiscal_entrada"))
                .qtdFiscalSaida(getBigDecimal(record, "qtd_fiscal_saida"))
                .qtdFiscalPendente(getBigDecimal(record, "qtd_fiscal_pendente"))
                .qtdFiscalReservado(getBigDecimal(record, "qtd_fiscal_reservado"))
                .qtdFiscalDisponivel(getBigDecimal(record, "qtd_fiscal_disponivel"))

                // RMA
                .rma(getBigDecimal(record, "rma"))
                .rmaFiscal(getBigDecimal(record, "rma_fiscal"))
                .rmaTotal(getBigDecimal(record, "rma_total"))
                .rmaTotalFiscal(getBigDecimal(record, "rma_total_fiscal"))
                .rmaReservado(getBigDecimal(record, "rma_reservado"))
                .rmaReservadoFiscal(getBigDecimal(record, "rma_reservado_fiscal"))
                .rmaGarantia(getBigDecimal(record, "rma_garantia"))
                .rmaGarantiaFiscal(getBigDecimal(record, "rma_garantia_fiscal"))

                // Limites
                .produtoEstoqueMaximo(getBigDecimal(record, "produto_estoque_maximo"))
                .produtoEstoqueMinimo(getBigDecimal(record, "produto_estoque_minimo"))
                .produtoEstoqueSeguranca(getBigDecimal(record, "produto_estoque_seguranca"))

                // Flags calculadas
                .isAbaixoMinimo(getBoolean(record, "is_abaixo_minimo"))
                .isAcimaMaximo(getBoolean(record, "is_acima_maximo"))

                // Custos / Preços
                .custo(getBigDecimal(record, "custo"))
                .custoMedio(getBigDecimal(record, "custo_medio"))
                .custoCompra(getBigDecimal(record, "custo_compra"))
                .custoCompraMedio(getBigDecimal(record, "custo_compra_medio"))
                .precoVenda(getBigDecimal(record, "preco_venda"))
                .precoVendaMinimo(getBigDecimal(record, "preco_venda_minimo"))
                .markup(getBigDecimal(record, "markup"))

                // Valor calculado
                .valorEstoque(getBigDecimal(record, "valor_estoque"))

                // Inventário
                .dataInventario(getLocalDateTime(record, "data_inventario"))

                // Auditoria
                .operadorCadastro(getString(record, "operador_cadastro"))
                .operadorAlteracao(getString(record, "operador_alteracao"))
                .dataCadastro(getLocalDateTime(record, "data_cadastro"))
                .dataAlteracao(getLocalDateTime(record, "data_alteracao"))

                // Metadados
                .snapshotDatetime(getLocalDateTime(record, "snapshot_datetime"))
                .build();
    }

    // =========================================================================
    // FILTROS
    // =========================================================================

    private boolean matchesFilters(ProductStock ps, ProductStockPageRequest request) {

        if (request.codEmpresa() != null && !request.codEmpresa().equals(ps.getCodEmpresa())) {
            return false;
        }

        if (request.codProduto() != null && !request.codProduto().equals(ps.getCodProduto())) {
            return false;
        }

        if (request.nomeProduto() != null && ps.getNomeProduto() != null
                && !ps.getNomeProduto().toLowerCase().contains(request.nomeProduto().toLowerCase())) {
            return false;
        }

        if (request.referencia() != null && !request.referencia().equals(ps.getReferencia())) {
            return false;
        }

        if (request.codBarras() != null && !request.codBarras().equals(ps.getCodBarras())) {
            return false;
        }

        if (request.codGrupo() != null && !request.codGrupo().equals(ps.getCodGrupo())) {
            return false;
        }

        if (request.codSubgrupo() != null && !request.codSubgrupo().equals(ps.getCodSubgrupo())) {
            return false;
        }

        if (request.marca() != null && ps.getMarca() != null
                && !ps.getMarca().toLowerCase().contains(request.marca().toLowerCase())) {
            return false;
        }

        if (request.codFornecedor() != null && !request.codFornecedor().equals(ps.getCodFornecedor())) {
            return false;
        }

        if (request.tipoProduto() != null && !request.tipoProduto().equals(ps.getTipoProduto())) {
            return false;
        }

        if (request.situacao() != null && !request.situacao().equals(ps.getSituacao())) {
            return false;
        }

        // Filtro: somente abaixo do mínimo
        if (Boolean.TRUE.equals(request.abaixoMinimo()) && !Boolean.TRUE.equals(ps.getIsAbaixoMinimo())) {
            return false;
        }

        // Filtro: somente acima do máximo
        if (Boolean.TRUE.equals(request.acimaMaximo()) && !Boolean.TRUE.equals(ps.getIsAcimaMaximo())) {
            return false;
        }

        // Filtro: estoque zerado
        if (request.estoqueZerado() != null) {

            BigDecimal estoque = ps.getQtdEstoque();

            if (Boolean.TRUE.equals(request.estoqueZerado())) {
                if (estoque == null || estoque.compareTo(BigDecimal.ZERO) != 0) {
                    return false;
                }
            } else {
                if (estoque == null || estoque.compareTo(BigDecimal.ZERO) == 0) {
                    return false;
                }
            }
        }

        return true;
    }

    // =========================================================================
    // PAGINAÇÃO E ORDENAÇÃO
    // =========================================================================

    private Pageable buildPageable(ProductStockPageRequest request) {
        if (request.sort() == null || request.sort().isEmpty()) {
            return PageRequest.of(request.page(), request.size());
        }

        List<Sort.Order> orders = request.sort().stream()
                .map(s -> {
                    String[] parts = s.split(",");
                    String field = parts[0];
                    Sort.Direction direction = parts.length > 1 && "desc".equalsIgnoreCase(parts[1])
                            ? Sort.Direction.DESC
                            : Sort.Direction.ASC;
                    return new Sort.Order(direction, field);
                })
                .toList();

        return PageRequest.of(request.page(), request.size(), Sort.by(orders));
    }

    private List<ProductStock> applySorting(List<ProductStock> list, Sort sort) {
        if (sort.isUnsorted()) {
            return list;
        }

        Comparator<ProductStock> comparator = null;

        for (Sort.Order order : sort) {
            Comparator<ProductStock> fieldComparator = getComparator(order.getProperty());

            if (fieldComparator != null) {
                if (order.isDescending()) {
                    fieldComparator = fieldComparator.reversed();
                }
                comparator = (comparator == null) ? fieldComparator : comparator.thenComparing(fieldComparator);
            }
        }

        if (comparator == null) {
            return list;
        }

        return list.stream().sorted(comparator).collect(Collectors.toList());
    }

    private Comparator<ProductStock> getComparator(String field) {
        return switch (field) {
            case "codEmpresa" -> Comparator.comparing(ProductStock::getCodEmpresa, Comparator.nullsLast(Comparator.naturalOrder()));
            case "codProduto" -> Comparator.comparing(ProductStock::getCodProduto, Comparator.nullsLast(Comparator.naturalOrder()));
            case "nomeProduto" -> Comparator.comparing(ProductStock::getNomeProduto, Comparator.nullsLast(Comparator.naturalOrder()));
            case "qtdEstoque" -> Comparator.comparing(ProductStock::getQtdEstoque, Comparator.nullsLast(Comparator.naturalOrder()));
            case "qtdEstoqueDisponivel" -> Comparator.comparing(ProductStock::getQtdEstoqueDisponivel, Comparator.nullsLast(Comparator.naturalOrder()));
            case "valorEstoque" -> Comparator.comparing(ProductStock::getValorEstoque, Comparator.nullsLast(Comparator.naturalOrder()));
            case "custoMedio" -> Comparator.comparing(ProductStock::getCustoMedio, Comparator.nullsLast(Comparator.naturalOrder()));
            case "precoVenda" -> Comparator.comparing(ProductStock::getPrecoVenda, Comparator.nullsLast(Comparator.naturalOrder()));
            case "dataInventario" -> Comparator.comparing(ProductStock::getDataInventario, Comparator.nullsLast(Comparator.naturalOrder()));
            default -> null;
        };
    }

    private Page<ProductStock> toPage(List<ProductStock> list, Pageable pageable) {
        List<ProductStock> sorted = applySorting(list, pageable.getSort());

        int start = (int) pageable.getOffset();
        int end = Math.min(start + pageable.getPageSize(), sorted.size());
        if (start > sorted.size()) {
            return new PageImpl<>(Collections.emptyList(), pageable, sorted.size());
        }
        return new PageImpl<>(sorted.subList(start, end), pageable, sorted.size());
    }

    private PageResponse<ProductStockResponse> toPageResponse(Page<ProductStock> page) {
        List<ProductStockResponse> content = page.getContent().stream()
                .map(ProductStockResponse::from)
                .toList();

        return new PageResponse<>(
                new Pagination(
                        page.getNumber(),
                        page.getSize(),
                        page.getTotalElements(),
                        page.getTotalPages()
                ),
                content
        );
    }

}