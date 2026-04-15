package com.prestobr.estoque.dto.response;

import com.prestobr.estoque.domain.entity.ProductStock;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
public class ProductStockResponse {

    // Chave
    private Integer codEmpresa;
    private String nomeEmpresa;
    private Integer codProduto;

    // Produto - Identificação
    private String nomeProduto;
    private String nomeResumido;
    private String referencia;
    private String codBarras;
    private Integer codGrupo;
    private Integer codSubgrupo;
    private String marca;
    private String unidadeProduto;
    private String classificacaoFiscal;
    private String tipoProduto;
    private String situacao;
    private String localizacaoProduto;
    private String prateleira;

    // Fornecedor principal
    private Integer codFornecedor;
    private String nomeFornecedor;
    private String fantasiaFornecedor;

    // Estoque atual
    private BigDecimal qtdEstoque;
    private BigDecimal qtdEstoqueBloqueado;
    private BigDecimal qtdEstoqueEntrada;
    private BigDecimal qtdEstoqueSaida;
    private BigDecimal qtdEstoquePendente;
    private BigDecimal qtdEstoqueReservado;
    private BigDecimal qtdEstoqueDisponivel;

    // Estoque fiscal
    private BigDecimal qtdFiscal;
    private BigDecimal qtdFiscalBloqueado;
    private BigDecimal qtdFiscalEntrada;
    private BigDecimal qtdFiscalSaida;
    private BigDecimal qtdFiscalPendente;
    private BigDecimal qtdFiscalReservado;
    private BigDecimal qtdFiscalDisponivel;

    // RMA
    private BigDecimal rma;
    private BigDecimal rmaFiscal;
    private BigDecimal rmaTotal;
    private BigDecimal rmaTotalFiscal;
    private BigDecimal rmaReservado;
    private BigDecimal rmaReservadoFiscal;
    private BigDecimal rmaGarantia;
    private BigDecimal rmaGarantiaFiscal;

    // Limites
    private BigDecimal produtoEstoqueMaximo;
    private BigDecimal produtoEstoqueMinimo;
    private BigDecimal produtoEstoqueSeguranca;

    // Flags calculadas
    private Boolean isAbaixoMinimo;
    private Boolean isAcimaMaximo;

    // Custos / Preços
    private BigDecimal custo;
    private BigDecimal custoMedio;
    private BigDecimal custoCompra;
    private BigDecimal custoCompraMedio;
    private BigDecimal precoVenda;
    private BigDecimal precoVendaMinimo;
    private BigDecimal markup;

    // Valor calculado
    private BigDecimal valorEstoque;

    // Inventário
    private LocalDateTime dataInventario;

    // Auditoria
    private String operadorCadastro;
    private String operadorAlteracao;
    private LocalDateTime dataCadastro;
    private LocalDateTime dataAlteracao;
    private LocalDateTime snapshotDatetime;

    public static ProductStockResponse from(ProductStock ps) {
        return ProductStockResponse.builder()
                // Chave
                .codEmpresa(ps.getCodEmpresa())
                .nomeEmpresa(ps.getNomeEmpresa())
                .codProduto(ps.getCodProduto())

                // Produto - Identificação
                .nomeProduto(ps.getNomeProduto())
                .nomeResumido(ps.getNomeResumido())
                .referencia(ps.getReferencia())
                .codBarras(ps.getCodBarras())
                .codGrupo(ps.getCodGrupo())
                .codSubgrupo(ps.getCodSubgrupo())
                .marca(ps.getMarca())
                .unidadeProduto(ps.getUnidadeProduto())
                .classificacaoFiscal(ps.getClassificacaoFiscal())
                .tipoProduto(ps.getTipoProduto())
                .situacao(ps.getSituacao())
                .localizacaoProduto(ps.getLocalizacaoProduto())
                .prateleira(ps.getPrateleira())

                // Fornecedor principal
                .codFornecedor(ps.getCodFornecedor())
                .nomeFornecedor(ps.getNomeFornecedor())
                .fantasiaFornecedor(ps.getFantasiaFornecedor())

                // Estoque atual
                .qtdEstoque(ps.getQtdEstoque())
                .qtdEstoqueBloqueado(ps.getQtdEstoqueBloqueado())
                .qtdEstoqueEntrada(ps.getQtdEstoqueEntrada())
                .qtdEstoqueSaida(ps.getQtdEstoqueSaida())
                .qtdEstoquePendente(ps.getQtdEstoquePendente())
                .qtdEstoqueReservado(ps.getQtdEstoqueReservado())
                .qtdEstoqueDisponivel(ps.getQtdEstoqueDisponivel())

                // Estoque fiscal
                .qtdFiscal(ps.getQtdFiscal())
                .qtdFiscalBloqueado(ps.getQtdFiscalBloqueado())
                .qtdFiscalEntrada(ps.getQtdFiscalEntrada())
                .qtdFiscalSaida(ps.getQtdFiscalSaida())
                .qtdFiscalPendente(ps.getQtdFiscalPendente())
                .qtdFiscalReservado(ps.getQtdFiscalReservado())
                .qtdFiscalDisponivel(ps.getQtdFiscalDisponivel())

                // RMA
                .rma(ps.getRma())
                .rmaFiscal(ps.getRmaFiscal())
                .rmaTotal(ps.getRmaTotal())
                .rmaTotalFiscal(ps.getRmaTotalFiscal())
                .rmaReservado(ps.getRmaReservado())
                .rmaReservadoFiscal(ps.getRmaReservadoFiscal())
                .rmaGarantia(ps.getRmaGarantia())
                .rmaGarantiaFiscal(ps.getRmaGarantiaFiscal())

                // Limites
                .produtoEstoqueMaximo(ps.getProdutoEstoqueMaximo())
                .produtoEstoqueMinimo(ps.getProdutoEstoqueMinimo())
                .produtoEstoqueSeguranca(ps.getProdutoEstoqueSeguranca())

                // Flags calculadas
                .isAbaixoMinimo(ps.getIsAbaixoMinimo())
                .isAcimaMaximo(ps.getIsAcimaMaximo())

                // Custos / Preços
                .custo(ps.getCusto())
                .custoMedio(ps.getCustoMedio())
                .custoCompra(ps.getCustoCompra())
                .custoCompraMedio(ps.getCustoCompraMedio())
                .precoVenda(ps.getPrecoVenda())
                .precoVendaMinimo(ps.getPrecoVendaMinimo())
                .markup(ps.getMarkup())

                // Valor calculado
                .valorEstoque(ps.getValorEstoque())

                // Inventário
                .dataInventario(ps.getDataInventario())

                // Auditoria
                .operadorCadastro(ps.getOperadorCadastro())
                .operadorAlteracao(ps.getOperadorAlteracao())
                .dataCadastro(ps.getDataCadastro())
                .dataAlteracao(ps.getDataAlteracao())
                .snapshotDatetime(ps.getSnapshotDatetime())
                .build();
    }
}