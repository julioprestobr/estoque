package com.prestobr.estoque.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Modelo de dados de estoque por produto/empresa.
 * Leitura direta dos arquivos Parquet (camada Gold) no S3.
 *
 * Chave composta: codEmpresa + codProduto
 *
 * Fonte: gold/estoque/estoque_produto/
 * Particionamento S3: /system=databit/empresa={codEmpresa}/
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductStock {

    // =========================================================================
    // CHAVE
    // =========================================================================
    private Integer codEmpresa;
    private String nomeEmpresa;
    private Integer codProduto;

    // =========================================================================
    // PRODUTO - IDENTIFICAÇÃO
    // =========================================================================
    private String nomeProduto;
    private String nomeResumido;
    private String referencia;
    private String codBarras;
    private Integer codGrupo;
    private String nomeGrupo;
    private Integer codSubgrupo;
    private String marca;
    private String unidadeProduto;
    private String classificacaoFiscal;
    private String tipoProduto;
    private String situacao;
    private String localizacaoProduto;
    private String prateleira;

    // =========================================================================
    // FORNECEDOR PRINCIPAL
    // =========================================================================
    private Integer codFornecedor;
    private String nomeFornecedor;
    private String fantasiaFornecedor;

    // =========================================================================
    // ESTOQUE ATUAL
    // =========================================================================
    private BigDecimal qtdEstoque;
    private BigDecimal qtdEstoqueBloqueado;
    private BigDecimal qtdEstoqueEntrada;
    private BigDecimal qtdEstoqueSaida;
    private BigDecimal qtdEstoquePendente;
    private BigDecimal qtdEstoqueReservado;
    private BigDecimal qtdEstoqueDisponivel;

    // =========================================================================
    // ESTOQUE FISCAL
    // =========================================================================
    private BigDecimal qtdFiscal;
    private BigDecimal qtdFiscalBloqueado;
    private BigDecimal qtdFiscalEntrada;
    private BigDecimal qtdFiscalSaida;
    private BigDecimal qtdFiscalPendente;
    private BigDecimal qtdFiscalReservado;
    private BigDecimal qtdFiscalDisponivel;

    // =========================================================================
    // RMA
    // =========================================================================
    private BigDecimal rma;
    private BigDecimal rmaFiscal;
    private BigDecimal rmaTotal;
    private BigDecimal rmaTotalFiscal;
    private BigDecimal rmaReservado;
    private BigDecimal rmaReservadoFiscal;
    private BigDecimal rmaGarantia;
    private BigDecimal rmaGarantiaFiscal;

    // =========================================================================
    // LIMITES (CADASTRO DE PRODUTO)
    // =========================================================================
    private BigDecimal produtoEstoqueMaximo;
    private BigDecimal produtoEstoqueMinimo;
    private BigDecimal produtoEstoqueSeguranca;

    // =========================================================================
    // FLAGS CALCULADAS
    // =========================================================================
    private Boolean isAbaixoMinimo;
    private Boolean isAcimaMaximo;

    // =========================================================================
    // CUSTOS / PREÇOS
    // =========================================================================
    private BigDecimal custo;
    private BigDecimal custoMedio;
    private BigDecimal custoCompra;
    private BigDecimal custoCompraMedio;
    private BigDecimal precoVenda;
    private BigDecimal precoVendaMinimo;
    private BigDecimal markup;

    // =========================================================================
    // VALOR CALCULADO
    // =========================================================================
    private BigDecimal valorEstoque;

    // =========================================================================
    // INVENTÁRIO
    // =========================================================================
    private LocalDateTime dataInventario;

    // =========================================================================
    // AUDITORIA / METADADOS
    // =========================================================================
    private String operadorCadastro;
    private String operadorAlteracao;
    private LocalDateTime dataCadastro;
    private LocalDateTime dataAlteracao;
    private LocalDateTime snapshotDatetime;
}