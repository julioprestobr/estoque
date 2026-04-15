package com.prestobr.estoque.dto.response;

public record Pagination(
        int page,
        int size,
        long total,
        int pages
) {}