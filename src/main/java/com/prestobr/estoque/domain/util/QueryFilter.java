package com.prestobr.estoque.domain.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Getter
@AllArgsConstructor
public class QueryFilter {

    private String field;
    private String operator;
    private String value;

    public boolean matches(Object fieldValue) {
        if (fieldValue == null) {
            return "!=".equals(operator) || "<>".equals(operator);
        }

        return switch (operator) {
            case "=" -> equalsValue(fieldValue);
            case "!=", "<>" -> !equalsValue(fieldValue);
            case ">" -> compareValue(fieldValue) > 0;
            case ">=" -> compareValue(fieldValue) >= 0;
            case "<" -> compareValue(fieldValue) < 0;
            case "<=" -> compareValue(fieldValue) <= 0;
            case "LIKE" -> fieldValue.toString().toLowerCase().contains(value.toLowerCase().replace("%", ""));
            default -> true;
        };
    }

    private boolean equalsValue(Object fieldValue) {
        if (fieldValue instanceof Boolean) {
            return fieldValue.equals(Boolean.parseBoolean(value));
        }
        return fieldValue.toString().equalsIgnoreCase(value);
    }

    private int compareValue(Object fieldValue) {
        if (fieldValue instanceof BigDecimal bd) {
            return bd.compareTo(new BigDecimal(value));
        }
        if (fieldValue instanceof Integer i) {
            return i.compareTo(Integer.parseInt(value));
        }
        if (fieldValue instanceof LocalDateTime ldt) {
            LocalDate filterDate = LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
            return ldt.toLocalDate().compareTo(filterDate);
        }
        if (fieldValue instanceof LocalDate ld) {
            LocalDate filterDate = LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
            return ld.compareTo(filterDate);
        }
        return fieldValue.toString().compareTo(value);
    }
}