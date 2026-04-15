package com.prestobr.estoque.domain.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
public class ParquetUtils {

    private ParquetUtils() {}

    public static String getString(GenericRecord record, String field) {
        try {
            Object value = record.get(field);
            return value != null ? value.toString() : null;
        } catch (Exception e) {
            return null;
        }
    }

    public static Integer getInteger(GenericRecord record, String field) {
        try {
            Object value = record.get(field);
            if (value == null) return null;
            if (value instanceof Integer) return (Integer) value;
            if (value instanceof Long) return ((Long) value).intValue();
            if (value instanceof Number) return ((Number) value).intValue();
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            return null;
        }
    }

    public static Long getLong(GenericRecord record, String field) {
        try {
            Object value = record.get(field);
            if (value == null) return null;
            if (value instanceof Long) return (Long) value;
            if (value instanceof Number) return ((Number) value).longValue();
            return Long.parseLong(value.toString());
        } catch (Exception e) {
            return null;
        }
    }

    public static BigDecimal getBigDecimal(GenericRecord record, String field) {
        try {
            Object value = record.get(field);
            if (value == null) return null;

            // pega schema do campo
            Schema.Field schemaField = record.getSchema().getField(field);
            if (schemaField == null) return null;

            Schema fieldSchema = schemaField.schema();

            // trata union (nullable)
            if (fieldSchema.getType() == Schema.Type.UNION) {
                for (Schema s : fieldSchema.getTypes()) {
                    if (s.getType() != Schema.Type.NULL) {
                        fieldSchema = s;
                        break;
                    }
                }
            }

            // pega logical type
            LogicalTypes.Decimal decimalType =
                    (LogicalTypes.Decimal) fieldSchema.getLogicalType();

            int scale = decimalType.getScale();

            // converte FIXED
            if (value instanceof GenericData.Fixed fixed) {
                byte[] bytes = fixed.bytes();
                return new BigDecimal(new BigInteger(bytes), scale);
            }

            return new BigDecimal(value.toString());

        } catch (Exception e) {
            return null;
        }
    }

    public static Boolean getBoolean(GenericRecord record, String field) {
        try {
            Object value = record.get(field);
            if (value == null) return null;
            if (value instanceof Boolean) return (Boolean) value;
            String strValue = value.toString().toLowerCase();
            return "true".equals(strValue) || "s".equals(strValue) || "1".equals(strValue);
        } catch (Exception e) {
            return null;
        }
    }

    public static LocalDateTime getLocalDateTime(GenericRecord record, String field) {
        try {
            Object value = record.get(field);
            if (value == null) return null;

            if (value instanceof Long) {
                long micros = (Long) value;
                long millis = micros / 1000;
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
            }

            if (value instanceof CharSequence) {
                return LocalDateTime.parse(value.toString());
            }

            return null;
        } catch (Exception e) {
            log.trace("Erro ao converter campo {} para LocalDateTime: {}", field, e.getMessage());
            return null;
        }
    }
}