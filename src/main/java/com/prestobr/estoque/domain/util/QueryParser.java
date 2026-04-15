package com.prestobr.estoque.domain.util;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
public class QueryParser {

    private List<String> selectColumns = new ArrayList<>();
    private List<QueryFilter> filters = new ArrayList<>();
    private String groupBy;
    private int limit = 100; // default

    public QueryParser(String query) {
        parse(query.trim());
    }

    private void parse(String query) {
        String upperQuery = query.toUpperCase();

        // Parse SELECT
        int selectIdx = upperQuery.indexOf("SELECT");
        int fromIdx = upperQuery.indexOf("FROM");

        if (selectIdx >= 0 && fromIdx > selectIdx) {
            String selectPart = query.substring(selectIdx + 6, fromIdx).trim();
            for (String col : selectPart.split(",")) {
                selectColumns.add(col.trim());
            }
        } else {
            selectColumns.add("*");
        }

        // Parse WHERE
        int whereIdx = upperQuery.indexOf("WHERE");
        int groupByIdx = upperQuery.indexOf("GROUP BY");
        int limitIdx = upperQuery.indexOf("LIMIT");

        if (whereIdx >= 0) {
            int endWhere = groupByIdx > 0 ? groupByIdx : (limitIdx > 0 ? limitIdx : query.length());
            String wherePart = query.substring(whereIdx + 5, endWhere).trim();
            parseFilters(wherePart);
        }

        // Parse LIMIT
        if (limitIdx >= 0) {
            String limitPart = query.substring(limitIdx + 5).trim().split("\\s+")[0];
            try {
                limit = Integer.parseInt(limitPart);
            } catch (NumberFormatException ignored) {}
        }

        // Parse GROUP BY
        if (groupByIdx >= 0) {
            int endGroup = limitIdx > 0 ? limitIdx : query.length();
            groupBy = query.substring(groupByIdx + 8, endGroup).trim();
        }
    }

    private void parseFilters(String wherePart) {
        // Suporta AND apenas por simplicidade
        String[] conditions = wherePart.split("(?i)\\s+AND\\s+");

        for (String condition : conditions) {
            condition = condition.trim();

            // Padrões: field = 'value', field > 'value', field < 'value', etc.
            Pattern pattern = Pattern.compile("(\\w+)\\s*(=|!=|<>|>=|<=|>|<|LIKE)\\s*'?([^']+)'?", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(condition);

            if (matcher.find()) {
                String field = matcher.group(1);
                String operator = matcher.group(2).toUpperCase();
                String value = matcher.group(3).replace("'", "").trim();
                filters.add(new QueryFilter(field, operator, value));
            }
        }
    }

    public boolean hasAggregation() {
        for (String col : selectColumns) {
            String upper = col.toUpperCase();
            if (upper.startsWith("SUM(") || upper.startsWith("COUNT(") ||
                    upper.startsWith("AVG(") || upper.startsWith("MIN(") || upper.startsWith("MAX(")) {
                return true;
            }
        }
        return false;
    }

    public String getGroupByColumn() {
        return groupBy;
    }
}