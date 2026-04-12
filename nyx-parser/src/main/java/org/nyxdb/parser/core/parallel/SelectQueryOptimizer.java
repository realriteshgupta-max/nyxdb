package org.nyxdb.parser.core.parallel;

import org.nyxdb.parser.core.parallel.logical.AggregateOperator;
import org.nyxdb.parser.core.parallel.logical.AggregateOperator.AggregateFunction;
import org.nyxdb.parser.core.parallel.logical.FilterOperator;
import org.nyxdb.parser.core.parallel.logical.JoinOperator;
import org.nyxdb.parser.core.parallel.logical.LogicalOperator;
import org.nyxdb.parser.core.parallel.logical.LogicalQueryPlan;
import org.nyxdb.parser.core.parallel.logical.ProjectOperator;
import org.nyxdb.parser.core.parallel.logical.ScanOperator;
import org.nyxdb.parser.core.parallel.logical.SortOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Optimizes SELECT queries for distributed parallel execution.
 * Builds logical query plans and applies optimization rules.
 */
public class SelectQueryOptimizer {
    private static final Logger logger = LogManager.getLogger(SelectQueryOptimizer.class);

    /**
     * Builds a logical query plan from a SELECT statement.
     */
    public LogicalQueryPlan buildLogicalPlan(Query query) {
        logger.info("Building logical plan for query: {}", query.getId());

        if (query.getType() != QueryType.SELECT) {
            logger.warn("Query {} is not SELECT type", query.getId());
            return null;
        }

        String sql = query.getSql();

        // Start from the bottom of the operator tree (table scans)
        LogicalOperator currentOperator = buildScans(query);

        // Apply WHERE filters
        if (sql.toUpperCase().contains("WHERE")) {
            FilterOperator filter = buildWhereFilter(sql, query);
            if (filter != null) {
                filter.addInput(currentOperator);
                currentOperator = filter;
                logger.debug("Added WHERE filter operator");
            }
        }

        // Apply JOINs
        if (query.hasJoin()) {
            JoinOperator join = buildJoins(sql, query);
            if (join != null) {
                currentOperator = buildJoinTree(join, currentOperator, query);
                logger.debug("Added JOIN operators");
            }
        }

        // Apply GROUP BY and aggregates
        if (query.hasGroupBy() || !query.getAggregates().isEmpty()) {
            AggregateOperator aggregate = buildAggregation(sql, query);
            if (aggregate != null) {
                aggregate.addInput(currentOperator);
                currentOperator = aggregate;
                logger.debug("Added AGGREGATE operator");
            }
        }

        // Apply HAVING filter (after aggregation)
        if (sql.toUpperCase().contains("HAVING")) {
            FilterOperator having = buildHavingFilter(sql, query);
            if (having != null) {
                having.addInput(currentOperator);
                currentOperator = having;
                logger.debug("Added HAVING filter operator");
            }
        }

        // Apply ORDER BY
        if (query.hasOrderBy()) {
            SortOperator sort = buildSort(sql, query);
            if (sort != null) {
                sort.addInput(currentOperator);
                currentOperator = sort;
                logger.debug("Added SORT operator");
            }
        }

        // Apply column projection (final SELECT list)
        ProjectOperator project = buildProjection(sql, query);
        if (project != null) {
            project.addInput(currentOperator);
            currentOperator = project;
            logger.debug("Added PROJECT operator");
        }

        LogicalQueryPlan plan = new LogicalQueryPlan(query.getId(), sql, currentOperator);

        if (!plan.validate()) {
            logger.warn("Logical plan validation failed for query: {}", query.getId());
        }

        logger.info("Logical plan built: {}", plan);
        return plan;
    }

    /**
     * Builds table scan operators for the FROM clause.
     */
    private LogicalOperator buildScans(Query query) {
        Set<String> tables = query.getReadTables();
        if (tables.isEmpty()) {
            logger.warn("No tables found in query");
            return null;
        }

        if (tables.size() == 1) {
            String table = tables.iterator().next();
            ScanOperator scan = new ScanOperator("scan_" + table, table,
                    extractColumns(query.getSql()));
            logger.debug("Created SCAN operator for table: {}", table);
            return scan;
        }

        // Multiple tables (from comma-separated FROM) - create scan for each
        // and combine with cross product (will be optimized by joins)
        List<LogicalOperator> scans = new ArrayList<>();
        for (String table : tables) {
            ScanOperator scan = new ScanOperator("scan_" + table, table,
                    extractColumns(query.getSql()));
            scans.add(scan);
        }

        // This is a simplification - actual implementation would handle cross product
        return scans.get(0);
    }

    /**
     * Builds WHERE filter operator.
     */
    private FilterOperator buildWhereFilter(String sql, Query query) {
        String whereClause = extractWhereClause(sql);
        if (whereClause == null || whereClause.isEmpty()) {
            return null;
        }

        return new FilterOperator("filter_where", whereClause, FilterOperator.FilterType.WHERE_CLAUSE,
                extractColumns(sql), query.getReadTables());
    }

    /**
     * Builds JOIN operators from SQL.
     */
    private JoinOperator buildJoins(String sql, Query query) {
        String upperSql = sql.toUpperCase();

        // Find INNER JOIN pattern
        Pattern joinPattern = Pattern.compile(
                "(INNER\\s+JOIN|LEFT\\s+(?:OUTER\\s+)?JOIN|RIGHT\\s+(?:OUTER\\s+)?JOIN|FULL\\s+(?:OUTER\\s+)?JOIN|CROSS\\s+JOIN)\\s+"
                        +
                        "([a-zA-Z0-9_]+)\\s+(?:(?:AS\\s+)?([a-zA-Z0-9_]+)\\s+)?ON\\s+(.+?)(?=WHERE|GROUP|ORDER|LIMIT|$)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

        Matcher matcher = joinPattern.matcher(sql);
        if (matcher.find()) {
            String joinTypeStr = matcher.group(1).toUpperCase();
            String rightTable = matcher.group(2);
            String joinCondition = matcher.group(4);

            JoinOperator.JoinType joinType = parseJoinType(joinTypeStr);

            // Assume left table is from FROM clause
            Set<String> readTables = query.getReadTables();
            String leftTable = readTables.stream().findFirst().orElse("unknown");

            return new JoinOperator("join_1", joinType, joinCondition, leftTable, rightTable,
                    extractColumns(sql), query.getReadTables());
        }

        return null;
    }

    /**
     * Builds the complete join tree.
     */
    private LogicalOperator buildJoinTree(JoinOperator join, LogicalOperator leftOperator, Query query) {
        // Create scan for right table
        ScanOperator rightScan = new ScanOperator("scan_" + join.getRightTable(),
                join.getRightTable(),
                extractColumns(query.getSql()));

        join.addInput(leftOperator);
        join.addInput(rightScan);

        logger.debug("Built join tree: {} JOIN {}", join.getLeftTable(), join.getRightTable());
        return join;
    }

    /**
     * Builds aggregation operator.
     */
    private AggregateOperator buildAggregation(String sql, Query query) {
        Set<String> groupByColumns = extractGroupByColumns(sql);
        Set<AggregateFunction> aggregates = new HashSet<>();

        for (String agg : query.getAggregates()) {
            String column = "*"; // Typically COUNT(*), but could be specific column
            aggregates.add(new AggregateFunction(agg, column, agg + "_" + column));
        }

        if (aggregates.isEmpty() && groupByColumns.isEmpty()) {
            return null;
        }

        return new AggregateOperator("agg_1", groupByColumns, aggregates,
                extractColumns(sql), query.getReadTables());
    }

    /**
     * Builds HAVING filter operator.
     */
    private FilterOperator buildHavingFilter(String sql, Query query) {
        String havingClause = extractHavingClause(sql);
        if (havingClause == null || havingClause.isEmpty()) {
            return null;
        }

        return new FilterOperator("filter_having", havingClause, FilterOperator.FilterType.HAVING_CLAUSE,
                extractColumns(sql), query.getReadTables());
    }

    /**
     * Builds sort operator.
     */
    private SortOperator buildSort(String sql, Query query) {
        List<SortOperator.SortColumn> sortColumns = extractOrderByColumns(sql);
        if (sortColumns.isEmpty()) {
            return null;
        }

        return new SortOperator("sort_1", sortColumns, extractColumns(sql), query.getReadTables());
    }

    /**
     * Builds projection operator for SELECT columns.
     */
    private ProjectOperator buildProjection(String sql, Query query) {
        Set<String> selectedColumns = extractSelectColumns(sql);

        boolean isDistinct = sql.toUpperCase().contains("SELECT DISTINCT");

        ProjectOperator project = new ProjectOperator("project_final", selectedColumns,
                query.getReadTables());
        project.setDistinct(isDistinct);

        return project;
    }

    // ======================== SQL Extraction Helper Methods
    // ========================

    private String extractWhereClause(String sql) {
        int whereIdx = sql.toUpperCase().indexOf("WHERE");
        if (whereIdx < 0)
            return null;

        int endIdx = findNextClauseIndex(sql, whereIdx + 5);
        return sql.substring(whereIdx + 5, endIdx).trim();
    }

    private String extractHavingClause(String sql) {
        int havingIdx = sql.toUpperCase().indexOf("HAVING");
        if (havingIdx < 0)
            return null;

        int endIdx = findNextClauseIndex(sql, havingIdx + 6);
        return sql.substring(havingIdx + 6, endIdx).trim();
    }

    private Set<String> extractGroupByColumns(String sql) {
        Set<String> columns = new HashSet<>();
        int groupIdx = sql.toUpperCase().indexOf("GROUP BY");
        if (groupIdx < 0)
            return columns;

        int endIdx = findNextClauseIndex(sql, groupIdx + 8);
        String groupSection = sql.substring(groupIdx + 8, endIdx).trim();

        for (String col : groupSection.split(",")) {
            columns.add(col.trim());
        }
        return columns;
    }

    private List<SortOperator.SortColumn> extractOrderByColumns(String sql) {
        List<SortOperator.SortColumn> columns = new ArrayList<>();
        int orderIdx = sql.toUpperCase().indexOf("ORDER BY");
        if (orderIdx < 0)
            return columns;

        int endIdx = findNextClauseIndex(sql, orderIdx + 8);
        String orderSection = sql.substring(orderIdx + 8, endIdx).trim();

        for (String col : orderSection.split(",")) {
            String col_trimmed = col.trim();
            SortOperator.SortColumn.SortDirection direction = col_trimmed.toUpperCase().endsWith("DESC")
                    ? SortOperator.SortColumn.SortDirection.DESC
                    : SortOperator.SortColumn.SortDirection.ASC;
            String colName = col_trimmed.replaceAll("(ASC|DESC)", "").trim();
            columns.add(new SortOperator.SortColumn(colName, direction));
        }
        return columns;
    }

    private Set<String> extractSelectColumns(String sql) {
        Set<String> columns = new HashSet<>();
        int selectIdx = sql.toUpperCase().indexOf("SELECT");
        int fromIdx = sql.toUpperCase().indexOf("FROM", selectIdx);

        if (selectIdx >= 0 && fromIdx > selectIdx) {
            String selectSection = sql.substring(selectIdx + 6, fromIdx).trim();
            if (selectSection.equalsIgnoreCase("*")) {
                columns.add("*");
            } else {
                for (String col : selectSection.split(",")) {
                    columns.add(col.trim());
                }
            }
        }
        return columns;
    }

    private Set<String> extractColumns(String sql) {
        Set<String> columns = new HashSet<>();
        columns.add("*"); // Default to all columns
        return columns;
    }

    private int findNextClauseIndex(String sql, int startIdx) {
        String upperSql = sql.toUpperCase();
        String[] clauses = { "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", ";" };
        int earliest = sql.length();

        for (String clause : clauses) {
            int idx = upperSql.indexOf(clause, startIdx);
            if (idx >= 0 && idx < earliest) {
                earliest = idx;
            }
        }
        return earliest;
    }

    private JoinOperator.JoinType parseJoinType(String joinTypeStr) {
        if (joinTypeStr.contains("LEFT"))
            return JoinOperator.JoinType.LEFT;
        if (joinTypeStr.contains("RIGHT"))
            return JoinOperator.JoinType.RIGHT;
        if (joinTypeStr.contains("FULL"))
            return JoinOperator.JoinType.FULL;
        if (joinTypeStr.contains("CROSS"))
            return JoinOperator.JoinType.CROSS;
        return JoinOperator.JoinType.INNER;
    }
}
