package org.nyxdb.parser.core.parallel;

import org.nyxdb.parser.core.NyxParser;
import org.nyxdb.parser.core.models.TableInfo;
import org.nyxdb.parser.core.models.InsertInfo;
import org.nyxdb.parser.core.models.DatabaseInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses SQL queries and extracts information needed for parallel execution
 * planning.
 * Integrates with the existing NyxParser to extract table/database metadata.
 */
public class ParallelQueryParser {
    private static final Logger logger = LogManager.getLogger(ParallelQueryParser.class);

    /**
     * Parses a batch of SQL statements and extracts queries for parallel execution.
     *
     * @param sqlBatch a batch of SQL statements (separated by semicolons)
     * @return a list of Query objects with dependency metadata
     */

    // Map from parent query ID to list of child (subquery) IDs
    private final java.util.Map<String, java.util.List<String>> subqueryMap = new java.util.HashMap<>();
    // Map from query ID to Query object for quick lookup
    private final java.util.Map<String, Query> idQueryMap = new java.util.HashMap<>();
    // Map normalized subquery SQL -> generated subId to avoid duplicate extraction
    private final java.util.Map<String, String> subSqlToId = new java.util.HashMap<>();

    public List<Query> parseQueryBatch(String sqlBatch) {
        logger.info("Parsing query batch");
        List<Query> queries = new ArrayList<>();
        subqueryMap.clear();
        idQueryMap.clear();
        subSqlToId.clear();
        String[] statements = sqlBatch.split(";");
        logger.debug("Found {} potential statements in batch", statements.length);
        int[] queryId = { 0 };
        for (String statement : statements) {
            String trimmedSql = statement.trim();
            if (trimmedSql.isEmpty())
                continue;
            extractQueriesRecursive(trimmedSql, "Q" + (queryId[0]++), queries, queryId, null);
        }
        logger.info("Successfully parsed {} queries from batch", queries.size());
        return queries;
    }

    /**
     * Recursively extracts queries and subqueries, adding them to the list.
     */
    private void extractQueriesRecursive(String sql, String queryId, List<Query> queries, int[] queryIdCounter,
            String parentId) {
        // Look for subqueries anywhere in the SQL (FROM, WHERE, IN, etc.)
        List<String> subqueries = extractSubqueriesAnywhere(sql);
        List<String> subIds = new ArrayList<>();
        String rewrittenSql = sql;
        int subIdx = 0;
        for (String sub : subqueries) {
            String normSub = sub.replaceAll("\\s+", " ").trim();
            String subId;
            if (subSqlToId.containsKey(normSub)) {
                // already extracted this identical subquery elsewhere; reuse id
                subId = subSqlToId.get(normSub);
            } else {
                subId = queryId + "_sub" + (queryIdCounter[0]++);
                subSqlToId.put(normSub, subId);
                // recursively extract new subquery
                extractQueriesRecursive(sub, subId, queries, queryIdCounter, queryId);
            }
            subIds.add(subId);
            // Replace only the first occurrence of the subquery with the subId (as alias)
            rewrittenSql = replaceFirstSubquery(rewrittenSql, sub, subId);
            subIdx++;
        }
        Query query = parseQuery(rewrittenSql, queryId);
        if (query != null) {
            queries.add(query);
            // keep id->Query mapping for building execution trees
            idQueryMap.put(query.getId(), query);
            if (parentId != null) {
                subqueryMap.computeIfAbsent(parentId, k -> new java.util.ArrayList<>()).add(queryId);
            }
        }
    }

    // Helper to replace only the first occurrence of a subquery (with parentheses)
    // with a subId
    private String replaceFirstSubquery(String sql, String subquery, String subId) {
        // Normalize whitespace for matching
        String normSql = sql.replaceAll("\\s+", " ");
        String normSub = subquery.replaceAll("\\s+", " ");
        // Find the subquery in parentheses (allowing for whitespace)
        int idx = indexOfNormalized(normSql, normSub);
        if (idx == -1)
            return sql; // Not found
        // Now find the corresponding position in the original SQL
        int origIdx = findOriginalIndex(sql, subquery, idx);
        if (origIdx == -1)
            return sql;
        int endIdx = origIdx + subquery.length() + 1; // +1 for '('
        // Try to find the alias after the subquery
        int aliasStart = endIdx;
        while (aliasStart < sql.length() && Character.isWhitespace(sql.charAt(aliasStart)))
            aliasStart++;
        int aliasEnd = aliasStart;
        while (aliasEnd < sql.length()
                && (Character.isJavaIdentifierPart(sql.charAt(aliasEnd)) || sql.charAt(aliasEnd) == '_'))
            aliasEnd++;
        // Replace the subquery (with parentheses and alias) with the subId (as a table
        // reference)
        String before = sql.substring(0, origIdx - 1); // -1 to include '('
        String after = sql.substring(aliasEnd);
        return before + subId + after;
    }

    // Find the index of the normalized subquery in the normalized SQL
    private int indexOfNormalized(String normSql, String normSub) {
        return normSql.indexOf("(" + normSub);
    }

    // Find the corresponding index in the original SQL for the normalized match
    private int findOriginalIndex(String sql, String subquery, int normIdx) {
        // Try to find the first '(' followed by the subquery (ignoring whitespace)
        String normalized = subquery.trim().replaceAll("\\s+", " ");
        String quoted = java.util.regex.Pattern.quote(normalized).replace(" ", "\\\\s+");
        String pattern = "\\(\\s*" + quoted;
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(pattern, java.util.regex.Pattern.DOTALL)
                .matcher(sql);
        if (matcher.find()) {
            return matcher.start() + 1; // position after '('
        }
        return -1;
    }

    public java.util.Map<String, java.util.List<String>> getSubqueryMap() {
        return subqueryMap;
    }

    /**
     * Build execution order for queries so that subqueries (children) appear
     * before their parents. Returns a list of Query objects in execution order.
     */
    public List<Query> buildExecutionOrder(List<Query> queries) {
        java.util.List<Query> ordered = new ArrayList<>();
        // build id set and child id set
        java.util.Set<String> allIds = new java.util.HashSet<>();
        java.util.Set<String> childIds = new java.util.HashSet<>();
        for (Query q : queries) {
            allIds.add(q.getId());
        }
        for (java.util.List<String> children : subqueryMap.values()) {
            childIds.addAll(children);
        }

        // roots are queries that are not children of any other
        java.util.List<String> roots = new ArrayList<>();
        for (String id : allIds) {
            if (!childIds.contains(id))
                roots.add(id);
        }

        java.util.Set<String> visited = new java.util.HashSet<>();

        for (String root : roots) {
            dfsPostOrder(root, visited, ordered);
        }

        // There may be disconnected components (e.g., cycles or orphaned ids)
        // ensure all queries are included
        for (String id : allIds) {
            if (!visited.contains(id)) {
                dfsPostOrder(id, visited, ordered);
            }
        }

        return ordered;
    }

    private void dfsPostOrder(String id, java.util.Set<String> visited, java.util.List<Query> out) {
        if (visited.contains(id))
            return;
        visited.add(id);
        java.util.List<String> children = subqueryMap.get(id);
        if (children != null) {
            for (String c : children) {
                dfsPostOrder(c, visited, out);
            }
        }
        Query q = idQueryMap.get(id);
        if (q != null)
            out.add(q);
    }

    /**
     * Extracts subqueries from the FROM clause using a simple regex approach.
     * Only handles one level of nesting and assumes subqueries are wrapped in
     * parentheses.
     */
    private List<String> extractSubqueriesFromFromClause(String sql) {
        List<String> subqueries = new ArrayList<>();
        String upperSql = sql.toUpperCase();
        int fromIdx = upperSql.indexOf("FROM");
        if (fromIdx < 0)
            return subqueries;
        int endIdx = sql.length();
        int whereIdx = upperSql.indexOf("WHERE", fromIdx);
        int groupIdx = upperSql.indexOf("GROUP BY", fromIdx);
        int orderIdx = upperSql.indexOf("ORDER BY", fromIdx);
        if (whereIdx > 0)
            endIdx = Math.min(endIdx, whereIdx);
        if (groupIdx > 0)
            endIdx = Math.min(endIdx, groupIdx);
        if (orderIdx > 0)
            endIdx = Math.min(endIdx, orderIdx);
        String fromClause = sql.substring(fromIdx + 4, endIdx);
        // Look for subqueries in parentheses
        int idx = 0;
        while (idx < fromClause.length()) {
            if (fromClause.charAt(idx) == '(') {
                int depth = 1;
                int start = idx + 1;
                int end = start;
                while (end < fromClause.length() && depth > 0) {
                    if (fromClause.charAt(end) == '(')
                        depth++;
                    else if (fromClause.charAt(end) == ')')
                        depth--;
                    end++;
                }
                if (depth == 0) {
                    String sub = fromClause.substring(start, end - 1).trim();
                    // Only treat as subquery if it starts with SELECT
                    if (sub.toUpperCase().startsWith("SELECT")) {
                        subqueries.add(sub);
                    }
                    idx = end;
                } else {
                    break; // Unbalanced parentheses
                }
            } else {
                idx++;
            }
        }
        return subqueries;
    }

    /**
     * Extracts subqueries anywhere in the SQL string by scanning for
     * parenthesized sections that begin with SELECT. Handles nested
     * parentheses.
     */
    private List<String> extractSubqueriesAnywhere(String sql) {
        List<String> subqueries = new ArrayList<>();
        int idx = 0;
        while (idx < sql.length()) {
            if (sql.charAt(idx) == '(') {
                int depth = 1;
                int start = idx + 1;
                int end = start;
                while (end < sql.length() && depth > 0) {
                    if (sql.charAt(end) == '(')
                        depth++;
                    else if (sql.charAt(end) == ')')
                        depth--;
                    end++;
                }
                if (depth == 0) {
                    String inner = sql.substring(start, end - 1).trim();
                    if (inner.toUpperCase().startsWith("SELECT")) {
                        subqueries.add(inner);
                    }
                    // continue scanning inside the parentheses to find nested ones
                    List<String> nested = extractSubqueriesAnywhere(inner);
                    subqueries.addAll(nested);
                    idx = end;
                    continue;
                } else {
                    break; // unbalanced
                }
            }
            idx++;
        }
        return subqueries;
    }

    /**
     * Parses a single SQL statement and extracts query metadata.
     *
     * @param sql     the SQL statement
     * @param queryId an identifier for this query
     * @return a Query object with metadata
     */
    public Query parseQuery(String sql, String queryId) {
        sql = sql.trim();

        // Determine query type
        QueryType type = determineQueryType(sql);

        Query query = new Query(queryId, sql, type);

        try {
            // Use NyxParser to extract metadata
            NyxParser parser = NyxParser.extract(sql);

            // Extract table information
            extractTableMetadata(parser, query);

            // Extract database information
            extractDatabaseMetadata(parser, query);

            // Extract insert information
            extractInsertMetadata(parser, query);

            // Extract SELECT-specific metadata (joins, aggregates, orderby, groupby)
            if (type == QueryType.SELECT) {
                extractSelectMetadata(sql, query);
            } else if (type == QueryType.UPDATE) {
                extractUpdateMetadata(sql, query);
            } else if (type == QueryType.DELETE) {
                extractDeleteMetadata(sql, query);
            }

        } catch (Exception e) {
            // If parsing fails, return basic query without metadata
            logger.warn("Failed to parse query for dependency analysis: {}", e.getMessage());
            // Still try to extract basic metadata from SELECT statements
            if (type == QueryType.SELECT) {
                extractSelectMetadata(sql, query);
            }
        }

        return query;
    }

    /**
     * Determines the type of SQL statement.
     */
    private QueryType determineQueryType(String sql) {
        String upperSql = sql.toUpperCase().trim();

        if (upperSql.startsWith("SELECT")) {
            return QueryType.SELECT;
        } else if (upperSql.startsWith("CREATE TABLE")) {
            return QueryType.CREATE_TABLE;
        } else if (upperSql.startsWith("CREATE DATABASE")) {
            return QueryType.CREATE_DATABASE;
        } else if (upperSql.startsWith("DROP TABLE")) {
            return QueryType.DROP_TABLE;
        } else if (upperSql.startsWith("DROP DATABASE")) {
            return QueryType.DROP_DATABASE;
        } else if (upperSql.startsWith("INSERT INTO")) {
            return QueryType.INSERT;
        } else if (upperSql.startsWith("UPDATE")) {
            return QueryType.UPDATE;
        } else if (upperSql.startsWith("DELETE FROM")) {
            return QueryType.DELETE;
        } else if (upperSql.startsWith("ALTER TABLE")) {
            return QueryType.ALTER_TABLE;
        } else if (upperSql.startsWith("TRUNCATE TABLE")) {
            return QueryType.TRUNCATE_TABLE;
        } else {
            return QueryType.UNKNOWN;
        }
    }

    /**
     * Extracts table information from parsed data.
     */
    private void extractTableMetadata(NyxParser parser, Query query) {
        // Created tables
        for (TableInfo table : parser.getTables()) {
            String tableName = table.getName();
            if (tableName != null) {
                query.addWriteTable(tableName);
            }
        }

        // Dropped tables
        for (String droppedTable : parser.getDroppedTables()) {
            query.addWriteTable(droppedTable);
        }

        // Truncated tables
        for (String truncatedTable : parser.getTruncatedTables()) {
            query.addWriteTable(truncatedTable);
        }
    }

    /**
     * Extracts database information from parsed data.
     */
    private void extractDatabaseMetadata(NyxParser parser, Query query) {
        // Created/dropped databases
        for (DatabaseInfo db : parser.getDatabases()) {
            String dbName = db.getName();
            if (dbName != null) {
                query.addAffectedDatabase(dbName);
            }
        }

        // Dropped databases
        for (String droppedDb : parser.getDroppedDatabases()) {
            query.addAffectedDatabase(droppedDb);
        }
    }

    /**
     * Extracts insert information from parsed data.
     */
    private void extractInsertMetadata(NyxParser parser, Query query) {
        for (InsertInfo insertInfo : parser.getInserts()) {
            String tableName = insertInfo.getTableName();
            if (tableName != null) {
                query.addWriteTable(tableName);
            }
        }
    }

    /**
     * Extracts SELECT-specific metadata (tables, JOINs, aggregates, ORDER BY, GROUP
     * BY).
     */
    private void extractSelectMetadata(String sql, Query query) {
        String upperSql = sql.toUpperCase();

        // Extract tables from FROM clause
        extractTablesFromFromClause(sql, query);

        // Check for JOIN operations
        if (upperSql.contains("JOIN") || upperSql.contains("INNER JOIN") ||
                upperSql.contains("LEFT JOIN") || upperSql.contains("RIGHT JOIN") ||
                upperSql.contains("FULL JOIN")) {
            extractJoinMetadata(sql, query);
        }

        // Check for aggregate functions
        extractAggregateMetadata(sql, query);

        // Check for ORDER BY clause
        if (upperSql.contains("ORDER BY")) {
            query.setHasOrderBy(true);
            logger.debug("Query {} has ORDER BY clause", query.getId());
        }

        // Check for GROUP BY clause
        if (upperSql.contains("GROUP BY")) {
            query.setHasGroupBy(true);
            logger.debug("Query {} has GROUP BY clause", query.getId());
        }
    }

    /**
     * Extracts UPDATE-specific metadata.
     */
    private void extractUpdateMetadata(String sql, Query query) {
        String upperSql = sql.toUpperCase();

        // Extract table being updated
        int updateIdx = upperSql.indexOf("UPDATE");
        int setIdx = upperSql.indexOf("SET", updateIdx);
        if (updateIdx >= 0 && setIdx > updateIdx) {
            String tablePart = sql.substring(updateIdx + 6, setIdx).trim();
            String[] parts = tablePart.split("\\s+");
            if (parts.length > 0) {
                String table = parts[0].replace("`", "").replace("\"", "");
                query.addWriteTable(table);
                logger.debug("Query {} updates table: {}", query.getId(), table);
            }
        }

        // Check for WHERE clause with JOINs
        if (upperSql.contains("JOIN")) {
            extractJoinMetadata(sql, query);
        }
    }

    /**
     * Extracts DELETE-specific metadata.
     */
    private void extractDeleteMetadata(String sql, Query query) {
        String upperSql = sql.toUpperCase();

        // Extract table being deleted from
        int deleteIdx = upperSql.indexOf("DELETE FROM");
        int whereIdx = upperSql.indexOf("WHERE", deleteIdx);
        int fromIdx = upperSql.indexOf("FROM", deleteIdx);

        if (deleteIdx >= 0 && fromIdx > deleteIdx) {
            int endIdx = (whereIdx > 0 && whereIdx > fromIdx) ? whereIdx : sql.length();
            String tablePart = sql.substring(fromIdx + 4, endIdx).trim();
            String[] parts = tablePart.split("\\s+");
            if (parts.length > 0) {
                String table = parts[0].replace("`", "").replace("\"", "");
                query.addWriteTable(table);
                logger.debug("Query {} deletes from table: {}", query.getId(), table);
            }
        }
    }

    /**
     * Extracts tables from the FROM clause using simple regex matching.
     */
    private void extractTablesFromFromClause(String sql, Query query) {
        String upperSql = sql.toUpperCase();

        int fromIdx = upperSql.indexOf("FROM");
        if (fromIdx < 0) {
            return;
        }

        int whereIdx = upperSql.indexOf("WHERE", fromIdx);
        int joinIdx = upperSql.indexOf("JOIN", fromIdx);
        int groupIdx = upperSql.indexOf("GROUP BY", fromIdx);
        int orderIdx = upperSql.indexOf("ORDER BY", fromIdx);

        int endIdx = sql.length();
        if (whereIdx > 0)
            endIdx = Math.min(endIdx, whereIdx);
        if (joinIdx > 0)
            endIdx = Math.min(endIdx, joinIdx);
        if (groupIdx > 0)
            endIdx = Math.min(endIdx, groupIdx);
        if (orderIdx > 0)
            endIdx = Math.min(endIdx, orderIdx);

        String fromClause = sql.substring(fromIdx + 4, endIdx);

        // Simple table extraction (handles aliases)
        String[] tables = fromClause.split(",");
        for (String tableSpec : tables) {
            tableSpec = tableSpec.trim();
            // Get first identifier (actual table name before alias)
            String[] parts = tableSpec.split("\\s+");
            if (parts.length > 0) {
                String table = parts[0].replace("`", "").replace("\"", "");
                if (!table.isEmpty() && !table.equalsIgnoreCase("JOIN")) {
                    query.addReadTable(table);
                    logger.debug("Query {} reads from table: {}", query.getId(), table);
                }
            }
        }
    }

    /**
     * Extracts JOIN metadata from SQL.
     */
    private void extractJoinMetadata(String sql, Query query) {
        String upperSql = sql.toUpperCase();

        // Find all JOIN keywords and extract joined tables
        String[] joinTypes = { "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN", "JOIN" };

        for (String joinType : joinTypes) {
            int idx = upperSql.indexOf(joinType);
            while (idx >= 0) {
                int startIdx = idx + joinType.length();
                int nextJoinIdx = upperSql.indexOf("JOIN", startIdx);
                int onIdx = upperSql.indexOf("ON", startIdx);
                int whereIdx = upperSql.indexOf("WHERE", startIdx);

                int endIdx = sql.length();
                if (nextJoinIdx > 0)
                    endIdx = Math.min(endIdx, nextJoinIdx);
                if (onIdx > 0)
                    endIdx = Math.min(endIdx, onIdx);
                if (whereIdx > 0)
                    endIdx = Math.min(endIdx, whereIdx);

                if (endIdx <= startIdx) {
                    idx = upperSql.indexOf(joinType, idx + 1);
                    continue;
                }

                String tablePart = sql.substring(startIdx, endIdx).trim();
                String[] parts = tablePart.split("\\s+");
                if (parts.length > 0) {
                    String table = parts[0].replace("`", "").replace("\"", "");
                    if (!table.isEmpty()) {
                        query.addJoinedTable(table);
                        query.addReadTable(table);
                        logger.debug("Query {} JOINs with table: {}", query.getId(), table);
                    }
                }

                idx = upperSql.indexOf(joinType, idx + 1);
            }
        }
    }

    /**
     * Extracts aggregate function metadata.
     */
    private void extractAggregateMetadata(String sql, Query query) {
        String upperSql = sql.toUpperCase();

        String[] aggregates = { "COUNT", "SUM", "AVG", "MAX", "MIN", "GROUP_CONCAT" };

        for (String aggregate : aggregates) {
            if (upperSql.contains(aggregate)) {
                query.addAggregate(aggregate);
                logger.debug("Query {} uses aggregate: {}", query.getId(), aggregate);
            }
        }
    }

    /**
     * Parses multiple SQL files or batches and combines them into a single query
     * list.
     *
     * @param sqlStatements array of SQL statements
     * @return combined list of Query objects
     */
    public List<Query> parseMultipleStatements(String[] sqlStatements) {
        List<Query> allQueries = new ArrayList<>();
        int globalId = 0;

        for (String sql : sqlStatements) {
            if (sql != null && !sql.trim().isEmpty()) {
                Query query = parseQuery(sql.trim(), "Q" + (globalId++));
                allQueries.add(query);
            }
        }

        return allQueries;
    }

    /**
     * Generates a detailed analysis report for a list of parsed queries.
     *
     * @param queries the parsed queries
     * @return analysis report string
     */
    public String generateAnalysisReport(List<Query> queries) {
        StringBuilder report = new StringBuilder();
        report.append("\n==========================================\n");
        report.append("QUERY BATCH ANALYSIS REPORT\n");
        report.append("==========================================\n\n");

        report.append("Total Queries: ").append(queries.size()).append("\n");

        // Count by type
        java.util.Map<QueryType, Long> typeCount = queries.stream()
                .collect(java.util.stream.Collectors.groupingBy(Query::getType,
                        java.util.stream.Collectors.counting()));
        report.append("\nQuery Types:\n");
        typeCount.forEach((type, count) -> report.append("  ").append(type).append(": ").append(count).append("\n"));

        // Read-only vs write queries
        long readOnlyCount = queries.stream().filter(Query::isReadOnly).count();
        report.append("\nRead-Only Queries: ").append(readOnlyCount).append("\n");
        report.append("Write Queries: ").append(queries.size() - readOnlyCount).append("\n");

        // All affected tables
        java.util.Set<String> allTables = new java.util.HashSet<>();
        queries.forEach(q -> allTables.addAll(q.getReadTables()));
        queries.forEach(q -> allTables.addAll(q.getWriteTables()));
        report.append("\nTotal Unique Tables: ").append(allTables.size()).append("\n");
        if (!allTables.isEmpty()) {
            report.append("Tables: ").append(allTables).append("\n");
        }

        // All affected databases
        java.util.Set<String> allDatabases = new java.util.HashSet<>();
        queries.forEach(q -> allDatabases.addAll(q.getAffectedDatabases()));
        report.append("Total Unique Databases: ").append(allDatabases.size()).append("\n");
        if (!allDatabases.isEmpty()) {
            report.append("Databases: ").append(allDatabases).append("\n");
        }

        report.append("\nDetailed Query Information:\n");
        for (Query query : queries) {
            report.append("\n  Query ").append(query.getId()).append(":\n");
            report.append("    Type: ").append(query.getType()).append("\n");
            if (!query.getReadTables().isEmpty()) {
                report.append("    Reads: ").append(query.getReadTables()).append("\n");
            }
            if (!query.getWriteTables().isEmpty()) {
                report.append("    Writes: ").append(query.getWriteTables()).append("\n");
            }
            if (!query.getAffectedDatabases().isEmpty()) {
                report.append("    Affects DB: ").append(query.getAffectedDatabases()).append("\n");
            }
        }

        return report.toString();
    }
}
