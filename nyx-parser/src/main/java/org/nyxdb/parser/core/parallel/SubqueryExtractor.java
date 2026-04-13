package org.nyxdb.parser.core.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Extracts and parses nested SELECT queries (subqueries) from SQL statements.
 * Handles subqueries in FROM, WHERE, SELECT list, and JOIN clauses.
 */
public class SubqueryExtractor {
    private static final Logger logger = LogManager.getLogger(SubqueryExtractor.class);

    /**
     * Represents a subquery with its location and context.
     */
    public static class SubqueryInfo {
        public final String subquerySql;
        public final String alias;
        public final SubqueryType type;
        public final int depth; // Nesting level

        public enum SubqueryType {
            FROM_SUBQUERY, // Subquery in FROM clause
            WHERE_SUBQUERY, // Subquery in WHERE clause (scalar or EXISTS)
            JOIN_SUBQUERY, // Subquery in JOIN condition
            SELECT_SUBQUERY, // Subquery in SELECT list
            SET_OPERATION // UNION, INTERSECT, EXCEPT
        }

        public SubqueryInfo(String subquerySql, String alias, SubqueryType type, int depth) {
            this.subquerySql = subquerySql;
            this.alias = alias;
            this.type = type;
            this.depth = depth;
        }

        @Override
        public String toString() {
            return String.format("Subquery(type=%s, alias=%s, depth=%d)", type.name(), alias, depth);
        }
    }

    /**
     * Extracts all subqueries from a SQL statement.
     */
    public List<SubqueryInfo> extractSubqueries(String sql) {
        logger.debug("Extracting subqueries from: {}", sql);
        List<SubqueryInfo> subqueries = new ArrayList<>();

        // Extract FROM subqueries
        extractFromSubqueries(sql, subqueries);

        // Extract WHERE subqueries
        extractWhereSubqueries(sql, subqueries);

        // Extract JOIN subqueries
        extractJoinSubqueries(sql, subqueries);

        logger.info("Found {} subqueries", subqueries.size());
        return subqueries;
    }

    /**
     * Extracts subqueries from FROM clause.
     * Example: FROM (SELECT * FROM users) u
     */
    private void extractFromSubqueries(String sql, List<SubqueryInfo> subqueries) {
        String upperSql = sql.toUpperCase();
        int fromIdx = upperSql.indexOf("FROM");

        if (fromIdx < 0) {
            return;
        }

        int fromEnd = findNextClause(upperSql, fromIdx + 4);
        String fromSection = sql.substring(fromIdx + 4, fromEnd);

        // Pattern: (SELECT ... ) alias
        Pattern pattern = Pattern.compile("\\(\\s*SELECT\\s+.*?\\)\\s+(?:AS\\s+)?([a-zA-Z0-9_]+)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(fromSection);

        while (matcher.find()) {
            String alias = matcher.group(1);
            String subquery = extractBalancedParentheses(fromSection, matcher.start());
            if (subquery != null) {
                subqueries.add(new SubqueryInfo(subquery, alias, SubqueryInfo.SubqueryType.FROM_SUBQUERY, 1));
                logger.debug("Found FROM subquery with alias: {}", alias);
            }
        }
    }

    /**
     * Extracts subqueries from WHERE clause.
     * Examples: WHERE id IN (SELECT ...), WHERE EXISTS (SELECT ...)
     */
    private void extractWhereSubqueries(String sql, List<SubqueryInfo> subqueries) {
        String upperSql = sql.toUpperCase();
        int whereIdx = upperSql.indexOf("WHERE");

        if (whereIdx < 0) {
            return;
        }

        int whereEnd = findNextClause(upperSql, whereIdx + 5);
        String whereSection = sql.substring(whereIdx + 5, whereEnd);

        // Pattern: IN (SELECT ...), EXISTS (SELECT ...), = (SELECT ...)
        Pattern pattern = Pattern.compile("\\b(?:IN|EXISTS|NOT\\s+EXISTS|=)\\s*\\(\\s*SELECT",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(whereSection);

        int offset = 0;
        while (matcher.find()) {
            int parenStart = whereSection.indexOf('(', matcher.start());
            String subquery = extractBalancedParentheses(whereSection, parenStart);
            if (subquery != null) {
                subqueries.add(new SubqueryInfo(subquery, null, SubqueryInfo.SubqueryType.WHERE_SUBQUERY, 1));
                logger.debug("Found WHERE subquery: {}", subquery.substring(0, Math.min(50, subquery.length())));
                offset = parenStart + subquery.length();
            }
        }
    }

    /**
     * Extracts subqueries from JOIN condition.
     */
    private void extractJoinSubqueries(String sql, List<SubqueryInfo> subqueries) {
        String upperSql = sql.toUpperCase();
        String[] joinTypes = { "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN" };

        for (String joinType : joinTypes) {
            int idx = upperSql.indexOf(joinType);
            while (idx >= 0) {
                int joinStart = idx + joinType.length();
                int onIdx = upperSql.indexOf("ON", joinStart);
                int nextJoinIdx = upperSql.indexOf("JOIN", joinStart + 1);
                int whereIdx = upperSql.indexOf("WHERE", joinStart);

                if (onIdx > 0) {
                    int onEnd = Math.min(
                            nextJoinIdx > 0 ? nextJoinIdx : Integer.MAX_VALUE,
                            whereIdx > 0 ? whereIdx : Integer.MAX_VALUE);
                    String onSection = sql.substring(onIdx, Math.min(onEnd, sql.length()));

                    if (onSection.contains("(SELECT")) {
                        int selectParen = onSection.indexOf('(');
                        String subquery = extractBalancedParentheses(onSection, selectParen);
                        if (subquery != null) {
                            subqueries.add(new SubqueryInfo(subquery, null,
                                    SubqueryInfo.SubqueryType.JOIN_SUBQUERY, 1));
                            logger.debug("Found JOIN subquery");
                        }
                    }
                }

                idx = upperSql.indexOf(joinType, idx + 1);
            }
        }
    }

    /**
     * Extracts balanced parentheses content starting at given position.
     */
    private String extractBalancedParentheses(String text, int startIdx) {
        if (startIdx < 0 || startIdx >= text.length() || text.charAt(startIdx) != '(') {
            return null;
        }

        int balance = 0;
        int endIdx = startIdx;

        for (int i = startIdx; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                balance++;
            } else if (c == ')') {
                balance--;
                if (balance == 0) {
                    endIdx = i;
                    break;
                }
            }
        }

        if (balance == 0) {
            return text.substring(startIdx + 1, endIdx);
        }
        return null;
    }

    /**
     * Finds the next SQL clause after current position.
     */
    private int findNextClause(String sql, int startIdx) {
        String[] clauses = { "WHERE", "GROUP", "ORDER", "LIMIT", "UNION", "EXCEPT", "INTERSECT", ";" };
        int earliest = sql.length();

        for (String clause : clauses) {
            int idx = sql.indexOf(clause, startIdx);
            if (idx >= 0 && idx < earliest) {
                earliest = idx;
            }
        }

        return earliest;
    }

    /**
     * Checks if a query has nested subqueries.
     */
    public boolean hasSubqueries(String sql) {
        String upperSql = sql.toUpperCase();
        return (upperSql.contains("(SELECT") || upperSql.contains("(WITH")) &&
                !upperSql.equals(sql.trim()); // Not just the query itself
    }

    /**
     * Counts the maximum nesting depth of subqueries.
     */
    public int getMaxNestingDepth(String sql) {
        int maxDepth = 0;
        int currentDepth = 0;
        boolean inSelect = false;

        for (int i = 0; i < sql.length(); i++) {
            String remaining = sql.substring(i).toUpperCase();
            if (remaining.startsWith("SELECT")) {
                inSelect = true;
                currentDepth++;
                maxDepth = Math.max(maxDepth, currentDepth);
            } else if (remaining.startsWith(")") && inSelect) {
                currentDepth--;
            }
        }

        return maxDepth;
    }
}
