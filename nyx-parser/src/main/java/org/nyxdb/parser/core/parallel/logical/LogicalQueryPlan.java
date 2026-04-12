package org.nyxdb.parser.core.parallel.logical;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a logical query execution plan as a tree of operators.
 * This is generated from the SELECT statement and serves as input to
 * optimization.
 */
public class LogicalQueryPlan {
    private static final Logger logger = LogManager.getLogger(LogicalQueryPlan.class);

    private final String queryId;
    private final String originalSql;
    private final LogicalOperator rootOperator;
    private final List<LogicalOperator> allOperators;
    private final Set<String> involvedTables;
    private final Set<String> subqueries; // Nested SELECT queries

    public LogicalQueryPlan(String queryId, String originalSql, LogicalOperator rootOperator) {
        this.queryId = queryId;
        this.originalSql = originalSql;
        this.rootOperator = rootOperator;
        this.allOperators = new ArrayList<>();
        this.involvedTables = new HashSet<>();
        this.subqueries = new HashSet<>();

        // Collect all operators from tree
        collectOperators(rootOperator);
    }

    /**
     * Recursively collects all operators from the operator tree.
     */
    private void collectOperators(LogicalOperator operator) {
        if (operator == null) {
            return;
        }

        allOperators.add(operator);
        involvedTables.addAll(operator.getInvolvedTables());

        for (LogicalOperator input : operator.getInputs()) {
            collectOperators(input);
        }
    }

    /**
     * Validates the entire query plan.
     */
    public boolean validate() {
        for (LogicalOperator op : allOperators) {
            if (!op.validate()) {
                logger.warn("Operator validation failed for: {}", op.getDescription());
                return false;
            }
        }
        return true;
    }

    /**
     * Generates a visual representation of the operator tree.
     */
    public String printTree() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nLogical Query Plan: ").append(queryId).append("\n");
        sb.append("==========================================\n");
        sb.append("SQL: ").append(originalSql).append("\n");
        sb.append("Operators:\n");
        printOperatorTree(rootOperator, 0, sb);
        sb.append("Total Operators: ").append(allOperators.size()).append("\n");
        sb.append("Involved Tables: ").append(involvedTables).append("\n");
        return sb.toString();
    }

    /**
     * Recursively prints the operator tree with indentation.
     */
    private void printOperatorTree(LogicalOperator operator, int depth, StringBuilder sb) {
        if (operator == null) {
            return;
        }

        String indent = "│   ".repeat(Math.max(0, depth));
        String branch = depth > 0 ? "├─ " : "";

        sb.append(indent).append(branch).append(operator.toString()).append("\n");
        sb.append(indent).append("   ├─ Description: ").append(operator.getDescription()).append("\n");
        sb.append(indent).append("   └─ Estimated Cost: ").append(operator.getEstimatedCost()).append("\n");

        for (LogicalOperator input : operator.getInputs()) {
            printOperatorTree(input, depth + 1, sb);
        }
    }

    // Getters
    public String getQueryId() {
        return queryId;
    }

    public String getOriginalSql() {
        return originalSql;
    }

    public LogicalOperator getRootOperator() {
        return rootOperator;
    }

    public List<LogicalOperator> getAllOperators() {
        return new ArrayList<>(allOperators);
    }

    public Set<String> getInvolvedTables() {
        return new HashSet<>(involvedTables);
    }

    public Set<String> getSubqueries() {
        return new HashSet<>(subqueries);
    }

    public void addSubquery(String subquery) {
        subqueries.add(subquery);
    }

    /**
     * Counts operators of a specific type.
     */
    public int countOperatorType(String operatorType) {
        return (int) allOperators.stream()
                .filter(op -> operatorType.equals(op.getOperatorType()))
                .count();
    }

    /**
     * Gets the estimated total cost of the plan.
     */
    public long getTotalEstimatedCost() {
        return allOperators.stream()
                .mapToLong(LogicalOperator::getEstimatedCost)
                .sum();
    }

    /**
     * Gets join operators for optimization.
     */
    public List<JoinOperator> getJoinOperators() {
        List<JoinOperator> joins = new ArrayList<>();
        for (LogicalOperator op : allOperators) {
            if (op instanceof JoinOperator) {
                joins.add((JoinOperator) op);
            }
        }
        return joins;
    }

    /**
     * Checks if this query has subqueries.
     */
    public boolean hasSubqueries() {
        return !subqueries.isEmpty();
    }

    @Override
    public String toString() {
        return "LogicalQueryPlan{" +
                "queryId='" + queryId + '\'' +
                ", operators=" + allOperators.size() +
                ", tables=" + involvedTables +
                ", totalCost=" + getTotalEstimatedCost() +
                '}';
    }
}
