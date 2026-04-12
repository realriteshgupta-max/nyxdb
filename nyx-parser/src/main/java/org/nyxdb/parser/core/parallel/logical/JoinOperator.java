package org.nyxdb.parser.core.parallel.logical;

import java.util.Set;

/**
 * Join operator - combines rows from two tables based on a join condition.
 * Supports various join strategies for distributed execution.
 */
public class JoinOperator extends LogicalOperator {
    private final JoinType joinType;
    private final String joinCondition; // ON condition
    private final String leftTable;
    private final String rightTable;
    private JoinStrategy joinStrategy; // Determines physical execution strategy

    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        CROSS
    }

    public enum JoinStrategy {
        NESTED_LOOP, // Simple nested loop join
        HASH_JOIN, // Hash-based join (good for large tables)
        SORT_MERGE, // Sort-merge join (good for sorted input)
        BROADCAST, // Broadcast join (one side fits in memory)
        SHUFFLE, // Shuffle join (distributed hash join)
        SEMI_JOIN // Semi-join (for distributed optimization)
    }

    public JoinOperator(String operatorId, JoinType joinType, String joinCondition,
            String leftTable, String rightTable, Set<String> outputColumns,
            Set<String> involvedTables) {
        super(operatorId, outputColumns, involvedTables);
        this.joinType = joinType;
        this.joinCondition = joinCondition;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinStrategy = JoinStrategy.NESTED_LOOP; // Default, will be optimized
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public String getJoinCondition() {
        return joinCondition;
    }

    public String getLeftTable() {
        return leftTable;
    }

    public String getRightTable() {
        return rightTable;
    }

    public JoinStrategy getJoinStrategy() {
        return joinStrategy;
    }

    public void setJoinStrategy(JoinStrategy strategy) {
        this.joinStrategy = strategy;
    }

    /**
     * Determines if this join can be executed in parallel with other joins.
     */
    public boolean isParallelizable() {
        // INNER and CROSS joins can be parallelized
        // LEFT/RIGHT/FULL joins have dependencies on outer table
        return joinType == JoinType.INNER || joinType == JoinType.CROSS;
    }

    /**
     * Estimates if this join is suitable for broadcast strategy
     * (when one side is small enough to fit in memory).
     */
    public boolean isBroadcastCandidate(long smallTableEstimatedSize) {
        // If one table is small enough, use broadcast
        return smallTableEstimatedSize < 100_000_000; // 100MB threshold
    }

    @Override
    public String getOperatorType() {
        return "JOIN";
    }

    @Override
    public String getDescription() {
        return String.format("Join(%s %s ON %s, strategy=%s)",
                joinType.name(), leftTable + " <-> " + rightTable,
                joinCondition, joinStrategy.name());
    }

    @Override
    public boolean validate() {
        return joinCondition != null && !joinCondition.isEmpty() &&
                leftTable != null && rightTable != null &&
                inputs.size() == 2; // Join must have exactly 2 inputs
    }
}
