package org.nyxdb.parser.core.parallel.logical;

import java.util.HashSet;
import java.util.Set;

/**
 * Aggregate operator - performs GROUP BY and aggregate functions.
 * Supports two-phase aggregation for distributed execution.
 */
public class AggregateOperator extends LogicalOperator {
    private final Set<String> groupByColumns;
    private final Set<AggregateFunction> aggregateFunctions;
    private boolean isPartialAggregate; // For distributed two-phase aggregation

    public static class AggregateFunction {
        public final String function; // COUNT, SUM, AVG, MAX, MIN, GROUP_CONCAT
        public final String column;
        public final String alias;

        public AggregateFunction(String function, String column, String alias) {
            this.function = function;
            this.column = column;
            this.alias = alias;
        }

        @Override
        public String toString() {
            return function + "(" + column + ") AS " + alias;
        }
    }

    public AggregateOperator(String operatorId, Set<String> groupByColumns,
            Set<AggregateFunction> aggregateFunctions,
            Set<String> outputColumns, Set<String> involvedTables) {
        super(operatorId, outputColumns, involvedTables);
        this.groupByColumns = new HashSet<>(groupByColumns);
        this.aggregateFunctions = new HashSet<>(aggregateFunctions);
        this.isPartialAggregate = false;

        // Estimate output rows: reduced by group by columns
        long groupCardinality = Math.max(1, 1000 / Math.max(1, groupByColumns.size()));
        this.estimatedRowCount = groupCardinality;
    }

    public Set<String> getGroupByColumns() {
        return new HashSet<>(groupByColumns);
    }

    public Set<AggregateFunction> getAggregateFunctions() {
        return new HashSet<>(aggregateFunctions);
    }

    public boolean isPartialAggregate() {
        return isPartialAggregate;
    }

    public void setPartialAggregate(boolean partial) {
        this.isPartialAggregate = partial;
    }

    /**
     * Checks if this aggregate can be parallelized using two-phase aggregation.
     * Most aggregates support this except for certain ones like STDDEV.
     */
    public boolean supportsDistributedAggregation() {
        for (AggregateFunction agg : aggregateFunctions) {
            // These aggregates can be computed in parallel
            if (!agg.function.matches("COUNT|SUM|AVG|MAX|MIN|GROUP_CONCAT")) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getOperatorType() {
        return "AGGREGATE";
    }

    @Override
    public String getDescription() {
        StringBuilder desc = new StringBuilder("Aggregate(");
        if (!groupByColumns.isEmpty()) {
            desc.append("GROUP BY ").append(groupByColumns).append(", ");
        }
        desc.append("functions=[");
        boolean first = true;
        for (AggregateFunction agg : aggregateFunctions) {
            if (!first)
                desc.append(", ");
            desc.append(agg);
            first = false;
        }
        desc.append("]");
        if (isPartialAggregate) {
            desc.append(" [PARTIAL]");
        }
        desc.append(")");
        return desc.toString();
    }

    @Override
    public boolean validate() {
        return !aggregateFunctions.isEmpty() && !inputs.isEmpty();
    }
}
