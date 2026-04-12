package org.nyxdb.parser.core.parallel.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Sort operator - sorts rows based on ORDER BY columns.
 * Can be expensive in distributed systems.
 */
public class SortOperator extends LogicalOperator {
    private final List<SortColumn> sortColumns;
    private boolean isTopK; // Optimization for LIMIT
    private long limitValue; // For LIMIT clause

    public static class SortColumn {
        public final String columnName;
        public final SortDirection direction;

        public SortColumn(String columnName, SortDirection direction) {
            this.columnName = columnName;
            this.direction = direction;
        }

        public enum SortDirection {
            ASC,
            DESC
        }

        @Override
        public String toString() {
            return columnName + " " + direction.name();
        }
    }

    public SortOperator(String operatorId, List<SortColumn> sortColumns,
            Set<String> outputColumns, Set<String> involvedTables) {
        super(operatorId, outputColumns, involvedTables);
        this.sortColumns = new ArrayList<>(sortColumns);
        this.isTopK = false;
        this.limitValue = 0;
        // Sorting is expensive but doesn't change row count
    }

    public List<SortColumn> getSortColumns() {
        return new ArrayList<>(sortColumns);
    }

    public boolean isTopK() {
        return isTopK;
    }

    public void setTopK(boolean topK, long limit) {
        this.isTopK = topK;
        this.limitValue = limit;
    }

    public long getLimitValue() {
        return limitValue;
    }

    /**
     * Checks if sorting can be pushed down to source tables.
     */
    public boolean canPushdownSort() {
        // Can only push down if there's a single table source
        return involvedTables.size() == 1;
    }

    @Override
    public String getOperatorType() {
        return "SORT";
    }

    @Override
    public String getDescription() {
        String desc = "Sort(";
        boolean first = true;
        for (SortColumn col : sortColumns) {
            if (!first)
                desc += ", ";
            desc += col;
            first = false;
        }
        desc += ")";
        if (isTopK) {
            desc += " [LIMIT " + limitValue + "]";
        }
        return desc;
    }

    @Override
    public boolean validate() {
        return !sortColumns.isEmpty() && !inputs.isEmpty();
    }
}
