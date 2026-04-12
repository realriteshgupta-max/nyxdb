package org.nyxdb.parser.core.parallel.logical;

import java.util.HashSet;
import java.util.Set;

/**
 * Project operator - selects specific columns from the result.
 * Corresponds to SELECT column list in SQL.
 */
public class ProjectOperator extends LogicalOperator {
    private final Set<String> projectedColumns;
    private boolean isDistinct; // For DISTINCT clause

    public ProjectOperator(String operatorId, Set<String> projectedColumns,
            Set<String> involvedTables) {
        super(operatorId, projectedColumns, involvedTables);
        this.projectedColumns = new HashSet<>(projectedColumns);
        this.isDistinct = false;
    }

    public Set<String> getProjectedColumns() {
        return new HashSet<>(projectedColumns);
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean distinct) {
        this.isDistinct = distinct;
        // DISTINCT reduces row count
        if (distinct) {
            this.estimatedRowCount = Math.min(estimatedRowCount,
                    estimatedRowCount / Math.max(1, projectedColumns.size()));
        }
    }

    @Override
    public String getOperatorType() {
        return "PROJECT";
    }

    @Override
    public String getDescription() {
        String desc = "Project(columns=[" + String.join(", ", projectedColumns) + "]";
        if (isDistinct) {
            desc += ", DISTINCT";
        }
        desc += ")";
        return desc;
    }

    @Override
    public boolean validate() {
        return !projectedColumns.isEmpty() && !inputs.isEmpty();
    }
}
