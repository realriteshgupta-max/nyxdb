package org.nyxdb.parser.core.parallel.logical;

import java.util.Set;

/**
 * Filter operator - applies WHERE conditions and join predicates.
 * Reduces the number of rows based on filter conditions.
 */
public class FilterOperator extends LogicalOperator {
    private final String filterCondition; // WHERE clause or JOIN ON condition
    private final FilterType filterType;

    public enum FilterType {
        WHERE_CLAUSE,
        JOIN_CONDITION,
        HAVING_CLAUSE
    }

    public FilterOperator(String operatorId, String filterCondition, FilterType filterType,
            Set<String> outputColumns, Set<String> involvedTables) {
        super(operatorId, outputColumns, involvedTables);
        this.filterCondition = filterCondition;
        this.filterType = filterType;
        // Estimate selectivity: typically assumes 10% of rows pass filter
        this.estimatedRowCount = 0; // Will be calculated based on input
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public FilterType getFilterType() {
        return filterType;
    }

    @Override
    public String getOperatorType() {
        return "FILTER";
    }

    @Override
    public String getDescription() {
        return "Filter(" + filterType.name() + ": " + filterCondition + ")";
    }

    @Override
    public boolean validate() {
        return filterCondition != null && !filterCondition.isEmpty() && !inputs.isEmpty();
    }
}
