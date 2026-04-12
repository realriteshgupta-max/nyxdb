package org.nyxdb.parser.core.parallel.logical;

import java.util.Set;

/**
 * Table Scan operator - reads data from a table.
 * This is the leaf operator in a logical query plan.
 */
public class ScanOperator extends LogicalOperator {
    private final String tableName;
    private String pushdownPredicate; // For predicate push-down optimization

    public ScanOperator(String operatorId, String tableName, Set<String> outputColumns) {
        super(operatorId, outputColumns, Set.of(tableName));
        this.tableName = tableName;
        this.pushdownPredicate = null;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPushdownPredicate() {
        return pushdownPredicate;
    }

    public void setPushdownPredicate(String predicate) {
        this.pushdownPredicate = predicate;
    }

    @Override
    public String getOperatorType() {
        return "SCAN";
    }

    @Override
    public String getDescription() {
        String desc = "Scan(" + tableName + ")";
        if (pushdownPredicate != null) {
            desc += " [pushdown: " + pushdownPredicate + "]";
        }
        return desc;
    }

    @Override
    public boolean validate() {
        return tableName != null && !tableName.isEmpty() && outputColumns != null;
    }
}
