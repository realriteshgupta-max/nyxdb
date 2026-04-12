package org.nyxdb.parser.core.parallel.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Base class for all logical query plan operators.
 * Represents a step in the logical query execution tree.
 * Each operator transforms its input data according to its operation type.
 */
public abstract class LogicalOperator {
    protected final String operatorId;
    protected final List<LogicalOperator> inputs;
    protected final Set<String> outputColumns;
    protected final Set<String> involvedTables;
    protected long estimatedRowCount;
    protected long estimatedCost;

    public LogicalOperator(String operatorId, Set<String> outputColumns, Set<String> involvedTables) {
        this.operatorId = operatorId;
        this.outputColumns = outputColumns;
        this.involvedTables = involvedTables;
        this.inputs = new ArrayList<>();
        this.estimatedRowCount = 1000; // Default estimate
        this.estimatedCost = 1000;
    }

    /**
     * Adds an input operator to this operator.
     */
    public void addInput(LogicalOperator input) {
        this.inputs.add(input);
    }

    /**
     * Gets the list of input operators.
     */
    public List<LogicalOperator> getInputs() {
        return new ArrayList<>(inputs);
    }

    /**
     * Gets the operator type.
     */
    public abstract String getOperatorType();

    /**
     * Gets the operator description for debugging.
     */
    public abstract String getDescription();

    /**
     * Validates the operator configuration.
     */
    public abstract boolean validate();

    // Getters
    public String getOperatorId() {
        return operatorId;
    }

    public Set<String> getOutputColumns() {
        return outputColumns;
    }

    public Set<String> getInvolvedTables() {
        return involvedTables;
    }

    public long getEstimatedRowCount() {
        return estimatedRowCount;
    }

    public long getEstimatedCost() {
        return estimatedCost;
    }

    // Setters for optimization
    public void setEstimatedRowCount(long rowCount) {
        this.estimatedRowCount = rowCount;
    }

    public void setEstimatedCost(long cost) {
        this.estimatedCost = cost;
    }

    @Override
    public String toString() {
        return String.format("%s[id=%s, rows=%d, cost=%d]",
                getOperatorType(), operatorId, estimatedRowCount, estimatedCost);
    }
}
