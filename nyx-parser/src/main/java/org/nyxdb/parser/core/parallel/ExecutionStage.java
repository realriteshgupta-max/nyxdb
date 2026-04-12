package org.nyxdb.parser.core.parallel;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a stage of parallel query execution.
 * Queries in the same stage can be executed in parallel.
 */
public class ExecutionStage {
    private final int stageNumber;
    private final List<Query> queries;
    private long executionTimeMs;
    private boolean completed;

    public ExecutionStage(int stageNumber) {
        this.stageNumber = stageNumber;
        this.queries = new ArrayList<>();
        this.executionTimeMs = 0;
        this.completed = false;
    }

    public int getStageNumber() {
        return stageNumber;
    }

    public List<Query> getQueries() {
        return queries;
    }

    public void addQuery(Query query) {
        queries.add(query);
    }

    public void addQueries(List<Query> queriesToAdd) {
        queries.addAll(queriesToAdd);
    }

    public int getQueryCount() {
        return queries.size();
    }

    public long getExecutionTimeMs() {
        return executionTimeMs;
    }

    public void setExecutionTimeMs(long timeMs) {
        this.executionTimeMs = timeMs;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void markCompleted() {
        this.completed = true;
    }

    @Override
    public String toString() {
        return "ExecutionStage{" +
                "stageNumber=" + stageNumber +
                ", queryCount=" + queries.size() +
                ", queries=" + queries.stream().map(Query::getId).toList() +
                ", completed=" + completed +
                '}';
    }
}
