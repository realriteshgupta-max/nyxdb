package org.nyxdb.parser.core.parallel;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a physical query execution plan for parallel execution.
 * The plan consists of multiple stages, where queries in each stage can execute
 * in parallel.
 */
public class PhysicalQueryPlan {
    private static final Logger logger = LogManager.getLogger(PhysicalQueryPlan.class);
    private final List<ExecutionStage> stages;
    private final int totalQueries;
    private int maxParallelism;
    private long estimatedTotalTimeMs;

    public PhysicalQueryPlan(int totalQueries) {
        this.stages = new ArrayList<>();
        this.totalQueries = totalQueries;
        this.maxParallelism = 1;
        this.estimatedTotalTimeMs = 0;
    }

    public void addStage(ExecutionStage stage) {
        stages.add(stage);
        maxParallelism = Math.max(maxParallelism, stage.getQueryCount());
    }

    public List<ExecutionStage> getStages() {
        return stages;
    }

    public ExecutionStage getStage(int stageNumber) {
        return stages.stream()
                .filter(s -> s.getStageNumber() == stageNumber)
                .findFirst()
                .orElse(null);
    }

    public int getStageCount() {
        return stages.size();
    }

    public int getTotalQueries() {
        return totalQueries;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public long getEstimatedTotalTimeMs() {
        return estimatedTotalTimeMs;
    }

    public void setEstimatedTotalTimeMs(long timeMs) {
        this.estimatedTotalTimeMs = timeMs;
    }

    public double getParallelizationFactor() {
        if (stages.isEmpty()) {
            return 1.0;
        }
        // Assuming equal query execution time, parallelization factor =
        // (total queries) / (number of stages * avg queries per stage)
        return (double) totalQueries / stages.size();
    }

    /**
     * Returns a user-friendly representation of the execution plan.
     */
    public String getPlanSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("Physical Query Execution Plan\n");
        sb.append("==============================\n");
        sb.append("Total Queries: ").append(totalQueries).append("\n");
        sb.append("Execution Stages: ").append(stages.size()).append("\n");
        sb.append("Max Parallelism: ").append(maxParallelism).append(" queries\n");
        sb.append("Parallelization Factor: ").append(String.format("%.2f", getParallelizationFactor())).append("\n");
        sb.append("Estimated Total Time: ").append(estimatedTotalTimeMs).append("ms\n");
        sb.append("\nExecution Plan:\n");
        for (ExecutionStage stage : stages) {
            sb.append("  Stage ").append(stage.getStageNumber()).append(": ");
            sb.append(stage.getQueryCount()).append(" queries");
            if (!stage.getQueries().isEmpty()) {
                sb.append(" [").append(stage.getQueries().stream()
                        .map(Query::getId).reduce((a, b) -> a + ", " + b).orElse("")).append("]");
            }
            // Include node placement if available
            if (!stage.getQueryNodeMap().isEmpty()) {
                sb.append(" -> Nodes: ").append(stage.getQueryNodeMap());
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "PhysicalQueryPlan{" +
                "stages=" + stages.size() +
                ", totalQueries=" + totalQueries +
                ", maxParallelism=" + maxParallelism +
                ", parallelizationFactor=" + String.format("%.2f", getParallelizationFactor()) +
                '}';
    }
}
