package org.nyxdb.parser.core.parallel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates a PhysicalQueryPlan for a list of queries to enable parallel
 * execution.
 * Uses topological sorting and greedy scheduling to create execution stages.
 */
public class ParallelQueryPlanner {
    private static final Logger logger = LogManager.getLogger(ParallelQueryPlanner.class);
    private final QueryDependencyAnalyzer dependencyAnalyzer;

    public ParallelQueryPlanner() {
        this.dependencyAnalyzer = new QueryDependencyAnalyzer();
    }

    /**
     * Creates a physical query plan for parallel execution.
     *
     * @param queries the list of queries to plan
     * @return the physical query execution plan
     */
    public PhysicalQueryPlan createQueryPlan(List<Query> queries) {
        if (queries == null || queries.isEmpty()) {
            logger.info("Creating physical query plan for 0 queries");
            return new PhysicalQueryPlan(0);
        }

        logger.info("Creating physical query plan for {} queries", queries.size());
        PhysicalQueryPlan plan = new PhysicalQueryPlan(queries.size());

        // Analyze dependencies
        List<QueryDependencyAnalyzer.DependencyInfo> dependencies = dependencyAnalyzer.analyzeDependencies(queries);
        logger.debug("Detected {} dependencies between queries", dependencies.size());

        // Build dependency graph
        Map<String, Set<String>> dependencyGraph = dependencyAnalyzer.buildDependencyGraph(dependencies);

        // Create execution stages using topological sort
        List<ExecutionStage> stages = createExecutionStages(queries, dependencyGraph, dependencies);
        logger.info("Created execution plan with {} stages", stages.size());

        // Add stages to plan
        for (ExecutionStage stage : stages) {
            plan.addStage(stage);
        }

        // Calculate estimated execution time (basic estimation)
        long estimatedTime = calculateEstimatedTime(stages);
        plan.setEstimatedTotalTimeMs(estimatedTime);

        return plan;
    }

    /**
     * Creates execution stages using a greedy approach.
     * Assigns queries to the earliest possible stage based on their dependencies.
     */
    private List<ExecutionStage> createExecutionStages(
            List<Query> queries,
            Map<String, Set<String>> dependencyGraph,
            List<QueryDependencyAnalyzer.DependencyInfo> dependencies) {

        List<ExecutionStage> stages = new ArrayList<>();
        Map<String, Integer> queryToStage = new HashMap<>();

        // Calculate in-degree (number of incoming dependencies) for each query
        Map<String, Integer> inDegree = new HashMap<>();
        for (Query query : queries) {
            inDegree.put(query.getId(), 0);
        }

        for (Query query : queries) {
            Set<String> predecessors = getPredecessors(query.getId(), dependencies);
            inDegree.put(query.getId(), predecessors.size());
        }

        // Assign queries to stages greedily
        Set<String> scheduled = new HashSet<>();
        int currentStage = 0;

        while (scheduled.size() < queries.size()) {
            ExecutionStage stage = new ExecutionStage(currentStage);
            List<Query> stageQueries = new ArrayList<>();

            // Find all queries that can be scheduled in this stage
            for (Query query : queries) {
                if (scheduled.contains(query.getId())) {
                    continue;
                }

                // Check if all predecessors have been scheduled
                Set<String> predecessors = getPredecessors(query.getId(), dependencies);
                if (scheduled.containsAll(predecessors)) {
                    stageQueries.add(query);
                    queryToStage.put(query.getId(), currentStage);
                }
            }

            if (stageQueries.isEmpty()) {
                // This shouldn't happen with valid queries
                throw new IllegalStateException("Circular dependency detected in queries");
            }

            stage.addQueries(stageQueries);
            stages.add(stage);
            scheduled.addAll(stageQueries.stream().map(Query::getId).collect(Collectors.toSet()));
            currentStage++;
        }

        return stages;
    }

    /**
     * Gets all predecessor queries (queries that must complete before the given
     * query).
     */
    private Set<String> getPredecessors(String queryId,
            List<QueryDependencyAnalyzer.DependencyInfo> dependencies) {
        Set<String> predecessors = new HashSet<>();

        for (QueryDependencyAnalyzer.DependencyInfo dep : dependencies) {
            if (dep.getTarget().getId().equals(queryId)) {
                predecessors.add(dep.getSource().getId());
            }
        }

        return predecessors;
    }

    /**
     * Calculates estimated execution time based on stage structure.
     * This is a simple estimate assuming each query takes 1 unit of time.
     */
    private long calculateEstimatedTime(List<ExecutionStage> stages) {
        // Basic estimate: number of stages (since all queries in a stage run in
        // parallel)
        // In a real system, this would be based on query complexity and actual
        // execution statistics
        return stages.size() * 100; // 100ms per stage as a simple estimate
    }

    /**
     * Analyzes whether a set of queries can be fully parallelized.
     *
     * @param queries the queries to analyze
     * @return true if all queries can be executed in parallel (single stage)
     */
    public boolean canFullyParallelize(List<Query> queries) {
        if (queries == null || queries.isEmpty()) {
            return true;
        }

        PhysicalQueryPlan plan = createQueryPlan(queries);
        return plan.getStageCount() == 1;
    }

    /**
     * Gets the critical path (longest dependency chain).
     *
     * @param plan the physical query plan
     * @return the length of the critical path
     */
    public int getCriticalPathLength(PhysicalQueryPlan plan) {
        return plan.getStageCount();
    }

    /**
     * Generates a detailed execution report.
     *
     * @param plan the physical query plan
     * @return a detailed report string
     */
    public String generateDetailedReport(PhysicalQueryPlan plan) {
        StringBuilder report = new StringBuilder();
        report.append("\n=====================================\n");
        report.append("PARALLEL QUERY EXECUTION PLAN REPORT\n");
        report.append("=====================================\n\n");

        report.append(plan.getPlanSummary());

        report.append("\nExecution Timeline:\n");
        for (ExecutionStage stage : plan.getStages()) {
            report.append("\n  ┌─ Stage ").append(stage.getStageNumber()).append("\n");
            for (Query query : stage.getQueries()) {
                report.append("  │  ├─ ").append(query.getId()).append(" (")
                        .append(query.getType()).append(")\n");
                if (!query.getReadTables().isEmpty()) {
                    report.append("  │  │  ├─ Reads: ").append(query.getReadTables()).append("\n");
                }
                if (!query.getWriteTables().isEmpty()) {
                    report.append("  │  │  └─ Writes: ").append(query.getWriteTables()).append("\n");
                }
            }
            report.append("  │\n");
        }
        report.append("  └─ End\n");

        report.append("\nPerformance Metrics:\n");
        report.append("  • Total Queries: ").append(plan.getTotalQueries()).append("\n");
        report.append("  • Number of Stages: ").append(plan.getStageCount()).append("\n");
        report.append("  • Max Parallelism: ").append(plan.getMaxParallelism()).append(" queries/stage\n");
        report.append("  • Parallelization Factor: ")
                .append(String.format("%.2f", plan.getParallelizationFactor())).append("x\n");
        report.append("  • Critical Path Length: ").append(getCriticalPathLength(plan)).append(" stages\n");

        return report.toString();
    }
}
