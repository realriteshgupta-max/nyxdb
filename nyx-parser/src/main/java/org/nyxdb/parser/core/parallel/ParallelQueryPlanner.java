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
     * Create a physical plan using the parser's execution order (children before
     * parents). This helps the planner respect staged execution for nested
     * queries.
     */
    public PhysicalQueryPlan createQueryPlanUsingParser(ParallelQueryParser parser, List<Query> queries) {
        if (queries == null || queries.isEmpty()) {
            logger.info("Creating physical query plan for 0 queries");
            return new PhysicalQueryPlan(0);
        }

        logger.info("Creating physical query plan (using parser order) for {} queries", queries.size());

        // Let parser produce an execution-ordered list (post-order: children first)
        List<Query> ordered = parser.buildExecutionOrder(queries);

        PhysicalQueryPlan plan = new PhysicalQueryPlan(ordered.size());

        // Analyze dependencies (same as before)
        List<QueryDependencyAnalyzer.DependencyInfo> dependencies = dependencyAnalyzer.analyzeDependencies(ordered);
        logger.debug("Detected {} dependencies between queries", dependencies.size());

        Map<String, Set<String>> dependencyGraph = dependencyAnalyzer.buildDependencyGraph(dependencies);

        // Use the ordered list when creating execution stages to prefer
        // subqueries-first assignment
        List<ExecutionStage> stages = createExecutionStages(ordered, dependencyGraph, dependencies);
        logger.info("Created execution plan with {} stages", stages.size());

        for (ExecutionStage stage : stages) {
            plan.addStage(stage);
        }

        long estimatedTime = calculateEstimatedTime(stages);
        plan.setEstimatedTotalTimeMs(estimatedTime);

        return plan;
    }

    /**
     * Compute maximal parallel groups (layers) of queries using dependency
     * analysis. Each set contains queries that can run in parallel (no
     * predecessors among them).
     */
    public List<java.util.Set<Query>> getParallelGroupsUsingParser(ParallelQueryParser parser, List<Query> queries) {
        List<java.util.Set<Query>> groups = new ArrayList<>();
        if (queries == null || queries.isEmpty())
            return groups;

        // analyze dependencies
        List<QueryDependencyAnalyzer.DependencyInfo> dependencies = dependencyAnalyzer.analyzeDependencies(queries);

        // build predecessor map (target -> set of sources)
        Map<String, java.util.Set<String>> predecessors = new HashMap<>();
        Map<String, Query> idToQuery = queries.stream().collect(Collectors.toMap(Query::getId, q -> q));

        for (Query q : queries)
            predecessors.put(q.getId(), new HashSet<>());

        for (QueryDependencyAnalyzer.DependencyInfo d : dependencies) {
            String src = d.getSource().getId();
            String tgt = d.getTarget().getId();
            predecessors.computeIfAbsent(tgt, k -> new HashSet<>()).add(src);
        }

        // Kahn-like layering: collect nodes with no predecessors
        java.util.Set<String> remaining = new HashSet<>(predecessors.keySet());

        while (!remaining.isEmpty()) {
            java.util.Set<String> layer = remaining.stream()
                    .filter(id -> predecessors.getOrDefault(id, java.util.Collections.emptySet()).isEmpty())
                    .collect(Collectors.toSet());
            if (layer.isEmpty()) {
                // cycle detected; put remaining nodes in a final group
                java.util.Set<Query> last = remaining.stream().map(idToQuery::get).filter(java.util.Objects::nonNull)
                        .collect(Collectors.toSet());
                groups.add(last);
                break;
            }
            // convert ids to Query objects
            java.util.Set<Query> layerQueries = layer.stream().map(idToQuery::get).filter(java.util.Objects::nonNull)
                    .collect(Collectors.toSet());
            groups.add(layerQueries);

            // remove layer nodes from predecessors of others
            for (String id : layer) {
                remaining.remove(id);
            }
            for (String id : remaining) {
                java.util.Set<String> preds = predecessors.get(id);
                if (preds != null)
                    preds.removeAll(layer);
            }
        }

        return groups;
    }

    /**
     * Build a PhysicalQueryPlan where each stage corresponds to a parallel group
     * computed from dependencies (maximal sets of queries that can run in
     * parallel).
     */
    public PhysicalQueryPlan createQueryPlanFromParallelGroups(ParallelQueryParser parser, List<Query> queries) {
        List<java.util.Set<Query>> groups = getParallelGroupsUsingParser(parser, queries);
        PhysicalQueryPlan plan = new PhysicalQueryPlan(queries == null ? 0 : queries.size());
        int stageNum = 0;
        for (java.util.Set<Query> group : groups) {
            ExecutionStage stage = new ExecutionStage(stageNum++);
            List<Query> qlist = new ArrayList<>(group);
            stage.addQueries(qlist);
            assignNodesToStage(stage);
            plan.addStage(stage);
        }
        plan.setEstimatedTotalTimeMs(calculateEstimatedTime(plan.getStages()));
        return plan;
    }

    /**
     * Export the dependency graph and parallel groups as Graphviz DOT markup.
     * Nodes are queries (by id) and edges represent dependencies.
     */
    public String exportParallelGroupsDot(ParallelQueryParser parser, List<Query> queries) {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph ParallelGroups {\n");
        sb.append("  rankdir=LR;\n");

        // all nodes
        for (Query q : queries) {
            sb.append("  \"").append(q.getId()).append("\" [label=\"").append(q.getId()).append("\n")
                    .append(escapeLabel(q.getSql())).append("\"];\n");
        }

        // dependencies edges
        List<QueryDependencyAnalyzer.DependencyInfo> deps = dependencyAnalyzer.analyzeDependencies(queries);
        for (QueryDependencyAnalyzer.DependencyInfo d : deps) {
            sb.append("  \"").append(d.getSource().getId()).append("\" -> \"").append(d.getTarget().getId())
                    .append("\" [label=\"").append(d.getType().getSymbol()).append("\"];\n");
        }

        // group subgraphs
        List<java.util.Set<Query>> groups = getParallelGroupsUsingParser(parser, queries);
        for (int i = 0; i < groups.size(); i++) {
            sb.append("  subgraph cluster_group_").append(i).append(" {\n");
            sb.append("    label=\"group_").append(i).append("\";\n");
            for (Query q : groups.get(i)) {
                sb.append("    \"").append(q.getId()).append("\";\n");
            }
            sb.append("  }\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    private String escapeLabel(String s) {
        return s.replaceAll("\"", "\\\"").replaceAll("\\n", "\\\n");
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
                // Circular dependency or unresolved ordering detected. Fall back
                // to placing all remaining unscheduled queries into a single
                // stage to make progress instead of throwing.
                List<Query> remaining = new ArrayList<>();
                for (Query q : queries) {
                    if (!scheduled.contains(q.getId()))
                        remaining.add(q);
                }
                if (remaining.isEmpty()) {
                    break;
                }
                stage.addQueries(remaining);
                assignNodesToStage(stage);
                stages.add(stage);
                scheduled.addAll(remaining.stream().map(Query::getId).collect(Collectors.toSet()));
                currentStage++;
                continue;
            }

            stage.addQueries(stageQueries);
            // Assign nodes to queries in this stage (simple heuristic)
            assignNodesToStage(stage);
            stages.add(stage);
            scheduled.addAll(stageQueries.stream().map(Query::getId).collect(Collectors.toSet()));
            currentStage++;
        }

        return stages;
    }

    // Simple node assignment: pick from a small set of available nodes based on
    // a hash of the query's read tables (or query id fallback).
    private final List<String> availableNodes = List.of("nodeA", "nodeB", "nodeC");

    private void assignNodesToStage(ExecutionStage stage) {
        for (Query q : stage.getQueries()) {
            String key = q.getReadTables().stream().sorted().reduce((a, b) -> a + "," + b).orElse(q.getId());
            int idx = Math.abs(key.hashCode()) % availableNodes.size();
            String node = availableNodes.get(idx);
            stage.assignNodeToQuery(q.getId(), node);
        }
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
