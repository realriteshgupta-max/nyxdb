package org.nyxdb.parser.core.parallel;

import org.nyxdb.parser.core.parallel.logical.LogicalQueryPlan;
import org.nyxdb.parser.core.parallel.logical.LogicalOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executes optimized SELECT queries on a distributed query engine.
 * Handles query distribution, data shuffling, and result aggregation.
 */
public class DistributedSelectExecutor {
    private static final Logger logger = LogManager.getLogger(DistributedSelectExecutor.class);

    /**
     * Represents a distributed execution node.
     */
    public static class ExecutionNode {
        public final String nodeId;
        public final String host;
        public final int port;
        public volatile boolean isActive;
        public long estimatedCapacity; // In rows/sec

        public ExecutionNode(String nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
            this.isActive = true;
            this.estimatedCapacity = 1000000; // Default: 1M rows/sec
        }

        @Override
        public String toString() {
            return String.format("Node[%s @ %s:%d, capacity=%d rows/sec]",
                    nodeId, host, port, estimatedCapacity);
        }
    }

    /**
     * Distributed execution plan with operator placement.
     */
    public static class DistributedPlan {
        public final String queryId;
        public final LogicalQueryPlan logicalPlan;
        public final List<ExecutionNode> nodes;
        public final Map<String, ExecutionNode> operatorPlacement; // operatorId -> node
        public final int parallelDegree;
        public long estimatedExecutionTimeMs;

        public DistributedPlan(String queryId, LogicalQueryPlan logicalPlan,
                List<ExecutionNode> nodes, int parallelDegree) {
            this.queryId = queryId;
            this.logicalPlan = logicalPlan;
            this.nodes = new ArrayList<>(nodes);
            this.parallelDegree = parallelDegree;
            this.operatorPlacement = new ConcurrentHashMap<>();
            this.estimatedExecutionTimeMs = 0;
        }

        public void placeOperator(String operatorId, ExecutionNode node) {
            operatorPlacement.put(operatorId, node);
            logger.debug("Placed operator {} on node {}", operatorId, node.nodeId);
        }

        public String getExecutionPlan() {
            StringBuilder sb = new StringBuilder();
            sb.append("Distributed Execution Plan for ").append(queryId).append("\n");
            sb.append("============================================\n");
            sb.append("Execution Nodes: ").append(nodes.size()).append("\n");
            for (ExecutionNode node : nodes) {
                sb.append("  ").append(node).append("\n");
            }
            sb.append("\nOperator Placement:\n");
            operatorPlacement.forEach(
                    (opId, node) -> sb.append("  ").append(opId).append(" -> ").append(node.nodeId).append("\n"));
            sb.append("\nParallel Degree: ").append(parallelDegree).append("\n");
            sb.append("Estimated Execution Time: ").append(estimatedExecutionTimeMs).append("ms\n");
            return sb.toString();
        }
    }

    private final List<ExecutionNode> availableNodes;
    private final JoinOptimizer joinOptimizer;

    public DistributedSelectExecutor(List<ExecutionNode> nodes) {
        this.availableNodes = new ArrayList<>(nodes);
        this.joinOptimizer = new JoinOptimizer();
        logger.info("DistributedSelectExecutor initialized with {} nodes", nodes.size());
    }

    /**
     * Creates a distributed execution plan from a logical query plan.
     */
    public DistributedPlan createDistributedPlan(LogicalQueryPlan logicalPlan) {
        logger.info("Creating distributed plan for: {}", logicalPlan.getQueryId());

        DistributedPlan plan = new DistributedPlan(
                logicalPlan.getQueryId(),
                logicalPlan,
                availableNodes,
                Math.min(availableNodes.size(), logicalPlan.getAllOperators().size()));

        // Place operators on nodes
        placeOperators(logicalPlan, plan);

        // Calculate estimated execution time
        plan.estimatedExecutionTimeMs = estimateExecutionTime(logicalPlan, plan);

        logger.info("Distributed plan created: {}", plan);
        return plan;
    }

    /**
     * Places operators on execution nodes based on data locality and join strategy.
     */
    private void placeOperators(LogicalQueryPlan logicalPlan, DistributedPlan plan) {
        List<LogicalOperator> operators = logicalPlan.getAllOperators();
        int nodeIndex = 0;

        for (LogicalOperator operator : operators) {
            ExecutionNode node = availableNodes.get(nodeIndex % availableNodes.size());
            plan.placeOperator(operator.getOperatorId(), node);
            nodeIndex++;

            logger.debug("Placed {} on {}", operator.getOperatorId(), node.nodeId);
        }
    }

    /**
     * Estimates total execution time based on operator costs and node capacity.
     */
    private long estimateExecutionTime(LogicalQueryPlan logicalPlan, DistributedPlan plan) {
        long totalCost = logicalPlan.getTotalEstimatedCost();
        double avgNodeCapacity = availableNodes.stream()
                .mapToLong(n -> n.estimatedCapacity)
                .average()
                .orElse(1000000);

        // Simple estimation: cost / parallel degree / average capacity
        long estimatedMs = (long) (totalCost / Math.max(1, plan.parallelDegree) / (avgNodeCapacity / 1000));

        logger.debug("Estimated execution time: {}ms (cost={}, parallelDegree={})",
                estimatedMs, totalCost, plan.parallelDegree);

        return estimatedMs;
    }

    /**
     * Executes a distributed plan (simulated).
     */
    public ExecutionResult execute(DistributedPlan plan) throws Exception {
        logger.info("Executing distributed plan: {}", plan.queryId);
        long startTime = System.currentTimeMillis();

        // Check all nodes are active
        List<ExecutionNode> activeNodes = new ArrayList<>();
        for (ExecutionNode node : plan.nodes) {
            if (node.isActive) {
                activeNodes.add(node);
                logger.debug("Node {} is active", node.nodeId);
            } else {
                logger.warn("Node {} is inactive", node.nodeId);
            }
        }

        if (activeNodes.isEmpty()) {
            throw new Exception("No active execution nodes available");
        }

        // Execute operators in DAG order
        executeOperatorDAG(plan);

        long executionTimeMs = System.currentTimeMillis() - startTime;
        ExecutionResult result = new ExecutionResult(plan.queryId, true, executionTimeMs, activeNodes.size());

        logger.info("Distributed execution completed in {}ms", executionTimeMs);
        return result;
    }

    /**
     * Executes the operator DAG on distributed nodes.
     */
    private void executeOperatorDAG(DistributedPlan plan) {
        List<LogicalOperator> operators = plan.logicalPlan.getAllOperators();

        for (LogicalOperator operator : operators) {
            ExecutionNode node = plan.operatorPlacement.get(operator.getOperatorId());
            if (node != null && node.isActive) {
                logger.info("Executing {} on {}", operator.getDescription(), node.nodeId);

                // Simulate execution delay based on estimated cost
                try {
                    long delay = Math.max(10, operator.getEstimatedCost() / 10000);
                    Thread.sleep(Math.min(delay, 200)); // Cap at 200ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Execution interrupted for operator: {}", operator.getOperatorId());
                }
            }
        }
    }

    /**
     * Execution result containing status and metrics.
     */
    public static class ExecutionResult {
        public final String queryId;
        public final boolean success;
        public final long executionTimeMs;
        public final int nodesUsed;
        public long rowsProcessed;

        public ExecutionResult(String queryId, boolean success, long executionTimeMs, int nodesUsed) {
            this.queryId = queryId;
            this.success = success;
            this.executionTimeMs = executionTimeMs;
            this.nodesUsed = nodesUsed;
            this.rowsProcessed = 0;
        }

        @Override
        public String toString() {
            return String.format("ExecutionResult{query=%s, success=%s, time=%dms, nodes=%d, rows=%d}",
                    queryId, success, executionTimeMs, nodesUsed, rowsProcessed);
        }
    }

    /**
     * Adds an execution node to the cluster.
     */
    public void addNode(ExecutionNode node) {
        availableNodes.add(node);
        logger.info("Added execution node: {}", node);
    }

    /**
     * Removes an execution node (for failure handling).
     */
    public void removeNode(String nodeId) {
        availableNodes.removeIf(n -> n.nodeId.equals(nodeId));
        logger.info("Removed execution node: {}", nodeId);
    }

    /**
     * Gets summary of available nodes.
     */
    public String getClusterStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("Cluster Status:\n");
        sb.append("===============\n");
        sb.append("Total Nodes: ").append(availableNodes.size()).append("\n");
        sb.append("Active Nodes: ").append(availableNodes.stream().filter(n -> n.isActive).count()).append("\n");
        sb.append("Total Capacity: ").append(availableNodes.stream()
                .mapToLong(n -> n.estimatedCapacity)
                .sum()).append(" rows/sec\n");
        sb.append("\nNodes:\n");
        for (ExecutionNode node : availableNodes) {
            sb.append("  ").append(node).append("\n");
        }
        return sb.toString();
    }
}
