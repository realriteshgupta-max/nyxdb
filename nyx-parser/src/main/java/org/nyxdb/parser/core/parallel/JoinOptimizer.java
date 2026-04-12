package org.nyxdb.parser.core.parallel;

import org.nyxdb.parser.core.parallel.logical.JoinOperator;
import org.nyxdb.parser.core.parallel.logical.JoinOperator.JoinStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Optimizes JOIN operations for distributed execution.
 * Determines optimal join strategies and execution orders.
 */
public class JoinOptimizer {
    private static final Logger logger = LogManager.getLogger(JoinOptimizer.class);

    /**
     * Table statistics for cost estimation.
     */
    public static class TableStats {
        public final String tableName;
        public long rowCount;
        public long estimatedSizeBytes;

        public TableStats(String tableName, long rowCount, long estimatedSizeBytes) {
            this.tableName = tableName;
            this.rowCount = rowCount;
            this.estimatedSizeBytes = estimatedSizeBytes;
        }

        public TableStats(String tableName) {
            this(tableName, 1000000, 100_000_000); // Default estimates
        }
    }

    /**
     * Optimizes a list of JOIN operators for distributed execution.
     */
    public void optimizeJoins(List<JoinOperator> joins, Map<String, TableStats> tableStats) {
        logger.info("Optimizing {} joins for distributed execution", joins.size());

        for (JoinOperator join : joins) {
            JoinStrategy strategy = selectJoinStrategy(join, tableStats);
            join.setJoinStrategy(strategy);
            logger.debug("Selected {} strategy for join: {}", strategy.name(), join.getDescription());
        }

        // Determine join order if multiple joins
        if (joins.size() > 1) {
            optimizeJoinOrder(joins, tableStats);
        }
    }

    /**
     * Selects the optimal join strategy based on table sizes and join type.
     */
    private JoinStrategy selectJoinStrategy(JoinOperator join, Map<String, TableStats> tableStats) {
        String leftTable = join.getLeftTable();
        String rightTable = join.getRightTable();

        TableStats leftStats = tableStats.getOrDefault(leftTable, new TableStats(leftTable));
        TableStats rightStats = tableStats.getOrDefault(rightTable, new TableStats(rightTable));

        // For broadcast joins: smaller table is replicated to all nodes
        long broadcastThreshold = 100_000_000; // 100MB
        if (leftStats.estimatedSizeBytes < broadcastThreshold) {
            logger.debug("Small table {} suitable for broadcast", leftTable);
            return JoinStrategy.BROADCAST;
        }
        if (rightStats.estimatedSizeBytes < broadcastThreshold) {
            logger.debug("Small table {} suitable for broadcast", rightTable);
            return JoinStrategy.BROADCAST;
        }

        // For hash joins: both tables are large, partition on join key
        if (leftStats.estimatedSizeBytes > broadcastThreshold &&
                rightStats.estimatedSizeBytes > broadcastThreshold) {
            // Check if inner join (can be parallelized better)
            if (join.getJoinType() == JoinOperator.JoinType.INNER) {
                logger.debug("Large inner join, using shuffle strategy");
                return JoinStrategy.SHUFFLE;
            }
            logger.debug("Large join with outer semantics, using hash join");
            return JoinStrategy.HASH_JOIN;
        }

        // Default for small joins
        return JoinStrategy.HASH_JOIN;
    }

    /**
     * Determines optimal join order using simple heuristics.
     * Sorts joins by estimated cost to execute smaller/cheaper joins first.
     */
    private void optimizeJoinOrder(List<JoinOperator> joins, Map<String, TableStats> tableStats) {
        logger.info("Optimizing join order for {} joins", joins.size());

        // Simple heuristic: sort by estimated cost of left and right tables
        joins.sort(new Comparator<JoinOperator>() {
            @Override
            public int compare(JoinOperator j1, JoinOperator j2) {
                long cost1 = estimateJoinCost(j1, tableStats);
                long cost2 = estimateJoinCost(j2, tableStats);
                return Long.compare(cost1, cost2);
            }
        });

        for (int i = 0; i < joins.size(); i++) {
            logger.debug("Join order[{}]: {} - estimated cost: {}",
                    i, joins.get(i).getDescription(), estimateJoinCost(joins.get(i), tableStats));
        }
    }

    /**
     * Estimates the cost of executing a join operation.
     * Cost = (left rows * right rows) * join operation cost
     */
    private long estimateJoinCost(JoinOperator join, Map<String, TableStats> tableStats) {
        String leftTable = join.getLeftTable();
        String rightTable = join.getRightTable();

        TableStats leftStats = tableStats.getOrDefault(leftTable, new TableStats(leftTable));
        TableStats rightStats = tableStats.getOrDefault(rightTable, new TableStats(rightTable));

        // Basic cost model
        long baseCost = leftStats.rowCount * rightStats.rowCount;

        // Adjust based on join strategy
        switch (join.getJoinStrategy()) {
            case BROADCAST:
                // Broadcast is cheaper for one small table
                baseCost = Math.min(leftStats.rowCount, rightStats.rowCount) * 10;
                break;
            case HASH_JOIN:
                // Hash join: O(n + m)
                baseCost = (leftStats.rowCount + rightStats.rowCount) * 2;
                break;
            case SHUFFLE:
                // Shuffle: O(n log n + m log m) + network cost
                baseCost = (long) ((leftStats.rowCount * Math.log(leftStats.rowCount)) +
                        (rightStats.rowCount * Math.log(rightStats.rowCount))) * 3;
                break;
            case SORT_MERGE:
                // Sort-merge: O(n log n + m log m)
                baseCost = (long) ((leftStats.rowCount * Math.log(leftStats.rowCount)) +
                        (rightStats.rowCount * Math.log(rightStats.rowCount)));
                break;
            default:
                baseCost = leftStats.rowCount + rightStats.rowCount;
        }

        return baseCost;
    }

    /**
     * Analyzes join dependencies to determine parallelization potential.
     */
    public JoinParallelizationPlan analyzeDependencies(List<JoinOperator> joins) {
        logger.info("Analyzing join dependencies for parallelization");

        JoinParallelizationPlan plan = new JoinParallelizationPlan();

        for (JoinOperator join : joins) {
            if (join.isParallelizable()) {
                plan.parallelizableJoins.add(join);
                logger.debug("Join can be parallelized: {}", join.getDescription());
            } else {
                plan.sequentialJoins.add(join);
                logger.debug("Join must be sequential: {}", join.getDescription());
            }
        }

        plan.parallelizationPotential = (double) plan.parallelizableJoins.size() / Math.max(1, joins.size());

        logger.info("Parallelization potential: {:.2%}", plan.parallelizationPotential);
        return plan;
    }

    /**
     * Represents a parallelization plan for joins.
     */
    public static class JoinParallelizationPlan {
        public final List<JoinOperator> parallelizableJoins = new ArrayList<>();
        public final List<JoinOperator> sequentialJoins = new ArrayList<>();
        public double parallelizationPotential; // Percentage of joins that can run in parallel

        @Override
        public String toString() {
            return String.format("JoinParallelizationPlan{parallelizable=%d, sequential=%d, potential=%.2f%%}",
                    parallelizableJoins.size(), sequentialJoins.size(), parallelizationPotential * 100);
        }
    }

    /**
     * Recommends distributed processing strategy based on joins.
     */
    public String recommendStrategy(List<JoinOperator> joins, Map<String, TableStats> tableStats) {
        if (joins.isEmpty()) {
            return "Single table scan strategy";
        }

        JoinParallelizationPlan plan = analyzeDependencies(joins);

        if (plan.parallelizationPotential > 0.8) {
            return "Highly parallelizable - use distributed hash join strategy";
        } else if (plan.parallelizationPotential > 0.5) {
            return "Moderately parallelizable - use hybrid shuffle-merge strategy";
        } else {
            return "Sequential joins required - use broadcast or nested loop strategy";
        }
    }
}
