package org.nyxdb.parser.core.parallel;

import org.nyxdb.parser.core.parallel.logical.LogicalQueryPlan;
import org.nyxdb.parser.core.parallel.JoinOptimizer.TableStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Comprehensive example demonstrating distributed SELECT query execution
 * with parallel joins, subqueries, and aggregations.
 */
public class DistributedSelectExample {
    private static final Logger logger = LogManager.getLogger(DistributedSelectExample.class);

    public static void main(String[] args) {
        logger.info("===========================================");
        logger.info("Distributed SELECT Query Execution Example");
        logger.info("===========================================\n");

        example1_SimpleSelectOptimization();
        example2_SelectWithParallelJoins();
        example3_SelectWithAggregation();
        example4_NestedQueryOptimization();
        example5_DistributedExecution();
    }

    /**
     * Example 1: Simple SELECT optimization and logical plan generation.
     */
    private static void example1_SimpleSelectOptimization() {
        logger.info("\n========== Example 1: Simple SELECT Optimization ==========");

        String sql = "SELECT u.id, u.name, COUNT(*) as order_count FROM users u GROUP BY u.id, u.name ORDER BY order_count DESC";

        ParallelQueryParser parser = new ParallelQueryParser();
        Query query = parser.parseQuery(sql, "Q1");

        logger.info("Query Type: {}", query.getType());
        logger.info("Read Tables: {}", query.getReadTables());
        logger.info("Has JOINs: {}", query.hasJoin());
        logger.info("Has GROUP BY: {}", query.hasGroupBy());
        logger.info("Has ORDER BY: {}", query.hasOrderBy());
        logger.info("Aggregates: {}", query.getAggregates());

        // Build logical plan
        SelectQueryOptimizer optimizer = new SelectQueryOptimizer();
        LogicalQueryPlan logicalPlan = optimizer.buildLogicalPlan(query);

        logger.info(logicalPlan.printTree());
        logger.info("Total Estimated Cost: {}", logicalPlan.getTotalEstimatedCost());
        logger.info("Total Operators: {}", logicalPlan.getAllOperators().size());
    }

    /**
     * Example 2: SELECT with parallel JOINs optimization.
     */
    private static void example2_SelectWithParallelJoins() {
        logger.info("\n========== Example 2: SELECT with Parallel JOINs ==========");

        String sql = "SELECT u.name, o.id, o.amount, p.price " +
                "FROM users u " +
                "INNER JOIN orders o ON u.id = o.user_id " +
                "INNER JOIN products p ON o.product_id = p.id " +
                "WHERE o.amount > 100 " +
                "ORDER BY o.amount DESC";

        ParallelQueryParser parser = new ParallelQueryParser();
        Query query = parser.parseQuery(sql, "Q2");

        logger.info("Query Type: {}", query.getType());
        logger.info("Read Tables: {}", query.getReadTables());
        logger.info("Joined Tables: {}", query.getJoinedTables());
        logger.info("Has JOINs: {}", query.hasJoin());

        // Build logical plan
        SelectQueryOptimizer optimizer = new SelectQueryOptimizer();
        LogicalQueryPlan logicalPlan = optimizer.buildLogicalPlan(query);

        logger.info(logicalPlan.printTree());

        // Analyze join optimization
        List<org.nyxdb.parser.core.parallel.logical.JoinOperator> joinOps = logicalPlan.getJoinOperators();
        logger.info("Number of JOINs: {}", joinOps.size());

        // Create table statistics for optimization
        Map<String, TableStats> tableStats = new HashMap<>();
        tableStats.put("users", new TableStats("users", 100000, 10_000_000));
        tableStats.put("orders", new TableStats("orders", 500000, 100_000_000));
        tableStats.put("products", new TableStats("products", 10000, 1_000_000));

        JoinOptimizer joinOptimizer = new JoinOptimizer();
        joinOptimizer.optimizeJoins(joinOps, tableStats);

        logger.info("Join Strategy Recommendations:");
        for (org.nyxdb.parser.core.parallel.logical.JoinOperator join : joinOps) {
            logger.info("  {} strategy: {}", join.getDescription(), join.getJoinStrategy());
        }

        JoinOptimizer.JoinParallelizationPlan parallelPlan = joinOptimizer.analyzeDependencies(joinOps);
        logger.info("Parallelization Plan: {}", parallelPlan);
        logger.info("Recommended Strategy: {}", joinOptimizer.recommendStrategy(joinOps, tableStats));
    }

    /**
     * Example 3: SELECT with aggregation and distributed computation.
     */
    private static void example3_SelectWithAggregation() {
        logger.info("\n========== Example 3: SELECT with Aggregation ==========");

        String sql = "SELECT " +
                "  u.region, " +
                "  COUNT(DISTINCT u.id) as num_users, " +
                "  SUM(o.amount) as total_sales, " +
                "  AVG(o.amount) as avg_order, " +
                "  MAX(o.amount) as max_order " +
                "FROM users u " +
                "LEFT JOIN orders o ON u.id = o.user_id " +
                "WHERE o.created_date >= '2024-01-01' " +
                "GROUP BY u.region " +
                "HAVING SUM(o.amount) > 10000 " +
                "ORDER BY total_sales DESC " +
                "LIMIT 10";

        ParallelQueryParser parser = new ParallelQueryParser();
        Query query = parser.parseQuery(sql, "Q3");

        logger.info("Query Type: {}", query.getType());
        logger.info("Aggregates: {}", query.getAggregates());
        logger.info("Has GROUP BY: {}", query.hasGroupBy());
        logger.info("Has HAVING: {}", query.getSql().toUpperCase().contains("HAVING"));
        logger.info("Has ORDER BY: {}", query.hasOrderBy());

        // Build logical plan
        SelectQueryOptimizer optimizer = new SelectQueryOptimizer();
        LogicalQueryPlan logicalPlan = optimizer.buildLogicalPlan(query);

        logger.info("Operator Breakdown:");
        logger.info("  SCAN operators: {}", logicalPlan.countOperatorType("SCAN"));
        logger.info("  FILTER operators: {}", logicalPlan.countOperatorType("FILTER"));
        logger.info("  JOIN operators: {}", logicalPlan.countOperatorType("JOIN"));
        logger.info("  AGGREGATE operators: {}", logicalPlan.countOperatorType("AGGREGATE"));
        logger.info("  SORT operators: {}", logicalPlan.countOperatorType("SORT"));
        logger.info("  PROJECT operators: {}", logicalPlan.countOperatorType("PROJECT"));

        logger.info(logicalPlan.printTree());
    }

    /**
     * Example 4: Nested query (subquery) optimization.
     */
    private static void example4_NestedQueryOptimization() {
        logger.info("\n========== Example 4: Nested Query Optimization ==========");

        String sql = "SELECT u.id, u.name, order_summary.total_amount " +
                "FROM users u " +
                "JOIN (" +
                "  SELECT user_id, SUM(amount) as total_amount, COUNT(*) as num_orders " +
                "  FROM orders " +
                "  GROUP BY user_id " +
                "  HAVING COUNT(*) > 5" +
                ") order_summary ON u.id = order_summary.user_id " +
                "WHERE u.active = true " +
                "ORDER BY order_summary.total_amount DESC";

        ParallelQueryParser parser = new ParallelQueryParser();
        Query query = parser.parseQuery(sql, "Q4");

        logger.info("Main Query Type: {}", query.getType());

        // Extract subqueries
        SubqueryExtractor subqueryExtractor = new SubqueryExtractor();
        boolean hasNested = subqueryExtractor.hasSubqueries(sql);
        int maxDepth = subqueryExtractor.getMaxNestingDepth(sql);

        logger.info("Has Subqueries: {}", hasNested);
        logger.info("Max Nesting Depth: {}", maxDepth);

        List<SubqueryExtractor.SubqueryInfo> subqueries = subqueryExtractor.extractSubqueries(sql);
        logger.info("Extracted Subqueries: {}", subqueries.size());
        for (SubqueryExtractor.SubqueryInfo subquery : subqueries) {
            logger.info("  Subquery[type={}, alias={}, depth={}]",
                    subquery.type.name(), subquery.alias, subquery.depth);
        }

        // Build logical plan (would handle nested plans in full implementation)
        SelectQueryOptimizer optimizer = new SelectQueryOptimizer();
        LogicalQueryPlan logicalPlan = optimizer.buildLogicalPlan(query);

        logger.info(logicalPlan.printTree());
    }

    /**
     * Example 5: Full distributed execution planning and simulation.
     */
    private static void example5_DistributedExecution() {
        logger.info("\n========== Example 5: Distributed Execution ==========");

        String sql = "SELECT u.id, u.name, SUM(o.amount) as total_spent " +
                "FROM users u " +
                "LEFT JOIN orders o ON u.id = o.user_id " +
                "WHERE o.created_date >= '2024-01-01' " +
                "GROUP BY u.id, u.name " +
                "ORDER BY total_spent DESC";

        // Step 1: Parse query
        ParallelQueryParser parser = new ParallelQueryParser();
        Query query = parser.parseQuery(sql, "Q_DISTRIBUTED");

        logger.info("Parsed Query: {}", query);

        // Step 2: Build logical plan
        SelectQueryOptimizer optimizer = new SelectQueryOptimizer();
        LogicalQueryPlan logicalPlan = optimizer.buildLogicalPlan(query);

        logger.info("Logical Plan: {}", logicalPlan);

        // Step 3: Set up distributed cluster
        List<DistributedSelectExecutor.ExecutionNode> cluster = new ArrayList<>();
        cluster.add(new DistributedSelectExecutor.ExecutionNode("node-1", "10.0.0.1", 5432));
        cluster.add(new DistributedSelectExecutor.ExecutionNode("node-2", "10.0.0.2", 5432));
        cluster.add(new DistributedSelectExecutor.ExecutionNode("node-3", "10.0.0.3", 5432));
        cluster.add(new DistributedSelectExecutor.ExecutionNode("node-4", "10.0.0.4", 5432));

        DistributedSelectExecutor executor = new DistributedSelectExecutor(cluster);
        logger.info(executor.getClusterStatus());

        // Step 4: Create distributed execution plan
        DistributedSelectExecutor.DistributedPlan distributedPlan = executor.createDistributedPlan(logicalPlan);

        logger.info(distributedPlan.getExecutionPlan());

        // Step 5: Execute distributed plan
        try {
            DistributedSelectExecutor.ExecutionResult result = executor.execute(distributedPlan);
            logger.info("Execution Result: {}", result);
            logger.info("Actual Execution Time: {}ms (estimated: {}ms)",
                    result.executionTimeMs, distributedPlan.estimatedExecutionTimeMs);
            logger.info("Parallel Efficiency: {:.2%}",
                    (double) distributedPlan.estimatedExecutionTimeMs / result.executionTimeMs);
        } catch (Exception e) {
            logger.error("Execution failed: {}", e.getMessage());
        }
    }
}
