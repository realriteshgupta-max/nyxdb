package org.nyxdb.parser.core.parallel;

import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Example usage of the parallel query execution system.
 * Demonstrates how to parse queries, analyze dependencies, create execution
 * plans, and execute in parallel.
 */
public class ParallelQueryExecutionExample {
    private static final Logger logger = LogManager.getLogger(ParallelQueryExecutionExample.class);

    public static void main(String[] args) {
        // Example 1: Basic parallel query execution
        example1_BasicParallelQueries();

        // Example 2: Queries with dependencies
        logger.info("\n\n");
        example2_QueriesWithDependencies();

        // Example 3: SELECT queries with JOINs and aggregates
        logger.info("\n\n");
        example3_SelectWithJoinsAndAggregates();

        // Example 4: Full pipeline
        logger.info("\n\n");
        example4_FullPipeline();
    }

    /**
     * Example 1: Demonstrates parsing and planning of independent queries.
     */
    public static void example1_BasicParallelQueries() {
        logger.info("EXAMPLE 1: Basic Parallel Queries");
        logger.info("==================================");

        // Parse queries
        ParallelQueryParser parser = new ParallelQueryParser();
        String[] queries = {
                "CREATE TABLE users (id INT, name VARCHAR(50));",
                "CREATE TABLE products (id INT, title VARCHAR(100));",
                "CREATE TABLE orders (id INT, user_id INT, product_id INT);"
        };

        List<Query> parsedQueries = parser.parseMultipleStatements(queries);

        // Print analysis
        logger.info(parser.generateAnalysisReport(parsedQueries));

        // Create execution plan
        ParallelQueryPlanner planner = new ParallelQueryPlanner();
        PhysicalQueryPlan plan = planner.createQueryPlan(parsedQueries);

        logger.info(planner.generateDetailedReport(plan));
    }

    /**
     * Example 2: Demonstrates dependency analysis for queries with dependencies.
     */
    public static void example2_QueriesWithDependencies() {
        logger.info("EXAMPLE 2: Queries with Dependencies");
        logger.info("====================================");

        // Create queries manually to show dependencies
        Query q1 = new Query("Q1", "CREATE TABLE users (id INT);", QueryType.CREATE_TABLE);
        q1.addWriteTable("users");

        Query q2 = new Query("Q2", "INSERT INTO users VALUES (1);", QueryType.INSERT);
        q2.addWriteTable("users");

        Query q3 = new Query("Q3", "CREATE TABLE orders (id INT, user_id INT);", QueryType.CREATE_TABLE);
        q3.addWriteTable("orders");

        Query q4 = new Query("Q4", "INSERT INTO orders VALUES (100, 1);", QueryType.INSERT);
        q4.addWriteTable("orders");

        List<Query> queries = List.of(q1, q2, q3, q4);

        // Analyze dependencies
        QueryDependencyAnalyzer analyzer = new QueryDependencyAnalyzer();
        List<QueryDependencyAnalyzer.DependencyInfo> dependencies = analyzer.analyzeDependencies(queries);

        logger.info("Dependency Analysis:");
        logger.info("-------------------");
        if (dependencies.isEmpty()) {
            logger.info("No dependencies found - all queries can run in parallel!");
        } else {
            for (QueryDependencyAnalyzer.DependencyInfo dep : dependencies) {
                logger.info("  " + dep);
            }
        }

        // Create execution plan
        ParallelQueryPlanner planner = new ParallelQueryPlanner();
        PhysicalQueryPlan plan = planner.createQueryPlan(queries);

        logger.info(planner.generateDetailedReport(plan));
    }

    /**
     * Example 3: Demonstrates SELECT queries with JOINs and aggregates.
     */
    public static void example3_SelectWithJoinsAndAggregates() {
        logger.info("EXAMPLE 3: SELECT Queries with JOINs and Aggregates");
        logger.info("====================================================");

        ParallelQueryParser parser = new ParallelQueryParser();
        String[] queries = {
                // Setup: Create and populate tables
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));",
                "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2));",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');",
                "INSERT INTO orders VALUES (100, 1, 250.00), (101, 2, 500.00), (102, 1, 150.00);",

                // Complex SELECT queries with JOINs and aggregates
                "SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_spent FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name ORDER BY total_spent DESC;",
                "SELECT u.name, o.id, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.amount > 200 ORDER BY o.amount;",
                "SELECT COUNT(*) as total_orders, AVG(amount) as avg_order FROM orders;",
                "SELECT u.id, u.name FROM users u WHERE u.id NOT IN (SELECT DISTINCT user_id FROM orders);"
        };

        List<Query> parsedQueries = parser.parseMultipleStatements(queries);

        // Print detailed analysis
        logger.info(parser.generateAnalysisReport(parsedQueries));

        // Show detailed query information
        logger.info("\nDetailed Query Information:");
        logger.info("--------------------------");
        for (Query q : parsedQueries) {
            logger.info("Query {}: Type={}, ReadTables={}, WriteTable={}, HasJoin={}, HasOrderBy={}, Aggregates={}",
                    q.getId(), q.getType(), q.getReadTables(), q.getWriteTables(),
                    q.hasJoin(), q.hasOrderBy(), q.getAggregates());
        }

        // Analyze dependencies
        QueryDependencyAnalyzer analyzer = new QueryDependencyAnalyzer();
        List<QueryDependencyAnalyzer.DependencyInfo> dependencies = analyzer.analyzeDependencies(parsedQueries);

        logger.info("\nDependency Analysis:");
        logger.info("-------------------");
        if (dependencies.isEmpty()) {
            logger.info("No dependencies detected.");
        } else {
            for (QueryDependencyAnalyzer.DependencyInfo dep : dependencies) {
                logger.info("  {} -> {} ({})", dep.getSource().getId(), dep.getTarget().getId(), dep.getType());
            }
        }

        // Create execution plan
        ParallelQueryPlanner planner = new ParallelQueryPlanner();
        PhysicalQueryPlan plan = planner.createQueryPlan(parsedQueries);

        logger.info(planner.generateDetailedReport(plan));
    }

    /**
     * Example 4: Full pipeline with execution simulation.
     */
    public static void example4_FullPipeline() {
        logger.info("EXAMPLE 4: Full Pipeline with Execution Simulation");
        logger.info("==================================================");

        // Step 1: Parse queries
        ParallelQueryParser parser = new ParallelQueryParser();
        String sqlBatch = """
                CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100));
                CREATE TABLE invoices (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2));
                ALTER TABLE invoices ADD COLUMN paid BOOLEAN;
                INSERT INTO customers VALUES (1, 'John Doe', 'john@example.com');
                INSERT INTO customers VALUES (2, 'Jane Smith', 'jane@example.com');
                INSERT INTO invoices VALUES (100, 1, 500.00, false);
                INSERT INTO invoices VALUES (101, 2, 750.00, false);
                """;

        List<Query> queries = parser.parseQueryBatch(sqlBatch);
        logger.info(parser.generateAnalysisReport(queries));

        // Step 2: Create execution plan
        ParallelQueryPlanner planner = new ParallelQueryPlanner();
        PhysicalQueryPlan plan = planner.createQueryPlan(queries);
        logger.info(planner.generateDetailedReport(plan));

        // Step 3: Create execution listener
        ParallelQueryExecutor.QueryExecutionListener listener = new ParallelQueryExecutor.QueryExecutionListener() {
            @Override
            public void onStageStart(int stageNumber) {
                logger.info("  ▶ Starting Stage " + stageNumber);
            }

            @Override
            public void onStageComplete(int stageNumber, long durationMs) {
                logger.info("  ✓ Stage " + stageNumber + " completed in " + durationMs + "ms");
            }

            @Override
            public void onQueryStart(String queryId) {
                logger.info("    → " + queryId + " started");
            }

            @Override
            public void onQueryComplete(String queryId, long durationMs) {
                logger.info("    ✓ " + queryId + " completed in " + durationMs + "ms");
            }

            @Override
            public void onQueryError(String queryId, Exception e) {
                logger.error("    ✗ " + queryId + " failed: " + e.getMessage());
            }

            @Override
            public void onExecutionComplete(long totalDurationMs) {
                logger.info("  ✓ All stages completed in " + totalDurationMs + "ms");
            }
        };

        // Step 4: Create executor and execute (with simulated executor)
        ParallelQueryExecutor executor = new ParallelQueryExecutor(4, listener);

        logger.info("\nExecution Trace:");
        logger.info("================");

        try {
            Map<String, ParallelQueryExecutor.QueryExecutionResult> results = executor.execute(plan, query -> {
                // Simulate query execution with small delay
                Thread.sleep(100 + (long) (Math.random() * 50));
                return new ParallelQueryExecutor.QueryExecutionResult(
                        query.getId(),
                        true,
                        100,
                        null,
                        "Query executed successfully");
            });

            logger.info(executor.getResultsSummary());

        } catch (InterruptedException e) {
            logger.error("Execution interrupted: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }
}
