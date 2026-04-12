package org.nyxdb.parser.core.parallel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParallelQueryExecutionTest {

    private static final Logger logger = LogManager.getLogger(ParallelQueryExecutionTest.class);
    private ParallelQueryParser queryParser;
    private ParallelQueryPlanner queryPlanner;
    private QueryDependencyAnalyzer dependencyAnalyzer;

    @BeforeEach
    public void setUp() {
        queryParser = new ParallelQueryParser();
        queryPlanner = new ParallelQueryPlanner();
        dependencyAnalyzer = new QueryDependencyAnalyzer();
    }

    @Test
    public void testQueryParsing() {
        String sql = "CREATE TABLE users (id INT, name VARCHAR(50));";
        Query query = queryParser.parseQuery(sql, "Q1");

        assertNotNull(query);
        assertEquals("Q1", query.getId());
        assertEquals(QueryType.CREATE_TABLE, query.getType());
        assertTrue(query.getWriteTables().contains("users"));
    }

    @Test
    public void testMultipleStatementParsing() {
        String[] sqls = {
                "CREATE TABLE users (id INT);",
                "CREATE TABLE orders (id INT, user_id INT);",
                "INSERT INTO users VALUES (1);"
        };

        List<Query> queries = queryParser.parseMultipleStatements(sqls);

        assertEquals(3, queries.size());
        assertEquals(QueryType.CREATE_TABLE, queries.get(0).getType());
        assertEquals(QueryType.INSERT, queries.get(2).getType());
    }

    @Test
    public void testDependencyAnalysis_IndependentQueries() {
        Query q1 = new Query("Q1", "CREATE TABLE users (id INT);", QueryType.CREATE_TABLE);
        q1.addWriteTable("users");

        Query q2 = new Query("Q2", "CREATE TABLE products (id INT);", QueryType.CREATE_TABLE);
        q2.addWriteTable("products");

        List<Query> queries = List.of(q1, q2);
        List<QueryDependencyAnalyzer.DependencyInfo> dependencies = dependencyAnalyzer.analyzeDependencies(queries);

        assertTrue(dependencies.isEmpty(), "Independent queries should have no dependencies");
    }

    @Test
    public void testDependencyAnalysis_WriteAfterWrite() {
        Query q1 = new Query("Q1", "CREATE TABLE users (id INT);", QueryType.CREATE_TABLE);
        q1.addWriteTable("users");

        Query q2 = new Query("Q2", "INSERT INTO users VALUES (1);", QueryType.INSERT);
        q2.addWriteTable("users");

        List<Query> queries = List.of(q1, q2);
        List<QueryDependencyAnalyzer.DependencyInfo> dependencies = dependencyAnalyzer.analyzeDependencies(queries);

        assertFalse(dependencies.isEmpty(), "Should detect write-after-write dependency");
        QueryDependencyAnalyzer.DependencyInfo dep = dependencies.get(0);
        assertEquals(QueryDependencyAnalyzer.DependencyType.WRITE_AFTER_WRITE, dep.getType());
    }

    @Test
    public void testExecutionPlanGeneration_SingleStage() {
        Query q1 = new Query("Q1", "CREATE TABLE users (id INT);", QueryType.CREATE_TABLE);
        q1.addWriteTable("users");

        Query q2 = new Query("Q2", "CREATE TABLE products (id INT);", QueryType.CREATE_TABLE);
        q2.addWriteTable("products");

        List<Query> queries = List.of(q1, q2);
        PhysicalQueryPlan plan = queryPlanner.createQueryPlan(queries);

        assertEquals(1, plan.getStageCount(), "Independent queries should be in single stage");
        assertEquals(2, plan.getStage(0).getQueryCount());
    }

    @Test
    public void testExecutionPlanGeneration_MultipleStages() {
        Query q1 = new Query("Q1", "CREATE TABLE users (id INT);", QueryType.CREATE_TABLE);
        q1.addWriteTable("users");

        Query q2 = new Query("Q2", "INSERT INTO users VALUES (1);", QueryType.INSERT);
        q2.addWriteTable("users");

        Query q3 = new Query("Q3", "INSERT INTO users VALUES (2);", QueryType.INSERT);
        q3.addWriteTable("users");

        List<Query> queries = List.of(q1, q2, q3);
        PhysicalQueryPlan plan = queryPlanner.createQueryPlan(queries);

        assertTrue(plan.getStageCount() >= 2, "Dependent queries should be in multiple stages");
    }

    @Test
    public void testParallelizationFactor() {
        Query q1 = new Query("Q1", "CREATE TABLE t1 (id INT);", QueryType.CREATE_TABLE);
        Query q2 = new Query("Q2", "CREATE TABLE t2 (id INT);", QueryType.CREATE_TABLE);
        Query q3 = new Query("Q3", "CREATE TABLE t3 (id INT);", QueryType.CREATE_TABLE);

        List<Query> queries = List.of(q1, q2, q3);
        PhysicalQueryPlan plan = queryPlanner.createQueryPlan(queries);

        double factor = plan.getParallelizationFactor();
        assertTrue(factor >= 1.0, "Parallelization factor should be >= 1");
    }

    @Test
    public void testCanFullyParallelize() {
        Query q1 = new Query("Q1", "CREATE TABLE users (id INT);", QueryType.CREATE_TABLE);
        q1.addWriteTable("users");

        Query q2 = new Query("Q2", "CREATE TABLE products (id INT);", QueryType.CREATE_TABLE);
        q2.addWriteTable("products");

        List<Query> queries = List.of(q1, q2);
        assertTrue(queryPlanner.canFullyParallelize(queries),
                "Independent queries should be fully parallelizable");
    }

    @Test
    public void testQueryType_Identification() {
        assertEquals(QueryType.CREATE_TABLE,
                queryParser.parseQuery("CREATE TABLE t (id INT);", "Q1").getType());
        assertEquals(QueryType.INSERT,
                queryParser.parseQuery("INSERT INTO t VALUES (1);", "Q2").getType());
        assertEquals(QueryType.DROP_TABLE,
                queryParser.parseQuery("DROP TABLE t;", "Q3").getType());
        assertEquals(QueryType.ALTER_TABLE,
                queryParser.parseQuery("ALTER TABLE t ADD COLUMN name VARCHAR(50);", "Q4").getType());
    }

    @Test
    public void testDependencyGraph_Construction() {
        Query q1 = new Query("Q1", "CREATE TABLE t (id INT);", QueryType.CREATE_TABLE);
        q1.addWriteTable("t");

        Query q2 = new Query("Q2", "INSERT INTO t VALUES (1);", QueryType.INSERT);
        q2.addWriteTable("t");

        List<Query> queries = List.of(q1, q2);
        List<QueryDependencyAnalyzer.DependencyInfo> dependencies = dependencyAnalyzer.analyzeDependencies(queries);
        Map<String, Set<String>> graph = dependencyAnalyzer.buildDependencyGraph(dependencies);

        assertNotNull(graph);
        assertTrue(graph.containsKey("Q1"), "Dependency graph should contain Q1");
    }

    @Test
    public void testQueryBatchParsing() {
        String batch = """
                CREATE TABLE users (id INT);
                CREATE TABLE orders (id INT);
                INSERT INTO users VALUES (1);
                """;

        List<Query> queries = queryParser.parseQueryBatch(batch);
        assertEquals(3, queries.size(), "Should parse 3 queries from batch");
    }

    @Test
    public void testReadAndWriteTableTracking() {
        Query insertQuery = new Query("Q1", "INSERT INTO users VALUES (1);", QueryType.INSERT);
        insertQuery.addWriteTable("users");
        insertQuery.addReadTable("users");

        assertTrue(insertQuery.getWriteTables().contains("users"));
        assertTrue(insertQuery.getReadTables().contains("users"));
        assertFalse(insertQuery.isReadOnly(), "Write queries should not be read-only");
    }

    @Test
    public void testComplexDependencyScenario() {
        // Scenario: Multiple operations on same table
        Query createUsers = new Query("Q1", "CREATE TABLE users ...", QueryType.CREATE_TABLE);
        createUsers.addWriteTable("users");

        Query insertUser1 = new Query("Q2", "INSERT INTO users ...", QueryType.INSERT);
        insertUser1.addWriteTable("users");

        Query insertUser2 = new Query("Q3", "INSERT INTO users ...", QueryType.INSERT);
        insertUser2.addWriteTable("users");

        Query createOrders = new Query("Q4", "CREATE TABLE orders ...", QueryType.CREATE_TABLE);
        createOrders.addWriteTable("orders");

        List<Query> queries = List.of(createUsers, insertUser1, insertUser2, createOrders);
        PhysicalQueryPlan plan = queryPlanner.createQueryPlan(queries);

        // createUsers must be first
        assertEquals(createUsers, plan.getStage(0).getQueries().get(0));

        // insertUser1, insertUser2, createOrders can be in stage 1
        assertTrue(plan.getStageCount() >= 2, "Should have at least 2 stages");
    }

    @Test
    public void testSelectQueryParsing_WithJoinsAndAggregates() {
        ParallelQueryParser parser = new ParallelQueryParser();

        String selectWithJoin = "SELECT u.name, COUNT(o.id) FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY COUNT(o.id) DESC;";
        Query query = parser.parseQuery(selectWithJoin, "Q_select");

        assertEquals(QueryType.SELECT, query.getType());
        assertTrue(query.getReadTables().contains("users"));
        assertTrue(query.getReadTables().contains("orders"));
        assertTrue(query.hasJoin());
        assertTrue(query.hasOrderBy());
        assertTrue(query.hasGroupBy());
        assertTrue(query.getAggregates().contains("COUNT"));
    }

    @Test
    public void testMultipleSelectQueries_WithDependencies() {
        ParallelQueryParser parser = new ParallelQueryParser();

        // Create a batch with setup and SELECT queries
        Query createTable = new Query("Q0", "CREATE TABLE users ...", QueryType.CREATE_TABLE);
        createTable.addWriteTable("users");

        Query insertData = new Query("Q1", "INSERT INTO users ...", QueryType.INSERT);
        insertData.addWriteTable("users");

        Query selectQuery = new Query("Q2", "SELECT * FROM users WHERE id > 10;", QueryType.SELECT);
        selectQuery.addReadTable("users");

        List<Query> queries = List.of(createTable, insertData, selectQuery);
        QueryDependencyAnalyzer analyzer = new QueryDependencyAnalyzer();
        List<QueryDependencyAnalyzer.DependencyInfo> dependencies = analyzer.analyzeDependencies(queries);

        // Should have dependencies: Q0->Q1 (WAW), Q0->Q2 (RAW), Q1->Q2 (RAW)
        assertTrue(dependencies.size() >= 2, "Should have at least 2 dependencies");

        // Verify that SELECT depends on CREATE and INSERT
        boolean hasCreateToSelect = dependencies.stream()
                .anyMatch(d -> d.getSource().getId().equals("Q0") && d.getTarget().getId().equals("Q2"));
        assertTrue(hasCreateToSelect, "SELECT should depend on CREATE TABLE");
    }

    @Test
    public void testUpdateAndDeleteQueries() {
        ParallelQueryParser parser = new ParallelQueryParser();

        String updateQuery = "UPDATE users SET name='John' WHERE id=1;";
        Query update = parser.parseQuery(updateQuery, "Q_update");
        assertEquals(QueryType.UPDATE, update.getType());
        assertTrue(update.getWriteTables().contains("users"));

        String deleteQuery = "DELETE FROM users WHERE id=1;";
        Query delete = parser.parseQuery(deleteQuery, "Q_delete");
        assertEquals(QueryType.DELETE, delete.getType());
        assertTrue(delete.getWriteTables().contains("users"));
    }

    @Test
    public void testNestedQueryPhysicalPlanLogging() {
        String sql = "SELECT u.name, sub.total_spent " +
                "FROM users u " +
                "INNER JOIN (" +
                "  SELECT user_id, SUM(amount) as total_spent " +
                "  FROM orders " +
                "  GROUP BY user_id" +
                ") sub ON u.id = sub.user_id " +
                "ORDER BY sub.total_spent DESC;";

        // Parse the nested query to extract metadata for dependency analysis
        Query query = queryParser.parseQuery(sql, "Q_NESTED_JOIN");

        // 1. Generate and log the physical execution plan (scheduling and stages)
        PhysicalQueryPlan plan = queryPlanner.createQueryPlan(List.of(query));
        String report = queryPlanner.generateDetailedReport(plan);
        logger.info("--- Physical Execution Plan Report ---{}", report);

        // 2. Build and log the logical operator tree (showing the operator-level
        // physical structure)
        SelectQueryOptimizer optimizer = new SelectQueryOptimizer();
        org.nyxdb.parser.core.parallel.logical.LogicalQueryPlan logicalPlan = optimizer.buildLogicalPlan(query);
        if (logicalPlan != null) {
            logger.info("\n--- Logical Operator Tree (Physical Structure) ---\n{}", logicalPlan.printTree());
        }

        assertNotNull(plan);
        assertEquals(1, plan.getTotalQueries());
    }
}
