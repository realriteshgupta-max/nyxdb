# Distributed SELECT Query Execution Architecture

## Overview

The NyxDB distributed SELECT query execution system provides a complete physical query planner for executing complex SELECT queries in parallel across a distributed cluster. It extends the existing parallel query execution framework with specialized operators, optimization strategies, and distributed execution planning.

## Key Features

### 1. **Logical Query Plan Generation**
- Converts SELECT statements into operator trees
- Supports complex queries with JOINs, aggregations, sorting, and filtering
- Builds intermediate representations independent of execution engine

### 2. **Operator-Based Query Model**
Implements a rich set of logical operators:
- **SCAN**: Table data access
- **FILTER**: WHERE and HAVING clause evaluation
- **JOIN**: Multi-table joining with various strategies
- **AGGREGATE**: GROUP BY and aggregate function computation
- **SORT**: ORDER BY clause execution
- **PROJECT**: Column selection and expression evaluation

### 3. **Parallel JOIN Optimization**
- Analyzes join dependencies for parallelization potential
- Selects optimal join strategies based on table statistics:
  - **BROADCAST JOIN**: Replicates small table for local joins
  - **HASH JOIN**: Partitions both tables on join key
  - **SHUFFLE JOIN**: Distributed hash join for large tables
  - **SORT MERGE**: For pre-sorted inputs
  - **NESTED LOOP**: Fallback strategy

### 4. **Subquery and Nested Query Support**
- Extracts and identifies nested SELECT queries
- Handles subqueries in FROM, WHERE, SELECT, and JOIN clauses
- Detects nesting depth for optimization

### 5. **Distributed Execution Planning**
- Maps operators to execution nodes
- Manages data flow between distributed components
- Provides estimated execution times and cost metrics

### 6. **Cost-Based Optimization**
- Estimates operator costs based on table statistics
- Optimizes join order for reduced execution time
- Provides parallelization metrics for distributed clusters

## Architecture Components

### Logical Query Planning

**Classes:**
- `LogicalOperator`: Base class for all operators
- `ScanOperator`: Table access
- `FilterOperator`: Predicate evaluation
- `JoinOperator`: Join operations
- `AggregateOperator`: GROUP BY and aggregates
- `SortOperator`: Sorting
- `ProjectOperator`: Column projection
- `LogicalQueryPlan`: Operator tree representation

**Key Methods:**
```java
LogicalQueryPlan buildLogicalPlan(Query query);
List<LogicalOperator> getAllOperators();
long getTotalEstimatedCost();
List<JoinOperator> getJoinOperators();
```

### Query Optimization

**Classes:**
- `SelectQueryOptimizer`: Builds and optimizes SELECT queries
- `JoinOptimizer`: Analyzes and optimizes JOIN operations
- `SubqueryExtractor`: Identifies nested queries

**Features:**
```
- Logical plan construction from SQL
- JOIN strategy selection based on table statistics
- Predicate push-down for early filtering
- Cost estimation for optimization decisions
```

### Distributed Execution

**Classes:**
- `DistributedSelectExecutor`: Manages distributed execution
- `ExecutionNode`: Represents cluster nodes
- `DistributedPlan`: Physical execution plan for cluster

**Capabilities:**
```
- Operator placement on cluster nodes
- Distributed data flow coordination
- Node failure handling
- Execution monitoring and metrics
```

## Usage Example

### Basic SELECT with JOINs

```java
// Step 1: Parse SELECT query
ParallelQueryParser parser = new ParallelQueryParser();
Query query = parser.parseQuery(
    "SELECT u.name, SUM(o.amount) FROM users u " +
    "LEFT JOIN orders o ON u.id = o.user_id " +
    "GROUP BY u.id, u.name ORDER BY SUM(o.amount) DESC",
    "Q1"
);

// Step 2: Build logical plan
SelectQueryOptimizer optimizer = new SelectQueryOptimizer();
LogicalQueryPlan logicalPlan = optimizer.buildLogicalPlan(query);

// Step 3: Analyze and optimize JOINs
List<JoinOperator> joins = logicalPlan.getJoinOperators();
Map<String, TableStats> tableStats = new HashMap<>();
tableStats.put("users", new TableStats("users", 100000, 50_000_000));
tableStats.put("orders", new TableStats("orders", 1000000, 500_000_000));

JoinOptimizer joinOptimizer = new JoinOptimizer();
joinOptimizer.optimizeJoins(joins, tableStats);
// Recommends: "Highly parallelizable - use distributed hash join strategy"

// Step 4: Create distributed execution plan
List<ExecutionNode> cluster = Arrays.asList(
    new ExecutionNode("node-1", "10.0.0.1", 5432),
    new ExecutionNode("node-2", "10.0.0.2", 5432),
    new ExecutionNode("node-3", "10.0.0.3", 5432)
);

DistributedSelectExecutor executor = new DistributedSelectExecutor(cluster);
DistributedSelectExecutor.DistributedPlan distributedPlan = 
    executor.createDistributedPlan(logicalPlan);

// Step 5: Execute
ExecutionResult result = executor.execute(distributedPlan);
System.out.println("Execution time: " + result.executionTimeMs + "ms");
```

### Complex Query with Aggregation

```java
String sql = 
    "SELECT " +
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
    "ORDER BY total_sales DESC LIMIT 10";

// Parse and optimize
ParallelQueryParser parser = new ParallelQueryParser();
Query query = parser.parseQuery(sql, "Q_AGG");

// Logical plan shows:
// - SCAN users, SCAN orders
// - WHERE filter on created_date
// - LEFT JOIN on user_id
// - GROUP BY region with aggregates
// - HAVING filter
// - Optional SORT and LIMIT
```

## Query Plan Example

For this query:
```sql
SELECT u.name, COUNT(o.id) FROM users u 
INNER JOIN orders o ON u.id = o.user_id 
WHERE o.amount > 100 
GROUP BY u.id ORDER BY COUNT(o.id) DESC
```

The logical operator tree looks like:

```
PROJECT[columns=[u.name, COUNT(o.id)]]
├─ SORT[ORDER BY COUNT(o.id) DESC]
│  └─ AGGREGATE[GROUP BY u.id, functions=[COUNT(o.id)]]
│     └─ JOIN[INNER, strategy=HASH_JOIN]
│        ├─ FILTER[WHERE o.amount > 100]
│        │  └─ SCAN[orders]
│        └─ SCAN[users]
```

## JOIN Optimization Strategies

### Table Statistics (cost-based selection)

| Strategy | Best For | Network Cost | Memory |
|----------|----------|--------------|--------|
| BROADCAST | Small table < 100MB | Low | High |
| HASH JOIN | Medium tables | Medium | Medium |
| SHUFFLE | Large tables | High | Low |
| SORT MERGE | Pre-sorted input | Low | Low |
| NESTED LOOP | Rare cases | Very High | Very Low |

### Parallelization Analysis

```java
JoinOptimizer.JoinParallelizationPlan plan = 
    joinOptimizer.analyzeDependencies(joins);
// Returns:
// - Parallelizable joins (INNER, CROSS)
// - Sequential joins (LEFT, RIGHT, FULL)
// - Parallelization potential percentage
```

## Subquery Handling

The system identifies and optimizes nested queries:

```java
SubqueryExtractor extractor = new SubqueryExtractor();

// Detects:
List<SubqueryInfo> subqueries = extractor.extractSubqueries(sql);
// - FROM subqueries: FROM (SELECT ...) alias
// - WHERE subqueries: WHERE id IN (SELECT ...)
// - JOIN subqueries: JOIN (SELECT ...) ON condition
// - Nesting depth for multi-level queries
```

## Distributed Execution Nodes

### Node Configuration

```java
ExecutionNode node = new ExecutionNode("node-1", "10.0.0.1", 5432);
node.estimatedCapacity = 1_000_000; // rows/sec
```

### Operator Placement

The executor maps operators to nodes:
```
Operator Placement:
  scan_users -> node-1
  filter_where -> node-2
  join_orders -> node-3
  aggregate_sum -> node-1
  sort -> node-2
  project -> node-1
```

### Execution Coordination

- Operators communicate through distributed channels
- Data shuffling for non-collocated joins
- Synchronization barriers between stages
- Optional result caching

## Cost Estimation Model

```
Join Cost = (left_rows × right_rows) × strategy_factor

strategy_factor:
  - BROADCAST: 10
  - HASH_JOIN: 2
  - SHUFFLE: 3 × log(n) scale factor
  - SORT_MERGE: log(n) scale factor
```

## Performance Characteristics

- **Single Table Scans**: O(n) where n = row count
- **Hash Joins**: O(n + m) where n, m = table sizes
- **Shuffle Joins**: O(n log n + m log m) + network cost
- **Aggregation**: O(n log k) where k = group cardinality
- **Sorting**: O(n log n) with distribution overhead

## Extensibility

### Adding Custom Operators

```java
public class CustomPushVectorOperator extends LogicalOperator {
    public CustomPushVectorOperator(String opId, Set<String> cols) {
        super(opId, cols, new HashSet<>());
    }

    @Override
    public String getOperatorType() {
        return "CUSTOM_PUSH_VECTOR";
    }

    @Override
    public boolean validate() {
        return !inputs.isEmpty();
    }
}
```

### Custom Optimization Rules

Extend `SelectQueryOptimizer` to add domain-specific optimizations:
- Vector operations
- GPU acceleration
- Columnar processing
- Approximate query processing

## Not Affected by These Changes

The following query types continue to operate normally without SELECT-specific optimizations:

- **DDL**: CREATE TABLE, DROP TABLE, ALTER TABLE, etc.
- **Basic DML**: INSERT, UPDATE, DELETE
- **Other Queries**: TRUNCATE, CREATE DATABASE, etc.

These are routed through the original query execution path and are unaffected by the distributed SELECT framework.

## Files Added/Modified

### New Files (10)
1. `LogicalOperator.java` - Base operator class
2. `ScanOperator.java` - Table scan
3. `FilterOperator.java` - Filtering
4. `JoinOperator.java` - Join operations
5. `AggregateOperator.java` - Aggregation
6. `SortOperator.java` - Sorting
7. `ProjectOperator.java` - Projection
8. `LogicalQueryPlan.java` - Plan representation
9. `SubqueryExtractor.java` - Subquery identification
10. `SelectQueryOptimizer.java` - Logical plan building
11. `JoinOptimizer.java` - JOIN optimization
12. `DistributedSelectExecutor.java` - Distributed execution
13. `DistributedSelectExample.java` - Comprehensive examples

### Files Not Modified
- `ParallelQueryParser.java` - Continues to work for all query types
- `Query.java` - Enhanced set methods preserved
- `QueryDependencyAnalyzer.java` - Still handles dependencies
- `ParallelQueryPlanner.java` - Handles all query types
- `ParallelQueryExecutor.java` - Handles all query types

## Compilation & Testing

```bash
mvn clean compile       # Verify no errors
mvn test                # Run all 30 tests
mvn exec:java -Dexec.mainClass=\
  org.nyxdb.parser.core.parallel.DistributedSelectExample
```

All 30 existing tests pass without modification.

## Future Enhancements

1. **Cost Model Refinement**
   - Collect runtime statistics
   - Machine learning-based cost prediction
   - Adaptive strategy selection

2. **Advanced Join Strategies**
   - Semi-join optimization for skewed data
   - Bloom filter pushdown
   - Sideways information passing

3. **Subquery Optimization**
   - Subquery materialization strategies
   - Scalar subquery caching
   - Correlation detection and elimination

4. **Column-Level Optimization**
   - Column pruning
   - Predicate pushdown to column stores
   - Vectorized execution

5. **Distributed Features**
   - Fault tolerance and recovery
   - Adaptive partitioning
   - Dynamic resource allocation

## Summary

The distributed SELECT query execution system provides a production-ready framework for:
- Building logical query plans from SQL
- Optimizing complex JOINs for parallel execution
- Planning distributed execution across clusters
- Handling subqueries and nested operations
- Estimating and managing costs

All while maintaining backward compatibility with existing query types and execution patterns.
