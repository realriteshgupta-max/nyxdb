# Parallel Query Execution System

## Overview

The Parallel Query Execution System is a sophisticated framework for parsing, analyzing, and executing SQL queries in parallel. It automatically detects data dependencies between queries and creates an optimized execution plan that maximizes parallelism while maintaining correctness.

## Key Features

### 1. Query Parser (`ParallelQueryParser`)
- Parses SQL queries and extracts metadata needed for dependency analysis
- Identifies table and database operations (reads/writes)
- Integrates with existing NyxParser for schema information
- Supports batch processing of multiple SQL statements

### 2. Dependency Analyzer (`QueryDependencyAnalyzer`)
- Analyzes relationships between queries based on data dependencies
- Detects three types of dependencies:
  - **Read-After-Write (RAW)**: One query reads data another writes
  - **Write-After-Write (WAW)**: Multiple queries write to same table/database
  - **Write-After-Read (WAR)**: One query writes to data another reads
- Builds dependency graphs for visualization and analysis

### 3. Physical Query Planner (`ParallelQueryPlanner`)
- Creates optimized execution plans for parallel execution
- Uses topological sorting to order queries correctly
- Groups independent queries into execution stages
- Calculates parallelization metrics and performance estimates
- Generates detailed execution plans and analysis reports

### 4. Parallel Query Executor (`ParallelQueryExecutor`)
- Executes queries in parallel using thread pools
- Synchronizes execution stages with CountDownLatch
- Provides execution listeners for monitoring and logging
- Collects execution statistics and results
- Handles error conditions and timeouts

## Architecture

```
┌─────────────────────────────────────┐
│  SQL Query Batch                    │
│  (Multiple SQL Statements)          │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│  ParallelQueryParser                │
│  (Extract Metadata)                 │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│  QueryDependencyAnalyzer            │
│  (Build Dependency Graph)           │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│  ParallelQueryPlanner               │
│  (Create Execution Plan)            │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│  ParallelQueryExecutor              │
│  (Execute in Parallel)              │
└─────────────────────────────────────┘
```

## Usage Examples

### Example 1: Basic Parallel Query Execution

```java
// Step 1: Parse queries
ParallelQueryParser parser = new ParallelQueryParser();
String[] queries = {
    "CREATE TABLE users (id INT, name VARCHAR(50));",
    "CREATE TABLE products (id INT, title VARCHAR(100));",
    "CREATE TABLE orders (id INT, user_id INT, product_id INT);"
};
List<Query> parsedQueries = parser.parseMultipleStatements(queries);

// Step 2: Analyze dependencies
QueryDependencyAnalyzer analyzer = new QueryDependencyAnalyzer();
List<QueryDependencyAnalyzer.DependencyInfo> deps = analyzer.analyzeDependencies(parsedQueries);

// Step 3: Create execution plan
ParallelQueryPlanner planner = new ParallelQueryPlanner();
PhysicalQueryPlan plan = planner.createQueryPlan(parsedQueries);

// Step 4: Print plan details
System.out.println(planner.generateDetailedReport(plan));
```

### Example 2: Execute with Custom Executor

```java
// Create executor with listener
ParallelQueryExecutor.QueryExecutionListener listener = 
    new ParallelQueryExecutor.QueryExecutionListener() {
        @Override
        public void onStageStart(int stageNumber) {
            System.out.println("Stage " + stageNumber + " starting");
        }
        
        @Override
        public void onQueryComplete(String queryId, long durationMs) {
            System.out.println(queryId + " completed in " + durationMs + "ms");
        }
        
        // ... implement other callback methods
    };

ParallelQueryExecutor executor = new ParallelQueryExecutor(4, listener);

// Execute with custom query executor
Map<String, ParallelQueryExecutor.QueryExecutionResult> results = 
    executor.execute(plan, query -> {
        // Your custom query execution logic here
        // e.g., execute against database, file system, etc.
        return new ParallelQueryExecutor.QueryExecutionResult(
            query.getId(), 
            true,  // success
            100,   // execution time in ms
            null,  // no error
            "Results"
        );
    });

// Print execution summary
System.out.println(executor.getResultsSummary());
executor.shutdown();
```

### Example 3: Dependency Analysis

```java
// Create queries manually
Query q1 = new Query("Q1", "CREATE TABLE users ...", QueryType.CREATE_TABLE);
q1.addWriteTable("users");

Query q2 = new Query("Q2", "INSERT INTO users VALUES ...", QueryType.INSERT);
q2.addWriteTable("users");

// Analyze dependencies
QueryDependencyAnalyzer analyzer = new QueryDependencyAnalyzer();
List<QueryDependencyAnalyzer.DependencyInfo> deps = 
    analyzer.analyzeDependencies(List.of(q1, q2));

// Print dependencies
for (var dep : deps) {
    System.out.println(dep.getSource().getId() + " → " + 
                      dep.getTarget().getId() + " (" + 
                      dep.getType() + ")");
}
```

## Data Structures

### Query
Represents a single SQL query with metadata for parallel execution analysis.

**Properties:**
- `id`: Unique identifier
- `sql`: SQL statement text
- `type`: Query type (CREATE_TABLE, INSERT, DROP_TABLE, etc.)
- `readTables`: Set of tables the query reads from
- `writeTables`: Set of tables the query writes to
- `affectedDatabases`: Set of databases affected by the query

### ExecutionStage
Represents a stage of parallel execution where multiple queries can run concurrently.

**Properties:**
- `stageNumber`: Sequential stage identifier
- `queries`: List of queries in this stage
- `executionTimeMs`: Time taken to execute all queries in stage
- `completed`: Execution status

### PhysicalQueryPlan
Represents the complete execution plan.

**Properties:**
- `stages`: List of execution stages
- `totalQueries`: Total number of queries to execute
- `maxParallelism`: Maximum queries running concurrently
- `estimatedTotalTimeMs`: Estimated execution time

## Dependency Types

### Read-After-Write (RAW)
Query A must complete before Query B if A writes to tables that B reads.

```
Query A: CREATE TABLE users ...
Query B: INSERT INTO users VALUES ...
Dependency: A must complete before B
```

### Write-After-Write (WAW)
Queries that write to the same table must execute sequentially.

```
Query A: INSERT INTO users VALUES (1) ...
Query B: INSERT INTO users VALUES (2) ...
Dependency: Must maintain insertion order
```

### Write-After-Read (WAR)
Query A must complete before Query B if B writes to tables that A reads.

```
Query A: SELECT * FROM users ...
Query B: INSERT INTO users VALUES ...
Dependency: A must read before B modifies data
```

## Performance Metrics

### Parallelization Factor
Measures how effectively queries are parallelized.

```
Factor = Total Queries / Number of Stages
```

- Factor = 1.0: No parallelism (all queries sequential)
- Factor = N: N independent queries executed in single stage
- Higher is better

### Critical Path Length
The number of sequential stages in the execution plan.

```
Critical Path = Number of Stages
```

Lower critical path length means faster overall execution.

### Max Parallelism
Maximum number of queries executing concurrently.

```
Max Parallelism = max(queries per stage)
```

## Query Type Classification

The system recognizes the following query types:

- **CREATE_TABLE**: Create a new table
- **CREATE_DATABASE**: Create a new database
- **DROP_TABLE**: Delete a table
- **DROP_DATABASE**: Delete a database
- **INSERT**: Insert data into a table
- **ALTER_TABLE**: Modify table structure
- **TRUNCATE_TABLE**: Remove all rows from a table
- **UNKNOWN**: Unrecognized query type

## Running the Examples

```bash
# Compile the project
mvn clean compile

# Run the complete example
mvn exec:java -Dexec.mainClass="org.nyxdb.parser.core.parallel.ParallelQueryExecutionExample"

# Run the unit tests
mvn test -Dtest=ParallelQueryExecutionTest
```

## Implementation Details

### Dependency Detection Algorithm
1. For each pair of queries (Q1, Q2):
   - Check if Q1 writes and Q2 reads same tables → RAW dependency
   - Check if both Q1 and Q2 write to same tables → WAW dependency
   - Check if Q1 reads and Q2 writes to same tables → WAR dependency

### Execution Stage Creation
1. Calculate in-degree (incoming dependencies) for each query
2. Find all queries with in-degree 0 (no dependencies) → Stage 0
3. Remove Stage 0 queries from dependency graph
4. Repeat until all queries assigned
5. Preserve execution order using topological sort

### Parallel Execution
1. Create thread pool with configurable size
2. For each execution stage:
   - Submit all queries in stage to thread pool
   - Wait for all queries to complete (CountDownLatch)
   - Move to next stage
3. Collect results and statistics

## Error Handling

### Circular Dependencies
The system detects and reports circular dependencies:
```
IllegalStateException: "Circular dependency detected in queries"
```

### Execution Timeouts
Stage execution has a default 5-minute timeout:
```
InterruptedException: "Stage X execution timeout"
```

### Query Parsing Errors
Gracefully handles parsing errors, returning Query objects with available metadata:
```
Warning: Failed to parse query for dependency analysis: <error message>
```

## Future Enhancements

1. **Cost-based Scheduling**: Estimate query execution costs and schedule based on cost
2. **Transaction Support**: Handle transaction boundaries and isolation levels
3. **Distributed Execution**: Extend to distributed query execution across multiple nodes
4. **Cache Awareness**: Leverage query result caching to eliminate duplicate work
5. **Automated Indexing**: Suggest indexes based on query patterns
6. **Query Optimization**: Apply query rewriting and optimization techniques

## Performance Characteristics

- **Parsing**: O(n) where n is number of statements
- **Dependency Analysis**: O(n²) for pairwise dependency checking
- **Planning**: O(n log n) with topological sort
- **Execution**: Depends on individual query execution + synchronization overhead

## Thread Safety

- `PhysicalQueryPlan`: Immutable after construction (thread-safe for reading)
- `ParallelQueryExecutor`: Thread-safe using synchronized data structures
- `Query`: Immutable after metadata extraction

## Testing

Comprehensive unit tests cover:
- Query parsing and type identification
- Single and multi-stage execution plans
- Dependency detection (RAW, WAW, WAR)
- Parallelization factor calculation
- Complex dependency scenarios
- Batch query processing

Run tests with:
```bash
mvn test -Dtest=ParallelQueryExecutionTest
```

## References

- Query dependency analysis techniques from database systems literature
- Topological sorting for dependency-aware scheduling
- Thread pool pattern for parallel execution
- CountDownLatch for stage synchronization

---

**Version**: 1.0.0  
**Last Updated**: April 2026
