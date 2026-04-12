package org.nyxdb.parser.core.parallel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Executes queries in parallel according to a physical query plan.
 * Manages thread pools and synchronization between execution stages.
 */
public class ParallelQueryExecutor {
    private static final Logger logger = LogManager.getLogger(ParallelQueryExecutor.class);
    private final ExecutorService executorService;
    private final int maxThreads;
    private final QueryExecutionListener listener;
    private final Map<String, QueryExecutionResult> results;

    public interface QueryExecutor {
        QueryExecutionResult execute(Query query) throws Exception;
    }

    public interface QueryExecutionListener {
        void onStageStart(int stageNumber);

        void onStageComplete(int stageNumber, long durationMs);

        void onQueryStart(String queryId);

        void onQueryComplete(String queryId, long durationMs);

        void onQueryError(String queryId, Exception e);

        void onExecutionComplete(long totalDurationMs);
    }

    public static class QueryExecutionResult {
        private final String queryId;
        private final boolean success;
        private final long executionTimeMs;
        private final Exception error;
        private final String resultMessage;

        public QueryExecutionResult(String queryId, boolean success, long executionTimeMs,
                Exception error, String resultMessage) {
            this.queryId = queryId;
            this.success = success;
            this.executionTimeMs = executionTimeMs;
            this.error = error;
            this.resultMessage = resultMessage;
        }

        public String getQueryId() {
            return queryId;
        }

        public boolean isSuccess() {
            return success;
        }

        public long getExecutionTimeMs() {
            return executionTimeMs;
        }

        public Exception getError() {
            return error;
        }

        public String getResultMessage() {
            return resultMessage;
        }

        @Override
        public String toString() {
            return "QueryExecutionResult{" +
                    "queryId='" + queryId + '\'' +
                    ", success=" + success +
                    ", executionTimeMs=" + executionTimeMs +
                    '}';
        }
    }

    public ParallelQueryExecutor(int maxThreads) {
        this(maxThreads, null);
    }

    public ParallelQueryExecutor(int maxThreads, QueryExecutionListener listener) {
        this.maxThreads = maxThreads;
        this.executorService = Executors.newFixedThreadPool(maxThreads);
        this.listener = listener;
        this.results = new HashMap<>();
    }

    /**
     * Executes a physical query plan using the provided executor.
     *
     * @param plan     the physical query plan
     * @param executor the query executor implementation
     * @return execution results for all queries
     * @throws InterruptedException if execution is interrupted
     */
    public Map<String, QueryExecutionResult> execute(PhysicalQueryPlan plan, QueryExecutor executor)
            throws InterruptedException {
        logger.info("Starting parallel execution of {} stages with {} total queries",
                plan.getStageCount(), plan.getTotalQueries());
        long totalStartTime = System.currentTimeMillis();

        for (ExecutionStage stage : plan.getStages()) {
            executeStage(stage, executor);
        }

        long totalDuration = System.currentTimeMillis() - totalStartTime;
        logger.info("Parallel execution completed in {}ms", totalDuration);
        if (listener != null) {
            listener.onExecutionComplete(totalDuration);
        }

        return results;
    }

    /**
     * Executes a single execution stage in parallel.
     */
    private void executeStage(ExecutionStage stage, QueryExecutor executor)
            throws InterruptedException {
        logger.debug("Executing stage {} with {} queries", stage.getStageNumber(), stage.getQueryCount());
        long stageStartTime = System.currentTimeMillis();

        if (listener != null) {
            listener.onStageStart(stage.getStageNumber());
        }

        CountDownLatch latch = new CountDownLatch(stage.getQueryCount());
        AtomicInteger completedCount = new AtomicInteger(0);

        for (Query query : stage.getQueries()) {
            executorService.submit(() -> {
                long queryStartTime = System.currentTimeMillis();
                if (listener != null) {
                    listener.onQueryStart(query.getId());
                }

                try {
                    QueryExecutionResult result = executor.execute(query);
                    results.put(query.getId(), result);

                    long queryDuration = System.currentTimeMillis() - queryStartTime;
                    if (listener != null) {
                        listener.onQueryComplete(query.getId(), queryDuration);
                    }

                } catch (Exception e) {
                    if (listener != null) {
                        listener.onQueryError(query.getId(), e);
                    }
                    results.put(query.getId(),
                            new QueryExecutionResult(query.getId(), false,
                                    System.currentTimeMillis() - queryStartTime, e, null));
                } finally {
                    completedCount.incrementAndGet();
                    latch.countDown();
                }
            });
        }

        // Wait for all queries in this stage to complete
        boolean completed = latch.await(5, TimeUnit.MINUTES);
        if (!completed) {
            throw new InterruptedException("Stage " + stage.getStageNumber() + " execution timeout");
        }

        long stageDuration = System.currentTimeMillis() - stageStartTime;
        stage.setExecutionTimeMs(stageDuration);
        stage.markCompleted();

        if (listener != null) {
            listener.onStageComplete(stage.getStageNumber(), stageDuration);
        }
    }

    /**
     * Gets the results of query execution.
     */
    public Map<String, QueryExecutionResult> getResults() {
        return results;
    }

    /**
     * Gets execution results summary.
     */
    public String getResultsSummary() {
        StringBuilder summary = new StringBuilder();
        summary.append("\n===== EXECUTION RESULTS =====\n");
        summary.append("Total Queries: ").append(results.size()).append("\n");

        long successCount = results.values().stream().filter(QueryExecutionResult::isSuccess).count();
        summary.append("Successful: ").append(successCount).append("\n");
        summary.append("Failed: ").append(results.size() - successCount).append("\n");

        long totalTime = results.values().stream()
                .mapToLong(QueryExecutionResult::getExecutionTimeMs).sum();
        summary.append("Total Execution Time: ").append(totalTime).append("ms\n");

        long avgTime = results.isEmpty() ? 0 : totalTime / results.size();
        summary.append("Average Query Time: ").append(avgTime).append("ms\n");

        summary.append("\nDetailed Results:\n");
        results.forEach((queryId, result) -> {
            summary.append("  ").append(queryId).append(": ");
            if (result.isSuccess()) {
                summary.append("SUCCESS (").append(result.getExecutionTimeMs()).append("ms)");
            } else {
                summary.append("FAILED - ").append(result.getError().getMessage());
            }
            summary.append("\n");
        });

        return summary.toString();
    }

    /**
     * Shuts down the executor.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
