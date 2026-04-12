package org.nyxdb.parser.core.parallel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Analyzes dependencies between queries to determine which queries can be
 * executed in parallel.
 * Two queries are dependent if:
 * 1. One writes to a table/database that the other reads from (Read-After-Write
 * dependency)
 * 2. Both write to the same table/database (Write-After-Write dependency)
 * 3. One reads from a table/database that the other writes to (Write-After-Read
 * dependency)
 */
public class QueryDependencyAnalyzer {
    private static final Logger logger = LogManager.getLogger(QueryDependencyAnalyzer.class);

    public static class DependencyInfo {
        private final Query source;
        private final Query target;
        private final DependencyType type;

        public DependencyInfo(Query source, Query target, DependencyType type) {
            this.source = source;
            this.target = target;
            this.type = type;
        }

        public Query getSource() {
            return source;
        }

        public Query getTarget() {
            return target;
        }

        public DependencyType getType() {
            return type;
        }

        @Override
        public String toString() {
            return source.getId() + " (" + type + ") -> " + target.getId();
        }
    }

    public enum DependencyType {
        READ_AFTER_WRITE("RAW"), // source writes, target reads
        WRITE_AFTER_WRITE("WAW"), // source writes, target writes
        WRITE_AFTER_READ("WAR"), // source reads, target writes
        NONE("NONE"); // no dependency

        private final String symbol;

        DependencyType(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return symbol;
        }
    }

    /**
     * Analyzes dependencies between a list of queries.
     *
     * @param queries the list of queries to analyze
     * @return a list of dependency info
     */
    public List<DependencyInfo> analyzeDependencies(List<Query> queries) {
        logger.debug("Analyzing dependencies for {} queries", queries.size());
        List<DependencyInfo> dependencies = new ArrayList<>();

        for (int i = 0; i < queries.size(); i++) {
            Query query1 = queries.get(i);
            for (int j = i + 1; j < queries.size(); j++) {
                Query query2 = queries.get(j);

                // Check dependency from query1 to query2
                // Only check one direction to avoid circular dependencies
                // Earlier queries must execute before later queries if there's any conflict
                DependencyType depType = getDependencyType(query1, query2);
                if (depType != DependencyType.NONE) {
                    logger.debug("Found {} dependency: {} -> {}", depType, query1.getId(), query2.getId());
                    dependencies.add(new DependencyInfo(query1, query2, depType));
                }
            }
        }

        return dependencies;
    }

    /**
     * Determines the dependency type from source to target query.
     */
    private DependencyType getDependencyType(Query source, Query target) {
        // Write-After-Write: source writes to something that target also writes to
        if (hasOverlap(source.getWriteTables(), target.getWriteTables()) ||
                hasOverlap(source.getAffectedDatabases(), target.getAffectedDatabases())) {
            return DependencyType.WRITE_AFTER_WRITE;
        }

        // Read-After-Write: source writes to something that target reads
        if (hasOverlap(source.getWriteTables(), target.getReadTables()) ||
                hasOverlap(source.getAffectedDatabases(), target.getAffectedDatabases())) {
            return DependencyType.READ_AFTER_WRITE;
        }

        // Write-After-Read: source reads from something that target writes to
        if (hasOverlap(source.getReadTables(), target.getWriteTables())) {
            return DependencyType.WRITE_AFTER_READ;
        }

        return DependencyType.NONE;
    }

    /**
     * Checks if two sets have any common elements.
     */
    private boolean hasOverlap(Set<String> set1, Set<String> set2) {
        for (String item1 : set1) {
            if (set2.contains(item1)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets all queries that can be executed in parallel with a given query.
     */
    public Set<Query> getParallelizableQueries(Query query, List<Query> allQueries,
            List<DependencyInfo> dependencies) {
        Set<Query> parallelQueries = new HashSet<>();

        for (Query other : allQueries) {
            if (query.equals(other)) {
                continue;
            }

            boolean hasDirectDependency = dependencies.stream()
                    .anyMatch(dep -> (dep.getSource().getId().equals(query.getId()) &&
                            dep.getTarget().getId().equals(other.getId())) ||
                            (dep.getSource().getId().equals(other.getId()) &&
                                    dep.getTarget().getId().equals(query.getId())));

            if (!hasDirectDependency) {
                parallelQueries.add(other);
            }
        }

        return parallelQueries;
    }

    /**
     * Builds a dependency graph represented as an adjacency map.
     */
    public Map<String, Set<String>> buildDependencyGraph(List<DependencyInfo> dependencies) {
        Map<String, Set<String>> graph = new HashMap<>();

        for (DependencyInfo dep : dependencies) {
            graph.computeIfAbsent(dep.getSource().getId(), k -> new HashSet<>())
                    .add(dep.getTarget().getId());
        }

        return graph;
    }
}
