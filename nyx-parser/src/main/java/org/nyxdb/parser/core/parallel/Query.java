package org.nyxdb.parser.core.parallel;

import java.util.HashSet;
import java.util.Set;

public class Query {
    private final String id;
    private final String sql;
    private final QueryType type;
    private final Set<String> readTables;
    private final Set<String> writeTables;
    private final Set<String> affectedDatabases;
    private final Set<String> joinedTables; // Tables involved in JOIN operations
    private final Set<String> aggregates; // Aggregate functions (COUNT, SUM, AVG, MAX, MIN)
    private boolean hasOrderBy; // Whether query has ORDER BY clause
    private boolean hasGroupBy; // Whether query has GROUP BY clause
    private boolean hasJoin; // Whether query contains JOIN operations

    public Query(String id, String sql, QueryType type) {
        this.id = id;
        this.sql = sql;
        this.type = type;
        this.readTables = new HashSet<>();
        this.writeTables = new HashSet<>();
        this.affectedDatabases = new HashSet<>();
        this.joinedTables = new HashSet<>();
        this.aggregates = new HashSet<>();
        this.hasOrderBy = false;
        this.hasGroupBy = false;
        this.hasJoin = false;
    }

    // Getters
    public String getId() {
        return id;
    }

    public String getSql() {
        return sql;
    }

    public QueryType getType() {
        return type;
    }

    public Set<String> getReadTables() {
        return readTables;
    }

    public Set<String> getWriteTables() {
        return writeTables;
    }

    public Set<String> getAffectedDatabases() {
        return affectedDatabases;
    }

    public Set<String> getJoinedTables() {
        return joinedTables;
    }

    public Set<String> getAggregates() {
        return aggregates;
    }

    public boolean hasOrderBy() {
        return hasOrderBy;
    }

    public boolean hasGroupBy() {
        return hasGroupBy;
    }

    public boolean hasJoin() {
        return hasJoin;
    }

    // Setters
    public void addReadTable(String table) {
        readTables.add(table);
    }

    public void addWriteTable(String table) {
        writeTables.add(table);
    }

    public void addAffectedDatabase(String database) {
        affectedDatabases.add(database);
    }

    public void addJoinedTable(String table) {
        joinedTables.add(table);
        hasJoin = true;
    }

    public void addAggregate(String aggregateFunction) {
        aggregates.add(aggregateFunction);
    }

    public void setHasOrderBy(boolean hasOrderBy) {
        this.hasOrderBy = hasOrderBy;
    }

    public void setHasGroupBy(boolean hasGroupBy) {
        this.hasGroupBy = hasGroupBy;
    }

    public boolean isReadOnly() {
        return writeTables.isEmpty() && affectedDatabases.isEmpty();
    }

    public boolean isWriteQuery() {
        return !writeTables.isEmpty() || !affectedDatabases.isEmpty();
    }

    @Override
    public String toString() {
        return "Query{" +
                "id='" + id + '\'' +
                ", type=" + type +
                ", readTables=" + readTables +
                ", writeTables=" + writeTables +
                ", affectedDatabases=" + affectedDatabases +
                ", joinedTables=" + joinedTables +
                ", aggregates=" + aggregates +
                ", hasOrderBy=" + hasOrderBy +
                ", hasGroupBy=" + hasGroupBy +
                '}';
    }
}
