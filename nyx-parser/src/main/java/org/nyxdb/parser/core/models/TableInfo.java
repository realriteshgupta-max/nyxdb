package org.nyxdb.parser.core.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableInfo {
    private final String schema;
    private final String name;
    private final List<ColumnInfo> columns = new ArrayList<>();
    private final List<String> primaryKey = new ArrayList<>();
    private final List<ForeignKeyInfo> foreignKeys = new ArrayList<>();

    public TableInfo(String name) {
        this(null, name);
    }

    public TableInfo(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getSchema() {
        return schema;
    }

    public void addColumn(ColumnInfo col) {
        columns.add(col);
    }

    public boolean removeColumnByName(String name) {
        return columns.removeIf(c -> c.getName() != null && c.getName().equals(name));
    }

    public List<ColumnInfo> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public void addPrimaryKeyColumn(String col) {
        primaryKey.add(col);
    }

    public List<String> getPrimaryKeyColumns() {
        return Collections.unmodifiableList(primaryKey);
    }

    public List<String> getPrimaryKeys() {
        return getPrimaryKeyColumns();
    }

    public void addForeignKey(ForeignKeyInfo fk) {
        foreignKeys.add(fk);
    }

    public List<ForeignKeyInfo> getForeignKeys() {
        return Collections.unmodifiableList(foreignKeys);
    }

    @Override
    public String toString() {
        return "TableInfo{" + "name='" + name + '\'' + ", columns=" + columns + ", pk=" + primaryKey + ", fks="
                + foreignKeys + '}';
    }
}
