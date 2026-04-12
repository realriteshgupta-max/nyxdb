package org.nyxdb.parser.core.models;

import java.util.ArrayList;
import java.util.List;

public class InsertInfo {

    private final String table;
    private final List<String> columns = new ArrayList<>();
    private final List<List<String>> values = new ArrayList<>();

    public InsertInfo(String table) {
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public String getTableName() {
        return getTable();
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<List<String>> getValues() {
        return values;
    }

    public void addColumn(String c) {
        columns.add(c);
    }

    public void addValueRow(List<String> row) {
        values.add(row);
    }
}
