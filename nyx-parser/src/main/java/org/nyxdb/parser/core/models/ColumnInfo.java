package org.nyxdb.parser.core.models;

public class ColumnInfo {
    private final String name;
    private final String type;

    public ColumnInfo(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "ColumnInfo{" + "name='" + name + '\'' + ", type='" + type + '\'' + '}';
    }
}
