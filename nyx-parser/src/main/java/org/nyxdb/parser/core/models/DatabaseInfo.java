package org.nyxdb.parser.core.models;

public class DatabaseInfo {
    private final String name;

    public DatabaseInfo(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "DatabaseInfo{" + "name='" + name + '\'' + '}';
    }
}
