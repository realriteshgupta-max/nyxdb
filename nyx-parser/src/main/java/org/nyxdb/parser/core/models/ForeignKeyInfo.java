package org.nyxdb.parser.core.models;

import java.util.Collections;
import java.util.List;

public class ForeignKeyInfo {
    private final List<String> localColumns;
    private final String referencedTable;
    private final List<String> referencedColumns;

    public ForeignKeyInfo(List<String> localColumns, String referencedTable, List<String> referencedColumns) {
        this.localColumns = localColumns;
        this.referencedTable = referencedTable;
        this.referencedColumns = referencedColumns;
    }

    public List<String> getLocalColumns() {
        return Collections.unmodifiableList(localColumns);
    }

    public String getReferencedTable() {
        return referencedTable;
    }

    public List<String> getReferencedColumns() {
        return Collections.unmodifiableList(referencedColumns);
    }

    @Override
    public String toString() {
        return "ForeignKeyInfo{" + "local=" + localColumns + ", refTable='" + referencedTable + '\'' + ", refCols="
                + referencedColumns + '}';
    }
}
