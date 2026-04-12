package org.nyxdb.parser.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.nyxdb.parser.core.models.DatabaseInfo;
import java.util.List;

public class NyxDBDropExtractTest {

    @Test
    public void testDropDatabaseExtract() {
        String sql = "DROP DATABASE testdb;";
        NyxParser ex = NyxParser.extract(sql);
        List<DatabaseInfo> dbs = ex.getDatabases();
        assertEquals(1, dbs.size());
        assertEquals("testdb", dbs.get(0).getName());
    }

    @Test
    public void testDropTableExtract() {
        String sql = "DROP TABLE users;";
        NyxParser ex = NyxParser.extract(sql);
        assertEquals(1, ex.getDroppedTables().size());
        assertEquals("users", ex.getDroppedTables().get(0));
    }
}
