package org.nyxdb.parser.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.nyxdb.parser.core.models.DatabaseInfo;
import java.util.List;

public class NyxDBCreateDatabaseTest {

    @Test
    public void testCreateDatabaseExtract() {
        String sql = "CREATE DATABASE testdb;";
        NyxParser ex = NyxParser.extract(sql);
        List<DatabaseInfo> dbs = ex.getDatabases();
        assertEquals(1, dbs.size());
        assertEquals("testdb", dbs.get(0).getName());
    }
}
