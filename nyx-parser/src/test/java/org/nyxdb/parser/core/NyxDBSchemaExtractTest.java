package org.nyxdb.parser.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.nyxdb.parser.core.models.TableInfo;
import org.nyxdb.parser.core.models.DatabaseInfo;
import java.util.List;

public class NyxDBSchemaExtractTest {

    @Test
    public void testSchemaExtract() {
        String sql = "CREATE TABLE myschema.users (id INT); CREATE DATABASE testdb;";
        NyxParser ex = NyxParser.extract(sql);
        List<TableInfo> tables = ex.getTables();
        List<DatabaseInfo> dbs = ex.getDatabases();
        assertEquals(1, tables.size());
        assertEquals("users", tables.get(0).getName());
        assertEquals(1, dbs.size());
        assertEquals("testdb", dbs.get(0).getName());
    }
}
