package org.nyxdb.parser.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.nyxdb.parser.core.models.TableInfo;
import java.util.List;

public class NyxDBKeyExtractTest {

    @Test
    public void testPrimaryKeyExtract() {
        String sql = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(20));";
        NyxParser ex = NyxParser.extract(sql);
        List<TableInfo> tables = ex.getTables();
        assertEquals(1, tables.size());
        assertEquals(1, tables.get(0).getPrimaryKeys().size());
    }

    @Test
    public void testForeignKeyExtract() {
        String sql = "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id));";
        NyxParser ex = NyxParser.extract(sql);
        List<TableInfo> tables = ex.getTables();
        assertEquals(1, tables.size());
        assertEquals(1, tables.get(0).getForeignKeys().size());
    }
}
