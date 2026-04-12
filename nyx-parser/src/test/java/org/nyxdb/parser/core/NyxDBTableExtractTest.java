package org.nyxdb.parser.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.nyxdb.parser.core.models.TableInfo;
import java.util.List;

public class NyxDBTableExtractTest {

    @Test
    public void testCreateTableExtract() {
        String sql = "CREATE TABLE users (id INT, name VARCHAR(20));";
        NyxParser ex = NyxParser.extract(sql);
        List<TableInfo> tables = ex.getTables();
        assertEquals(1, tables.size());
        assertEquals("users", tables.get(0).getName());
    }
}
