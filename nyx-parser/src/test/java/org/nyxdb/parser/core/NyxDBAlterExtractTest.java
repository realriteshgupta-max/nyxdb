package org.nyxdb.parser.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.nyxdb.parser.core.models.TableInfo;
import java.util.List;

public class NyxDBAlterExtractTest {

    @Test
    public void testAlterTableAddColumn() {
        String sql = "CREATE TABLE t (id INT); ALTER TABLE t ADD COLUMN name VARCHAR(20);";
        NyxParser ex = NyxParser.extract(sql);
        List<TableInfo> tables = ex.getTables();
        assertEquals(1, tables.size());
        assertEquals(2, tables.get(0).getColumns().size());
    }
}
