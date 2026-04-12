package org.nyxdb.parser.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.nyxdb.parser.core.models.InsertInfo;
import java.util.List;

public class NyxDBInsertExtractTest {

    @Test
    public void testInsertIntoTable() {
        String sql = "INSERT INTO users (id, name) VALUES (1, 'alice');";
        NyxParser ex = NyxParser.extract(sql);
        List<InsertInfo> inserts = ex.getInserts();
        assertEquals(1, inserts.size());
        assertEquals("users", inserts.get(0).getTableName());
    }
}
