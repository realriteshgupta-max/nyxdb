package org.nyxdb.parser.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class NyxDBSchemaQualifiedTest {

    @Test
    public void testSchemaQualifiedTable() {
        String sql = "CREATE TABLE myschema.users (id INT);";
        NyxParser ex = NyxParser.extract(sql);
        assertEquals(1, ex.getTables().size());
        assertEquals("users", ex.getTables().get(0).getName());
    }
}
