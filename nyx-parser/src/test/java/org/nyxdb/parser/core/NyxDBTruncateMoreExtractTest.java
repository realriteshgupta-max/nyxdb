package org.nyxdb.parser.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class NyxDBTruncateMoreExtractTest {

    @Test
    public void testQuotedIdentifiers() {
        String sql = "TRUNCATE TABLE \"myschema\".\"users\";";
        NyxParser ex = NyxParser.extract(sql);
        assertEquals(1, ex.getTruncatedTables().size());
    }

    @Test
    public void testTruncateWithoutIfExists() {
        String sql = "TRUNCATE TABLE users;";
        NyxParser ex = NyxParser.extract(sql);
        assertEquals(1, ex.getTruncatedTables().size());
    }

    @Test
    public void testMultipleTruncates() {
        String sql = "TRUNCATE TABLE a; TRUNCATE TABLE b;";
        NyxParser ex = NyxParser.extract(sql);
        assertEquals(2, ex.getTruncatedTables().size());
    }

    @Test
    public void testLowercaseTruncate() {
        String sql = "truncate table users;";
        NyxParser ex = NyxParser.extract(sql);
        assertEquals(1, ex.getTruncatedTables().size());
    }
}
