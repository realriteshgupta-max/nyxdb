package org.nyxdb.flight;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.nyxdb.duckdb.DuckDBManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class NyxFlightServiceTest {

    @Test
    public void testCreateInsertSelect() throws Exception {
        DuckDBManager manager = new DuckDBManager("jdbc:duckdb:");
        NyxFlightService service = new NyxFlightService(manager);

        String batch = "CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR);" +
                " INSERT INTO users VALUES (1, 'alice');" +
                " INSERT INTO users VALUES (2, 'bob');" +
                " SELECT id, name FROM users ORDER BY id;";

        Connection c = manager.getConnection();
        try {
            // Execute the batch on the same connection so state is shared
            service.executeSqlBatchOnConnection(c, batch);

            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM users")) {
                rs.next();
                int count = rs.getInt(1);
                assertEquals(2, count);
            }
        } finally {
            manager.closeConnection(c);
            manager.shutdown();
        }
    }
}
