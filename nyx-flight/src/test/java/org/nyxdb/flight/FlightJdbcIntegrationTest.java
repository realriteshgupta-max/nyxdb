package org.nyxdb.flight;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Flight JDBC Integration Test
 * 
 * Requires Flight server running on localhost:8815 with useEncryption=false
 * Start with: ./run-flight-server.sh
 */
public class FlightJdbcIntegrationTest {

    private static final Logger logger = LogManager.getLogger(FlightJdbcIntegrationTest.class);
    private static final String FLIGHT_URL = "jdbc:arrow-flight-sql://localhost:8815?useEncryption=false";

    @Test
    // @Disabled("Requires Flight server running on localhost:8815 with
    // useEncryption=false")
    public void testFlightJdbcInsertSelect() throws SQLException {
        logger.info("Testing Flight JDBC client against {} ", FLIGHT_URL);

        try (Connection connection = DriverManager.getConnection(FLIGHT_URL)) {
            logger.info("✓ Connected to Flight server");

            try (Statement statement = connection.createStatement()) {
                // Create table
                logger.debug("Creating 'users' table");
                statement.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR)");
                logger.info("✓ Table created");

                // Insert data
                logger.debug("Inserting test data");
                statement.executeUpdate("INSERT INTO users VALUES (1, 'alice')");
                statement.executeUpdate("INSERT INTO users VALUES (2, 'bob')");
                logger.info("✓ Inserted 2 rows");

                // Query
                logger.debug("Executing SELECT query");
                try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM users")) {
                    rs.next();
                    int count = rs.getInt(1);
                    logger.info("✓ Query result: {} rows", count);
                    assertEquals(2, count);
                }
            }

        }
    }
}
