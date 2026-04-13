package org.nyxdb.flight;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;

/**
 * Tiny demo main for the nyx-flight module. This does NOT start a full
 * Flight SQL server; instead it demonstrates using `NyxFlightService` which
 * would be the core execution layer behind a Flight producer.
 *
 * Usage: run this main to exercise create table / insert / select against
 * an in-memory DuckDB instance.
 */
public class NyxFlightServerStarter {
    private static final Logger logger = LogManager.getLogger(NyxFlightServerStarter.class);

    public static void main(String[] args) throws Exception {
        // create an in-memory DuckDB manager (temporary per-connection instance)
        NyxFlightService service = new NyxFlightService();
        // start a Flight server on port 8815 using the service allocator.
        // Creating the allocator may trigger platform-specific native initialization
        // (Netty/Arrow). Catch initialization errors so the demo can still run.
        FlightServer server = null;
        try {

            /*
             * // Example: create a table, insert data, then select
             * String create =
             * "CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR);";
             * String insert1 = "INSERT INTO users VALUES (1, 'alice');";
             * String insert2 = "INSERT INTO users VALUES (2, 'bob');";
             * String select = "SELECT * FROM users ORDER BY id;";
             * service.executeSqlBatch(create + " " + insert1 + " " + insert2 + " " + select
             * + " ");
             */

            Location location = Location.forGrpcInsecure("0.0.0.0", 8815);
            server = FlightServer.builder(service.getAllocator(), location, service).build();
            server.start();
            logger.info("Nyx Flight server started at {}", location.getUri());
            server.awaitTermination();
        } catch (Throwable t) {
            logger.error("Could not start Flight server (allocator init failed): {}", t);
            server = null;
        }

        /*
         * // Example: create a table, insert data, then select
         * String create =
         * "CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR);";
         * String insert1 = "INSERT INTO users VALUES (1, 'alice');";
         * String insert2 = "INSERT INTO users VALUES (2, 'bob');";
         * String select = "SELECT * FROM users ORDER BY id;";
         * 
         * try {
         * service.executeSqlBatch(create + " " + insert1 + " " + insert2 + " " + select
         * + " ");
         * } finally {
         * // ensure we shutdown the manager to close connections
         * manager.shutdown();
         * try {
         * service.close();
         * } catch (Exception ignored) {
         * }
         * }
         * 
         * logger.info("Demo finished");
         */
    }
}
