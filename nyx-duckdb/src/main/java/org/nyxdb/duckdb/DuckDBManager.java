package org.nyxdb.duckdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Lightweight DuckDB connection manager.
 *
 * Provides a tiny connection pool over JDBC connections and lifecycle methods
 * to create and shutdown connections. Designed for in-memory or file-backed
 * DuckDB instances via JDBC URL.
 */
public class DuckDBManager {
    private static final Logger logger = LogManager.getLogger(DuckDBManager.class);

    private final String jdbcUrl;
    private final Properties props;
    private final ConcurrentLinkedQueue<Connection> pool = new ConcurrentLinkedQueue<>();
    // Track all created connections so we can remove closed ones
    private final ConcurrentHashMap<Connection, Boolean> connectionMap = new ConcurrentHashMap<>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final int maxPoolSize;

    public DuckDBManager(String jdbcUrl) {
        this(jdbcUrl, new Properties(), 10);
    }

    public DuckDBManager(String jdbcUrl, Properties props) {
        this(jdbcUrl, props, 10);
    }

    public DuckDBManager(String jdbcUrl, Properties props, int maxPoolSize) {
        this.jdbcUrl = jdbcUrl;
        this.props = props == null ? new Properties() : props;
        this.maxPoolSize = maxPoolSize <= 0 ? 10 : maxPoolSize;
    }

    /**
     * Convenience factory for an in-memory DuckDB manager.
     * DuckDB's plain `jdbc:duckdb:` URL creates a separate temporary database per
     * connection. For pooled/shared access we use a private temp database file so
     * statements executed on different JDBC connections still see the same state.
     */
    public static DuckDBManager createInMemoryManager() {
        try {
            Path tempDb = Files.createTempFile("nyxdb-duckdb-", ".duckdb");
            Files.deleteIfExists(tempDb);
            tempDb.toFile().deleteOnExit();
            logger.info("Created temporary DuckDB database at {}", tempDb.toAbsolutePath());
            return new DuckDBManager("jdbc:duckdb:" + tempDb.toAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary DuckDB database", e);
        }
    }

    /**
     * Get a connection from the pool or create a new one.
     */
    public Connection getConnection() throws SQLException {
        if (shutdown.get()) {
            throw new SQLException("DuckDBManager is shutdown");
        }
        // Try to clean closed connections first
        removeClosedConnectionsFromMap();

        Connection c = pool.poll();
        if (c != null) {
            try {
                if (c.isClosed()) {
                    c = null;
                }
            } catch (SQLException e) {
                c = null;
            }
        }
        if (c == null) {
            // Explicitly load DuckDB driver to ensure it's used instead of other JDBC
            // drivers
            try {
                Class.forName("org.duckdb.DuckDBDriver");
            } catch (ClassNotFoundException e) {
                logger.warn("DuckDB driver not found in classpath: {}", e.getMessage());
            }
            logger.info("Opening new DuckDB connection to {}", jdbcUrl);
            c = DriverManager.getConnection(jdbcUrl, props);
            c.setAutoCommit(true);
            // Track it in the connection map
            connectionMap.put(c, Boolean.TRUE);
            logger.info("✓ DuckDB connection created: {} (pool size: {}/{}, total: {})",
                    c, pool.size(), maxPoolSize, connectionMap.size());
        }
        return c;
    }

    /**
     * Release a connection back to the pool. If the manager is shutdown the
     * connection will be closed.
     */
    public void releaseConnection(Connection c) {
        if (c == null)
            return;
        try {
            if (shutdown.get()) {
                if (!c.getAutoCommit()) {
                    c.commit();
                }
                c.close();
                return;
            }
            if (c.isClosed())
                return;
            if (!c.getAutoCommit()) {
                c.commit();
            }
            // Offer back to pool if below maxPoolSize, otherwise close
            if (pool.size() < maxPoolSize) {
                pool.offer(c);
            } else {
                try {
                    c.close();
                } finally {
                    connectionMap.remove(c);
                }
            }
        } catch (SQLException e) {
            logger.warn("Error while releasing connection: {}", e.getMessage());
        }
    }

    /**
     * Close all pooled connections and mark manager as shutdown.
     */
    public void shutdown() {
        if (!shutdown.compareAndSet(false, true))
            return;
        logger.info("Shutting down DuckDBManager, closing {} pooled connections", pool.size());
        Connection c;
        while ((c = pool.poll()) != null) {
            try {
                c.close();
            } catch (SQLException e) {
                logger.warn("Error closing connection during shutdown: {}", e.getMessage());
            }
        }
        // Close any remaining tracked connections
        for (Connection conn : connectionMap.keySet()) {
            try {
                if (conn != null && !conn.isClosed())
                    conn.close();
            } catch (SQLException e) {
                logger.warn("Error closing tracked connection during shutdown: {}", e.getMessage());
            }
        }
    }

    /**
     * Execute SQL statement and get the number of affected rows.
     * Useful for DDL and DML statements.
     */
    public int executeSql(String sql) throws SQLException {
        Connection c = getConnection();
        try (java.sql.Statement st = c.createStatement()) {
            return st.executeUpdate(sql);
        } finally {
            releaseConnection(c);
        }
    }

    /**
     * Explicitly close a connection and remove it from the map/pool.
     */
    public void closeConnection(Connection c) {
        if (c == null)
            return;
        try {
            if (!c.isClosed())
                c.close();
        } catch (SQLException e) {
            logger.warn("Error closing connection: {}", e.getMessage());
        } finally {
            connectionMap.remove(c);
            pool.remove(c);
        }
    }

    private void removeClosedConnectionsFromMap() {
        for (Connection conn : connectionMap.keySet()) {
            try {
                if (conn == null || conn.isClosed()) {
                    connectionMap.remove(conn);
                    pool.remove(conn);
                }
            } catch (SQLException e) {
                connectionMap.remove(conn);
                pool.remove(conn);
            }
        }
    }
}
