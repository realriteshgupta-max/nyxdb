package org.nyxdb.flight;

import org.nyxdb.duckdb.DuckDBManager;
import org.nyxdb.parser.core.parallel.ParallelQueryParser;
import org.nyxdb.parser.core.parallel.Query;
import org.nyxdb.parser.core.parallel.QueryType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.flight.OutboundStreamListener;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.CallStatus;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.flight.sql.impl.FlightSql;

/**
 * Minimal service that would back a Flight SQL server.
 *
 * It uses the existing `ParallelQueryParser` to split a SQL batch and
 * executes statements against a provided `DuckDBManager`.
 *
 * This is intentionally small: a full Flight SQL producer requires deeper
 * integration. This class focuses on wiring parser -> execution on DuckDB.
 */
public class NyxFlightService extends NoOpFlightSqlProducer {
    private static final Logger logger = LogManager.getLogger(NyxFlightService.class);

    private final DuckDBManager dbManager;
    private final ParallelQueryParser parser = new ParallelQueryParser();
    // Lazily initialize the allocator to avoid triggering Arrow/Netty static
    // initialization during tests that don't need Flight streaming.
    private volatile BufferAllocator allocator;
    // prepared statement handle -> SQL
    private final Map<String, String> preparedStatements = new ConcurrentHashMap<>();

    public NyxFlightService(DuckDBManager dbManager) {
        this.dbManager = dbManager;
    }

    /**
     * Execute a batch of SQL statements. Each parsed Query is executed in order.
     * SELECT statements will print up to the first 10 rows to stdout.
     */
    public void executeSqlBatch(String sqlBatch) throws SQLException {
        logger.info("Parsing and executing SQL batch");
        // Execute the batch using a single connection so statements see each other's
        // changes
        Connection c = dbManager.getConnection();
        try {
            executeSqlBatchOnConnection(c, sqlBatch);
        } finally {
            dbManager.releaseConnection(c);
        }
    }

    /**
     * Execute the SQL batch using a provided connection. Caller is responsible
     * for managing the connection lifecycle.
     */
    public void executeSqlBatchOnConnection(Connection c, String sqlBatch) throws SQLException {
        List<Query> queries = parser.parseQueryBatch(sqlBatch);
        try (Statement st = c.createStatement()) {
            for (Query q : queries) {
                String sql = q.getSql();
                QueryType type = q.getType();
                logger.info("Executing query {} -> type {}", q.getId(), type);
                if (type == QueryType.SELECT) {
                    try (ResultSet rs = st.executeQuery(sql)) {
                        int cols = rs.getMetaData().getColumnCount();
                        int printed = 0;
                        while (rs.next() && printed < 10) {
                            StringBuilder row = new StringBuilder();
                            for (int i = 1; i <= cols; i++) {
                                if (i > 1)
                                    row.append(" | ");
                                row.append(rs.getString(i));
                            }
                            System.out.println(row.toString());
                            printed++;
                        }
                    }
                } else {
                    int count = st.executeUpdate(sql);
                    logger.info("Statement affected {} rows", count);
                }
            }
        }
    }

    private void executeQuery(Query q) throws SQLException {
        String sql = q.getSql();
        QueryType type = q.getType();
        logger.info("Executing query {} -> type {}", q.getId(), type);
        try (Connection c = dbManager.getConnection(); Statement st = c.createStatement()) {
            if (type == QueryType.SELECT) {
                try (ResultSet rs = st.executeQuery(sql)) {
                    int cols = rs.getMetaData().getColumnCount();
                    int printed = 0;
                    while (rs.next() && printed < 10) {
                        StringBuilder row = new StringBuilder();
                        for (int i = 1; i <= cols; i++) {
                            if (i > 1)
                                row.append(" | ");
                            row.append(rs.getString(i));
                        }
                        System.out.println(row.toString());
                        printed++;
                    }
                }
            } else {
                int count = st.executeUpdate(sql);
                logger.info("Statement affected {} rows", count);
            }
        }
    }

    // FlightProducer implementations (minimal, rows returned as a single-string
    // column named "row")

    private Schema textRowSchema() {
        Field f = new Field("row", FieldType.nullable(new ArrowType.Utf8()), /* children */ null);
        return new Schema(Collections.singletonList(f));
    }

    private FlightEndpoint endpointForSql(String sql) {
        Ticket t = new Ticket(sql.getBytes());
        Location loc = Location.forGrpcInsecure("0.0.0.0", 8815);
        return new FlightEndpoint(t, loc);
    }

    private BufferAllocator getAllocator() {
        BufferAllocator a = allocator;
        if (a == null) {
            synchronized (this) {
                if (allocator == null) {
                    allocator = new RootAllocator(Long.MAX_VALUE);
                }
                a = allocator;
            }
        }
        return a;
    }

    // ----- Flight SQL specifics -----
    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        String sql = command.getQuery();
        Schema schema = textRowSchema();
        List<FlightEndpoint> endpoints = new ArrayList<>();
        endpoints.add(endpointForSql(sql));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command, FlightProducer.CallContext context,
            FlightDescriptor descriptor) {
        return new SchemaResult(textRowSchema());
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket, FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        String handle = ticket.getStatementHandle().toStringUtf8();
        String sql = preparedStatements.get(handle);
        if (sql == null) {
            listener.error(CallStatus.INVALID_ARGUMENT.withDescription("Unknown prepared statement handle")
                    .toRuntimeException());
            return;
        }
        // execute the SQL associated with the prepared handle and stream results
        try (Connection c = dbManager.getConnection();
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            Schema schema = textRowSchema();
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, getAllocator())) {
                VarCharVector vec = (VarCharVector) root.getVector("row");
                vec.setInitialCapacity(1024);
                root.allocateNew();
                OutboundStreamListener out = (OutboundStreamListener) listener;
                out.start(root, null, new IpcOption());

                int idx = 0;
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    int cols = rs.getMetaData().getColumnCount();
                    for (int i = 1; i <= cols; i++) {
                        if (i > 1)
                            sb.append(',');
                        String v = rs.getString(i);
                        sb.append(v == null ? "" : v.replaceAll("\\r|\\n", " "));
                    }
                    vec.setSafe(idx, new Text(sb.toString()));
                    idx++;
                    if (idx >= 1024) {
                        vec.setValueCount(idx);
                        root.setRowCount(idx);
                        out.putNext();
                        root.clear();
                        root.allocateNew();
                        vec = (VarCharVector) root.getVector("row");
                        idx = 0;
                    }
                }
                if (idx > 0) {
                    vec.setValueCount(idx);
                    root.setRowCount(idx);
                    out.putNext();
                }
                out.completed();
            }
        } catch (SQLException e) {
            if (listener instanceof OutboundStreamListener) {
                ((OutboundStreamListener) listener).error(e);
            }
        }
    }

    @Override
    public FlightInfo getFlightInfo(FlightProducer.CallContext context, FlightDescriptor descriptor) {
        String sql;
        if (descriptor.getCommand() != null && descriptor.getCommand().length > 0) {
            sql = new String(descriptor.getCommand());
        } else if (descriptor.getPath() != null && !descriptor.getPath().isEmpty()) {
            sql = String.join(" ", descriptor.getPath());
        } else {
            sql = "";
        }

        Schema schema = textRowSchema();
        List<FlightEndpoint> endpoints = new ArrayList<>();
        endpoints.add(endpointForSql(sql));
        return new FlightInfo(schema, descriptor, endpoints, /* bytes */ -1, /* records */ -1);
    }

    // Prepared statement lifecycle
    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request,
            FlightProducer.CallContext context, FlightProducer.StreamListener<Result> listener) {
        String sql = request.getQuery();
        String handle = UUID.randomUUID().toString();
        preparedStatements.put(handle, sql);
        FlightSql.ActionCreatePreparedStatementResult res = FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                .setPreparedStatementHandle(com.google.protobuf.ByteString.copyFromUtf8(handle))
                .build();
        listener.onNext(new Result(res.toByteArray()));
        listener.onCompleted();
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request,
            FlightProducer.CallContext context, FlightProducer.StreamListener<Result> listener) {
        String handle = request.getPreparedStatementHandle().toStringUtf8();
        preparedStatements.remove(handle);
        // return empty result
        listener.onNext(new Result(new byte[0]));
        listener.onCompleted();
    }

    @Override
    public void getStream(FlightProducer.CallContext context, Ticket ticket,
            FlightProducer.ServerStreamListener listener) {
        String sql = new String(Objects.requireNonNull(ticket.getBytes()));
        try (Connection c = dbManager.getConnection();
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            Schema schema = textRowSchema();
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, getAllocator())) {
                VarCharVector vec = (VarCharVector) root.getVector("row");
                vec.setInitialCapacity(1024);
                root.allocateNew();

                OutboundStreamListener out = (OutboundStreamListener) listener;
                out.start(root, null, new IpcOption());

                int idx = 0;
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    int cols = rs.getMetaData().getColumnCount();
                    for (int i = 1; i <= cols; i++) {
                        if (i > 1)
                            sb.append(',');
                        String v = rs.getString(i);
                        sb.append(v == null ? "" : v.replaceAll("\\r|\\n", " "));
                    }
                    vec.setSafe(idx, new Text(sb.toString()));
                    idx++;
                    // flush batches of 1024
                    if (idx >= 1024) {
                        vec.setValueCount(idx);
                        root.setRowCount(idx);
                        out.putNext();
                        root.clear();
                        root.allocateNew();
                        vec = (VarCharVector) root.getVector("row");
                        idx = 0;
                    }
                }
                if (idx > 0) {
                    vec.setValueCount(idx);
                    root.setRowCount(idx);
                    out.putNext();
                }
                out.completed();
            }
        } catch (SQLException e) {
            if (listener instanceof OutboundStreamListener) {
                ((OutboundStreamListener) listener).error(e);
            }
        }
    }

    @Override
    public Runnable acceptPut(FlightProducer.CallContext context, org.apache.arrow.flight.FlightStream flightStream,
            FlightProducer.StreamListener<org.apache.arrow.flight.PutResult> ackStream) {
        throw new UnsupportedOperationException("acceptPut not implemented");
    }

    @Override
    public void doAction(FlightProducer.CallContext context, Action action,
            FlightProducer.StreamListener<Result> listener) {
        String type = action.getType();
        if ("execute".equals(type)) {
            String sql = new String(action.getBody());
            try (Connection c = dbManager.getConnection(); Statement st = c.createStatement()) {
                int updated = st.executeUpdate(sql);
                listener.onNext(new Result(("affected=" + updated).getBytes()));
                listener.onCompleted();
            } catch (SQLException ex) {
                listener.onError(ex);
            }
        } else {
            listener.onError(new UnsupportedOperationException("Action not supported: " + type));
        }
    }

    @Override
    public void listActions(FlightProducer.CallContext context, FlightProducer.StreamListener<ActionType> listener) {
        listener.onNext(new ActionType("execute", "Execute SQL (returns affected rows)"));
        listener.onCompleted();
    }

    @Override
    public void listFlights(FlightProducer.CallContext context, Criteria criteria,
            FlightProducer.StreamListener<FlightInfo> listener) {
        // not implemented: dynamic listing
        listener.onCompleted();
    }

    @Override
    public SchemaResult getSchema(FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return new SchemaResult(textRowSchema());
    }

    // ----- Minimal metadata endpoints -----
    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request, FlightProducer.CallContext context,
            FlightDescriptor descriptor) {
        List<FlightEndpoint> eps = Collections.singletonList(endpointForSql(""));
        return new FlightInfo(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA, descriptor,
                eps, -1, -1);
    }

    @Override
    public void getStreamCatalogs(FlightProducer.CallContext context, FlightProducer.ServerStreamListener listener) {
        // no catalogs; return empty stream
        listener.completed();
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, FlightProducer.CallContext context,
            FlightDescriptor descriptor) {
        List<FlightEndpoint> eps = Collections.singletonList(endpointForSql(""));
        return new FlightInfo(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA, descriptor, eps,
                -1, -1);
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        listener.completed();
    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request, FlightProducer.CallContext context,
            FlightDescriptor descriptor) {
        List<FlightEndpoint> eps = Collections.singletonList(endpointForSql(""));
        if (request.getIncludeSchema()) {
            return new FlightInfo(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLES_SCHEMA, descriptor,
                    eps, -1, -1);
        }
        return new FlightInfo(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA,
                descriptor, eps, -1, -1);
    }

    @Override
    public void getStreamTables(FlightSql.CommandGetTables command, FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        listener.completed();
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request, FlightProducer.CallContext context,
            FlightDescriptor descriptor) {
        List<FlightEndpoint> eps = Collections.singletonList(endpointForSql(""));
        return new FlightInfo(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA, descriptor,
                eps, -1, -1);
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        listener.completed();
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        List<FlightEndpoint> eps = Collections.singletonList(endpointForSql(""));
        return new FlightInfo(org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_TYPE_INFO_SCHEMA, descriptor,
                eps, -1, -1);
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        listener.completed();
    }

    @Override
    public void close() {
        try {
            dbManager.shutdown();
        } catch (Exception e) {
            logger.warn("Error shutting down db manager", e);
        }
        if (allocator != null) {
            allocator.close();
        }
    }
}
