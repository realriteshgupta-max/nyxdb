package org.nyxdb.flight;

import org.nyxdb.duckdb.DuckDBManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.flight.OutboundStreamListener;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.CallStatus;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.flight.sql.impl.FlightSql;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * Minimal service that would back a Flight SQL server.
 *
 * It executes SQL against a provided `DuckDBManager`.
 */
public class NyxFlightService extends NoOpFlightSqlProducer {
    private static final Logger logger = LogManager.getLogger(NyxFlightService.class);

    private final DuckDBManager dbManager;
    // Lazily initialize the allocator to avoid triggering Arrow/Netty static
    // initialization during tests that don't need Flight streaming.
    private volatile BufferAllocator allocator;
    // prepared statement handle -> SQL
    private final Map<String, String> preparedStatements = new ConcurrentHashMap<>();

    public NyxFlightService(DuckDBManager dbManager) {
        this.dbManager = dbManager;
    }

    /**
     * Convenience constructor that creates an in-memory DuckDB manager.
     * This avoids forcing callers (tests) to reference `DuckDBManager`.
     */
    public NyxFlightService() {
        this(DuckDBManager.createInMemoryManager());
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
        try (Statement st = c.createStatement()) {
            int statementIndex = 0;
            for (String sql : splitStatements(sqlBatch)) {
                logger.info("Executing statement {}", statementIndex++);
                if (isQuerySql(sql)) {
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

    private List<String> splitStatements(String sqlBatch) {
        List<String> statements = new ArrayList<>();
        if (sqlBatch == null || sqlBatch.isBlank()) {
            return statements;
        }

        for (String sql : sqlBatch.split(";")) {
            String trimmedSql = sql.trim();
            if (!trimmedSql.isEmpty()) {
                statements.add(trimmedSql);
            }
        }
        return statements;
    }

    // FlightProducer implementations (minimal, rows returned as a single-string
    // column named "row")

    private Schema textRowSchema() {
        Field f = new Field("row", FieldType.nullable(new ArrowType.Utf8()), /* children */ null);
        return new Schema(Collections.singletonList(f));
    }

    private FlightEndpoint endpointForMessage(Message message) {
        Ticket t = new Ticket(Any.pack(message).toByteArray());
        Location loc = Location.forGrpcInsecure("127.0.0.1", 8815);
        return new FlightEndpoint(t, loc);
    }

    private FlightEndpoint endpointForStatementHandle(String handle) {
        return endpointForMessage(FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(handle))
                .build());
    }

    private FlightEndpoint endpointForPreparedStatementHandle(String handle) {
        return endpointForMessage(FlightSql.CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(ByteString.copyFromUtf8(handle))
                .build());
    }

    private boolean isQuerySql(String sql) {
        String trimmedSql = sql == null ? "" : sql.trim().toUpperCase();
        return trimmedSql.startsWith("SELECT") || trimmedSql.startsWith("WITH") || trimmedSql.startsWith("SHOW")
                || trimmedSql.startsWith("DESCRIBE") || trimmedSql.startsWith("EXPLAIN");
    }

    private ByteString serializeSchema(Schema schema) {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(output)), schema);
            return ByteString.copyFrom(output.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Arrow schema", e);
        }
    }

    private void sendUpdateResult(FlightProducer.StreamListener<PutResult> ackStream, long recordCount) {
        byte[] metadata = FlightSql.DoPutUpdateResult.newBuilder()
                .setRecordCount(recordCount)
                .build()
                .toByteArray();
        try (ArrowBuf metadataBuffer = getAllocator().buffer(metadata.length)) {
            metadataBuffer.writeBytes(metadata);
            PutResult result = PutResult.metadata(metadataBuffer);
            ackStream.onNext(result);
            ackStream.onCompleted();
        }
    }

    private void consumePutStream(FlightStream flightStream) {
        while (flightStream.next()) {
            VectorSchemaRoot root = flightStream.getRoot();
            if (root != null) {
                logger.debug("DoPut: Received parameter batch with {} rows", root.getRowCount());
            }
        }
    }

    public BufferAllocator getAllocator() {
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

        if (!isQuerySql(sql)) {
            try {
                long rowsAffected = dbManager.executeSql(sql);
                logger.info("✓ Executed statement via GetFlightInfo, {} rows affected: {}", rowsAffected, sql);
                return new FlightInfo(schema, descriptor, endpoints, -1, rowsAffected);
            } catch (SQLException e) {
                throw CallStatus.INTERNAL.withDescription("SQL Error: " + e.getMessage()).withCause(e)
                        .toRuntimeException();
            }
        }

        String handle = UUID.randomUUID().toString();
        preparedStatements.put(handle, sql);
        endpoints.add(endpointForStatementHandle(handle));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command, FlightProducer.CallContext context,
            FlightDescriptor descriptor) {
        return new SchemaResult(textRowSchema());
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        String handle = command.getPreparedStatementHandle().toStringUtf8();
        logger.debug("getFlightInfoPreparedStatement: handle={}", handle);
        String sql = preparedStatements.get(handle);
        if (sql == null) {
            // Prepared statement not found - this shouldn't happen
            logger.warn("Prepared statement handle not found: {}", handle);
        }
        Schema schema = textRowSchema();
        List<FlightEndpoint> endpoints = new ArrayList<>();

        if (sql != null && !isQuerySql(sql)) {
            try {
                long rowsAffected = dbManager.executeSql(sql);
                logger.info("✓ Executed prepared statement via GetFlightInfo, {} rows affected: {}",
                        rowsAffected, sql);
                return new FlightInfo(schema, descriptor, endpoints, -1, rowsAffected);
            } catch (SQLException e) {
                throw CallStatus.INTERNAL.withDescription("SQL Error: " + e.getMessage()).withCause(e)
                        .toRuntimeException();
            }
        }

        endpoints.add(endpointForPreparedStatementHandle(handle));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    @Override
    public SchemaResult getSchemaPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
            FlightProducer.CallContext context, FlightDescriptor descriptor) {
        return new SchemaResult(textRowSchema());
    }

    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
            FlightProducer.CallContext context, FlightProducer.ServerStreamListener listener) {
        String handle = command.getPreparedStatementHandle().toStringUtf8();
        String sql = preparedStatements.get(handle);
        logger.debug("getStreamPreparedStatement: handle={}, sql={}, sql_found={}", handle, sql, sql != null);
        if (sql == null) {
            listener.error(CallStatus.INVALID_ARGUMENT.withDescription("Unknown prepared statement handle: " + handle)
                    .toRuntimeException());
            return;
        }

        // Check if this is a SELECT query or a DDL/DML statement
        String trimmedSql = sql.trim().toUpperCase();
        if (!trimmedSql.startsWith("SELECT")) {
            // For non-SELECT statements (CREATE, INSERT, UPDATE, etc), execute and return
            // empty result
            Connection c = null;
            try {
                c = dbManager.getConnection();
                try (Statement st = c.createStatement()) {
                    logger.info("Executing {} statement via Flight (prepared): {}", trimmedSql.split("\\s+")[0], sql);
                    int rowsAffected = st.executeUpdate(sql);
                    logger.info("✓ Successfully executed prepared statement, {} rows affected", rowsAffected);

                    // Return properly formatted empty result set for DDL/DML
                    Schema schema = textRowSchema();
                    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, getAllocator())) {
                        VarCharVector vec = (VarCharVector) root.getVector("row");
                        vec.allocateNew(0);
                        root.setRowCount(0);

                        OutboundStreamListener out = (OutboundStreamListener) listener;
                        out.start(root, null, new IpcOption());
                        out.putNext();
                        out.completed();
                    }
                }
            } catch (Exception e) {
                logger.error("✗ Error executing prepared DDL/DML statement: {} - Error: {}", sql, e.getMessage(), e);
                listener.error(
                        CallStatus.INTERNAL
                                .withDescription("SQL Error: " + e.getClass().getSimpleName() + ": " + e.getMessage())
                                .toRuntimeException());
            } finally {
                dbManager.releaseConnection(c);
            }
            return;
        }

        // For SELECT queries, stream results
        Connection c = null;
        try {
            c = dbManager.getConnection();
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
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
            }
        } catch (Exception e) {
            logger.error("✗ Error streaming prepared SELECT statement: {}", sql, e);
            listener.error(
                    CallStatus.INTERNAL.withCause(e).withDescription("There was an error servicing your request.")
                            .toRuntimeException());
        } finally {
            dbManager.releaseConnection(c);
        }
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command,
            FlightProducer.CallContext context, FlightStream flightStream,
            FlightProducer.StreamListener<PutResult> ackStream) {
        logger.debug("acceptPutPreparedStatementQuery called");
        return acceptPutPreparedStatement(command.getPreparedStatementHandle(), context, flightStream, ackStream);
    }

    private Runnable acceptPutPreparedStatement(com.google.protobuf.ByteString handle,
            FlightProducer.CallContext context, FlightStream flightStream,
            FlightProducer.StreamListener<PutResult> ackStream) {
        return () -> {
            try {
                String handleStr = handle.toStringUtf8();
                logger.debug("acceptPutPreparedStatement: handle={}", handleStr);
                String sql = preparedStatements.get(handleStr);
                if (sql == null) {
                    logger.error("Prepared statement not found: {}", handleStr);
                    ackStream.onError(new Exception("Prepared statement not found: " + handleStr));
                    return;
                }

                logger.debug("DoPut: Consuming parameter bindings");
                consumePutStream(flightStream);
                logger.info("✓ DoPut: Successfully received parameter bindings for prepared statement {}", handleStr);

                if (!isQuerySql(sql)) {
                    long rowsAffected = dbManager.executeSql(sql);
                    logger.info("✓ Executed prepared statement via Flight SQL DoPut, {} rows affected",
                            rowsAffected);
                    sendUpdateResult(ackStream, rowsAffected);
                    return;
                }

                PutResult result = PutResult.empty();
                try {
                    ackStream.onNext(result);
                    ackStream.onCompleted();
                } finally {
                    result.close();
                }
            } catch (Exception e) {
                logger.error("✗ Error in acceptPutPreparedStatement: {}", e.getMessage(), e);
                try {
                    ackStream.onError(e);
                } catch (Exception ex) {
                    logger.error("Error sending error response: {}", ex.getMessage());
                }
            }
        };
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket, FlightProducer.CallContext context,
            FlightProducer.ServerStreamListener listener) {
        String handle = ticket.getStatementHandle().toStringUtf8();
        String sql = preparedStatements.get(handle);
        logger.debug("getStreamStatement: handle={}, sql={}, sql_found={}", handle, sql, sql != null);
        if (sql == null) {
            listener.error(CallStatus.INVALID_ARGUMENT.withDescription("Unknown prepared statement handle: " + handle)
                    .toRuntimeException());
            return;
        }

        // Check if this is a SELECT query or a DDL/DML statement
        String trimmedSql = sql.trim().toUpperCase();
        if (!trimmedSql.startsWith("SELECT")) {
            // For non-SELECT statements (CREATE, INSERT, UPDATE, etc), execute and return
            // empty result
            Connection c = null;
            try {
                c = dbManager.getConnection();
                try (Statement st = c.createStatement()) {
                    logger.info("Executing {} statement via Flight: {}", trimmedSql.split("\\s+")[0], sql);
                    int rowsAffected = st.executeUpdate(sql);
                    logger.info("✓ Successfully executed statement, {} rows affected", rowsAffected);

                    // Return properly formatted empty result set for DDL/DML
                    Schema schema = textRowSchema();
                    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, getAllocator())) {
                        VarCharVector vec = (VarCharVector) root.getVector("row");
                        vec.allocateNew(0);
                        root.setRowCount(0);

                        OutboundStreamListener out = (OutboundStreamListener) listener;
                        out.start(root, null, new IpcOption());
                        out.putNext();
                        out.completed();
                    }
                }
            } catch (Exception e) {
                logger.error("✗ Error executing non-SELECT statement: {} - Error: {}", sql, e.getMessage(), e);
                listener.error(
                        CallStatus.INTERNAL
                                .withDescription("SQL Error: " + e.getClass().getSimpleName() + ": " + e.getMessage())
                                .toRuntimeException());
            } finally {
                dbManager.releaseConnection(c);
            }
            return;
        }

        // execute the SQL associated with the prepared handle and stream results (for
        // SELECT only)
        Connection c = null;
        try {
            c = dbManager.getConnection();
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
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
            }
        } catch (Exception e) {
            logger.error("✗ Error streaming statement: {}", sql, e);
            listener.error(
                    CallStatus.INTERNAL.withCause(e).withDescription("There was an error servicing your request.")
                            .toRuntimeException());
        } finally {
            dbManager.releaseConnection(c);
        }
    }

    @Override
    public FlightInfo getFlightInfo(FlightProducer.CallContext context, FlightDescriptor descriptor) {
        Schema schema = textRowSchema();
        List<FlightEndpoint> endpoints = new ArrayList<>();
        if (descriptor.getCommand() != null && descriptor.getCommand().length > 0) {
            endpoints.add(
                    new FlightEndpoint(new Ticket(descriptor.getCommand()),
                            Location.forGrpcInsecure("127.0.0.1", 8815)));
        }
        return new FlightInfo(schema, descriptor, endpoints, /* bytes */ -1, /* records */ -1);
    }

    // Prepared statement lifecycle
    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request,
            FlightProducer.CallContext context, FlightProducer.StreamListener<Result> listener) {
        try {
            String sql = request.getQuery();
            String handle = UUID.randomUUID().toString();
            logger.info("Creating prepared statement: handle={}, SQL: {}", handle, sql);
            preparedStatements.put(handle, sql);
            FlightSql.ActionCreatePreparedStatementResult res = FlightSql.ActionCreatePreparedStatementResult
                    .newBuilder()
                    .setPreparedStatementHandle(com.google.protobuf.ByteString.copyFromUtf8(handle))
                    .setDatasetSchema(isQuerySql(sql) ? serializeSchema(textRowSchema()) : ByteString.EMPTY)
                    .setParameterSchema(ByteString.EMPTY)
                    .build();
            logger.info("✓ Prepared statement created successfully with handle: {}", handle);
            com.google.protobuf.Any wrapped = com.google.protobuf.Any.pack(res);
            listener.onNext(new Result(wrapped.toByteArray()));
            listener.onCompleted();
        } catch (Exception e) {
            logger.error("✗ Error creating prepared statement", e);
            listener.onError(e);
        }
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request,
            FlightProducer.CallContext context, FlightProducer.StreamListener<Result> listener) {
        try {
            String handle = request.getPreparedStatementHandle().toStringUtf8();
            preparedStatements.remove(handle);
            listener.onCompleted();
        } catch (Exception e) {
            logger.error("✗ Error closing prepared statement", e);
            listener.onError(e);
        }
    }

    @Override
    public void getStream(FlightProducer.CallContext context, Ticket ticket,
            FlightProducer.ServerStreamListener listener) {
        try {
            Any command = Any.parseFrom(Objects.requireNonNull(ticket.getBytes()));
            if (command.is(FlightSql.TicketStatementQuery.class)) {
                getStreamStatement(command.unpack(FlightSql.TicketStatementQuery.class), context, listener);
                return;
            }
            if (command.is(FlightSql.CommandPreparedStatementQuery.class)) {
                getStreamPreparedStatement(command.unpack(FlightSql.CommandPreparedStatementQuery.class), context,
                        listener);
                return;
            }
            listener.error(CallStatus.INVALID_ARGUMENT.withDescription("Unsupported ticket.").toRuntimeException());
        } catch (Exception e) {
            listener.error(
                    CallStatus.INTERNAL.withCause(e).withDescription("There was an error servicing your request.")
                            .toRuntimeException());
        }
    }

    @Override
    public Runnable acceptPut(FlightProducer.CallContext context, org.apache.arrow.flight.FlightStream flightStream,
            FlightProducer.StreamListener<org.apache.arrow.flight.PutResult> ackStream) {
        try {
            Any command = Any.parseFrom(flightStream.getDescriptor().getCommand());
            if (command.is(FlightSql.CommandStatementUpdate.class)) {
                return acceptPutStatement(command.unpack(FlightSql.CommandStatementUpdate.class), context,
                        flightStream, ackStream);
            }
            if (command.is(FlightSql.CommandPreparedStatementUpdate.class)) {
                return acceptPutPreparedStatementUpdate(command.unpack(FlightSql.CommandPreparedStatementUpdate.class),
                        context, flightStream, ackStream);
            }
            if (command.is(FlightSql.CommandPreparedStatementQuery.class)) {
                return acceptPutPreparedStatementQuery(command.unpack(FlightSql.CommandPreparedStatementQuery.class),
                        context, flightStream, ackStream);
            }
            throw CallStatus.INVALID_ARGUMENT.withDescription("The defined request is invalid.").toRuntimeException();
        } catch (Exception e) {
            return () -> ackStream.onError(e);
        }
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
            // Delegate to parent class to handle Flight SQL actions like
            // CreatePreparedStatement
            super.doAction(context, action, listener);
        }
    }

    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command,
            FlightProducer.CallContext context, FlightStream flightStream,
            FlightProducer.StreamListener<PutResult> ackStream) {
        return () -> {
            try {
                consumePutStream(flightStream);
                long rowsAffected = dbManager.executeSql(command.getQuery());
                logger.info("✓ Executed statement update via Flight SQL DoPut, {} rows affected", rowsAffected);
                sendUpdateResult(ackStream, rowsAffected);
            } catch (Exception e) {
                logger.error("✗ Error executing statement update via Flight SQL DoPut", e);
                ackStream.onError(e);
            }
        };
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command,
            FlightProducer.CallContext context, FlightStream flightStream,
            FlightProducer.StreamListener<PutResult> ackStream) {
        return () -> {
            String handle = command.getPreparedStatementHandle().toStringUtf8();
            String sql = preparedStatements.get(handle);
            if (sql == null) {
                ackStream.onError(CallStatus.INVALID_ARGUMENT
                        .withDescription("Unknown prepared statement handle: " + handle)
                        .toRuntimeException());
                return;
            }

            try {
                consumePutStream(flightStream);
                long rowsAffected = dbManager.executeSql(sql);
                logger.info("✓ Executed prepared statement update via Flight SQL DoPut, {} rows affected",
                        rowsAffected);
                sendUpdateResult(ackStream, rowsAffected);
            } catch (Exception e) {
                logger.error("✗ Error executing prepared statement update via Flight SQL DoPut", e);
                ackStream.onError(e);
            }
        };
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
        List<FlightEndpoint> eps = Collections.singletonList(endpointForMessage(request));
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
        List<FlightEndpoint> eps = Collections.singletonList(endpointForMessage(request));
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
        List<FlightEndpoint> eps = Collections.singletonList(endpointForMessage(request));
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
        List<FlightEndpoint> eps = Collections.singletonList(endpointForMessage(request));
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
        List<FlightEndpoint> eps = Collections.singletonList(endpointForMessage(request));
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
