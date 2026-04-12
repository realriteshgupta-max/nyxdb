package org.nyxdb.parser.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import org.nyxdb.parser.core.models.TableInfo;
import org.nyxdb.parser.core.models.ColumnInfo;
import org.nyxdb.parser.core.models.DatabaseInfo;
import org.nyxdb.parser.core.models.ForeignKeyInfo;
import org.nyxdb.parser.core.models.InsertInfo;

// ANTLR-generated classes
import org.antlr.v4.runtime.tree.ParseTree;

public class NyxParser {

    private final List<TableInfo> tables = new ArrayList<>();
    private final List<DatabaseInfo> databases = new ArrayList<>();
    private final List<String> droppedTables = new ArrayList<>();
    private final List<String> truncatedTables = new ArrayList<>();
    private final List<String> droppedDatabases = new ArrayList<>();
    private final List<InsertInfo> inserts = new ArrayList<>();

    // NyxTreeListener removed; NyxParser now uses composable visitors via
    // extract().

    public List<TableInfo> getTables() {
        return Collections.unmodifiableList(tables);
    }

    protected List<TableInfo> getTablesForModification() {
        return tables;
    }

    protected List<DatabaseInfo> getDatabasesForModification() {
        return databases;
    }

    public List<InsertInfo> getInserts() {
        return Collections.unmodifiableList(inserts);
    }

    public List<String> getDroppedTables() {
        return Collections.unmodifiableList(droppedTables);
    }

    public List<String> getTruncatedTables() {
        return Collections.unmodifiableList(truncatedTables);
    }

    public List<DatabaseInfo> getDatabases() {
        return Collections.unmodifiableList(databases);
    }

    public List<String> getDroppedDatabases() {
        return Collections.unmodifiableList(droppedDatabases);
    }

    public static NyxParser extract(String sql) {
        NyxDbLexer lexer = new NyxDbLexer(
                CharStreams.fromString(sql));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        NyxDbParser parser = new NyxDbParser(
                tokens);
        NyxDbParser.DdlFileContext tree = parser.ddlFile();

        // Create extractor first
        NyxParser extractor = new NyxParser();

        // Run visitors that collect their own data
        CreateTableVisitor ctv = new CreateTableVisitor();
        CreateDatabaseVisitor cdv = new CreateDatabaseVisitor();
        DropTableVisitor dtv = new DropTableVisitor();
        InsertVisitor iv = new InsertVisitor();
        TruncateVisitor tv = new TruncateVisitor();

        // First pass: collect base schema information
        ctv.visit(tree);
        cdv.visit(tree);
        dtv.visit(tree);
        iv.visit(tree);
        tv.visit(tree);

        // Populate extractor with base results before running in-place visitors
        extractor.tables.addAll(ctv.getTables());
        extractor.databases.addAll(cdv.getDatabases());
        extractor.droppedTables.addAll(dtv.getDroppedTables());
        extractor.inserts.addAll(iv.getInserts());
        extractor.truncatedTables.addAll(tv.getTruncatedTables());

        // Second pass: run visitors that modify the parser in-place
        AlterVisitor av = new AlterVisitor(extractor);
        DropDatabaseVisitor ddv = new DropDatabaseVisitor(extractor);

        av.visit(tree); // Alters tables in extractor
        ddv.visit(tree); // Adds databases to extractor

        // Add dropped databases from ddv
        extractor.droppedDatabases.addAll(ddv.getDroppedDatabases());

        return extractor;
    }
}
