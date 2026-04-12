package org.nyxdb.parser.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.nyxdb.parser.core.models.DatabaseInfo;

public class DropDatabaseVisitor extends NyxDbBaseVisitor<Void> {

    private final NyxParser parser;
    private final List<String> droppedDatabases = new ArrayList<>();

    public DropDatabaseVisitor(NyxParser parser) {
        this.parser = parser;
    }

    public List<String> getDroppedDatabases() {
        return Collections.unmodifiableList(droppedDatabases);
    }

    @Override
    public Void visitDropDatabaseStmt(NyxDbParser.DropDatabaseStmtContext ctx) {
        String name = ctx.identifier() != null ? ctx.identifier().getText() : null;
        droppedDatabases.add(name);
        // Also add to the databases list for metadata purposes
        parser.getDatabasesForModification().add(new DatabaseInfo(name));
        return super.visitDropDatabaseStmt(ctx);
    }
}
