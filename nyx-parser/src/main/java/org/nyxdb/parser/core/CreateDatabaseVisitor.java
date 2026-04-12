package org.nyxdb.parser.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.nyxdb.parser.core.models.DatabaseInfo;

public class CreateDatabaseVisitor extends NyxDbBaseVisitor<Void> {

    private final List<DatabaseInfo> databases = new ArrayList<>();

    public List<DatabaseInfo> getDatabases() {
        return Collections.unmodifiableList(databases);
    }

    @Override
    public Void visitCreateDatabaseStmt(NyxDbParser.CreateDatabaseStmtContext ctx) {
        String name = ctx.identifier() != null ? ctx.identifier().getText() : null;
        databases.add(new DatabaseInfo(name));
        return super.visitCreateDatabaseStmt(ctx);
    }
}
