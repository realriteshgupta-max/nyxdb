package org.nyxdb.parser.core;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.nyxdb.parser.core.models.ColumnInfo;
import org.nyxdb.parser.core.models.TableInfo;
import org.nyxdb.parser.core.NyxDbBaseVisitor;

/**
 * Visitor that applies ALTER TABLE actions to tables discovered earlier.
 */
public class AlterVisitor extends NyxDbBaseVisitor<Void> {

    private final NyxParser parser;
    private final Deque<TableInfo> stack = new ArrayDeque<>();

    public AlterVisitor(NyxParser parser) {
        this.parser = parser;
    }

    public List<TableInfo> getTables() {
        return parser.getTables();
    }

    @Override
    public Void visitAlterTableStmt(NyxDbParser.AlterTableStmtContext ctx) {
        String schema = null;
        String tableName = null;
        if (ctx.qualifiedName() != null) {
            java.util.List<NyxDbParser.IdentifierContext> ids = ctx.qualifiedName().identifier();
            if (ids != null && !ids.isEmpty()) {
                tableName = ids.get(ids.size() - 1).getText();
                if (ids.size() > 1) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < ids.size() - 1; i++) {
                        if (i > 0)
                            sb.append('.');
                        sb.append(ids.get(i).getText());
                    }
                    schema = sb.toString();
                }
            }
        }
        TableInfo found = null;
        for (TableInfo t : parser.getTablesForModification()) {
            if ((schema == null || schema.equals(t.getSchema())) && t.getName() != null
                    && t.getName().equals(tableName)) {
                found = t;
                break;
            }
        }
        if (found == null) {
            found = new TableInfo(schema, tableName);
            parser.getTablesForModification().add(found);
        }
        stack.push(found);
        try {
            return super.visitAlterTableStmt(ctx);
        } finally {
            stack.pop();
        }
    }

    @Override
    public Void visitAlterAction(NyxDbParser.AlterActionContext ctx) {
        TableInfo current = stack.peek();
        if (current == null)
            return super.visitAlterAction(ctx);
        if (ctx.ADD() != null && ctx.columnDef() != null) {
            NyxDbParser.ColumnDefContext cd = ctx.columnDef();
            String colName = cd.identifier() != null ? cd.identifier().getText() : null;
            String colType = cd.dataType() != null ? cd.dataType().getText() : null;
            current.addColumn(new ColumnInfo(colName, colType));
            return super.visitAlterAction(ctx);
        }
        if (ctx.DROP() != null && ctx.COLUMN() != null && ctx.identifier() != null) {
            String colName = ctx.identifier().getText();
            current.removeColumnByName(colName);
            return super.visitAlterAction(ctx);
        }
        return super.visitAlterAction(ctx);
    }
}
