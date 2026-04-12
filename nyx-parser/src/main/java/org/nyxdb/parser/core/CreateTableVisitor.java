package org.nyxdb.parser.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import org.nyxdb.parser.core.models.ColumnInfo;
import org.nyxdb.parser.core.models.ForeignKeyInfo;
import org.nyxdb.parser.core.models.TableInfo;

/**
 * A small parse-tree visitor that collects CREATE TABLE information.
 */
public class CreateTableVisitor extends NyxDbBaseVisitor<Void> {
    private final List<TableInfo> tables = new ArrayList<>();
    private final Deque<TableInfo> stack = new ArrayDeque<>();

    public List<TableInfo> getTables() {
        return Collections.unmodifiableList(tables);
    }

    @Override
    public Void visitCreateTableStmt(NyxDbParser.CreateTableStmtContext ctx) {
        handleCreateTable(ctx);
        // visit children to pick up table-level constraints
        stack.push(tables.get(tables.size() - 1));
        try {
            return super.visitCreateTableStmt(ctx);
        } finally {
            stack.pop();
        }
    }

    @Override
    public Void visitTableConstraint(NyxDbParser.TableConstraintContext ctx) {
        TableInfo current = stack.peek();
        if (current == null)
            return super.visitTableConstraint(ctx);
        if (ctx.PRIMARY() != null) {
            NyxDbParser.ColumnListContext cl = ctx.columnList(0);
            if (cl != null) {
                for (NyxDbParser.IdentifierContext id : cl.identifier()) {
                    current.addPrimaryKeyColumn(id.getText());
                }
            }
            return super.visitTableConstraint(ctx);
        }
        if (ctx.FOREIGN() != null) {
            NyxDbParser.ColumnListContext local = ctx.columnList(0);
            NyxDbParser.QualifiedNameContext qn = ctx.qualifiedName();
            NyxDbParser.ColumnListContext refs = null;
            java.util.List<NyxDbParser.ColumnListContext> cls = ctx.columnList();
            if (cls != null && cls.size() > 1)
                refs = cls.get(1);
            java.util.List<String> localCols = new ArrayList<>();
            java.util.List<String> refCols = new ArrayList<>();
            if (local != null) {
                for (NyxDbParser.IdentifierContext id : local.identifier())
                    localCols.add(id.getText());
            }
            if (refs != null) {
                for (NyxDbParser.IdentifierContext id : refs.identifier())
                    refCols.add(id.getText());
            }
            String refTable = null;
            if (qn != null) {
                java.util.List<NyxDbParser.IdentifierContext> ids = qn.identifier();
                if (ids != null && !ids.isEmpty()) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < ids.size(); i++) {
                        if (i > 0)
                            sb.append('.');
                        sb.append(ids.get(i).getText());
                    }
                    refTable = sb.toString();
                }
            }
            current.addForeignKey(new ForeignKeyInfo(localCols, refTable, refCols));
        }
        return super.visitTableConstraint(ctx);
    }

    private void handleCreateTable(NyxDbParser.CreateTableStmtContext ctx) {
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

        TableInfo table = new TableInfo(schema, tableName);
        for (NyxDbParser.ColumnDefContext cd : ctx.columnDef()) {
            String colName = cd.identifier() != null ? cd.identifier().getText() : null;
            String colType = cd.dataType() != null ? cd.dataType().getText() : null;
            table.addColumn(new ColumnInfo(colName, colType));
            java.util.List<NyxDbParser.ColumnConstraintContext> ccs = cd.columnConstraint();
            if (ccs != null) {
                for (NyxDbParser.ColumnConstraintContext cc : ccs) {
                    if (cc.PRIMARY() != null) {
                        table.addPrimaryKeyColumn(colName);
                        break;
                    }
                }
            }
        }
        tables.add(table);
    }
}
