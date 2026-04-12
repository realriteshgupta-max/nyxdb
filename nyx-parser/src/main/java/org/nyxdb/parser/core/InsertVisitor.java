package org.nyxdb.parser.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.nyxdb.parser.core.models.InsertInfo;

public class InsertVisitor extends NyxDbBaseVisitor<Void> {

    private final List<InsertInfo> inserts = new ArrayList<>();

    public List<InsertInfo> getInserts() {
        return Collections.unmodifiableList(inserts);
    }

    @Override
    public Void visitInsertStmt(NyxDbParser.InsertStmtContext ctx) {
        String fullName = null;
        if (ctx.qualifiedName() != null) {
            java.util.List<NyxDbParser.IdentifierContext> ids = ctx.qualifiedName().identifier();
            if (ids != null && !ids.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < ids.size(); i++) {
                    if (i > 0)
                        sb.append('.');
                    sb.append(ids.get(i).getText());
                }
                fullName = sb.toString();
            }
        }
        InsertInfo info = new InsertInfo(fullName);
        if (ctx.columnList() != null) {
            for (NyxDbParser.IdentifierContext id : ctx.columnList().identifier()) {
                info.addColumn(id.getText());
            }
        }
        java.util.List<NyxDbParser.ValueListContext> vlists = ctx.valueList();
        if (vlists != null) {
            for (NyxDbParser.ValueListContext vl : vlists) {
                java.util.List<String> row = new ArrayList<>();
                for (NyxDbParser.LiteralContext lit : vl.literal()) {
                    row.add(lit.getText());
                }
                info.addValueRow(row);
            }
        }
        inserts.add(info);
        return super.visitInsertStmt(ctx);
    }
}
