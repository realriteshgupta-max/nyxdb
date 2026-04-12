package org.nyxdb.parser.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TruncateVisitor extends NyxDbBaseVisitor<Void> {

    private final List<String> truncatedTables = new ArrayList<>();

    public List<String> getTruncatedTables() {
        return Collections.unmodifiableList(truncatedTables);
    }

    @Override
    public Void visitTruncateTableStmt(NyxDbParser.TruncateTableStmtContext ctx) {
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
        truncatedTables.add(fullName);
        return super.visitTruncateTableStmt(ctx);
    }
}
