package com.collinswebsite.db.miniql;

import com.collinswebsite.db.Column;
import com.collinswebsite.db.Table;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.List;

public class ColumnListVisitor {
    private final Table table;

    public ColumnListVisitor(Table t) {
        this.table = t;
    }

    public List<Column> visit(List<Column> list, MiniQLParser.ColumnListContext ctx) {
        Column c = table.getColumn(ctx.columnName().getText());
        if(c == null) {
            throw new ParseCancellationException("no such column in table '" + table.getName() + "': " + ctx.columnName().getText());
        }
        list.add(c);

        if(ctx.columnList() != null) {
            visit(list, ctx.columnList());
        }

        return list;
    }
}
