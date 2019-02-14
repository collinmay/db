package com.collinswebsite.db.miniql;

import com.collinswebsite.db.Column;
import com.collinswebsite.db.Table;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.ArrayList;
import java.util.List;

public class ColumnListVisitor extends MiniQLBaseVisitor<List<Column>> {
    private final Table table;
    private List<Column> columns = new ArrayList<>();

    public ColumnListVisitor(Table t) {
        this.table = t;
    }

    @Override
    public List<Column> visitColumnName(MiniQLParser.ColumnNameContext ctx) {
        Column c = table.getColumn(ctx.IDENTIFIER().toString());
        if(c == null) {
            throw new ParseCancellationException("no such column in table '" + table.getName() + "': " + ctx.IDENTIFIER().toString());
        }
        columns.add(c);

        return columns;
    }
}
