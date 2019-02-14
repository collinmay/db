package com.collinswebsite.db.miniql;

import com.collinswebsite.db.Table;

import java.util.ArrayList;
import java.util.List;

public class ExpressionListVisitor extends MiniQLBaseVisitor<List<Expression>> {
    private final Table table;
    private List<Expression> list;

    public ExpressionListVisitor(Table table) {
        this.table = table;
        this.list = new ArrayList<>();
    }

    @Override
    public List<Expression> visitExpressionList(MiniQLParser.ExpressionListContext ctx) {
        list.add(new ExpressionVisitor(table).visitChildren(ctx.expression()));
        if(ctx.expressionList() != null) {
            visitExpressionList(ctx.expressionList());
        }
        return list;
    }
}
