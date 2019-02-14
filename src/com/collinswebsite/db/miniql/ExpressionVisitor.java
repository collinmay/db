package com.collinswebsite.db.miniql;

import com.collinswebsite.db.Table;
import org.antlr.v4.runtime.misc.ParseCancellationException;

public class ExpressionVisitor extends MiniQLBaseVisitor<Expression> {
    private final Table table;

    public ExpressionVisitor(Table table) {
        this.table = table;
    }

    @Override
    public Expression visitColumnExpression(MiniQLParser.ColumnExpressionContext ctx) {
        return visitColumnName(ctx.columnName());
    }

    @Override
    public Expression visitColumnName(MiniQLParser.ColumnNameContext ctx) {
        return new ColumnExpression(table.getColumn(ctx.IDENTIFIER().toString()));
    }

    @Override
    public Expression visitComparisonExpression(MiniQLParser.ComparisonExpressionContext ctx) {
        return new ComparisonExpression(
                visitChildren(ctx.left),
                ComparisonExpression.getOperator(ctx.op),
                visitChildren(ctx.right)
                );
    }

    @Override
    public Expression visitParenExpression(MiniQLParser.ParenExpressionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Expression visitLiteral(MiniQLParser.LiteralContext ctx) {
        if(ctx.INTEGER_LITERAL() != null) {
            return new IntegerLiteral(Long.parseLong(ctx.INTEGER_LITERAL().toString()));
        }
        if(ctx.STRING_LITERAL() != null) {
            // manually strip quotes
            String st = ctx.STRING_LITERAL().toString();
            return new StringLiteral(st.substring(1, st.length() - 1));
        }
        throw new ParseCancellationException("invalid literal");
    }
}
