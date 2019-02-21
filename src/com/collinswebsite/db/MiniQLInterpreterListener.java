package com.collinswebsite.db;

import com.collinswebsite.db.miniql.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.io.IOException;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

public class MiniQLInterpreterListener implements MiniQLListener {
    private final SocketConnectionState socketConnectionState;
    private final DatabaseServer db;

    public MiniQLInterpreterListener(SocketConnectionState socketConnectionState, DatabaseServer db) {
        this.socketConnectionState = socketConnectionState;
        this.db = db;
    }

    @Override
    public void enterSelectStatement(MiniQLParser.SelectStatementContext ctx) {
        Table table = db.getTable(ctx.tableName().getText());
        if(table == null) {
            throw new ParseCancellationException("no such table: " + ctx.tableName().getText());
        }

        Cursor cursor = table.createFullTableScanCursor();

        List<Column> columns;
        if(ctx.columnList() == null) {
            columns = table.getColumns();
        } else {
            columns = new ColumnListVisitor(table).visitColumnList(ctx.columnList());
        }

        if(ctx.whereFilter != null) {
            try {
                cursor.setFilter(new ExpressionVisitor(table).visit(ctx.whereFilter));
            } catch(InvalidExpressionException e) {
                throw new ParseCancellationException(e);
            }
        }

        if(ctx.orderList != null) {
            cursor = new SortingCursor(cursor, new ExpressionListVisitor(table).visit(ctx.orderList));
        }

        socketConnectionState.key.attach((BooleanSupplier) new SocketConnectionResponseWriter(
                socketConnectionState,
                cursor,
                columns)::process);
    }

    @Override
    public void enterInsertStatement(MiniQLParser.InsertStatementContext ctx) {
        Table table = db.getTable(ctx.tableName().getText());
        if(table == null) {
            throw new ParseCancellationException("no such table: " + ctx.tableName().getText());
        }

        ExpressionVisitor v = new ExpressionVisitor(table);
        List<Object> objects = ctx.literal().stream().map((l) -> v.visitLiteral(l).evaluate(null)).collect(Collectors.toList());
        Row r = null;
        try {
            r = table.insertRow(objects);
        } catch(IOException | SerializationException e) {
            throw new ParseCancellationException(e);
        }
        socketConnectionState.key.attach((BooleanSupplier) new SocketConnectionErrorWriter(
                socketConnectionState,
                "inserted at id " + r.getId())::process);
    }

    @Override
    public void enterDeleteStatement(MiniQLParser.DeleteStatementContext ctx) {
        Table table = db.getTable(ctx.tableName().getText());
        if(table == null) {
            throw new ParseCancellationException("no such table: " + ctx.tableName().getText());
        }

        Cursor cursor = table.createFullTableScanCursor();

        if(ctx.whereFilter != null) {
            try {
                cursor.setFilter(new ExpressionVisitor(table).visit(ctx.whereFilter));
            } catch(InvalidExpressionException e) {
                throw new ParseCancellationException(e);
            }
        }

        int count = 0;
        try {
            Row r;
            while((r = cursor.getNext()) != null) {
                r.delete();
                count++;
            }
        } catch(DeserializationException | IOException e) {
            throw new ParseCancellationException(e);
        }

        socketConnectionState.key.attach((BooleanSupplier) new SocketConnectionErrorWriter(
                socketConnectionState,
                "deleted " + count + " rows")::process);
    }

    // stubs...

    @Override
    public void exitDeleteStatement(MiniQLParser.DeleteStatementContext ctx) {

    }

    @Override
    public void exitInsertStatement(MiniQLParser.InsertStatementContext ctx) {

    }

    @Override
    public void exitSelectStatement(MiniQLParser.SelectStatementContext ctx) {

    }

    @Override
    public void enterStatement(MiniQLParser.StatementContext ctx) {

    }

    @Override
    public void exitStatement(MiniQLParser.StatementContext ctx) {

    }

    @Override
    public void enterColumnList(MiniQLParser.ColumnListContext ctx) {

    }

    @Override
    public void exitColumnList(MiniQLParser.ColumnListContext ctx) {

    }

    @Override
    public void enterColumnName(MiniQLParser.ColumnNameContext ctx) {

    }

    @Override
    public void exitColumnName(MiniQLParser.ColumnNameContext ctx) {

    }

    @Override
    public void enterTableName(MiniQLParser.TableNameContext ctx) {

    }

    @Override
    public void exitTableName(MiniQLParser.TableNameContext ctx) {

    }

    @Override
    public void enterExpressionList(MiniQLParser.ExpressionListContext ctx) {

    }

    @Override
    public void exitExpressionList(MiniQLParser.ExpressionListContext ctx) {

    }

    @Override
    public void enterColumnExpression(MiniQLParser.ColumnExpressionContext ctx) {

    }

    @Override
    public void exitColumnExpression(MiniQLParser.ColumnExpressionContext ctx) {

    }

    @Override
    public void enterComparisonExpression(MiniQLParser.ComparisonExpressionContext ctx) {

    }

    @Override
    public void exitComparisonExpression(MiniQLParser.ComparisonExpressionContext ctx) {

    }

    @Override
    public void enterParenExpression(MiniQLParser.ParenExpressionContext ctx) {

    }

    @Override
    public void exitParenExpression(MiniQLParser.ParenExpressionContext ctx) {

    }

    @Override
    public void enterLiteralExpression(MiniQLParser.LiteralExpressionContext ctx) {

    }

    @Override
    public void exitLiteralExpression(MiniQLParser.LiteralExpressionContext ctx) {

    }

    @Override
    public void enterComparator(MiniQLParser.ComparatorContext ctx) {

    }

    @Override
    public void exitComparator(MiniQLParser.ComparatorContext ctx) {

    }

    @Override
    public void enterLiteral(MiniQLParser.LiteralContext ctx) {

    }

    @Override
    public void exitLiteral(MiniQLParser.LiteralContext ctx) {

    }

    @Override
    public void visitTerminal(TerminalNode node) {

    }

    @Override
    public void visitErrorNode(ErrorNode node) {

    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {

    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {

    }
}
