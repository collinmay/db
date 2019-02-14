package com.collinswebsite.db.miniql;

import com.collinswebsite.db.Column;
import com.collinswebsite.db.Row;
import com.collinswebsite.db.types.BooleanDataType;
import com.collinswebsite.db.types.DataType;
import com.collinswebsite.db.types.IntegerDataType;

public class ColumnExpression implements Expression {
    private final Column column;

    public ColumnExpression(Column column) {
        this.column = column;
    }

    @Override
    public DataType getType() {
        return column.getType();
    }

    @Override
    public Object evaluate(Row r) {
        return r.getValueForColumn(column);
    }

    @Override
    public long evaluateAsInteger(Row r) {
        if(column.getType() instanceof IntegerDataType) {
            return (Long) evaluate(r);
        } else {
            return Expression.super.evaluateAsInteger(r);
        }
    }

    @Override
    public boolean evaluateAsBoolean(Row r) {
        if(column.getType() instanceof BooleanDataType) {
            return (Boolean) evaluate(r);
        } else {
            return Expression.super.evaluateAsBoolean(r);
        }
    }

    @Override
    public void validate() throws InvalidExpressionException {

    }
}
