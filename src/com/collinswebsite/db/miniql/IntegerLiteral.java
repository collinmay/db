package com.collinswebsite.db.miniql;

import com.collinswebsite.db.Row;
import com.collinswebsite.db.types.DataType;
import com.collinswebsite.db.types.IntegerDataType;

public class IntegerLiteral implements Expression {
    private final long value;

    public IntegerLiteral(long value) {
        this.value = value;
    }

    @Override
    public DataType getType() {
        return IntegerDataType.DEFAULT;
    }

    @Override
    public Object evaluate(Row r) {
        return value;
    }

    @Override
    public long evaluateAsInteger(Row r) {
        return value;
    }
}
