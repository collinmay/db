package com.collinswebsite.db.miniql;

import com.collinswebsite.db.Row;
import com.collinswebsite.db.types.DataType;
import com.collinswebsite.db.types.StringDataType;

public class StringLiteral implements Expression {
    private final String value;

    public StringLiteral(String value) {
        this.value = value;
    }

    @Override
    public DataType getType() {
        return StringDataType.DEFAULT;
    }

    @Override
    public Object evaluate(Row r) {
        return value;
    }
}
