package com.collinswebsite.db.miniql;

import com.collinswebsite.db.Row;
import com.collinswebsite.db.types.DataType;

public interface Expression {
    DataType getType();

    Object evaluate(Row r);

    default long evaluateAsInteger(Row r) {
        throw new IllegalStateException("can't evaluate as integer");
    }
    default boolean evaluateAsBoolean(Row r) {
        throw new IllegalStateException("can't evaluate as boolean");
    }

    default void validate() throws InvalidExpressionException {

    }
}
