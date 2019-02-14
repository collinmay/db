package com.collinswebsite.db;

import com.collinswebsite.db.miniql.Expression;
import com.collinswebsite.db.miniql.InvalidExpressionException;

import java.util.concurrent.CompletionStage;

public interface Cursor {
    boolean isAtEnd() throws DeserializationException;
    Row getNext() throws DeserializationException;
    Table getTable();
    CompletionStage<Void> await();
    void setFilter(Expression filter) throws InvalidExpressionException;
}
