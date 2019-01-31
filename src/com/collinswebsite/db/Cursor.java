package com.collinswebsite.db;

import java.util.concurrent.CompletionStage;

public interface Cursor {
    boolean isAtEnd();
    boolean isNextReady();
    Row getNext() throws DeserializationException;
    Table getTable();
    CompletionStage<Void> await();
}
