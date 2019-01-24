package com.collinswebsite.db;

import java.util.concurrent.CompletionStage;

public interface Cursor {
    boolean isAtEnd();
    boolean isNextReady();
    Row getNext();
    Table getTable();
    CompletionStage<Void> await();
}
