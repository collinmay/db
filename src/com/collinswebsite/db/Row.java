package com.collinswebsite.db;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class Row {
    private final Table table;
    private final int id;
    private CompletionStage<Void> cacheCompletion;
    private List<Object> cache; // Cache may be null if values have not yet been fetched from the table.
    private Throwable cacheError;

    public Row(Table table, int id) {
        this.table = table;
        this.id = id;
        this.cacheCompletion = table.fetch(id).handle((values, error) -> {
            this.cache = values;
            this.cacheError = error;
            return null;
        });
    }

    public CompletionStage<Void> getCacheCompletion() {
        return cacheCompletion;
    }

    public boolean isReady() {
        return cache != null || cacheError != null;
    }

    public Row assertOk() throws Throwable {
        if(cacheError != null) {
            throw cacheError;
        }
        return this;
    }

    public void serialize(ByteBuffer buffer) throws SerializationException {
        List<Column> columns = table.getColumns();
        for(int i = 0; i < columns.size(); i++) {
            columns.get(i).getType().serialize(buffer, cache.get(i));
        }
    }
}
