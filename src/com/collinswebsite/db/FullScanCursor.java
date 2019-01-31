package com.collinswebsite.db;

import java.nio.BufferUnderflowException;
import java.util.concurrent.CompletionStage;

public class FullScanCursor implements Cursor {
    private static final int BUFFER_SIZE = 16; // how many rows to attempt to hold
    private static final int REFILL_THRESHOLD = BUFFER_SIZE / 2;

    // this array is used as a ring buffer
    private final Row[] rows;

    // indices into rows array
    private int writeBufferHead = 0; // tracks how many entries in rows have been written and are valid
    private int readBufferHead = 0; // tracks how many entries in rows have been read out

    private int writeIndex = 0; // tracks the last row ID we requested
    private int readIndex = 0; // tracks the last row ID that was read out

    private Table table;

    public FullScanCursor(Table table) {
        this.table = table;
        this.rows = new Row[BUFFER_SIZE];
    }

    private void compact() {
        if(readBufferHead > 0) {
            System.arraycopy(rows, readBufferHead, rows, 0, readBufferHead);
            writeBufferHead -= readBufferHead;
            readBufferHead = 0;
        }
    }

    private void fetchRows() throws DeserializationException {
        compact();
        while(writeBufferHead < rows.length && writeIndex < table.getTotalCount()) {
            rows[writeBufferHead++] = table.fetch(writeIndex++);
        }
    }

    @Override
    public boolean isAtEnd() {
        return readIndex >= table.getTotalCount();
    }

    @Override
    public boolean isNextReady() {
        /*
        TODO: use a thread to prefetch rows
         */
        return readBufferHead < writeBufferHead && rows[readBufferHead] != null;
    }

    @Override
    public Row getNext() throws DeserializationException {
        if(readBufferHead >= writeBufferHead) {
            throw new BufferUnderflowException();
        }
        Row r = rows[readBufferHead++];

        if(writeBufferHead - readBufferHead <= REFILL_THRESHOLD) {
            fetchRows();
        }

        return r;
    }

    @Override
    public CompletionStage<Void> await() {
        throw new IllegalStateException(); // with this synchronous implementation, there should never be a need to await.
    }

    @Override
    public Table getTable() {
        return table;
    }
}
