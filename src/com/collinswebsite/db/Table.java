package com.collinswebsite.db;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class Table {
    private final String name;
    private final List<Column> columns;
    private final int rowSize;
    private AsynchronousFileChannel channel;

    public Table(String name, List<Column> columns) throws IOException {
        this.name = name;
        this.columns = columns;
        this.rowSize = columns.stream().mapToInt((c) -> c.getType().getSize()).sum();
        this.channel = AsynchronousFileChannel.open(FileSystems.getDefault().getPath("tables", name),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                //StandardOpenOption.APPEND,
                StandardOpenOption.CREATE);
    }

    /**
     * Convenience constructor.
     */
    public Table(String name, Column... columns) throws IOException {
        this(name, Arrays.asList(columns));
    }

    /**
     * @return The name of this table
     */
    public String getName() {
        return name;
    }

    public int getRowSize() {
        return this.rowSize;
    }

    public CompletionStage<List<Object>> fetch(int id) {
        ByteBuffer buffer = ByteBuffer.allocate(rowSize);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        channel.read(buffer, (long) id * (long) this.rowSize, future, FutureCompletionHandler.integer());
        return future.thenCompose((readLength) -> { // compose used for decent error handling
           if(readLength != rowSize) {
               return FutureUtil.exceptionalFuture(new EOFException());
           } else {
               buffer.flip();
               List<Object> values = new ArrayList<>(columns.size());
               try {
                   for(int i = 0; i < columns.size(); i++) {
                       values.add(columns.get(i).getType().deserialize(buffer));
                   }
               } catch(DeserializationException e) {
                   return FutureUtil.exceptionalFuture(e);
               }
               return CompletableFuture.completedFuture(values);
           }
        });
    }

    public FullScanCursor createFullTableScanCursor() {
        return new FullScanCursor(this);
    }

    public int getTotalCount() {
        try {
            return (int) (channel.size() / rowSize);
        } catch(IOException e) {
            return 0;
        }
    }

    public List<Column> getColumns() {
        return columns;
    }
}
