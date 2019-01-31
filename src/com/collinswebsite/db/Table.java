package com.collinswebsite.db;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Table {
    private final String name;
    private final List<Column> columns;
    private final int rowSize;
    private FileChannel channel;

    public Table(String name, List<Column> columns) throws IOException {
        this.name = name;
        this.columns = columns;
        this.rowSize = columns.stream().mapToInt((c) -> c.getType().getSize()).sum();
        this.channel = FileChannel.open(FileSystems.getDefault().getPath("tables", name),
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

    public Row fetch(int id) throws DeserializationException {
        ByteBuffer buffer = ByteBuffer.allocate(rowSize);
        try {
            if(channel.read(buffer, (long) id * (long) this.rowSize) != rowSize) {
                throw new DeserializationException(new EOFException());
            }
        } catch(IOException e) {
            throw new DeserializationException(e);
        }

        buffer.flip();
        List<Object> values = new ArrayList<>(columns.size());
        for(int i = 0; i < columns.size(); i++) {
            values.add(columns.get(i).getType().deserialize(buffer));
        }

        return new Row(this, id, values);
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
