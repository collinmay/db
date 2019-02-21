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
    /*
    Free list scheme:
    Each row in the table starts with a field called the "free link". For rows that are not deleted, this field is zero.
    For rows that are deleted, this field contains either the index of the next deleted row or -1 to indicate the end of
    the free list.
    At the start of the table is row 0. Row 0 is the "invalid row", and is used as the head of the free list. This means
    that we can find an empty row by starting at row 0 and walking the linked list until we either find a row we can
    claim or reach the end of the file, at which point we will have to extend it.
    When we delete a row, we can insert it in the free list immediately after the head row, since it's not important
    that the order of the free list reflect the order of the rows in the file.
     */

    private static final int FREE_LINK_FIELD_SIZE = 8;
    private final String name;
    private final List<Column> columns;
    private final int rowSize;
    private FileChannel channel;

    public Table(String name, List<Column> columns) throws IOException {
        this.name = name;
        this.columns = columns;
        for(int i = 0; i < this.columns.size(); i++) {
            this.columns.get(i).setPosition(i);
        }
        this.rowSize = columns.stream().mapToInt((c) -> c.getType().getSize()).sum() + FREE_LINK_FIELD_SIZE;
        this.channel = FileChannel.open(FileSystems.getDefault().getPath("tables", name),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);

        if(getCapacity() == 0) {
            // this is probably a new table, so write out the free list head.
            ByteBuffer buffer = ByteBuffer.allocate(rowSize);
            buffer.putLong(-1);
            buffer.position(rowSize);
            buffer.flip();

            channel.write(buffer, 0);
        }
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

    // returns null if row has been deleted
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

        long freeLink = buffer.getLong();
        if(freeLink != 0) {
            return null;
        }

        List<Object> values = new ArrayList<>(columns.size());
        for(int i = 0; i < columns.size(); i++) {
            values.add(columns.get(i).getType().deserialize(buffer));
        }

        return new Row(this, id, values);
    }

    private void alterLink(int id, long link) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(FREE_LINK_FIELD_SIZE);
        buffer.putLong(link);
        if(channel.write(buffer, (long) id * (long) this.rowSize) != FREE_LINK_FIELD_SIZE) {
            throw new IOException("couldn't write completely");
        }
    }

    private long readLink(int id) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(FREE_LINK_FIELD_SIZE);
        if(channel.read(buffer, (long) id * (long) this.rowSize) != FREE_LINK_FIELD_SIZE) {
            throw new EOFException();
        }
        buffer.flip();
        return buffer.getLong();
    }

    private void truncateTable() throws IOException {
        // figure out how many rows we can truncate the table to.
        int i = getCapacity();
        while(readLink(--i) != 0) { }
        channel.truncate((long) i * (long) rowSize);

        // TODO: walk free list and remove entries that now point past the end of the table
    }

    public void deleteRow(int id) throws IOException {
        alterLink(id, readLink(0));
        alterLink(0, id);
    }

    public Row insertRow(List<Object> values) throws IOException, SerializationException {
        int id = (int) readLink(0);
        if(id == -1) { // free list is empty
            // append to table
            id = getCapacity();
        } else {
            // remove from free list
            alterLink(0, readLink(id));
        }

        Row r = new Row(this, id, values);
        ByteBuffer buffer = ByteBuffer.allocate(rowSize);

        buffer.putLong(0); // free link
        r.serialize(buffer);

        buffer.flip();

        if(channel.write(buffer, (long) id * (long) this.rowSize) != this.rowSize) {
            throw new IOException("couldn't write completely");
        }

        return r;
    }

    public FullScanCursor createFullTableScanCursor() {
        return new FullScanCursor(this);
    }

    public int getCapacity() {
        try {
            return (int) (channel.size() / rowSize);
        } catch(IOException e) {
            return 0;
        }
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(String name) {
        return columns.stream().filter((c) -> c.getName().equals(name)).findFirst().orElse(null);
    }
}
