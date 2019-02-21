package com.collinswebsite.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class Row {
    private final Table table;
    private final int id;
    private final List<Object> values;

    public Row(Table table, int id, List<Object> values) {
        this.table = table;
        this.id = id;
        this.values = values;
    }

    public void serialize(ByteBuffer buffer) throws SerializationException {
        List<Column> columns = table.getColumns();
        for(int i = 0; i < columns.size(); i++) {
            columns.get(i).getType().serialize(buffer, values.get(i));
        }
    }

    public Object getValueForColumn(Column column) {
        return values.get(column.getPosition());
    }

    public void delete() throws IOException {
        table.deleteRow(id);
    }

    public int getId() {
        return id;
    }
}
