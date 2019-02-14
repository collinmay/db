package com.collinswebsite.db;

import com.collinswebsite.db.types.DataType;

/**
 * Represents a column within a table.
 */
public class Column {
    private final String name;
    private final DataType type;
    private int position;

    public Column(String name, DataType type) {
        this.name = name;
        this.type = type;
    }

    public DataType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public void setPosition(int i) {
        this.position = i;
    }

    public int getPosition() {
        return this.position;
    }
}
