package com.collinswebsite.db.types;

import java.nio.ByteBuffer;

public class IntegerDataType implements DataType {
    public static final IntegerDataType DEFAULT = new IntegerDataType();

    @Override
    public Object deserialize(ByteBuffer buffer) {
        return buffer.getLong();
    }

    @Override
    public void serialize(ByteBuffer buffer, Object o) {
        buffer.putLong((Long) o);
    }

    @Override
    public int getSize() {
        return 8;
    }

    @Override
    public boolean isComparable(DataType other) {
        return other instanceof IntegerDataType;
    }

    @Override
    public String getName() {
        return "int";
    }
}
