package com.collinswebsite.db.types;

import java.nio.ByteBuffer;

public class IntegerDataType implements DataType {
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
}
