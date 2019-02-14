package com.collinswebsite.db.types;

import com.collinswebsite.db.DeserializationException;
import com.collinswebsite.db.SerializationException;

import java.nio.ByteBuffer;

public class BooleanDataType implements DataType {
    public static final BooleanDataType DEFAULT = new BooleanDataType();

    @Override
    public Object deserialize(ByteBuffer buffer) throws DeserializationException {
        return buffer.get() != 0;
    }

    @Override
    public void serialize(ByteBuffer buffer, Object o) throws SerializationException {
        buffer.put((byte) ((Boolean) o ? 1 : 0));
    }

    @Override
    public int getSize() {
        return 1;
    }

    @Override
    public boolean isComparable(DataType other) {
        return other instanceof BooleanDataType;
    }

    @Override
    public String getName() {
        return "boolean";
    }
}
