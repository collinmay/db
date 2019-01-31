package com.collinswebsite.db.types;

import com.collinswebsite.db.DeserializationException;
import com.collinswebsite.db.SerializationException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringDataType implements DataType {
    private final int maximumLength;

    public StringDataType(int maximumLength) {
        this.maximumLength = maximumLength;
    }

    @Override
    public Object deserialize(ByteBuffer buffer) throws DeserializationException {
        int length = buffer.getInt();
        if(length > maximumLength) {
            throw new DeserializationException();
        }

        byte bytes[] = new byte[length];
        buffer.get(bytes);
        buffer.position(buffer.position() + maximumLength - length); // skip the rest of the field

        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void serialize(ByteBuffer buffer, Object o) throws SerializationException {
        String s = (String) o;
        if(s.length() > maximumLength) {
            throw new SerializationException();
        }

        buffer.putInt(s.length());
        buffer.put(s.getBytes());
        buffer.position(buffer.position() + maximumLength - s.length()); // skip the rest of the field
    }

    @Override
    public int getSize() {
        return 4 + maximumLength;
    }
}
