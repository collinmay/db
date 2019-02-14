package com.collinswebsite.db.types;

import com.collinswebsite.db.DeserializationException;
import com.collinswebsite.db.SerializationException;

import java.nio.ByteBuffer;

/**
 * Represents a data type. Provides serialization/deserialization.
 */
public interface DataType {
    Object deserialize(ByteBuffer buffer) throws DeserializationException;
    void serialize(ByteBuffer buffer, Object o) throws SerializationException;
    int getSize();

    boolean isComparable(DataType other);
    String getName();
}
