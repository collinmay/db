package com.collinswebsite.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Represents a data type. This includes the raw data type (int/string/etc.) and any parameterizations (maximum length),
 * and also provides serialization/deserialization.
 */
public class DataType {
    public Object deserialize(ByteBuffer buffer) throws DeserializationException {
        return rawType.deserialize(this, buffer);
    }

    public void serialize(ByteBuffer buffer, Object o) throws SerializationException {
        rawType.serialize(this, buffer, o);
    }

    private enum RawType {
        INTEGER { // integers are stored as 64-bit unsigned, but may be interpreted as signed.
            @Override
            public int getSize(DataType dataType) {
                return 8;
            }

            @Override
            public Object deserialize(DataType dataType, ByteBuffer buffer) throws DeserializationException {
                return buffer.getLong();
            }

            @Override
            public void serialize(DataType dataType, ByteBuffer buffer, Object o) {
                buffer.putLong((Long) o);
            }
        },
        STRING {  // these strings are 8-bit clean and not zero-terminated.
            @Override
            public int getSize(DataType dataType) {
                return 4 + dataType.maximumLength; // 4-byte size field + maximum data length
            }

            @Override
            public Object deserialize(DataType dataType, ByteBuffer buffer) throws DeserializationException {
                int length = buffer.getInt();
                if(length < 0 || length > dataType.maximumLength) {
                    throw new InvalidLengthException();
                }
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                buffer.position(buffer.position() + dataType.maximumLength - bytes.length);
                return new String(bytes, StandardCharsets.UTF_8);
            }

            @Override
            public void serialize(DataType dataType, ByteBuffer buffer, Object o) throws SerializationException {
                String s = (String) o;
                if(s.length() > dataType.maximumLength) {
                    throw new SerializationException();
                }
                buffer.putLong(s.length());
                buffer.put(s.getBytes());
                buffer.position(buffer.position() + dataType.maximumLength - s.length());
            }
        },
        ;

        public abstract int getSize(DataType dataType);
        public abstract Object deserialize(DataType dataType, ByteBuffer buffer) throws DeserializationException;
        public abstract void serialize(DataType dataType, ByteBuffer buffer, Object o) throws SerializationException;
    }

    private final RawType rawType;
    private final boolean isSigned; // only useful for integers
    private final int maximumLength; // only useful for strings

    /**
     * Don't use this constructor. Use DataType.integer and DataType.string instead.
     */
    private DataType(RawType rawType, boolean isSigned, int maximumLength) {
        this.rawType = rawType;
        this.isSigned = isSigned;
        this.maximumLength = maximumLength;
    }

    /**
     * @param isSigned Whether or not the returned data type should represent a signed integer.
     * @return A data type representing an integer.
     */
    public static DataType integer(boolean isSigned) {
        return new DataType(RawType.INTEGER, isSigned, 0);
    }

    /**
     * @param maximumLength Maximum length of string
     * @return A data type representing a string with the given maximum length.
     */
    public static DataType string(int maximumLength) {
        return new DataType(RawType.STRING, false, maximumLength);
    }

    /**
     * @return The size (in bytes) of one of this data type's values when serialized.
     */
    public int getSize() {
        return rawType.getSize(this);
    }
}
