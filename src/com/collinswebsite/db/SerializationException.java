package com.collinswebsite.db;

import java.io.EOFException;

public class SerializationException extends Exception {
    public SerializationException(EOFException e) {
        super(e);
    }

    public SerializationException() {

    }
}
