package com.collinswebsite.db;

import java.io.IOException;

public class DeserializationException extends Exception {
    public DeserializationException() {

    }

    public DeserializationException(IOException e) {
        super(e);
    }
}
