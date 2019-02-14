package com.collinswebsite.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import static java.nio.channels.SelectionKey.OP_READ;

// Maintains some common state for socket connections, such as the channel, I/O buffer, and error handling.
public class SocketConnectionState {
    public final SocketChannel channel;
    public SelectionKey key; // I can't seem to convince the compiler that all code paths initialize this.

    public Throwable error;

    public ByteBuffer buffer = ByteBuffer.allocate(4096);
    private final DatabaseServer db;

    public SocketConnectionState(SocketChannel channel, Selector sel, DatabaseServer db) throws IOException {
        this.channel = channel;
        channel.configureBlocking(false);
        this.key = channel.register(sel, OP_READ);
        this.db = db;
    }

    public void enterErrorState(Throwable e) {
        e.printStackTrace();
        if(this.error == null) {
            // only track the first error, since it's probably the important one.
            this.error = e;

            try {
                if(this.channel != null) {
                    this.channel.close();
                }
            } catch(IOException ignored) {
                // ignore; there's really nothing reasonable we can do with this error
            }
            if(this.key != null) {
                this.key.cancel();
            }
        }
    }

    /**
     * @return Whether this socket connection should be removed from the active connection list
     */
    public boolean hasError() {
        return error != null;
    }

    public DatabaseServer getDb() {
        return db;
    }
}
