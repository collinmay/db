package com.collinswebsite.db;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;

/**
 * Handles the request reading + parsing part of a connection's lifetime.
 */
public class SocketConnectionReader {
    private final DatabaseServer db;
    private SocketConnectionState state;

    public SocketConnectionReader(DatabaseServer db, SocketChannel channel, Selector sel) {
        this.db = db;

        try {
            channel.configureBlocking(false);
            SelectionKey key = channel.register(sel, SelectionKey.OP_READ, (BooleanSupplier) SocketConnectionReader.this::process);
            this.state = new SocketConnectionState(channel, key);
        } catch(IOException e) {
            state.enterErrorState(e);
        }

        state.buffer.clear();
        state.buffer.flip();
    }

    /**
     * Run any tasks that might need to be run on the I/O thread.
     * @return Whether to remove the selection key from the ready set or not.
     */
    public boolean process() {
        if(!state.key.isValid()) {
            state.enterErrorState(new ClosedChannelException());
            return true;
        }

        try {
            if(state.key.isReadable()) {
                state.buffer.compact();
                if(state.channel.read(state.buffer) == -1) {
                    state.enterErrorState(new EOFException());
                    return true;
                }
                state.buffer.flip();

                byte[] bytes = new byte[state.buffer.remaining()];
                state.buffer.get(bytes);
                System.out.println("Connection: received \"" + new String(bytes, StandardCharsets.UTF_8) + "\"");
                state.key.attach((BooleanSupplier) new SocketConnectionWriter(state, db.defaultTable().createFullTableScanCursor())::process);
            }
        } catch(IOException e) {
            state.enterErrorState(e);
        }
        return true;
    }
}
