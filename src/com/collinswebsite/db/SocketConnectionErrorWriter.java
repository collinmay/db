package com.collinswebsite.db;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.function.BooleanSupplier;

public class SocketConnectionErrorWriter {
    private final SocketConnectionState state;

    public SocketConnectionErrorWriter(SocketConnectionState state, String message) {
        this.state = state;

        // adjust our interest
        state.key.interestOps(SelectionKey.OP_WRITE);

        // reset the buffer
        state.buffer.clear();

        state.buffer.put(message.getBytes());
        state.buffer.put((byte) '\n');
        state.buffer.flip();
    }

    public boolean process() {
        if(state.buffer.remaining() > 0) {
            try {
                state.channel.write(state.buffer);
            } catch(IOException e) {
                state.enterErrorState(e);
                return true;
            }
        } else {
            state.key.attach((BooleanSupplier) new SocketConnectionReader(state)::process);
        }
        return true;
    }
}
