package com.collinswebsite.db;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.List;

public class SocketConnectionWriter {
    private final Cursor cursor;
    private final List<Column> columns;
    private final SocketConnectionState state;

    public SocketConnectionWriter(SocketConnectionState state, Cursor cursor, List<Column> columns) {
        this.state = state;
        this.cursor = cursor;
        this.columns = columns;

        // adjust our interest
        state.key.interestOps(SelectionKey.OP_WRITE);

        // reset the buffer
        state.buffer.clear();
    }

    public boolean process() {
        System.out.println("writer pumping out...");
        boolean ranOut = false;
        while(!cursor.isAtEnd() && state.buffer.remaining() >= cursor.getTable().getRowSize()) {
            try {
                Row r = cursor.getNext();
                if(r == null) {
                    // we have run out of rows
                    ranOut = true;
                    break;
                }
                r.serialize(state.buffer);
            } catch(Throwable throwable) {
                state.enterErrorState(throwable);
                return true;
            }
        }

        state.buffer.flip();
        if(state.buffer.remaining() > 0) {
            try {
                state.channel.write(state.buffer);
            } catch(IOException e) {
                state.enterErrorState(e);
                return true;
            }
        } else {
            if(ranOut && !cursor.isAtEnd()) {
                // cursor needs to fetch more rows...
                state.key.interestOps(0); // we're not interested in being able to write anymore

                cursor.await().thenRun(() -> {
                   state.key.interestOps(SelectionKey.OP_WRITE); // we are now interested in writing again.
                });
            }
        }

        if(cursor.isAtEnd()) {
            try {
                state.channel.close();
            } catch(IOException e) {
                // not much to do here...
            }
            state.key.cancel();
        }
        return true;
    }
}
