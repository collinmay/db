package com.collinswebsite.db;

import com.collinswebsite.db.miniql.MiniQLLexer;
import com.collinswebsite.db.miniql.MiniQLListener;
import com.collinswebsite.db.miniql.MiniQLParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.util.function.BooleanSupplier;

/**
 * Handles the request reading + parsing part of a connection's lifetime.
 */
public class SocketConnectionReader {
    private final DatabaseServer db;
    private SocketConnectionState state;
    private StringBuilder currentRequest = new StringBuilder();

    public SocketConnectionReader(SocketConnectionState state) {
        this.state = state;
        this.db = state.getDb();

        // adjust our interest
        state.key.interestOps(SelectionKey.OP_READ);

        // reset the buffer
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

                char ch = 0;
                while(state.buffer.remaining() > 0) {
                    ch = (char) state.buffer.get();
                    if(ch != '\n') {
                        currentRequest.append(ch);
                    } else {
                        break;
                    }
                }

                if(ch == '\n') {
                    // Reached end of request. Parse it and process it.
                    try {
                        MiniQLLexer lexer = new MiniQLLexer(CharStreams.fromString(currentRequest.toString()));
                        CommonTokenStream tokens = new CommonTokenStream(lexer);
                        MiniQLParser parser = new MiniQLParser(tokens);
                        MiniQLParser.StatementFragmentContext ctx = parser.statement().statementFragment();

                        MiniQLListener interpreterListener = new MiniQLInterpreterListener(state, db);
                        ParseTreeWalker.DEFAULT.walk(interpreterListener, ctx);
                    } catch(Exception e) {
                        e.printStackTrace();
                        state.key.attach((BooleanSupplier) new SocketConnectionErrorWriter(
                                state,
                                "ERROR: " + e.toString())::process);
                    }
                } else {
                    // Haven't reached end of request yet.
                }
            }
        } catch(IOException e) {
            state.enterErrorState(e);
        }
        return true;
    }
}
