package com.collinswebsite.db;

import com.collinswebsite.db.types.IntegerDataType;
import com.collinswebsite.db.types.StringDataType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

public class DatabaseServer {
    private Map<String, Table> tables = new HashMap<>();

    public DatabaseServer() {

    }

    public void addTable(Table table) {
        tables.put(table.getName(), table);
    }

    private void launch() throws IOException {
        Selector sel = Selector.open();
        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(7788));
        server.configureBlocking(false);

        // The attachments I use here are BooleanSuppliers that perform whatever tasks are necessary and return
        // whether the selection key should be removed from the ready set or not.
        SelectionKey serverKey = server.register(sel, SelectionKey.OP_ACCEPT);

        serverKey.attach((BooleanSupplier) () -> {
            if(serverKey.isAcceptable()) {
                SocketChannel ch;
                try {
                    ch = server.accept();
                } catch(IOException e) {
                    System.out.println("Failed to accept connection:");
                    e.printStackTrace();
                    return true;
                }

                new SocketConnectionReader(this, ch, sel); // connections manage their own lifetimes via their SelectionKey registration.
            }
            return true;
        });

        while(true) {
            sel.select();

            sel.selectedKeys().removeIf(selectionKey -> ((BooleanSupplier) selectionKey.attachment()).getAsBoolean());
        }
    }

    public static void main(String[] args) throws IOException {
        DatabaseServer db = new DatabaseServer();
        db.addTable(new Table("people",
                new Column("birth_year", new IntegerDataType()),
                new Column("name", new StringDataType(64))));

        try {
            db.launch();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public Table defaultTable() {
        return tables.values().stream().findFirst().get();
    }

    public Table getTable(String name) {
        return tables.get(name);
    }
}
