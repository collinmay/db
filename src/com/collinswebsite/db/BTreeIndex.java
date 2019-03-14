package com.collinswebsite.db;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Stack;

public class BTreeIndex implements TableIndex {
    private static final int VALUE_SIZE = 8;
    private static final int LOCATION_SIZE = 8;
    private static final int HEADER_SIZE = 4 + 4 + LOCATION_SIZE;

    private final FileChannel channel;
    private final int minimumDegree;
    private final int keySize;
    private final int nodeSize;
    private final ByteBuffer nodeBuffer;

    private final BTreeNode root;

    private static int compare(byte[] a, byte[] b) {
        for(int i = 0; i < a.length && i < b.length; i++) {
            if(a[i] != b[i]) {
                // make unsigned
                int aByte = a[i] & 0xFF;
                int bByte = b[i] & 0xFF;
                return aByte - bByte;
            }
        }
        return a.length - b.length;
    }

    private class BTreeNode {
        private final long location;
        private int numKeys;
        private byte[][] keys;
        private long[] values;
        private long[] children;
        private boolean isLeaf;

        public BTreeNode(long location) {
            this.location = location;
            this.numKeys = 0;
            this.keys = new byte[getMaxKeys()][];
            this.values = new long[getMaxKeys()];
            this.children = new long[getMaxChildren()];
            this.isLeaf = true;
        }

        public void load() throws DeserializationException, IOException {
            nodeBuffer.clear();
            if(channel.read(nodeBuffer, HEADER_SIZE + (nodeSize * location)) != nodeSize) {
                throw new DeserializationException(new EOFException());
            }
            nodeBuffer.flip();

            //System.out.println("loading node from " + location);

            this.numKeys = nodeBuffer.getInt();
            //System.out.println("numKeys: " + numKeys);

            for(int i = 0; i < getMaxKeys(); i++) {
                int length = (int) nodeBuffer.getLong();
                keys[i] = new byte[length];
                nodeBuffer.get(keys[i]);
                nodeBuffer.position(nodeBuffer.position() + (keySize - length));
                //System.out.println("  keys[" + i + "] = " + new String(keys[i]));
            }
            for(int i = 0; i < getMaxKeys(); i++) {
                values[i] = nodeBuffer.getLong();
                //System.out.println("  values[" + i + "] = " + values[i]);
            }
            for(int i = 0; i < getMaxChildren(); i++) {
                children[i] = nodeBuffer.getLong();
                //System.out.println("  children[" + i + "] = " + children[i]);
            }
            isLeaf = nodeBuffer.get() != 0;
            //System.out.println("isLeaf: " + isLeaf);
        }

        public void store() throws SerializationException, IOException {
            nodeBuffer.clear();
            nodeBuffer.putInt(numKeys);
            for(int i = 0; i < getMaxKeys(); i++) {
                nodeBuffer.putLong(keys[i].length);
                nodeBuffer.put(keys[i]);
                nodeBuffer.position(nodeBuffer.position() + (keySize - keys[i].length));
            }
            for(int i = 0; i < getMaxKeys(); i++) {
                nodeBuffer.putLong(values[i]);
            }
            for(int i = 0; i < getMaxChildren(); i++) {
                nodeBuffer.putLong(children[i]);
            }
            nodeBuffer.put((byte) (isLeaf ? 1 : 0));

            if(channel.write(nodeBuffer, HEADER_SIZE + nodeSize * location) != nodeSize) {
                throw new SerializationException(new EOFException());
            }
        }

        public boolean search(Stack<IterationRecord> path, byte[] key) throws DeserializationException, IOException {
            //System.out.println("searching for " + new String(key));
            int i = 0;
            // find first key that compares greater than what we're looking for
            while(i < numKeys && compare(key, keys[i]) > 0) {
                //System.out.println("  skipping over " + new String(keys[i]));
                i++;
            }

            // check if we found it
            if(i < numKeys) {
                path.push(new IterationRecord(this, i));
                if(compare(key, keys[i]) == 0) {
                    return true;
                }
            }

            if(isLeaf) {
                // must not be in the tree
                return false;
            } else {
                // recurse
                return fetchNode(children[i]).search(path, key);
            }
        }
    }

    private class IterationRecord {
        public BTreeNode node;
        public int index;

        public IterationRecord(BTreeNode node, int index) {
            this.node = node;
            this.index = index;
        }
    }

    public class Entry {
        public byte[] key;
        public long value;

        public Entry(byte[] key, long value) {
            this.key = key;
            this.value = value;
        }
    }

    public BTreeIndex(Column name, String fileName) throws IOException, DeserializationException {
        this.channel = FileChannel.open(FileSystems.getDefault().getPath("indices", fileName),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
        if(channel.read(headerBuffer, 0) != HEADER_SIZE) {
            throw new DeserializationException(new EOFException());
        }
        headerBuffer.flip();

        this.minimumDegree = headerBuffer.getInt();
        this.keySize = headerBuffer.getInt();

        this.nodeSize = 4 + // number of keys
                (getMaxKeys() * (8 + keySize)) + // keys
                (getMaxKeys() * VALUE_SIZE) + // values
                (getMaxChildren() * LOCATION_SIZE) + // locations
                1; // is leaf
        this.nodeBuffer = ByteBuffer.allocate(nodeSize);

        this.root = fetchNode(headerBuffer.getLong());
    }

    private int getMinKeys() {
        return this.minimumDegree - 1;
    }

    private int getMinChildren() {
        return this.minimumDegree;
    }

    private int getMaxKeys() {
        return this.minimumDegree * 2 - 1;
    }

    private int getMaxChildren() {
        return this.minimumDegree * 2;
    }

    private BTreeNode fetchNode(long location) throws DeserializationException, IOException {
        BTreeNode node = new BTreeNode(location);
        node.load();
        return node;
    }

    public Iterator<Entry> iterate(byte[] start) throws DeserializationException, IOException {
        Stack<IterationRecord> path = new Stack<>();
        root.search(path, start);
        return new BTreeIterator(path);
    }

    private class BTreeIterator implements Iterator<Entry> {
        private Stack<IterationRecord> path;

        public BTreeIterator(Stack<IterationRecord> path) {
            this.path = path;
            //dumpIterationState();
        }

        private void dumpIterationState() {
            for(int i = 0; i < path.size(); i++) {
                IterationRecord r = path.get(i);
                System.out.println("node<" + r.node.location + ">[" + r.index + "] = (" + new String(r.node.keys[r.index]) + ")");
            }
        }

        @Override
        public boolean hasNext() {
            return !path.empty();
        }

        @Override
        public Entry next() {
            IterationRecord lastRecord = path.peek();
            Entry e = new Entry(lastRecord.node.keys[lastRecord.index], lastRecord.node.values[lastRecord.index]);
            lastRecord.index++;
            // if we've run off the end, pop us off the stack.
            if(lastRecord.index >= lastRecord.node.numKeys) {
                // go up a level
                path.pop();
            }
            // if we weren't a leaf, enter the next child.
            try {
                while(!lastRecord.node.isLeaf) {
                    lastRecord = new IterationRecord(fetchNode(lastRecord.node.children[lastRecord.index]), 0);
                    path.push(lastRecord);
                }
            } catch(DeserializationException | IOException ex) {
                throw new RuntimeException(ex);
            }
            return e;
        }
    }
}
