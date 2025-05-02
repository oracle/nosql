/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * An implementation of {@link ByteInputStream} using NIO {@link ByteBuffer}
 */
public class NioByteInputStream implements ByteInputStream {

    final ByteBuffer buffer;
    int offset;

    /**
     * Creates an instance based on an existing buffer
     * @param buffer the buffer to use
     */
    public NioByteInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
        offset = 0;
    }

    @Override
    public int getOffset() {
        return offset;
    }

    @Override
    public boolean isDirect() {
        return buffer.isDirect();
    }

    @Override
    public byte[] array() {
        return buffer.array();
    }

    @Override
    public void setOffset(int offset) {
        if (offset > this.offset) {
            throw new IllegalArgumentException(
                "setOffset: offset must be less than current offset");
        }
        this.offset = offset;
    }

    @Override
    public void skip(int numBytes) {
        ensureCapacity(numBytes);
        offset += numBytes;
    }

    @Override
    public void close() {}

    /*
     * DataIntput
     */

    @Override
    public void readFully(byte[] b) {
        ensureCapacity(b.length);
        buffer.position(offset);
        buffer.get(b);
        offset += b.length;
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        ensureCapacity(len);
        buffer.position(offset);
        buffer.get(b, off, len);
        offset += len;
    }

    @Override
    public boolean readBoolean() {
        ensureCapacity(1);
        byte b = buffer.get(offset);
        offset += 1;
        return (b == 0 ? false : true);
    }

    @Override
    public byte readByte() {
        ensureCapacity(1);
        byte b = buffer.get(offset);
        offset += 1;
        return b;
    }

    @Override
    public char readChar() {
        ensureCapacity(2);
        char ch = buffer.getChar(offset);
        offset += 2;
        return ch;
    }

    @Override
    public double readDouble() {
        ensureCapacity(8);
        double v = buffer.getDouble(offset);
        offset += 8;
        return v;
    }

    @Override
    public float readFloat() {
        ensureCapacity(4);
        float v = buffer.getFloat(offset);
        offset += 4;
        return v;
    }

    @Override
    public int readInt() {
        ensureCapacity(4);
        int v = buffer.getInt(offset);
        offset += 4;
        return v;
    }

    @Override
    public long readLong() {
        ensureCapacity(8);
        long v = buffer.getLong(offset);
        offset += 8;
        return v;
    }

    @Override
    public short readShort() {
        ensureCapacity(2);
        short v = buffer.getShort(offset);
        offset += 2;
        return v;
    }

    @Override
    public String readUTF() {
        try {
            return DataInputStream.readUTF(this);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Failed to read UTF : " + ioe);
        }
    }

    @Override
    public int skipBytes(int n) {
        skip(n);
        return n;
    }

    @Override
    public String readLine() {
        throw new IllegalArgumentException("readLine");
    }

    @Override
    public int readUnsignedByte() {
        return readByte() & 0xff;
    }

    @Override
    public int readUnsignedShort() {
        return readShort() & 0xffff;
    }

    @Override
    public void ensureCapacity(int nbytes) {
        if ((offset + nbytes) >  buffer.capacity()) {
            throw new IllegalArgumentException(
                "Operation exceeds capacity of the buffer; it requires: " +
                nbytes + ", available: " + (buffer.capacity() - offset));
        }
    }
}
