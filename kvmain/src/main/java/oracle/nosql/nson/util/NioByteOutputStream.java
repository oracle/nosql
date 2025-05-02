/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.util;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An implementation of ByteOutputStream using NIO ByteBuffer
 */
public class NioByteOutputStream implements ByteOutputStream {
    private static final int DEFAULT_INITIAL_CAPACITY = 2048;
    ByteBuffer buffer;
    int offset;
    boolean autoGrow;
    int autoGrowCount;

    /**
     * Creates a NioByteOutputStream, also allocating a heap-based ByteBuffer,
     * the capacity of ByteBuffer will grow automatically.
     * @return a new instance of NioByteOutputStream
     */
    public static NioByteOutputStream createNioByteOutputStream() {
        return new NioByteOutputStream(DEFAULT_INITIAL_CAPACITY, false);
    }

    /**
     * Creates a NioByteOutputStream, also allocating a direct ByteBuffer,
     * the capacity of ByteBuffer will grow automatically.
     * @return a new instance of NioByteOutputStream
     */
    public static NioByteOutputStream createDirectNioByteOutputStream() {
        return new NioByteOutputStream(DEFAULT_INITIAL_CAPACITY, true);
    }

    /**
     * Creates an instance, allocating a ByteBuffer with specified capacity,
     * the capacity of ByteBuffer will grow automatically. The ByteBuffer
     * can be direct or heap-based.
     * @param initCapacity the capacity, in bytes of the buffer
     * @param isDirect set to true if a direct (vs heap-allocated) buffer
     * is desired
     */
    public NioByteOutputStream(int initCapacity, boolean isDirect) {
        buffer = isDirect ? ByteBuffer.allocateDirect(initCapacity) :
            ByteBuffer.allocate(initCapacity);
        offset = 0;
        autoGrow = true;
        autoGrowCount = 0;
    }

    /**
     * Creates an instance with specified ByteBuffer with auto-grow enabled.
     * A new ByteBuffer will be created when bytes written exceed the capacity
     * of specified buffer.
     * @param buf the ByteBuffer to use
     */
    public NioByteOutputStream(ByteBuffer buf) {
        buffer = buf;
        offset = 0;
        autoGrow = true;
        autoGrowCount = 0;
    }

    /**
     * Creates an instance wrapping an existing byte[] with auto-grow disabled.
     * @param buf the byte[] buffer to use
     */
    public NioByteOutputStream(byte[] buf) {
        buffer = ByteBuffer.wrap(buf);
        offset = 0;
        autoGrow = false;
        autoGrowCount = 0;
    }

    /**
     * Returns the underlying ByteBuffer
     * @return the buffer
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }

    /**
     * Enable or disable autoGrow
     * @param isAutoGrow if autoGrow is enabled
     * @return the instance of NioByteOutputStream
     */
    public NioByteOutputStream setAutoGrow(boolean isAutoGrow) {
        this.autoGrow = isAutoGrow;
        return this;
    }

    /**
     * Returns the total number of auto-grow has been triggered.
     * @return total number of auto-grow
     */
    public int getAutoGrowCount() {
        return autoGrowCount;
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
    public void setWriteIndex(int index) {
        offset = index;
    }

    @Override
    public void skip(int numBytes) {
        ensureCapacity(numBytes);
        offset += numBytes;
    }

    @Override
    public void writeIntAtOffset(int woffset, int value) throws IOException {
        if (woffset > offset) {
            throw new IllegalArgumentException(
                "Invalid offset: " + woffset +
                " must be less than current offset: " + offset);
        }
        int currentOffset = offset;
        offset = woffset;
        writeInt(value);

        offset = currentOffset;
    }

    @Override
    public void writeBooleanAtOffset(int woffset, boolean value)
        throws IOException {

        if (woffset > offset) {
            throw new IllegalArgumentException(
                "Invalid offset: " + woffset +
                " must be less than current offset: " + offset);
        }
        int currentOffset = offset;
        offset = woffset;
        writeBoolean(value);
        offset = currentOffset;
    }

    @Override
    public void writeArrayAtOffset(int woffset, byte[] value)
        throws IOException {

        if ((woffset + value.length) > offset) {
            throw new IllegalArgumentException(
                "Invalid offset and length: " + (woffset + value.length) +
                " must be less than current offset: " + offset);
        }
        int currentOffset = offset;
        offset = woffset;
        write(value);
        offset = currentOffset;
    }

    @Override
    public void close() {}

    /*
     * DataOutput
     */

    @Override
    public void write(byte[] b) {
        ensureCapacity(b.length);
        buffer.position(offset);
        buffer.put(b);
        offset += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        ensureCapacity(len);
        buffer.position(offset);
        buffer.put(b, off, len);
        offset += len;
    }

    @Override
    public void write(int b) {
        /* see the interface contract for DataOutput.write(int) - one byte */
        writeByte((byte)b);
    }

    @Override
    public void writeBoolean(boolean v) {
        ensureCapacity(1);
        byte val = (byte) (v ? 1 : 0);
        buffer.put(offset, val);
        offset += 1;
    }

    @Override
    public void writeByte(int v) {
        ensureCapacity(1);
        buffer.put(offset, (byte)v);
        offset += 1;
    }

    @Override
    public void writeBytes(String s) {
        write(s.getBytes());
    }

    @Override
    public void writeChar(int v) {
        ensureCapacity(2);
        buffer.putChar(offset, (char)v);
        offset += 2;
    }

    @Override
    public void writeChars(String s) {
        throw new IllegalArgumentException("writeChars");
    }

    @Override
    public void writeDouble(double v) {
        ensureCapacity(8);
        buffer.putDouble(offset, v);
        offset += 8;
    }

    @Override
    public void writeFloat(float v) {
        ensureCapacity(4);
        buffer.putFloat(offset, v);
        offset += 4;
    }

    @Override
    public void writeInt(int v) {
        ensureCapacity(4);
        buffer.putInt(offset, v);
        offset += 4;
    }

    @Override
    public void writeLong(long v) {
        ensureCapacity(8);
        buffer.putLong(offset, v);
        offset += 8;
    }

    @Override
    public void writeShort(int v) {
        ensureCapacity(2);
        buffer.putShort(offset, (short)v);
        offset += 2;
    }

    @Override
    public void writeUTF(String s) {
        write(s.getBytes());
    }

    private void grow(int newCapacity) {
        if (newCapacity <= buffer.capacity()) {
            return;
        }

        int position = buffer.position();

        ByteBuffer oldBuffer = buffer;
        ByteBuffer newBuffer =  buffer.isDirect() ?
            ByteBuffer.allocateDirect(newCapacity) :
            ByteBuffer.allocate(newCapacity);
        oldBuffer.clear();
        newBuffer.put(oldBuffer);
        buffer = newBuffer;
        buffer.position(position);
    }

    @Override
    public void ensureCapacity(int nbytes) {
        int expectedCapacity = offset + nbytes;
        if (expectedCapacity >=  buffer.capacity()) {
            if (autoGrow) {
                /* Normalize to power of 2 for optimal memory usage */
                int newCapacity = Integer.highestOneBit(expectedCapacity);
                newCapacity <<= (newCapacity < expectedCapacity ? 1 : 0);
                grow(newCapacity);
                autoGrowCount++;
            } else {
                throw new IllegalArgumentException(
                    "Operation exceeds capacity of the buffer; it requires: " +
                    nbytes + ", available: " + (buffer.capacity() - offset));
            }
        }
    }
}
