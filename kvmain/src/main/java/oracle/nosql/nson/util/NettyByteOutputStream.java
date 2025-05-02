/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.util;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

/**
 * An implementation of ByteOutputStream that uses Netty's ByteBufOutputStream.
 */
public class NettyByteOutputStream extends ByteBufOutputStream
    implements ByteOutputStream {

    final ByteBuf buffer;
    final boolean releaseBuffer;

    /**
     * Creates an instance using a (Netty) ByteBuf. The ByteBuf remains
     * owned by the caller and is not released upon close().
     *
     * @param buffer the buffer
     */
    public NettyByteOutputStream(ByteBuf buffer) {
        super(buffer);
        this.buffer = buffer;
        this.releaseBuffer = false;
    }

    /**
     * Creates a NettyByteOutputStream, also allocating a heap-based ByteBuf.
     * This buffer must be released by calling {@link #close}.
     * @return a new instance
     */
    public static NettyByteOutputStream createNettyByteOutputStream() {
        return new NettyByteOutputStream(Unpooled.buffer(), true);
    }

    /**
     * Creates a NettyByteOutputStream, also allocating a direct ByteBuf. This
     * buffer must be released by calling {@link #close}.
     * @return a new instance
     */
    public static NettyByteOutputStream createDirectNettyByteOutputStream() {
        return new NettyByteOutputStream(Unpooled.directBuffer(), true);
    }

    protected NettyByteOutputStream(ByteBuf buffer, boolean release) {
        super(buffer);
        this.buffer = buffer;
        this.releaseBuffer = release;
    }

    @Override
    public void close() {
        if (releaseBuffer) {
            buffer.release();
        }
        try {
            super.close();
        } catch (IOException ioe) {}
    }

    /**
     * Returns the underlying ByteBuf
     * @return the buffer
     */
    public ByteBuf getBuffer() {
        return buffer;
    }

    /*
     * From ByteOutputStream
     */

    @Override
    public int getOffset() {
        return buffer.writerIndex();
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
        buffer.writerIndex(index);
    }

    @Override
    public void skip(int numBytes) {
        buffer.ensureWritable(numBytes);
        buffer.writerIndex(buffer.writerIndex() + numBytes);
    }

    @Override
    public void writeIntAtOffset(int offset, int value) throws IOException {
        int currentOffset = buffer.writerIndex();
        if (offset > currentOffset) {
            throw new IllegalArgumentException(
                "Invalid offset: " + offset +
                " must be less than current offset: " + currentOffset);
        }
        buffer.writerIndex(offset);
        writeInt(value);

        /* reset */
        buffer.writerIndex(currentOffset);

    }

    @Override
    public void writeBooleanAtOffset(int offset, boolean value)
        throws IOException {

        int currentOffset = buffer.writerIndex();
        if (offset > currentOffset) {
            throw new IllegalArgumentException(
                "Invalid offset: " + offset +
                " must be less than current offset: " + currentOffset);
        }
        buffer.writerIndex(offset);
        writeBoolean(value);

        /* reset */
        buffer.writerIndex(currentOffset);

    }

    @Override
    public void writeArrayAtOffset(int offset, byte[] value)
        throws IOException {

        int currentOffset = buffer.writerIndex();
        if ((offset + value.length) > currentOffset) {
            throw new IllegalArgumentException(
                "Invalid offset and length: " + (offset + value.length) +
                " must be less than current offset: " + currentOffset);
        }
        buffer.writerIndex(offset);
        write(value);

        /* reset */
        buffer.writerIndex(currentOffset);
    }

    @Override
    public void ensureCapacity(int nbytes) {
        /*
         * this method will expand the buffer but not beyond its maxCapacity
         * if set. To expand beyond maxCapacity, an additional "true" argument
         * is required. Honor the maxCapacity if set.
         */
        buffer.ensureWritable(nbytes);
    }
}
