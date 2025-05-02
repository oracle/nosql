/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.util;

/**
 * An implementation of ByteInputStream that uses Netty's
 * ByteBufInputStream
 */
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

/**
 * A ByteInputStream based on Netty's ByteBufInputStream
 */
public class NettyByteInputStream extends ByteBufInputStream
    implements ByteInputStream {

    final ByteBuf buffer;

    /**
     * Creates an instance using a (Netty) ByteBuf
     * @param buffer the buffer
     */
    public NettyByteInputStream(ByteBuf buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    /**
     * Creates an instance using an existing byte array
     * @param bytes the byte array
     * @return the stream
     */
    public static NettyByteInputStream createFromBytes(byte[] bytes) {
        return new NettyByteInputStream(Unpooled.wrappedBuffer(bytes));
    }

    @Override
    public int getOffset() {
        return buffer.readerIndex();
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
        try {
            buffer.readerIndex(offset);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void skip(int toSkip) {
        setOffset(getOffset() + toSkip);
    }

    /**
     * Returns the underlying (Netty) ByteBuf
     * @return the buffer
     */
    public ByteBuf buffer() {
        return buffer;
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (Exception e) {}
    }

    @Override
    public void ensureCapacity(int nbytes) {
        if (nbytes > buffer.readableBytes()) {
            throw new IllegalArgumentException(
                "Operation exceeds capacity of the buffer; it requires: " +
                nbytes + ", available: " + buffer.readableBytes());
        }
    }
}
