/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.async.dialog.netty;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import oracle.kv.impl.async.BytesInput;
import oracle.kv.impl.async.dialog.ChannelInput;

import com.sleepycat.util.PackedInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

/**
 * Input used to wrap around the netty byte buffer.
 */
class NettyChannelInput implements ChannelInput {

    private static final Charset utf8 = Charset.forName("UTF-8");

    private final byte[] packedLongBytes =
        new byte[PackedInteger.MAX_LONG_LENGTH];
    private ByteBuf buffer = null;

    /**
     * Sets the mark at the current position.
     *
     * Sets the reader index.
     */
    @Override
    public void mark() {
        buffer.markReaderIndex();
    }

    /**
     * Resets the position to a previously marked position.
     */
    @Override
    public void reset() {
        buffer.resetReaderIndex();
    }

    /**
     * Gets the number of readable bytes in the input.
     */
    @Override
    public int readableBytes() {
        return buffer.readableBytes();
    }

    /**
     * Reads a byte from the input.
     */
    @Override
    public byte readByte() {
        return buffer.readByte();
    }

    /**
     * Reads {@code len} bytes from the input.
     */
    @Override
    public BytesInput readBytes(int len) {
        ByteBuf buf = buffer.readRetainedSlice(len);
        return new NettyBytesInput(buf);
    }

    /**
     * Peeks at the input to see if there is enough data for {@code
     * readPackedLong}.
     */
    @Override
    public boolean canReadPackedLong() {
        if (buffer.readableBytes() == 0) {
            return false;
        }
        return (buffer.readableBytes() >= peekPackedLongLength());
    }

    /**
     * Reads a packed long.
     */
    @Override
    public long readPackedLong() {
        /* Get the length. */
        final int len = peekPackedLongLength();
        /* Get the bytes. */
        buffer.readBytes(packedLongBytes, 0, len);
        return PackedInteger.readLong(packedLongBytes, 0);
    }

    private int peekPackedLongLength() {
        packedLongBytes[0] = buffer.getByte(buffer.readerIndex());
        return PackedInteger.getReadLongLength(packedLongBytes, 0);
    }

    /**
     * Reads a {@code String} in standard UTF8 format.
     */
    @Override
    public String readUTF8(int length) {
        if (buffer.readableBytes() < length) {
            return null;
        }
        /* Read the UTF8 bytes */
        final byte[] bytes = new byte[length];
        buffer.readBytes(bytes, 0, length);
        return utf8.decode(ByteBuffer.wrap(bytes)).toString();
    }

    /**
     * Closes the input and release resources.
     */
    @Override
    public void close() {
        buffer = null;
    }

    /**
     * Returns the string representation.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ChannelInput:");
        if (buffer == null) {
            return builder.toString();
        }
        final int rlen = Math.min(32, buffer.readableBytes());
        final int wlen = Math.min(32, buffer.writableBytes());
        builder.append("R:");
        builder.append(ByteBufUtil.hexDump(buffer, buffer.readerIndex(), rlen));
        builder.append("W:");
        builder.append(ByteBufUtil.hexDump(buffer, buffer.writerIndex(), wlen));
        return builder.toString();
    }

    /**
     * Feeds the netty byte buf to the channel input.
     *
     * The input operates from the feeded buf afterwards. The caller is
     * guaranteed to feed this input the proper accumulative buf.
     */
    void feed(ByteBuf buf) {
        buffer = buf;
    }
}
