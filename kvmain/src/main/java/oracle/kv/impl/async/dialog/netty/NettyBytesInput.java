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

import java.io.EOFException;

import oracle.kv.impl.async.BytesInput;
import oracle.kv.impl.async.dialog.DialogContextImpl;

import io.netty.buffer.ByteBuf;

/**
 * An input that consumes chunks of bytes which wraps around the netty byte
 * buffer.
 */
class NettyBytesInput implements BytesInput {

    private final ByteBuf buffer;

    NettyBytesInput(ByteBuf buffer) {
        this.buffer = buffer;
    }

    /**
     * Fills the byte array with {@code len} bytes.
     */
    @Override
    public void readFully(byte[] b, int off, int len) throws EOFException {
        if (buffer.readableBytes() < len) {
            throw new EOFException();
        }
        buffer.readBytes(b, off, len);
        discardIfEmpty();
    }

    /**
     * Skips {@code len} bytes.
     */
    @Override
    public void skipBytes(int n) throws EOFException {
        if (buffer.readableBytes() < n) {
            throw new EOFException();
        }
        buffer.readerIndex(buffer.readerIndex() + n);
        discardIfEmpty();
    }

    /**
     * Reads a byte.
     */
    @Override
    public byte readByte() throws EOFException {
        if (buffer.readableBytes() == 0) {
            throw new EOFException();
        }
        byte b = buffer.readByte();
        discardIfEmpty();
        return b;
    }

    /**
     * Returns the number of remaining bytes.
     */
    @Override
    public int remaining() {
        return buffer.readableBytes();
    }

    /**
     * Discards the bytes input and does clean up.
     */
    @Override
    public void discard() {
        buffer.release();
    }

    @Override
    public void trackDialog(DialogContextImpl dialog) {
    }

    @Override
    public void markMessageInputPolled() {
    }

    public void discardIfEmpty() {
        if (buffer.readableBytes() == 0) {
            buffer.release();
        }
    }

}
