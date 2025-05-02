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

package oracle.kv.impl.async;

import java.io.EOFException;

import oracle.kv.impl.async.dialog.DialogContextImpl;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An input that consumes chunks of bytes.
 *
 * <p>The class provides methods to read from chunks of bytes for {@link
 * MessageInput}. It serves as a shared interface for a list of different byte
 * buffer implementations (e.g. netty ByteBuf and nio ByteBuffer).
 */
public interface BytesInput {
    /**
     * Fills the byte array with {@code len} bytes.
     *
     * @param b byte array
     * @param off starting offset to fill the array
     * @param len the maximum number of bytes to read
     * @throws EOFException if there is not enough data
     */
    void readFully(byte[] b, int off, int len) throws EOFException;

    /**
     * Skips {@code len} bytes.
     *
     * @param n the maximum number of bytes to skip
     * @throws EOFException if there is not enough data
     */
    void skipBytes(int n) throws EOFException;

    /**
     * Reads a byte.
     *
     * @return the byte read
     * @throws EOFException if there is not enough data
     */
    byte readByte() throws EOFException;

    /**
     * Returns the number of remaining bytes.
     *
     * @return the number of remaining bytes
     */
    int remaining();

    /**
     * Discards the bytes input and does clean up.
     */
    void discard();

    /**
     * Specifies the dialog that is reading this bytes input, for tracking.
     */
    void trackDialog(@Nullable DialogContextImpl dialog);

    /**
     * Marks that the belonging message input has been polled by the upper layer, for tracker.
     */
    void markMessageInputPolled();
}
