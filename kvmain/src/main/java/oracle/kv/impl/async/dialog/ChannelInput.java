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

package oracle.kv.impl.async.dialog;

import oracle.kv.impl.async.BytesInput;

/**
 * An input interface to read from async channels.
 *
 * This class provides method to read all supported primitive data types from
 * the async channel.
 *
 * Each method is non-blocking and deal with cases when there is not enough
 * data. If the return value is a java primitive data type, the read method
 * throws some {@link RuntimeException} (e.g., {@link IllegalStateException} or
 * {@link IndexOutOfBoundsException}) if there is not enough data; caller
 * should check the data availablity before read. If the return value is an
 * object, the read method returns {@code null}.
 */
public interface ChannelInput {
    /**
     * Sets a mark on the current read position.
     */
    void mark();

    /**
     * Rewinds to the marked position and clears the mark.
     */
    void reset();

    /**
     * Gets the number of readable bytes in the input.
     */
    int readableBytes();

    /**
     * Reads a byte from the input.
     *
     * @return the byte read
     */
    byte readByte();

    /**
     * Reads {@code len} bytes from the input.
     *
     * @param len the number of bytes to read
     * @return the {@link BytesInput} containing the bytes, null if not enough
     * data
     */
    BytesInput readBytes(int len);

    /**
     * Peeks at the input to see if there is enough data for {@code
     * readPackedLong}.
     *
     * @return {@code true} if there is enough data
     */
    boolean canReadPackedLong();

    /**
     * Reads a packed long.
     *
     * @return the long read
     * @throws RuntimeException (or something) if not enough data
     */
    long readPackedLong();

    /**
     * Reads a UTF8-encoded {@code String} with the specified length.
     *
     * @param length the length of the UTF8 string
     * @return the string, null if not enough data.
     */
    String readUTF8(int length);

    /**
     * Close the input and release resources.
     */
    void close();
}
