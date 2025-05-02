/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.util;

import java.io.IOException;

/**
 * An extension of DataOutput that operates on an underlying stream and
 * provides access to methods to get and set offsets into the buffer
 * that underlies the stream.
 */
public interface ByteOutputStream extends java.io.DataOutput, AutoCloseable {

    /**
     * Returns the current write offset into the byte buffer
     * @return the offset
     */
    public int getOffset();

    /**
     * Returns true if the backing buffer is a direct buffer, indicating
     * that the underlying byte array is not accessible via the array() call.
     * @return true if direct
     */
    public boolean isDirect();

    /**
     * Returns the backing byte array
     * @return the array
     */
    public byte[] array();

    /**
     * Sets the current write offset into the byte buffer
     * @param index the offset/index
     */
    public void setWriteIndex(int index);

    /**
     * Skip numBytes, resetting the offset.
     * @param numBytes the number of bytes to skip
     */
    public void skip(int numBytes);

    /**
     * Writes the value at the specified offset. The offset must be less
     * than the current offset.
     * @param offset the offset
     * @param value the value to write
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if the offset exceeds the current offset
     */
    public void writeIntAtOffset(int offset, int value)
        throws IOException;

    /**
     * Writes the value at the specified offset. The offset must be less
     * than the current offset.
     * @param offset the offset
     * @param value the value to write
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if the offset exceeds the current offset
     */
    public void writeBooleanAtOffset(int offset, boolean value)
        throws IOException;

    /**
     * Writes the byte array at the specified offset. The offset must be less
     * than the current offset.
     * @param offset the offset
     * @param value the value to write
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if the offset exceeds the current offset
     */
    public void writeArrayAtOffset(int offset, byte[] value)
        throws IOException;

    /**
     * This override avoids the default signature of Closeable that throws
     * Exception
     */
    @Override
    public void close();

    /**
     * Ensure that the buffer has at least this many bytes available for
     * writing in the backing array. If the backing array automatically
     * grows it will be reallocated to a larger size and an exception is
     * not thrown unless there is insufficient memory available.
     *
     * @param nbytes the number of bytes required
     * @throws IllegalArgumentException if the specified number of bytes are
     * not available and the buffer does not grow on demand
     */
    public void ensureCapacity(int nbytes);
}
