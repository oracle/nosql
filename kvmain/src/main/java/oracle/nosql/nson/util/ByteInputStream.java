/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.util;

import java.io.DataInput;

/**
 * An extension of DataInput that provides access to methods
 * to get and set offsets into the byte buffer that underlies the stream.
 */

public interface ByteInputStream extends DataInput, AutoCloseable {

    /**
     * Returns the current read offset
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
     * Sets the read offset. It can only be set smaller than the current
     * offset
     * @param offset the offset
     * @throws IllegalArgumentException if the offset exceeds the current offset
     */
    public void setOffset(int offset);

    /**
     * Skips the specified number of bytes
     * @param toSkip the number of bytes
     * @throws IllegalArgumentException if toSkip exceeds the capacity of
     * the underlying stream
     */
    public void skip(int toSkip);

    /**
     * This override avoids the default signature of Closeable that throws
     * Exception
     */
    @Override
    public void close();

    /**
     * Ensure that the buffer has at least this many bytes available to read
     * from the backing array.
     * @param nbytes the number of bytes required
     * @throws IllegalArgumentException if the specified number of bytes are
     * not available
     */
    public void ensureCapacity(int nbytes);
}
