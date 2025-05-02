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

package oracle.kv.impl.util;

import java.io.DataOutput;
import java.io.IOException;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Represents the interface for serializing an object when implementing {@link
 * FastExternalizable}.
 *
 * <p>Note that objects that implement {@link FastExternalizable} can be used
 * as lambdas for this interface because the object's this reference counts as
 * the first argument.
 *
 * @param <T> the type of the object being serialized
 */
public interface WriteFastExternal<T> {

    /**
     * Writes an object to the stream.
     *
     * @param object the object to write
     * @param out the output stream
     * @param serialVersion the serial version to use for writing
     * @throws IOException if an I/O failure occurs
     */
    void writeFastExternal(T object,
                           @NonNull DataOutput out,
                           short serialVersion)
        throws IOException;

    /**
     * Writes a String to the stream using the same parameter order as
     * {#writeFastExternal} so that it can be used conveniently as a {@link
     * WriteFastExternal}.
     *
     * @param value the string to write
     * @param out the output stream
     * @param serialVersion the serial version to use for writing
     * @throws IOException if an I/O failure occurs
     */
    static void writeString(String value,
                            @NonNull DataOutput out,
                            short serialVersion)
        throws IOException
    {
        SerializationUtil.writeString(out, serialVersion, value);
    }
}
