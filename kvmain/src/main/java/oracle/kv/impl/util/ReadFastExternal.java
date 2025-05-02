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

import java.io.DataInput;
import java.io.IOException;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Represents the interface for deserializing an object when implementing
 * {@link FastExternalizable}.
 *
 * @param <T> the type of the object created
 */
public interface ReadFastExternal<T> {

    /**
     * Reads a fast externalizable object from the input stream.
     *
     * @param in the input stream
     * @param serialVersion the serial version to use for reading
     * @return the deserialized value, which may be null
     * @throws IOException if the data format is invalid or I/O failure occurs
     */
    T readFastExternal(@NonNull DataInput in, short serialVersion)
        throws IOException;
}
