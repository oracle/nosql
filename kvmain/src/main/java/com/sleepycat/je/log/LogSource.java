/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.log;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;

/**
 * A class that implements LogSource can return portions of the log.
 * Is public for unit testing.
 */
public interface LogSource {

    /**
     * We're done with this log source.
     */
    void release() throws DatabaseException;

    /**
     * Fill the destination byte array with the requested number of bytes.  The
     * offset indicates the absolute position in the log file.
     *
     * @return a buffer containing the bytes, which may be the localBuffer if
     * non-null and large enough, and may be a buffer that can only be used
     * temporarily (until {@link #release()} is called).
     */
    ByteBuffer getBytes(long fileOffset, int numBytes, ByteBuffer localBuffer)
        throws DatabaseException;

    /**
     * Returns the log version of the log entries from this source.
     */
    int getLogVersion();
}
