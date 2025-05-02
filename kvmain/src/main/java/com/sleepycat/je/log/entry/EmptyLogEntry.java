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

package com.sleepycat.je.log.entry;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.Loggable;

/**
 * Contains no information, implying that the LogEntryType is the only
 * information needed.
 * <p>
 * A single byte is actually written, but this is only to satisfy non-null
 * buffer dependencies in ChecksumValidator and file readers.
 */
public class EmptyLogEntry implements Loggable {

    public EmptyLogEntry() {
    }

    @Override
    public int getLogSize() {
        return 1;
    }

    @Override
    public void writeToLog(ByteBuffer logBuffer) {
        logBuffer.put((byte) 42);
    }

    @Override
    public void readFromLog(EnvironmentImpl envImpl,
                            ByteBuffer logBuffer,
                            int entryVersion) {
        logBuffer.get();
    }

    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<Empty/>");
    }

    /**
     * Always return false, this item should never be compared.
     */
    @Override
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    @Override
    public long getTransactionId() {
        return 0;
    }
}
