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

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;

/**
 * When a log entry is no longer used, this class can be used to skip over it
 * when a file reader encounters it. All information about the entry is
 * simply discarded.
 *
 * <p>{@link UnsupportedOperationException} is thrown if an attempt is made
 * to write this entry.</p>
 */
public class ObsoleteLogEntry extends BaseEntry {

    @Override
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer) {
        entryBuffer.position(entryBuffer.position() + header.getItemSize());
    }

    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {
        return sb.append("<Obsolete/>");
    }

    @Override
    public Object getMainItem() {
        return null;
    }

    @Override
    public DatabaseId getDbId() {
        return null;
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    @Override
    public int getSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeEntry(ByteBuffer logBuffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean logicalEquals(LogEntry other) {
        return false;
    }

    @Override
    public void dumpRep(StringBuilder sb) {
    }
}
