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

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.utilint.VLSN;

/**
 * When a log entry is erase, it is overwritten in place. Its type is set to
 * {@link LogEntryType#LOG_ERASED} and its item bytes are set to zero. However,
 * an entry may be partially erased, in which case {@link #isAllZeros}
 * returns false.
 *
 * The original entry type is lost, but the other fields of the header are left
 * in place. The item size, previous offset, VLSN and flags are left intact.
 * The checksum field is left intact, but is no longer valid.
 *
 * @see com.sleepycat.je.cleaner.DataEraser
 */
public class ErasedLogEntry implements LogEntry {

    private boolean allZeros;

    public ErasedLogEntry () {
    }

    @Override
    public int getSize() {
        throw EnvironmentFailureException.unexpectedState();
    }

    @Override
    public void writeEntry(ByteBuffer logBuffer) {
        throw EnvironmentFailureException.unexpectedState();
    }

    @Override
    public boolean isImmediatelyObsolete(DatabaseImpl dbImpl) {
        throw EnvironmentFailureException.unexpectedState();
    }

    @Override
    public boolean isDeleted() {
        return false;
    }

    @Override
    public void postLogWork(LogEntryHeader header,
                            long justLoggedLsn,
                            long vlsn) {
        throw EnvironmentFailureException.unexpectedState();
    }

    @Override
    public ErasedLogEntry clone() {
        try {
            return (ErasedLogEntry) super.clone();
        } catch (CloneNotSupportedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    @Override
    public void setLogType(LogEntryType entryType) {
    }

    @Override
    public LogEntryType getLogType() {
        return LogEntryType.LOG_ERASED;
    }

    @Override
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer) {

        final byte[] buf = new byte[100];
        int remaining = header.getItemSize();
        allZeros = true;

        while (remaining > 0) {

            final int size = Math.min(remaining, buf.length);
            entryBuffer.get(buf, 0, size);
            remaining -= size;

            for (int i = 0; i < size; i += 1) {
                if (buf[i] != 0) {
                    allZeros = false;
                }
            }
        }
    }

    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {
        sb.append("<Erased allZeros=\"").append(allZeros).append("\"/>");
        return sb;
    }

    /**
     * Returns whether the item bytes are all zeros, meaning that erasure of
     * this entry is complete. A partially erased entry can occur when a
     * crash occurs between writing the header type byte persistently and
     * writing the item bytes persistently.
     */
    public boolean isAllZeros() {
        return allZeros;
    }

    @Override
    public Object getMainItem() {
        return null;
    }

    @Override
    public Object getResolvedItem(DatabaseImpl dbImpl) {
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
    public boolean logicalEquals(LogEntry other) {
        return false;
    }

    @Override
    public void dumpRep(StringBuilder sb) {
    }
}
