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
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;

/**
 * A Log entry allows you to read, write and dump a database log entry.  Each
 * entry may be made up of one or more loggable items.
 *
 * <p>The log entry on disk consists of a log header defined by LogManager and
 * the specific contents of the log entry.
 *
 * <p>Log entries that support replication are required to implement {@link
 * ReplicableLogEntry}.
 */
public interface LogEntry extends Cloneable {

    /**
     * Inform a LogEntry instance of its corresponding LogEntryType.
     */
    public void setLogType(LogEntryType entryType);

    /**
     * @return the type of log entry
     */
    public LogEntryType getLogType();

    /**
     * Read in a log entry.
     */
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer);

    /**
     * Print out the contents of an entry.
     */
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose);

    /**
     * @return the first item of the log entry
     */
    public Object getMainItem();

    /**
     * Construct a complete item from a item entry, fetching additional log
     * entries as needed to ensure that a usable main object is available.
     *
     * For the BINDeltaLogEntry, the full BIN is not fetched,
     * since the partial BIN (the delta) is usable as a Node.
     */
    public Object getResolvedItem(DatabaseImpl dbImpl);

    /**
     * @return the ID of the database containing this entry, or null if this
     * entry type is not part of a database.
     */
    public DatabaseId getDbId();

    /**
     * @return return the transaction id if this log entry is transactional,
     * 0 otherwise.
     */
    public long getTransactionId();

    /**
     * @return size of byte buffer needed to store this entry.
     */
    public int getSize();

    /**
     * Serialize this object into the buffer.
     * @param logBuffer is the destination buffer
     */
    public void writeEntry(ByteBuffer logBuffer);

    /**
     * Returns true if this item should be counted as obsolete when logged.
     */
    public boolean isImmediatelyObsolete(DatabaseImpl dbImpl);

    /**
     * Returns whether this is a deleted LN.
     */
    public boolean isDeleted();

    /**
     * Do any processing we need to do after logging, while under the logging
     * latch.
     */
    public void postLogWork(LogEntryHeader header,
                            long justLoggedLsn,
                            long vlsn);

    /**
     * @return a shallow clone.
     */
    public LogEntry clone();

    /**
     * Used for determining that two matchpoints are logically the
     * same, to avoid selecting a matchpoint in divergent rep streams.

     * @return true if these two log entries are logically the same.
     */
    public boolean logicalEquals(LogEntry other);

    /**
     * Dump the contents of the log entry that are interesting for
     * replication.
     */
    public void dumpRep(StringBuilder sb);

    /* Convenience method to get a string rep of the log entry */
    default String dumpRep() {
        StringBuilder sb = new StringBuilder();
        dumpRep(sb);
        return sb.toString();
    }

    /**
     * A hook method which will be run under LogManger#serialLogWork when we
     * hold the serial lock. States that must be decided or comply to the
     * serial order (e.g., the master term for a MasterTxn) can be run inside
     * the hook.
     */
    default void runSerialHook() {
        /* Do nothing by default. */
    }
}
