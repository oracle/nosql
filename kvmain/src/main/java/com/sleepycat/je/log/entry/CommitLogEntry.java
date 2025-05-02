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

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.txn.TxnCommit;

/**
 * Log entry for a transaction commit.
 */
public class CommitLogEntry extends SingleItemReplicableEntry<TxnCommit> {

    /**
     * The log version number of the most recent change for this log entry,
     * including any changes to the format of the underlying {@link TxnCommit}
     * object.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE =
        LogEntryType.LOG_VERSION_MASTER_TERM;

    /** Construct a log entry for reading a {@link TxnCommit} object. */
    public CommitLogEntry() {
        super(TxnCommit.class);
    }

    /** Construct a log entry for writing a {@link TxnCommit} object. */
    public CommitLogEntry(final TxnCommit commit) {
        super(LogEntryType.LOG_TXN_COMMIT, commit);
    }

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public void runSerialHook() {
        final TxnCommit commit = getMainItem();
        commit.updateMasterIdTerm();
    }
}
