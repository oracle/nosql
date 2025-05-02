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

package com.sleepycat.je.cleaner;

import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Accumulates changes to the utilization profile during recovery.
 *
 * <p>Per-database information is keyed by DatabaseId because the DatabaseImpl
 * is not always available during recovery.  In fact this is the only reason
 * that a "local" tracker is used during recovery -- to avoid requiring that
 * the DatabaseImpl is available, which is necessary to use the "global"
 * UtilizationTracker.  There is no requirement to accumulate totals locally,
 * since recovery is single threaded.</p>
 *
 * <p>When finished with this object, its information should be added to the
 * Environment's UtilizationTracker and DatabaseImpl objects by calling
 * transferToUtilizationTracker.  This is done at the end of recovery, just
 * prior to the checkpoint.  It does not have to be done under the log write
 * latch, since recovery is single threaded.</p>
 */
public class RecoveryUtilizationTracker extends BaseUtilizationTracker {

    /* File number -> LSN of FileSummaryLN */
    private final Map<Long, Long> fileSummaryLsns;

    public RecoveryUtilizationTracker(EnvironmentImpl env) {
        super(env);
        fileSummaryLsns = new HashMap<>();
    }

    /**
     * Saves the LSN of the last logged FileSummaryLN.
     */
    public void saveLastLoggedFileSummaryLN(long fileNum, long lsn) {
        fileSummaryLsns.put(fileNum, lsn);
    }

    /**
     * Counts the addition of all new log entries including LNs.
     */
    public void countNewLogEntry(
        long lsn,
        LogEntryType type,
        int size) {
        
        countNew(lsn, type, size);
    }

    /**
     * Counts the LSN of a node obsolete unconditionally.
     *
     * Even when trackOffset is true, duplicate offsets are not checked (no
     * assertion is fired) because recovery is known to count the same LSN
     * offset twice in certain circumstances.
     */
    public void countObsoleteUnconditional(
        long lsn,
        LogEntryType type,
        int size,
        boolean trackOffset) {

        countObsolete(
            lsn, type, size,
            trackOffset,
            false);     // checkDupOffsets
    }

    /**
     * Counts the oldLsn of a node obsolete if it has not already been counted
     * at the point of lsn in the log.
     *
     * Even when trackOffset is true, duplicate offsets are not checked (no
     * assertion is fired) because recovery is known to count the same LSN
     * offset twice in certain circumstances.
     */
    public void countObsoleteIfUncounted(
        long oldLsn,
        long newLsn,
        LogEntryType type,
        int size,
        boolean trackOffset) {
        
        Long fileNum = DbLsn.getFileNumber(oldLsn);

        if (!isFileUncounted(fileNum, newLsn)) {
            return;
        }

        countObsolete(
            oldLsn, type, size,
            trackOffset,
            false);        // checkDupOffsets
    }

    /**
     * Overrides this method for recovery and returns whether the most recently
     * seen FileSummaryLN for the given file is prior to the given LSN.
     */
    @Override
    boolean isFileUncounted(Long fileNum, long lsn) {

        long fsLsn = DbLsn.longToLsn(fileSummaryLsns.get(fileNum));

        int cmpFsLsnToNewLsn = (fsLsn != DbLsn.NULL_LSN ?
                                DbLsn.compareTo(fsLsn, lsn) : -1);

        return cmpFsLsnToNewLsn < 0;
    }

    /**
     * Clears all accumulated utilization info for the given file.
     */
    public void resetFileInfo(long fileNum) {
        TrackedFileSummary trackedSummary = getTrackedFile(fileNum);
        if (trackedSummary != null) {
            trackedSummary.reset();
        }
    }
}
