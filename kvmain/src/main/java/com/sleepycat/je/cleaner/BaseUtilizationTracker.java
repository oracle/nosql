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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Shared implementation for all utilization trackers.  The base implementation
 * keeps track of per-file utilization info only.
 */
public abstract class BaseUtilizationTracker {

    EnvironmentImpl env;
    Cleaner cleaner;
    private long activeFile;

    /**
     * The tracked files are maintained in a volatile field Map that is "copied
     * on write" whenever an element is added or removed.  Add and remove are
     * called only under the log write latch, but get and iteration may be
     * performed at any time because the map is read-only.
     */
    volatile Map<Long,TrackedFileSummary> fileSummaries;

    BaseUtilizationTracker(EnvironmentImpl env) {
        this(env, env.getCleaner());
    }

    BaseUtilizationTracker(EnvironmentImpl env, Cleaner cleaner) {
        assert cleaner != null;
        this.env = env;
        this.cleaner = cleaner;
        fileSummaries = Collections.emptyMap();
        activeFile = -1;
    }

    public EnvironmentImpl getEnvironment() {
        return env;
    }

    /**
     * Returns a snapshot of the files being tracked as of the last time a
     * log entry was added.  The summary info returned is the delta since the
     * last checkpoint, not the grand totals, and is approximate since it is
     * changing in real time.  This method may be called without holding the
     * log write latch.
     *
     * <p>If files are added or removed from the collection of tracked files in
     * real time, the returned collection will not be changed since it is a
     * snapshot.  But the objects contained in the collection are live and will
     * be updated in real time under the log write latch.  The collection and
     * the objects in the collection should not be modified by the caller.</p>
     */
    public Collection<TrackedFileSummary> getTrackedFiles() {
        return fileSummaries.values();
    }

    /**
     * Returns one file from the snapshot of tracked files, or null if the
     * given file number is not in the snapshot array.
     *
     * @see #getTrackedFiles
     */
    public TrackedFileSummary getTrackedFile(long fileNum) {

        return fileSummaries.get(fileNum);
    }

    /**
     * Counts the addition of all new log entries including LNs.
     *
     * <p>For the global tracker, must be called under the log write latch.</p>
     */
    final void countNew(long lsn,
                        LogEntryType type,
                        int size) {
        assert type != null;
        /* Count in per-file and per-file-per-db summaries. */
        long fileNum = DbLsn.getFileNumber(lsn);
        FileSummary fileSummary = getFileSummary(fileNum);
        fileSummary.totalCount += 1;
        fileSummary.totalSize += size;
        boolean isLN = isLNType(type);
        if (isLN && fileSummary.maxLNSize < size) {
            fileSummary.maxLNSize = size;
        }
        if (trackObsoleteInfo(type)) {
            if (isLN) {
                fileSummary.totalLNCount += 1;
                fileSummary.totalLNSize += size;
            } else {
                fileSummary.totalINCount += 1;
                fileSummary.totalINSize += size;
            }
        }
    }

    /**
     * Counts an obsolete node by incrementing the obsolete count and size.
     * Tracks the LSN offset if trackOffset is true and the offset is non-zero.
     *
     * <p>For the global tracker, must be called under the log write latch.</p>
     */
    final void countObsolete(
        long lsn,
        LogEntryType type,
        int size,
        boolean trackOffset,
        boolean checkDupOffsets) {

        assert trackObsoleteInfo(type);

        boolean isLN = isLNType(type);
        long fileNum = DbLsn.getFileNumber(lsn);

        TrackedFileSummary fileSummary = getFileSummary(fileNum);
        if (isLN) {
            fileSummary.obsoleteLNCount += 1;
            /* The size is optional when tracking obsolete LNs. */
            if (size > 0) {
                fileSummary.obsoleteLNSize += size;
                fileSummary.obsoleteLNSizeCounted += 1;
            }
        } else {
            fileSummary.obsoleteINCount += 1;
            /* The size is not used when tracking obsolete INs. */
        }

        if (trackOffset) {
            long offset = DbLsn.getFileOffset(lsn);
            if (offset != 0) {
                fileSummary.trackObsolete(offset, checkDupOffsets);
            }
        }
    }

    /**
     * Returns whether file summary information for the given LSN is not
     * already counted.  Outside of recovery, always returns true.  For
     * recovery, is overridden by RecoveryUtilizationTracker and returns
     * whether the FileSummaryLN for the given file is prior to the given LSN.
     * .
     */
    boolean isFileUncounted(Long fileNum, long lsn) {
        return true;
    }

    /**
     * Transfers counts and offsets from this local tracker to the given
     * (global) UtilizationTracker.
     *
     * <p>When called after recovery has finished, must be called under the log
     * write latch.</p>
     */
    public void transferToUtilizationTracker(UtilizationTracker tracker)
        throws DatabaseException {

        /* Add file summary information, including obsolete offsets. */
        for (TrackedFileSummary localSummary : getTrackedFiles()) {
            TrackedFileSummary fileSummary =
                tracker.getFileSummary(localSummary.getFileNumber());
            fileSummary.addTrackedSummary(localSummary);
        }
    }

    /**
     * Counts all active LSNs in a database as obsolete in the per-file
     * utilization summaries.
     *
     * When an old version (version LT 16) MapLN deletion is replayed by
     * recovery, we must count it obsolete using the DbFileSummary metadata
     * that was maintained in this older version. For version 16 and above,
     * DB extinction is used instead.
     *
     * <p>For the global tracker, must be called under the log write latch.</p>
     *
     * @param dbFileSummaries the map of Long file number to DbFileSummary for
     * a database that is being deleted.
     *
     * @param mapLnLsn is the LSN of the MapLN when recovery is replaying the
     * truncate/remove, or NULL_LSN when called outside of recovery; obsolete
     * totals should only be counted when this LSN is prior to the LSN of the
     * FileSummaryLN for the file being counted.
     */
    public void countObsoleteOldVersionDb(
        DbFileSummaryMap dbFileSummaries,
        long mapLnLsn) {

        for (Map.Entry<Long, DbFileSummary> entry :
            dbFileSummaries.entrySet()) {

            Long fileNum = entry.getKey();

            if (isFileUncounted(fileNum, mapLnLsn)) {
                DbFileSummary dbFileSummary = entry.getValue();
                TrackedFileSummary fileSummary = getFileSummary(fileNum);

                /*
                 * Count as obsolete the currently active amounts in the
                 * database, which are the total amounts minus the previously
                 * counted obsolete amounts.
                 */
                int lnObsoleteCount = dbFileSummary.totalLNCount -
                                      dbFileSummary.obsoleteLNCount;
                int lnObsoleteSize  = dbFileSummary.totalLNSize -
                                      dbFileSummary.obsoleteLNSize;
                int inObsoleteCount = dbFileSummary.totalINCount -
                                      dbFileSummary.obsoleteINCount;
                fileSummary.obsoleteLNCount += lnObsoleteCount;
                fileSummary.obsoleteLNSize += lnObsoleteSize;
                fileSummary.obsoleteINCount += inObsoleteCount;

                /*
                 * When a DB becomes obsolete, the size of all obsolete LNs can
                 * now be counted accurately because all LN bytes in the DB are
                 * now obsolete.  The lnObsoleteSize value calculated above
                 * includes LNs that become obsolete now, plus those that
                 * became obsolete previously but whose size was not counted.
                 * The obsoleteLNSizeCounted field is updated accordingly
                 * below.  In other words, DB obsolescence is self-healing with
                 * respect to obsolete LN sizes.  [#19144]
                 */
                int lnObsoleteSizeCounted = lnObsoleteCount +
                                     (dbFileSummary.obsoleteLNCount -
                                      dbFileSummary.obsoleteLNSizeCounted);
                fileSummary.obsoleteLNSizeCounted += lnObsoleteSizeCounted;

                /*
                 * Do not update the DbFileSummary.  It will be flushed when
                 * the MapLN is deleted.  If later replayed during recovery, we
                 * will call this method to update the per-file utilization.
                 */
            }
        }
    }

    boolean isEmpty() {
        return fileSummaries.isEmpty();
    }

    /**
     * Returns a tracked file for the given file number, adding an empty one
     * if the file is not already being tracked.
     *
     * <p>For the global tracker, must be called under the log write latch.</p>
     */
    TrackedFileSummary getFileSummary(long fileNum) {

        if (activeFile < fileNum) {
            activeFile = fileNum;
        }
        Long fileNumLong = fileNum;
        TrackedFileSummary file = fileSummaries.get(fileNumLong);
        if (file == null) {
            /* Assign fileSummaries field after modifying the new map. */
            file = new TrackedFileSummary(this, fileNum, cleaner.trackDetail);
            Map<Long, TrackedFileSummary> newFiles =
                new HashMap<>(fileSummaries);
            newFiles.put(fileNumLong, file);
            fileSummaries = newFiles;
        }
        return file;
    }

    /**
     * Called after the FileSummaryLN is written to the log during checkpoint.
     *
     * <p>We keep the active file summary in the tracked file map, but we
     * remove older files to prevent unbounded growth of the map.</p>
     *
     * <p>Must be called under the log write latch.</p>
     */
    void resetFile(TrackedFileSummary fileSummary) {

        if (fileSummary.getFileNumber() < activeFile &&
            fileSummary.getAllowFlush()) {
            /* Assign fileSummaries field after modifying the new map. */
            Map<Long, TrackedFileSummary> newFiles =
                new HashMap<>(fileSummaries);
            newFiles.remove(fileSummary.getFileNumber());
            fileSummaries = newFiles;
        }
    }

    /**
     * Returns whether obsoleteness is tracked for the given type. Obsoleteness
     * is tracked for node types and BIN-deltas. A null type is assumed to be
     * an LN.
     */
    public static boolean trackObsoleteInfo(LogEntryType type) {
        return type == null ||
            type.isNodeType() ||
            type.equals(LogEntryType.LOG_BIN_DELTA);
    }

    /**
     * Returns whether the given type is an LN; a null type is assumed to be an
     * LN.
     */
    public static boolean isLNType(LogEntryType type) {
        return type == null || type.isLNType();
    }
}
