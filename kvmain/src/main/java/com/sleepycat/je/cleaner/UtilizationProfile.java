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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.StartupTracker;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.recovery.RecoveryInfo;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.VLSN;

/**
 * The UP tracks utilization summary information for all log files.
 *
 * <p>Unlike the UtilizationTracker, the UP is not accessed under the log write
 * latch and is instead synchronized on itself for protecting the cache.  It is
 * not accessed during the primary data access path, except for when flushing
 * (writing) file summary LNs.  This occurs in the following cases:
 * <ol>
 * <li>The summary information is flushed at the end of a checkpoint.  This
 * allows tracking to occur in memory in between checkpoints, and replayed
 * during recovery.</li>
 * <li>When committing the truncateDatabase and removeDatabase operations, the
 * summary information is flushed because detail tracking for those operations
 * is not replayed during recovery</li>
 * <li>The evictor will ask the UtilizationTracker to flush the largest summary
 * if the memory taken by the tracker exceeds its budget.</li>
 * </ol>
 *
 * <p>The cache is populated by the RecoveryManager just before performing the
 * initial checkpoint.  The UP must be open and populated in order to respond
 * to requests to flush summaries and to evict tracked detail, even if the
 * cleaner is disabled.</p>
 *
 * <p>WARNING: While synchronized on this object, eviction is not permitted.
 * If it were, this could cause deadlocks because the order of locking would be
 * the UP object and then the evictor.  During normal eviction the order is to
 * first lock the evictor and then the UP, when evicting tracked detail.</p>
 *
 * <p>The methods in this class synchronize to protect the cached summary
 * information.  Some methods also access the UP database.  However, because
 * eviction must not occur while synchronized, UP database access is not
 * performed while synchronized except in one case: when inserting a new
 * summary record.  In that case we disallow eviction during the database
 * operation.</p>
 */
public class UtilizationProfile {

    private static final Runnable EMPTY_RUNNABLE = () -> {};

    private final EnvironmentImpl env;
    private final UtilizationTracker tracker;
    private DatabaseImpl fileSummaryDb;
    private DatabaseImpl reservedFilesDb;
    private SortedMap<Long, FileSummary> fileSummaryMap;
    private boolean cachePopulated;
    private final Logger logger;

    private static TestHook<EnvironmentImpl> incompleteReactivateHook;

    /**
     * Creates an empty UP.
     */
    public UtilizationProfile(EnvironmentImpl env,
                              UtilizationTracker tracker) {
        this.env = env;
        this.tracker = tracker;
        fileSummaryMap = new TreeMap<>();

        logger = LoggerUtils.getLogger(getClass());
    }

    /**
     * Gets the base summary from the cached map.  Add the tracked summary, if
     * one exists, to the base summary.  Sets all entries obsolete, if the file
     * is in the migrateFiles set.
     */
    public synchronized FileSummary getFileSummary(Long file) {

        /* Get base summary. */
        FileSummary summary = fileSummaryMap.get(file);

        /* Add tracked summary */
        TrackedFileSummary trackedSummary = tracker.getTrackedFile(file);
        if (trackedSummary != null) {
            FileSummary totals = new FileSummary();
            if (summary != null) {
                totals.add(summary);
            }
            totals.add(trackedSummary);
            summary = totals;
        }

        return summary;
    }

    /**
     * Count the given locally tracked info as obsolete and then log the file
     * info.
     */
    public void flushLocalTracker(LocalUtilizationTracker localTracker)
        throws DatabaseException {

        if (localTracker.isEmpty()) {
            return;
        }

        /* Count tracked info under the log write latch. */
        env.getLogManager().transferToUtilizationTracker(localTracker);

        /* Write out the modified file info. */
        flushFileUtilization(localTracker.getTrackedFiles());
    }

    /**
     * Flush a FileSummaryLN node for each given TrackedFileSummary.
     */
    public void flushFileUtilization
        (Collection<TrackedFileSummary> activeFiles)
        throws DatabaseException {

        /* Utilization flushing may be disabled for unittests. */
        if (!DbInternal.getCheckpointUP
            (env.getConfigManager().getEnvironmentConfig())) {
            return;
        }

        /* Write out the modified file summaries. */
        for (TrackedFileSummary activeFile : activeFiles) {
            long fileNum = activeFile.getFileNumber();
            TrackedFileSummary tfs = tracker.getTrackedFile(fileNum);
            if (tfs != null) {
                flushFileSummary(tfs);
            }
        }
    }

    /**
     * Returns a copy of the current file summary map, optionally including
     * tracked summary information, for use by the DbSpace utility and by unit
     * tests.  The returned map's key is a Long file number and its value is a
     * FileSummary.
     */
    public synchronized SortedMap<Long, FileSummary>
        getFileSummaryMap(boolean includeTrackedFiles) {

        assert cachePopulated;

        if (includeTrackedFiles) {

            /*
             * Copy the fileSummaryMap to a new map, adding in the tracked
             * summary information for each entry.
             */
            TreeMap<Long, FileSummary> map = new TreeMap<>();
            for (Long file : fileSummaryMap.keySet()) {
                FileSummary summary = getFileSummary(file);
                map.put(file, summary);
            }

            /* Add tracked files that are not in fileSummaryMap yet. */
            for (TrackedFileSummary summary : tracker.getTrackedFiles()) {
                Long fileNum = summary.getFileNumber();
                if (!map.containsKey(fileNum)) {
                    map.put(fileNum, summary);
                }
            }
            return map;
        } else {
            return new TreeMap<>(fileSummaryMap);
        }
    }

    /**
     * Gets the size of the file. If the file does not exist in fileSummaryMap,
     * then return -1.
     */
    public synchronized int getFileSize(Long file) {

        FileSummary summary = getFileSummary(file);
        if (summary == null) {
            return -1;
        } else {
            return summary.totalSize;
        }
    }

    /**
     * Returns a simplified copy of the current file summary map, i.e. the
     * value is only the total size of the file.
     *
     * Besides, we are not sure whether the FileSummary for the current
     * last file is complete, so we remove the entry for the last file.
     */
    public synchronized SortedMap<Long, Integer> getFileSizeSummaryMap() {

        TreeMap<Long, Integer> map = new TreeMap<>();

        for (Long fileNum : fileSummaryMap.keySet()) {
            int totalSize = getFileSize(fileNum);
            map.put(fileNum, totalSize);
        }

        /* Add tracked size, or create entry if not yet in the map. */
        for (TrackedFileSummary trackedSummary : tracker.getTrackedFiles()) {
            Long fileNum = trackedSummary.getFileNumber();
            if (!map.containsKey(fileNum)) {
                map.put(fileNum, trackedSummary.totalSize);
            }
        }

        /* Remove the last file. */
        if (!map.isEmpty()) {
            map.remove(map.lastKey());
        }

        return map;
    }

    /**
     * Updates the reserved file db and file summary db after removing a set of
     * files from the FileSelector and changing their status to reserved in the
     * FileProtector.
     * 1. Insert reserved file record for each file.
     * 2. Delete file summary db records for each file.
     *
     * See populateCache for how a crash during these steps is handled.
     */
    void reserveFiles(final Map<Long, FileSelector.FileInfo> reservedFiles) {

        for (final Map.Entry<Long, FileSelector.FileInfo> entry :
                reservedFiles.entrySet()) {

            final FileSelector.FileInfo fsInfo = entry.getValue();
            putReservedFileRecord(entry.getKey(), fsInfo);
        }

        final Set<Long> files = reservedFiles.keySet();

        for (final Long file : files) {
            deleteFileSummary(file);
        }
    }

    /**
     * Stores a reserved file db record for a file that has been cleaned and
     * is ready-to-deleted.
     */
    private void putReservedFileRecord(
        final Long file,
        final FileSelector.FileInfo fsInfo) {

        final DatabaseEntry keyEntry = new DatabaseEntry();
        ReservedFileInfo.keyToEntry(file, keyEntry);

        final ReservedFileInfo rfInfo = new ReservedFileInfo(
            fsInfo.firstVlsn, fsInfo.lastVlsn);

        final DatabaseEntry dataEntry = new DatabaseEntry();
        ReservedFileInfo.objectToEntry(rfInfo, dataEntry);

        final Locker locker =
            BasicLocker.createBasicLocker(env, false /*noWait*/);

        try {
            try (Cursor cursor = DbInternal.makeCursor(
                reservedFilesDb, locker, null, false /*retainNonTxnLocks*/)) {

                cursor.put(keyEntry, dataEntry, Put.OVERWRITE, null);
            }
        } finally {
            locker.operationEnd();
        }
    }

    /**
     * Deletes a reserved file db record after the file has been deleted.
     * Also used when reactivating a reserved file.
     */
    void deleteReservedFileRecord(final Long file) {

        final DatabaseEntry keyEntry = new DatabaseEntry();
        ReservedFileInfo.keyToEntry(file, keyEntry);

        final ReadOptions readOptions =
            new ReadOptions().setLockMode(LockMode.RMW);

        final Locker locker =
            BasicLocker.createBasicLocker(env, false /*noWait*/);
        try {
            try (Cursor cursor =
                    DbInternal.makeCursor(reservedFilesDb, locker, null)) {

                if (cursor.get(
                    keyEntry, null, Get.SEARCH, readOptions) != null) {

                    cursor.delete(null);
                }
            }
        } finally {
            locker.operationEnd();
        }
    }

    /**
     * Removes a file from the utilization database and the profile, after
     * it has been determined that the file does not exist. This is unusual.
     */
    void removeDeletedFile(Long fileNum)
        throws DatabaseException {

        removeFileSummaries(Collections.singleton(fileNum));
        deleteFileSummary(fileNum);
    }

    /**
     * Remove newly reserved files from the utilization profile cache. This is
     * called while synchronized on the FileSelector to prevent a window where
     * the file could be selected for cleaning again.
     *
     * This method does not delete the utilization db records.
     */
    synchronized void removeFileSummaries(final Set<Long> files) {
        assert cachePopulated;

        for (final Long fileNum : files) {
            fileSummaryMap.remove(fileNum);
        }
    }

    /**
     * Deletes all FileSummaryLNs for the file.  This method performs eviction
     * and is not synchronized.
     */
    private void deleteFileSummary(final Long fileNum)
        throws DatabaseException {

        Locker locker = null;
        CursorImpl cursor = null;
        try {
            locker = BasicLocker.createBasicLocker(env, false /*noWait*/);
            cursor = new CursorImpl(fileSummaryDb, locker);
            /* Perform eviction in unsynchronized methods. */
            cursor.setAllowEviction(true);

            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry dataEntry = new DatabaseEntry();

            /* Do not return data to avoid a fetch of the existing LN. */
            dataEntry.setPartial(0, 0, true);

            /* Search by file number. */
            OperationResult result = null;
            if (getFirstFSLN(
                cursor, fileNum, keyEntry, dataEntry, LockType.WRITE)) {
                result = DbInternal.DEFAULT_RESULT;
            }

            /* Delete all LNs for this file number. */
            while (result != null &&
                   fileNum ==
                   FileSummaryLN.getFileNumber(keyEntry.getData())) {

                /* Perform eviction once per operation. */
                env.daemonEviction(true /*backgroundIO*/);

                /*
                 * Eviction after deleting is not necessary since we did not
                 * fetch the LN.
                 */
                cursor.deleteCurrentRecord(ReplicationContext.NO_REPLICATE);

                result = cursor.getNext(keyEntry, dataEntry, LockType.WRITE);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }

        /* Explicitly remove the file from the tracker.  */
        TrackedFileSummary tfs = tracker.getTrackedFile(fileNum);
        if (tfs != null) {
            env.getLogManager().removeTrackedFile(tfs);
        }
    }

    /**
     * Updates and stores the FileSummary for a given tracked file, if flushing
     * of the summary is allowed.
     */
    void flushFileSummary(TrackedFileSummary tfs)
        throws DatabaseException {

        if (tfs.getAllowFlush()) {
            putFileSummary(tfs);
        }
    }

    /**
     * Updates and stores the FileSummary for a given tracked file.  This
     * method is synchronized and may not perform eviction.
     */
    private synchronized PackedOffsets putFileSummary(TrackedFileSummary tfs)
        throws DatabaseException {

        if (env.isReadOnly()) {
            throw EnvironmentFailureException.unexpectedState
                ("Cannot write file summary in a read-only environment");
        }

        if (tfs.isEmpty()) {
            return null; // no delta
        }

        if (!cachePopulated) {
            /* Db does not exist and this is a read-only environment. */
            return null;
        }

        long fileNum = tfs.getFileNumber();
        Long fileNumLong = fileNum;

        /* Get existing file summary or create an empty one. */
        FileSummary summary = fileSummaryMap.get(fileNumLong);
        if (summary == null) {

            /*
             * An obsolete node may have been counted after its file was
             * reserved or even deleted, for example, when compressing a BIN.
             * Do not insert a new profile record if the file is reserved or
             * if no corresponding log file exists.
             */
            if (!env.getFileProtector().isActiveOrNewFile(fileNumLong)) {

                /*
                 * File was deleted by the cleaner.  Remove it from the
                 * UtilizationTracker and return.  Note that a file is normally
                 * removed from the tracker by FileSummaryLN.writeToLog method
                 * when it is called via insertFileSummary below. [#15512]
                 */
                env.getLogManager().removeTrackedFile(tfs);
                return null;
            }

            summary = new FileSummary();
        }

        /*
         * The key discriminator is a sequence that must be increasing over the
         * life of the file.  We use the sum of all entries counted.  We must
         * add the tracked and current summaries here to calculate the key.
         */
        FileSummary tmp = new FileSummary();
        tmp.add(summary);
        tmp.add(tfs);
        int sequence = tmp.getEntriesCounted();

        /* Insert an LN with the existing and tracked summary info. */
        FileSummaryLN ln = new FileSummaryLN(summary, env);
        ln.setTrackedSummary(tfs);
        addFileSummary(ln, fileNumLong, sequence);

        return ln.getObsoleteOffsets();
    }

    /**
     * Inserts the FileSummaryLN and adds the summary to the profile map.
     */
    private synchronized void addFileSummary(
        FileSummaryLN ln,
        Long fileNum,
        int sequence) {

        insertFileSummary(ln, fileNum, sequence);

        /* Cache the updated summary object.  */
        fileSummaryMap.put(fileNum, ln.getBaseSummary());
    }

    /**
     * Using a dummy FileSummary, inserts a FileSummaryLN in the file summary
     * DB and adds the summary to the utilization profile map.
     *
     * <p>The dummy file summary has the correct total size for the file, and
     * its obsolete size is also set to the total size to cause cleaning.
     * Performing cleaning is the simplest way to update overall utilization
     * after reactivating a file, and such files normally have low utilization
     * anyway (since they were previously cleaned and deleted.)</p>
     *
     * @return the size of the file.
     */
    private synchronized long addReactivatedFileSummary(Long file) {

        /*
         * It is important that no other FileSummaryLNs are present for the
         * file, since for simplicity we use sequence zero for the LN's
         * record key. There should be no existing FileSummaryLNs for this
         * file, but it is inexpensive and safe to delete any existing LNs.
         */
        deleteFileSummary(file);

        final File fileObj =
            new File(env.getFileManager().getFullFileName(file));
        final int size = (int) fileObj.length();

        final FileSummary summary = new FileSummary();
        summary.totalCount = 1;
        summary.totalSize = size;
        summary.totalLNCount = 1;
        summary.totalLNSize = size;
        summary.maxLNSize = size;
        summary.obsoleteLNCount = 1;
        summary.obsoleteLNSize = size;
        summary.obsoleteLNSizeCounted = 1;

        final FileSummaryLN ln = new FileSummaryLN(summary, env);
        addFileSummary(ln, file, 0 /*sequence*/);

        return size;
    }

    /**
     * Reactivates a reserved file and causes it to be cleaned.
     *
     * <p>This is used when explicitly activating a reserved file, e.g., when
     * we believe it was incorrectly cleaned. The following actions are
     * performed in the order listed:</p>
     * <ol>
     *     <li>The file is removed from the reserved file DB and an explicit
     *     flush-no-sync is performed.</li>
     *
     *     <li>A FileSummaryLN for the file is added to the file summary DB
     *     using a dummy file summary that will cause the file to be cleaned.
     *     The summary is also added to the utilization profile map. No flush
     *     is performed.</li>
     *
     *     <li>The file is removed from the FileProtector's reserved file set
     *     and added to its active file set.</li>
     * </ol>
     *
     * <p>Step 1 makes the active state (the repair) durable. If a crash
     * occurs after the flush, {@link #populateCache} will ensure that the
     * actions in steps 2 and 3 are completed at startup time.</p>
     *
     * <p>Steps 2 and 3 could be done in any order but it seems safer to
     * update the in-memory state last to prevent operations causing
     * utilization tracking, which could cause a FileSummaryLN to be added
     * by another thread. Note that {@link #putFileSummary} prevents writing
     * FileSummaryLNs for reserved files.</p>
     */
    public void reactivateReservedFile(final Long file) {

        deleteReservedFileRecord(file);
        env.flushLog(false /*fsync*/);

        LoggerUtils.warning(
            logger, env,
            "Reactivated reserved file: 0x" + Long.toHexString(file));

        if (incompleteReactivateHook != null) {
            incompleteReactivateHook.doHook(env);
        }

        final long size = addReactivatedFileSummary(file);
        env.getFileProtector().reactivateReservedFile(file, size);
    }

    /**
     * Returns the stored/packed obsolete offsets for the given file.
     *
     * @param logUpdate if true, log any updates to the utilization profile. If
     * false, only retrieve the new information.
     *
     * @param beforeWork is a method to be called before each significant work
     * unit. Can be used to throw a RuntimeException in order to cancel the
     * operation.
     *
     * @param obsoleteBeforeCkpt is true if offsets should be returned only
     * if a full checkpoint was performed after the entry become obsolete.
     */
    PackedOffsets getObsoleteDetailPacked(Long fileNum,
                                          boolean logUpdate,
                                          Runnable beforeWork,
                                          boolean obsoleteBeforeCkpt)
        throws DatabaseException {

        final PackedOffsets packedOffsets = new PackedOffsets();

        /* Return if no detail is being tracked. */
        if (!env.getCleaner().trackDetail) {
            return packedOffsets;
        }

        packedOffsets.pack(
            getObsoleteDetailInternal(
                fileNum, logUpdate, beforeWork, obsoleteBeforeCkpt));

        return packedOffsets;
    }

    /**
     * Returns the sorted obsolete offsets for the given file.
     */
    public long[] getObsoleteDetailSorted(Long fileNum)
        throws DatabaseException {

        long[] sortedOffsets = new long[0];

        /* Return if no detail is being tracked. */
        if (!env.getCleaner().trackDetail) {
            return sortedOffsets;
        }

        sortedOffsets = getObsoleteDetailInternal(fileNum, false, null, false);
        Arrays.sort(sortedOffsets);
        return sortedOffsets;

    }

    private long[] getObsoleteDetailInternal(Long fileNum,
                                             boolean logUpdate,
                                             Runnable beforeWork,
                                             boolean obsoleteBeforeCkpt)
        throws DatabaseException {

        assert cachePopulated;

        if (beforeWork == null) {
            beforeWork = EMPTY_RUNNABLE;
        }

        final long fileNumVal = fileNum;
        final List<long[]> list = new ArrayList<>();

        boolean foundAnchorLsn = false;
        final long maxAnchorLsn =
            env.getCheckpointer().getLastCheckpointFirstActiveLsn();

        /*
         * Get a TrackedFileSummary that cannot be flushed (evicted) while we
         * gather obsolete offsets.
         */
        final TrackedFileSummary tfs =
            env.getLogManager().getUnflushableTrackedSummary(fileNumVal);
        try {
            /* Read the summary db. */
            final Locker locker =
                BasicLocker.createBasicLocker(env, false /*noWait*/);
            final CursorImpl cursor = new CursorImpl(fileSummaryDb, locker);
            try {
                /* Perform eviction in unsynchronized methods. */
                cursor.setAllowEviction(true);

                final DatabaseEntry keyEntry = new DatabaseEntry();
                final DatabaseEntry dataEntry = new DatabaseEntry();

                beforeWork.run();

                /* Search by file number. */
                OperationResult result = null;
                if (getFirstFSLN(cursor, fileNumVal, keyEntry, dataEntry,
                                  LockType.NONE)) {
                    result = DbInternal.DEFAULT_RESULT;
                }

                /* Read all LNs for this file number. */
                while (result != null) {

                    /* Perform eviction once per operation. */
                    env.daemonEviction(true /*backgroundIO*/);

                    final FileSummaryLN ln = (FileSummaryLN)
                        cursor.lockAndGetCurrentLN(LockType.NONE);

                    if (ln != null) {
                        /* Stop if the file number changes. */
                        if (fileNumVal !=
                            FileSummaryLN.getFileNumber(keyEntry.getData())) {
                            break;
                        }

                        final long anchorLsn = ln.getAnchorLsn();

                        if (!obsoleteBeforeCkpt ||
                            anchorLsn == DbLsn.NULL_LSN ||
                            DbLsn.compareTo(anchorLsn, maxAnchorLsn) < 0) {

                            foundAnchorLsn = anchorLsn != DbLsn.NULL_LSN;

                            final PackedOffsets offsets =
                                ln.getObsoleteOffsets();
                            if (offsets != null) {
                                list.add(offsets.toArray());
                            }
                        }

                        /* Always evict after using a file summary LN. */
                        cursor.evictLN();
                    }

                    beforeWork.run();

                    result = cursor.getNext(
                        keyEntry, dataEntry, LockType.NONE);
                }
            } finally {
                cursor.close();
                locker.operationEnd();
            }

            beforeWork.run();

            /*
             * If foundAnchorLsn then we know the most recent FSLN has a
             * non-null anchorLsn, and any FSLNs with null anchorLsns were
             * logged earlier and therefore their offsets can be included.
             */
            if (obsoleteBeforeCkpt && !foundAnchorLsn) {
                /* All FSLNs were created prior to log version 19. */
                return new long[0];
            }

            /*
             * Write out tracked detail, if any, and add its offsets to the
             * list.
             */
            if (!tfs.isEmpty() && !obsoleteBeforeCkpt) {
                if (logUpdate) {
                    final PackedOffsets offsets = putFileSummary(tfs);
                    if (offsets != null) {
                        list.add(offsets.toArray());
                    }
                } else {
                    final long[] offsetList = tfs.getObsoleteOffsets();
                    if (offsetList != null) {
                        list.add(offsetList);
                    }
                }
            }
        } finally {
            /* Allow flushing of TFS when all offsets have been gathered. */
            tfs.setAllowFlush(true);
        }

        beforeWork.run();

        /* Merge all offsets into a single array and pack the result. */
        int size = 0;
        for (final long[] a : list) {
            size += a.length;
        }
        final long[] offsets = new long[size];
        int index = 0;
        for (long[] a : list) {
            System.arraycopy(a, 0, offsets, index, a.length);
            index += a.length;
        }
        assert index == offsets.length;
        return offsets;
    }

    /**
     * Populate the profile for file selection.  This method performs eviction
     * and is not synchronized.  It must be called before recovery is complete
     * so that synchronization is unnecessary.  It should be called before the
     * recovery checkpoint so that the checkpoint can flush dirty metadata.
     *
     * After a file is cleaned, at checkpoint end it is moved to reserved
     * status and these steps are taken to delete its metadata:
     *  1. The file is cleaned and a checkpoint flushes the updated INs.
     *  2. Utilization info in MapLNs referencing the file are updated in
     *     cache and the MapLNs are dirtied. At some point later, the dirty
     *     MapLNs are flushed by a normally scheduled checkpoint. This could
     *     happen after any of the steps below.
     *  3. A record is inserted for the file in the reserved file db.
     *  4. All file summary db records for the file are deleted.
     *
     * When the reserved file is deleted, this is done in these steps.
     *  5. The file itself is deleted. Note that because we do not flush the
     *     log before deleting the file, step 3 and 4 may not be durable.
     *  6. The files's record in the reserved file db is deleted.
     *
     * Data file deletion and file metadata deletion cannot be performed
     * atomically. In fact none of the steps are grouped atomically into
     * transactions, and no-sync logging is used when writing to the file
     * summary db and the reserved file db. A crash can occur at any point, and
     * we must handle that in some way, either in recovery or here (in the
     * populateCache method).
     *
     *  - A crash prior to durable completion of step 3 will be handled
     *    naturally by cleaning the file again, since it is present in the file
     *    summary db but not in the reserved file db.
     *
     *  - If step 3 completes durably but a crash occurs before the dirty
     *    MapLNs are flushed, this is indicated by the presence of a reserved
     *    file record in the recovery interval, and step 2 will be repeated
     *    here during recovery.
     *
     *  - If step 3 completes durably but not step 4 or 5, then step 4 is
     *    completed here.
     *
     *  - If step 3 and 5 complete durably but not step 4 and 6, then steps 4
     *    and 6 are completed here.
     *
     *  - If step 4 and 5 complete durably but not step 6, then step 6 is
     *    completed here.
     *
     * Prior to log version 15, the reserved file db did not exist and reserved
     * files were re-cleaned if they were not deleted before a crash. The old
     * steps were:
     *  A. The file is cleaned and a checkpoint flushes the updated INs.
     *  B. If the file is reserved, this is tracked in memory but nothing is
     *     logged.
     *
     * When deleting a file:
     *  C. The file itself is deleted.
     *  D. All file summary db records for the file are deleted.
     *
     * This is handled now as follows, which is the same as the old approach.
     *
     *  - A crash prior to step C results in re-cleaning the file.
     *  - If step C completes but step D does not, step D is performed here.
     */
    public void populateCache(
        final StartupTracker.Counter counter,
        final RecoveryInfo recoveryInfo)
        throws DatabaseException {

        assert !cachePopulated;

        /* Open the file summary db on first use. */
        if (!openFileSummaryDatabase()) {
            /* Db does not exist and this is a read-only environment. */
            return;
        }

        final Long[] existingFiles = env.getFileManager().getAllFileNumbers();
        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry dataEntry = new DatabaseEntry();
        final FileProtector fileProtector = env.getFileProtector();

        /*
         * Open the reserved file db on first use. This may return false in a
         * read-only old-version env where the file summary db exists but the
         * reserved file db does not.
         */
        if (openReservedFilesDatabase()) {

            /*
             * First read through the reserved file db and add reserved
             * files to the file protector. It is possible that a file does
             * not exist for a record in the db, and again we must clean up
             * as described in method comments.
             */
            final Locker locker =
                BasicLocker.createBasicLocker(env, false /*noWait*/);
            try {
                final ReadOptions options =
                    new ReadOptions().setLockMode(LockMode.READ_UNCOMMITTED);

                try (final Cursor cursor = DbInternal.makeCursor(
                        reservedFilesDb, locker, null,
                        false /*retainNonTxnLocks*/)) {

                    while (cursor.get(
                        keyEntry, dataEntry, Get.NEXT, options) != null) {

                        counter.incNumRead();
                        env.daemonEviction(false /*backgroundIO*/);

                        final Long file =
                            ReservedFileInfo.entryToKey(keyEntry);

                        final ReservedFileInfo info =
                            ReservedFileInfo.entryToObject(dataEntry);

                        if (Arrays.binarySearch(existingFiles, file) >= 0) {

                            counter.incNumProcessed();
                            fileProtector.reserveFile(file, info.lastVLSN);

                        } else {
                            counter.incNumDeleted();

                            if (!VLSN.isNull(info.lastVLSN)) {
                                recoveryInfo.lastMissingFileNumber = file;
                                recoveryInfo.lastMissingFileVLSN =
                                    info.lastVLSN;
                            }

                            if (env.isReadOnly()) {
                                continue;
                            }

                            cursor.delete();
                        }
                    }
                }
            } finally {
                locker.operationEnd();
            }
        }

        /*
         * Next read through the file summary db and populate the profile.
         * As above, it is possible that a file does not exist for a record in
         * the db, and again we must clean up as described in method comments.
         */
        Locker locker = null;
        CursorImpl cursor = null;
        try {
            locker = BasicLocker.createBasicLocker(env, false /*noWait*/);

            cursor = new CursorImpl(
                fileSummaryDb, locker, false /*retainNonTxnLocks*/,
                false /*isSecondaryCursor*/);

            /* Perform eviction in unsynchronized methods. */
            cursor.setAllowEviction(true);

            if (cursor.positionFirstOrLast(true)) {

                /* Retrieve the first record. */
                OperationResult result = cursor.lockAndGetCurrent(
                    keyEntry, dataEntry, LockType.NONE,
                    false /*excludeTombstones*/, false /*dirtyReadAll*/,
                    true /*isLatched*/, true /*unlatch*/);

                if (result == null) {
                    /* The record we're pointing at may be deleted. */
                    result = cursor.getNext(
                        keyEntry, dataEntry, LockType.NONE);
                }

                while (result != null) {
                    counter.incNumRead();

                    /*
                     * Perform eviction once per operation.  Pass false for
                     * backgroundIO because this is done during recovery and
                     * there is no reason to sleep.
                     */
                    env.daemonEviction(false /*backgroundIO*/);

                    final FileSummaryLN ln = (FileSummaryLN)
                        cursor.lockAndGetCurrentLN(LockType.NONE);

                    if (ln == null) {
                        /* Advance past a cleaned record. */
                        result = cursor.getNext(
                            keyEntry, dataEntry, LockType.NONE);
                        continue;
                    }

                    final byte[] keyBytes = keyEntry.getData();
                    final long fileNum = FileSummaryLN.getFileNumber(keyBytes);
                    final Long fileNumLong = fileNum;

                    if (!fileProtector.isReservedFile(fileNumLong) &&
                        Arrays.binarySearch(existingFiles, fileNumLong) >= 0) {

                        counter.incNumProcessed();

                        /* File is active, cache the FileSummaryLN. */
                        final FileSummary summary = ln.getBaseSummary();
                        fileSummaryMap.put(fileNumLong, summary);

                        /* Always evict after using a file summary LN. */
                        cursor.evictLN();
                    } else {

                        /*
                         * File does not exist or is a reserved file. Remove
                         * the summary from the map and delete all
                         * FileSummaryLN records. If the file has a reserved
                         * file record (even if it was deleted above) then we
                         * can rely on the reserved file record mechanism to
                         * update MapLNs (per-db metadata); otherwise we must
                         * update the MapLNs here.
                         */
                        counter.incNumDeleted();

                        fileSummaryMap.remove(fileNumLong);

                        if (!env.isReadOnly()) {
                            deleteFileSummary(fileNumLong);
                        }

                        /*
                         * Do not evict after deleting since the compressor
                         * would have to fetch it again.
                         */
                    }

                    /*
                     * Go on to the next entry. Skip over other records for
                     * this file by adding one to the file number and doing
                     * a range search.
                     */
                    if (!getFirstFSLN
                        (cursor,
                         fileNum + 1,
                         keyEntry, dataEntry,
                         LockType.NONE)) {
                        result = null;
                    }
                }
            }
        } finally {
            if (cursor != null) {
                /* positionFirstOrLast may leave BIN latched. */
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }

        /*
         * Reactivate existing files that were not in the reserved file DB or
         * the file summary DB, by adding them to the file summary DB and the
         * utilization profile map. Files may be in this condition for two
         * reasons:
         *
         * 1. UtilizationProfile.reactivateReservedFile was called earlier and
         * the file was durably removed from the reserved files DB, but a
         * crash occurred before the FileSummaryLN insertion was made durable.
         *
         * 2. A previously cleaned and deleted file was manually restored from
         * backup, e.g., to work around a bug or repair a corruption.
         *
         * All existing files will be placed in the FileProtector's active
         * file set when it is initialized, so there is no need to call
         * FileProtector.reactivateReservedFile here.
         *
         * Files in the recovery interval (>= firstActiveFile) are not
         * considered because they may or may not have file summary DB entries
         * and they could not have not been previously cleaned and deleted.
         */
        final long firstActiveFile =
            (recoveryInfo.checkpointStartLsn != DbLsn.NULL_LSN) ?
            DbLsn.getFileNumber(recoveryInfo.checkpointStartLsn) : 0;

        for (Long file : existingFiles) {
            if (file < firstActiveFile &&
                !fileProtector.isReservedFile(file) &&
                !fileSummaryMap.containsKey(file)) {

                addReactivatedFileSummary(file);

                LoggerUtils.info(
                    logger, env,
                    "Reactivated file during recovery: 0x" +
                        Long.toHexString(file));
            }
        }

        cachePopulated = true;
    }

    /**
     * Returns whether an LN exists in the reserved file DB for the given file.
     * Intended for testing.
     */
    public boolean hasReservedFileLN(long fileNum) {

        final Locker locker =
            BasicLocker.createBasicLocker(env, false /*noWait*/);
        try {
            final ReadOptions options =
                new ReadOptions().setLockMode(LockMode.READ_UNCOMMITTED);

            try (final Cursor cursor = DbInternal.makeCursor(
                reservedFilesDb, locker, null,
                false /*retainNonTxnLocks*/)) {

                final DatabaseEntry keyEntry = new DatabaseEntry();
                ReservedFileInfo.keyToEntry(fileNum, keyEntry);

                return cursor.get(
                    keyEntry, null, Get.SEARCH, options) != null;
            }
        } finally {
            locker.operationEnd();
        }
    }

    /**
     * Returns whether an LN exists in the file summary DB for the given file.
     * Intended for testing.
     */
    public boolean hasFileSummaryLN(long fileNum) {

        Locker locker = null;
        CursorImpl cursor = null;
        try {
            locker = BasicLocker.createBasicLocker(env, false /*noWait*/);
            cursor = new CursorImpl(fileSummaryDb, locker);

            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry dataEntry = new DatabaseEntry();

            /* Do not return data to avoid a fetch of the existing LN. */
            dataEntry.setPartial(0, 0, true);

            /* Search by file number. */
            return getFirstFSLN(
                cursor, fileNum, keyEntry, dataEntry, LockType.NONE) &&
                fileNum == FileSummaryLN.getFileNumber(keyEntry.getData());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }

    /**
     * Positions at the most recent LN for the given file number.
     */
    private boolean getFirstFSLN(CursorImpl cursor,
                                 long fileNum,
                                 DatabaseEntry keyEntry,
                                 DatabaseEntry dataEntry,
                                 LockType lockType)
        throws DatabaseException {

        byte[] keyBytes = FileSummaryLN.makePartialKey(fileNum);
        keyEntry.setData(keyBytes);

        cursor.reset();

        try {
            int result = cursor.searchRange(keyEntry, null /*comparator*/);

            if ((result & CursorImpl.FOUND) == 0) {
                return false;
            }

            boolean exactKeyMatch = ((result & CursorImpl.EXACT_KEY) != 0);

            if (exactKeyMatch &&
                cursor.lockAndGetCurrent(
                    keyEntry, dataEntry, lockType,
                    false /*excludeTombstones*/, false /*dirtyReadAll*/,
                    true /*isLatched*/, false /*unlatch*/) != null) {
                return true;
            }
        } finally {
            cursor.releaseBIN();
        }

        /* Always evict after using a file summary LN. */
        cursor.evictLN();

        OperationResult result = cursor.getNext(keyEntry, dataEntry, lockType);
        return result != null;
    }

    /**
     * If the reserved files db is already open, return, otherwise attempt to
     * open it.  If the environment is read-only and the database doesn't
     * exist, return false.  If the environment is read-write the database will
     * be created if it doesn't exist.
     */
    private boolean openReservedFilesDatabase()
        throws DatabaseException {

        if (reservedFilesDb != null) {
            return true;
        }

        reservedFilesDb =
            env.getDbTree().openNonRepInternalDB(DbType.RESERVED_FILES);

        return (reservedFilesDb != null);
    }

    /**
     * If the file summary db is already open, return, otherwise attempt to
     * open it.  If the environment is read-only and the database doesn't
     * exist, return false.  If the environment is read-write the database will
     * be created if it doesn't exist.
     */
    private boolean openFileSummaryDatabase()
        throws DatabaseException {

        if (fileSummaryDb != null) {
            return true;
        }

        fileSummaryDb =
            env.getDbTree().openNonRepInternalDB(DbType.UTILIZATION);

        return (fileSummaryDb != null);
    }

    /**
     * For unit testing.
     */
    DatabaseImpl getFileSummaryDb() {
        return fileSummaryDb;
    }

    /**
     * Insert the given LN with the given key values.  This method is
     * synchronized and may not perform eviction.
     */
    synchronized boolean insertFileSummary(
        FileSummaryLN ln,
        long fileNum,
        int sequence)
        throws DatabaseException {

        byte[] keyBytes = FileSummaryLN.makeFullKey(fileNum, sequence);

        Locker locker = null;
        CursorImpl cursor = null;
        try {
            locker = BasicLocker.createBasicLocker(env, false /*noWait*/);
            cursor = new CursorImpl(fileSummaryDb, locker);

            /* Insert the LN. */
            boolean inserted = cursor.insertRecord(
                keyBytes, ln, false /*blindInsertion*/,
                ReplicationContext.NO_REPLICATE);

            if (!inserted) {
                LoggerUtils.traceAndLog
                    (logger, env, Level.SEVERE,
                     "Cleaner duplicate key sequence file=0x" +
                     Long.toHexString(fileNum) + " sequence=0x" +
                     Long.toHexString(sequence));
                return false;
            }

            /* Always evict after using a file summary LN. */
            cursor.evictLN();
            return true;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }

    /**
     * Checks that all FSLN offsets are indeed obsolete.  Assumes that the
     * system is quiesent (does not lock LNs).  This method is not synchronized
     * (because it doesn't access fileSummaryMap) and eviction is allowed.
     *
     * @return true if no verification failures.
     */
    boolean verifyFileSummaryDatabase()
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        openFileSummaryDatabase();
        Locker locker = null;
        CursorImpl cursor = null;
        boolean ok = true;

        try {
            locker = BasicLocker.createBasicLocker(env, false /*noWait*/);
            cursor = new CursorImpl(fileSummaryDb, locker);
            cursor.setAllowEviction(true);

            if (cursor.positionFirstOrLast(true)) {

                OperationResult result = cursor.lockAndGetCurrent(
                    key, data, LockType.NONE,
                    false /*excludeTombstones*/, false /*dirtyReadAll*/,
                    true /*isLatched*/, true /*unlatch*/);

                /* Iterate over all file summary lns. */
                while (result != null) {

                    /* Perform eviction once per operation. */
                    env.daemonEviction(true /*backgroundIO*/);

                    FileSummaryLN ln = (FileSummaryLN)
                        cursor.lockAndGetCurrentLN(LockType.NONE);

                    if (ln != null) {
                        long fileNumVal =
                            FileSummaryLN.getFileNumber(key.getData());
                        PackedOffsets offsets = ln.getObsoleteOffsets();

                        /*
                         * Check every offset in the fsln to make sure it's
                         * truly obsolete.
                         */
                        if (offsets != null) {
                            long[] vals = offsets.toArray();
                            for (long val : vals) {
                                long lsn = DbLsn.makeLsn(fileNumVal, val);
                                if (!verifyLsnIsObsolete(lsn)) {
                                    ok = false;
                                }
                            }
                        }

                        cursor.evictLN();
                    }

                    result = cursor.getNext(key, data, LockType.NONE);
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }

        return ok;
    }

    /*
     * Return true if the LN at this lsn is obsolete.
     */
    private boolean verifyLsnIsObsolete(long lsn)
        throws DatabaseException {

        /* Read the whole entry out of the log. */
        Object o = env.getLogManager().getLogEntryHandleNotFound(lsn, -1);
        if (!(o instanceof LNLogEntry)) {
            return true;
        }
        LNLogEntry<?> entry = (LNLogEntry<?>) o;

        /* Find the owning database. */
        DatabaseId dbId = entry.getDbId();
        DatabaseImpl db = env.getDbTree().getDb(dbId);

        /*
         * Search down to the bottom most level for the parent of this LN.
         */
        BIN bin = null;
        try {
            /*
             * The whole database is gone, so this LN is obsolete. No need
             * to worry about delete cleanup; this is just verification and
             * no cleaning is done.
             */
            if (db == null || db.isDeleting()) {
                return true;
            }

            if (entry.isImmediatelyObsolete(db)) {
                return true;
            }

            entry.postFetchInit(db);

            Tree tree = db.getTree();
            TreeLocation location = new TreeLocation();
            boolean parentFound = tree.getParentBINForChildLN(
                location, entry.getKey(), false /*splitsAllowed*/,
                false /*blindDeltaOps*/, CacheMode.UNCHANGED);

            bin = location.bin;
            int index = location.index;

            /* Is bin latched ? */
            if (!parentFound) {
                return true;
            }

            /*
             * Now we're at the BIN parent for this LN.  If knownDeleted, LN is
             * deleted and can be purged.
             */
            if (bin.isEntryKnownDeleted(index)) {
                return true;
            }

            if (bin.getLsn(index) != lsn) {
                return true;
            }

            /* Oh no -- this lsn is in the tree. */
            /* should print, or trace? */
            System.err.println("lsn " + DbLsn.getNoFormatString(lsn)+
                               " was found in tree.");
            return false;
        } finally {
            env.getDbTree().releaseDb(db);
            if (bin != null) {
                bin.releaseLatch();
            }
        }
    }

    /* For unit testing only. */
    public static void setIncompleteReactivateHook(TestHook<EnvironmentImpl> hook) {
        incompleteReactivateHook = hook;
    }
}
