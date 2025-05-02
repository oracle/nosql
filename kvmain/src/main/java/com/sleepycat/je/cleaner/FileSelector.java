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

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.Pair;
import com.sleepycat.je.utilint.TracerFormatter;

/**
 * Keeps track of the status of files for which cleaning is in progress.
 */
public class FileSelector {

    /**
     * Each file for which cleaning is in progress has one of the following
     * status values.  Files numbers migrate from one status to another, in
     * the order declared below.
     */
    enum FileStatus {

        /**
         * A file's status is initially TO_BE_CLEANED when it is selected as
         * part of a batch of files that, when deleted, will bring total
         * utilization down to the minimum configured value.  All files with
         * this status will be cleaned in lowest-cost-to-clean order.  For two
         * files of equal cost to clean, the lower numbered (oldest) files is
         * selected; this is why the fileInfoMap is sorted by key (file
         * number).
         */
        TO_BE_CLEANED,

        /**
         * When a TO_BE_CLEANED file is selected for processing by
         * FileProcessor, it is moved to the BEING_CLEANED status.  This
         * distinction is used to prevent a file from being processed by more
         * than one thread.
         */
        BEING_CLEANED,

        /**
         * A file is moved to the CLEANED status when all its log entries have
         * been read and processed.  However, entries that could not be locked
         * will be in the file's pending LN set, and the DBs that were pending
         * deletion will be in the file's pending DB set.
         */
        CLEANED,
    }

    /**
     * Information about a cleaned file with pending entries.
     *
     * A separate object and map are used (rather than adding the pending info
     * to FileInfo) to make it very fast to iterate over all pending files,
     * since this is done often.
     */
    private static class PendingInfo {

        long cleaningTime = TimeSupplier.currentTimeMillis();

        /**
         * Pending LN info, keyed by log LSN.  These are LNs that could not be
         * locked, either during processing or during migration.
         */
        Map<Long, LNInfo> pendingLNs;

        /**
         * For processed entries with DBs that are pending deletion, we
         * consider them to be obsolete but we store their DatabaseIds in a
         * set. Until the DB deletion is complete, we can't delete the log
         * files containing those entries.
         */
        Set<DatabaseId> pendingDBs;

        @Override
        public String toString() {
            return "pendingLN = " + pendingLNs +
                " pendingDBs = " + pendingDBs;
        }

        /**
         * Get a warning when a file cannot be deleted due to pending entries.
         */
        String getMessage(Long file,
                          EnvironmentImpl env,
                          DateFormat dateFormat) {

            StringBuilder s = new StringBuilder();
            s.append("File 0x").append(Long.toHexString(file));
            s.append(" cannot be deleted after checkpoint due to");
            s.append(" pending entries. File cleaned at ");
            s.append(dateFormat.format(new Date(cleaningTime)));
            s.append(".");

            if (pendingLNs != null) {

                s.append(" Write lock is held on ");
                s.append(pendingLNs.size()).append(" records:");
                int count = 0;

                for (Map.Entry<Long, LNInfo> entry : pendingLNs.entrySet()) {

                    count += 1;
                    if (count > 3) {
                        s.append(" (only first 3 records shown)");
                        break;
                    }

                    long lsn = entry.getKey();

                    Locker locker = env.getTxnManager()
                        .getLockManager()
                        .getWriteOwnerLocker(lsn);

                    s.append(" [lsn=");
                    s.append(DbLsn.getNoFormatString(lsn));
                    s.append(" db=");
                    s.append(getDbName(entry.getValue().getDbId(), env));
                    if (locker != null) {
                        s.append(" locker=").append(locker.toString());
                    }
                    s.append("]");
                }
            }

            if (pendingDBs != null) {

                s.append(" Database remove/truncate in progress on ");
                s.append(pendingDBs.size()).append(" DBs:");
                int count = 0;

                for (DatabaseId dbId : pendingDBs) {

                    count += 1;
                    if (count > 3) {
                        s.append(" (only first 3 DBs shown)");
                        break;
                    }

                    s.append(" [db=");
                    s.append(getDbName(dbId, env));
                    s.append("]");
                }
            }

            return s.toString();
        }

        private static String getDbName(DatabaseId dbId,
                                        EnvironmentImpl env) {
            DbTree dbTree = env.getDbTree();
            DatabaseImpl db = dbTree.getDb(dbId);
            try {
                return (db != null) ? db.getName() : ("id=" + dbId);
            } finally {
                dbTree.releaseDb(db);
            }
        }
    }

    /**
     * Map of PendingInfo for all CLEANED files with pending entries.
     */
    private final Map<Long, PendingInfo> pendingInfoMap;

    /**
     * Information about a file being cleaned.
     */
    static class FileInfo {
        private FileStatus status;
        private int requiredUtil = -1;

        /* Per-file metadata. */
        long firstVlsn = NULL_VLSN;
        long lastVlsn = NULL_VLSN;

        @Override
        public String toString() {
            return "status = " + status +
                   " firstVlsn = " + firstVlsn +
                   " lastVlsn = " + lastVlsn;
        }
    }

    /**
     * Information about files being cleaned, keyed by file number.  The map is
     * sorted by file number to clean older files before newer files.
     */
    private final SortedMap<Long, FileInfo> fileInfoMap;

    /**
     * Must be used only while synchronized on FileSelector.
     */
    private final DateFormat dateFormat = TracerFormatter.makeDateFormat();

    FileSelector() {
        fileInfoMap = new TreeMap<>();
        pendingInfoMap = new HashMap<>();
    }

    /**
     * Returns the best file that qualifies for cleaning, or null if no file
     * qualifies.
     *
     * @param forceCleaning is true to always select a file, even if its
     * utilization is above the minimum utilization threshold.
     *
     * @return {file number, required utilization for 2-pass cleaning},
     * or null if no file qualifies for cleaning.
     */
    synchronized Pair<Long, Integer> selectFileForCleaning(
        UtilizationCalculator calculator,
        SortedMap<Long, FileSummary> fileSummaryMap,
        boolean forceCleaning) {

        final Set<Long> toBeCleaned = getToBeCleanedFiles();

        if (!toBeCleaned.isEmpty()) {
            final Long fileNum = toBeCleaned.iterator().next();
            final FileInfo info = setStatus(fileNum, FileStatus.BEING_CLEANED);
            return new Pair<>(fileNum, info.requiredUtil);
        }

        final Pair<Long, Integer> result = calculator.getBestFile(
            fileSummaryMap, forceCleaning);

        if (result == null) {
            return null;
        }

        final Long fileNum = result.first();
        final int requiredUtil = result.second();

        assert !fileInfoMap.containsKey(fileNum);

        final FileInfo info = setStatus(fileNum, FileStatus.BEING_CLEANED);
        info.requiredUtil = requiredUtil;

        return result;
    }

    /**
     * Returns the number of files having the given status.
     */
    private synchronized int getNumberOfFiles(FileStatus status) {
        int count = 0;
        for (FileInfo info : fileInfoMap.values()) {
            if (info.status == status) {
                count += 1;
            }
        }
        return count;
    }

    /**
     * Returns a sorted set of files having the given status.
     */
    private synchronized NavigableSet<Long> getFiles(FileStatus status) {
        final NavigableSet<Long> set = new TreeSet<>();
        for (Map.Entry<Long, FileInfo> entry : fileInfoMap.entrySet()) {
            if (entry.getValue().status == status) {
                set.add(entry.getKey());
            }
        }
        return set;
    }

    /**
     * Moves a file to a given status, adding the file to the fileInfoMap if
     * necessary.
     *
     * This method must be called while synchronized.
     */
    private FileInfo setStatus(Long fileNum, FileStatus newStatus) {
        FileInfo info = fileInfoMap.get(fileNum);
        if (info == null) {
            info = new FileInfo();
            fileInfoMap.put(fileNum, info);
        }
        info.status = newStatus;
        return info;
    }

    /**
     * Asserts that a file has a given status.  Should only be called under an
     * assertion to avoid the overhead of the method call and synchronization.
     * Always returns true to enable calling it under an assertion.
     *
     * This method must be called while synchronized.
     */
    private boolean checkStatus(Long fileNum, FileStatus expectStatus) {
        final FileInfo info = fileInfoMap.get(fileNum);
        assert info != null : "Expected " + expectStatus + " but was missing";
        assert info.status == expectStatus :
            "Expected " + expectStatus + " but was " + info.status;
        return true;
    }

    /**
     * Returns whether the file is in any stage of the cleaning process.
     */
    private synchronized boolean isFileCleaningInProgress(Long fileNum) {
        return fileInfoMap.containsKey(fileNum);
    }

    synchronized int getRequiredUtil(Long fileNum) {
        FileInfo info = fileInfoMap.get(fileNum);
        return (info != null) ? info.requiredUtil : -1;
    }

    /**
     * Removes all references to a file.
     */
    synchronized FileInfo removeFile(Long fileNum) {
        FileInfo info = fileInfoMap.get(fileNum);
        if (info == null) {
            return null;
        }
        fileInfoMap.remove(fileNum);
        return info;
    }

    /**
     * When file cleaning is aborted, move the file back from BEING_CLEANED to
     * TO_BE_CLEANED.
     */
    synchronized void putBackFileForCleaning(Long fileNum) {
        assert checkStatus(fileNum, FileStatus.BEING_CLEANED);
        setStatus(fileNum, FileStatus.TO_BE_CLEANED);
    }

    /**
     * For unit testing.
     */
    public synchronized void injectFileForCleaning(Long fileNum) {
        if (!isFileCleaningInProgress(fileNum)) {
            final FileInfo info = setStatus(fileNum, FileStatus.TO_BE_CLEANED);
            info.requiredUtil = -1;
        }
    }

    /**
     * When cleaning is complete, move the file from the BEING_CLEANED to
     * CLEANED.
     */
    synchronized void addCleanedFile(Long fileNum,
                                     long firstVlsn,
                                     long lastVlsn) {
        assert checkStatus(fileNum, FileStatus.BEING_CLEANED);
        FileInfo info = setStatus(fileNum, FileStatus.CLEANED);
        info.firstVlsn = firstVlsn;
        info.lastVlsn = lastVlsn;
    }

    /**
     * Returns a read-only copy of TO_BE_CLEANED files that can be accessed
     * without synchronization.
     */
    synchronized Set<Long> getToBeCleanedFiles() {
        return getFiles(FileStatus.TO_BE_CLEANED);
    }

    /**
     * Returns a copy of the CLEANED files at the time a checkpoint starts.
     * CLEANED files with pending LNs or DBs are not returned, since the
     * pending entries must be resolved before the file can be deleted.
     *
     * FUTURE: Pending LNs must gate the checkpoint that is performed prior to
     * file deletion, because migration of the LN will dirty the BIN and
     * the BIN must be flushed to remove the reference to the file. However,
     * pending DBs need only gate file deletion -- not the checkpoint. A future
     * optimization is to allow the file to become reserved when only pending
     * DBs (no pending LNs) are present. Perhaps it should be considered to be
     * "protected" by the pending DBs, but there are design issues to work out.
     */
    synchronized CheckpointStartCleanerState getFilesAtCheckpointStart(
        EnvironmentImpl env,
        Logger logger) {

        Set<Long> set = null;
        for (Map.Entry<Long, FileInfo> entry : fileInfoMap.entrySet()) {

            if (entry.getValue().status != FileStatus.CLEANED) {
                continue;
            }

            Long file = entry.getKey();
            PendingInfo pInfo = pendingInfoMap.get(file);

            if (pInfo != null) {
                LoggerUtils.logMsg(
                    logger, env, Level.INFO,
                    pInfo.getMessage(file, env, dateFormat));
                continue;
            }

            if (set == null) {
                set = new TreeSet<>();
            }

            set.add(file);
        }

        return new CheckpointStartCleanerState(set);
    }

    /**
     * Returns whether any files are cleaned, meaning that a checkpoint is
     * needed before they can be deleted.
     */
    public synchronized boolean isCheckpointNeeded() {
        return getNumberOfFiles(FileStatus.CLEANED) > 0;
    }

    /**
     * When a checkpoint is complete, move the previously CLEANED files to the
     * reserved status. Reserved files are removed from the FileSelector and
     * their reserved status is maintained in FileProtector.
     *
     * @return map of {fileNum, FileInfo} for the files whose status was
     * changed to reserved.
     */
    synchronized Map<Long, FileInfo> updateFilesAtCheckpointEnd(
        final EnvironmentImpl env,
        final CheckpointStartCleanerState checkpointInfo) {

        if (checkpointInfo.isEmpty()) {
            return Collections.emptyMap();
        }

        final FileProtector fileProtector = env.getFileProtector();
        final Map<Long, FileInfo> reservedFiles = new HashMap<>();
        final Set<Long> safeToDeleteFiles = checkpointInfo.getCleanedFiles();

        for (Long file : safeToDeleteFiles) {
            final FileInfo info = removeFile(file);
            fileProtector.reserveFile(file, info.lastVlsn);
            reservedFiles.put(file, info);
        }

        env.getUtilizationProfile().removeFileSummaries(safeToDeleteFiles);

        return reservedFiles;
    }

    /**
     * Adds the given LN info to the pending LN set.
     */
    synchronized void addPendingLN(final long logLsn, final LNInfo lnInfo) {
        
        Long file = DbLsn.getFileNumber(logLsn);
        assert checkStatus(file, FileStatus.BEING_CLEANED);

        PendingInfo pInfo = pendingInfoMap.get(file);

        if (pInfo == null) {
            pInfo = new PendingInfo();
            pendingInfoMap.put(file, pInfo);
        }

        if (pInfo.pendingLNs == null) {
            pInfo.pendingLNs = new HashMap<>();
        }

        pInfo.pendingLNs.put(logLsn, lnInfo);
    }

    /**
     * Returns a map of LNInfo for LNs that could not be migrated in a prior
     * cleaning attempt, or null if no LNs are pending.
     */
    synchronized Map<Long, LNInfo> getPendingLNs() {

        Map<Long, LNInfo> map = null;

        for (PendingInfo info : pendingInfoMap.values()) {
            if (info.pendingLNs == null) {
                continue;
            }
            if (map == null) {
                map = new HashMap<>();
            }
            map.putAll(info.pendingLNs);
         }

        return map;
    }

    /**
     * Removes the LN for the given LSN from the pending LN set.
     */
    synchronized void removePendingLN(long logLsn) {

        final Long file = DbLsn.getFileNumber(logLsn);
        final PendingInfo info = pendingInfoMap.get(file);

        if (info == null || info.pendingLNs == null) {
            return;
        }

        info.pendingLNs.remove(logLsn);

        if (info.pendingLNs.size() == 0) {
            info.pendingLNs = null;
            if (info.pendingDBs == null) {
                pendingInfoMap.remove(file);
            }
        }
    }

    /**
     * Returns number of LNs and DBs pending.
     */
    synchronized Pair<Integer, Integer> getPendingQueueSizes() {

        int lns = 0;
        int dbs = 0;

        for (PendingInfo info : pendingInfoMap.values()) {
            if (info.pendingLNs != null) {
                lns += info.pendingLNs.size();
            }
            if (info.pendingDBs != null) {
                dbs += info.pendingDBs.size();
            }
        }

        return new Pair<>(lns, dbs);
    }

    /**
     * Adds the given DatabaseId to the pending DB set.
     */
    synchronized boolean addPendingDB(Long file, DatabaseId dbId) {

        assert dbId != null;
        PendingInfo info = pendingInfoMap.get(file);

        if (info == null) {
            info = new PendingInfo();
            pendingInfoMap.put(file, info);
        }

        if (info.pendingDBs == null) {
            info.pendingDBs = new HashSet<>();
        }

        return info.pendingDBs.add(dbId);
    }

    /**
     * Returns an array of DatabaseIds for DBs that were pending deletion in a
     * prior cleaning attempt, or null if no DBs are pending.
     */
    synchronized List<DatabaseId> getPendingDBs() {
        List<DatabaseId> list = null;

        for (final PendingInfo info : pendingInfoMap.values()) {
            if (info.pendingDBs == null) {
                continue;
            }
            if (list == null) {
                list = new ArrayList<>();
            }
            list.addAll(info.pendingDBs);
         }

        return list;
    }

    /**
     * Removes the DatabaseId from the pending DB set.
     */
    synchronized void removePendingDB(DatabaseId dbId) {

        Iterator<PendingInfo> iter = pendingInfoMap.values().iterator();
        while (iter.hasNext()) {
            PendingInfo info = iter.next();
            if (info.pendingDBs == null) {
                continue;
            }
            info.pendingDBs.remove(dbId);

            if (info.pendingDBs.size() == 0) {
                info.pendingDBs = null;
                if (info.pendingLNs == null) {
                    iter.remove();
                }
            }
        }
    }

    /**
     * Returns a copy of the in-progress files, or an empty set if there are
     * none.
     */
    public synchronized NavigableSet<Long> getInProgressFiles() {
        return new TreeSet<>(fileInfoMap.keySet());
    }

    /**
     * Holds copy of all checkpoint-dependent cleaner state.
     */
    public static class CheckpointStartCleanerState {

        /* A snapshot of the cleaned files at the checkpoint start. */
        private Set<Long> cleanedFiles;

        private CheckpointStartCleanerState(Set<Long> cleanedFiles) {
            this.cleanedFiles = cleanedFiles;
        }

        public boolean isEmpty() {
            return cleanedFiles == null;
        }

        Set<Long> getCleanedFiles() {
            return cleanedFiles;
        }
    }

    @Override
    public synchronized String toString() {
        return "files = " + fileInfoMap;
    }
}
