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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.ExtinctionScanner;
import com.sleepycat.je.cleaner.RecoveryUtilizationTracker;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.INContainingEntry;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.recovery.VLSNRecoveryProxy;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.utilint.DbLsn;

/**
 * INFileReader supports recovery by scanning log files during the IN rebuild
 * pass. It looks for internal nodes (all types), segregated by whether they
 * belong to the main tree or the duplicate trees.
 *
 * <p>This file reader can also be run in tracking mode to keep track of the
 * maximum node ID, database ID and txn ID seen so those sequences can be
 * updated properly at recovery.  In this mode it also performs utilization
 * counting.  It is only run once in tracking mode per recovery, in the first
 * phase of recovery.</p>
 */
public class INFileReader extends FileReader {

    /* Information about the last entry seen. */
    private LogEntryType fromLogType;
    private boolean isProvisional;

    /*
     * targetEntryMap maps DbLogEntryTypes to log entries. We use this
     * collection to find the right LogEntry instance to read in the current
     * entry.
     */
    private Map<LogEntryType, LogEntry> targetEntryMap;
    private LogEntry targetLogEntry;

    /* Set of non-target log entry types for ID tracking. */
    private Set<LogEntryType> idTrackingSet;
    /* Cache of non-target log entries for ID tracking. */
    private Map<LogEntryType, LogEntry> idTrackingMap;

    private boolean trackIds;
    private long minReplicatedNodeId;
    private long maxNodeId;
    private long minReplicatedDbId;
    private long maxDbId;
    private long minReplicatedTxnId;
    private long maxTxnId;
    private long minReplicatedExtinctionId;
    private long maxExtinctionId;
    private long ckptEnd;

    /* Used for utilization tracking. */
    private long partialCkptStart;
    private RecoveryUtilizationTracker tracker;

    /* Used for replication. */
    private VLSNRecoveryProxy vlsnProxy;

    /**
     * Create this reader to start at a given LSN.
     */
    public INFileReader(EnvironmentImpl env,
                        int readBufferSize,
                        long startLsn,
                        long finishLsn,
                        boolean trackIds,
                        long partialCkptStart,
                        long ckptEnd,
                        RecoveryUtilizationTracker tracker) {

        /*
         * IN/LN file readers do not verify checksums since they are run only
         * during recovery. See RecoveryManager.VerifyCheckpointInterval.
         */
        super(env, readBufferSize, true, startLsn, null,
              DbLsn.NULL_LSN, finishLsn, false /*doChecksumOnRead*/);

        this.trackIds = trackIds;
        this.ckptEnd = ckptEnd;
        targetEntryMap = new HashMap<LogEntryType, LogEntry>();

        if (trackIds) {
            maxNodeId = 0;
            maxDbId = 0;
            maxTxnId = 0;
            minReplicatedNodeId = 0;
            minReplicatedDbId = DbTree.NEG_DB_ID_START;
            minReplicatedTxnId = 0;
            minReplicatedExtinctionId = 0;
            maxExtinctionId = 0;
            this.tracker = tracker;
            this.partialCkptStart = partialCkptStart;

            idTrackingSet = new HashSet<LogEntryType>();
            idTrackingMap = new HashMap<LogEntryType, LogEntry>();

            /*
             * Need all nodes for tracking:
             * - Need all INs for node ID tracking.
             * - Need all LNs for obsolete tracking.
             * - Need txnal LNs for txn ID tracking.
             * - Need FileSummaryLN for obsolete tracking.
             * - Need MapLN for obsolete and DB ID tracking.
             * - Need BIN-delta for obsolete tracking.
             */
            for (LogEntryType entryType : LogEntryType.getAllTypes()) {
                if (entryType.isNodeType()) {
                    idTrackingSet.add(entryType);
                }
            }
            idTrackingSet.add(LogEntryType.LOG_BIN_DELTA);

            /* For tracking VLSNs. */
            vlsnProxy = envImpl.getVLSNProxy();
            idTrackingSet.add(LogEntryType.LOG_ROLLBACK_START);
        }
    }

    /**
     * Configure this reader to target this kind of entry.
     */
    public void addTargetType(LogEntryType entryType)
        throws DatabaseException {

        targetEntryMap.put(entryType, entryType.getNewLogEntry());
    }

    /*
     * Utilization Tracking
     * --------------------
     * This class counts all new log entries and obsolete INs.  Obsolete LNs,
     * on the other hand, are counted by RecoveryManager undo/redo.
     *
     * Utilization counting is done in the first recovery pass where IDs are
     * tracked (trackIds=true).  Processing is split between isTargetEntry
     * and processEntry as follows.
     *
     * isTargetEntry counts only new non-node entries; this can be done very
     * efficiently using only the LSN and entry type, without reading and
     * unmarshalling the entry.
     *
     * processEntry counts new node entries and obsolete INs.
     *
     * processEntry also resets (sets all counters to zero and clears obsolete
     * offsets) the tracked summary for a file or database when a FileSummaryLN
     * or MapLN is encountered.  This clears the totals that have accumulated
     * during this recovery pass for entries prior to that point.  We only want
     * to count utilization for entries after that point.
     *
     * In addition, when processEntry encounters a FileSummaryLN or MapLN, its
     * LSN is recorded in the tracker.  This information is used during IN and
     * LN utilization counting.  For each file, knowing the LSN of the last
     * logged FileSummaryLN for that file allows the undo/redo code to know
     * whether to perform obsolete countng.  If the LSN of the FileSummaryLN is
     * less than (to the left of) the LN's LSN, obsolete counting should be
     * performed.  If it is greater, obsolete counting is already included in
     * the logged FileSummaryLN and should not be repeated to prevent double
     * counting.  The same thing is true of counting per-database utilization
     * relative to the LSN of the last logged MapLN.
     */

    /**
     * If we're tracking node, database and txn IDs, we want to see all node
     * log entries. If not, we only want to see IN entries.
     */
    @Override
    protected boolean isTargetEntry()
        throws DatabaseException {

        targetLogEntry = null;
        isProvisional = currentEntryHeader.getProvisional().isProvisional
            (getLastLsn(), ckptEnd);

        /* Get the log entry type instance we need to read the entry. */
        fromLogType = LogEntryType.findType(currentEntryHeader.getType());
        LogEntry possibleTarget = targetEntryMap.get(fromLogType);

        /* Always select a non-provisional target entry. */
        if (!isProvisional) {
            targetLogEntry = possibleTarget;
        }

        /* If we're not tracking IDs, select only the targeted entry. */
        if (!trackIds) {
            return (targetLogEntry != null);
        }

        /*
         * Count all non-node non-delta entries except for the file header as
         * new.  UtilizationTracker does not count the file header.  Node/delta
         * entries will be counted in processEntry.  Null is passed for the
         * database ID; it is only needed for node entries.
         */
        if (!fromLogType.isNodeType() &&
            !fromLogType.equals(LogEntryType.LOG_BIN_DELTA) &&
            !LogEntryType.LOG_FILE_HEADER.equals(fromLogType)) {
            tracker.countNewLogEntry(getLastLsn(),
                                     fromLogType,
                                     currentEntryHeader.getSize() +
                                     currentEntryHeader.getItemSize());
        }

        /* Track VLSNs in the log entry header of all replicated entries. */
        if (currentEntryHeader.getReplicated()) {
            vlsnProxy.trackMapping(getLastLsn(),
                                   currentEntryHeader,
                                   null /*targetLogEntry*/);
        }

        /* Return true if this logrec should be passed on to processEntry. */
        return (targetLogEntry != null ||
                idTrackingSet.contains(fromLogType));
    }

    /**
     * This reader returns non-provisional INs and IN delete entries.
     * In tracking mode, it may also scan log entries that aren't returned:
     *  -to set the sequences for txn, node, and database ID.
     *  -to update utilization and obsolete offset information.
     *  -for VLSN mappings for recovery
     */
    @Override
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        boolean useEntry = false;

        /* Read targeted entry. */
        if (targetLogEntry != null) {
            targetLogEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);
            useEntry = true;
        }

        /* If we're not tracking IDs, we're done. */
        if (!trackIds) {
            return useEntry;
        }

        /* Read non-target entry. */
        if (targetLogEntry == null) {
            assert idTrackingSet.contains(fromLogType);

            targetLogEntry = idTrackingMap.get(fromLogType);
            if (targetLogEntry == null) {
                targetLogEntry = fromLogType.getNewLogEntry();
                idTrackingMap.put(fromLogType, targetLogEntry);
            }

            targetLogEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);
        }

        /*
         * Count node and delta entries as new.  Non-node/delta entries are
         * counted in isTargetEntry.
         */
        if (fromLogType.isNodeType() ||
            fromLogType.equals(LogEntryType.LOG_BIN_DELTA)) {
            tracker.countNewLogEntry(getLastLsn(), fromLogType,
                                     currentEntryHeader.getSize() +
                                     currentEntryHeader.getItemSize());
        }

        /* Track VLSNs in RollbackStart. */
        if (fromLogType.equals(LogEntryType.LOG_ROLLBACK_START)) {
            vlsnProxy.trackMapping(getLastLsn(),
                                   currentEntryHeader,
                                   targetLogEntry);
        }

        /* Process LN types. */
        if (fromLogType.isLNType()) {

            LNLogEntry<?> lnEntry = (LNLogEntry<?>) targetLogEntry;

            /*
             * When a MapLN is encountered, reset the tracked info for that
             * DB. This clears what we have accummulated so far for the DB
             * during this recovery pass. This is important to eliminate 
             * potential double counting of obsolete logrecs done earlier
             * in this recovery pass.  
             *
             * Also, save the LSN of the MapLN for use in utilization counting
             * during LN undo/redo.
             */
            if (fromLogType.equals(LogEntryType.LOG_MAPLN)) {

                MapLN mapLN = (MapLN) lnEntry.getMainItem();
                DatabaseId dbId = mapLN.getDatabase().getId();

                /* Track latest DB ID. */
                long dbIdVal = dbId.getId();
                maxDbId = (dbIdVal > maxDbId ? dbIdVal : maxDbId);
                minReplicatedDbId = (dbIdVal < minReplicatedDbId ?
                                     dbIdVal : minReplicatedDbId);
            }

            /* Track latest extinction scan ID. */
            if (fromLogType.equals(LogEntryType.LOG_EXTINCT_SCAN_LN) ||
                fromLogType.equals(
                    LogEntryType.LOG_EXTINCT_SCAN_LN_TRANSACTIONAL)) {

                DatabaseEntry entry = new DatabaseEntry();
                lnEntry.postFetchInit(false);
                lnEntry.getUserKeyData(entry, null);
                long id = ExtinctionScanner.materializeKey(entry);

                maxExtinctionId = Math.max(id, maxExtinctionId);
                minReplicatedExtinctionId = Math.min(
                    id, minReplicatedExtinctionId);
            }

            /* Track latest txn ID. */
            if (fromLogType.isTransactional()) {
                long txnId = lnEntry.getTxnId();
                maxTxnId = (txnId > maxTxnId ? txnId : maxTxnId);
                minReplicatedTxnId = (txnId < minReplicatedTxnId ?
                                      txnId : minReplicatedTxnId);
            }

            /*
             * When a FSLN is encountered, reset the tracked summary info for
             * that file. This clears what we have accummulated so far for the
             * file during this recovery pass. This is important to eliminate 
             * potential double counting of obsolete logrecs done earlier
             * in this recovery pass. 
             * 
             * Also, save the LSN of the FSLN for use in utilization counting
             * during LN undo/redo.
             */
            if (LogEntryType.LOG_FILESUMMARYLN.equals(fromLogType)) {

                lnEntry.postFetchInit(false /*isDupDb*/);

                long fileNum = FileSummaryLN.getFileNumber(lnEntry.getKey());

                tracker.resetFileInfo(fileNum);

                tracker.saveLastLoggedFileSummaryLN(fileNum, getLastLsn());

                /*
                 * Do not cache the file summary in the UtilizationProfile here,
                 * since it may be for a deleted log file. [#10395]
                 */
            }
        }

        /* Process IN types. */
        if (fromLogType.isINType()) {
            INLogEntry<?> inEntry = (INLogEntry<?>) targetLogEntry;

            /* Keep track of the largest node ID seen. */
            long nodeId = inEntry.getNodeId();
            assert (nodeId != Node.NULL_NODE_ID);

            maxNodeId = (nodeId > maxNodeId ? nodeId : maxNodeId);

            minReplicatedNodeId = 
                (nodeId < minReplicatedNodeId ? nodeId : minReplicatedNodeId);
        }

        /* Process INContainingEntry types. */
        if (fromLogType.isINType()) {

            INContainingEntry inEntry = (INContainingEntry) targetLogEntry;
            DatabaseId dbId = inEntry.getDbId();

            long newLsn = getLastLsn();

            /*
             * Count the previous version of this IN as obsolete. If lsn
             * (i.e. the current version) is non-provisional, then oldLsn is
             * indeed obsolete. However, if lsn is provisional, oldLsn is
             * obsolete only if an ancestor of this IN has been logged non-
             * provisionally later in the log, and unless lsn < CKPT_END
             * we cannot know if this is true or not. For this reason, we 
             * conservatively assume that oldLsn is indeed obsolete, but we
             * use inexact counting, so that the oldLsn value will not be
             * recorded by the tracker. (another reason is that earlier log
             * versions did not have a full LSN in oldLsn; they had only the
             * file number).
             *
             * Notice also that there may be an FSLN logrec after lsn that
             * includes oldLsn as an obsolete IN. If so, we are double counting
             * here, but the double counting will go away when we later process
             * that FSLN and as a result wipe-out the utilization info we have
             * collected so far about the log file containing oldLsn.
             */
            long oldLsn = inEntry.getLastFullLsn();

            if (oldLsn != DbLsn.NULL_LSN && !inEntry.isBINDelta()) {
                tracker.countObsoleteUnconditional(
                    oldLsn, fromLogType, 0, false /*countExact*/);
            }

            oldLsn = inEntry.getLastDeltaLsn();

            if (oldLsn != DbLsn.NULL_LSN) {
                tracker.countObsoleteUnconditional(
                    oldLsn, fromLogType, 0, false /*countExact*/);
            }

            /*
             * Count the current IN version as obsolete if lsn is
             * provisional and is after partialCkptStart. In this case, the
             * crash occurred during a ckpt and it the crash event itself
             * that may make the current logrec be obsolete. Again, whether
             * the lsn is indeed obsolete or not depends on whether an
             * ancestor of this IN has been logged non-provisionally later
             * in the log. At this point we cannot know if this is true or
             * not. For this reason, we conservatively assume that lsn
             * is indeed obsolete, but we use inexact counting, so that the
             * lsn value will not be recorded by the tracker. As explained
             * above, we may be double counting here, but this will be fixed
             * if we later find an FSLN for the same log file as lsn.
             *
             * We are too conservative here in assuming that lsn is
             * obsolete. This is because of the "grouping" behaviour of the
             * checpointer. Specifically:
             *
             * Most of the provisional IN logrecs in this region of the log
             * (i.e., after the start of an incomplete ckpt) are for BINs
             * logged by the checkpointer (eviction during a ckpt logs dirty
             * BINs and UINs provisionally as well). The checkpointer logs
             * all the dirty BIN siblings and then logs their parent non-
             * provisionally, before logging any other BINs. So, not taking
             * eviction into account, there can be at most 128 BIN logrecs
             * after the partialCkptStart that are trully obsolete.
             *
             * Note that older versions of the checkpointer did not use to
             * group together the logging of siblings BINs and their parent.
             * Without this grouping, the assumption done here that most
             * provisional logrecs after partialCkptStart are obsolete is
             * much more accurate.
             *
             * A potential solution: instead of counting these logrecs as
             * obsolete here, save their LSNs on-the-side inside the tracker.
             * When, later, a UIN N is replayed and attached to the tree, 
             * remove from the saved LSN set any LSNs that appear in the slots
             * of N. After all REDO-INs passes are done, count as obsolete any
             * LSNs remaining in the saved set. 
             */
            if (isProvisional &&
                partialCkptStart != DbLsn.NULL_LSN &&
                DbLsn.compareTo(partialCkptStart, newLsn) < 0) {
                tracker.countObsoleteUnconditional(
                    newLsn, fromLogType, 0, false /*countExact*/);
            }
        }

        /* Return true if this is a targeted entry. */
        return useEntry;
    }

    /**
     * Get the last IN seen by the reader.
     */
    public IN getIN(DatabaseImpl dbImpl)
        throws DatabaseException {

        return ((INContainingEntry) targetLogEntry).getIN(dbImpl);
    }

    /**
     * Get the last databaseId seen by the reader.
     */
    public DatabaseId getDatabaseId() {
        return ((INContainingEntry) targetLogEntry).getDbId();
    }

    /**
     * Get the maximum node ID seen by the reader.
     */
    public long getMaxNodeId() {
        return maxNodeId;
    }

    public long getMinReplicatedNodeId() {
        return minReplicatedNodeId;
    }

    /**
     * Get the maximum DB ID seen by the reader.
     */
    public long getMaxDbId() {
        return maxDbId;
    }

    public long getMinReplicatedDbId() {
        return minReplicatedDbId;
    }

    /**
     * Get the maximum txn ID seen by the reader.
     */
    public long getMaxTxnId() {
        return maxTxnId;
    }

    public long getMinReplicatedTxnId() {
        return minReplicatedTxnId;
    }

    /**
     * Get the maximum extinction scan ID seen by the reader.
     */
    public long getMaxExtinctionId() {
        return maxExtinctionId;
    }

    /**
     * Get the minimum extinction scan ID seen by the reader.
     */
    public long getMinReplicatedExtinctionId() {
        return minReplicatedExtinctionId;
    }

    /**
     * @return true if the last entry was a BIN-delta.
     */
    public boolean isBINDelta() {
        return
            targetLogEntry.getLogType().equals(LogEntryType.LOG_BIN_DELTA);
    }

    public VLSNRecoveryProxy getVLSNProxy() {
        return vlsnProxy;
    }

    public WholeEntry getWholeEntry() {
        return new WholeEntry(currentEntryHeader, targetLogEntry);
    }
}
