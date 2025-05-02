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
package com.sleepycat.je.rep.stream;

import static com.sleepycat.je.utilint.DbLsn.NULL_LSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.recovery.CheckpointEnd;
import com.sleepycat.je.recovery.RecoveryManager;
import com.sleepycat.je.rep.SyncUpFailedException;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.vlsn.VLSNIndex;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.txn.TxnAbort;
import com.sleepycat.je.txn.TxnCommit;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * The ReplicaSyncupReader scans the log backwards for requested log entries.
 * The reader must track whether it has passed a checkpoint, and therefore
 * can not used the vlsn index to skip over entries.
 *
 * The ReplicaSyncupReader is not thread safe, and can only be used
 * serially.
 */
public class ReplicaSyncupReader extends VLSNReader {

    /*
     * True if this particular record retrieval is for a syncable record.
     * False if the reader is looking for a specific VLSN
     */
    private boolean syncableSearch;

    private final LogEntry ckptEndLogEntry =
        LogEntryType.LOG_CKPT_END.getNewLogEntry();

    private final LogEntry commitLogEntry =
        LogEntryType.LOG_TXN_COMMIT.getNewLogEntry();

    private final LogEntry abortLogEntry =
        LogEntryType.LOG_TXN_ABORT.getNewLogEntry();

    /*
     * SearchResults retains the information as to whether the found
     * matchpoint is valid.
     */
    private final MatchpointSearchResults searchResults;

    private final Logger logger1;

    private static TestHook<Long> fileGapHook;

    private  ReplicaFeederSyncup syncup;

    private final int CHECK_CHANNEL_INTERVAL;

    private int timesChecked;

    private long lastTimeChecked;

    /*
     * Ping message frequency, will be 1/5 of RepParams.PRE_HEARTBEAT_TIMEOUT
     */
    private final int PING_INTERVAL;

    public ReplicaSyncupReader(EnvironmentImpl envImpl,
                               VLSNIndex vlsnIndex,
                               long endOfLogLsn,
                               int readBufferSize,
                               long startVLSN,
                               long finishLsn,
                               MatchpointSearchResults searchResults,
                               ReplicaFeederSyncup syncup)
            throws DatabaseException {

        /*
         * If we go backwards, endOfFileLsn and startLsn must not be null.
         * Make them the same, so we always start at the same very end.
         */
        this(envImpl,
                vlsnIndex,
                endOfLogLsn,
                readBufferSize,// forward
                startVLSN,
                finishLsn,
                searchResults);
        this.syncup = syncup;
        lastTimeChecked = TimeSupplier.currentTimeMillis();
    }

    public ReplicaSyncupReader(EnvironmentImpl envImpl,
                               VLSNIndex vlsnIndex,
                               long endOfLogLsn,
                               int readBufferSize,
                               long startVLSN,
                               long finishLsn,
                               MatchpointSearchResults searchResults)
        throws DatabaseException {

        /*
         * If we go backwards, endOfFileLsn and startLsn must not be null.
         * Make them the same, so we always start at the same very end.
         */
        super(envImpl,
              vlsnIndex,
              false,           // forward
              endOfLogLsn,
              readBufferSize,
              finishLsn);

        initScan(startVLSN, endOfLogLsn);
        this.searchResults = searchResults;
        logger1 = LoggerUtils.getLogger(getClass());
        CHECK_CHANNEL_INTERVAL = 100;
        PING_INTERVAL = envImpl.getConfigManager().getDuration(
                                    RepParams.PRE_HEARTBEAT_TIMEOUT) / 5;
        timesChecked = 0;
    }

    /**
     * Set up the ReplicaSyncupReader to start scanning from this VLSN.
     */
    private void initScan(long startVLSN, long endOfLogLsn) {

        if (startVLSN == NULL_VLSN) {
            throw EnvironmentFailureException.unexpectedState
                ("ReplicaSyncupReader start can't be NULL_VLSN");
        }

        startLsn = endOfLogLsn;
        assert startLsn != NULL_LSN;

        /*
         * Flush the log so that syncup can assume that all log entries that
         * are represented in the VLSNIndex  are safely out of the log buffers
         * and on disk. Simplifies this reader, so it can use the regular
         * ReadWindow, which only works on a file.
         */
        envImpl.getLogManager().flushNoSync();

        window.initAtFileStart(startLsn);
        currentEntryPrevOffset = window.getEndOffset();
        currentEntryOffset = window.getEndOffset();
        currentVLSN = startVLSN;
    }

    /**
     * Backward scanning for the replica's part in syncup.
     */
    public OutputWireRecord scanBackwards(long vlsn)
        throws DatabaseException {

        syncableSearch = false;
        VLSNRange range = vlsnIndex.getRange();
        if (vlsn < range.getFirst()) {
            /*
             * The requested VLSN is before the start of our range, we don't
             * have this record.
             */
            return null;
        }

        currentVLSN = vlsn;

        if (readNextEntry()) {
            return currentFeedRecord;
        }

        return null;
    }

    /**
     * Backward scanning for finding an earlier candidate syncup matchpoint.
     */
    public OutputWireRecord findPrevSyncEntry(boolean startAtPrev)
        throws DatabaseException {

        currentFeedRecord = null;
        syncableSearch = true;

        if (startAtPrev) {
            /* Start by looking at the entry before the current record. */
            currentVLSN = VLSN.getPrev(currentVLSN);
        } else {
            LoggerUtils.info(logger1, envImpl,
                             "Restart ReplicaSyncupReader at " +
                             "vlsn " + currentVLSN);
        }

        VLSNRange range = vlsnIndex.getRange();
        if (currentVLSN < range.getFirst()) {

            /*
             * We've walked off the end of the contiguous VLSN range.
             */
            return null;
        }

        if (readNextEntry() == false) {
            /*
             * We scanned all the way to the front of the log, no
             * other sync-able entry found.
             */
            return null;
        }

        assert LogEntryType.isSyncPoint(currentFeedRecord.getEntryType()) :
        "Unexpected log type= " + currentFeedRecord;

        return currentFeedRecord;
    }

    /**
     * @throw an EnvironmentFailureException if we were scanning for a
     * particular VLSN and we have passed it by.
     */
    private void checkForPassingTarget(long compareResult) {

        if (compareResult < 0) {
            /* Hey, we passed the VLSN we wanted. */
            throw EnvironmentFailureException.unexpectedState
                ("want to read " + currentVLSN + " but reader at " +
                 currentEntryHeader.getVLSN());
        }
    }

    /**
     * Return true for ckpt entries, for syncable entries, and if we're in
     * specific vlsn scan mode, any replicated entry.  There is an additional
     * level of filtering in processEntry.
     */
    @Override
    protected boolean isTargetEntry()
        throws DatabaseException {

        nScanned++;

        if(syncup != null) {
            timesChecked ++;
            if(timesChecked >= CHECK_CHANNEL_INTERVAL) {
                timesChecked = 0;
                if(!syncup.isNameChannelOpen()) {

                    /*
                     * This exception will be thrown all the way up to
                     * Replica.runReplicaLoopInternal(), an IOException will be
                     * handled there, causing the syncup process to fail.
                     */
                    throw new SyncUpFailedException
                            ("Replica-Feeder Syncup Failed.Replica-feeder" +
                             " channel has been shut down while replica scans" +
                             " log files, syncup is terminated consequently");
                }
                sendPingMessage();
            }
        }

        if (logger1.isLoggable(Level.FINEST)) {
            LoggerUtils.finest(logger1, envImpl,
                               " isTargetEntry " +  currentEntryHeader);
        }

        /* Skip invisible entries. */
        if (currentEntryHeader.isInvisible()) {
            return false;
        }

        byte currentType = currentEntryHeader.getType();

        /*
         * Return true if this entry is replicated. All entries need to be
         * perused by processEntry, when we are doing a vlsn based search,
         * even if they are not a sync point, because:
         *  (a) If this is a vlsn-based search, it's possible that the replica
         * and feeder are mismatched. The feeder will only propose a sync type
         * entry as a matchpoint but it might be that the replica has a non-
         * sync entry at that vlsn.
         *  (b) We need to note passed commits in processEntry.
         */
        if (entryIsReplicated()) {
            if (syncableSearch) {
                if (LogEntryType.isSyncPoint(currentType)) {
                    return true;
                }
                currentVLSN = VLSN.getPrev(currentEntryHeader.getVLSN());
            } else {
                return true;
            }
        }

        /*
         * We'll also need to read checkpoint end records to record their
         * presence.
         */
        if (LogEntryType.LOG_CKPT_END.equalsType(currentType)) {
            return true;
        }

        return false;
    }

    /**
     * Ping messages will be sent in a fixed frequency during a long scanning
     * of log files to keep the channel alive
     */
    private void sendPingMessage() {

        long currentTime = TimeSupplier.currentTimeMillis();
        if(currentTime - lastTimeChecked >= PING_INTERVAL) {
            try {
                syncup.sendPingMessage();
            } catch (IOException e) {
                throw new SyncUpFailedException
                        ("Replica-Feeder Syncup Failed. Replica" +
                         " IOException: " + e.getMessage() +
                         " Failed to keep ping messages ongoing between" +
                         " Feeder and Replica, syncup is terminated" +
                         " consequently");
            }
            lastTimeChecked = TimeSupplier.currentTimeMillis();
        }
    }

    /**
     * ProcessEntry does additional filtering before deciding whether to
     * return an entry as a candidate for matching.
     *
     * If this is a record we are submitting as a matchpoint candidate,
     * instantiate a WireRecord to house this log entry. If this is a
     * non-replicated entry or a txn end that follows the candidate matchpoint,
     * record whatever status we need to, but don't use it for comparisons.
     *
     * For example, suppose the log is like this:
     *
     * VLSN  entry
     * 10    LN
     * 11    commit
     * 12    LN
     *  --   ckpt end
     * 13    commit
     * 14    abort
     *
     * And that the master only has VLSNs 1-12. The replica will suggest vlsn
     * 14 as the first matchpoint. The feeder will counter with a suggestion
     * of vlsn 11, since it does not have vlsn 14.
     *
     * At that point, the ReplicaSyncupReader will scan backwards in the log,
     * looking for vlsn 11. Although the reader should only return an entry
     * when it gets to vlsn 11. The reader must process commits and ckpts that
     * follow 11, so that they can be recorded in the searchResults, so the
     * number of rolled back commits can be accurately reported.
     */
    @Override
    protected boolean processEntry(ByteBuffer entryBuffer) {

        if (logger1.isLoggable(Level.FINEST)) {
            LoggerUtils.finest(logger1, envImpl,
                               " syncup reader saw " +  currentEntryHeader);
        }
        byte currentType = currentEntryHeader.getType();


        /*
         * CheckpointEnd entries are tracked in order to see if a rollback
         * must be done, but are not returned as possible matchpoints.
         */
        if (LogEntryType.LOG_CKPT_END.equalsType(currentType)) {

            /*
             * Read the entry, which both lets us decipher its contents and
             * also advances the file reader position.
             */
            ckptEndLogEntry.readEntry(envImpl, currentEntryHeader,
                                      entryBuffer);

            if (logger1.isLoggable(Level.FINEST)) {
                LoggerUtils.finest(logger1, envImpl,
                                   " syncup reader read " +
                                   currentEntryHeader + ckptEndLogEntry);
            }

            CheckpointEnd ckptEnd = (CheckpointEnd)
            	ckptEndLogEntry.getMainItem();
            /*
             * If a checkpoint end is the last log in the logs, which is the
             * case on a normal close, then we can ignore the lastActiveLSN
             * for it.  There has not been any time to erase or clean any
             * files after it.
             */
            if (getLastLsn() !=
                envImpl.getFileManager().getLastUsedLsn() &&
                !ckptEnd.getInvoker().matches(
                    RecoveryManager.CHECKPOINT_INVOKER)) {
                searchResults.setFirstActiveLSN(ckptEnd.getFirstActiveLsn());
            }
            if (ckptEnd.getCleanedFilesToDelete()) {
                searchResults.notePassedCheckpointEnd();
            }

            return false;
        }

        /*
         * Setup the log entry as a wire record so we can compare it to
         * the entry from the feeder as we look for a matchpoint. Do this
         * before we change positions on the entry buffer by reading it.
         */
        ByteBuffer buffer = entryBuffer.slice();
        buffer.limit(currentEntryHeader.getItemSize());
        currentFeedRecord =
            new OutputWireRecord(envImpl, currentEntryHeader, buffer);

        /*
         * All commit records must be tracked to figure out if we've exceeded
         * the txn rollback limit. For reporting reasons, we'll need to
         * unmarshal the log entry, so we can read the timestamp in the commit
         * record.
         */
        if (LogEntryType.LOG_TXN_COMMIT.equalsType(currentType)) {

            commitLogEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);
            final TxnCommit commit = (TxnCommit) commitLogEntry.getMainItem();
            searchResults.notePassedCommits(commit,
                                            currentEntryHeader.getVLSN(),
                                            getLastLsn());

            if (logger1.isLoggable(Level.FINEST)) {
                LoggerUtils.finest(logger1, envImpl,
                                   "syncup reader read " +
                                   currentEntryHeader + commitLogEntry);
            }
        } else if (LogEntryType.LOG_TXN_ABORT.equalsType(currentType)) {
            abortLogEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);
            final TxnAbort abort = (TxnAbort) abortLogEntry.getMainItem();

            searchResults.notePassedAborts(abort,
                                           currentEntryHeader.getVLSN());
            if (logger1.isLoggable(Level.FINEST)) {
                LoggerUtils.finest(logger1, envImpl,
                                   "syncup reader read " +
                                   currentEntryHeader + abortLogEntry);
            }
        } else {
            entryBuffer.position(entryBuffer.position() +
                                 currentEntryHeader.getItemSize());
        }

        if (syncableSearch) {
            return true;
        }

        /* We're looking for a particular VLSN. */
        long entryVLSN = currentEntryHeader.getVLSN();
        checkForPassingTarget(entryVLSN - currentVLSN);

        /* return true if this is the entry we want. */
        return (entryVLSN == currentVLSN);
    }

    /**
     * TBW
     */
    @Override
    protected void handleGapInBackwardsScan(long prevFileNum) {
        SkipGapException e = new SkipGapException(window.currentFileNum(),
                                                  prevFileNum,
                                                  currentVLSN);
        LoggerUtils.warning(logger1, envImpl, e.toString());
        assert TestHookExecute.doHookIfSet(fileGapHook, prevFileNum);
        throw e;
    }

    /*
     * An internal exception indicating that the reader must scan across a
     * gap in the log files. The gap may have been created by cleaning.
     */
    @SuppressWarnings("serial")
    public static class SkipGapException extends DatabaseException
        implements NotSerializable {

        private final long currentVLSN;

        public SkipGapException(long currentFileNum,
                                long nextFileNum,
                                long currentVLSN) {
            super("Restarting reader in order to read backwards across gap " +
                  "from file 0x" + Long.toHexString(currentFileNum) +
                  " to file 0x" + Long.toHexString(nextFileNum) +
                  ". Reset position to vlsn " + currentVLSN +
                  ". This will delay completion of replication syncup.");
            this.currentVLSN = currentVLSN;
        }

        public long getVLSN() {
            return currentVLSN;
        }
    }

    public static void setFileGapHook(TestHook<Long> hook) {
        fileGapHook = hook;
    }
}
