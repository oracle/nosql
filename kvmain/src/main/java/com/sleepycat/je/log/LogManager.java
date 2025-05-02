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

import static com.sleepycat.je.EnvironmentFailureException.unexpectedState;
import static com.sleepycat.je.log.LogStatDefinition.GROUP_DESC;
import static com.sleepycat.je.log.LogStatDefinition.GROUP_NAME;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_END_OF_LOG;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_ITEM_BUFFER_POOL_EMPTY;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_ITEM_BUFFER_TOO_SMALL;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_REPEAT_FAULT_READS;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_REPEAT_ITERATOR_READS;
import static com.sleepycat.je.log.LogStatDefinition.LOGMGR_TEMP_BUFFER_WRITES;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.ExpirationTracker;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager.FIOStatsCollector;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.ReplicableLogEntry;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.util.verify.VerifierUtils;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.FIOStatsCollectingThread;
import com.sleepycat.je.utilint.LSNStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * The LogManager supports reading and writing to the JE log.
 * The writing of data to the log is serialized via the logWriteMutex.
 * Typically space is allocated under the LWL. The client computes
 * the checksum and copies the data into the log buffer (not holding
 * the LWL).
 */
public class LogManager {

    private final LogBufferPool logBufferPool; // log buffers
    private final Object logWriteMutex;           // synchronizes log writes
    private final boolean doChecksumOnRead;      // if true, do checksum on read
    private final FileManager fileManager;       // access to files
    private final FSyncManager grpManager;
    private final EnvironmentImpl envImpl;
    private final boolean readOnly;

    private final ThreadLocal<ByteBuffer> threadItemBuffer;
    private final ThreadLocal<LogEntryHeader> threadItemHeader;

    private final int itemPoolSize;
    private final AtomicInteger cachedBufferCount = new AtomicInteger(0);
    private final Queue<CachedLogItemBuffer> pooledBuffers;

    /*
     * Item buffer size for reads and writes. Determines the size of cached
     * buffers (thread locals and pooled buffers). Also determines initial
     * read size when item size unknown, e.g., when reading INs.
     */
    private final int itemBufferSize;

    /* The last LSN in the log during recovery. */
    private long lastLsnAtRecovery = DbLsn.NULL_LSN;

    /* Stats */
    private final StatGroup stats;

    /* Cache this stats collector, since it's frequently used. */
    private final FIOStatsCollector miscStatsCollector;

    /*
     * Number of times we have to repeat a read when we fault in an object
     * because the initial read was too small.
     */
    private final LongStat nRepeatFaultReads;

    /*
     * Number of times that FileReaders can't grown the read buffer to
     * accomodate a large log entry.
     */
    private final LongStat nRepeatIteratorReads;

    /*
     * Number of times we have to use the temporary marshalling buffer to
     * write to the log.
     */
    private final LongStat nTempBufferWrites;

    /* The location of the next entry to be written to the log. */
    private final LSNStat endOfLog;

    /* Stats related to use of the LogItem buffers. */
    private final LongStat nItemBufTooSmall;
    private final LongStat nItemBufPoolEmpty;

    /*
     * Used to determine if we switched log buffers. For
     * NOSYNC durability, if we switched log buffers,
     * the thread will write the previous dirty buffers.
     */
    private LogBuffer prevLogBuffer = null;

    /* For unit tests */
    private TestHook<?> readHook; // used for generating exceptions on log reads

    /* For unit tests. */
    private TestHook<Object> delayVLSNRegisterHook;
    private TestHook<Object> flushHook;

    /* A queue to hold log entries which are to be logged lazily. */
    private final Queue<LazyQueueEntry> lazyLogQueue =
        new ConcurrentLinkedQueue<>();

    /*
     * Used for tracking the current file. Is null if no tracking should occur.
     * Read/write of this field is protected by the LWL, but the tracking
     * actually occurs outside the LWL.
     */
    private ExpirationTracker expirationTracker = null;

    /*
     * An entry in the lazyLogQueue. A struct to hold the entry and repContext.
     */
    private static class LazyQueueEntry {
        private final LogEntry entry;
        private final ReplicationContext repContext;

        private LazyQueueEntry(LogEntry entry, ReplicationContext repContext) {
            this.entry = entry;
            this.repContext = repContext;
        }
    }

    /**
     * There is a single log manager per database environment.
     */
    public LogManager(EnvironmentImpl envImpl,
                      boolean readOnly)
        throws DatabaseException {

        /* Set up log buffers. */
        this.envImpl = envImpl;
        this.fileManager = envImpl.getFileManager();
        miscStatsCollector = fileManager.getMiscStatsCollector();
        this.grpManager = new FSyncManager(this.envImpl);
        final DbConfigManager configManager = envImpl.getConfigManager();
        this.readOnly = readOnly;
        logBufferPool = new LogBufferPool(fileManager, envImpl);

        /* See if we're configured to do a checksum when reading in objects. */
        doChecksumOnRead =
            configManager.getBoolean(EnvironmentParams.LOG_CHECKSUM_READ);

        logWriteMutex = new Object();
        itemBufferSize =
            configManager.getInt(EnvironmentParams.LOG_FAULT_READ_SIZE);

        /* Do the stats definitions. */
        stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        nRepeatFaultReads = new LongStat(stats, LOGMGR_REPEAT_FAULT_READS);
        nRepeatIteratorReads =
            new LongStat(stats, LOGMGR_REPEAT_ITERATOR_READS);
        nTempBufferWrites = new LongStat(stats, LOGMGR_TEMP_BUFFER_WRITES);
        endOfLog = new LSNStat(stats, LOGMGR_END_OF_LOG);

        nItemBufTooSmall = new LongStat(stats, LOGMGR_ITEM_BUFFER_TOO_SMALL);
        nItemBufPoolEmpty = new LongStat(stats, LOGMGR_ITEM_BUFFER_POOL_EMPTY);

        threadItemBuffer = configManager.getBoolean(
                EnvironmentParams.LOG_ITEM_THREAD_LOCAL) ?
            ThreadLocal.withInitial(() ->
                ByteBuffer.allocate(itemBufferSize)) :
            null;

        threadItemHeader =
            ThreadLocal.withInitial(LogEntryHeader::new);

        itemPoolSize =
            configManager.getInt(EnvironmentParams.LOG_ITEM_POOL_SIZE);

        pooledBuffers = (envImpl.isReplicated() && itemPoolSize > 0) ?
            new ConcurrentLinkedQueue<>() :
            null;
    }

    public boolean getChecksumOnRead() {
        return doChecksumOnRead;
    }

    public int getItemBufferSize() {
        return itemBufferSize;
    }

    public long getLastLsnAtRecovery() {
        return lastLsnAtRecovery;
    }

    public void setLastLsnAtRecovery(long lastLsnAtRecovery) {
        this.lastLsnAtRecovery = lastLsnAtRecovery;
    }

    /**
     * Called at the end of recovery to begin expiration tracking using the
     * given tracker. During recovery we are single threaded, so we can set
     * the field without taking the LWL.
     */
    public void initExpirationTracker(final ExpirationTracker tracker) {
        expirationTracker = tracker;
    }

    /**
     * Reset the pool when the cache is resized.  This method is called after
     * the memory budget has been calculated.
     */
    public void resetPool(DbConfigManager configManager)
            throws DatabaseException {
        synchronized (logWriteMutex) {
           logBufferPool.reset(configManager);
        }
    }

    /*
     * Writing to the log
     */

    /**
     * Log this single object and force a write of the log files.
     * @param entry object to be logged
     * @param fsyncRequired if true, log files should also be fsynced.
     * @return LSN of the new log entry
     */
    public long logForceFlush(LogEntry entry,
                              boolean fsyncRequired,
                              ReplicationContext repContext)
        throws DatabaseException {

        return log(entry,
                   Provisional.NO,
                   true,           // flush required
                   fsyncRequired,
                   false,          // forceNewLogFile
                   repContext);    // repContext
    }

    /**
     * Log this single object and force a flip of the log files.
     * @param entry object to be logged
     * @return LSN of the new log entry
     */
    public long logForceFlip(LogEntry entry)
        throws DatabaseException {

        return log(entry,
                   Provisional.NO,
                   true,           // flush required
                   false,          // fsync required
                   true,           // forceNewLogFile
                   ReplicationContext.NO_REPLICATE);
    }

    /**
     * Write a log entry.
     * @param entry object to be logged
     * @return LSN of the new log entry
     */
    public long log(LogEntry entry, ReplicationContext repContext)
        throws DatabaseException {

        return log(entry,
                   Provisional.NO,
                   false,           // flush required
                   false,           // fsync required
                   false,           // forceNewLogFile
                   repContext);
    }

    /**
     * Write a log entry lazily.
     * @param entry object to be logged
     */
    void logLazily(LogEntry entry, ReplicationContext repContext) {

        lazyLogQueue.add(new LazyQueueEntry(entry, repContext));
    }

    /**
     * Translates individual log params to LogItem and LogContext fields.
     */
    private long log(final LogEntry entry,
                     final Provisional provisional,
                     final boolean flushRequired,
                     final boolean fsyncRequired,
                     final boolean forceNewLogFile,
                     final ReplicationContext repContext)
        throws DatabaseException {

        final LogParams params = new LogParams();

        params.entry = entry;
        params.provisional = provisional;
        params.repContext = repContext;
        params.flushRequired = flushRequired;
        params.fsyncRequired = fsyncRequired;
        params.forceNewLogFile = forceNewLogFile;

        final LogItem item = log(params);

        return item.lsn;
    }

    /**
     * Log an item, first logging any items on the lazyLogQueue, and finally
     * flushing and sync'ing (if requested).
     */
    public LogItem log(LogParams params)
        throws DatabaseException {

        /*
         * In a read-only env we return NULL_LSN (the default value for
         * LogItem.lsn) for all entries.  We allow this to proceed, rather
         * than throwing an exception, to support logging INs for splits that
         * occur during recovery, for one reason.  Logging LNs in a read-only
         * env is not allowed, and this is checked in the LN class.
         */
        if (readOnly) {
            return LogItem.EMPTY_ITEM;
        }

        try {
            /* Flush any pending lazy entries. */
            for (LazyQueueEntry lqe = lazyLogQueue.poll();
                 lqe != null;
                 lqe = lazyLogQueue.poll()) {

                final LogParams lqeParams = new LogParams();
                lqeParams.entry = lqe.entry;
                lqeParams.provisional = Provisional.NO;
                lqeParams.repContext = lqe.repContext;

                logItem(new LogItem(), lqeParams);
            }

            final LogItem item = new LogItem();
            logItem(item, params);
            return item;

        } catch (EnvironmentFailureException e) {
            /*
             * Final checks are below for unexpected exceptions during the
             * critical write path.  Most should be caught by
             * serialLogInternal, but the catches here account for other
             * exceptions above.  Note that Errors must be caught here as well
             * as Exceptions.  [#21929]
             *
             * If we've already invalidated the environment, rethrow so as not
             * to excessively wrap the exception.
             */
            if (!envImpl.isValid()) {
                throw e;
            }
            throw EnvironmentFailureException.unexpectedException(envImpl, e);

        } catch (Exception e) {
            throw EnvironmentFailureException.unexpectedException(envImpl, e);

        } catch (Error e) {
            envImpl.invalidate(e);
            throw e;
        }
    }

    private void logItem(final LogItem item, final LogParams params) {

        final boolean inRepStream = params.repContext.inReplicationStream();

        /* TODO: LogItem could also be in a thread local, if !inRepStream. */
        item.header = !inRepStream ?
            threadItemHeader.get() : new LogEntryHeader();

        item.header.initForWrite(
            params.entry, params.provisional, params.repContext);

        /*
         * If possible, marshall this entry outside the log write latch to
         * allow greater concurrency by shortening the write critical
         * section.  Note that the header may only be created during
         * marshalling because it calls entry.getSize().
         */
        if (params.entry.getLogType().marshallOutsideLatch()) {
            item.header.initEntrySize(params.entry);
            marshallIntoBuffer(item, params, inRepStream);
        }

        serialLog(item, params);

        if (params.segment != null) {

            /* Add checksum, prev offset and VLSN to the entry. */
            item.header.addPostMarshallingInfo(
                params.buffer, params.prevOffset, params.vlsn);

            /* Copy entry buffer into the log buffer. */
            params.segment.put(params.buffer, params.vlsn);
        }

        updateObsolete(params);

        /* Expiration tracking is protected by Btree latch, not the LWL. */
        if (params.expirationTrackerToUse != null) {
            params.expirationTrackerToUse.track(
                envImpl, params.entry, item.size);
        }

        /* Queue flushing of expiration tracker after a file flip. */
        if (params.expirationTrackerCompleted != null) {
            envImpl.getExpirationProfile().addCompletedTracker(
                params.expirationTrackerCompleted);
        }

        if (params.fsyncRequired || params.flushRequired) {

            /* Flush log buffers and write queue, and optionally fsync. */
            grpManager.flushAndSync(params.fsyncRequired);

        } else if (params.switchedLogBuffer) {
            /*
             * The operation does not require writing to the log file, but
             * since we switched log buffers, this thread will write the
             * previously dirty log buffers (not this thread's log entry
             * though). This is done for NOSYNC durability so those types
             * of transactions won't fill all the log buffers thus forcing
             * to have to write the buffers under the log write latch.
             */
            logBufferPool.writeDirty(false /*flushWriteQueue*/);
        }

        TestHookExecute.doHookIfSet(flushHook);

        /*
         * We've logged this log entry from the replication stream. Let the
         * Replicator know, so this node can create a VLSN->LSN mapping. Do
         * this before the ckpt so we have a better chance of writing this
         * mapping to disk.
         */
        if (inRepStream) {
            assert (item.header.getVLSN() != INVALID_VLSN) :
                "Unexpected null vlsn: " + item.header + " " +
                params.repContext;

            /* Block the VLSN registration, used by unit tests. */
            TestHookExecute.doHookIfSet(delayVLSNRegisterHook);

            /*
             * Position buffer before adding to tip cache so that slice()
             * will produce entry buffer (not including header). Buffer will
             * be accessed by multiple threads and treated as immutable.
             */
            params.buffer.position(item.header.getSize());

            /* Add VLSN to VLSNIndex and add LogItem to tip cache. */
            envImpl.registerVLSN(item);
        }

        /*
         * Periodically, as a function of how much data is written, ask the
         * checkpointer or the cleaner to wake up.
         */
        envImpl.getCheckpointer().wakeupAfterWrite();
        envImpl.getCleaner().wakeupAfterWrite(item.size);

        /* Update background writes. */
        if (params.backgroundIO) {
            envImpl.updateBackgroundWrites(
                item.size, logBufferPool.getLogBufferSize());
        }
    }

    /**
     * This method handles exceptions to be certain that the Environment is
     * invalidated when any exception occurs in the critical write path, and it
     * checks for an invalid environment to be sure that no subsequent write is
     * allowed.  [#21929]
     *
     * Invalidation is necessary because a logging operation does not ensure
     * that the internal state -- correspondence of LSN pointer, log buffer
     * position and file position, and the integrity of the VLSN index [#20919]
     * -- is maintained correctly when an exception occurs.  Allowing a
     * subsequent write can cause log corruption.
     */
    private void serialLog(
        final LogItem item,
        final LogParams params) {

        synchronized (logWriteMutex) {

            /* Do not attempt to write with an invalid environment. */
            envImpl.checkIfInvalid();

            try {
                serialLogWork(item, params);

            } catch (EnvironmentFailureException e) {
                /*
                 * If we've already invalidated the environment, rethrow so
                 * as not to excessively wrap the exception.
                 */
                if (!envImpl.isValid()) {
                    throw e;
                }

                /* Otherwise, invalidate the environment. */
                throw EnvironmentFailureException.unexpectedException(
                    envImpl, e);

            } catch (Exception e) {
                throw EnvironmentFailureException.unexpectedException(
                    envImpl, e);

            } catch (Error e) {
                /* Errors must be caught here as well as Exceptions.[#21929] */
                envImpl.invalidate(e);
                throw e;
            }
        }

        /*
         * Collect here, rather than in the try block, so the computation
         * is outside the mutex and does not add to contention.
         */
        FIOStatsCollectingThread.collectIf(false, item.size,
                                           miscStatsCollector);
    }

    /**
     * This method is used as part of writing data to the log. Called
     * under the LogWriteLatch.
     * Data is either written to the LogBuffer or allocates space in the
     * LogBuffer. The LogBufferSegment object is used to save information about
     * the space allocate in the LogBuffer. The caller uses the object to
     * copy data into the underlying LogBuffer. A null LogBufferSegment
     * indicates that the item was written to the log. This occurs when the
     * data item is too big to fit into an empty LogBuffer.
     */
    private void serialLogWork(
        final LogItem item,
        final LogParams params)
        throws IOException {

        /*
         * Do obsolete tracking before marshalling a FileSummaryLN into the
         * log buffer so that a FileSummaryLN counts itself.
         * countObsoleteNode must be called before computing the entry
         * size, since it can change the size of a FileSummaryLN entry that
         * we're logging
         */
        final LogEntryType entryType = params.entry.getLogType();
        final UtilizationTracker tracker = envImpl.getUtilizationTracker();

        if (params.oldLsn != DbLsn.NULL_LSN) {
            if (params.obsoleteDupsAllowed) {
                tracker.countObsoleteNodeDupsAllowed(
                    params.oldLsn, entryType, params.oldSize);
            } else {
                tracker.countObsoleteNode(
                    params.oldLsn, entryType, params.oldSize);
            }
        }

        /* Count auxOldLsn for same database; no specified size. */
        if (params.auxOldLsn != DbLsn.NULL_LSN) {
            if (params.obsoleteDupsAllowed) {
                tracker.countObsoleteNodeDupsAllowed(
                    params.auxOldLsn, entryType, 0);
            } else {
                tracker.countObsoleteNode(params.auxOldLsn, entryType, 0);
            }
        }

        /* Run the serial hook on the entry. */
        params.entry.runSerialHook();

        /*
         * Compute the VLSNs and modify the DTVLSN in commit/abort entries
         * before the entry is marshalled or its size is required. At that
         * at this point we are committed to writing a log entry with  the
         * computed VLSN.
         */
        final long vlsn;

        if (params.repContext.getClientVLSN() != INVALID_VLSN ||
            params.repContext.mustGenerateVLSN()) {

            if (params.repContext.mustGenerateVLSN()) {
                vlsn = envImpl.assignVLSNs(params.entry);
            } else {
                vlsn = params.repContext.getClientVLSN();
                if (params.repContext.inReplicationStream()) {
                    envImpl.setReplayVLSNCounter(vlsn);
                }
            }
        } else {
            vlsn = INVALID_VLSN;
        }

        /*
         * If an entry must be protected within the log write latch for
         * marshalling, take care to also calculate its size in the
         * protected section. Note that we have to get the size *before*
         * marshalling so that the currentLsn and size are correct for
         * utilization tracking.
         */
        if (!entryType.marshallOutsideLatch()) {
            item.header.initEntrySize(params.entry);
        }

        final int entrySize = item.header.getEntrySize();

        /*
         * Get the next free slot in the log, under the log write latch.
         */
        if (params.forceNewLogFile) {
            fileManager.forceNewLogFile();
        }

        final boolean flippedFile = fileManager.shouldFlipFile(entrySize);
        final long currentLsn = fileManager.calculateNextLsn(flippedFile);

        /*
         * TODO: Count file header, since it is not logged via LogManager.
         * Some tests (e.g., INUtilizationTest) will need to be adjusted.
         *
        final int fileHeaderSize = FileManager.firstLogEntryOffset();
        if (DbLsn.getFileOffset(currentLsn) == fileHeaderSize) {
            final long fileNum = DbLsn.getFileNumber(currentLsn);

            tracker.countNewLogEntry(
                DbLsn.makeLsn(fileNum, 0), LogEntryType.LOG_FILE_HEADER,
                fileHeaderSize, null);
        }
        */

        /*
         * countNewLogEntry and countObsoleteNodeInexact cannot change
         * a FileSummaryLN size, so they are safe to call after
         * getSizeForWrite.
         */
        tracker.countNewLogEntry(currentLsn, entryType, entrySize);

        /*
         * LN deletions and dup DB LNs are obsolete immediately.  Inexact
         * counting is used to save resources because the cleaner knows
         * that all such LNs are obsolete.
         */
        if (params.entry.isImmediatelyObsolete(params.nodeDb)) {
            tracker.countObsoleteNodeInexact(currentLsn, entryType, entrySize);
        }

        /* Marshall after tracking as noted above. */
        if (!entryType.marshallOutsideLatch()) {
            marshallIntoBuffer(
                item, params, params.repContext.inReplicationStream());
        }

        /*
         * Ask for a log buffer suitable for holding this new entry. If
         * entrySize is larger than the LogBuffer capacity, this will flush
         * all dirty buffers and return the next empty (but too small) buffer.
         * The returned buffer is not latched.
         */
        final LogBuffer lastLogBuffer =
            logBufferPool.getWriteBuffer(entrySize, flippedFile);

        /*
         * Bump the LSN values, which gives us a valid previous pointer,
         * which is part of the log entry header. This must be done:
         *  - before logging the currentLsn.
         *  - after calling getWriteBuffer, to flush the prior file when
         *    flippedFile is true.
         */
        final long prevOffset = fileManager.advanceLsn(
            currentLsn, entrySize, flippedFile);

        if (lastLogBuffer != prevLogBuffer) {
            params.switchedLogBuffer = true;
        }
        prevLogBuffer = lastLogBuffer;

        final LogBufferSegment bufferSegment;

        lastLogBuffer.latchForWrite();
        try {
            bufferSegment = lastLogBuffer.allocate(entrySize);

            if (bufferSegment != null) {
                /* Register the lsn while holding the buffer latch. */
                lastLogBuffer.registerLsn(currentLsn);
            } else {
                /*
                 * The item buffer is larger than the LogBuffer capacity, so
                 * write the item buffer to the file directly. Note that
                 * getWriteBuffer has flushed all dirty buffers.
                 *
                 * First add checksum, prev offset, and VLSN to the entry.
                 */
                item.header.addPostMarshallingInfo(
                    params.buffer, prevOffset, vlsn);

                final boolean flushWriteQueue =
                    params.flushRequired && !params.fsyncRequired;

                fileManager.writeLogBuffer(
                    new LogBuffer(params.buffer, currentLsn, params.vlsn),
                    flushWriteQueue);

                assert lastLogBuffer.getDataBuffer().position() == 0;

                /* Leave a clue that the buffer size needs to be increased. */
                nTempBufferWrites.increment();
            }
        } finally {
            lastLogBuffer.release();
        }

        /*
         * If the txn is not null, the first entry is an LN. Update the txn
         * with info about the latest LSN. Note that this has to happen
         * within the log write latch.
         */
        params.entry.postLogWork(item.header, currentLsn, vlsn);

        item.lsn = currentLsn;
        item.size = entrySize;
        params.vlsn = vlsn;
        params.segment = bufferSegment;
        params.prevOffset = prevOffset;

        /* If the expirationTracker field is null, no tracking should occur. */
        if (expirationTracker != null) {
            /*
             * When logging to a new file, also flip the expirationTracker
             * under the LWL and return expirationTrackerCompleted so it will
             * be queued for flushing.
             */
            if (flippedFile) {
                final long newFile = DbLsn.getFileNumber(item.lsn);
                if (newFile != expirationTracker.getFileNum()) {
                    params.expirationTrackerCompleted = expirationTracker;
                    expirationTracker = new ExpirationTracker(newFile);
                }
            }
            /*
             * Increment the pending calls under the LWL, so we can determine
             * when we're finished.
             */
            expirationTracker.incrementPendingTrackCalls();
            params.expirationTrackerToUse = expirationTracker;
        }
    }

    /**
     * Serialize a loggable object into this buffer.
     */
    private void marshallIntoBuffer(final LogItem item,
                                    final LogParams params,
                                    final boolean inRepStream) {

        final int entrySize = item.header.getEntrySize();

        ByteBuffer buffer = null;
        CachedLogItemBuffer pooledBuffer = null;

        if (inRepStream && !params.immutableLogEntry) {
            if (pooledBuffers != null) {
                if (entrySize <= itemBufferSize) {
                    pooledBuffer = pooledBuffers.poll();
                    if (pooledBuffer == null &&
                        cachedBufferCount.get() < itemPoolSize) {
                        cachedBufferCount.incrementAndGet();
                        pooledBuffer = new CachedLogItemBuffer(
                            pooledBuffers, itemBufferSize);
                    }
                    if (pooledBuffer != null) {
                        pooledBuffer.allocate();
                        if (params.incLogItemUsage) {
                            pooledBuffer.incrementUse();
                        }
                        buffer = pooledBuffer.getBuffer();
                        buffer.clear();
                    } else {
                        nItemBufPoolEmpty.increment();
                    }
                } else {
                    nItemBufTooSmall.increment();
                }
            }
        } else {
            if (entrySize <= itemBufferSize) {
                buffer = threadItemBuffer.get();
                buffer.clear();
            } else {
                nItemBufTooSmall.increment();
            }
        }

        if (buffer == null) {
            buffer = ByteBuffer.allocate(entrySize);
        }

        params.buffer = buffer;

        if (inRepStream) {
            if (params.immutableLogEntry) {
                item.cachedEntry = (ReplicableLogEntry) params.entry;
                item.cachedBuffer = null;
                item.nonCachedBuffer = null;
            } else {
                item.cachedEntry = null;
                item.cachedBuffer = pooledBuffer;
                item.nonCachedBuffer = (pooledBuffer != null) ? null : buffer;
            }
        }

        item.header.writeToLog(buffer);
        params.entry.writeEntry(buffer);

        /* Set the limit so it can be used as the size of the entry. */
        buffer.flip();

        /* Sanity check */
        if (entrySize != buffer.limit()) {
            throw unexpectedState(
                "Logged entry entrySize= " + entrySize +
                    " but marshalledSize=" + buffer.limit() +
                    " header=" + item.header);
        }
    }

    /**
     * Serialize a log entry into this buffer with proper entry header. Return
     * it ready for a copy. May only be used for non-transactional,
     * non-replicated entries that are not logged by {@link #serialLog}.
     *
     * Currently this method is only used for serializing the FileHeader
     * and RestoreMarker.
     */
    ByteBuffer putIntoBuffer(LogEntry entry,
                             long prevLogEntryOffset) {

        assert !entry.getLogType().isTransactional();

        final LogEntryHeader header = new LogEntryHeader();

        header.initForWrite(
            entry, Provisional.NO, ReplicationContext.NO_REPLICATE);

        header.initEntrySize(entry);

        final ByteBuffer destBuffer =
            ByteBuffer.allocate(header.getEntrySize());

        header.writeToLog(destBuffer);
        entry.writeEntry(destBuffer);

        destBuffer.flip();

        header.addPostMarshallingInfo(destBuffer, prevLogEntryOffset,
                                      INVALID_VLSN);

        return destBuffer;
    }

    /*
     * Reading from the log.
     */

    /**
     * Instantiate all the objects in the log entry at this LSN.
     */
    public LogEntry getLogEntry(long lsn, int lastLoggedSize)
        throws FileNotFoundException, ErasedException {

        return getLogEntry(
            lsn, lastLoggedSize, false /*invisibleReadAllowed*/).getEntry();
    }

    public WholeEntry getWholeLogEntry(long lsn, int lastLoggedSize)
        throws FileNotFoundException, ErasedException {

        return getLogEntry(
            lsn, lastLoggedSize, false /*invisibleReadAllowed*/);
    }

    /**
     * Instantiate all the objects in the log entry at this LSN. Allow the
     * fetch of invisible log entries if we are in recovery.
     */
    public WholeEntry getLogEntryAllowInvisibleAtRecovery(long lsn, int size)
        throws FileNotFoundException, ErasedException {

        return getLogEntry(
            lsn, size, envImpl.isInInit() /*invisibleReadAllowed*/);
    }

    /**
     * Instantiate all the objects in the log entry at this LSN. The entry
     * may be marked invisible.
     */
    public WholeEntry getLogEntryAllowInvisible(long lsn, int lastLoggedSize)
        throws FileNotFoundException, ErasedException {

        return getLogEntry(lsn, lastLoggedSize, true);
    }

    /**
     * Instantiate all the objects in the log entry at this LSN.
     * @param lsn location of entry in log.
     * @param invisibleReadAllowed true if it's expected that the target log
     * entry might be invisible. Correct the known-to-be-bad checksum before
     * proceeding.
     * @return log entry that embodies all the objects in the log entry.
     */
    private WholeEntry getLogEntry(
        long lsn,
        int lastLoggedSize,
        boolean invisibleReadAllowed)
        throws FileNotFoundException, ErasedException {

        /* Fail loudly if the environment is invalid. */
        envImpl.checkIfInvalid();

        LogSource logSource = null;
        try {

            /*
             * Get a log source for the log entry which provides an abstraction
             * that hides whether the entry is in a buffer or on disk. Will
             * register as a reader for the buffer or the file, which will take
             * a latch if necessary. Latch is released in finally block.
             */
            logSource = getLogSource(lsn);

            try {
                return getLogEntryFromLogSource(
                    lsn, lastLoggedSize, logSource, true,
                    invisibleReadAllowed);

            } catch (ChecksumException ce) {

                /*
                 * When using a FileSource, a checksum error indicates a
                 * persistent corruption. An EFE with LOG_CHECKSUM is created
                 * in the catch below and EFE.isCorrupted will return true.
                 */
                if (!(logSource instanceof LogBuffer)) {
                    assert logSource instanceof FileSource;
                    throw ce;
                }

                /*
                 * When using a LogBuffer source, we must try to read the entry
                 * from disk to see if the corruption is persistent.
                 */
                final LogBuffer logBuffer = (LogBuffer) logSource;
                FileHandle fileHandle = null;
                long fileLength = -1;
                try {
                    fileHandle =
                        fileManager.getFileHandle(DbLsn.getFileNumber(lsn));
                    fileLength = fileHandle.getFile().length();
                } catch (IOException ioe) {
                    /* FileNotFound or another IOException was thrown. */
                }

                /*
                 * If the file does not exist (FileNotFoundException is thrown
                 * above) or the firstLsn in the buffer does not appear in the
                 * file (the buffer was not flushed), then the corruption is
                 * not persistent and we throw a EFE for which isCorrupted
                 * will return false.
                 */
                if (fileHandle == null ||
                    fileLength <=
                        DbLsn.getFileOffset(logBuffer.getFirstLsn())) {

                    throw EnvironmentFailureException.unexpectedException(
                        envImpl,
                        "Corruption detected in log buffer, " +
                            "but was not written to disk.",
                        ce);
                }

                /*
                 * The log entry should have been written to the file. Try
                 * getting the log entry from the FileSource. If a
                 * ChecksumException is thrown, the corruption is persistent
                 * and an EFE with LOG_CHECKSUM is thrown below.
                 */
                final FileSource fileSource = new FileHandleSource(
                    fileHandle, fileManager);
                try {
                    return getLogEntryFromLogSource(
                        lsn, lastLoggedSize, fileSource, true,
                        invisibleReadAllowed);
                } finally {
                    fileSource.release();
                }
            }

        } catch (ChecksumException e) {
            /*
             * Error was detected in a buffer that was written to the file.
             * This is media corruption and is handled specially.
             *
             * WARNING: EFE with LOG_CHECKSUM indicates a persistent corruption
             * and therefore LogSource.release must not be called until after
             * invalidating the environment (in the finally below). The buffer
             * latch prevents the corrupt buffer from being logged by another
             * thread.
             */
            throw VerifierUtils.handleChecksumError(lsn, e, envImpl);

        } catch (Error e) {
            envImpl.invalidate(e);
            throw e;

        } finally {
            if (logSource != null) {
                logSource.release();
            }
        }
    }

    public LogEntry getLogEntryHandleNotFound(long lsn, int lastLoggedSize)
        throws DatabaseException {

        try {
            return getLogEntry(lsn, lastLoggedSize);
        } catch (FileNotFoundException e) {
            throw new EnvironmentFailureException(
                envImpl,
                EnvironmentFailureReason.LOG_FILE_NOT_FOUND, e);
        } catch (ErasedException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM,
                "Entry is erased unexpectedly, implied corruption", e);
        }
    }

    public WholeEntry getWholeLogEntryHandleNotFound(long lsn,
                                                     int lastLoggedSize)
        throws DatabaseException {

        try {
            return getWholeLogEntry(lsn, lastLoggedSize);
        } catch (FileNotFoundException e) {
            throw new EnvironmentFailureException
                (envImpl,
                    EnvironmentFailureReason.LOG_FILE_NOT_FOUND, e);
        } catch (ErasedException e) {
            throw new EnvironmentFailureException(
                envImpl, EnvironmentFailureReason.LOG_CHECKSUM,
                "Entry is erased unexpectedly, implied corruption", e);
        }
    }

    /**
     * Throws ChecksumException rather than translating it to
     * EnvironmentFailureException and invalidating the environment.  Used
     * instead of getLogEntry when a ChecksumException is handled specially.
     */
    LogEntry getLogEntryAllowChecksumException(long lsn, int lastLoggedSize)
        throws ChecksumException, FileNotFoundException, ErasedException,
                DatabaseException {

        final LogSource logSource = getLogSource(lsn);

        try {
            return getLogEntryFromLogSource(
                lsn, lastLoggedSize, logSource, true, false).getEntry();
        } finally {
            logSource.release();
        }
    }

    /**
     * Always reads from a file (not the log buffers). May throw
     * ChecksumException (does not handle it here).
     */
    LogEntry getLogEntryFromFile(long lsn,
                                 int lastLoggedSize,
                                 RandomAccessFile file,
                                 boolean useThreadLocalBuffer,
                                 int logVersion)
        throws ChecksumException, ErasedException, DatabaseException {

        final LogSource logSource = new FileSource(
            file, fileManager, DbLsn.getFileNumber(lsn), logVersion);

        try {
            return getLogEntryFromLogSource(
                lsn, lastLoggedSize, logSource, useThreadLocalBuffer, false).
                getEntry();
        } finally {
            logSource.release();
        }
    }

    /**
     * Gets log entry from the given source. The caller is responsible for
     * calling logSource.release and handling ChecksumException and
     * ErasedException.
     *
     * <p>This method normally reads no more than the actual size of the
     * log entry by using the lastLoggedSize as the read size. The
     * lastLoggedSize param may be one of the following:</p>
     * <ul>
     *   <li>GT zero : This indicates the the entry size is known and will be
     *   used for a sized read. If incorrect and smaller than the actual size,
     *   two reads will be performed.</li>
     *
     *   <li>Zero : This indicates that the size was obtained from the parent
     *   IN but is missing because the parent IN is log format 20 or earlier.
     *   Two reads are performed. An assertion fires if the log version of the
     *   entry is 21 or greater.</li>
     *
     *   <li>-1 : This indicates that the log size is unknown and that two
     *   reads are acceptable. This value is passed for log entries that are
     *   infrequently read such as root INs, when reading LNs during undo and
     *   rollback, and in tests.</li>
     * </ul>
     *
     * <p>If two reads are necessary because the lastLoggedSize is not
     * available or inaccurate, the
     * {@link EnvironmentStats#getNRepeatFaultReads()} stat is incremented.
     * If lastLoggedSize is not available, the first read is very small and
     * is used to get the entry header, and the second read uses the size in
     * the header to read the rest of the entry.</p>
     *
     * <p>Even when lastLoggedSize is available, we do not assume that it
     * is always accurate, because this is not currently guaranteed in corner
     * cases such as transaction aborts. We do the initial read with
     * lastLoggedSize. If lastLoggedSize is larger than the actual size, we
     * will simply read more bytes than needed. If lastLoggedSize is smaller
     * than the actual size, we will do a repeat-read.</p>
     *
     * <p>If the thread local buffer is too small (configured using
     * {@link com.sleepycat.je.EnvironmentConfig#LOG_FAULT_READ_SIZE}),
     * a temporary buffer is allocated here and the
     * {@link EnvironmentStats#getNItemBufferTooSmall()} stat is
     * incremented.</p>
     *
     * @param lsn location of the entry in the log.
     *
     * @param lastLoggedSize is GT zero to use this size for the read; see
     * above for details.
     *
     * @param logSource the file or log buffer containing the entry.
     *
     * @param useThreadLocalBuffer is normally true, but false is passed in
     * special cases such as when reading the FileHeader, which can occur
     * during this method call.
     *
     * @param invisibleReadAllowed if true, we will permit the read of invisible
     * log entries, and we will adjust the invisible bit so that the checksum
     * will validate.
     *
     * @return log entry that embodies all the objects in the log entry
     */
    WholeEntry getLogEntryFromLogSource(long lsn,
                                        int lastLoggedSize,
                                        LogSource logSource,
                                        boolean useThreadLocalBuffer,
                                        boolean invisibleReadAllowed)
        throws ChecksumException, ErasedException, DatabaseException {

        final ByteBuffer localBuffer =
            (useThreadLocalBuffer && threadItemBuffer != null) ?
            threadItemBuffer.get() : null;

        final long fileOffset = DbLsn.getFileOffset(lsn);

        ByteBuffer entryBuffer = logSource.getBytes(
            fileOffset,
            (lastLoggedSize > 0)
                ? lastLoggedSize
                : LogEntryHeader.MAX_HEADER_SIZE,
            localBuffer);

        if (entryBuffer.remaining() < LogEntryHeader.MIN_HEADER_SIZE) {
            throw new ChecksumException(
                "Incomplete log entry header in " + logSource +
                " needed=" + LogEntryHeader.MIN_HEADER_SIZE +
                " remaining=" + entryBuffer.remaining() +
                " lsn=" + DbLsn.getNoFormatString(lsn));
        }

        /* Read the fixed length portion of the header. */
        final LogEntryHeader header = new LogEntryHeader(
            entryBuffer, logSource.getLogVersion(), lsn);

        /* Read the variable length portion of the header. */
        if (header.isVariableLength()) {
            if (entryBuffer.remaining() <
                header.getVariablePortionSize()) {
                throw new ChecksumException(
                    "Incomplete log entry header in " + logSource +
                    " needed=" + header.getVariablePortionSize() +
                    " remaining=" + entryBuffer.remaining() +
                    " lsn=" + DbLsn.getNoFormatString(lsn));
            }
            header.readVariablePortion(envImpl, entryBuffer);
        }

        final ChecksumValidator validator;
        if (header.hasChecksum() && doChecksumOnRead) {
            int itemStart = entryBuffer.position();

            /*
             * We're about to read an invisible log entry, which has knowingly
             * been left on disk with a bad checksum. Flip the invisible bit in
             * the backing byte buffer now, so the checksum will be valid. The
             * LogEntryHeader object itself still has the invisible bit set,
             * which is useful for debugging.
             */
            if (header.isInvisible()) {
                LogEntryHeader.turnOffInvisible
                    (entryBuffer, itemStart - header.getSize());
            }

            /* Add header to checksum bytes */
            validator = new ChecksumValidator(envImpl);
            final int headerSizeMinusChecksum = header.getSizeMinusChecksum();
            entryBuffer.position(itemStart -
                                 headerSizeMinusChecksum);
            validator.update(entryBuffer, headerSizeMinusChecksum);
            entryBuffer.position(itemStart);
        } else {
            validator = null;
        }

        /*
         * Now that we know the size, read the rest of the entry if the first
         * read didn't get enough.
         */
        final int itemSize = header.getItemSize();
        if (entryBuffer.remaining() < itemSize) {
            entryBuffer = logSource.getBytes(
                fileOffset + header.getSize(), itemSize, localBuffer);
            if (entryBuffer.remaining() < itemSize) {
                throw new ChecksumException(
                    "Incomplete log entry item in " + logSource +
                    " needed=" + itemSize +
                    " remaining=" + entryBuffer.remaining() +
                    " lsn=" + DbLsn.getNoFormatString(lsn));
            }
            nRepeatFaultReads.increment();
        }

        if (localBuffer != null && header.getEntrySize() > itemBufferSize) {
            nItemBufTooSmall.increment();
        }

        /*
         * Do entry validation. Run checksum before checking the entry type, it
         * will be the more encompassing error.
         */
        if (validator != null) {
            /* Check the checksum first. */
            validator.update(entryBuffer, itemSize);
            validator.validate(header.getChecksum(), lsn, header);
        }

        /*
         * If invisibleReadAllowed == false, we should not be fetching an
         * invisible log entry.
         */
        if (header.isInvisible() && !invisibleReadAllowed) {
            throw new EnvironmentFailureException
                (envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                 "Read invisible log entry at " +
                 DbLsn.getNoFormatString(lsn) + " " + header);
        }

        /* Disallow zero lastLoggedSize. */
        assert !(header.getVersion() >= 21 && lastLoggedSize == 0) :
            "lastLoggedSize is zero";

        assert LogEntryType.isValidType(header.getType()):
            "Read non-valid log entry type: " + header.getType();

        /* Read the entry. */
        final LogEntryType type = LogEntryType.findType(header.getType());
        final LogEntry logEntry = type.getNewLogEntry();
        logEntry.readEntry(envImpl, header, entryBuffer);

        /* For testing only; generate a read io exception. */
        if (readHook != null) {
            try {
                readHook.doIOHook();
            } catch (IOException e) {
                /* Simulate what the FileManager would do. */
                throw new EnvironmentFailureException
                    (envImpl, EnvironmentFailureReason.LOG_READ, e);
            }
        }

        /* Fetching an erased entry is handled via a checked exception. */
        if (header.isErased()) {
            throw new ErasedException(lsn, header);
        }

        return new WholeEntry(header, logEntry);
    }

    /**
     * Fault in the first object in the log entry log entry at this LSN.
     * @param lsn location of object in log
     * @return the object in the log
     */
    public Object getEntry(long lsn, int lastLoggedSize)
        throws FileNotFoundException, ErasedException, DatabaseException {

        LogEntry entry = getLogEntry(lsn, lastLoggedSize);
        return entry.getMainItem();
    }

    public Object getEntryHandleNotFound(long lsn, int lastLoggedSize) {
        LogEntry entry = getLogEntryHandleNotFound(lsn, lastLoggedSize);
        return entry.getMainItem();
    }

    /**
     * Find the LSN, whether in a file or still in the log buffers.
     * Is public for unit testing.
     */
    public LogSource getLogSource(long lsn)
        throws FileNotFoundException, ChecksumException, DatabaseException {

        /*
         * First look in log to see if this LSN is still in memory.
         */
        LogBuffer logBuffer = logBufferPool.getReadBufferByLsn(lsn);

        if (logBuffer == null) {
            try {
                /* Not in the in-memory log -- read it off disk. */
                long fileNum = DbLsn.getFileNumber(lsn);
                return new FileHandleSource(
                    fileManager.getFileHandle(fileNum), fileManager);
            } catch (DatabaseException e) {
                /* Add LSN to exception message. */
                e.addErrorMessage("lsn= " + DbLsn.getNoFormatString(lsn));
                throw e;
            }
        }
        return logBuffer;
    }

    /**
     * Return a log buffer locked for reading, or null if no log buffer
     * holds this LSN location.
     */
    public LogBuffer getReadBufferByLsn(long lsn) {

        assert DbLsn.getFileOffset(lsn) != 0 :
             "Read of lsn " + DbLsn.getNoFormatString(lsn)  +
            " is illegal because file header entry is not in the log buffer";

        return logBufferPool.getReadBufferByLsn(lsn);
    }

    /**
     * Flush all log entries to the log and perform an fsync.
     */
    public long flushSync()
        throws DatabaseException {

        if (readOnly) {
            return 0;
        }

        /* The write queue is flushed by syncLogEnd. */
        flushInternal(false /*flushWriteQueue*/);
        return fileManager.syncLogEnd();
    }

    /**
     * Flush all log entries to the log but do not fsync.
     */
    public void flushNoSync()
        throws DatabaseException {

        if (readOnly) {
            return;
        }

        flushInternal(true /*flushWriteQueue*/);
    }

    /**
     * Flush log buffers, but do not flush the write queue. This is used only
     * by FsyncManager, just prior to an fsync. When FsyncManager performs the
     * fsync, the write queue will be flushed by FileManager.fsyncLogEnd.
     */
    void flushBeforeSync()
        throws DatabaseException {

        if (readOnly) {
            return;
        }

        flushInternal(false /*flushWriteQueue*/);
    }

    /**
     * Flush the dirty log buffers, and optionally the write queue as well.
     *
     * Flushing logically means flushing all write buffers to the file system,
     * so flushWriteQueue should be false only when this method is called just
     * before an fsync (FileManager.syncLogEnd will flush the write queue).
     */
    private void flushInternal(boolean flushWriteQueue)
        throws DatabaseException {

        assert !readOnly;

        /*
         * If we cannot bump the current buffer because there are no
         * free buffers, the only recourse is to write all buffers
         * under the LWL.
         */
        synchronized (logWriteMutex) {
            if (!logBufferPool.bumpCurrent(0)) {
                logBufferPool.bumpAndWriteDirty(0, flushWriteQueue);
                return;
            }
        }

        /*
         * We bumped the current buffer but did not write any buffers above.
         * Write the dirty buffers now.  Hopefully this is the common case.
         */
        logBufferPool.writeDirty(flushWriteQueue);
    }

    public StatGroup loadStats(StatsConfig config)
        throws DatabaseException {

        endOfLog.set(fileManager.getLastUsedLsn());

        StatGroup copyStats = stats.cloneGroup(config.getClear());
        copyStats.addAll(logBufferPool.loadStats(config));
        copyStats.addAll(fileManager.loadStats(config));
        copyStats.addAll(grpManager.loadStats(config));

        return copyStats;
    }

    /**
     * Return the current number of cache misses in a lightweight fashion,
     * without incurring the cost of loading all the stats, and without
     * clearing any stats.
     */
    public long getNCacheMiss() {
        return logBufferPool.getNCacheMiss();
    }

    /**
     * For unit testing.
     */
    StatGroup getBufferPoolLatchStats() {
        return logBufferPool.getBufferPoolLatchStats();
    }

    /**
     * Returns a tracked summary for the given file which will not be flushed.
     */
    public TrackedFileSummary getUnflushableTrackedSummary(long file) {
        synchronized (logWriteMutex) {
            return envImpl.getUtilizationTracker().
                    getUnflushableTrackedSummary(file);
        }
    }

    /**
     * Removes the tracked summary for the given file.
     */
    public void removeTrackedFile(TrackedFileSummary tfs) {
        synchronized (logWriteMutex) {
            tfs.reset();
        }
    }

    private void updateObsolete(LogParams params) {

        if (params.packedObsoleteInfo == null &&
            params.obsoleteWriteLockInfo == null) {
            return;
        }

        final UtilizationTracker tracker = envImpl.getUtilizationTracker();

        synchronized (logWriteMutex) {

            /* Count other obsolete info under the log write latch. */
            if (params.packedObsoleteInfo != null) {
                params.packedObsoleteInfo.countObsoleteInfo(tracker);
            }

            if (params.obsoleteWriteLockInfo != null) {
                for (WriteLockInfo info : params.obsoleteWriteLockInfo) {
                    tracker.countObsoleteNode(info.getAbortLsn(),
                                              null /*type*/,
                                              info.getAbortLogSize());
                }
            }
        }
    }

    /**
     * Count node as obsolete under the log write latch.  This is done here
     * because the log write latch is managed here, and all utilization
     * counting must be performed under the log write latch.
     */
    public void countObsoleteNode(long lsn,
                                  LogEntryType type,
                                  int size,
                                  boolean countExact) {
        synchronized (logWriteMutex) {
            UtilizationTracker tracker = envImpl.getUtilizationTracker();
            if (countExact) {
                tracker.countObsoleteNode(lsn, type, size);
            } else {
                tracker.countObsoleteNodeInexact(lsn, type, size);
            }
        }
    }

    /**
     * A flavor of countObsoleteNode which does not fire an assert if the
     * offset has already been counted. Called through the LogManager so that
     * this incidence of all utilization counting can be performed under the
     * log write latch.
     */
    public void countObsoleteNodeDupsAllowed(long lsn,
                                              LogEntryType type,
                                              int size) {
        synchronized (logWriteMutex) {
            UtilizationTracker tracker = envImpl.getUtilizationTracker();
            tracker.countObsoleteNodeDupsAllowed(lsn, type, size);
        }
    }

    /**
     * @see LocalUtilizationTracker#transferToUtilizationTracker
     */
    public void transferToUtilizationTracker(LocalUtilizationTracker
                                             localTracker)
        throws DatabaseException {
        synchronized (logWriteMutex) {
            UtilizationTracker tracker = envImpl.getUtilizationTracker();
            localTracker.transferToUtilizationTracker(tracker);
        }
    }

    public void incRepeatIteratorReads() {
        nRepeatIteratorReads.increment();
    }

    /* For unit testing only. */
    public void setReadHook(TestHook<?> hook) {
        readHook = hook;
    }

    /* For unit testing only. */
    public void setDelayVLSNRegisterHook(TestHook<Object> hook) {
        delayVLSNRegisterHook = hook;
    }

    /* For unit testing only. */
    public void setFlushLogHook(TestHook<Object> hook) {
        flushHook = hook;
        grpManager.setFlushLogHook(hook);
    }
}
