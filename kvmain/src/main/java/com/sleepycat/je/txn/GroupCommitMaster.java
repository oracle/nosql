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

package com.sleepycat.je.txn;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.stream.FeederTxnStatDefinition;
import com.sleepycat.je.rep.stream.FeederTxns;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.utilint.AtomicLongStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

/**
 * The GroupCommitMaster class is responsible for managing transaction batching
 * and fsync operations when the node acts as the Master
 *
 * <p><b>Workflow:</b></p>
 * <ul>
 *   <li>When a transaction arrives, the GroupCommitMaster checks whether an fsync
 *       operation is currently in progress:
 *       <ul>
 *         <li>If an fsync is in progress: The transaction is added to a pending
 *             queue for inclusion in the next batch.</li>
 *         <li>If no fsync is in progress: The transaction is added to the current
 *             batch, and thresholds (time or size) are evaluated to determine whether
 *             to trigger an fsync.</li>
 *       </ul>
 *   </li>
 *   <li>If the txn has recently been assumed to be the leader:
 *       <ul>
 *         <li> The leader will sleep for a period of time(which is the
 *         time threshold), after waking up, it will decide whether to do a fsync or not.</li>
 *         <li>The leader may skip an immediate fsync if the durability of recent
 *             transactions has already been ensured by a prior fsync operation
 *             issue by a non-leader txn trigger by size threshold.</li>
 *       </ul>
 *   </li>
 *   <li>After the fsync completes, transactions in the batch are durable,
 *       and will send ack to the txn. Then the process continues
 *       with the next batch.</li>
 * </ul>
 */


public class GroupCommitMaster implements GroupCommit {

    private  final StatGroup statistics;
    private  final LongStat nGroupCommitTimeouts;
    private  final LongStat nGroupCommitMaxExceeded;
    private  final AtomicLongStat nGroupCommitTxns;
    private  final LongStat nGroupCommits;

    private final ConcurrentLinkedQueue<Long> pendingBuffer = new ConcurrentLinkedQueue<>();
    private final Object mutex = new Object();
    private final Object leaderMutex = new Object();

    private final AtomicBoolean fsyncInProgress = new AtomicBoolean(false);
    private final AtomicBoolean leaderExists = new AtomicBoolean(false);
    private final AtomicLong highestVLSNFsynced = new AtomicLong(NULL_VLSN);

    private final EnvironmentImpl envImpl;

    private final long groupCommitIntervalMs;
    private final int maxGroupCommit;
    private final long fsyncTimeout;

    private FeederTxns feederTxns;

    public GroupCommitMaster(EnvironmentImpl envImpl) {

        this.envImpl = envImpl;

        this.maxGroupCommit = envImpl.getConfigManager().
            getInt(RepParams.MASTER_MAX_GROUP_COMMIT);

        this.groupCommitIntervalMs = envImpl.getConfigManager().
            getDuration(RepParams.MASTER_GROUP_COMMIT_INTERVAL);

        fsyncTimeout = envImpl.getConfigManager().getDuration(
                EnvironmentParams.LOG_FSYNC_TIMEOUT);

        feederTxns = ((RepImpl)envImpl).getFeederTxns();

        statistics = new StatGroup("GroupCommit",
            "Related group commit stats");
        nGroupCommits = new LongStat(statistics, FeederTxnStatDefinition.N_MASTER_GROUP_COMMITS);
        nGroupCommitTxns = new AtomicLongStat(statistics, FeederTxnStatDefinition.N_MASTER_GROUP_COMMIT_TXNS);
        nGroupCommitMaxExceeded = new LongStat(statistics, FeederTxnStatDefinition.N_MASTER_GROUP_COMMIT_MAX_EXCEEDED);
        nGroupCommitTimeouts = new LongStat(statistics, FeederTxnStatDefinition.N_MASTER_GROUP_COMMIT_TIMEOUTS);
    }

    /**
     * Chances are that a txn can skip adding itself to groupCommit due to a
     * previous fsync.
     */
    public boolean canSkip(MasterTxn txn, long commitVLSN) {
        if (highestVLSNFsynced.get() >= commitVLSN) {
            txn.countdownAck();

            // This may not be accurate since the txn could be fsync by LogFLusher
            nGroupCommitTxns.increment();
            return true;
        }
        return false;
    }


    @Override
    public boolean isEnabled() {
        return maxGroupCommit != 0;
    }

    @Override
    public void bufferCommit(long nowNs, Txn txn,
                             long commitVLSN) {

        MasterTxn masterTxn = (MasterTxn)txn;

        while (!canSkip(masterTxn, commitVLSN)) {

            if (!fsyncInProgress.get()) {

                pendingBuffer.add(txn.getId());

                /*
                 * Txn will compete for leader, if succeeds, it will be responsible
                 * for next fsync.
                 */
                if (tryBecomeLeader()) {

                    fsyncAsLeader(nowNs, commitVLSN);

                } else if (pendingBuffer.size() >= maxGroupCommit) {

                    /*
                     * Failed to become leader but the size limit is triggered,
                     * so the txn should do a fsync even if it is not leader.
                     * Note that there could be multiple txns reaching this stage,
                     * but only one txn should do a fsync.
                     * Other txns can simply return, this is based on:
                     * As long as a txn comes in and finds there are no fsync in
                     * progress, it is for sure that next fsync issued will flush
                     * it, so it can return now.
                     */
                    if (fsyncInProgress.compareAndSet(false, true)) {
                        flushPendingAcks(nowNs);
                        nGroupCommits.increment();
                        nGroupCommitMaxExceeded.increment();
                    }
                }
                return;
            } else {
                waitForInProgressFsync();
                /*
                 * If the in progress fsync throws out an IOException, or a
                 * MasterTransfer happens, return.
                 */
                if (!envImpl.isValid() || txn.isFrozen()) {
                    return;
                }
            }
        }
    }

    private void waitForInProgressFsync() {
        /*
         * There is a fsync in progress, txns should wait here until the
         * fsync finishes.
         */
        synchronized (mutex) {
            if (fsyncInProgress.get()) {
                try {
                    mutex.wait(fsyncTimeout);
                } catch (InterruptedException e) {
                    throw new ThreadInterruptedException(envImpl,
                        "Unexpected interrupt while waiting " +
                        "for a in progress fsync in GroupCommitMaster", e);
                }
            }
        }
    }

    private void fsyncAsLeader(long nowNs, long commitVLSN) {

        /* nowNs is the time when entering bufferCommit(). */
        try {
            long curTime = System.nanoTime();
            long sleepTimeNs = TimeUnit.MILLISECONDS.toNanos(groupCommitIntervalMs)
                - (curTime - nowNs);
            if (sleepTimeNs > 0) {
                long sleepTimeMs = sleepTimeNs / 1_000_000;
                int nanos = (int) (sleepTimeNs % 1_000_000);
                synchronized (leaderMutex) {
                    leaderMutex.wait(sleepTimeMs, nanos);
                }
            }
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(envImpl,
                "Unexpected interrupt while sleeping " +
                "as a leader in GroupCommitMaster", e);
        }

        /*
         * Upon waking up, if there are no fsync in progress, leader
         * will decide whether to do a fsync here.
         */
        if (fsyncInProgress.compareAndSet(false, true)) {

            /*
             * While leader was sleeping, another txn could come in and
             * do a fsync since the size of the buffer exceeded
             * maxGroupCommit, hence leader can reset fsyncState and
             * return.
             * flushPendingAcks() will reset leaderState() after a
             * fsync is done, so leader doesn't need to reset
             * leaderState here.
             */
            if (this.highestVLSNFsynced.get() >= commitVLSN) {
                fsyncInProgress.set(false);
            } else {
                // leader is responsible for a fsync.
                flushPendingAcks(nowNs);
                nGroupCommitTimeouts.increment();
                nGroupCommits.increment();
            }
        } else {

            /*
             * There is a fsync call in progress, and the fsync is
             * issued after leader added to the buffer, so leader
             * will be benefit from the fsync, can just simply return.
             */
            leaderExists.set(false);
        }
    }

    private boolean tryBecomeLeader() {
        return leaderExists.compareAndSet(false, true);
    }

    @Override
    public void flushPendingAcks(long nowNs) {

        // notify the leader in sleep to return
        synchronized (leaderMutex) {
            leaderMutex.notify();
        }
        long highestVLSNFsynced;
        try {
            highestVLSNFsynced = envImpl.getLogManager().flushSync();
        } catch (DatabaseException e) {
            /* If fsync failed, wake up other txns that are waiting fsync to finish*/
            synchronized (mutex) {
                mutex.notifyAll();
            }
            throw e;
        }

        this.highestVLSNFsynced.accumulateAndGet(highestVLSNFsynced, (newValue, currentValue) -> {
            if (newValue >= currentValue) {
                return newValue;
            }
            return currentValue;
        });

        leaderExists.set(false);
        postFsyncTasks();
    }


    /**
     * Performs the post-fsync tasks after a batch of transactions has been successfully
     * flushed to disk. This method is responsible for:
     * <ul>
     *   <li>Notifying txns that have been waiting for an on-going fsync to
     *      finish.
     *   <li>Send ack(from the master) to the txns that just got fsynced to the disk</li>
     * </ul>
     */
    private void postFsyncTasks() {

        /*
         * Transactions arriving after fsyncState is set to FSYNC_IN_PROGRESS
         * will wait on the mutex. The recently completed fsync has covered all
         * transactions in the pendingBuffer.
         * Reset fsyncState only after all transactions in the pendingBuffer
         * have been processed.
         */
        nGroupCommitTxns.add(pendingBuffer.size());

        while (!pendingBuffer.isEmpty()) {
            MasterTxn ackTxn = feederTxns.getAckTxn(pendingBuffer.poll());
            if (ackTxn != null) {
                ackTxn.countdownAck();
            }
        }

        fsyncInProgress.set(false);

        // Wake up all txns that have been waiting on mutex
        synchronized (mutex) {
            mutex.notifyAll();
        }
    }

    @Override
    public StatGroup getStats(StatsConfig config) {
        return statistics.cloneGroup(config.getClear());
    }

    @Override
    public void clearStats() {
        statistics.clear();
    }
}
