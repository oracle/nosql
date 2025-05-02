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

import com.sleepycat.je.Durability;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.Replay;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.NanoTimeUtil;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHookExecute;

import java.io.IOException;

import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_GROUP_COMMITS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_GROUP_COMMIT_TXNS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_GROUP_COMMIT_TIMEOUTS;
import static com.sleepycat.je.rep.impl.node.ReplayStatDefinition.N_GROUP_COMMIT_MAX_EXCEEDED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Since replay is single threaded, the GroupCommitReplica mechanism works
 * differently in the replica than in the master. In the replica, SYNC
 * transactions are converted into NO_SYNC transactions and executed
 * immediately, and the actual fsync operation is delayed until after either
 * the REPLICA_GROUP_COMMIT_INTERVAL (the max amount the first transaction
 * in the group is delayed) has expired, or the size of the group (as
 * specified by REPLICA_MAX_GROUP_COMMIT) has been exceeded.
 */
public class GroupCommitReplica implements GroupCommit {

    private final StatGroup statistics;

    /* Size determines max fsync commits that can be grouped. */
    private final long[] pendingCommitAcks;

    /* Number of entries currently in pendingCommitAcks */
    private int nPendingAcks;

    /*
     * If this time limit is reached, the group will be forced to commit.
     * Invariant: nPendingAcks > 0 ==> limitGroupCommitNs > 0
     */
    private long limitGroupCommitNs = 0;

    /* The time interval that an open group is held back. */
    private final long groupCommitIntervalNs;

    private final LongStat nGroupCommitTimeouts;
    private final LongStat nGroupCommitMaxExceeded;
    private final LongStat nGroupCommits;
    private final LongStat nGroupCommitTxns;
    private final RepImpl envImpl;
    private final Replay replay;

    public GroupCommitReplica(EnvironmentImpl envImpl, Replay replay) {

        this.replay = replay;
        this.envImpl = (RepImpl) envImpl;
        statistics = new StatGroup("GroupCommitReplica",
            "Related group commit stats on replica side");

        DbConfigManager configManager = envImpl.getConfigManager();

        pendingCommitAcks = new long[configManager.
            getInt(RepParams.REPLICA_MAX_GROUP_COMMIT)];

        nPendingAcks = 0;

        final long groupCommitIntervalMs = configManager.
            getDuration(RepParams.REPLICA_GROUP_COMMIT_INTERVAL);

        groupCommitIntervalNs =
            NANOSECONDS.convert(groupCommitIntervalMs, MILLISECONDS);
        nGroupCommitTimeouts =
            new LongStat(statistics, N_GROUP_COMMIT_TIMEOUTS);

        nGroupCommitMaxExceeded =
            new LongStat(statistics, N_GROUP_COMMIT_MAX_EXCEEDED);

        nGroupCommitTxns =
            new LongStat(statistics, N_GROUP_COMMIT_TXNS);

        nGroupCommits =
            new LongStat(statistics, N_GROUP_COMMITS);
    }
    @Override
    public boolean isEnabled() {
        return pendingCommitAcks.length > 0;
    }

    /**
     * The interval used to poll for incoming log entries. The time is
     * lowered from the defaultNs time, if there are pending
     * acknowledgments.
     *
     * @param defaultNs the default poll interval
     *
     * @return the actual poll interval
     */
    public long getPollIntervalNs(long defaultNs) {
        if (nPendingAcks == 0) {
            return defaultNs;
        }
        final long now = System.nanoTime();

        final long interval = limitGroupCommitNs - now;
        return Math.min(interval, defaultNs);
    }

    /**
     * Buffers the acknowledgment if the commit calls for a sync, or if
     * there are pending acknowledgments to ensure that acks are sent
     * in order.
     *
     * @param nowNs the current time
     * @param ackTxn the txn associated with the ack
     * @param commitVLSN the commitVLSN of this txn
     */
    @Override
    public void bufferCommit(long nowNs,
                                 Txn ackTxn,
                                 long commitVLSN)
            throws DatabaseException {

        pendingCommitAcks[nPendingAcks++] = ackTxn.getId();

        if (nPendingAcks == 1) {
            /* First txn in group, start the clock. */
            limitGroupCommitNs = nowNs + groupCommitIntervalNs;
        } else {
            try {
                flushPendingAcks(nowNs);
            } catch (IOException e) {
                throw new ThreadInterruptedException(envImpl, e.getMessage(), e);
            }
        }
    }

    /**
     *  Used in Replay.replayEntry(), is used to determine whether to send out
     *  an acknowledgement for a transaction immediately
     *  or wait for the group commit to finish.
     * @param txnSyncPolicy SyncPolicy of the transaction
     * @return true if conditions are met to send out acknowledgement immediately
     */
    public boolean queueAck(Durability.SyncPolicy txnSyncPolicy) {
        if (!isEnabled() ||
            !((txnSyncPolicy == Durability.SyncPolicy.SYNC) ||
              (nPendingAcks > 0))) {
            return true;
        }
        return false;
    }

    /**
     * Flush if there are pending acks and either the buffer limit or the
     * group interval has been reached.
     *
     * @param nowNs the current time (passed in to minimize system calls)
     */
    @Override
    public void flushPendingAcks(long nowNs) throws IOException {
        assert TestHookExecute.doHookIfSet(Replay.softShutdownHook);
        if ((nPendingAcks == 0) ||
            ((nPendingAcks != pendingCommitAcks.length) &&
            (NanoTimeUtil.compare(nowNs, limitGroupCommitNs) < 0))) {

            return;
        }

        /* Update statistics. */
        nGroupCommits.increment();
        nGroupCommitTxns.add(nPendingAcks);
        if (NanoTimeUtil.compare(nowNs, limitGroupCommitNs) >= 0) {
            nGroupCommitTimeouts.increment();
        } else if (nPendingAcks >= pendingCommitAcks.length) {
            nGroupCommitMaxExceeded.increment();
        }
        /* flush log buffer and fsync to disk */
        envImpl.getLogManager().flushSync();

        /* commits are on disk, send out acknowledgments on the network. */
        for (int i=0; i < nPendingAcks; i++) {
            if (!replay.queueAck(pendingCommitAcks[i]) &&
                nowNs == Long.MAX_VALUE /*soft exit*/) {
                throw new EnvironmentFailureException(envImpl,
                    EnvironmentFailureReason.SHUTDOWN_REQUESTED,
                    "flushing pending acks failed as buffering " +
                    "acknowledgments to the queue to be sent to network failed");
            }
            pendingCommitAcks[i] = 0;
        }

        nPendingAcks = 0;
        limitGroupCommitNs = 0;
    }

    /* Only for unit test. */
    public void setPendingAcks(int value) {
        this.nPendingAcks = value;
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
