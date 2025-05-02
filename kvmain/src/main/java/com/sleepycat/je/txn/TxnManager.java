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

import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ABORTS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ACTIVE;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_ACTIVE_TXNS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_BEGINS;
import static com.sleepycat.je.dbi.TxnStatDefinition.TXN_COMMITS;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.TransactionStats;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.LatchFactory;
import com.sleepycat.je.latch.SharedLatch;
import com.sleepycat.je.rep.impl.node.Replay;
import com.sleepycat.je.utilint.ActiveTxnArrayStat;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.SimpleTxnMap;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Class to manage transactions.  Basically a Set of all transactions with add
 * and remove methods and a latch around the set.
 */
public class TxnManager {

    /*
     * All NullTxns share the same id so as not to eat from the id number
     * space.
     *
     * Negative transaction ids are used by the master node of a replication
     * group. That sequence begins at -10 to avoid conflict with the
     * NULL_TXN_ID and leave room for other special purpose ids.
     */
    static final long NULL_TXN_ID = -1;
    private static final long FIRST_NEGATIVE_ID = -10;
    private LockManager lockManager;
    private final EnvironmentImpl envImpl;

    /* We depend that the txn Latch is held before we acquire the allTxnsLatch
     * to avoid the deadlock by acquiring them in reverse order. above ordering 
     * needs to be followed when using this latch.
     */ 
    private final SharedLatch allTxnsLatch;
    private final SimpleTxnMap<Txn> allTxns;

    private AtomicReference<GroupCommit> groupCommit;

    /*
     * Positive and negative transaction ids are used in a replicated system,
     * to let replicated transactions intermingle with local transactions.
     */
    private final AtomicLong lastUsedLocalTxnId;
    private final AtomicLong lastUsedReplicatedTxnId;

    /* Locker Stats */
    private final StatGroup stats;
    private final IntStat nActive;
    private final LongStat numBegins;
    private final LongStat numCommits;
    private final LongStat numAborts;
    private final ActiveTxnArrayStat activeTxns;
    private volatile long nTotalCommits = 0;

    public TxnManager(EnvironmentImpl envImpl) {
        lockManager = new SyncedLockManager(envImpl);

        if (envImpl.isNoLocking()) {
            lockManager = new DummyLockManager(envImpl, lockManager);
        }

        this.envImpl = envImpl;
        allTxnsLatch = LatchFactory.createSharedLatch(
            envImpl, "TxnManager.allTxns", false /*exclusiveOnly*/);
        allTxns = new SimpleTxnMap<>(1024);

        lastUsedLocalTxnId = new AtomicLong(0);
        lastUsedReplicatedTxnId = new AtomicLong(FIRST_NEGATIVE_ID);
        groupCommit = new AtomicReference<>();

        /* Do the stats definition. */
        stats = new StatGroup("Transaction", "Transaction statistics");
        nActive = new IntStat(stats, TXN_ACTIVE);
        numBegins = new LongStat(stats, TXN_BEGINS);
        numCommits = new LongStat(stats, TXN_COMMITS);
        numAborts = new LongStat(stats, TXN_ABORTS);
        activeTxns = new ActiveTxnArrayStat(stats, TXN_ACTIVE_TXNS);
    }

    /**
     * Set the txn id sequence.
     */
    public void setLastTxnId(long lastReplicatedTxnId, long lastLocalId) {
        lastUsedReplicatedTxnId.set(lastReplicatedTxnId);
        lastUsedLocalTxnId.set(lastLocalId);
    }

    /**
     * Get the last used id, for checkpoint info.
     */
    public long getLastLocalTxnId() {
        return lastUsedLocalTxnId.get();
    }

    public long getLastReplicatedTxnId() {
        return lastUsedReplicatedTxnId.get();
    }

    public long getNextReplicatedTxnId() {
        return lastUsedReplicatedTxnId.decrementAndGet();
    }

    /* @return true if this id is for a replicated txn. */
    public static boolean isReplicatedTxn(long txnId) {
        return (txnId <= FIRST_NEGATIVE_ID);
    }

    /**
     * Get the next transaction id for a non-replicated transaction. Note
     * than in the future, a replicated node could conceivable issue an
     * application level, non-replicated transaction.
     */
    long getNextTxnId() {
        return lastUsedLocalTxnId.incrementAndGet();
    }

    /*
     * Tracks the lowest replicated transaction id used during a replay of the
     * replication stream, so that it's available as the starting point if this
     * replica transitions to being the master.
     */
    public void updateFromReplay(long replayTxnId) {
        assert !envImpl.isMaster();

        if (!isReplicatedTxn(replayTxnId)) {
            throw EnvironmentFailureException.unexpectedState(
                "Replay txn id is not a replicated id " + replayTxnId);
        }

        if (replayTxnId < lastUsedReplicatedTxnId.get()) {
            lastUsedReplicatedTxnId.set(replayTxnId);
        }
    }

    /**
     * Returns commit counter that is never cleared (as stats are), so it can
     * be used for monitoring un-flushed txns.
     */
    public long getNTotalCommits() {
        return nTotalCommits;
    }

    /**
     * Create a new transaction.
     * @param parent for nested transactions, not yet supported
     * @param txnConfig specifies txn attributes
     * @return the new txn
     */
    public Txn txnBegin(Transaction parent, TransactionConfig txnConfig)
        throws DatabaseException {

        return Txn.createUserTxn(envImpl, txnConfig);
    }

    /**
     * Give transactions and environment access to lock manager.
     */
    public LockManager getLockManager() {
        return lockManager;
    }

    /**
     * Called when txn is created.
     */
    public void registerTxn(Txn txn) {
        allTxnsLatch.acquireShared();
        try {
            allTxns.put(txn);
            numBegins.increment();
        } finally {
            allTxnsLatch.release();
        }
    }

    /**
     * Called when txn ends.
     */
    void unRegisterTxn(Txn txn, boolean isCommit) {
        allTxnsLatch.acquireShared();
        try {
            long id = txn.getId();
            Txn currentTxn = allTxns.get(id);
            /*
             * MasterTxn set to MUST_ABORT can accidently replace the
             * ReplayTxn that replaced it.  KVSTORE-2124
             */
            if (currentTxn == null || currentTxn == txn) {
                allTxns.remove(id);
            }

            /* Remove any accumulated MemoryBudget delta for the Txn. */
            envImpl.getMemoryBudget().
                updateTxnMemoryUsage(0 - txn.getBudgetedMemorySize());
            if (isCommit) {
                numCommits.increment();
                nTotalCommits += 1;
            } else {
                numAborts.increment();
            }
        } finally {
            allTxnsLatch.release();
        }
    }

    public GroupCommit getGroupCommit () {
        return groupCommit.get();
    }

    public void setupGroupCommitMaster() {
        this.groupCommit.set(new GroupCommitMaster(envImpl));
    }

    public void setupGroupCommitReplica(Replay replay) {
        this.groupCommit.set(new GroupCommitReplica(envImpl, replay));
    }

    /**
     * Get the earliest LSN of all the active transactions, for checkpoint.
     * Returns NULL_LSN is no transaction is currently active.
     */
    public long getFirstActiveLsn() {

        /*
         * Note that the latching hierarchy calls for synchronizing on
         * allTxns first, hence dont synchronize on the individual txns as 
         * there are few places we are first synchronizing on the txn latch and
         * synchronizing on the allTxns. @see Replica#replicaTransitionCleanup.
         */
        long firstActive = DbLsn.NULL_LSN;
        allTxnsLatch.acquireExclusive();
        try {
            for (Txn txn : allTxns.getMap().values()) {
                long txnFirstActive = txn.getFirstActiveLsn();
                if (firstActive == DbLsn.NULL_LSN) {
                    firstActive = txnFirstActive;
                } else if (txnFirstActive != DbLsn.NULL_LSN) {
                    if (DbLsn.compareTo(txnFirstActive, firstActive) < 0) {
                        firstActive = txnFirstActive;
                    }
                }
            }
        } finally {
            allTxnsLatch.release();
        }

        return firstActive;
    }

    /*
     * Statistics
     */

    /**
     * Collect transaction related stats.
     */
    public TransactionStats txnStat(StatsConfig config) {
        TransactionStats txnStats = null;
        allTxnsLatch.acquireShared();
        try {
            nActive.set(allTxns.size());
            TransactionStats.Active[] activeSet =
                new TransactionStats.Active[nActive.get()];
            Iterator<Txn> iter = allTxns.getMap().values().iterator();
            int i = 0;
            while (iter.hasNext() && i < activeSet.length) {
                Locker txn = iter.next();
                activeSet[i] = new TransactionStats.Active
                    (txn.toString(), txn.getId(), 0);
                i++;
            }
            activeTxns.set(activeSet);
            txnStats = new TransactionStats(stats.cloneGroup(false));
            if (config.getClear()) {
                numCommits.clear();
                numAborts.clear();
            }
        } finally {
            allTxnsLatch.release();
        }

        return txnStats;
    }

    public StatGroup loadStats(StatsConfig config) {
        return lockManager.loadStats(config);
    }

    /**
     * Examine the transaction set and return Txn that match the class or are
     * subclasses of that class. This method is used to obtain Master and Replay
     * TXns for HA
     */
    public <T extends Txn> Set<T> getTxns(Class<T> txnClass) {
        final Set<T> targetSet = new HashSet<>();

        allTxnsLatch.acquireShared();
        try {
            Collection<Txn> all = allTxns.getMap().values();
            for (Txn t: all) {
                if (txnClass.isAssignableFrom(t.getClass())) {
                    @SuppressWarnings("unchecked")
                    final T t2 = (T)t;
                    targetSet.add(t2);
                }
            }
        } finally {
            allTxnsLatch.release();
        }

        return targetSet;
    }
}
