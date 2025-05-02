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

import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.ACK_TXN_AVG_NS;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.LAST_COMMIT_TIMESTAMP;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.LAST_COMMIT_VLSN;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.LOCAL_TXN_AVG_NS;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TXNS_ACKED;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TXNS_ASYNC_ACKED;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.TXNS_NOT_ACKED;
import static com.sleepycat.je.rep.stream.FeederTxnStatDefinition.VLSN_RATE;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.AsyncAckHandler;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.node.DurabilityQuorum;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.txn.GroupCommit;
import com.sleepycat.je.txn.GroupCommitMaster;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.AtomicLongStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongAvgRateStat;
import com.sleepycat.je.utilint.LongAvgStat;
import com.sleepycat.je.utilint.NoClearAtomicLongStat;
import com.sleepycat.je.utilint.SimpleTxnMap;
import com.sleepycat.je.utilint.StatGroup;

/**
 * FeederTxns manages transactions that need acknowledgments.
 *
 * <p>The lastCommitVLSN, lastCommitTimestamp, and vlsnRate statistics provide
 * general information about committed transactions on the master, but are also
 * intended to be used programmatically along with other statistics for the
 * feeder to provide information about how up-to-date the replicas are.  See
 * the Feeder class for more details.
 */
public class FeederTxns {

    /** The moving average period in milliseconds */
    private static final long MOVING_AVG_PERIOD_MILLIS = 10000;

    /*
     * Tracks transactions that have not yet been acknowledged for the entire
     * replication node.
     */
    private final AckExpiringMap txnMap;

    /* Special one element cache used to track the last nulltxn */
    private volatile MasterTxn lastNullTxn;

    private final RepImpl repImpl;
    private final StatGroup statistics;
    private final AtomicLongStat txnsAcked;
    private final AtomicLongStat txnsAsyncAcked;
    private final AtomicLongStat txnsNotAcked;
    private final NoClearAtomicLongStat lastCommitVLSN;
    private final NoClearAtomicLongStat lastCommitTimestamp;
    private final LongAvgRateStat vlsnRate;
    private final LongAvgStat ackTxnAvgNs;
    private final LongAvgStat localTxnAvgNs;


    public FeederTxns(RepImpl repImpl) {
        // TODO: increase size of active TXN map for async and parameterize it.
        this.repImpl = repImpl;
        txnMap = new AckExpiringMap(1024);
        statistics = new StatGroup(FeederTxnStatDefinition.GROUP_NAME,
                                   FeederTxnStatDefinition.GROUP_DESC);
        txnsAcked = new AtomicLongStat(statistics, TXNS_ACKED);
        txnsAsyncAcked = new AtomicLongStat(statistics, TXNS_ASYNC_ACKED);
        txnsNotAcked = new AtomicLongStat(statistics, TXNS_NOT_ACKED);
        lastCommitVLSN =
            new NoClearAtomicLongStat(statistics, LAST_COMMIT_VLSN);
        lastCommitTimestamp =
            new NoClearAtomicLongStat(statistics, LAST_COMMIT_TIMESTAMP);
        vlsnRate = new LongAvgRateStat(
            statistics, VLSN_RATE, MOVING_AVG_PERIOD_MILLIS, TimeUnit.MINUTES);
        localTxnAvgNs = new LongAvgStat(statistics, LOCAL_TXN_AVG_NS);
        ackTxnAvgNs = new LongAvgStat(statistics, ACK_TXN_AVG_NS);
    }

    public void shutdown() {
        txnMap.shutdown();
    }

    public AtomicLongStat getLastCommitVLSN() {
        return lastCommitVLSN;
    }

    public AtomicLongStat getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    public LongAvgRateStat getVLSNRate() {
        return vlsnRate;
    }

    /**
     * Track the txn in the transaction map if it needs acknowledgments
     *
     * @param txn identifies the transaction.
     */
    public void setupForAcks(MasterTxn txn) {
        if (txn.getRequiredAckCount() == 0) {
            /* No acks called for, no setup needed. */
            return;
        }
        if (txn.isNullTxn()) {
            /* Add to one element cache */
            lastNullTxn = txn;
        } else {
            txnMap.put(txn);
        }
    }

    /**
     * Returns the transaction if it's waiting for acknowledgments. Returns
     * null otherwise.
     */
    public MasterTxn getAckTxn(long txnId) {
        final MasterTxn txn = lastNullTxn;

        return ((txn != null) && (txn.getId() == txnId)) ?
                txn :  txnMap.get(txnId);
    }

    public LongAvgStat getLocalTxnAvgNs() {
        return localTxnAvgNs;
    }

    public LongAvgStat getAckTxnAvgNs() {
        return ackTxnAvgNs;
    }

    /*
     * Clears any ack requirements associated with the transaction. It's
     * typically invoked on a transaction abort.
     */
    public void clearTransactionAcks(Txn txn) {
        txnMap.remove(txn.getId());
    }

    /**
     * Notes that an acknowledgment was received from a replica, count it down.
     * Invokes the async handler if one is registered and acknowledgments have
     * been satisfied.
     *
     * @param replica the replica node supplying the ack
     * @param txnId the locally committed transaction that was acknowledged.
     *
     * @return the MasterTxn associated with the txnId, if txnId needs an ack,
     * null otherwise
     */
    public MasterTxn noteReplicaAck(final RepNodeImpl replica,
                                    final long txnId) {
        final DurabilityQuorum durabilityQuorum =
            repImpl.getRepNode().getDurabilityQuorum();
        if (!durabilityQuorum.replicaAcksQualify(replica)) {
            return null;
        }
        final MasterTxn txn = getAckTxn(txnId);
        if (txn == null) {
            return null;
        }
        txn.countdownAck();
        if (!txn.usesAsyncAcks() || (txn.getPendingAcks() > 0)) {
            return txn;
        }
        invokeOnQuorumAcks(txn);
        return txn;
    }

    /**
     * Helper method to contain any exceptions handling resulting from
     * application-defined handler malfunctions.
     *
     * @param mtxn the transaction that was acknowledged.
     */
    private void invokeOnQuorumAcks(final MasterTxn mtxn) {

        final AsyncAckHandler asyncAckHandler =
            mtxn.getAsyncInvocationHandler();
        if (asyncAckHandler == null) {
            /* Already invoked. */
            return;
        }
        txnsAsyncAcked.increment();
        try {
            asyncAckHandler.onQuorumAcks(mtxn);
        } catch (Exception e) {
            final String msg = "Async handler invokeOnQuorumAcks:" +
                asyncAckHandler.getClass().getName() +
                " failed with exception:" + e.getClass().getName() +
                " for transaction:" + mtxn.getId();
            LoggerUtils.warning(repImpl.getLogger(), repImpl, msg);
        } finally {
            updateTxnStats(mtxn, true);
            txnMap.remove(mtxn.getId());
        }
    }

    /**
     * Helper method to contain any exceptions handling resulting from
     * application-defined handler malfunctions.
     *
     * @param mtxn the transaction that was acknowledged.
     * @param e the exception for which the handler is being invoked
     */
    private void invokeOnExceptionHandler(MasterTxn mtxn, Exception e) {
        final AsyncAckHandler asyncAckHandler =
            mtxn.getAsyncInvocationHandler();
        if (asyncAckHandler == null) {
            /* Already invoked. */
            return;
        }
        try {
            final String msg = "Async ack exception handler invoked for " +
                "txn:" + mtxn.getId() +
                " exception:" + e.getClass().getSimpleName() +
                " message:" + e.getMessage();
            LoggerUtils.warning(repImpl.getLogger(), repImpl, msg);
            asyncAckHandler.onException(mtxn, e);
        } catch (Exception ne) {
            final String msg = "Async onException handler:" +
                asyncAckHandler.getClass().getName() +
                " failed with exception:" + ne.getClass().getName() +
                " for transaction:" + mtxn.getId() +
                " msg:" + ne.getMessage() +
                " while handling exception:" + e.getClass().getName() +
                " msg:" + e.getMessage();
            LoggerUtils.warning(repImpl.getLogger(), repImpl, msg);
        } finally {
            updateTxnStats(mtxn, false);
            txnMap.remove(mtxn.getId());
        }
    }

    /**
     * Waits for the required number of replica acks to come through. If there
     * is an async ack handler, it will not wait but return immediately The
     * {@link AsyncAckHandler} callbacks will be invoked, as appropriate, on
     * success or failure.
     *
     * @param txn identifies the transaction to wait for.
     *
     * @param timeoutMs the amount of time to wait for the acknowledgments
     * before giving up.
     *
     * @throws InsufficientAcksException if the ack requirements were not met
     */
    public void awaitReplicaAcks(MasterTxn txn, int timeoutMs)
        throws InterruptedException {

        /* Record master commit information even if no acks are needed */
        final long vlsn = txn.getCommitVLSN();
        final long ackAwaitStartMs = TimeSupplier.currentTimeMillis();

        txn.startAckTimeouts(timeoutMs);
        lastCommitVLSN.set(vlsn);
        lastCommitTimestamp.set(ackAwaitStartMs);
        vlsnRate.add(vlsn, ackAwaitStartMs);

        if (txn.getRequiredAckCount() <= 0) {
            /* Transaction does need acks */
            return;
        }

        if (txn.isNullTxn()) {
            /* Treat a null txn as though it's not acked for stats purposes. */
            updateTxnStats(txn, false);
            return;
        }

        if (txn.usesAsyncAcks()) {
            /*
             * Don't wait, return immediately to app thread. Async handler will
             * be invoked instead.
             */
            return;
        }

        boolean gotAcks = false;
        try {
            gotAcks = txn.await(ackAwaitStartMs, timeoutMs);
        } finally {
            updateTxnStats(txn, gotAcks);
            txnMap.remove(txn.getId());
        }

        final RepNode repNode = repImpl.getRepNode();
        if (repNode != null) {
            repNode.getDurabilityQuorum().ensureSufficientAcks(txn);
        }
    }

    /* Update txn stats after receipt of sync or async acks. */
    private void updateTxnStats(MasterTxn txn,
                                final boolean gotAcks) {
        if (gotAcks) {
            txnsAcked.increment();
            final long nowNs = System.nanoTime();
            ackTxnAvgNs.add(nowNs -  txn.getStartNs());
        } else {
            txnsNotAcked.increment();
        }
    }

    public StatGroup getStats() {
        StatGroup ret = statistics.cloneGroup(false);
        GroupCommit groupCommit = repImpl.getTxnManager().getGroupCommit();
        if (groupCommit instanceof GroupCommitMaster) {
            ret.addAll(groupCommit.getStats(new StatsConfig()));
        }
        return ret;
    }

    public void resetStats() {
        statistics.clear();
        GroupCommit groupCommit = repImpl.getTxnManager().getGroupCommit();
        if (groupCommit != null) {
            groupCommit.clearStats();
        }
    }

    public StatGroup getStats(StatsConfig config) {

        StatGroup cloneStats = statistics.cloneGroup(config.getClear());
        GroupCommit groupCommit = repImpl.getTxnManager().getGroupCommit();
        if (groupCommit instanceof GroupCommitMaster) {
            cloneStats.addAll(groupCommit.getStats(config));
        }
        return cloneStats;
    }

    /**
     * A subclass of SimpleTxnMap that checks for master transactions that have
     * not received a quorum of replica acks within their ack timeout window
     * and need to signal an InsufficientAcksException back to the application.
     */
    private class AckExpiringMap extends SimpleTxnMap<MasterTxn> {
        private final Timer expiryTimer;
        final long periodNs;

        public AckExpiringMap(int arrayMapSize) {
            super(arrayMapSize);
            expiryTimer = new Timer("AsyncTxnExpiryThread", true);
            final int periodMs = repImpl.getConfigManager().
                getDuration(RepParams.ASYNC_ACK_TIMER_PERIOD);
            periodNs = TimeUnit.MILLISECONDS.toNanos(periodMs);
            expiryTimer.schedule(new TxnExpiryTask(), 0, periodMs);
            LoggerUtils.info(repImpl.getLogger(), repImpl,
                             "Ack expiry timer period:" + periodNs + " ns");
        }

        void shutdown() {
            expiryTimer.cancel();

            /* Any txns waiting for async acks should throw IAE */
            for (Txn t : filterTxns((t) ->
                         ((MasterTxn)t).ackTimedOut(Long.MAX_VALUE))) {
               final MasterTxn mt = (MasterTxn)t;
               if (!mt.usesAsyncAcks()) {
                   continue;
               }
               final InsufficientAcksException iae =
                   new InsufficientAcksException(mt, "Feeder terminated");
               invokeOnExceptionHandler(mt, iae);
               LoggerUtils.info(repImpl.getLogger(), repImpl,
                                "Transaction:" +  t.getId() +
                                " state:" + t.getState() +
                                " prematurely terminated with IAE");
            }
        }

        /**
         * It's important that this task interfere minimally with the
         * ongoing operations against the underlying map and minimize
         * locking or potentially blocking operations.
         */
        private class TxnExpiryTask extends TimerTask {
            @Override
            public void run() {
                final RepNode repNode = repImpl.getRepNode();

                if (repNode == null) {
                    return;
                }
                /**
                 * It's ok for the "now" time to be slightly inaccurate. in
                 * order to minimize nanoTime() calls.
                 */
                final long nowNs = System.nanoTime() + periodNs;

                for (Txn t :
                     filterTxns((t) ->
                                ((MasterTxn)t).ackTimedOut(nowNs))) {

                    final MasterTxn mt = (MasterTxn)t;
                    if (!mt.usesAsyncAcks()) {
                        continue;
                    }
                    try {
                        repNode.getDurabilityQuorum().ensureSufficientAcks(mt);
                        /*
                         * Expected IAE. Race with an ack, or arbiter activated
                         */
                        invokeOnQuorumAcks(mt);
                    } catch (InsufficientAcksException e) {
                        /* IAE expected. */
                        invokeOnExceptionHandler(mt, e);
                    } catch (Exception e) {
                        /* Unexpected, log it.  */
                        LoggerUtils.
                        warning(repImpl.getLogger(), repImpl,
                                " Expected IAE for txn:" +
                                    mt.getTransactionId() +
                                    " not exception:" +
                                    e.getClass().getName() +
                                    " message:" + e.getMessage());
                        invokeOnExceptionHandler(mt, e);
                    }
                }
            }
        }
    }
}
