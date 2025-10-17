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

package com.sleepycat.je.rep.txn;

import static com.sleepycat.je.EnvironmentFailureException.unexpectedState;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.AsyncAckHandler;
import com.sleepycat.je.CommitToken;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.Transaction.State;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbCleanup;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.rep.AbsoluteConsistencyPolicy;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.MasterIdTerm;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.rep.impl.node.MasterTransfer;
import com.sleepycat.je.rep.impl.node.NodeState;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.impl.node.Replay;
import com.sleepycat.je.rep.impl.node.Replica;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnManager;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.NotSerializable;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/**
 * A MasterTxn represents:
 *  - a user initiated Txn executed on the Master node, when local-write and
 *    read-only are not configured, or
 *  - an auto-commit Txn on the Master node for a replicated DB.
 *
 * This class uses the hooks defined by Txn to support the durability
 * requirements of a replicated transaction on the Master.
 */
public class MasterTxn extends Txn {
    /* Holds the commit VLSN after a successful commit. */
    private long commitVLSN = NULL_VLSN;

    /* The number of acks required by this txn commit. */
    private int requiredAckCount = -1;

    /* If this transaction requests an Arbiter ack. */
    private boolean needsArbiterAck;

    /*
     * The latch used to track transaction acknowledgments, for this master
     * txn. The latch, if needed, is set up in the pre-commit hook.
     */
    private volatile Latch ackLatch;

    /* The start relative delta time when the commit pre hook exited. */
    private int preLogCommitEndDeltaMs = 0;

    /*
     * The start relative delta time when the commit message was written to
     * the rep stream.
     */
    private int repWriteStartDeltaMs = 0;

    /* Set to the System.nanotime the ack was sent out. */
    private volatile long ackWaitStartNs;

    /*
     * Ack waits are timed out when
     * (System.nanoTime() - ackWaitLimitNs) > 0
     */
    private volatile long ackWaitLimitNs;

    /**
     * A lock stamp associated with the lock RepImpl.blockLatchLock. It's set
     * to non-zero when the read lock is acquired and reset to zero when it's
     * released.
     */
    private long readLockStamp = 0;

    /**
     * Flag to prevent any change to the txn's contents. Used in
     * master->replica transition. Intentionally volatile, so it can be
     * interleaved with use of the MasterTxn mutex.
     */
    private volatile boolean freeze;

    /*
     * Supplies the non-null master id and term associated with this txn
     * commit/abort.
     */
    private final Supplier<MasterIdTerm> masterIdTermSupplier;

    /*
     * Non null for transactions whose acks will be handled asynchronously
     */
    private final AsyncAckHandler asyncAckHandler;

    /* Set to true after an async quorum has been achieved or the txn has
     * timed out. This iv is used exclusively by getAsyncInvocationHandler()
     *
     * Invariant: asyncHandled => (asyncAckHandler != null)
     */
    private boolean asyncHandled = false;

    /* For unit testing */
    private TestHook<Integer> convertHook;
    private TestHook<Integer> openDBHook;
    private TestHook<Integer> convertTxnHook;
    private boolean raceOnDBCleanUp;

    private final CountDownLatch testLatch = new CountDownLatch(1);

    /* Txn Hook invoked after all Txn commit processing has been completed. */
    private static volatile TestHook<MasterTxn> commitEpilogueHook;

    /* The default factory used to create MasterTxns */
    private static final MasterTxnFactory DEFAULT_FACTORY =
        new MasterTxnFactory() {

            @Override
            public MasterTxn create(EnvironmentImpl envImpl,
                                    TransactionConfig config) {
                return new MasterTxn(envImpl, config);
            }

            @Override
            public MasterTxn createNullTxn(EnvironmentImpl envImpl) {
                return new NullTxn(envImpl);
            }
    };

    /* The current Txn Factory. */
    private static MasterTxnFactory factory = DEFAULT_FACTORY;

    public MasterTxn(EnvironmentImpl envImpl, TransactionConfig config)
        throws DatabaseException {
       this(envImpl, config, config.getAsyncAckHandler());
   }

    /*
     * Internal, sideband constructor for tests so they can use async ack
     * handlers without explicit transaction configuration via
     * TransactionConfig.
     */
    public MasterTxn(EnvironmentImpl envImpl,
                     TransactionConfig config,
                     AsyncAckHandler asyncAckHandler)
        throws DatabaseException {

        super(envImpl, config, ReplicationContext.MASTER);
        startNs = System.nanoTime();
        this.asyncAckHandler = asyncAckHandler;
        masterIdTermSupplier =
            () -> {
                /*
                 * The following code obtains the masterIdTerm from NodeState.
                 * If that variable is null due to the node is not in the
                 * master state, the previous masterIdTerm when the node is in
                 * the master state will be obtained.
                 *
                 * We must guarantee any two consecutive
                 * masterIdTermSupplier.get() call returns increasing values.
                 *
                 * First some notations. Consider two supplier get calls.
                 * These two calls happen in the serialized code in the
                 * LogManger#serialLogWork and hence we can order the two calls
                 * as S1 and S2.  Ignoring the case of when PRIMODAL_ID_TERM is
                 * returned since that will trigger exception later when
                 * serializing the txn, the call will return two types of
                 * values: a current masterIdTerm (denoted a C value) when
                 * node.getMasterIdTerm() returns non-null or a previous one
                 * (denoted as P value). When the two call S1 and S2 returns
                 * different types of values, there must be two
                 * NodeState#changeAndNotify calls of which S1 and S2 reads
                 * from. Since changeAndNotify is synchronized, we can order
                 * the two calls as N1 and N2. We show the return value of S1
                 * and S2 must be increasing in the following four cases:
                 *
                 * - Both S1 and S2 return C values. Since
                 *   masterIdTerm in NodeState is always increasing, the
                 *   requirement is met.
                 * - Both S1 and S2 return P values. Since
                 *   latestThisMasterIdTerm in NodeState is always increasing,
                 *   the requirement is met.
                 * - S1 returns a C value and S2 returns a P value. This means
                 *   N1 transits the node into Master and N2 out of. The C
                 *   value is set in N1. The P value is set in N1 or a later
                 *   transition between N1 and N2. Hence the P value must be
                 *   equal to or greater than the C value.
                 * - S1 returns a P value and S2 returns a C value. This means
                 *   N1 transits the node out of the Master and N1 in. The P
                 *   value is set in a transition before N1. The C value is set
                 *   in N2. The C value must be greater than the P value.
                 */
                final NodeState nodeState = getRepImpl().getNodeState();
                MasterIdTerm masterIdTerm = nodeState.getMasterIdTerm();
                if (masterIdTerm == null) {
                    /*
                     * The current masterIdTerm is null, this means at the time
                     * when the term is needed, e.g., serializing to the log,
                     * the node is not a master any more. Use the Id and term of
                     * when this node is the master last time.
                     */
                    masterIdTerm = nodeState.getPreviousMasterIdTerm();
                }
                if (masterIdTerm == null) {
                    /*
                     * No previous master value. This should not happen, but if
                     * it did somehow, return a null MasterIdTerm.
                     */
                    masterIdTerm = MasterIdTerm.PRIMODAL_ID_TERM;
                }
                return masterIdTerm;
            };
        assert !config.getLocalWrite();
    }

    /**
     * Returns an information-rich string with details about the txn suitable
     * for use in logs and exception messages.
     */
    public String logString() {
        final long elapsedMs =
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        final String startTime =
            Instant.ofEpochMilli(getTxnStartMillis()).toString();
        return "txn id:" + getId() + " null txn:" + isNullTxn() +
            " start time:" + startTime +
            " elapsed ms:" + elapsedMs +
            " master term:" + MasterTerm.logString(masterIdTermSupplier.get().term) +
            " vlsn:" + getCommitVLSN() +
            " pendingAcks:" +
            ((ackLatch != null) ? ackLatch.getCount() : Integer.MIN_VALUE) +
            " freeze:" + freeze;
    }

    @Override
    public boolean isLocalWrite() {
        return false;
    }

    /**
     * Returns the transaction commit token used to identify the transaction.
     *
     * @see com.sleepycat.je.txn.Txn#getCommitToken()
     */
    @Override
    public CommitToken getCommitToken() {
        if (VLSN.isNull(commitVLSN)) {
            return null;
        }
        return new CommitToken(getRepImpl().getUUID(), commitVLSN);
    }

    private final RepImpl getRepImpl() {
        return (RepImpl)envImpl;
    }

    public long getCommitVLSN() {
        return commitVLSN;
    }

    @Override
    public long getAuthoritativeTimeout() {
        /*
         * The timeout is the lesser of the transaction timeout and the
         * insufficient replicas timeout.
         */
        final long txnTimeout = getTxnTimeout();
        long timeout = getRepImpl().getInsufficientReplicaTimeout();
        if ((txnTimeout != 0) && (txnTimeout < timeout)) {
            timeout = txnTimeout;
        }
        return timeout;
    }

    /**
     * MasterTxns use txn ids from a reserved negative space. So override
     * the default generation of ids.
     */
    @Override
    protected long generateId(TxnManager txnManager,
                              long ignore /* mandatedId */) {
        assert(ignore == 0);
        return txnManager.getNextReplicatedTxnId();
    }

    /**
     * Causes the transaction to wait until we have sufficient replicas to
     * acknowledge the commit.
     */
    @Override
    protected void txnBeginHook(TransactionConfig config)
        throws DatabaseException {

        /* AbsoluteConsistencyPolicy only applies to readonly transactions. */
        ReplicaConsistencyPolicy policy = config.getConsistencyPolicy();
        if (policy instanceof AbsoluteConsistencyPolicy) {
            throw new IllegalArgumentException
                ("AbsoluteConsistencyPolicy can only apply to read-only" +
                 " transactions.");
        }
        final RepImpl rep = getRepImpl();
        rep.checkIfInvalid();
        final ReplicaAckPolicy ackPolicy =
            getDefaultDurability().getReplicaAck();
        final RepNode node = rep.getRepNode();
        final int requiredReplicaAckCount = node.
            getDurabilityQuorum().getCurrentRequiredAckCount(ackPolicy);
        /*
         * No need to wait or authorize if this txn is not durable or there
         * are no other sites.
         */
        if (requiredReplicaAckCount == 0) {
            return;
        }

        try {
            node.awaitAuthoritativeMaster(this);
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(envImpl, e);
        }
    }

    @Override
    protected void preLogCommitHook()
        throws DatabaseException {

        super.preLogCommitHook();
        ReplicaAckPolicy ackPolicy = getCommitDurability().getReplicaAck();
        requiredAckCount = getRepImpl().getRepNode().getDurabilityQuorum().
            getCurrentRequiredAckCount(ackPolicy);
        /*
         * What getCurrentRequiredAckCount() returns is required ack from
         * replicas. If this txn needs to be group committed, it requires an
         * "ack" from the master itself.
         */
        if (isGroupCommitted) {
            requiredAckCount ++;
        }
        ackLatch = (requiredAckCount == 0) ?
            null :
            ((asyncAckHandler != null) ? new AsyncLatch(requiredAckCount) :
                ((requiredAckCount == 1) ? new SingleWaiterLatch() :
                new MultiWaiterLatch(requiredAckCount)));

        /*
         * TODO: An optimization we'd like to do is to identify transactions
         * that only modify non-replicated databases, so they can avoid waiting
         * for Replica commit acks and avoid checks like the one that requires
         * that the node be a master before proceeding with the transaction.
         */
        getRepImpl().preLogCommitHook(this);
        preLogCommitEndDeltaMs =
            (int) (TimeSupplier.currentTimeMillis() - getTxnStartMillis());
    }

    @Override
    protected boolean postLogCommitHook(LogItem commitItem)
        throws DatabaseException {

        super.postLogCommitHook(commitItem);
        commitVLSN = commitItem.header.getVLSN();
        try {
            return getRepImpl().postLogCommitHook(this, commitItem);
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(envImpl, e);
        }
    }

    @Override
    protected void preLogAbortHook()
        throws DatabaseException {

        getRepImpl().preLogAbortHook(this);
    }

    @Override
    protected void postLogCommitAbortHook() {
        getRepImpl().postLogCommitAbortHook(this);
    }

    @Override
    protected void postLogAbortHook() {
        getRepImpl().postLogAbortHook(this);
    }

    /**
     * Prevent this MasterTxn from taking locks if the node becomes a
     * replica. The application has a reference to this Txn, and may
     * attempt to use it well after the node has transitioned from master
     * to replica.
     */
    @Override
    public LockResult lockInternal(long lsn,
                                   LockType lockType,
                                   boolean noWait,
                                   boolean jumpAheadOfWaiters,
                                   DatabaseImpl database,
                                   CursorImpl cursor)
        throws LockNotAvailableException, LockConflictException,
               DatabaseException {
        ReplicatedEnvironment.State nodeState = ((RepImpl)envImpl).getState();
        if (nodeState.isMaster()) {
            return super.lockInternal(
                lsn, lockType, noWait, jumpAheadOfWaiters, database, cursor);
        }

        throwNotMaster(nodeState);
        return null; /* not reached */
    }

    /**
     * There is a small window between a LN logging the new entry and locking
     * the entry.  During this time it is possible for the master to transition
     * to another state, resulting in the lock failing if the regular
     * lockInternal is used, which can resulting in log corruption.  As such
     * this function is used to bypass the master status check and return the
     * lock.
     */
    @Override
    public LockResult postLogNonBlockingLock(long lsn,
                                             LockType lockType,
                                             boolean jumpAheadOfWaiters,
                                             DatabaseImpl database,
                                             CursorImpl cursor) {
        final LockResult result = super.lockInternal(
            lsn, lockType, true /*noWait*/, jumpAheadOfWaiters, database,
            cursor);
        if (result.getLockGrant() != LockGrantType.DENIED) {
            checkPreempted();
        }
        return result;
    }

    private void throwNotMaster(ReplicatedEnvironment.State nodeState) {
        if (nodeState.isReplica()) {
            throw new ReplicaWriteException
                (this, ((RepImpl)envImpl).getStateChangeEvent());
        }
        throw new UnknownMasterException
            ("Transaction " + getId() +
             " cannot execute write operations because this node is" +
             " no longer a master");
    }

    /**
     * If logging occurs before locking, we must screen out write locks here.
     */
    @Override
    public synchronized void preLogWithoutLock(DatabaseImpl database) {
        ReplicatedEnvironment.State nodeState = ((RepImpl)envImpl).getState();
        if (nodeState.isMaster()) {
            super.preLogWithoutLock(database);
            return;
        }

        throwNotMaster(nodeState);
    }

    public int getRequiredAckCount() {
        return requiredAckCount;
    }

    public void setRequiredAckCount(int ackCount) {
        requiredAckCount = ackCount;
    }

    /**
     * Used to determine whether a txn has exceeded the time it's allowed to
     * wait for acks. The method always return false, if the method is called
     * before the txn has been committed locally and acks have not been
     * requested as yet.
     *
     * @param nowNs passed in to avoid repeated nanotime calls.
     *
     * @return true if the txn did not get acks in its timeout window
     */
    public boolean ackTimedOut(long nowNs) {
        return (ackWaitLimitNs > 0) && ((nowNs - ackWaitLimitNs) > 0) ;
    }

    /**
     * Starts the timer for window within which we expect the txn to be
     * acknowledged. If txn is not acknowledged within this window, it results
     * in an InsufficientAcksException.
     */
    public void startAckTimeouts(int timeoutMs) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("Bad ack timeout:" + timeoutMs);
        }
        this.ackWaitStartNs = System.nanoTime();
        this.ackWaitLimitNs = ackWaitStartNs +
            TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    }

    public long getAckWaitStartNs() {
        return ackWaitStartNs;
    }

    public int getAckTimeoutMs() {
        if (ackWaitStartNs == 0) {
            throw new IllegalStateException("ack time has not been set");

        }
        return (int)TimeUnit.NANOSECONDS.
            toMillis(ackWaitLimitNs - ackWaitStartNs);
    }

    /**
     * A masterTxn always writes its own id along with its term into the
     * commit or abort.
     */
    @Override
    protected Supplier<MasterIdTerm> getMasterIdTermSupplier() {
        return masterIdTermSupplier;
    }

    @Override
    protected long getDTVLSN() {
        /*
         * For the master transaction, it should always be null, and will
         * be corrected under the write log latch on its way to disk.
         */
        return NULL_VLSN;
    }

    /**
     * Returns the async ack handler for invocation (on the handlers OnQuorum()
     * or onException() methods) after performing validity checks. It returns a
     * non null value exactly once: the first time it's invoked.
     *
     * @return the handler, or null if it has already been invoked
     */
    public synchronized AsyncAckHandler getAsyncInvocationHandler() {
        final State state = getState();
        if ((state != State.COMMITTED) &&
            /*
             * Expect COMMITTED, but allow OPEN, because, the feeder thread can
             * win the race with the application thread doing the commit.
             */
            (state != State.OPEN)) {
            throw new IllegalStateException("Unxpected txn state:" + state);
        }

        if (asyncAckHandler == null) {
            throw new IllegalStateException("txn:" + getId() +
                                            " missing its async handler");
        }
        if (asyncHandled) {
            return null;
        }

        asyncHandled = true;
        return asyncAckHandler;
    }

    public void stampRepWriteTime() {
        this.repWriteStartDeltaMs =
            (int)(TimeSupplier.currentTimeMillis() - getTxnStartMillis());
    }

    /**
     * Returns the amount of time it took to copy the commit record from the
     * log buffer to the rep stream. It's measured as the time interval
     * starting with the time the preCommit hook completed, to the time the
     * message write to the replication stream was initiated.
     */
    public long messageTransferMs() {
        return repWriteStartDeltaMs > 0 ?

                (repWriteStartDeltaMs - preLogCommitEndDeltaMs) :

                /*
                 * The message was invoked before the post commit hook fired.
                 */
                0;
    }

    /**
     * Wrapper method enforces the invariant that the read lock must
     * have been released after the commit commit has completed.
     */
    @Override
    public long commit(Durability durability) {
        final long lsn;
        try {
            lsn = super.commit(durability);
            assert TestHookExecute.doHookIfSet(commitEpilogueHook, this);
            return lsn;
        } finally {
            /* Rep Invariant: read lock must have been released at txn exit */
            if (envImpl.isValid() && (readLockStamp > 0)) {
                String msg = "transaction: " + getId() +
                             " has read lock after commit";
                LoggerUtils.severe(envImpl.getLogger(), envImpl, msg);

                throw EnvironmentFailureException.
                    unexpectedState(envImpl, msg);
            }
        }
    }

    /**
     * Wrapper method enforces the invariant that the read lock must
     * have been released after the abort has completed.
     */
    @Override
    public long abort(boolean forceFlush) {
        try {
            return super.abort(forceFlush);
        } finally {
            /* Rep Invariant: read lock must have been released at txn exit */
            if (envImpl.isValid() && (readLockStamp > 0)) {
                String msg = "transaction: " + getId() +
                             " has read lock after abort";
                LoggerUtils.severe(envImpl.getLogger(), envImpl, msg);

                throw EnvironmentFailureException.
                unexpectedState(envImpl, msg);
            }
        }
    }

    /**
     * Lock out the setting of the block latch by Master Transfer while a
     * commit/abort log record, which advances the VLSN is written. This lock
     * will be released by the {@code postLogXxxHook()} functions, one of which
     * is guaranteed to be called, unless an Environment-invalidating exception
     * occurs.
     *
     * The corresponding method {@link #unlockReadBlockLatch} unlocks the latch
     */
    public void lockReadBlockLatch() throws InterruptedException {
        if (readLockStamp > 0) {
            /* Enforce assertion that read lock cannot be set more than
             * once within a txn
             */
            throw new IllegalStateException("block latch read locked for txn:" +
                getId());
        }
        final long timeoutMs = 2 * MasterTransfer.getPhase2TimeoutMs();
        readLockStamp = getRepImpl().getBlockLatchLock().
            tryReadLock(timeoutMs, TimeUnit.MILLISECONDS);
        if (readLockStamp == 0) {
            final String msg =
                "Txn:" + this.toString() +
                " failed to aquire read lock on block latch within" +
                " timeout: " + timeoutMs  + " ms";
            LoggerUtils.fullThreadDump(getRepImpl().getLogger(), getRepImpl(),
                                       Level.SEVERE);
            throw EnvironmentFailureException.
                unexpectedState(getRepImpl(), msg);
        }
    }

    /**
     * Unlocks the readLocked latch, if it has been set by
     * {@link #lockReadBlockLatch}. Note that this method is more forgiving,
     * since there are instances of abort processing during a txn commit where
     * it's not possible to tell whether the latch has been set in a definitive
     * way.
     */
    public void unlockReadBlockLatch() {
        if (readLockStamp == 0) {
            LoggerUtils.info(getRepImpl().getLogger(), getRepImpl(),
                             "ignoring unset read lock:" + getId());
            return;
        }
        getRepImpl().getBlockLatchLock().unlock(readLockStamp);
        readLockStamp = 0;
    }

    @Override
    protected boolean
        propagatePostCommitException(DatabaseException postCommitException) {
        return (postCommitException instanceof InsufficientAcksException) ?
                true :
                super.propagatePostCommitException(postCommitException);
    }

    /* The Txn factory interface. */
    public interface MasterTxnFactory {
        MasterTxn create(EnvironmentImpl envImpl, TransactionConfig config);

        /**
         * Create a special "null" txn that does not result in any changes to
         * the environment. It's sole purpose is to persist and communicate
         * DTVLSN values.
         */
        default MasterTxn createNullTxn(EnvironmentImpl envImpl) {
            return new NullTxn(envImpl);
        }

        default MasterTxn createSyncNullTxn(EnvironmentImpl envImpl) {
            return new NullTxn(envImpl, true);
        }
    }

    /* The method used to create user Master Txns via the factory. */
    public static MasterTxn create(EnvironmentImpl envImpl,
                                   TransactionConfig config) {
        return factory.create(envImpl, config);
    }

    public static MasterTxn createNullTxn(EnvironmentImpl envImpl, boolean sync) {
        return sync
                ? factory.createSyncNullTxn(envImpl)
                : factory.createNullTxn(envImpl);
    }

    /**
     * Method used for unit testing.
     *
     * Sets the factory to the one supplied. If the argument is null it
     * restores the factory to the original default value.
     */
    public static void setFactory(MasterTxnFactory factory) {
        MasterTxn.factory = (factory == null) ? DEFAULT_FACTORY : factory;
    }

    /**
     * Convert a MasterTxn that has any write locks into a ReplayTxn, and close
     * the MasterTxn after it is disemboweled. A MasterTxn that only has read
     * locks is unchanged and is still usable by the application. To be clear,
     * the application can never use a MasterTxn to obtain a lock if the node
     * is in Replica mode, but may indeed be able to use a read-lock-only
     * MasterTxn if the node cycles back into Master status.
     *
     * For converted MasterTxns, all write locks are transferred to a replay
     * transaction, read locks are released, and the txn is closed. Used when a
     * node is transitioning from master to replica mode without recovery,
     * which may happen for an explicit master transfer request, or merely for
     * a network partition/election of new
     * master.
     *
     * The newly created replay transaction will need to be in the appropriate
     * state, holding all write locks, so that the node in replica form can
     * execute the proper syncups.  Note that the resulting replay txn will
     * only be aborted, and will never be committed, because the txn originated
     * on this node, which is transitioning from {@literal master -> replica}.
     *
     * We only transfer write locks. We need not transfer read locks, because
     * replays only operate on writes, and are never required to obtain read
     * locks. Read locks are released though, because (a) this txn is now only
     * abortable, and (b) although the Replay can preempt any read locks held
     * by the MasterTxn, such preemption will add delay.
     *
     * @return a ReplayTxn, if there were locks in this transaction, and
     * there's a need to create a ReplayTxn.
     */
    public ReplayTxn convertToReplayTxnAndClose(Logger logger,
                                                Replay replay) {

        /* Assertion */
        if (!freeze) {
            throw unexpectedState
                (envImpl,
                 "Txn " + getId() +
                 " should be frozen when converting to replay txn");
        }

        /*
         * This is an important and relatively rare operation, and worth
         * logging.
         */
        LoggerUtils.info(logger, envImpl,
                         "Transforming txn " + getId() +
                         " from MasterTxn to ReplayTxn");

        int hookCount = 0;
        ReplayTxn replayTxn = null;
        boolean needToClose = true;

        /*
         * Wake up any threads waiting for acks on this txn.  Calling notify()
         * on the transaction latch will not work since the code is already
         * designed to ignore any wakeup calls that come before the timeout
         * due to the problem of spurious wakeups.  This transaction will
         * throw an InsufficientAcksException when it sees that "freeze" is
         * true.
         */
        while (getPendingAcks() > 0) {
            countdownAck();
        }

        synchronized (this) {
            try {

                if (isClosed()) {
                    LoggerUtils.info(logger, envImpl,
                                     "Txn " + getId() +
                                     " is closed, no transform needed");
                    needToClose = false;
                    return null;
                }

                if (getCommitLsn() != DbLsn.NULL_LSN) {
                    /* Covers a window in Txn.commit() between the time a
                     * commit record is written and the txn is closed. Similar
                     * handling as above, except for log message.
                     */
                    LoggerUtils.info(logger, envImpl,
                                     "Txn " + getId() +
                                     " has written commit log entry, " +
                                     " no transform needed");
                    needToClose = false;
                    return null;
                }

                checkLockInvariant();

                /*
                 * Get the list of write locks, and process them in lsn order,
                 * so we properly maintain the lastLoggedLsn and firstLoggedLSN
                 * fields of the newly created ReplayTxn.
                 */
                final Set<Long> lockedLSNs = getWriteLockIds();

                /*
                 * This transaction may not actually have any write locks. In
                 * that case, we permit it to live on.
                 */
                if (lockedLSNs.size() == 0) {
                    LoggerUtils.info(logger, envImpl, "Txn " + getId() +
                                     " had no write locks, didn't create" +
                                     " ReplayTxn");
                    needToClose = false;
                    return null;
                }

                /*
                 * We have write locks. Make sure that this txn can now
                 * only be aborted. Otherwise, there could be this window
                 * in this method:
                 *  t1: locks stolen, no locks left in this txn
                 *  t2: txn unfrozen, commits and aborts possible
                 *    -- at this point, another thread could sneak in and
                 *    -- try to commit. The txn would commmit successfully,
                 *    -- because a commit w/no write locks is a no-op.
                 *    -- but that would convey the false impression that the
                 *    -- txn's write operations had commmitted.
                 *  t3: txn is closed
                 */
                setOnlyAbortable(new UnknownMasterException
                                 (envImpl.getName() +
                                  " is no longer a master"));

                if (!updateLoggedForTxn()) {
                    /*
                     * If a write-lock was acquired as part of an incomplete
                     * RMW operation then no updates were logged. Such a txn
                     * would not be aborted by the new master, since it is not
                     * known to the new master. In addition to making it
                     * abort-only we release the write locks now to prevent
                     * lock conflicts with replay in the event that the
                     * MasterTxn is not aborted immediately. Any read locks are
                     * also released now for good measure, although this may
                     * not be strictly necessary.
                     */
                    LoggerUtils.info(logger, envImpl,
                                     "Txn " + getId() +
                                     " has write locks but no updates," +
                                     " no transform needed");
                    clearWriteLocks(Collections.emptySet());
                    clearReadLocks();
                    needToClose = false;
                    return null;
                }

                replayTxn = replay.getReplayTxn(this);

                /*
                 * Order the lsns, so that the locks are taken in the proper
                 * order, and the txn's firstLoggedLsn and lastLoggedLsn fields
                 * are properly set.
                 */
                List<Long> sortedLsns = new ArrayList<>(lockedLSNs);
                Collections.sort(sortedLsns);
                LoggerUtils.info(logger, envImpl,
                                 "Txn " + getId()  + " has " +
                                 sortedLsns.size() + " locks to transform");

                /*
                 * Transfer each lock. Note that ultimately, since mastership
                 * is changing, and replicated commits will only be executed
                 * when a txn has originated on that node, the target ReplayTxn
                 * can never be committed, and will only be aborted.
                 */
                for (Long lsn: sortedLsns) {

                    LoggerUtils.fine(logger, envImpl,
                                     "Txn " + getId() +
                                     " is transferring lock " + lsn);

                    /*
                     * Use a special method to steal the lock. Another approach
                     * might have been to have the replayTxn merely attempt a
                     * lock(); as an importunate txn, the replayTxn would
                     * preempt the MasterTxn's lock. However, that path doesn't
                     * work because lock() requires having a databaseImpl in
                     * hand, and that's not available here.
                     */
                    replayTxn.stealLockFromMasterTxn(lsn);

                    /*
                     * Copy all the lock's info into the Replay and remove it
                     * from the master. Normally, undo clears write locks, but
                     * this MasterTxn will not be executing undo.
                     */
                    WriteLockInfo replayWLI = replayTxn.getWriteLockInfo(lsn);
                    WriteLockInfo masterWLI = getWriteLockInfo(lsn);
                    replayWLI.copyAllInfo(masterWLI);
                    removeLock(lsn);
                }
                LoggerUtils.info(logger, envImpl,
                                 "Txn " + getId()  + " transformed " +
                                 sortedLsns.size() + " locks");

                /*
                 * Txns have collections of undoDatabases and dbCleanupSet.
                 * Undo databases are normally incrementally added to the txn
                 * as locks are obtained Unlike normal locking or recovery
                 * locking, in this case we don't have a reference to the
                 * databaseImpl that goes with this lock, so we copy the undo
                 * collection in one fell swoop.
                 */
                replayTxn.copyDatabasesForConversion(this);

                /*
                 * This txn is no longer responsible for databaseImpl
                 * cleanup, as that issue now lies with the ReplayTxn, so
                 * remove the collection.
                 */
                dbCleanupSet = null;

                /*
                 * All locks have been removed from this transaction. Clear
                 * the firstLoggedLsn and lastLoggedLsn so there's no danger
                 * of attempting to undo anything; this txn is no longer
                 * responsible for any records.
                 */
                lastLoggedLsn = DbLsn.NULL_LSN;
                firstLoggedLsn = DbLsn.NULL_LSN;

                /* If this txn also had read locks, clear them */
                clearReadLocks();
            } finally {
                assert TestHookExecute.doHookIfSet(convertHook, hookCount++);

                unfreeze();

                /*
                 * We need to abort the txn, but we can't call abort() as it
                 * checks whether we are the master! Instead, call the
                 * internal method, close(), in order to end this transaction
                 * and unregister it from the transactionManager.
                 */
                if (needToClose) {
                    LoggerUtils.info(logger, envImpl, "About to close txn " +
                                     getId() + " state=" + getState());
                    close(false /*isCommit */);
                    LoggerUtils.info(logger, envImpl, "Closed txn " +  getId() +
                                     " state=" + getState());
                }
            }
        }
        assert TestHookExecute.doHookIfSet(convertHook, hookCount++);
        assert TestHookExecute.doHookIfSet(convertTxnHook, 0);
        return replayTxn;
    }

    public void freeze() {
        freeze = true;
    }

    public boolean getFreeze() {
        return freeze;
    }

    private void unfreeze() {
        freeze = false;
    }

    /**
     * Used to hold the transaction stable while it is being cloned as a
     * ReplayTxn, during {@literal master->replica} transitions. Essentially,
     * there are two parties that now have a reference to this transaction --
     * the originating application thread, and the RepNode thread that is
     * trying to set up internal state so it can begin to act as a replica.
     *
     * The transaction will throw UnknownMasterException or
     * ReplicaWriteException if the transaction is frozen, so that the
     * application knows that the transaction is no longer viable, but it
     * doesn't attempt to do most of the follow-on cleanup and release of locks
     * that failed aborts and commits normally attempt. One aspect of
     * transaction cleanup can't be skipped though. It is necessary to do the
     * post log hooks to free up the block txn latch lock so that the
     * transaction can be closed by the RepNode thread. For example:
     * - application thread starts transaction
     * - application takes the block txn latch lock and attempts commit or
     * abort, but is stopped because the txn is frozen by master transfer.
     * - the application must release the block txn latch lock.
     * @see Replica#replicaTransitionCleanup
     */
    @Override
    protected void checkIfFrozen(boolean isCommit) {
        if (freeze) {
            try {
                ((RepImpl) envImpl).checkIfMaster(this);
            } catch (DatabaseException e) {
                if (isCommit) {
                    postLogCommitAbortHook();
                } else {
                    postLogAbortHook();
                }
                throw e;
            }
        }
    }

    /** @see #checkIfFrozen */
    @Override
    protected boolean isFrozen() {
        return freeze;
    }

    /* For unit testing */
    public void setConvertHook(TestHook<Integer> hook) {
        convertHook = hook;
    }

    /* For unit testing */
    public void setOpenDBHook(TestHook<Integer> hook) {
        openDBHook = hook;
    }

    /* For unit testing */
    public void setConvertTxnHook(TestHook<Integer> hook) {
        convertTxnHook = hook;
    }

    /**
     * @param hook the asyncAckTestHook to set
     */
    public static void setCommitEpilogueHook(TestHook<MasterTxn> hook) {
        commitEpilogueHook = hook;
    }

    @Override
    public boolean isMasterTxn() {
        return true;
    }

    public void setArbiterAck(boolean val) {
        needsArbiterAck = val;
    }

    @Override
    public boolean getArbiterAck() {
        return needsArbiterAck;
    }

    public CountDownLatch getTestLatch() {
        return testLatch;        
    }

    public void setRaceOnDBCleanUp(boolean isRace) {
        raceOnDBCleanUp = isRace;        
    }

    public boolean getRaceOnDBCleanUp() {
        return raceOnDBCleanUp;       
    }

    public void addDbCleanupAndCloseidDb
        (final DbCleanup cleanup, Locker idDbLocker) {

        /*
         * See[KVSTORE-1602] We observed a race conditon between 
         * Replica#replicaTransitionCleanup
         * and DatabaseImpl#doCreateDb. During the replica thread conversion of
         * master txn to replay txn(aborted), the dbCleanupSet may miss a 
         * cleanup if the replica thread and user thread runs the above methods
         * concurrently
         * Example :
         *     replica t1: replicaTransitionCleanup (read replaytxn Cleanupset)
         *     user t2: doCreateDb 
         *              -> nameLocker.addDbCleanup (write mastertxn Cleanupset)
         *     replica t1: initReplicaLoop
         *                  -> syncup.execute
         *                  -> replayTxn.rollback
         *                  -> cleanupDatabaseImpls (read replaytxn CleanupSet)
         * So we cleanup here,
         *   if order is t1 -> t2 , current method will cleanup the closed txn's
         *                          db
         *               t2 -> t1 , normal flow ,replicaTransitionCleanup 
         *               takes care of this              
         */
        assert TestHookExecute.doHookIfSet(openDBHook, 0);
        synchronized (this) {
            if (isClosed()) {
                idDbLocker.operationEnd(false);
                if (dbCleanupSet == null) {
                    dbCleanupSet = new HashSet<>(); 
                }
                dbCleanupSet.add(cleanup);
                setRaceOnDBCleanUp(true);
            }
        }
        if (getRaceOnDBCleanUp()) {
            cleanupDatabaseImpls(false);
            return;
        }
        addDbCleanup(cleanup);
    }

    /**
     * Wait for replicas to supply the requisite acknowledgments for
     * this transaction.
     *
     * @param awaitStartMs the time at which the wait for an ack was started
     * @param timeoutMs the max time to wait for acks
     *
     * @return true of the requisite acks were received
     *
     * @throws InterruptedException
     */
    public final boolean await(long awaitStartMs, int timeoutMs)
        throws InterruptedException {

        return (ackLatch == null) || ackLatch.await(awaitStartMs, timeoutMs);
    }

    /**
     * A test entrypoint to explicitly create spurious notifies on
     * Object.wait() used by the optimized SingleWaiterLatch based
     * implementation of Latch.
     */
    public final boolean testNotify() {
        if (ackLatch != null) {
            ackLatch.testNotify();
            return true;
        }
        return false;
    }


    /* Countdown the latch, in response to an ack. */
    public void countdownAck() {
        if (ackLatch == null) {
            return;
        }

        ackLatch.countDown();
    }

    /* Return the number of acks still to be received. The number can be
     * negative if more than the required number of acks are received. So
     * use getPendingAcks() <= 0 to check whether acks have been satisfied.
     */
    public final int getPendingAcks() {
        if (ackLatch == null) {
            return 0;
        }

        return (int) ackLatch.getCount();
    }

    private interface Latch {
        /**
         * Returns true if the latch was tripped, false if the timer expired
         * without tripping the latch.
         */
        boolean await(long awaitStartMs, int timeoutMs)
            throws InterruptedException;

        /**
         * Test entrypoint for spurious wakeups
         */
        void testNotify();

        /**
         * Counts down the latch.
         */
        void countDown();

        /**
         * Returns the count of waiters associated with the latch.
         */
        long getCount();
    }

    @SuppressWarnings("serial")
    private class AsyncLatch extends AtomicInteger implements Latch,
        NotSerializable {

        public AsyncLatch(int count) {
            super(count);
        }

        @Override
        public boolean await(long awaitStartMs, int timeoutMs)
            throws InterruptedException {
            throw new IllegalStateException("Cannot wait on async latch");
        }

        @Override
        public synchronized void testNotify() {
            throw new IllegalStateException("Not a SingleWaiterLatch");
        }

        @Override
        public void countDown() {
            /* Count can go negative. */
           decrementAndGet();
        }

        @Override
        public long getCount() {
            final int count = get();
            return count < 0 ? 0 : count;
        }

    }

    private class SingleWaiterLatch implements Latch {
        private volatile boolean done = false;

        @Override
        public synchronized void countDown() {
            done = true;
            /* Notify the single transaction in the post commit hook */
            this.notify();
        }

        @Override
        public synchronized long getCount() {
            return done ? 0 : 1l;
        }

        @Override
        public synchronized boolean await(long awaitStartMs,
                                          int timeoutMs)
            throws InterruptedException {
            if (done) {
                return done;
            }

            /* Loop to allow for spurious wakeups:
             * https://stackoverflow.com/questions/1050592/do-spurious-wakeups-in-java-actually-happen
             */
            for (long waitMs = timeoutMs; waitMs > 0; ){
                wait(waitMs);
                /*
                 * The master is becoming a replica due to MasterTransfer, so
                 * the latch was woken up early before the timeout and before
                 * it could get enough acks.
                 */
                if (freeze) {
                    return false;
                }
                if (done) {
                    return done;
                }
                /* Check for spurious wakeup. Only obtain time, when we
                 * have timed out, or there's a spurious wakeup. These should
                 * be rare occurrences and the extra overhead negligible.
                 */
                waitMs = timeoutMs -
                    (TimeSupplier.currentTimeMillis() - awaitStartMs);
            }
            return done; /* timed out. */
        }

        @Override
        public synchronized void testNotify() {
            this.notify();
        }
    }

    private class MultiWaiterLatch extends CountDownLatch implements Latch {

        public MultiWaiterLatch(int count) {
            super(count);
        }

        @Override
        public boolean await(long awaitStartMs,
                             int timeoutMs) throws InterruptedException {
            final boolean done = await(timeoutMs, TimeUnit.MILLISECONDS);
            /*
             * The master is becoming a replica due to MasterTransfer, so
             * the latch was woken up early before the timeout and before it
             * could get enough acks.
             */
            if (freeze) {
                return false;
            }
            return done;
        }

        @Override
        public synchronized void testNotify() {
            throw new IllegalStateException("Not a SingleWaiterLatch");
        }
    }

    /**
     * Returns true if the txn has been configured to use async acks
     */
    public boolean usesAsyncAcks() {
        return (asyncAckHandler != null);
    }

    /**
     * Returns true if the transaction satisfies the following requirements:
     *
     * 1) uses async acks
     * 2) the txn durability calls for them
     * 3) changes were logged in the transaction.
     *
     * Note that this determination can only be made at the time of the commit.
     *
     * This method is used by KV, to determine whether a response should be
     * held back, after return from a commit, until after async acks have been
     * received.
     */
    public boolean needsAsyncAcks() {

        final State state = getState();

        if (state == State.ABORTED) {
            return false;
        }
        if (state != State.COMMITTED) {
            throw new IllegalStateException("Transaction:" + getId() +
                                            " state:" + state);
        }

        return asyncAckHandler != null &&
            updateLoggedForTxn() &&
            (requiredAckCount > 0) ;
    }

}
