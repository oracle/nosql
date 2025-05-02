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

import static com.sleepycat.je.utilint.DbLsn.NULL_LSN;
import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;
import static com.sleepycat.je.utilint.VLSN.NULL_VLSN;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.recovery.RecoveryManager;
import com.sleepycat.je.rep.impl.node.MasterIdTerm;
import com.sleepycat.je.rep.impl.node.MasterTerm;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.txn.LockAttemptResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.TxnChain;
import com.sleepycat.je.txn.TxnChain.RevertInfo;
import com.sleepycat.je.txn.TxnEnd;
import com.sleepycat.je.txn.TxnManager;
import com.sleepycat.je.txn.UndoReader;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.SimpleTxnMap;
import com.sleepycat.je.utilint.Timestamp;
import com.sleepycat.je.utilint.VLSN;

/**
 * Used for replaying replicated operations at replica nodes.
 */
public class ReplayTxn extends Txn {

    /* The System.nanoTime the txn was committed or aborted */
    private long endTimeNs = 0;

    /*
     * The last VLSN applied by this txn. Used for sanity checking.
     * This field is currently not precisely set when a transaction is
     * resurrected. The sanity check could be made more robust by setting
     * this field at resurrection.
     */
    private long lastApplied = NULL_VLSN;

    /* Tracks whether the rep group db was changed by the transaction */
    private boolean repGroupDbChange = false;

    /* The commit/abort VLSN associated with this txn */
    private long txnEndVLSN;

    /*
     * The log entry that was used to create this replay txn. It contains
     * information like the masterId and the masterTerm that was used
     * to create this transaction.
     */
    private TxnEnd txnEnd;
    /* The supplier of the master term Id. Updated when txnEnd is updated. */
    private Supplier<MasterIdTerm> masterIdTermSupplier =
        () -> MasterIdTerm.PRIMODAL_ID_TERM;

    private SimpleTxnMap<ReplayTxn> activeTxns;

    /*
     * ReplayTxns are frequently constructed. Don't create its own logger;
     * instead, use the Replay's logger.
     */
    private final Logger logger;

    /**
     * Used when creating ReplayTxns for Replay.
     */
    public ReplayTxn(EnvironmentImpl envImpl,
                     long txnId,
                     long startNs,
                     SimpleTxnMap<ReplayTxn> activeTxns,
                     Logger logger)
        throws DatabaseException {
        /*
         * Durability will be explicitly specified when ReplayTxn.commit is
         * called, so TransactionConfig.DEFAULT is fine.
         */
        this(envImpl, TransactionConfig.DEFAULT, txnId, logger);
        this.startNs = startNs;
        this.activeTxns = activeTxns;
    }

    /**
     * Used when creating ReplayTxns at recovery. No ActiveTxns map is
     * available.
     */
    public ReplayTxn(EnvironmentImpl envImpl,
                     TransactionConfig config,
                     long txnId,
                     Logger logger)
        throws DatabaseException {

        super(envImpl,
              config,
              null,      // ReplicationContext set later
              txnId);    // mandatedId
        /* Preempt reader transactions when a lock conflict occurs. */
        setImportunate(true);
        startNs = System.nanoTime();
        this.logger = logger;
        assert !config.getLocalWrite();
    }

    public void setMasterCommitState(ReplicationContext context,
                                     TxnEnd txnEnd) {
        this.txnEndVLSN = context.getClientVLSN();
        validateDTVLSN(txnEnd.getDTVLSN());
        this.txnEnd = txnEnd;
        this.masterIdTermSupplier = () ->
            (txnEnd == null) ? MasterIdTerm.PRIMODAL_ID_TERM :
            new MasterIdTerm(txnEnd.getMasterId(), txnEnd.getMasterTerm());
    }

    public long abort(ReplicationContext context, TxnEnd logTxnEnd) {
        setMasterCommitState(context, logTxnEnd);
        return abort(context);
    }

    @Override
    protected void checkLockInvariant() {
        if ((lastLoggedLsn == DbLsn.NULL_LSN) && getWriteLockIds().isEmpty()) {
            /* A Null replay txn, used  to communicate a DTVLSN. */
            return;
        }
        super.checkLockInvariant();
    }

    /**
     * Performs checks on the two txns to ensure that this txn can correctly
     * follow the precedingTxn in the log. It enforces the sequencing behavior
     * required by the fields implementing the txn thus ensuring log integrity.
     *
     * This check must be performed before the commit entry is written to
     * ensure that the log is undamaged.
     *
     * @param precedingTxn the txn commit/abort immediately preceding this one.
     */
    public void checkCommitFollows(ReplayTxn precedingTxn) {
        if (precedingTxn == null) {
            return;
        }

        if (precedingTxn.getDTVLSN() <= getDTVLSN() &&
            precedingTxn.getVLSN() < txnEndVLSN &&
            MasterTerm.follows(precedingTxn.getMasterTerm(), getMasterTerm())){
            return;
        }

        throw new IllegalStateException(precedingTxn.logString() +
                                        " does not precede " + logString());
    }

    /**
     * Returns the vlsn associated with the txn commit/abort
     */
    public long getVLSN() {
        return txnEndVLSN;
    }

    /**
     * Don't let the ReplayTxn have a timeout of 0. If it did, it could be
     * deadlocked against a reader txn. As long as there is a non zero timeout,
     * any conflicts will be adjugated by the LockManager in its favor.
     */
    @Override
    protected long getInitialLockTimeout() {
        return envImpl.getReplayTxnTimeout();
    }

    @Override
    public boolean isLocalWrite() {
        return false;
    }

    public boolean getRepGroupDbChange() {
        return repGroupDbChange;
    }

    public void noteRepGroupDbChange() {
        repGroupDbChange = true;
    }

    public void registerWithActiveTxns(SimpleTxnMap<ReplayTxn> newActiveTxns) {
        assert activeTxns == null;
        activeTxns = newActiveTxns;
        activeTxns.put(this);
    }

    /**
     * Replay transactions always use the txn id that is included in its
     * replicated operation.
     */
    @Override
    protected long generateId(TxnManager txnManager,
                              long mandatedId) {
        return mandatedId;
    }

    /*
     * A ReplayTxn always writes the node ID of the master which generated
     * the commit or abort.
     */
    @Override
    protected Supplier<MasterIdTerm> getMasterIdTermSupplier() {
        return masterIdTermSupplier;
    }

    /**
     * Utility method to validate DTVLSN.
     */
    private long validateDTVLSN(long checkDTVLSN) {

        if ((txnEndVLSN != INVALID_VLSN) && VLSN.isNull(checkDTVLSN)) {
            throw new IllegalStateException("DTVLSN(" + txnEndVLSN +") is null");
        }

        if (txnEndVLSN == INVALID_VLSN) {
            /*
             * Can be null, if this is a in-flight replay Txn that is being
             * aborted as part of a replica -> master transition and
             * consequently does not have a pre-assigned vlsn; a VLSN will
             * be assigned when the abort is actually written
             */
            return checkDTVLSN;
        }

        if (checkDTVLSN > txnEndVLSN) {
            throw new IllegalStateException("DTVLSN(txnEndVLSN)=" + checkDTVLSN +
                                            " > " + "txnEndVlsn=" + txnEndVLSN);
        }

        return checkDTVLSN;
    }

    /**
     * Commits the txn being replayed.
     *
     * @param syncPolicy to be used for the commit.
     * @param clientRepContext the replication context it encapsulates the VLSN
     * associated with the txn.
     *
     * @return the commit LSN
     *
     * @throws DatabaseException
     */
    public long commit(SyncPolicy syncPolicy,
                       ReplicationContext clientRepContext)
        throws DatabaseException {


        if (logger.isLoggable(Level.FINE)) {
            LoggerUtils.fine(logger, envImpl, "commit called for " + getId());
        }

        setRepContext(clientRepContext);
        Durability durability = null;
        if (syncPolicy == SyncPolicy.SYNC) {
            durability = Durability.COMMIT_SYNC;
        } else if (syncPolicy == SyncPolicy.NO_SYNC) {
            durability = Durability.COMMIT_NO_SYNC;
        } else if (syncPolicy == SyncPolicy.WRITE_NO_SYNC) {
            durability = Durability.COMMIT_WRITE_NO_SYNC;
        } else {
            throw EnvironmentFailureException.unexpectedState
                ("Unknown sync policy: " + syncPolicy);
        }

        long lsn = super.commit(durability);
        endTimeNs = System.nanoTime();

        return lsn;
    }

    @Override
    public long commit() {
        throw EnvironmentFailureException.unexpectedState
            ("Replay Txn abort semantics require use of internal commit api");
    }

    @Override
    public long commit(Durability durability) {
        throw EnvironmentFailureException.unexpectedState
            ("Replay Txn abort semantics require use of internal commit api");
    }

    @Override
    public void abort() {
        throw EnvironmentFailureException.unexpectedState
            ("Replay Txn abort semantics require use of internal abort api");
    }

    @Override
    public long abort(boolean forceFlush) {
        throw EnvironmentFailureException.unexpectedState
            ("Replay Txn abort semantics require use of internal abort api");
    }

    @Override
    protected long getDTVLSN() {
        return (txnEnd != null) ?  txnEnd.getDTVLSN() : NULL_VLSN;
    }

    private long abort(ReplicationContext clientRepContext)
        throws DatabaseException {

        setRepContext(clientRepContext);
        long lsn = super.abort(false /* forceFlush */);
        endTimeNs = System.nanoTime();
        return lsn;
    }

    /**
     * Always return true in order to ensure that the VLSN is logged.  Normally
     * this method returns false when no LN has been logged by the txn.  But
     * when replaying a Master txn, we need to guarantee that the VLSN is
     * logged on the Replica.  [#20543]
     */
    @Override
    protected boolean updateLoggedForTxn() {
        return true;
    }

    @Override
    public void close(boolean isCommit)
        throws DatabaseException {

        super.close(isCommit);

        if (activeTxns != null) {
            Txn removed = activeTxns.remove(getId());
            assert removed != null : "txn was not in map " + this + " " +
                LoggerUtils.getStackTrace();
        }
    }

    /**
     * Invoked when a ReplayTxn is being abandoned on shutdown.
     */
    public void cleanup()
        throws DatabaseException {
        releaseWriteLocks();
        /* Close the transaction thus causing it to be unregistered. */
        close(false);
    }

    /**
     * Rollback all write operations that are logged with an LSN &gt; the
     * matchpointLsn parameter. This is logically a truncation of the log
     * entries written by this transaction. Any log entries created by this
     * transaction are marked obsolete.
     *
     * Note that this is by no means a complete implementation of what would be
     * needed to support user visible savepoints. This method only rolls back
     * write operations and doesn't handle other types of state, like read
     * locks and open cursors.
     *
     * There are several key assumptions:
     * - the transaction does not hold read locks.
     * - the transaction will either be resumed, and any rolled back
     * operations will be repeated, or the transaction will be aborted
     * in its entirety.
     *
     * If all operations in the transaction are rolled back, this transaction
     * is also unregistered and closed.
     *
     * Rolling back a log entry through rollback is akin to truncating the
     * transactional log. The on-disk entries should not be referred to by
     * anything in the in-memory tree or the transaction chain. JE's append
     * only storage and the fact that the transactional log entries are
     * intertwined through the physical log prohibits any log truncation. To
     * mimic log truncation, any rolled back log entry is marked as
     * obsolete. Since only the last version of any data record is alive,
     * any future uses of this transaction must use the obsoleteDupsAllowed
     * option (see Txn.countObsoleteExact) to prevent asserts about duplicate
     * obsolete offsets. For example, suppose the transaction logs this:
     *
     * 100 LNa (version1)
     * 200 LNa (version2)
     * 300 LNa (version3)
     *
     * At this point in time, LSN 100 and 200 are obsolete.
     *
     * Now, suppose we roll back to LSN 100. LSNs 200 and 300 are marked
     * obsolete by the rollback.(although LSN 200 was already obsolete).  It is
     * true that for an instance in time LSN 100 is incorrectly marked as
     * obsolete, when it's really alive. But this transaction is going to
     * either abort or resume exactly as it was before, so LSN 100 is going to
     * be obsolete again.
     *
     * Suppose txn.abort() is called. The abort() logic will mark LSN 100 as
     * obsolete, since it is the latest version of the record in the
     * transaction. Using the obsoleteDupsAllowed option avoids an assertion on
     * the double recording of LSN 100.
     *
     * Alternatively, suppose LNa (version2) is retransmitted and logged as LSN
     * 400. Normal execution of LN.log() marks LSN 100 as obsolete, which would
     * trigger the assertion were it not for obsoleteDupsAllowed.
     *
     * @return list of LSNs that were rolled back
     */
    public Collection<Long> rollback(long matchpointLsn)
        throws DatabaseException {

        List<Long> rollbackLsns = new ArrayList<>();
        LoggerUtils.finest(logger, envImpl, "Partial Rollback of " + this);
        synchronized (this) {
            checkState(true);

            /* This transaction didn't log anything, nothing to rollback. */
            if (lastLoggedLsn == NULL_LSN) {
                return rollbackLsns;
            }

            /*
             * This transaction doesn't include any operations that are after
             * the matchpointLsn. There is nothing to rollback.
             */
            if (DbLsn.compareTo(lastLoggedLsn, matchpointLsn) <= 0) {
                return rollbackLsns;
            }

            setRollback();
            undoWrites(matchpointLsn, rollbackLsns);
        }

        /*
         * The call to undoWrites() may have rolled everything back, and set
         * lastLoggedLsn to NULL_LSN.
         */
        if (lastLoggedLsn == NULL_LSN) {
            /* Everything was rolled back. */
            try {

                /*
                 * Apply any databaseImpl cleanup as a result of the abort.
                 * Be sure to do this outside the synchronization block, to
                 * avoid conflict w/checkpointer.
                 */
                cleanupDatabaseImpls(false);
            } finally {
                close(false /* isCommit */);
            }
        } else {
            cleanupDatabaseImpls(false, matchpointLsn);
        }

        /*
         * We don't expect there to be any database handles associated with
         * a ReplayTxn, because only DatabaseImpls are used. Because of that,
         * there should be no cleanup needed.
         */
        if (openedDatabaseHandles != null) {
            throw EnvironmentFailureException.unexpectedState
                ("Replay Txn " + getId() + " has a openedDatabaseHandles");
        }

        /*
         * There is no need to call cleanupDatabaseImpls if the txn still holds
         * locks. The operations in this txn will either be entirely aborted,
         * or will be repeated, so any cleanup will happen when the txn ends.
         */
        return rollbackLsns;
    }

    /**
     * Rollback the changes to this txn's write locked nodes up to but not
     * including the entry at the specified matchpoint. When we log a
     * transactional entry, we record the LSN of the original,
     * before-this-transaction version as the abort LSN. This means that if
     * there are multiple updates to a given record in a single transaction,
     * each update only references that original version and its true
     * predecessor.
     *
     * This was done to streamline abort processing, so that an undo reverts
     * directly to the original version rather than stepping through all the
     * intermediates. The intermediates are skipped.  However, undo to a
     * matchpoint may need to stop at an intermediate point, so we need to
     * create a true chain of versions.
     *
     * To do so, we read the transaction backwards from the last logged LSN
     * to reconstruct a transaction chain that links intermediate versions
     * of records. For example, suppose our transaction looks like this and
     * that we are undoing up to LSN 250
     *
     * lsn=100 node=A (version 1)
     * lsn=200 node=B (version 1)
     *                        <-- matchpointLsn
     * lsn=300 node=C (version 1)
     * lsn=400 node=A (version 2)
     * lsn=500 node=B (version 2)
     * lsn=600 node=A (version 3)
     * lsn=700 node=A (version 4)
     *
     * To setup the old versions, We walk from LSN 700 -> 100
     *   700 (A) rolls back to 600
     *   600 (A) rolls back to 400
     *   500 (B) rolls back to 200
     *   400 (A) rolls back to 100
     *   300 (C) rolls back to an empty slot (NULL_LSN).
     *
     * A partial rollback also requires resetting the lastLoggedLsn field,
     * because these operations are no longer in the btree and their on-disk
     * entries are no longer valid.
     *
     * Lastly, the appropriate write locks must be released.
     * @param matchpointLsn the rollback should go up to but not include this
     * LSN.
     */
    private void undoWrites(long matchpointLsn, List<Long> rollbackLsns)
        throws DatabaseException {

        /*
         * Generate a map of node->List of intermediate LSNs for this node.
         * to re-create the transaction chain.
         */
        TreeLocation location = new TreeLocation();
        Long undoLsn = lastLoggedLsn;

        TxnChain chain =
            new TxnChain(undoLsn, id, matchpointLsn, undoDatabases, envImpl);

        try {
            while ((undoLsn != DbLsn.NULL_LSN) &&
                    DbLsn.compareTo(undoLsn, matchpointLsn) > 0) {

                UndoReader undo =
                    UndoReader.create(envImpl, undoLsn, undoDatabases);

                RevertInfo revertTo = chain.pop();

                logFinest(undoLsn, undo, revertTo);

                /*
                 * When we undo this log entry, we've logically truncated
                 * it from the log. Remove it from the btree and mark it
                 * obsolete.
                 */
                RecoveryManager.rollbackUndo(
                    logger, Level.FINER, location,
                    undo.db, undo.logEntry, undoLsn, revertTo);

                countObsoleteInexact(undoLsn, undo);
                rollbackLsns.add(undoLsn);

                /*
                 * Move on to the previous log entry for this txn and update
                 * what is considered to be the end of the transaction chain.
                 */
                undoLsn = undo.logEntry.getUserTxn().getLastLsn();
                lastLoggedLsn = undoLsn;
            }

            /*
             * Correct the fields which hold LSN and VLSN state that may
             * now be changed.
             */
            lastApplied = chain.getLastValidVLSN();
            if (lastLoggedLsn == DbLsn.NULL_LSN) {
                firstLoggedLsn = NULL_LSN;
            }

        } catch (DatabaseException e) {
            LoggerUtils.traceAndLogException(envImpl, "Txn", "undo",
                                     "For LSN=" +
                                     DbLsn.getNoFormatString(undoLsn), e);
            throw e;
        } catch (RuntimeException e) {
            throw EnvironmentFailureException.unexpectedException
                ("Txn undo for LSN=" + DbLsn.getNoFormatString(undoLsn), e);
        }

        if (lastLoggedLsn == DbLsn.NULL_LSN) {

            /*
             * The whole txn is rolled back, and it may not appear again. This
             * is the equivalent of an abort. Do any delete processing for an
             * abort which is needed.
             *
             * Set database state for db ops before releasing any write
             * locks.
             */
            setDbCleanupState(false);
        }

        /* Clear any write locks that are no longer needed. */
        clearWriteLocks(chain.getRemainingLockedNodes());
    }

    /**
     * Count an LN obsolete that is being made invisble by rollback.
     *
     * Use inexact counting.  Since invisible entries are not processed by the
     * cleaner, recording the obsolete offset would be a waste of resources.
     * Since we don't count offsets, we don't need to worry about duplicate
     * offsets.
     *
     * Some entries may be double counted if they were previously counted
     * obsolete, for example, when multiple versions of an LN were logged.
     * This is tolerated for an exceptional situation like rollback.
     */
    private void countObsoleteInexact(long undoLsn, UndoReader undo) {

        /*
         * "Immediately obsolete" LNs are counted as obsolete when they are
         * logged, so no need to repeat here.
         */
        if (undo.logEntry.isImmediatelyObsolete(undo.db)) {
            return;
        }

        envImpl.getLogManager().countObsoleteNode(undoLsn,
                                                  null, /*type*/
                                                  undo.logEntrySize,
                                                  false /*countExact*/);
    }

    /**
     * Returns the elapsed time associated with this transaction. If the
     * transaction is in progress, it returns the running elapsed time.
     *
     * @return the elapsed time as above.
     */
    public long elapsedNs() {
        final long txnStartNs = getStartNs();
        if (txnStartNs == 0) {
            return 0;
        }
        return ((endTimeNs > 0) ? endTimeNs : System.nanoTime()) -
               txnStartNs;
    }

    public void setLastAppliedVLSN(long justApplied) {
        if (justApplied <= lastApplied) {
            throw EnvironmentFailureException.unexpectedState
                ("Txn " + getId() + " attempted VLSN = " + justApplied +
                 " txnLastApplied = " +  lastApplied);
        }
        this.lastApplied = justApplied;
    }

    /**
     * ReplicatedTxns set it when the txn commit
     * or abort arrives.
     */
    public void setRepContext(ReplicationContext repContext) {
        this.repContext = repContext;
    }

    /* Wrap the call to logger to reduce runtime overhead. */
    private void logFinest(long lsn, UndoReader undo, RevertInfo revertTo) {
        if ((logger != null) && (logger.isLoggable(Level.FINEST))) {
            LoggerUtils.finest(logger, envImpl,
                               "undoLsn=" + DbLsn.getNoFormatString(lsn) +
                               " undo=" + undo + " revertInfo=" + revertTo);
        }
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
            " master id:"  + getMasterId() +
            " masterm:" + MasterTerm.logString(getMasterTerm()) +
            " vlsn:" + getVLSN();
    }

    public static String logString(ReplayTxn txn) {
        return txn == null ? "null" : txn.logString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("<ReplayTxn id=\"");
        sb.append(id);
        sb.append("\">");
        sb.append(super.toString());
        return sb.toString();
    }

    /*
     * Copy all collections that will be needed to convert masterTxn to this
     * ReplayTxn. Note that we do not need to copy the openDatabaseHandle
     * collection.  That collection is only needed by an application-facing
     * Txn, so that those database handles can be invalidated if
     * needed. ReplayTxn is not application-facing, and uses DatabaseImpls
     * rather than Databases.
     */
    public void copyDatabasesForConversion(Txn masterTxn) {
        if (masterTxn.getUndoDatabases() != null) {
            if (undoDatabases == null) {
                undoDatabases = new HashMap<>();
            }
            undoDatabases.putAll(masterTxn.getUndoDatabases());
        }

        if (masterTxn.getDbCleanupSet() != null) {
            if (dbCleanupSet == null) {
                dbCleanupSet = new HashSet<>();
            }
            dbCleanupSet.addAll(masterTxn.getDbCleanupSet());
        }
    }

    /**
     * Transfer a lock from another transaction to this one. Used for {@literal
     * master-> replica} transitions, when a node has to transform a MasterTxn
     * into a ReplayTxn. Another approach would be to have this importunate
     * ReplayTxn call lock() on the lsn, but that path is not available because
     * we do not have a handle on a databaseImpl.
     */
    public void stealLockFromMasterTxn(Long lsn) {

        LockAttemptResult result = lockManager.stealLock
            (lsn, this, LockType.WRITE);

        /*
         * Assert, and if something strange happened, opt to invalidate
         * the environment and wipe the slate clean.
         */
        if (!result.success) {
            throw EnvironmentFailureException.unexpectedState
                (envImpl,
                 "Transferring from master to replica state, txn " +
                 getId() + " was unable to transfer lock for " +
                 DbLsn.getNoFormatString(lsn) + ", lock grant type=" +
                 result.lockGrant);
        }

        addLock(Long.valueOf(lsn), LockType.WRITE,  result.lockGrant);
        addLogInfo(lsn);
    }

    @Override
    public Timestamp getMasterCommitTime() {
    	return (txnEnd != null) ? txnEnd.getTime() : null ;
    }
}
