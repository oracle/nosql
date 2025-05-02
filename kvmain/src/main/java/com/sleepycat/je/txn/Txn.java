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

import static com.sleepycat.je.EnvironmentFailureException.unexpectedState;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_READ_LOCKS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_TOTAL;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WRITE_LOCKS;
import static com.sleepycat.je.utilint.DbLsn.NULL_LSN;
import static com.sleepycat.je.utilint.VLSN.UNINITIALIZED_VLSN;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.sleepycat.je.CommitToken;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbCleanup;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.dbi.TriggerManager;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogParams;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.VersionedWriteLoggable;
import com.sleepycat.je.log.entry.AbortLogEntry;
import com.sleepycat.je.log.entry.CommitLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.recovery.RecoveryManager;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.node.MasterIdTerm;
import com.sleepycat.je.rep.txn.MasterTxn;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.txn.TxnChain.CompareSlot;
import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.Timestamp;
import com.sleepycat.utilint.FormatUtil;

/**
 * A Txn is the internal representation of a transaction created by a call to
 * Environment.txnBegin. This class must support multi-threaded use.
 */
public class Txn extends Locker implements VersionedWriteLoggable {

    /**
     * The log version of the most recent format change for this loggable.
     *
     * @see #getLastFormatChange
     */
    private static final int LAST_FORMAT_CHANGE = 8;

    /* Use an AtomicInteger to record cursors opened under this txn. */
    private final AtomicInteger cursors = new AtomicInteger();

    /* Internal transient txn flags. */
    private byte txnFlags;
    /* Set if this rollback() has been called on this transaction. */
    private static final byte PAST_ROLLBACK = 0x1;

    /*
     * Set if this transaction may abort other transactions holding a needed
     * lock.  Note that this bit flag and the setImportunate method could be
     * removed in favor of overriding getImportunate in ReplayTxn.  This was
     * not done, for now, to avoid changing importunate tests that use a Txn
     * and call setImportunate. [#16513]
     */
    private static final byte IMPORTUNATE = 0x2;

    /* Holds the public Transaction state. */
    private volatile Transaction.State txnState;

    /* Information about why a Txn was made only abortable. */
    private OperationFailureException onlyAbortableCause;

    /*
     * A Txn can be used by multiple threads. Modification to the read and
     * write lock collections is done by synchronizing on the txn.
     */
    private Set<Long> readLocks; // key is LSN
    private Map<Long, WriteLockInfo> writeInfo; // key is LSN

    /*
     * In readCommitted mode, each read lock is normally associated with one
     * or more cursors. An association between an LSN and a cursor, tracked
     * using the data structure below, means that the LSN is currently locked
     * by that cursor. The associations are used to prevent releasing a read
     * lock while a cursor is positioned on a record.
     *
     * Note that there is a many-many relationship between LSNs and cursors.
     * Two cursors may be positioned on the same LSN, and a cursor may have
     * multiple LSNs locked at one position. Examples of the latter are: when
     * a secondary cursor locks the primary and the secondary record, and when
     * an LSN changes and both the old and new LSNs are locked (see
     * applyCursorAssociations).
     *
     * Arrays are used rather than collections to optimize memory usage. The
     * pair of elements at each index represents an association. The
     * justification for linear searches is that the number of elements will
     * be small: normally 1 or 2, and rarely a few more due to LSN changes.
     * The initial size is 3 and arrays are enlarged if necessary.
     *
     * cursorLocks is the number of associations, which is the number of slots
     * occupied in each array. If readCommitted is false, cursorLocks is zero
     * and the two arrays are null. The two array are allocated on demand and
     * cursorLocks is checked for zero before use.
     *
     * Access to these fields is synchronized on 'this' only because multiple
     * threads may call copyCursorLocks. (Txn access is otherwise
     * single-threaded.)
     */
    private static final int INITIAL_CURSOR_LOCKS = 3;
    private int cursorLocks = 0;
    private CursorImpl[] cursorLockers;
    private long[] cursorLockLsns;

    private static final int READ_LOCK_OVERHEAD =
        MemoryBudget.HASHSET_ENTRY_OVERHEAD;
    private static final int WRITE_LOCK_OVERHEAD =
        MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
        MemoryBudget.WRITE_LOCKINFO_OVERHEAD;

    /*
     * We have to keep a set of DbCleanup objects so after commit or abort of
     * Environment.truncateDatabase(), removeDatabase(), or renameDatabase we
     * can appropriately delete or update the MapLN and DatabaseImpl.
     * Synchronize access to this set on this object.
     */
    protected Set<DbCleanup> dbCleanupSet;

    /*
     * We need a map of the latest databaseImpl objects to drive the undo
     * during an abort, because it's too hard to look up the database object in
     * the mapping tree. (The normal code paths want to take locks, add
     * cursors, etc.
     */
    protected Map<DatabaseId, DatabaseImpl> undoDatabases;

    /**
     * @see #addOpenedDatabase
     * @see HandleLocker
     */
    protected Set<Database> openedDatabaseHandles;

    /*
     * First LSN logged for this transaction -- used for keeping track of the
     * first active LSN point, for checkpointing. This field is not persistent.
     *
     * [#16861] This field is volatile to avoid making getFirstActiveLsn
     * synchronized, which causes a deadlock in HA.
     */
    protected volatile long firstLoggedLsn = NULL_LSN;

    /*
     * Last LSN logged for this transaction. Serves as the handle onto the
     * chained log entries belonging to this transaction. Is persistent.
     */
    protected volatile long lastLoggedLsn = NULL_LSN;

    /*
     * The LSN used to commit the transaction. One of commitLSN or abortLSN
     * must be set after a commit() or abort() operation. Note that a commit()
     * may set abortLSN, if the commit failed, and the transaction had to be
     * aborted.
     */
    private volatile long commitLsn = NULL_LSN;

    /* The LSN used to record the abort of the transaction. */
    private long abortLsn = NULL_LSN;

    /* The configured durability at the time the transaction was created. */
    private final Durability defaultDurability;

    /* The durability used for the actual commit. */
    private Durability commitDurability;

    /* Whether to use Read-Committed isolation. */
    private final boolean readCommittedIsolation;

    protected volatile boolean isGroupCommitted = false;

    /*
     * In-memory size, in bytes. A Txn tracks the memory needed for itself and
     * the readlock, writeInfo, undoDatabases, and dbCleanupSet collections,
     * including the cost of each collection entry. However, the actual Lock
     * object memory cost is maintained within the Lock class.
     */
    private int inMemorySize;

    /*
     * Accumulated memory budget delta. Once this exceeds ACCUMULATED_LIMIT we
     * inform the MemoryBudget that a change has occurred.
     */
    private int accumulatedDelta = 0;

    /*
     * The set of databases for which triggers were invoked during the
     * course of this transaction. It's null if no triggers were invoked.
     */
    private Set<DatabaseImpl> triggerDbs = null;

    /*
     * The user Transaction handle associated with this Txn. It's null if there
     * isn't one, e.g. it's an internal transaction.
     */
    private Transaction transaction;

    /*
     * The System.nanotime when the txn was initiated. The value is only used
     * by the subtypes MasterTxn and ReplayTxn, where the iv is initialized by
     * their respective constructors.
     */
    protected long startNs = 0;

    /*
     * Max allowable accumulation of memory budget changes before MemoryBudget
     * should be updated. This allows for consolidating multiple calls to
     * updateXXXMemoryBudget() into one call. Not declared final so that unit
     * tests can modify this. See SR 12273.
     */
    public static int ACCUMULATED_LIMIT = 10000;

    /*
     * Each Txn instance has a handle on a ReplicationContext instance for use
     * in logging a TxnCommit or TxnAbort log entries.
     */
    protected ReplicationContext repContext;

    /* Determines whether the transaction is auto-commit */
    private boolean isAutoCommit = false;

    private final boolean readOnly;

    private TestHook<RuntimeException> preCommitExceptionHook;
    private TestHook<MasterTxn> masterTransferHook;

    /**
     * Constructor for reading from log.
     */
    public Txn() {
        defaultDurability = null;
        readCommittedIsolation = false;
        repContext = null;
        readOnly = false;
    }

    protected Txn(EnvironmentImpl envImpl,
                  TransactionConfig config,
                  ReplicationContext repContext) {
        this(envImpl, config, repContext, 0L /*mandatedId */ );
    }

    /**
     * A non-zero mandatedId is specified only by subtypes which arbitrarily
     * impose a transaction id value onto the transaction. This is done by
     * implementing a version of Locker.generateId() which uses the proposed
     * id.
     */
    @SuppressWarnings("deprecation")
    protected Txn(EnvironmentImpl envImpl,
                  TransactionConfig config,
                  ReplicationContext repContext,
                  long mandatedId)
        throws DatabaseException {

        /*
         * Initialize using the config but don't hold a reference to it, since
         * it has not been cloned.
         */
        super(envImpl, config.getReadUncommitted(), config.getNoWait(),
              mandatedId);
        this.repContext = repContext;

        readCommittedIsolation = config.getReadCommitted();
        if (config.getDurability() == null) {
            defaultDurability = config.getDefaultDurability(envImpl);
        } else {
            defaultDurability = config.getDurability();
        }

        readOnly = config.getReadOnly();

        final long txnTimeout = config.getTxnTimeout(TimeUnit.MILLISECONDS);
        if (txnTimeout != -1) {
            setTxnTimeout(txnTimeout);
        }

        lastLoggedLsn = NULL_LSN;
        firstLoggedLsn = NULL_LSN;

        txnFlags = 0;
        setState(Transaction.State.OPEN);

        txnBeginHook(config);

        /*
         * Note: readLocks, writeInfo, undoDatabases, deleteDatabases are
         * initialized lazily in order to conserve memory. WriteInfo and
         * undoDatabases are treated as a package deal, because they are both
         * only needed if a transaction does writes.
         *
         * When a lock is added to this transaction, we add the collection
         * entry overhead to the memory cost, but don't add the lock
         * itself. That's taken care of by the Lock class.
         */
        updateMemoryUsage(MemoryBudget.TXN_OVERHEAD);

        if (registerImmediately()) {
            this.envImpl.getTxnManager().registerTxn(this);
        }
    }

    public static Txn createLocalTxn(EnvironmentImpl envImpl,
                                     TransactionConfig config) {
        return new Txn(envImpl, config, ReplicationContext.NO_REPLICATE);
    }

    public static Txn createLocalAutoTxn(EnvironmentImpl envImpl,
                                         TransactionConfig config) {
        Txn txn = createLocalTxn(envImpl, config);
        txn.isAutoCommit = true;
        return txn;
    }

    /**
     * Make a transaction for a user instigated transaction. Whether the
     * environment is replicated or not determines whether a MasterTxn or
     * a plain local Txn is returned.
     */
    static Txn createUserTxn(EnvironmentImpl envImpl,
                             TransactionConfig config) {

        return envImpl.isReplicated() ?
            envImpl.createRepUserTxn(config) :
            createLocalTxn(envImpl, config);
    }

    public static Txn createAutoTxn(EnvironmentImpl envImpl,
                                    TransactionConfig config,
                                    ReplicationContext repContext)
        throws DatabaseException {

        final Txn ret =
            envImpl.isReplicated() && repContext.inReplicationStream() ?
            envImpl.createRepUserTxn(config) :
            new Txn(envImpl, config, repContext);

        ret.isAutoCommit = true;
        return ret;
    }

    /**
     * True if this transaction should be registered with the transaction
     * manager immediately at startup. True for all transactions except for
     * those ReplayTxns which were created as transformed master transactions.
     */
    protected boolean registerImmediately() {
        return true;
    }

    /**
     * UserTxns get a new unique id for each instance.
     */
    @Override
    @SuppressWarnings("unused")
    protected long generateId(TxnManager txnManager,
                              long ignore /* mandatedId */) {
        return txnManager.getNextTxnId();
    }

    /**
     * Returns the System.nanoTime() time that the transaction was started. On
     * the master it represents the time that the transaction was started by
     * the application. During replay it's the time that the transaction was
     * materialized due to the first replicated change that was part of the
     * transaction.
     */
    public long getStartNs() {
        return startNs;
    }

    /**
     * Access to last LSN.
     */
    public long getLastLsn() {
        return lastLoggedLsn;
    }

    /**
     * Returns the commitLsn or the NULL_LSN if a commit record has not been
     * written
     */
    protected long getCommitLsn() {
        return commitLsn;
    }

    /**
     * Returns the durability used for the commit operation. It's only
     * available after a commit operation has been initiated.
     *
     * @return the durability associated with the commit, or null if the
     * commit has not yet been initiated.
     */
    public Durability getCommitDurability() {
        return commitDurability;
    }

    /**
     * Returns the durability associated the transaction at the time it's first
     * created.
     *
     * @return the durability associated with the transaction at creation.
     */
    public Durability getDefaultDurability() {
        return defaultDurability;
    }

    protected void setRollback() {
        txnFlags |= PAST_ROLLBACK;
    }

    /**
     * @return if this transaction has ever executed a rollback.
     * A Rollback is an undo of the transaction that can return either to the
     * original pre-txn state, or to an intermediate intra-txn state. An abort
     * always returns the txn to the pre-txn state.
     */
    @Override
    public boolean isRolledBack() {
        return (txnFlags & PAST_ROLLBACK) != 0;
    }

    /**
     * Gets a lock on this LSN and, if it is a write lock, saves an abort
     * LSN. Caller will set the abortLsn later, after the write lock has been
     * obtained.
     *
     * @throws IllegalStateException via API read/write methods if the txn is
     * closed, in theory.  However, this should not occur from a user API call,
     * because the API methods first call Transaction.getLocker, which will
     * throw IllegalStateException if the txn is closed.  It might occur,
     * however, if the transaction ends in the window between the call to
     * getLocker and the lock attempt.
     *
     * @throws OperationFailureException via API read/write methods if an
     * OperationFailureException occurred earlier and set the txn to
     * abort-only.
     *
     * @see Locker#lockInternal
     */
    @Override
    protected LockResult lockInternal(long lsn,
                                      LockType lockType,
                                      boolean noWait,
                                      boolean jumpAheadOfWaiters,
                                      DatabaseImpl database,
                                      CursorImpl cursor)
        throws DatabaseException {

        long timeout = 0;
        boolean useNoWait = noWait || defaultNoWait;
        synchronized (this) {
            checkState(false);
            if (!useNoWait) {
                timeout = getLockTimeout();
            }
        }

        /* Ask for the lock. */
        LockGrantType grant = lockManager.lock
            (lsn, this, lockType, timeout, useNoWait, jumpAheadOfWaiters,
             database);

        WriteLockInfo info = null;
        if (writeInfo != null && database != null) {
            if (grant != LockGrantType.DENIED && lockType.isWriteLock()) {
                synchronized (this) {
                    info = writeInfo.get(lsn);
                    /* Save the latest version of this database for undoing. */
                    undoDatabases.put(database.getId(), database);
                }
            }
        }

        if (readCommittedIsolation &&
            grant != LockGrantType.DENIED &&
            !lockType.isWriteLock() &&
            cursor != null) {
            addCursorLock(lsn, cursor);
        }

        return new LockResult(grant, info);
    }

    @Override
    public synchronized void releaseLock(long lsn, CursorImpl cursor)
        throws DatabaseException {

        if (readCommittedIsolation && !removeCursorLock(lsn, cursor)) {
            return;
        }

        lockManager.release(lsn, this);
        removeLock(lsn);
    }

    @Override
    public synchronized void demoteLock(long lsn, CursorImpl cursor)
        throws DatabaseException {

        if (lockManager.demote(lsn, this) &&
            readCommittedIsolation &&
            cursor != null) {
            addCursorLock(lsn, cursor);
        }
    }

    /**
     * Prepare to undo in the (very unlikely) event that logging succeeds but
     * locking fails. Subclasses should call super.preLogWithoutLock. [#22875]
     */
    @Override
    public synchronized void preLogWithoutLock(DatabaseImpl database) {
        ensureWriteInfo();
        undoDatabases.put(database.getId(), database);
    }

    /**
     * Call commit() with the default sync configuration property.
     */
    public long commit()
        throws DatabaseException {

        return commit(defaultDurability);
    }

    /**
     * Commit this transaction; it involves the following logical steps:
     *
     * 1. Run pre-commit hook.
     *
     * 2. Release read locks.
     *
     * 3. Log a txn commit record and flush the log as indicated by the
     * durability policy.
     *
     * 4. Run the post-commit hook.
     *
     * 5. Add deleted LN info to IN compressor queue.
     *
     * 6. Release all write locks
     *
     * If this transaction has not made any changes to the database, that is,
     * it is a read-only transaction, no entry is made to the log. Otherwise,
     * a concerted effort is made to log a commit entry, or an abort entry,
     * but NOT both. If exceptions are encountered and neither entry can be
     * logged, a EnvironmentFailureException is thrown.
     *
     * Error conditions (in contrast to Exceptions) always result in the
     * environment being invalidated and the Error being propagated back to the
     * application.  In addition, if the environment is made invalid in another
     * thread, or the transaction is closed by another thread, then we
     * propagate the exception and we do not attempt to abort.  This special
     * handling is prior to the pre-commit stage.
     *
     * From an exception handling viewpoint the commit goes through two stages:
     * a pre-commit stage spanning steps 1-3, and a post-commit stage
     * spanning steps 4-5. The post-commit stage is entered only after a commit
     * entry has been successfully logged.
     *
     * Any exceptions detected during the pre-commit stage results in an
     * attempt to log an abort entry. A NULL commitLsn (and abortLsn)
     * indicates that we are in the pre-commit stage. Note in particular, that
     * if the log of the commit entry (step 3) fails due to an IOException,
     * then the lower levels are responsible for wrapping it in a
     * EnvironmentFailureException which is propagated directly to the
     * application.
     *
     * Exceptions thrown in the post-commit stage are examined to see if they
     * are expected and must be propagated back to the caller after completing
     * any pending cleanup; some replication exceptions fall into this
     * category. If the exception was unexpected, the environment is
     * invalidated and a EnvironmentFailureException is thrown instead. The
     * current implementation only allows propagation of exceptions from the
     * post-commit hook, since we do not expect exceptions from any of the
     * other post-commit operations.
     *
     * When there are multiple failures in commit(), we want the caller to
     * receive the first exception, to make the problem manifest. So an effort
     * is made to preserve that primary exception and propagate it instead of
     * any following, secondary exceptions. The secondary exception is always
     * logged in such a circumstance.
     *
     * @throws IllegalStateException via Transaction.commit if cursors are
     * open.
     *
     * @throws OperationFailureException via Transaction.commit if an
     * OperationFailureException occurred earlier and set the txn to
     * abort-only.
     *
     * Note that IllegalStateException should never be thrown by
     * Transaction.commit because of a closed txn, since Transaction.commit and
     * abort set the Transaction.txn to null and disallow subsequent method
     * calls (other than abort).  So in a sense the call to checkState(true) in
     * this method is unnecessary, although perhaps a good safeguard.
     */
    public long commit(Durability durability)
        throws DatabaseException {

        /*
         * If frozen, throw the appropriate exception, but don't attempt to
         * make any changes to cleanup the exception.
         */
        checkIfFrozen(true /* isCommit */);

        /*
         * A post commit exception that needs to be propagated back to the
         * caller. Its throw is delayed until the post commit cleanup has been
         * completed.
         */
        DatabaseException queuedPostCommitException = null;

        this.commitDurability = durability;

        /* Tracks whether hook was successfully called and completed. */
        boolean postLogCommitHookCompleted = false;

        /*
         * Stable value for whether the txn made any updates before the
         * commit record is written.
         *
         * Initialize with temporary true value to keep javac happy. Real
         * value is set unconditionally in the sync block.
         */
        boolean updateLoggedForTxn = true;
        try {
            assert(TestHookExecute.doHookIfSet(masterTransferHook));
            synchronized (this) {
                updateLoggedForTxn = updateLoggedForTxn();
                checkState(false);
                if (checkCursorsForClose()) {
                    throw new IllegalStateException
                        ("Transaction " + id +
                         " commit failed because there were open cursors.");
                }

                /*
                 * Do the pre-commit hook before executing any commit related
                 * actions like releasing locks.
                 */
                if (updateLoggedForTxn) {
                    checkLockInvariant();
                    preLogCommitHook();
                }

                /*
                 * Release all read locks, clear lock collection. Optimize for
                 * the case where there are no read locks.
                 */
                int numReadLocks = clearReadLocks();

                /*
                 * Log the commit if we ever logged any modifications for this
                 * txn. Refraining from logging empty commits is more efficient
                 * and makes for fewer edge cases for HA. Note that this is not
                 * the same as the question of whether we have held any write
                 * locks. Various scenarios, like RMW txns and
                 * Cursor.putNoOverwrite can take write locks without having
                 * actually made any modifications.
                 *
                 * If we have outstanding write locks, we must release them
                 * even if we won't log a commit.  TODO: This may have been
                 * true in the past because of dbhandle write locks that were
                 * transferred away, but is probably no longer true.
                 */
                int numWriteLocks = 0;
                Collection<WriteLockInfo> obsoleteLsns = null;
                if (writeInfo != null) {
                    numWriteLocks = writeInfo.size();
                    obsoleteLsns = getObsoleteLsnInfo();
                }

                /*
                 * If nothing was written to log for this txn, no need to log a
                 * commit.
                 */
                if (updateLoggedForTxn) {
                    // commitDurability maybe modified in preLogCommitHook()
                    final LogItem commitItem =
                        logCommitEntry(commitDurability.getLocalSync(),
                                       obsoleteLsns);
                    commitLsn = commitItem.lsn;

                    boolean queuedCommit = false;
                    try {
                        queuedCommit = postLogCommitHook(commitItem);
                        postLogCommitHookCompleted = true;
                    } catch (DatabaseException hookException) {
                        if (txnState == Transaction.State.MUST_ABORT) {
                            throw EnvironmentFailureException.
                                unexpectedException
                                ("postLogCommitHook may not set MUST_ABORT",
                                 hookException);
                        }
                        if (!propagatePostCommitException(hookException)) {
                            throw hookException;
                        }
                        queuedPostCommitException = hookException;
                    } finally {
                        /* See ArbiterFeederSource.addCommit. */
                        if (!queuedCommit) {
                            commitItem.decrementUse();
                        }
                    }
                }

                /*
                 * Set database state for DB ops before releasing any write
                 * locks.
                 */
                setDbCleanupState(true);

                /* Release all write locks, clear lock collection. */
                if (numWriteLocks > 0) {
                    releaseWriteLocks();
                }
                writeInfo = null;

                /* Unload delete info, but don't wake up the compressor. */
                if ((deleteInfo != null) && deleteInfo.size() > 0) {
                    envImpl.addToCompressorQueue(deleteInfo.values());
                    deleteInfo.clear();
                }
                traceCommit(numWriteLocks, numReadLocks);
            }

            /*
             * Apply any databaseImpl cleanup as a result of the commit. Be
             * sure to do this outside the synchronization block, to avoid
             * conflict w/ checkpointer.
             */
            cleanupDatabaseImpls(true);

            /*
             * Unregister this txn. Be sure to do this outside the
             * synchronization block, to avoid conflict w/ checkpointer.
             */
            close(true);

            if (queuedPostCommitException == null) {
                TriggerManager.runCommitTriggers(this);
                return commitLsn;
            }
        } catch (Error e) {
            envImpl.invalidate(e);
            throw e;
        } catch (RuntimeException commitException) {
            if (!envImpl.isValid()) {
                /* Env is invalid, propagate exception. */
                throw commitException;
            }
            if (commitLsn != NULL_LSN) {
                /* An unfiltered post commit exception */
                throw new EnvironmentFailureException
                    (envImpl,
                     EnvironmentFailureReason.LOG_INCOMPLETE,
                     "Failed after commiting transaction " +
                     id +
                     " during post transaction cleanup." +
                     "Original exception = " +
                     commitException.getMessage(),
                     commitException);
            }

            /*
             * If this transaction is frozen, just bail out, and don't try
             * to clean up with an abort.
             */
            checkIfFrozen(true);

            if (updateLoggedForTxn && !postLogCommitHookCompleted) {
                LoggerUtils.envLogMsg(Level.INFO, envImpl,
                                      "commit txn: " + getId() +
                                      " post log commit abort hook forcibly run" +
                                      " due to preCommitException");
                /* Clean up, for the hooks run by the ensuing abort
                 * operation.
                 */
                postLogCommitAbortHook();
                postLogCommitHookCompleted = true;
            }

            throwPreCommitException(durability, commitException);
            postLogCommitHookCompleted = true;
        } finally {
            if (updateLoggedForTxn && !postLogCommitHookCompleted) {
                LoggerUtils.envLogMsg(Level.INFO, envImpl,
                                      "commit txn: " + getId() +
                                      " post log commit abort hook forcibly run");
                postLogCommitAbortHook();
                postLogCommitHookCompleted = true;
            }

            /*
             * Final catch-all to ensure state is set, in case close(boolean)
             * is not called.
             * Make sure to set COMMITTED state when transaction is indeed
             * committed.
             */
            if (txnState == Transaction.State.OPEN &&
                commitLsn != NULL_LSN) {
                setState(Transaction.State.COMMITTED);
            }
        }
        throw queuedPostCommitException;
    }

    /**
     * Checks lock invariants associated with the txn before it is committed.
     *
     * The caller must have synchronized on the Txn object
     */
    protected void checkLockInvariant() {
        if (getCommitLsn() != DbLsn.NULL_LSN) {
            /*
             * The txn has been committed. Is it an invariant that it's the
             * same as last LoggedLsn? If so, it could be checked here as well.
             */
            return;
        }
        Set<Long> lockedLSNs = getWriteLockIds();
        if ((lastLoggedLsn != DbLsn.NULL_LSN) &&
            !lockedLSNs.contains(lastLoggedLsn)) {
            final String lsns = "[" +
                lockedLSNs.stream().map((l) -> DbLsn.getNoFormatString(l)).
                    collect(Collectors.joining(",")) +
                "]";
            final String msg = "No write lock on lastLoggedLsn=" +
                 DbLsn.getNoFormatString(lastLoggedLsn) +
                 " locked lsns:" + lsns + " txn=" + getId();
            LoggerUtils.severe(envImpl.getLogger(), envImpl, msg);
            throw unexpectedState(envImpl, msg);
        }
    }

    /**
     * Releases all write locks, nulls the lock collection.
     */
    protected void releaseWriteLocks() throws DatabaseException {
        if (writeInfo == null) {
            return;
        }
        for (Long lsn : writeInfo.keySet()) {
            lockManager.release(lsn, this);
        }
        writeInfo = null;
    }

    /**
     * Aborts the current transaction and throws the pre-commit Exception,
     * wrapped in a Database exception if it isn't already a DatabaseException.
     *
     * If the attempt at writing the abort entry fails, that is, if neither an
     * abort entry, nor a commit entry was successfully written to the log, the
     * environment is invalidated and a EnvironmentFailureException is thrown.
     * Note that for HA, it's necessary that either a commit or abort entry be
     * made in the log, so that it can be replayed to the replicas and the
     * transaction is not left in limbo at the other nodes.
     *
     * @param durability used to determine whether the abort record should be
     * flushed to the log.
     * @param preCommitException the exception being handled.
     * @throws DatabaseException this is the normal return for the method.
     */
    private void throwPreCommitException(Durability durability,
                                         RuntimeException preCommitException) {

        try {
            TestHookExecute.doHookIfSet(
                preCommitExceptionHook, preCommitException);

            if (!isClosed()) {
                abortInternal(durability.getLocalSync() == SyncPolicy.SYNC);
            } else {
                /* Asynchronously closed by MasterTxn.convertToReplayTxnAndClose
                 * in the window where we do not hold the synchronization
                 * lock.
                 */
                LoggerUtils.envLogMsg(Level.INFO, envImpl, "txn:" + getId() +
                                      " concurrently closed " +
                                      "abort internal(withoin commit) skipped");
            }

            if (!(preCommitException instanceof InsufficientReplicasException)) {
                LoggerUtils.traceAndLogException(envImpl, "Txn", "commit",
                                                 "Commit of transaction " +
                                                 id + " failed",
                                                 preCommitException);
            }
        } catch (Error e) {
            envImpl.invalidate(e);
            throw e;
        } catch (ReplicaWriteException | UnknownMasterException e) {
            /*
             * Since this is not a known master, it is OK to abort without
             * logging an abort entry. The txn will either be rolled back
             * during sync-up or aborted by the new master
             */
            throw e;
        } catch (RuntimeException abortT2) {
            if (!envImpl.isValid()) {
                /* Env already invalid, propagate exception. */
                throw abortT2;
            }
            String message = "Failed while attempting to commit transaction " +
                    id + ". The attempt to abort also failed. " +
                    "The original exception seen from commit = " +
                    preCommitException.getMessage() +
                    " The exception from the abort = " + abortT2.getMessage();
            if (updateLoggedForTxn() && (abortLsn == NULL_LSN)) {
                /* Failed to log an abort or commit entry on the master. */
                EnvironmentFailureException efe =
                    new EnvironmentFailureException(envImpl,
                     EnvironmentFailureReason.LOG_INCOMPLETE,
                     message, preCommitException);
                efe.addSuppressed(abortT2);
                throw efe;
            }

            /*
             * An abort entry has been written, so we can proceed. Log the
             * secondary exception, but throw the more meaningful original
             * exception further below.
             */
            LoggerUtils.envLogMsg(Level.WARNING, envImpl, message);
            /* The preCommitException exception will be thrown below. */
        }

        /*
         * Abort entry was written, wrap the exception if necessary and throw
         * it.  An IllegalStateException is thrown by commit() when cursors are
         * open.
         */
        if (preCommitException instanceof DatabaseException ||
            preCommitException instanceof IllegalStateException) {
            throw preCommitException;
        }

        /* Now throw an exception that shows the commit problem. */
        throw EnvironmentFailureException.unexpectedException
            ("Failed while attempting to commit transaction " +
              id + ", aborted instead. Original exception = " +
              preCommitException.getMessage(),
              preCommitException);
    }

    /**
     * Creates and logs the txn commit entry, enforcing the flush/Sync
     * behavior.
     *
     * @param flushSyncBehavior the local durability requirements
     *
     * @return the committed log item
     */
    private LogItem logCommitEntry(SyncPolicy flushSyncBehavior,
                                   Collection<WriteLockInfo> obsoleteLsns)
        throws DatabaseException {

        LogManager logManager = envImpl.getLogManager();
        assert checkForValidReplicatorNodeId();

        final CommitLogEntry commitEntry =
            new CommitLogEntry(new TxnCommit(id,
                                             lastLoggedLsn,
                                             getMasterIdTermSupplier(),
                                             getDTVLSN(),
                                             getMasterCommitTime()));

        LogParams params = new LogParams();
        params.entry = commitEntry;
        params.provisional = Provisional.NO;
        params.repContext = repContext;
        params.obsoleteWriteLockInfo = obsoleteLsns;

        /*
         * Commit entries are treated specially for caching because they are
         * very frequent but small, and also because they are sent to the
         * arbiter. See ArbiterFeederSource.addCommit.
         */
        params.immutableLogEntry = true;
        params.incLogItemUsage = true;

        switch (flushSyncBehavior) {
            case SYNC:
                params.flushRequired = true;
                params.fsyncRequired = true;
                break;
            case WRITE_NO_SYNC:
                params.flushRequired = true;
                params.fsyncRequired = false;
                break;
            default:
                params.flushRequired = false;
                params.fsyncRequired = false;
                break;
        }

        /*
         * Do a final pre-log check just before the logging call, to minimize
         * the window where the POSSIBLY_COMMITTED state may be set. [#21264]
         */
        preLogCommitCheck();

        /* Log the commit with requested durability. */
        boolean logSuccess = false;
        try {
            LogItem item = logManager.log(params);
            logSuccess = true;
            return item;
        } catch (RuntimeException e) {

            /*
             * Exceptions thrown during logging are expected to be fatal.
             * Ensure that the environment is invalidated when a non-fatal
             * exception is unexpectedly thrown, since the commit durability is
             * unknown [#21264].
             */
            if (envImpl.isValid()) {
                throw EnvironmentFailureException.unexpectedException
                    (envImpl,
                     "Unexpected non-fatal exception while logging commit",
                     e);
            }
            throw e;
        } catch (Error e) {
            /* Ensure that the environment is invalidated. [#21264] */
            envImpl.invalidate(e);
            throw e;
        } finally {

            /*
             * If logging fails, there is still a possibility that the commit
             * is durable. [#21264]
             */
            if (!logSuccess) {
                setState(Transaction.State.POSSIBLY_COMMITTED);
            }
        }
    }

    /**
     * Pre-log check for an invalid environment or interrupted thread (this
     * thread may have been interrupted but we haven't found out yet, because
     * we haven't done a wait or an I/O) to narrow the time window where a
     * commit could become partially durable.  See getPartialDurability.
     * [#21264]
     */
    private void preLogCommitCheck() {
        if (Thread.interrupted()) {
            throw new ThreadInterruptedException
                (envImpl, "Thread interrupted prior to logging the commit");
        }
        envImpl.checkIfInvalid();
    }

    /*
     * A replicated txn must know the node of the master which issued it.
     */
    private boolean checkForValidReplicatorNodeId() {
        if (isReplicated()) {
            if (getMasterId() <= 0) {
                return false;
            }

            /*
            return (repContext.getClientVLSN() != null) &&
                   (!repContext.getClientVLSN().isNull());
                   */
        }
        return true;
    }

    /**
     * Extract obsolete LSN info from writeInfo. Do not add a WriteInfo if a
     * slot with a deleted LN was reused (abortKnownDeleted), to avoid double
     * counting. And count each abortLSN only once.
     */
    private Collection<WriteLockInfo> getObsoleteLsnInfo() {

        /*
         * A Map is used to prevent double counting abortLNS if there is more
         * then one WriteLockInfo with the same abortLSN in this txn, which can
         * occur when the txn has performed more than one CUD ops on the same
         * record.
         */
        /* Optimize collection allocation for frequent use cases of size zero
         * e.g. pure insert, or single key update.
         */

        if (writeInfo.size() == 0) {
            /* check early to avoid materializing empty values() */
            return Collections.emptyList();
        }

        Map<Long, WriteLockInfo> map = null;

        for (WriteLockInfo info : writeInfo.values()) {
            if (!isObsoleteLSN(info)) {
                continue;
            }
            if (map == null) {
                if (writeInfo.size() == 1) {
                    return Collections.singleton(info);
                }
                map = new HashMap<>();
            }
            map.putIfAbsent(info.getAbortLsn(), info);
        }

        return map == null ? Collections.emptyList() : map.values();
    }

    private boolean isObsoleteLSN(WriteLockInfo info) {

        return !((info.getAbortLsn() == DbLsn.NULL_LSN ||
                  info.getAbortKnownDeleted()) ||
                 /* Was already counted obsolete during logging. */
                 ((info.getDb() != null) &&
                   info.getDb().isLNImmediatelyObsolete()) ||
                 /* Was already counted obsolete during logging. */
                 (info.getAbortData() != null));
    }

    /**
     * Abort this transaction. This flavor does not return an LSN, nor does it
     * require the logging of a durable abort record.
     */
    public void abort()
        throws DatabaseException {

        if (isClosed()) {
            return;
        }
        abort(false /* forceFlush */);
    }

    /*
     * For testing, forces an abort to occur without logging an abort log
     * entry.
     */
    public synchronized void testAbortNoLog() {
        /*
         * ABORTED state will cause an exception up front, preventing the
         * logging of the abort entry. The undo and close are also prevented,
         * so must be done explicitly to create the scenario for the bug.
         */
        try {
            txnState = Transaction.State.ABORTED;
            abortInternal(false);
        } finally {
            try {
                undo();
                close(false);
            } catch (Throwable e) {
                e.printStackTrace();
                assert false : "Unexpected exception";
            }
        }
    }

    /**
     * Abort this transaction. Steps are:
     * 1. Release LN read locks.
     * 2. Write a txn abort entry to the log. This is used for log file
     *    cleaning optimization and replication, and there's no need to
     *    guarantee a flush to disk.
     * 3. Find the last LN log entry written for this txn, and use that
     *    to traverse the log looking for nodes to undo. For each node,
     *    use the same undo logic as recovery to undo the transaction. Note
     *    that we walk the log in order to undo in reverse order of the
     *    actual operations. For example, suppose the txn did this:
     *       delete K1/D1 (in LN 10)
     *       create K1/D1 (in LN 20)
     *    If we process LN10 before LN 20, we'd inadvertently create a
     *    duplicate tree of "K1", which would be fatal for the mapping tree.
     * 4. Release the write lock for this LN.
     *
     * An abort differs from a rollback in that the former always undoes every
     * operation, and returns it to the pre-txn state. A rollback may return
     * the txn to an intermediate state, or to the pre-txn state.
     */
    public long abort(boolean forceFlush)
        throws DatabaseException {

        return abortInternal(forceFlush);
    }

    /**
     * @throws IllegalStateException via Transaction.abort if cursors are open.
     *
     * Note that IllegalStateException should never be thrown by
     * Transaction.abort because of a closed txn, since Transaction.commit and
     * abort set the Transaction.txn to null and disallow subsequent method
     * calls (other than abort).  So in a sense the call to checkState(true) in
     * this method is unnecessary, although perhaps a good safeguard.
     */
    private long abortInternal(boolean forceFlush)
        throws DatabaseException {

        /*
         * If frozen, throw the appropriate exception, but don't attempt to
         * make any changes to cleanup the exception.
         */
        boolean hooked = false;
        checkIfFrozen(false);

        boolean postLogAbortHookCompleted = false;
        boolean updateLoggedForTxn = true;
        boolean abortEntryMissing = false;
        try {
            try {
                synchronized (this) {
                    updateLoggedForTxn = updateLoggedForTxn();
                    checkState(true);

                    /*
                     * State is set to ABORTED before undo, so that other
                     * threads cannot access this txn in the middle of undo.
                     * [#19321]
                     */
                    setState(Transaction.State.ABORTED);

                    /* Log the abort. */
                    if (updateLoggedForTxn) {
                        preLogAbortHook();
                        hooked = true;
                        assert checkForValidReplicatorNodeId();
                        assert (commitLsn == NULL_LSN) &&
                               (abortLsn == NULL_LSN);
                        final AbortLogEntry abortEntry =
                            new AbortLogEntry(
                                new TxnAbort(id, lastLoggedLsn,
                                             getMasterIdTermSupplier(),
                                             getDTVLSN()));
                        abortLsn = forceFlush ?
                            envImpl.getLogManager().
                            logForceFlush(abortEntry,
                                          true /* fsyncRequired */,
                                          repContext) :
                            envImpl.getLogManager().log(abortEntry,
                                                        repContext);
                    }
                }
            } finally {
                /*
                 * If the abort entry was not logged, do not release locks or
                 * close the txn in order to prevent further damage. Instead we
                 * may wish to invalidate the env in the future, but
                 * interactions with master transfer are undetermined (TODO).
                 */
                abortEntryMissing = (abortLsn == NULL_LSN && updateLoggedForTxn);

                if (hooked) {
                    postLogAbortHook();
                    postLogAbortHookCompleted = true;
                }

                /*
                 * undo must be called outside the synchronization block to
                 * preserve locking order: For non-blocking locks, the BIN
                 * is latched before synchronizing on the Txn.  If we were
                 * to synchronize while calling undo, this order would be
                 * reversed.
                 */
                if (!(isFrozen() || abortEntryMissing)) {
                    undo();
                }
            }

            synchronized (this) {
                /* Invalidate any Db handles protected by this txn. */
                if (openedDatabaseHandles != null) {
                    for (Database handle : openedDatabaseHandles) {
                        DbInternal.invalidate(handle);
                    }
                }
            }

            /*
             * Apply any databaseImpl cleanup as a result of the abort. Be
             * sure to do this outside the synchronization block, to avoid
             * conflict w/ checkpointer. It must also be done after
             * invalidating any open DB handles above.
             */
            if (!(isFrozen() || abortEntryMissing)) {
                cleanupDatabaseImpls(false);
            }

            synchronized (this) {
                boolean openCursors = checkCursorsForClose();
                Logger logger = envImpl.getLogger();
                if (logger.isLoggable(Level.FINE)) {
                    LoggerUtils.fine(logger, envImpl,
                                     "Abort: id = " + id + " openCursors= " +
                                     openCursors);
                }
                /* Delay the exception until cleanup is complete. */
                if (openCursors) {
                    envImpl.checkIfInvalid();
                    throw new IllegalStateException
                            ("Transaction " + id +
                             " detected open cursors while aborting");
                }
            }
        } finally {
            if (updateLoggedForTxn && !postLogAbortHookCompleted) {

                LoggerUtils.envLogMsg(Level.INFO, envImpl,
                                      "txn abort: " + getId() +
                                      " post log abort hook forcibly run");
                postLogAbortHook();
            }

            /*
             * The close method, which unregisters the txn, and must be called
             * after undo and cleanupDatabaseImpls.  A transaction must remain
             * registered until all actions that modify/dirty INs are complete;
             * see Checkpointer class comments for details.  [#19321]
             *
             * close must be called, even though the state has already been set
             * to ABORTED above, for two reasons: 1) To unregister the txn, and
             * 2) to allow subclasses to override the close method.
             *
             * close must be called outside the synchronization block to avoid
             * conflict w/ checkpointer.
             */
            if (isFrozen() || abortEntryMissing) {
                setState(Transaction.State.MUST_ABORT);
            } else {
                close(false);
            }

            if (abortLsn != NULL_LSN) {
                TriggerManager.runAbortTriggers(this);
            }
        }

        return abortLsn;
    }

    /**
     * Undo write operations and release all resources held by the transaction.
     */
    protected void undo()
        throws DatabaseException {

        /*
         * We need to undo, or reverse the effect of any applied operations on
         * the in-memory btree. We also need to make the latest version of any
         * record modified by the transaction obsolete.
         */
        Set<Long> alreadyUndoneLsns = new HashSet<>();
        Set<CompareSlot> alreadyUndoneSlots = new TreeSet<>();
        TreeLocation location = new TreeLocation();
        long undoLsn = lastLoggedLsn;

        try {
            while (undoLsn != NULL_LSN) {
                UndoReader undo =
                    UndoReader.create(envImpl, undoLsn, undoDatabases);
                /*
                 * Only undo the first instance we see of any node. All log
                 * entries for a given node have the same abortLsn, so we don't
                 * need to undo it multiple times.
                 */
                if (firstInstance(
                    alreadyUndoneLsns, alreadyUndoneSlots, undo)) {

                    RecoveryManager.abortUndo(
                        envImpl.getLogger(), Level.FINER, location,
                        undo.db, undo.logEntry, undoLsn);

                    countObsoleteExact(undoLsn, undo, isRolledBack());
                }

                /* Move on to the previous log entry for this txn. */
                undoLsn = undo.logEntry.getUserTxn().getLastLsn();
            }
        } catch (DatabaseException e) {
            String lsnMsg = "LSN=" + DbLsn.getNoFormatString(undoLsn);
            LoggerUtils.traceAndLogException(envImpl, "Txn", "undo",
                                             lsnMsg, e);
            e.addErrorMessage(lsnMsg);
            throw e;
        } catch (RuntimeException e) {
            throw EnvironmentFailureException.unexpectedException
                ("Txn undo for LSN=" + DbLsn.getNoFormatString(undoLsn), e);
        }

        /*
         * Release all read locks after the undo (since the undo may need to
         * read in mapLNs).
         */
        if (readLocks != null) {
            clearReadLocks();
        }

        /* Set database state for DB ops before releasing any write locks. */
        setDbCleanupState(false);

        /* Throw away write lock collection, don't retain any locks. */
        Set<Long> empty = Collections.emptySet();
        clearWriteLocks(empty);

        /*
         * Let the delete related info (binreferences and dbs) get gc'ed. Don't
         * explicitly iterate and clear -- that's far less efficient, gives GC
         * wrong input.
         */
        deleteInfo = null;
    }

    /**
     * For an explanation of obsoleteDupsAllowed, see ReplayTxn.rollback.
     */
    private void countObsoleteExact(long undoLsn, UndoReader undo,
                                    boolean obsoleteDupsAllowed) {
        /*
         * "Immediately obsolete" LNs are counted as obsolete when they are
         * logged, so no need to repeat here.
         */
        if (undo.logEntry.isImmediatelyObsolete(undo.db)) {
            return;
        }

        LogManager logManager = envImpl.getLogManager();

        if (obsoleteDupsAllowed) {
            logManager.countObsoleteNodeDupsAllowed
                (undoLsn,
                 null, // type
                 undo.logEntrySize);
        } else {
            logManager.countObsoleteNode(undoLsn,
                                         null,  // type
                                         undo.logEntrySize,
                                         true); // countExact
        }
    }

    /**
     * Release any write locks that are not in the retainedNodes set.
     */
    protected void clearWriteLocks(Set<Long> retainedNodes)
        throws DatabaseException {

        if (writeInfo == null) {
            return;
        }

        /* Release all write locks, clear lock collection. */
        Iterator<Map.Entry<Long, WriteLockInfo>> iter =
            writeInfo.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, WriteLockInfo> entry = iter.next();
            Long lsn = entry.getKey();

            /* Release any write locks not in the retained set. */
            if (!retainedNodes.contains(lsn)) {
                lockManager.release(lsn, this);
                iter.remove();
            }
        }

        if (writeInfo.size() == 0) {
            writeInfo = null;
        }
    }

    protected int clearReadLocks()
        throws DatabaseException {

        int numReadLocks = 0;
        if (readLocks != null) {
            numReadLocks = readLocks.size();
            for (Long rLockNid : readLocks) {
                lockManager.release(rLockNid, this);
            }
            readLocks = null;
        }

        cursorLocks = 0;
        cursorLockers = null;
        cursorLockLsns = null;

        return numReadLocks;
    }

    /**
     * Called by LNLogEntry.postLogWork() via the LogManager (while still under
     * the LWL) after a transactional LN is logged. Also called by the recovery
     * manager when logging a transaction aware object.
     *
     * This method is synchronized by the caller, by being called within the
     * log latch. Record the last LSN for this transaction, to create the
     * transaction chain, and also record the LSN in the write info for abort
     * logic.
     */
    public synchronized void addLogInfo(long lastLsn) {
        /* Save the last LSN for maintaining the transaction LSN chain. */
        lastLoggedLsn = lastLsn;

        /*
         * Save handle to LSN for aborts.
         *
         * If this is the first LSN, save it for calculating the first LSN
         * of any active txn, for checkpointing.
         */
        if (firstLoggedLsn == NULL_LSN) {
            firstLoggedLsn = lastLsn;
        }
    }

    /**
     * [#16861] The firstLoggedLsn field is volatile to avoid making
     * getFirstActiveLsn synchronized, which causes a deadlock in HA.
     *
     * @return first logged LSN, to aid recovery undo
     */
    public long getFirstActiveLsn() {
        return firstLoggedLsn;
    }

    /**
     * @return true if this txn has logged any log entries.
     */
    protected boolean updateLoggedForTxn() {
        return (lastLoggedLsn != DbLsn.NULL_LSN);
    }

    @Override
    public synchronized void addDbCleanup(final DbCleanup cleanup) {

        int delta = 0;
        if (dbCleanupSet == null) {
            dbCleanupSet = new HashSet<>();
            delta += MemoryBudget.HASHSET_OVERHEAD;
        }

        dbCleanupSet.add(cleanup);

        delta += MemoryBudget.HASHSET_ENTRY_OVERHEAD +
            MemoryBudget.OBJECT_OVERHEAD;
        updateMemoryUsage(delta);

        /* releaseDb will be called by cleanupDatabaseImpls. */
    }

    public Set<DbCleanup> getDbCleanupSet() {
        return dbCleanupSet;
    }

    /*
     * Leftover databaseImpls that are a by-product of database operations like
     * removeDatabase(), truncateDatabase() and renameDatabase() will be
     * deleted or updated after the write locks are released. However, do
     * set the database state appropriately before the locks are released.
     */
    protected void setDbCleanupState(boolean isCommit) {
        if (dbCleanupSet != null) {
            for (DbCleanup cleanup : dbCleanupSet) {
                DbCleanup.setState(cleanup, isCommit);
            }
        }
    }

    /**
     * Cleanup leftover databaseImpls that are a by-product of database
     * operations like removeDatabase, truncateDatabase and renameDatabase.
     *
     * [#16861] We use to avoid cleanup under the txn latch.
     * this special handling is no longer needed, now
     * that firstLoggedLsn is volatile and getFirstActiveLsn is not
     * synchronized.
     */
    protected void cleanupDatabaseImpls(boolean isCommit) {
        cleanupDatabaseImpls(isCommit, DbLsn.NULL_LSN);
    }

    /**
     * Cleanup leftover databaseImpls up to the given lsn.
     */
    protected void cleanupDatabaseImpls(boolean isCommit, long lsn)
        throws DatabaseException {

        if (dbCleanupSet == null) {
            return;
        }

        /* Make a copy of the deleted databases while synchronized. */
        final DbCleanup[] array;
        synchronized (this) {
            array = new DbCleanup[dbCleanupSet.size()];
            dbCleanupSet.toArray(array);
        }

        DbCleanup.execute(envImpl, array, isCommit, lsn);

        if (lsn == DbLsn.NULL_LSN) {
            dbCleanupSet = null;
        } else {
            /*
             * Remove any cleanups that would have been executed on a partial
             * rollback.
             */
            synchronized (this) {
                dbCleanupSet.removeIf(
                    cleanup -> (cleanup.checkIfCleanedUpOnPartial(lsn)));
            }
        }
    }

    private synchronized void ensureWriteInfo() {
        if (writeInfo == null) {
            writeInfo = new HashMap<>();
            undoDatabases = new HashMap<>();
            updateMemoryUsage(MemoryBudget.TWOHASHMAPS_OVERHEAD);
        }
    }

    /**
     * Add lock to the appropriate queue.
     */
    @Override
    protected synchronized void addLock(Long lsn,
                                        LockType type,
                                        LockGrantType grantStatus) {
        if (type.isWriteLock()) {

            ensureWriteInfo();
            writeInfo.put(lsn, new WriteLockInfo());

            int delta = WRITE_LOCK_OVERHEAD;

            if ((grantStatus == LockGrantType.PROMOTION) ||
                (grantStatus == LockGrantType.WAIT_PROMOTION)) {
                readLocks.remove(lsn);
                delta -= READ_LOCK_OVERHEAD;
            }
            updateMemoryUsage(delta);
        } else {
            addReadLock(lsn);
        }
    }

    private void addReadLock(Long lsn) {
        int delta = 0;
        if (readLocks == null) {
            readLocks = new HashSet<>();
            delta = MemoryBudget.HASHSET_OVERHEAD;
        }

        readLocks.add(lsn);
        delta += READ_LOCK_OVERHEAD;
        updateMemoryUsage(delta);
    }

    /**
     * Remove the lock from the set owned by this transaction. If specified to
     * LockManager.release, the lock manager will call this when its releasing
     * a lock. Usually done because the transaction doesn't need to really keep
     * the lock, i.e for a deleted record.
     */
    @Override
    protected synchronized void removeLock(long lsn) {

        /*
         * We could optimize by passing the lock type so we know which
         * collection to look in. Be careful of demoted locks, which have
         * shifted collection.
         *
         * Don't bother updating memory utilization here -- we'll update at
         * transaction end.
         */
        if ((readLocks != null) &&
            readLocks.remove(lsn)) {
            updateMemoryUsage(0 - READ_LOCK_OVERHEAD);
        } else if ((writeInfo != null) &&
                   (writeInfo.remove(lsn) != null)) {
            updateMemoryUsage(0 - WRITE_LOCK_OVERHEAD);
        }
    }

    /**
     * A lock is being demoted. Move it from the write collection into the read
     * collection.
     */
    @Override
    @SuppressWarnings("unused")
    synchronized void moveWriteToReadLock(long lsn, Lock lock) {

        boolean found = false;
        if ((writeInfo != null) &&
            (writeInfo.remove(lsn) != null)) {
            found = true;
            updateMemoryUsage(0 - WRITE_LOCK_OVERHEAD);
        }

        assert found : "Couldn't find lock for Node " + lsn +
            " in writeInfo Map.";
        addReadLock(lsn);
    }

    private void updateMemoryUsage(int delta) {
        inMemorySize += delta;
        accumulatedDelta += delta;
        if (accumulatedDelta > ACCUMULATED_LIMIT ||
            accumulatedDelta < -ACCUMULATED_LIMIT) {
            envImpl.getMemoryBudget().updateTxnMemoryUsage(accumulatedDelta);
            accumulatedDelta = 0;
        }
    }

    /**
     * Returns the amount of memory currently budgeted for this transaction.
     */
    int getBudgetedMemorySize() {
        return inMemorySize - accumulatedDelta;
    }

    public long getAuthoritativeTimeout() {
        throw new UnsupportedOperationException(
            "This transaction does not support authoritative master.");
    }

    /**
     * @return the WriteLockInfo for this node.
     */
    @Override
    public WriteLockInfo getWriteLockInfo(long lsn) {
        WriteLockInfo wli = null;
        synchronized (this) {
            if (writeInfo != null) {
                wli = writeInfo.get(lsn);
            }
        }

        if (wli == null) {
            throw EnvironmentFailureException.unexpectedState
                ("writeInfo is null in Txn.getWriteLockInfo");
        }
        return wli;
    }

    /**
     * Is always transactional.
     */
    @Override
    public boolean isTransactional() {
        return true;
    }

    /**
     * Determines whether this is an auto transaction.
     */
    public boolean isAutoTxn() {
        return isAutoCommit;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Is read-committed isolation if so configured.
     */
    @Override
    public boolean isReadCommittedIsolation() {
        return readCommittedIsolation;
    }

    /**
     * This is a transactional locker.
     */
    @Override
    public Txn getTxnLocker() {
        return this;
    }

    /**
     * Returns 'this', since this locker holds no non-transactional locks.
     * Since this is returned, sharing of locks is obviously supported.
     */
    @Override
    public Locker newNonTxnLocker() {
        return this;
    }

    /**
     * When the txn is configured for read-committed, read locks are
     * considered non-transactional since they are released after each
     * operation.
     */
    @Override
    public synchronized void releaseNonTxnLocks(final CursorImpl cursor) {
        if (!readCommittedIsolation || readLocks == null) {
            return;
        }

        /*
         * Remove associations for the cursor, and release the lock if no
         * associations with other cursors exists.
         */
        for (Iterator<Long> iter = readLocks.iterator(); iter.hasNext();) {
            final Long lsn = iter.next();
            if (removeAllCursorLocks(lsn, cursor)) {
                lockManager.release(lsn, this);
                /*
                 * Caller Iterator.remove here rather than removeLock, to avoid
                 * a ConcurrentModificationException from readLocks.remove.
                 */
                iter.remove();
                updateMemoryUsage(0 - READ_LOCK_OVERHEAD);
            }
        }
    }

    @Override
    public void nonTxnOperationEnd(final CursorImpl cursor) {
        releaseNonTxnLocks(cursor);
    }

    @Override
    public void operationEnd(boolean operationOK)
        throws DatabaseException {

        if (!isAutoCommit) {
            /* Created transactions do nothing at the end of the operation. */
            return;
        }

        if (operationOK) {
            commit();
        } else {
            abort(false); // no sync required
        }
    }

    /**
     * Called at the end of a database open operation to add the database
     * handle to a user txn.  When a user txn aborts, handles opened using that
     * txn are invalidated.
     *
     * A non-txnal locker or auto-commit txn does not retain the handle,
     * because the open database operation will succeed or fail atomically and
     * no database invalidation is needed at a later time.
     *
     * @see HandleLocker
     */
    @Override
    public synchronized void addOpenedDatabase(Database dbHandle) {
        if (isAutoCommit) {
            return;
        }
        if (openedDatabaseHandles == null) {
            openedDatabaseHandles = new HashSet<>();
        }
        openedDatabaseHandles.add(dbHandle);
    }

    /**
     * Increase the counter if a new Cursor is opened under this transaction.
     */
    @Override
    @SuppressWarnings("unused")
    public void registerCursor(CursorImpl cursor) {
        cursors.getAndIncrement();
    }

    /**
     * Decrease the counter if a Cursor is closed under this transaction.
     */
    @Override
    @SuppressWarnings("unused")
    public void unRegisterCursor(CursorImpl cursor) {
        cursors.getAndDecrement();
    }

    /*
     * Txns always require locking.
     */
    @Override
    public boolean lockingRequired() {
        return true;
    }

    /**
     * Check if all cursors associated with the txn are closed. If not, those
     * open cursors will be forcibly closed.
     * @return true if open cursors exist
     */
    private boolean checkCursorsForClose() {
      return (cursors.get() != 0);
    }

    /**
     * stats
     */
    @Override
    public StatGroup collectStats() {
        StatGroup stats =
            new StatGroup("Transaction lock counts" ,
                          "Read and write locks held by transaction " + id);

        IntStat statReadLocks = new IntStat(stats, LOCK_READ_LOCKS);
        IntStat statWriteLocks = new IntStat(stats, LOCK_WRITE_LOCKS);
        IntStat statTotalLocks = new IntStat(stats, LOCK_TOTAL);

        synchronized (this) {
            int nReadLocks = (readLocks == null) ? 0 : readLocks.size();
            statReadLocks.add(nReadLocks);
            int nWriteLocks = (writeInfo == null) ? 0 : writeInfo.size();
            statWriteLocks.add(nWriteLocks);
            statTotalLocks.add(nReadLocks + nWriteLocks);
        }

        return stats;
    }

    /**
     * Set the state of a transaction to abort-only.  Should ONLY be called
     * by OperationFailureException.
     */
    @Override
    public void setOnlyAbortable(OperationFailureException cause) {
        assert cause != null;
        setState(Transaction.State.MUST_ABORT);
        onlyAbortableCause = cause;
    }

    /**
     * Set the state of a transaction's IMPORTUNATE bit.
     */
    @Override
    public void setImportunate(boolean importunate) {
        if (importunate) {
            txnFlags |= IMPORTUNATE;
        } else {
            txnFlags &= ~IMPORTUNATE;
        }
    }

    /**
     * Get the state of a transaction's IMPORTUNATE bit.
     */
    @Override
    public boolean getImportunate() {
        return (txnFlags & IMPORTUNATE) != 0;
    }

    /**
     * Throw an exception if the transaction is not open.
     *
     * If calledByAbort is true, it means we're being called from abort(). But
     * once closed, a Transaction never calls abort(). See comment at the top
     * of abortInternal.
     *
     * Caller must invoke with "this" synchronized.
     */
    @Override
    public void checkState(boolean calledByAbort)
        throws DatabaseException {

        switch (txnState) {

            case OPEN:
                return;

            case MUST_ABORT:

                /* Don't complain if the user is doing what we asked. */
                if (calledByAbort) {
                    return;
                }

                throw onlyAbortableCause.wrapSelf(
                    onlyAbortableCause.getMessage(),
                    FormatUtil.cloneBySerialization(onlyAbortableCause));

            default:
                /* All other states are equivalent to closed. */
                throw new IllegalStateException
                    ("Transaction " + id + " has been closed.");
        }
    }

    /**
     * Close and unregister this txn.
     */
    public void close(boolean isCommit) {

        if (isCommit) {
            /* Set final state to COMMITTED, if not set earlier. */
            if (txnState == Transaction.State.OPEN) {
                setState(Transaction.State.COMMITTED);
            }
        } else {
            /* This was set earlier by abort, but here also for safety. */
            setState(Transaction.State.ABORTED);
        }

        /*
         * [#16861] We use to avoid closing under the txn latch.
         * this special handling is no longer needed, now that firstLoggedLsn 
         * is volatile and getFirstActiveLsn is not synchronized.
         */
        envImpl.getTxnManager().unRegisterTxn(this, isCommit);

        /* Set the superclass Locker state to closed. */
        close();
    }

    private synchronized void setState(Transaction.State state) {
        txnState = state;
    }

    public Transaction.State getState() {
        return txnState;
    }

    @Override
    public boolean isValid() {
        return txnState == Transaction.State.OPEN;
    }

    public boolean isClosed() {
        return txnState != Transaction.State.OPEN &&
               txnState != Transaction.State.MUST_ABORT;
    }

    public boolean isOnlyAbortable() {
        return txnState == Transaction.State.MUST_ABORT;
    }

    /**
     * Determines whether this is the special null txn used to transmit dtvlsn
     * information and durability information.
     */
    public boolean isNullTxn() {
        return false;
    }

    /**
     * This method is overridden by HA txn subclasses and returns the node id
     * of the master node that committed or aborted the txn.
     */
    protected int getMasterId() {
        return getMasterIdTermSupplier().get().nodeId;
    }

    /**
     * Returns the master term associated with the Txn commit.
     */
    protected long getMasterTerm() {
        return getMasterIdTermSupplier().get().term;
    }

    /**
     * Returns a supplier that returns master ID and master term.
     */
    protected Supplier<MasterIdTerm> getMasterIdTermSupplier() {
        /* Non replicated txns don't use a node ID and a term. */
        return () -> MasterIdTerm.PRIMODAL_ID_TERM;
    }

    /**
     * This method is overridden by replication txn subclasses and returns the
     * DTVLSN associated with the Txn.
     */
    protected long getDTVLSN() {
       /*  Non replicated txns don't use VLSNs. */
        return UNINITIALIZED_VLSN;
    }

    /*
     * Log support
     */

    @Override
    public int getLastFormatChange() {
        return LAST_FORMAT_CHANGE;
    }

    @Override
    public Collection<VersionedWriteLoggable> getEmbeddedLoggables() {
        return Collections.emptyList();
    }

    @Override
    public int getLogSize() {
        return getLogSize(LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public void writeToLog(final ByteBuffer logBuffer) {
        writeToLog(
            logBuffer, LogEntryType.LOG_VERSION, false /*forReplication*/);
    }

    @Override
    public int getLogSize(final int logVersion, final boolean forReplication) {
        return LogUtils.getPackedLongLogSize(id) +
               LogUtils.getPackedLongLogSize(
                   forReplication ? DbLsn.NULL_LSN : lastLoggedLsn);
    }

    /**
     * It's ok for FindBugs to whine about id not being synchronized.
     */
    @Override
    public void writeToLog(final ByteBuffer logBuffer,
                           final int logVersion,
                           final boolean forReplication) {
        LogUtils.writePackedLong(logBuffer, id);
        LogUtils.writePackedLong(logBuffer,
            forReplication ? DbLsn.NULL_LSN : lastLoggedLsn);
    }

    /**
     * It's ok for FindBugs to whine about id not being synchronized.
     */
    @Override
    public void readFromLog(EnvironmentImpl envImpl,
                            ByteBuffer logBuffer,
                            int entryVersion) {
        id = LogUtils.readPackedLong(logBuffer);
        lastLoggedLsn = LogUtils.readPackedLong(logBuffer);
    }

    @Override
    public boolean hasReplicationFormat() {
        return false;
    }

    @Override
    public boolean isReplicationFormatWorthwhile(final ByteBuffer logBuffer,
                                                 final int srcVersion,
                                                 final int destVersion) {
        return false;
    }

    @Override
    @SuppressWarnings("unused")
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<txn id=\"");
        sb.append(getId());
        sb.append("\">");
        sb.append(DbLsn.toString(lastLoggedLsn));
        sb.append("</txn>");
    }

    @Override
    public long getTransactionId() {
        return getId();
    }

    @Override
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof Txn)) {
            return false;
        }

        return id == ((Txn) other).id;
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled. The string
     * construction can be numerous enough to show up on a performance profile.
     */
    private void traceCommit(int numWriteLocks, int numReadLocks) {
        Logger logger = envImpl.getLogger();
        if (logger.isLoggable(Level.FINE)) {
            String sb = " Commit: id = " + id +
                " numWriteLocks=" + numWriteLocks +
                " numReadLocks = " + numReadLocks;
            LoggerUtils.fine(logger, envImpl, sb);
        }
    }

    /* Transaction hooks used for replication support. */

    /**
     * A replicated environment introduces some new considerations when
     * entering a transaction scope via an Environment.transactionBegin()
     * operation.
     *
     * On a Replica, the transactionBegin() operation must wait until the
     * Replica has synched up to where it satisfies the ConsistencyPolicy that
     * is in effect.
     *
     * On a Master, the transactionBegin() must wait until the Feeder has
     * sufficient connections to ensure that it can satisfy the
     * ReplicaAckPolicy, since if it does not, it will fail at commit() and the
     * work done in the transaction will need to be undone.
     *
     * This hook provides the mechanism for implementing the above support for
     * replicated transactions. It ignores all non-replicated transactions.
     *
     * The hook throws ReplicaStateException, if a Master switches to a Replica
     * state while waiting for its Replicas connections. Changes from a Replica
     * to a Master are handled transparently to the application. Exceptions
     * manifest themselves as DatabaseException at the interface to minimize
     * use of Replication based exceptions in core JE.
     *
     * @param config the transaction config that applies to the txn
     *
     * @throws DatabaseException if there is a failure
     */
    protected void txnBeginHook(TransactionConfig config)
        throws DatabaseException {

        /* Overridden by Txn subclasses when appropriate */
    }

    /**
     * This hook is invoked before the commit of a transaction that made
     * changes to a replicated environment. It's invoked for transactions
     * executed on the master or replica(which are masterTxn and
     * ReplayTxn).
     *
     * The hook is invoked at a very specific point in the normal commit
     * sequence: immediately before the commit log entry is written to the log.
     * It represents the last chance to abort the transaction and provides an
     * opportunity to make some final checks before allowing the commit to go
     * ahead. Note that it should be possible to abort the transaction at the
     * time the hook is invoked.
     *
     * After invocation of the "pre" hook one of the "post" hooks:
     * postLogCommitHook or postLogCommitAbortHook is typically invoked, on
     * whether the log of the commit succeeds or fails. In the latter case, the
     * log abort hooks may additionally be invoked if an abort log entry is
     * written.
     *
     * Exceptions thrown by this hook result in the postLogCommitAbortHook
     * being run, the transaction being aborted, and the exception being
     * propagated back to the application.
     *
     * @throws DatabaseException if there was a problem and that the
     * transaction should be aborted.
     */
    protected void preLogCommitHook()
        throws DatabaseException {

        GroupCommit groupCommit = envImpl.getTxnManager().getGroupCommit();
        if (groupCommit != null &&
            groupCommit.isEnabled() &&
            this.repContext.inReplicationStream() &&
            this.commitDurability.getLocalSync() == SyncPolicy.SYNC) {
                this.isGroupCommitted = true;
                this.commitDurability =
                    new Durability(SyncPolicy.NO_SYNC,
                                   this.commitDurability.getReplicaSync(),
                                   this.commitDurability.getReplicaAck());
        }

        /* Overridden by Txn subclasses when appropriate */
    }

    /**
     * This hook is invoked after the commit record has been written to the
     * log, but before write locks have been released, so that other
     * application cannot see the changes made by the transaction. At this
     * point the transaction has been committed by the Master.
     *
     * Exceptions thrown by this hook result in the transaction being completed
     * on the Master, that is, locks are released, etc. and the exception is
     * propagated back to the application.
     *
     * @param commitItem the commit item that was just logged
     *
     * @return whether the commit LogItem was queued by the arbiter.
     * See ArbiterFeederSource.addCommit.
     *
     * @throws DatabaseException to indicate that there was a replication
     * related problem that needs to be communicated back to the application.
     */
    protected boolean postLogCommitHook(LogItem commitItem)
        throws DatabaseException {

        if (isGroupCommitted) {
            envImpl.getTxnManager().getGroupCommit().
                bufferCommit(System.nanoTime(),
                             this,
                             commitItem.header.getVLSN());
        }

        /* Overridden by Txn subclasses when appropriate */
        return false;
    }

    protected void preLogAbortHook()
        throws DatabaseException {

        /* Override by Txn subclasses when appropriate */
    }

    /**
     * Invoked if the transaction associated with the preLogCommitHook was
     * subsequently aborted, for example due to a lack of disk space. This
     * method is responsible for any cleanup that may need to be done as a
     * result of the abort.
     *
     * A note of caution regarding this hook, in some error situations, it may
     * be invoked even when the pre hook itself did not complete normally, eg.
     * it threw an IRE. The implementation must be robust in such
     * circumstances.
     */
    protected void postLogCommitAbortHook() {
        /* Overridden by Txn subclasses when appropriate */
    }

    protected void postLogAbortHook() {
        /* Overridden by Txn subclasses when appropriate */
    }

    /**
     * Returns the CommitToken associated with a successful replicated commit.
     *
     * @see com.sleepycat.je.Transaction#getCommitToken
     */
    public CommitToken getCommitToken() {
        return null;
    }

    /**
     * Identifies exceptions that may be propagated back to the caller during
     * the postCommit phase of a transaction commit.
     *
     * @param postCommitException the exception being evaluated
     *
     * @return true if the exception must be propagated back to the caller,
     * false if the exception indicates there is a serious problem with the
     * commit operation and the environment should be invalidated.
     */
    protected boolean
        propagatePostCommitException(DatabaseException postCommitException) {
        return false;
    }

    /**
     * Use the marker Sets to record whether this is the first time we've see
     * this logical node.
     */
    private boolean firstInstance(Set<Long> seenLsns,
                                  Set<CompareSlot> seenSlots,
                                  UndoReader undo) {
        final LNLogEntry<?> undoEntry = undo.logEntry;
        final long abortLsn1 = undoEntry.getAbortLsn();
        if (abortLsn1 != DbLsn.NULL_LSN) {
            return seenLsns.add(abortLsn1);
        }
        final CompareSlot slot = new CompareSlot(undo.db, undoEntry);
        return seenSlots.add(slot);
    }

    /**
     * Accumulates the set of databases for which transaction commit/abort
     * triggers must be run.
     *
     * @param dbImpl the database that associated with the trigger
     */
    public void noteTriggerDb(DatabaseImpl dbImpl) {
        if (triggerDbs == null) {
            triggerDbs = Collections.synchronizedSet(new HashSet<>());
        }
        triggerDbs.add(dbImpl);
    }

    /**
     * Returns the set of databases for which transaction commit/abort
     * triggers must be run. Returns Null if no triggers need to be run.
     */
    public Set<DatabaseImpl> getTriggerDbs() {
        return triggerDbs;
    }

    /** Get the set of lock ids owned by this transaction */
    public Set<Long> getWriteLockIds() {
        if (writeInfo == null) {
            return Collections.emptySet();
        }

        return writeInfo.keySet();
    }

    /* For unit tests. */
    public Set<Long> getReadLockIds() {
        if (readLocks == null) {
            return new HashSet<>();
        }
        return new HashSet<>(readLocks);
    }

    public EnvironmentImpl getEnvironmentImpl() {
        return envImpl;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public Transaction getTransaction() {
        return (transaction != null) ?
                transaction :
                (transaction = new AutoTransaction(this));
    }

    public boolean isGroupCommitted() {
        return isGroupCommitted;
    }

    private static class AutoTransaction extends Transaction {

        AutoTransaction(Txn txn) {
            /* AutoTransactions do not have a convenient environment handle. */
            super(txn.getEnvironmentImpl().getInternalEnvHandle(), txn);
        }

        @Override
        public synchronized void commit()
            throws DatabaseException {

            @SuppressWarnings({"unused", "ThrowableNotThrown"})
            Throwable unused = EnvironmentFailureException.unexpectedState(
               "commit() not permitted on an auto transaction");
        }

        @Override
        public synchronized void commit
            (@SuppressWarnings("unused") Durability durability) {

            @SuppressWarnings({"unused", "ThrowableNotThrown"})
            Throwable unused = EnvironmentFailureException.unexpectedState(
                "commit() not permitted on an auto transaction");
        }

        @Override
        public synchronized void commitNoSync()
            throws DatabaseException {

            @SuppressWarnings({"unused", "ThrowableNotThrown"})
            Throwable unused = EnvironmentFailureException.unexpectedState(
                "commit() not permitted on an auto transaction");
        }

        @Override
        public synchronized void commitWriteNoSync()
            throws DatabaseException {

            @SuppressWarnings({"unused", "ThrowableNotThrown"})
            Throwable unused = EnvironmentFailureException.unexpectedState(
                "commit() not permitted on an auto transaction");
        }

        @Override
        public synchronized void abort()
            throws DatabaseException {

            @SuppressWarnings({"unused", "ThrowableNotThrown"})
            Throwable unused = EnvironmentFailureException.unexpectedState(
                "abort() not permitted on an auto transaction");
        }
    }

    public Map<DatabaseId, DatabaseImpl> getUndoDatabases() {
        return undoDatabases;
    }

    /**
     * Txn freezing is used to prevent changes to transaction lock contents.  A
     * frozen transaction should ignore any transaction commit/abort
     * requests. This is used only by MasterTxns, as a way of holding a
     * transaction stable while cloning it to serve as a ReplayTxn during
     * {@literal master->replica} transitions.
     * @param isCommit true if called by commit.
     */
    protected void checkIfFrozen(boolean isCommit)
        throws DatabaseException {
    }

    /** @see #checkIfFrozen */
    protected boolean isFrozen() {
        return false;
    }

    /*
     * Used when creating a subset of MasterTxns. Using an explicit method
     * like this rather than checking class types insulates us from any
     * assumptions about the class hierarchy.
     */
    public boolean isMasterTxn() {
        return false;
    }

    /** Is overridden by HA classes. */
    public boolean getArbiterAck() {
        return false;
    }

    /**
     * The time on the master when the current transaction was committed,
     * returns the current time if this was not a replicated transaction.
     */
    protected Timestamp getMasterCommitTime() {
        return new Timestamp(TimeSupplier.currentTimeMillis());
    }

    /**
     * Called when an LSN changes due to a write by cleaner migration, and the
     * new LSN is locked for this Txn. (For such writes a write lock is not
     * acquired, so it is possible that read locks are held by other lockers.)
     * For each cursor associated with the old LSN, we must additionally
     * associate it with the new LSN.
     */
    @Override
    public synchronized void copyCursorLocks(final long oldLsn,
                                             final long newLsn) {
        assert oldLsn != NULL_LSN;
        assert newLsn != NULL_LSN;

        if (!readCommittedIsolation) {
            return;
        }

        /* Save the count because it will increase as we add locks. */
        final int origLocks = cursorLocks;

        for (int i = 0; i < origLocks; ++i) {
            if (cursorLockLsns[i] == oldLsn) {
                addCursorLock(newLsn, cursorLockers[i]);
            }
        }
    }

    /**
     * Called when the cursor used to lock a record is to be closed, and any
     * locks with which it is associated should be associated instead with
     * another cursor. For example, when reading by secondary, a cursor is
     * used to read the primary record and that cursor will be closed, but the
     * lock on the primary should be associated with the secondary cursor.
     */
    @Override
    public synchronized void transferCursorLocks(final CursorImpl oldCursor,
                                                 final CursorImpl newCursor) {
        assert oldCursor != null;
        assert newCursor != null;

        if (!readCommittedIsolation) {
            return;
        }

        for (int i = 0; i < cursorLocks; ++i) {
            if (cursorLockers[i] == oldCursor) {
                cursorLockers[i] = newCursor;
            }
        }
    }

    /**
     * Adds the given lsn/cursor association, if it is not already present.
     */
    private synchronized void addCursorLock(final long lsn,
                                            final CursorImpl cursor) {
        assert readCommittedIsolation;
        assert lsn != NULL_LSN;
        assert cursor != null;

        for (int i = 0; i < cursorLocks; ++i) {
            if (cursorLockLsns[i] == lsn && cursorLockers[i] == cursor) {
                return;
            }
        }

        if (cursorLockers == null) {
            cursorLockers = new CursorImpl[INITIAL_CURSOR_LOCKS];
            cursorLockLsns = new long[INITIAL_CURSOR_LOCKS];
        } else if (cursorLocks == cursorLockers.length) {
            cursorLockers = Arrays.copyOf(cursorLockers, cursorLocks * 2);
            cursorLockLsns = Arrays.copyOf(cursorLockLsns, cursorLocks * 2);
        }

        cursorLockers[cursorLocks] = cursor;
        cursorLockLsns[cursorLocks] = lsn;
        ++cursorLocks;
    }

    /**
     * Removes all associations for the given cursor. Returns whether no
     * remaining associations exist (with another cursor) for the given LSN.
     */
    private synchronized boolean removeAllCursorLocks(
        final long lsn,
        final CursorImpl cursor) {

        assert readCommittedIsolation;
        assert lsn != NULL_LSN;
        assert cursor != null;

        boolean noneRemaining = true;
        for (int i = 0; i < cursorLocks; ++i) {
            if (cursorLockers[i] == cursor) {
                removeCursorLockAt(i);
                --i;
            } else if (cursorLockLsns[i] == lsn) {
                noneRemaining = false;
            }
        }
        return noneRemaining;
    }

    /**
     * If the given cursor is non-null, removes the given lsn/cursor
     * association. Returns whether no remaining associations exist (with
     * another cursor) for the given LSN.
     */
    private synchronized boolean removeCursorLock(final long lsn,
                                                  final CursorImpl cursor) {
        assert readCommittedIsolation;
        assert lsn != NULL_LSN;

        boolean noneRemaining = true;
        for (int i = 0; i < cursorLocks; ++i) {
            if (cursorLockLsns[i] == lsn) {
                if (cursor != null && cursorLockers[i] == cursor) {
                    removeCursorLockAt(i);
                    --i;
                } else {
                    noneRemaining = false;
                }
            }
        }
        return noneRemaining;
    }

    /**
     * Removes the association at the given index.
     */
    private void removeCursorLockAt(final int i) {
        final int copyLen = (cursorLocks - 1) - i;
        if (copyLen > 0) {
            System.arraycopy(
                cursorLockers, i + 1, cursorLockers, i, copyLen);
            System.arraycopy(
                cursorLockLsns, i + 1, cursorLockLsns, i, copyLen);
        }
        --cursorLocks;
        cursorLockers[cursorLocks] = null;
        cursorLockLsns[cursorLocks] = 0;
    }

    /**
     * Intended for testing.
     */
    public synchronized Set<Long> getCursorLockLsns(final CursorImpl cursor) {
        final Set<Long> set = new HashSet<>();
        for (int i = 0; i < cursorLocks; ++i) {
            if (cursorLockers[i] == cursor) {
                set.add(cursorLockLsns[i]);
            }
        }
        return set;
    }

    public void setPreCommitExceptionHook(TestHook<RuntimeException> hook) {
        preCommitExceptionHook = hook;
    }

    public void setMasterTransferHook(TestHook<MasterTxn> hook) {
        masterTransferHook = hook;
    }
}
