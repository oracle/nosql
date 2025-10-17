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

package com.sleepycat.je;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.PropUtil;

/**
 * Specifies the attributes of a database environment transaction.
 */
public class TransactionConfig implements Cloneable {

    /**
     * Default configuration used if null is passed to methods that create a
     * transaction.
     */
    public static final TransactionConfig DEFAULT = new TransactionConfig();

    private Durability durability = null;
    private ReplicaConsistencyPolicy consistencyPolicy;
    private boolean noWait = false;
    private boolean readUncommitted = false;
    private boolean readCommitted = false;
    private boolean optimisticRead = false;
    private boolean readOnly = false;
    private boolean localWrite = false;
    private int txnTimeout = -1;
    private AsyncAckHandler asyncAckHandler = null;

    /**
     * An instance created using the default constructor is initialized with
     * the system's default settings.
     */
    public TransactionConfig() {
    }

    /**
     * @hidden
     * For internal use only.
     * If Durability is not set intentionally for txn, create a default
     * Durability config based on whether envImpl is replicated or not.
     *
     * @return the durability
     */
    public Durability getDefaultDurability(final EnvironmentImpl envImpl) {
        /*
         * Replicated environments default to commitNoSync, while standalone
         * default to commitSync.
         */
        return envImpl.isReplicated() ?
            Durability.COMMIT_NO_SYNC : Durability.COMMIT_SYNC;
    }

    /**
     * Configures the durability associated with a transaction when it commits.
     * Changes to durability are not reflected back to the "sync" booleans --
     * there isn't a one to one mapping.
     *
     * Note that you should not use both the durability and the XXXSync() apis
     * on the same config object.
     *
     * @param durability the durability definition
     *
     * @return this
     */
    public TransactionConfig setDurability(final Durability durability) {
        this.durability = durability;
        return this;
    }

    /**
     * Returns the durability associated with the configuration.
     *
     * If {@link #setDurability} has not been called, this method returns null.
     * When no durability settings have been specified using the
     * {@code TransactionConfig}, the default durability is applied to the
     * {@link Transaction} by {@link Environment#beginTransaction} using
     * {@link EnvironmentConfig} settings.
     *
     * @return the durability setting currently associated with this config.
     */
    public Durability getDurability() {
        return durability;
    }

    /**
     * Used internally to configure Durability, modifying the existing
     * Durability or explicit sync configuration.  This method is used to avoid
     * a mixed mode exception, since the existing config may be in either mode.
     */
    void overrideDurability(@SuppressWarnings("hiding")
                            final Durability durability) {
        this.durability = durability;
    }

    /**
     * Associates a consistency policy with this configuration.
     *
     * @param consistencyPolicy the consistency definition
     *
     * @return this
     */
    public TransactionConfig setConsistencyPolicy(
        final ReplicaConsistencyPolicy consistencyPolicy) {

        this.consistencyPolicy = consistencyPolicy;
        return this;
    }

    /**
     * Returns the consistency policy associated with the configuration.
     *
     * @return the consistency policy currently associated with this config.
     */
    public ReplicaConsistencyPolicy getConsistencyPolicy() {
        return consistencyPolicy;
    }

    /**
     * Configures the transaction to not wait if a lock request cannot be
     * immediately granted.
     *
     * <p>The default is false for this class and the database environment.</p>
     *
     * @param noWait If true, transactions will not wait if a lock request
     * cannot be immediately granted, instead {@link
     * com.sleepycat.je.LockNotAvailableException LockNotAvailableException}
     * will be thrown.
     *
     * @return this
     */
    public TransactionConfig setNoWait(final boolean noWait) {
        this.noWait = noWait;
        return this;
    }

    /**
     * Returns true if the transaction is configured to not wait if a lock
     * request cannot be immediately granted.
     *
     * @return true if the transaction is configured to not wait if a lock
     * request cannot be immediately granted.
     */
    public boolean getNoWait() {
        return noWait;
    }

    /**
     * Configures read operations performed by the transaction to return
     * modified but not yet committed data.
     *
     * @param readUncommitted If true, configure read operations performed by
     * the transaction to return modified but not yet committed data.
     *
     * @see LockMode#READ_UNCOMMITTED
     *
     * @return this
     */
    public TransactionConfig setReadUncommitted(
        final boolean readUncommitted) {

        this.readUncommitted = readUncommitted;
        return this;
    }

    /**
     * Returns true if read operations performed by the transaction are
     * configured to return modified but not yet committed data.
     *
     * @return true if read operations performed by the transaction are
     * configured to return modified but not yet committed data.
     *
     * @see LockMode#READ_UNCOMMITTED
     */
    public boolean getReadUncommitted() {
        return readUncommitted;
    }

    /**
     * Configures the transaction for read committed isolation.
     *
     * <p>Read committed isolation provides for cursor stability but not
     * repeatable reads.  Data items which have been previously read by this
     * transaction may be deleted or modified by other transactions before the
     * cursor is closed or the transaction completes.</p>
     *
     * <p>See the {@link LockMode locking rules} for information on how
     * read-committed impacts transactional and non-transactional locking.</p>
     *
     * <p>It is a common mistake to assume that read-committed must be used
     * to avoid reading uncommitted data. Rather, repeatable-read isolation
     * (which is a _stronger_ form of isolation than read-committed) is the
     * default, and uncommitted data is read if only read-uncommitted mode is
     * explicitly specified. These terms are confusing, but they are the
     * ANSI standard terms.</p>
     *
     * @param readCommitted If true, configure the transaction for read
     * committed isolation.
     *
     * @return this
     *
     * @see LockMode
     *
     * @see <a href="EnvironmentStats.html#cacheUnexpectedSizes">Cache
     * Statistics: Unexpected Sizes</a>
     */
    public TransactionConfig setReadCommitted(final boolean readCommitted) {
        this.readCommitted = readCommitted;
        return this;
    }

    /**
     * Configures the transaction for read committed isolation.
     *
     * <p>The major difference between Optimistic Read and
     * Read Committed/Repeatable Read lies in how they handle read locks
     * in the presence of a write lock.
     * In Read Committed and Repeatable Read, a read operation will block and
     * wait for the write lock to be released if it is held by an open
     * transaction on the LN. In contrast, Optimistic Read does not wait for the
     * lock; instead, it attempts to read the most recently committed version of
     * the data(i.e., the latest version that has been committed before the
     * uncommitted transaction). If it fails to do so, it falls back to
     * Read Committed semantics.</p>
     *
     * <p>Additionally, Optimistic Read does not guarantee cursor stability.
     * The read lock is released immediately after reading the LN-before
     * the BIN latch is released-ensuring that the read does not block
     * subsequent write operations.</p>
     *
     * @param optimisticRead Configures the transaction for optimistic read
     * isolation.
     * @return this
     */
    public TransactionConfig setOptimisticRead(final boolean optimisticRead) {
        this.optimisticRead = optimisticRead;
        return this;
    }

    /**
     * Returns true if the transaction is configured for read-committed
     * isolation.
     *
     * @return true if the transaction is configured for read-committed
     * isolation.
     *
     * @see #setReadCommitted
     * @see LockMode
     */
    public boolean getReadCommitted() {
        return readCommitted;
    }

    /**
     * Returns true if the transaction is configured for optimisticRead
     * isolation.
     *
     * @return true if the transaction is configured for read-committed
     * isolation.
     *
     * @see #setOptimisticRead
     */
    public boolean getOptimisticRead() {
        return optimisticRead;
    }

    /**
     * Configures this transaction to disallow write operations, regardless of
     * whether writes are allowed for the {@link Environment} or the
     * {@link Database}s that are accessed.
     *
     * <p>If a write operation is attempted using a read-only transaction,
     * an {@code UnsupportedOperationException} will be thrown.</p>
     *
     * <p>For a read-only transaction, the transaction's {@code Durability} is
     * ignored, even when it is explicitly specified using {@link
     * #setDurability(Durability)}.</p>
     *
     * <p>In a {@link com.sleepycat.je.rep.ReplicatedEnvironment}, a read-only
     * transaction implicitly uses
     * {@link com.sleepycat.je.Durability.ReplicaAckPolicy#NONE}.
     * A read-only transaction on a Master will thus not be held up, or
     * throw {@link com.sleepycat.je.rep.InsufficientReplicasException}, if the
     * Master is not in contact with a sufficient number of Replicas at the
     * time the transaction is initiated.</p>
     *
     * <p>The default setting is false (writes are allowed).</p>
     *
     * @return this
     */
    public TransactionConfig setReadOnly(final boolean readOnly) {
        if (localWrite && readOnly) {
            throw new IllegalArgumentException(
                "localWrite and readOnly may not both be true");
        }
        this.readOnly = readOnly;
        return this;
    }

    /**
     * Returns whether read-only is configured for this transaction.
     */
    public boolean getReadOnly() {
        return readOnly;
    }

    /**
     * Configures this transaction to allow writing to non-replicated
     * {@link Database}s in a
     * {@link com.sleepycat.je.rep.ReplicatedEnvironment}.
     *
     * <p>In a replicated environment, a given transaction may be used to
     * write to either replicated databases or non-replicated databases, but
     * not both. If a write operation to a replicated database is attempted
     * when local-write is true, or to a non-replicated database when
     * local-write is false, an {@code UnsupportedOperationException} will be
     * thrown.</p>
     *
     * <p>Note that for auto-commit transactions (when the {@code Transaction}
     * parameter is null), the local-write setting is automatically set to
     * correspond to whether the database is replicated. With auto-commit,
     * local-write is always true for a non-replicated database, and
     * always false for a replicated database.</p>
     *
     * <p>In a replicated environment, a local-write transaction implicitly
     * uses {@link com.sleepycat.je.Durability.ReplicaAckPolicy#NONE}.
     * A local-write transaction on a Master will thus not be held up, or
     * throw {@link com.sleepycat.je.rep.InsufficientReplicasException}, if the
     * Master is not in contact with a sufficient number of Replicas at the
     * time the transaction is initiated.</p>
     *
     * <p>By default the local-write setting is false, meaning that the
     * transaction may only write to replicated Databases in a replicated
     * environment.</p>
     *
     * <p>This configuration setting is ignored in a non-replicated Environment
     * since no databases are replicated.</p>
     *
     * @return this
     *
     * @see <a href="rep/ReplicatedEnvironment.html#nonRepDbs">Non-replicated
     * Databases in a Replicated Environment</a>
     */
    public TransactionConfig setLocalWrite(final boolean localWrite) {
        if (localWrite && readOnly) {
            throw new IllegalArgumentException(
                "localWrite and readOnly may not both be true");
        }
        this.localWrite = localWrite;
        return this;
    }

    /**
     * Returns whether local-write is configured for this transaction.
     */
    public boolean getLocalWrite() {
        return localWrite;
    }

    /**
     * Configures the timeout value for the transaction lifetime.  Specifying 0
     * disables timeouts for the transaction.  By default, the timeout is
     * specified by the {@link EnvironmentConfig#TXN_TIMEOUT} property.
     *
     * @param timeOut the timeout value, or 0 to disable timeouts
     *
     * @param unit the {@code TimeUnit} of the timeOut value.  May be null only
     * if timeOut is 0.
     *
     * @throws IllegalArgumentException if {@code timeOut} or {@code unit} have
     * an invalid value
     *
     * @return this
     *
     * @since 18.1
     */
    public TransactionConfig setTxnTimeout(long timeOut, TimeUnit unit) {
        txnTimeout = PropUtil.durationToMillis(timeOut, unit);
        return this;
    }

    /**
     * Returns the transaction timeout.  A return value of 0 means that the
     * transaction will not have a timeout.  A return value of -1 means that
     * the transaction will use the timeout value specified by the environment.
     * All other values specify the actual transaction timeout.
     *
     * @param unit the {@code TimeUnit} of the returned value.  Must not be
     * null.
     *
     * @return the transaction timeout
     */
    public long getTxnTimeout(TimeUnit unit) {
        if ((unit != null) && (txnTimeout == -1)) {
            return -1;
        }
        return PropUtil.millisToDuration(txnTimeout, unit);
    }

    /**
     * Returns a copy of this configuration object.
     */
    @Override
    public TransactionConfig clone() {
        try {
            return (TransactionConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Returns the values for each configuration attribute.
     *
     * @return the values for each configuration attribute.
     */
    @Override
    public String toString() {
        return "durability=" + durability +
            "\nconsistencyPolicy=" + consistencyPolicy +
            "\nnoWait=" + noWait +
            "\nreadUncommitted=" + readUncommitted +
            "\nreadCommitted=" + readCommitted +
            "\noptimisticRead=" + optimisticRead +
            "\n";
    }

    /**
     * Returns the configured async ack handler.
     * <p>
     * The default is null, which means that acknowledgments are processed
     * synchronously.
     *
     * @since 23.1
     */
    public AsyncAckHandler getAsyncAckHandler() {
        return asyncAckHandler;
    }

    /**
     * Set the async ack handler.
     * <p>
     * When this handler is set to a non-null value, {@link Transaction#commit}
     * returns immediately after the changes have been committed locally.
     * {@link AsyncAckHandler#onQuorumAcks} is invoked after a quorum of
     * acknowledgments (as determined by {@link #getDurability}) has been
     * received within the transaction timeout period. Note that the handler
     * is not invoked if the transaction does not make any modifications, or
     * has been configured to be a read-only transaction with
     * {@link #setReadOnly(boolean)}
     *
     * @param asyncAckHandler the ack handler to be invoked, or null if the
     * acknowledgments for this transaction are to be processed synchronously.
     *
     * @see AsyncAckHandler
     *
     * @since 23.1
     */
    public TransactionConfig
        setAsyncAckHandler(AsyncAckHandler asyncAckHandler) {
        this.asyncAckHandler = asyncAckHandler;
        return this;
    }
}
