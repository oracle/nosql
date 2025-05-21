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
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.beforeimage.BeforeImageContext;
import com.sleepycat.je.beforeimage.BeforeImageLNLogEntry;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.CursorImpl.LockStanding;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DupKeyData;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.InternalComparator;
import com.sleepycat.je.dbi.PutMode;
import com.sleepycat.je.dbi.RangeConstraint;
import com.sleepycat.je.dbi.ReplayPreprocessor;
import com.sleepycat.je.dbi.SearchMode;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.dbi.TriggerManager;
import com.sleepycat.je.dbi.WriteParams;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.CountEstimator;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.utilint.DatabaseUtil;
import com.sleepycat.je.utilint.InternalException;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.NotSerializable;

/**
 * A database cursor. Cursors are used for operating on collections of records,
 * for iterating over a database, and for saving handles to individual records,
 * so that they can be modified after they have been read.
 *
 * <p>Cursors which are opened with a transaction instance are transactional
 * cursors and may be used by multiple threads, but only serially.  That is,
 * the application must serialize access to the handle. Non-transactional
 * cursors, opened with a null transaction instance, may not be used by
 * multiple threads.</p>
 *
 * <p>If the cursor is to be used to perform operations on behalf of a
 * transaction, the cursor must be opened and closed within the context of that
 * single transaction.</p>
 *
 * <p>Once the cursor {@link #close} method has been called, the handle may not
 * be accessed again, regardless of the {@code close} method's success or
 * failure, with one exception:  the {@code close} method itself may be called
 * any number of times to simplify error handling.</p>
 *
 * <p>To obtain a cursor with default attributes:</p>
 *
 * <blockquote><pre>
 *     Cursor cursor = myDatabase.openCursor(txn, null);
 * </pre></blockquote>
 *
 * <p>To customize the attributes of a cursor, use a CursorConfig object.</p>
 *
 * <blockquote><pre>
 *     CursorConfig config = new CursorConfig();
 *     config.setReadUncommitted(true);
 *     Cursor cursor = myDatabase.openCursor(txn, config);
 * </pre></blockquote>
 *
 * <p>Modifications to the database during a sequential scan will be reflected
 * in the scan; that is, records inserted behind a cursor will not be returned
 * while records inserted in front of a cursor will be returned.</p>
 *
 * <p>When a cursor is initially opened its state is {@code uninitialized},
 * meaning that it is not positioned on any record. The cursor's state changes
 * to {@code initialized} when a cursor movement operation succeeds. After
 * being initialized, when a cursor movement operation does not succeed (e.g.,
 * a {@code next} operation is attempted and there are no more records), the
 * cursor's state again becomes {@code uninitialized}.</p>
 *
 * <h2><a id="partialEntry">Using Null and Partial DatabaseEntry
 * Parameters</a></h2>
 *
 * <p>Null can be passed for DatabaseEntry output parameters if the value is
 * not needed. The {@link DatabaseEntry#setPartial DatabaseEntry Partial}
 * property can also be used to optimize in certain cases. These provide
 * varying degrees of performance benefits that depend on the specific
 * operation, as described below.</p>
 *
 * <p>When retrieving a record with a {@link Database} or {@link Cursor}
 * method, if only the key is needed by the application then the retrieval of
 * the data item can be suppressed by passing null. If null is passed as
 * the data parameter, the data item will not be returned by the {@code
 * Database} or {@code Cursor} method.</p>
 *
 * <p>Suppressing the return of the data item potentially has a large
 * performance benefit. In this case, if the record data is not already in the
 * JE cache, it will not be read from disk. The performance benefit is
 * potentially large because random access disk reads may be reduced. Examples
 * use cases are:</p>
 * <ul>
 * <li>Scanning all records in key order, when the data is not needed.</li>
 * <li>Skipping over records quickly with {@code READ_UNCOMMITTED} isolation to
 * select records for further processing by examining the key value.</li>
 * </ul>
 *
 * <p>Note that by "record data" we mean both the {@code data} parameter for a
 * regular or primary DB, and the {@code pKey} parameter for a secondary DB.
 * However, the performance advantage of a key-only operation does not apply to
 * databases configured for duplicates. For a duplicates DB, the data is always
 * available along with the key and does not have to be fetched separately.</p>
 *
 * <p>The Partial property may also be used to retrieve or update only a
 * portion of a data item.  This avoids copying the entire record between the
 * JE cache and the application data parameter. However, this feature has less
 * of a performance benefit than one might assume, since the entire record is
 * always read or written to the database, and the entire record is cached. A
 * partial update may be performed only with
 * {@link Cursor#putCurrent Cursor.putCurrent}.</p>
 *
 * <p>A null or partial DatabaseEntry output parameter may also be used in
 * other cases, for example, to retrieve a partial key item. However, in
 * practice this has limited value since the entire key is usually needed by
 * the application, and the benefit of copying a portion of the key is
 * generally very small.</p>
 *
 * <p>Historical note: Prior to JE 7.0, null could not be passed for output
 * parameters. Instead, {@code DatabaseEntry.setPartial(0, 0, true)} was called
 * for a data parameter to avoid reading the record's data. Now, null can be
 * passed instead.</p>
 */
public class Cursor implements ForwardCursor {

    static final ReadOptions DEFAULT_READ_OPTIONS = new ReadOptions();
    static final WriteOptions DEFAULT_WRITE_OPTIONS = new WriteOptions();

    private static final DatabaseEntry EMPTY_DUP_DATA =
        new DatabaseEntry(new byte[0]);

    static final DatabaseEntry NO_RETURN_DATA = new DatabaseEntry();

    static {
        NO_RETURN_DATA.setPartial(0, 0, true);
    }

    /**
     * The CursorConfig used to configure this cursor.
     */
    CursorConfig config;

    /* User Transacational, or null if none. */
    private Transaction transaction;

    /**
     * Handle under which this cursor was created; may be null when the cursor
     * is used internally.
     */
    private Database dbHandle;

    /**
     * Database implementation.
     */
    private DatabaseImpl dbImpl;

    /**
     * The underlying cursor.
     */
    CursorImpl cursorImpl; // Used by subclasses.

    private boolean updateOperationsProhibited;

    /* Attributes */
    private boolean readUncommittedDefault;

    private CacheMode defaultCacheMode;

    private boolean includeInOpStats = true;

    /*
     * For range searches, it establishes the upper bound (K2) of the search
     * range via a function that returns false if a key is >= K2.
     */
    private RangeConstraint rangeConstraint;

    private Logger logger;

    /**
     * Creates a cursor for a given user transaction with
     * retainNonTxnLocks=false.
     *
     * <p>If txn is null, a non-transactional cursor will be created that
     * releases locks for the prior operation when the next operation
     * succeeds.</p>
     */
    Cursor(final Database dbHandle,
           final Transaction txn,
           CursorConfig cursorConfig) {

        if (cursorConfig == null) {
            cursorConfig = CursorConfig.DEFAULT;
        }

        /* Check that Database is open for internal Cursor usage. */
        final DatabaseImpl dbImpl = dbHandle.checkOpen();

        /* Do not allow auto-commit when creating a user cursor. */
        Locker locker = LockerFactory.getReadableLocker(dbHandle, txn);

        init(dbHandle, dbImpl, locker, cursorConfig,
             false /*retainNonTxnLocks*/);
    }

    /**
     * Creates a cursor for a given locker with retainNonTxnLocks=false.
     *
     * <p>If locker is null or is non-transactional, a non-transactional cursor
     * will be created that releases locks for the prior operation when the
     * next operation succeeds.</p>
     */
    Cursor(final Database dbHandle, Locker locker, CursorConfig cursorConfig) {

        if (cursorConfig == null) {
            cursorConfig = CursorConfig.DEFAULT;
        }

        /* Check that Database is open for internal Cursor usage. */
        final DatabaseImpl dbImpl = dbHandle.checkOpen();

        locker = LockerFactory.getReadableLocker(dbHandle, locker);

        init(dbHandle, dbImpl, locker, cursorConfig,
             false /*retainNonTxnLocks*/);
    }

    /**
     * Creates a cursor for a given locker and retainNonTxnLocks parameter.
     *
     * <p>The locker parameter must be non-null.  With this constructor, we use
     * the given locker and retainNonTxnLocks parameter without applying any
     * special rules for different lockers -- the caller must supply the
     * correct locker and retainNonTxnLocks combination.</p>
     */
    Cursor(final Database dbHandle,
           final Locker locker,
           CursorConfig cursorConfig,
           final boolean retainNonTxnLocks) {

        if (cursorConfig == null) {
            cursorConfig = CursorConfig.DEFAULT;
        }

        /* Check that Database is open for internal Cursor usage. */
        final DatabaseImpl dbImpl = dbHandle.checkOpen();

        init(dbHandle, dbImpl, locker, cursorConfig, retainNonTxnLocks);
    }

    /**
     * Creates a cursor for a given locker and retainNonTxnLocks parameter,
     * without a Database handle.
     *
     * <p>The locker parameter must be non-null.  With this constructor, we use
     * the given locker and retainNonTxnLocks parameter without applying any
     * special rules for different lockers -- the caller must supply the
     * correct locker and retainNonTxnLocks combination.</p>
     */
    Cursor(final DatabaseImpl databaseImpl,
           final Locker locker,
           CursorConfig cursorConfig,
           final boolean retainNonTxnLocks) {

        if (cursorConfig == null) {
            cursorConfig = CursorConfig.DEFAULT;
        }

        /* Check that Database is open for internal Cursor usage. */
        if (dbHandle != null) {
            dbHandle.checkOpen();
        }

        init(null /*dbHandle*/, databaseImpl, locker, cursorConfig,
             retainNonTxnLocks);
    }

    private void init(final Database dbHandle,
                      final DatabaseImpl databaseImpl,
                      final Locker locker,
                      final CursorConfig cursorConfig,
                      final boolean retainNonTxnLocks) {
        assert locker != null;

        /*
         * Allow locker to perform "open cursor" actions, such as consistency
         * checks for a non-transactional locker on a Replica.
         */
        try {
            locker.openCursorHook(databaseImpl);
        } catch (RuntimeException e) {
            locker.operationEnd();
            throw e;
        }

        cursorImpl = new CursorImpl(
            databaseImpl, locker, retainNonTxnLocks, isSecondaryCursor());

        transaction = locker.getTransaction();

        /* Perform eviction for user cursors. */
        cursorImpl.setAllowEviction(true);

        readUncommittedDefault =
            cursorConfig.getReadUncommitted() ||
            locker.isReadUncommittedDefault();

        /* Keep this logic in sync with updatesProhibitedException. */
        updateOperationsProhibited =
            locker.isReadOnly() ||
            (dbHandle != null && !dbHandle.isWritable()) ||
            (databaseImpl.isTransactional() && !locker.isTransactional()) ||
            (databaseImpl.isReplicated() && locker.isLocalWrite() &&
                !databaseImpl.getDbType().isMixedReplication()) ||
            (!databaseImpl.isReplicated() && !locker.isLocalWrite() &&
             !databaseImpl.getDbType().isMixedTransactional());

        this.dbImpl = databaseImpl;
        if (dbHandle != null) {
            this.dbHandle = dbHandle;
            dbHandle.addCursor(this);
        }

        this.config = cursorConfig;
        this.logger = databaseImpl.getEnv().getLogger();

        setCacheMode(null);
    }

    /**
     * Copy constructor.
     */
    Cursor(final Cursor cursor, final boolean samePosition) {

        readUncommittedDefault = cursor.readUncommittedDefault;
        updateOperationsProhibited = cursor.updateOperationsProhibited;

        cursorImpl = cursor.cursorImpl.cloneCursor(samePosition);
        dbImpl = cursor.dbImpl;
        dbHandle = cursor.dbHandle;
        if (dbHandle != null) {
            dbHandle.addCursor(this);
        }
        config = cursor.config;
        logger = dbImpl.getEnv().getLogger();
        defaultCacheMode = cursor.defaultCacheMode;
    }

    boolean isSecondaryCursor() {
        return false;
    }

    /**
     * Use to prevent internal ops from appearing in operation stats.
     */
    void excludeFromOpStats() {
        includeInOpStats = false;
    }

    /**
     * Internal entrypoint.
     */
    CursorImpl getCursorImpl() {
        return cursorImpl;
    }

    /**
     * Returns the Database handle associated with this Cursor.
     *
     * @return The Database handle associated with this Cursor.
     */
    @Override
    public Database getDatabase() {
        return dbHandle;
    }

    /**
     * Always returns non-null, while getDatabase() returns null if no handle
     * is associated with this cursor.
     */
    DatabaseImpl getDatabaseImpl() {
        return dbImpl;
    }

    /**
     * Returns this cursor's configuration.
     *
     * <p>This may differ from the configuration used to open this object if
     * the cursor existed previously.</p>
     *
     * @return This cursor's configuration.
     */
    public CursorConfig getConfig() {
        try {
            return config.clone();
        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        }
    }

    /**
     * Returns the default {@code CacheMode} used for subsequent operations
     * performed using this cursor. If {@link #setCacheMode} has not been
     * called with a non-null value, the configured Database or Environment
     * default is returned.
     *
     * @return the {@code CacheMode} default used for subsequent operations
     * using this cursor.
     */
    public CacheMode getCacheMode() {
        return defaultCacheMode;
    }

    /**
     * Sets the {@code CacheMode} default used for subsequent operations
     * performed using this cursor. This method may be used to override the
     * defaults specified using {@link DatabaseConfig#setCacheMode} and {@link
     * EnvironmentConfig#setCacheMode}. Note that the default is always
     * overridden by a non-null cache mode that is specified via
     * {@link ReadOptions} or {@link WriteOptions}.
     *
     * @param cacheMode is the default {@code CacheMode} used for subsequent
     * operations using this cursor, or null to configure the Database or
     * Environment default.
     *
     * @see CacheMode for further details.
     */
    public void setCacheMode(final CacheMode cacheMode) {

        this.defaultCacheMode =
            (cacheMode != null) ? cacheMode : dbImpl.getDefaultCacheMode();
    }

    /**
     * @hidden
     * For internal use only.
     * Used by KVStore.
     *
     * A RangeConstraint is used by search-range and next/previous methods to
     * prevent keys that are not inside the range from being returned.
     *
     * This method is not yet part of the public API because it has not been
     * designed with future-proofing or generality in mind, and has not been
     * reviewed.
     */
    public void setRangeConstraint(RangeConstraint rangeConstraint) {
        if (dbImpl.getSortedDuplicates()) {
            throw new UnsupportedOperationException("Not allowed with dups");
        }
        this.rangeConstraint = rangeConstraint;
    }

    /**
     * Sets range constraint to compare the first part of a given two-part key.
     */
    private void setPrefixConstraint(final Cursor c, final byte[] twoPartKey) {
        c.rangeConstraint = checkKey ->
            dbImpl.getMainKeyComparator().compare(checkKey, twoPartKey) == 0;
    }

    private void setPrefixConstraint(final Cursor c,
                                     final DatabaseEntry twoPartKey) {

        final byte[] key2Data = twoPartKey.getData();
        final int key2Off = twoPartKey.getOffset();
        final int key2Size = twoPartKey.getSize();

        c.rangeConstraint = checkKey ->
            dbImpl.getMainKeyComparator().compare(
                checkKey, 0, checkKey.length,
                null, 0, 0,
                key2Data, key2Off, key2Size) == 0;
    }

    private boolean checkRangeConstraint(final DatabaseEntry key) {
        assert key.getOffset() == 0;
        assert key.getData().length == key.getSize();

        if (rangeConstraint == null) {
            return true;
        }

        return rangeConstraint.inBounds(key.getData());
    }

    /**
     * Discards the cursor.
     *
     * <p>The cursor handle may not be used again after this method has been
     * called, regardless of the method's success or failure, with one
     * exception:  the {@code close} method itself may be called any number of
     * times.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    @Override
    public void close() {
        try {
            if (cursorImpl.isClosed()) {
                return;
            }

            /*
             * Do not call checkState here, to allow closing a cursor after an
             * operation failure.  [#17015]
             */
            checkEnv();
            cursorImpl.close();
            if (dbHandle != null) {
                dbHandle.removeCursor(this);
                dbHandle = null;
            }
        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        }
    }

    /**
     * Returns a new cursor with the same transaction and locker ID as the
     * original cursor.
     *
     * <p>This is useful when an application is using locking and requires
     * two or more cursors in the same thread of control.</p>
     *
     * @param samePosition If true, the newly created cursor is initialized
     * to refer to the same position in the database as the original cursor
     * (if any) and hold the same locks (if any). If false, or the original
     * cursor does not hold a database position and locks, the returned
     * cursor is uninitialized and will behave like a newly created cursor.
     *
     * @return A new cursor with the same transaction and locker ID as the
     * original cursor.
     *
     * @throws com.sleepycat.je.rep.DatabasePreemptedException in a replicated
     * environment if the master has truncated, removed or renamed the
     * database.
     *
     * @throws OperationFailureException if this exception occurred earlier and
     * caused the transaction to be invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed.
     */
    public Cursor dup(final boolean samePosition) {
        try {
            checkOpenAndState(false);
            return new Cursor(this, samePosition);
        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        }
    }

    /**
     * Deletes the record to which the cursor refers. When the database has
     * associated secondary databases, this method also deletes the associated
     * index records.
     *
     * <p>The cursor position is unchanged after a delete, and subsequent calls
     * to cursor functions expecting the cursor to refer to an existing record
     * will fail.</p>
     *
     * @param options the WriteOptions, or null to use default options.
     *
     * @return the OperationResult if the record is deleted, else null if the
     * record at the cursor position has already been deleted.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @since 7.0
     */
    public OperationResult delete(final WriteOptions options) {

        checkOpenAndState(true);
        checkDeleteWriteOptions(options);

        trace(Level.FINEST, "Cursor.delete: ", null);

        final CacheMode cacheMode =
            options != null ? options.getCacheMode() : null;

        BeforeImageContext bImgCtx = null;
        if (options != null && options.getBeforeImageTTL() > 0) {
            bImgCtx = new BeforeImageContext(
                    options.getBeforeImageTTL(),
                    options.getBeforeImageTTLUnit() == TimeUnit.HOURS);
        }
        return deleteInternal(dbImpl.getRepContext(), cacheMode, bImgCtx);
    }

    static void checkDeleteWriteOptions(final WriteOptions options) {

        if (options != null) {
            if (options.getModificationTime() != 0) {
                throw new IllegalArgumentException(
                    "modificationTime must be zero for a delete op.");
            }
            if (options.isTombstone()) {
                throw new IllegalArgumentException(
                    "Tombstone property must be false for a delete op.");
            }
        }
    }

    /**
     * Deletes the record to which the cursor refers. When the database has
     * associated secondary databases, this method also deletes the associated
     * index records.
     *
     * <p>The cursor position is unchanged after a delete, and subsequent calls
     * to cursor functions expecting the cursor to refer to an existing record
     * will fail.</p>
     *
     * <p>Calling this method is equivalent to calling {@link
     * #delete(WriteOptions)}.</p>
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if the record at the cursor position has
     * already been deleted; otherwise, {@link
     * com.sleepycat.je.OperationStatus#SUCCESS OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     */
    public OperationStatus delete() {
        final OperationResult result = delete(null);
        return result == null ?
            OperationStatus.KEYEMPTY : OperationStatus.SUCCESS;
    }

    /**
     * Inserts or updates a record according to the specified {@link Put}
     * type.
     *
     * <p>If the operation succeeds, the record will be locked according to the
     * {@link ReadOptions#getLockMode() lock mode} specified, the cursor will
     * be positioned on the record, its state will be {@code initialized},
     * and a non-null OperationResult will be returned. If the operation fails
     * because the record already exists (or does not exist, depending on the
     * putType), the cursor's state will be {@code uninitialized} and null is
     * returned. Note that {@link Put#CURRENT} does not move the cursor and
     * its state remains {@code initialized}.</p>
     *
     * <p>When the database has associated secondary databases, this method
     * also inserts or deletes associated index records as necessary.</p>
     *
     * <p>The following table lists each allowed operation. See the individual
     * {@link Put} operations for more information.</p>
     *
     * <div><table border="1">
	 * <caption style="display:none">""</caption>
     * <tr>
     *     <th>Put operation</th>
     *     <th>Description</th>
     *     <th>Returns null when?</th>
     *     <th>Other special rules</th>
     * </tr>
     * <tr>
     *     <td>{@link Put#OVERWRITE}</td>
     *     <td>Inserts or updates a record depending on whether a matching
     *     record is already present.</td>
     *     <td>Never returns null.</td>
     *     <td>Without duplicates, a matching record is one with the same key;
     *     with duplicates, it is one with the same key and data.</td>
     * </tr>
     * <tr>
     *     <td>{@link Put#NO_OVERWRITE}</td>
     *     <td>Inserts a record if a record with a matching key is not already
     *     present.</td>
     *     <td>When an existing record matches.</td>
     *     <td>If the database has duplicate keys, a record is inserted only if
     *     there are no records with a matching key.</td>
     * </tr>
     * <tr>
     *     <td>{@link Put#NO_DUP_DATA}</td>
     *     <td>Inserts a record in a database with duplicate keys if a record
     *     with a matching key and data is not already present.</td>
     *     <td>When an existing record matches.</td>
     *     <td>Without duplicates, this operation is not allowed.</td>
     * </tr>
     * <tr>
     *     <td>{@link Put#CURRENT}</td>
     *     <td>Updates the data of the record at the cursor position.</td>
     *     <td>When the record at the cursor position has been deleted.</td>
     *     <td>With duplicates, the data must be considered equal by the
     *     duplicate comparator, meaning that changing the data is only
     *     possible if a custom duplicate comparator is configured.
     *     <p>
     *     Cannot be used to update the key of an existing record and in
     *     fact the key parameter must be null.
     *     <p>
     *     A <a href="Cursor.html#partialEntry">partial data item</a> may be
     *     specified to optimize for partial data update.
     *     </td>
     * </tr>
     * </table></div>
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>. Must be null when
     * putType is {@code Put.CURRENT}.
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>. May be partial only when
     * putType is {@code Put.CURRENT}.
     *
     * @param putType the Put operation type. May not be null.
     *
     * @param options the WriteOptions, or null to use default options.
     *
     * @return the OperationResult if the record is written, else null.
     *
     * @throws DuplicateDataException if putType is Put.CURRENT and the old and
     * new data are not equal according to the configured duplicate comparator
     * or default comparator.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only, or putType is Put.NO_DUP_DATA and the
     * database is not configured for duplicates.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     * This includes passing a null putType, a null input key/data parameter,
     * an input key/data parameter with a null data array, a partial key/data
     * input parameter.
     *
     * @since 7.0
     */
    public OperationResult put(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final Put putType,
        final WriteOptions options) {

        try {
            checkOpen();

            trace(
                Level.FINEST, "Cursor.put: ", String.valueOf(putType),
                key, data, null);

            return putInternal(key, data, putType, options);

        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        }
    }

    /**
     * Performs the put() operation except for state checking and tracing.
     *
     * Allows passing a throughput stat index so it can be called for Database
     * and SecondaryCursor operations.
     */
    OperationResult putInternal(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final Put putType,
        WriteOptions options) {

        DatabaseUtil.checkForNullParam(putType, "putType");

        if (putType == Put.CURRENT) {
            if (key != null) {
                throw new IllegalArgumentException(
                    "The key must be null for Put.Current");
            }
        } else {
            DatabaseUtil.checkForNullDbt(key, "key", true);
            if (data != null) {
                DatabaseUtil.checkForPartial(data, "data");
            }
        }

        if (key != null) {
            DatabaseUtil.checkForPartial(key, "key");
        }

        DatabaseUtil.checkForNullDbt(data, "data", true);

        checkState(putType == Put.CURRENT /*mustBeInitialized*/);

        if (options == null) {
            options = DEFAULT_WRITE_OPTIONS;
        }

        return putInternal(
            key, data, new WriteParams(options, dbImpl),
            putType.getPutMode());
    }

    /**
     * Stores a key/data pair into the database.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #put(DatabaseEntry, DatabaseEntry, Put, WriteOptions)} with
     * {@link Put#OVERWRITE}.</p>
     *
     * <p>If the put method succeeds, the cursor is positioned to refer to the
     * newly inserted item.</p>
     *
     * <p>If the key already appears in the database and duplicates are
     * supported, the new data value is inserted at the correct sorted
     * location, unless the new data value also appears in the database
     * already. In the later case, although the given key/data pair compares
     * equal to an existing key/data pair, the two records may not be identical
     * if custom comparators are used, in which case the existing record will
     * be replaced with the new record. If the key already appears in the
     * database and duplicates are not supported, the data associated with
     * the key will be replaced.</p>
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>..
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @return {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus put(
        final DatabaseEntry key,
        final DatabaseEntry data) {

        final OperationResult result = put(key, data, Put.OVERWRITE, null);

        EnvironmentFailureException.assertState(result != null);
        return OperationStatus.SUCCESS;
    }

    /**
     * Stores a key/data pair into the database.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #put(DatabaseEntry, DatabaseEntry, Put, WriteOptions)} with
     * {@link Put#NO_OVERWRITE}.</p>
     *
     * <p>If the putNoOverwrite method succeeds, the cursor is positioned to
     * refer to the newly inserted item.</p>
     *
     * <p>If the key already appears in the database, putNoOverwrite will
     * return {@link com.sleepycat.je.OperationStatus#KEYEXIST
     * OperationStatus.KEYEXIST}.</p>
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>..
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEXIST
     * OperationStatus.KEYEXIST} if the key already appears in the database,
     * else {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus putNoOverwrite(
        final DatabaseEntry key,
        final DatabaseEntry data) {

        final OperationResult result = put(
            key, data, Put.NO_OVERWRITE, null);

        return result == null ?
            OperationStatus.KEYEXIST : OperationStatus.SUCCESS;
    }

    /**
     * Stores a key/data pair into the database. The database must be
     * configured for duplicates.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #put(DatabaseEntry, DatabaseEntry, Put, WriteOptions)} with
     * {@link Put#NO_DUP_DATA}.</p>
     *
     * <p>If the putNoDupData method succeeds, the cursor is positioned to
     * refer to the newly inserted item.</p>
     *
     * <p>Insert the specified key/data pair into the database, unless a
     * key/data pair comparing equally to it already exists in the database.
     * If a matching key/data pair already exists in the database, {@link
     * com.sleepycat.je.OperationStatus#KEYEXIST OperationStatus.KEYEXIST} is
     * returned.</p>
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>..
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEXIST
     * OperationStatus.KEYEXIST} if the key/data pair already appears in the
     * database, else {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter, or
     * the database is read-only, or the database is not configured for
     * duplicates.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus putNoDupData(
        final DatabaseEntry key,
        final DatabaseEntry data) {

        final OperationResult result = put(
            key, data, Put.NO_DUP_DATA, null);

        return result == null ?
            OperationStatus.KEYEXIST : OperationStatus.SUCCESS;
    }

    /**
     * Replaces the data in the key/data pair at the current cursor position.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #put(DatabaseEntry, DatabaseEntry, Put, WriteOptions)} with
     * {@link Put#CURRENT}.</p>
     *
     * <p>Overwrite the data of the key/data pair to which the cursor refers
     * with the specified data item. This method will return
     * OperationStatus.NOTFOUND if the cursor currently refers to an
     * already-deleted key/data pair.</p>
     *
     * <p>For a database that does not support duplicates, the data may be
     * changed by this method.  If duplicates are supported, the data may be
     * changed only if a custom partial comparator is configured and the
     * comparator considers the old and new data to be equal (that is, the
     * comparator returns zero).  For more information on partial comparators
     * see {@link DatabaseConfig#setDuplicateComparator}.</p>
     *
     * <p>If the old and new data are unequal according to the comparator, a
     * {@link DuplicateDataException} is thrown.  Changing the data in this
     * case would change the sort order of the record, which would change the
     * cursor position, and this is not allowed.  To change the sort order of a
     * record, delete it and then re-insert it.</p>
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for partial data update.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if the key/pair at the cursor position has
     * been deleted; otherwise, {@link
     * com.sleepycat.je.OperationStatus#SUCCESS OperationStatus.SUCCESS}.
     *
     * @throws DuplicateDataException if the old and new data are not equal
     * according to the configured duplicate comparator or default comparator.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus putCurrent(final DatabaseEntry data) {

        final OperationResult result = put(null, data, Put.CURRENT, null);

        return result == null ?
            OperationStatus.KEYEMPTY : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to a record according to the specified {@link Get}
     * type.
     *
     * <p>If the operation succeeds, the record at the resulting cursor
     * position will be locked according to the {@link
     * ReadOptions#getLockMode() lock mode} specified, its state will be
     * {@code initialized}, the key and/or data will be returned via the
     * (non-null) DatabaseEntry parameters, and a non-null OperationResult
     * will be returned. If the operation fails because the record requested
     * is not found, the cursor's state will be {@code uninitialized} and null
     * is returned. Note that {@link Get#CURRENT} does not move the cursor and
     * its state remains {@code initialized}</p>
     *
     * <p>The following table lists each allowed operation and whether the key
     * and data parameters are <a href="DatabaseEntry.html#params">input or
     * output parameters</a>. Also specified is whether the cursor must be
     * initialized (positioned on a record) before calling this method. See the
     * individual {@link Get} operations for more information.</p>
     *
     * <div><table border="1">
	 * <caption style="display:none">""</caption>
     * <tr>
     *     <th>Get operation</th>
     *     <th>Description</th>
     *     <th>'key' parameter</th>
     *     <th>'data' parameter</th>
     *     <th>Cursor position<br>must be initialized?</th>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH}</td>
     *     <td>Searches using an exact match by key.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH_BOTH}</td>
     *     <td>Searches using an exact match by key and data.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH_GTE}</td>
     *     <td>Searches using a GTE match by key.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input/output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#SEARCH_BOTH_GTE}</td>
     *     <td>Searches using an exact match by key and a GTE match by data.</td>
     *     <td><a href="DatabaseEntry.html#inParam">input</a></td>
     *     <td><a href="DatabaseEntry.html#inParam">input/output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#CURRENT}</td>
     *     <td>Accesses the current record</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>yes</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#FIRST}</td>
     *     <td>Finds the first record in the database.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#LAST}</td>
     *     <td>Finds the last record in the database.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#NEXT}</td>
     *     <td>Moves to the next record.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no**</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#NEXT_DUP}</td>
     *     <td>Moves to the next record with the same key.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>yes</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#NEXT_NO_DUP}</td>
     *     <td>Moves to the next record with a different key.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no**</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#PREV}</td>
     *     <td>Moves to the previous record.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no**</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#PREV_DUP}</td>
     *     <td>Moves to the previous record with the same key.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>yes</td>
     * </tr>
     * <tr>
     *     <td>{@link Get#PREV_NO_DUP}</td>
     *     <td>Moves to the previous record with a different key.</td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td><a href="DatabaseEntry.html#outParam">output</a></td>
     *     <td>no**</td>
     * </tr>
     * </table></div>
     *
     * <p>** - For these 'next' and 'previous' operations the cursor may be
     * uninitialized, in which case the cursor will be moved to the first or
     * last record, respectively.</p>
     *
     * @param key the key input or output parameter, depending on getType.
     *
     * @param data the data input or output parameter, depending on getType.
     *
     * @param getType the Get operation type. May not be null.
     *
     * @param options the ReadOptions, or null to use default options.
     *
     * @return the OperationResult if the record requested is found, else null.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * the cursor is uninitialized (not positioned on a record) and this is not
     * permitted (see above), or the non-transactional cursor was created in a
     * different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     * This includes passing a null getType, a null input key/data parameter,
     * an input key/data parameter with a null data array, a partial key/data
     * input parameter, and specifying a {@link ReadOptions#getLockMode()
     * lock mode} of READ_COMMITTED.
     *
     * @since 7.0
     */
    @Override
    public OperationResult get(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final Get getType,
        ReadOptions options) {

        try {
            checkOpen();

            if (options == null) {
                options = DEFAULT_READ_OPTIONS;
            }

            final LockMode lockMode = options.getLockMode();

            trace(
                Level.FINEST, "Cursor.get: ", String.valueOf(getType),
                key, data, lockMode);

            return getInternal(key, data, getType, options, lockMode);

        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        }
    }

    /**
     * Performs the get() operation except for state checking and tracing.
     *
     * The LockMode is passed because for Database operations it is sometimes
     * different than ReadOptions.getLockMode.
     *
     * Allows passing a throughput stat index so it can be called for Database
     * and SecondaryCursor operations.
     */
    OperationResult getInternal(
        DatabaseEntry key,
        DatabaseEntry data,
        Get getType,
        final ReadOptions options,
        final LockMode lockMode) {

        DatabaseUtil.checkForNullParam(getType, "getType");

        final CacheMode cacheMode = options.getCacheMode();
        final SearchMode searchMode = getType.getSearchMode();
        final boolean excludeTombstones = options.getExcludeTombstones();

        if (searchMode != null) {
            checkState(false /*mustBeInitialized*/);

            DatabaseUtil.checkForNullDbt(key, "key", true);
            DatabaseUtil.checkForPartial(key, "key");

            if (searchMode.isDataSearch() ||
                (searchMode == SearchMode.ANY_RANGE &&
                 dbImpl.getSortedDuplicates())) {
                DatabaseUtil.checkForNullDbt(data, "data", true);
                DatabaseUtil.checkForPartial(data, "data");
            } else {
                if (data == null) {
                    data = NO_RETURN_DATA;
                }
            }

            return search(
                key, data, lockMode, excludeTombstones, cacheMode, searchMode,
                true);
        }

        if (key == null) {
            key = NO_RETURN_DATA;
        }
        if (data == null) {
            data = NO_RETURN_DATA;
        }

        GetMode getMode = getType.getGetMode();

        if (getType.getAllowNextPrevUninitialized() &&
            cursorImpl.isNotInitialized()) {

            assert getMode != null;
            getType = getMode.isForward() ? Get.FIRST : Get.LAST;
            getMode = null;
        }

        if (getMode != null) {
            checkState(true /*mustBeInitialized*/);

            return retrieveNext(
                key, data, lockMode, excludeTombstones, cacheMode, getMode);
        }

        if (getType == Get.CURRENT) {
            checkState(true /*mustBeInitialized*/);

            return getCurrentInternal(
                key, data, lockMode, excludeTombstones, cacheMode);
        }

        assert getType == Get.FIRST || getType == Get.LAST;
        checkState(false /*mustBeInitialized*/);

        return position(
            key, data, lockMode, excludeTombstones, cacheMode,
            getType == Get.FIRST);
    }

    /**
     * Returns the key/data pair to which the cursor refers.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#CURRENT}.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if the key/pair at the cursor position has
     * been deleted; otherwise, {@link
     * com.sleepycat.je.OperationStatus#SUCCESS OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    @Override
    public OperationStatus getCurrent(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.CURRENT, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.KEYEMPTY : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to the first key/data pair of the database, and returns
     * that pair.  If the first key has duplicate values, the first data item
     * in the set of duplicates is returned.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#FIRST}.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getFirst(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.FIRST, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to the last key/data pair of the database, and returns
     * that pair.  If the last key has duplicate values, the last data item in
     * the set of duplicates is returned.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#LAST}.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getLast(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.LAST, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to the next key/data pair and returns that pair.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#NEXT}.</p>
     *
     * <p>If the cursor is not yet initialized, move the cursor to the first
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the next key/data pair of the database, and that pair
     * is returned.  In the presence of duplicate key values, the value of the
     * key may not change.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    @Override
    public OperationStatus getNext(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.NEXT, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * If the next key/data pair of the database is a duplicate data record for
     * the current key/data pair, moves the cursor to the next key/data pair of
     * the database and returns that pair.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#NEXT_DUP}.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getNextDup(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.NEXT_DUP, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to the next non-duplicate key/data pair and returns
     * that pair.  If the matching key has duplicate values, the first data
     * item in the set of duplicates is returned.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#NEXT_NO_DUP}.</p>
     *
     * <p>If the cursor is not yet initialized, move the cursor to the first
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the next non-duplicate key of the database, and that
     * key/data pair is returned.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getNextNoDup(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.NEXT_NO_DUP, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to the previous key/data pair and returns that pair.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#PREV}.</p>
     *
     * <p>If the cursor is not yet initialized, move the cursor to the last
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the previous key/data pair of the database, and that
     * pair is returned. In the presence of duplicate key values, the value of
     * the key may not change.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getPrev(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.PREV, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * If the previous key/data pair of the database is a duplicate data record
     * for the current key/data pair, moves the cursor to the previous key/data
     * pair of the database and returns that pair.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#PREV_DUP}.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getPrevDup(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.PREV_DUP, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to the previous non-duplicate key/data pair and returns
     * that pair.  If the matching key has duplicate values, the last data item
     * in the set of duplicates is returned.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#PREV_NO_DUP}.</p>
     *
     * <p>If the cursor is not yet initialized, move the cursor to the last
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the previous non-duplicate key of the database, and
     * that key/data pair is returned.</p>
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getPrevNoDup(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.PREV_NO_DUP, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Skips forward a given number of key/data pairs and returns the number by
     * which the cursor is moved.
     *
     * <p>Without regard to performance, calling this method is equivalent to
     * repeatedly calling {@link #getNext getNext} with {@link
     * LockMode#READ_UNCOMMITTED} to skip over the desired number of key/data
     * pairs, and then calling {@link #getCurrent getCurrent} with the {@code
     * lockMode} parameter to return the final key/data pair.</p>
     *
     * <p>With regard to performance, this method is optimized to skip over
     * key/value pairs using a smaller number of Btree operations.  When there
     * is no contention on the bottom internal nodes (BINs) and all BINs are in
     * cache, the number of Btree operations is reduced by roughly two orders
     * of magnitude, where the exact number depends on the {@link
     * EnvironmentConfig#NODE_MAX_ENTRIES} setting.  When there is contention
     * on BINs or fetching BINs is required, the scan is broken up into smaller
     * operations to avoid blocking other threads for long time periods.</p>
     *
     * <p>If the returned count is greater than zero, then the key/data pair at
     * the new cursor position is also returned.  If zero is returned, then
     * there are no key/value pairs that follow the cursor position and a
     * key/data pair is not returned.</p>
     *
     * <p>Tombstones are treated as ordinary records (they are counted) by
     * this method. TODO: Provide a way to exclude tombstones.</p>
     *
     * @param maxCount the maximum number of key/data pairs to skip, i.e., the
     * maximum number by which the cursor should be moved; must be greater
     * than zero.
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return the number of key/data pairs skipped, i.e., the number by which
     * the cursor has moved; if zero is returned, the cursor position is
     * unchanged and the key/data pair is not returned.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public long skipNext(
        final long maxCount,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {
        /* TODO: Add a way to configure excludeTombstones? */

        checkOpenAndState(true);
        if (maxCount <= 0) {
            throw new IllegalArgumentException("maxCount must be positive: " +
                                               maxCount);
        }
        trace(Level.FINEST, "Cursor.skipNext: ", lockMode);

        return skipInternal(
            maxCount, true /*forward*/, key, data, lockMode, false, null);
    }

    /**
     * Skips backward a given number of key/data pairs and returns the number
     * by which the cursor is moved.
     *
     * <p>Without regard to performance, calling this method is equivalent to
     * repeatedly calling {@link #getPrev getPrev} with {@link
     * LockMode#READ_UNCOMMITTED} to skip over the desired number of key/data
     * pairs, and then calling {@link #getCurrent getCurrent} with the {@code
     * lockMode} parameter to return the final key/data pair.</p>
     *
     * <p>With regard to performance, this method is optimized to skip over
     * key/value pairs using a smaller number of Btree operations.  When there
     * is no contention on the bottom internal nodes (BINs) and all BINs are in
     * cache, the number of Btree operations is reduced by roughly two orders
     * of magnitude, where the exact number depends on the {@link
     * EnvironmentConfig#NODE_MAX_ENTRIES} setting.  When there is contention
     * on BINs or fetching BINs is required, the scan is broken up into smaller
     * operations to avoid blocking other threads for long time periods.</p>
     *
     * <p>If the returned count is greater than zero, then the key/data pair at
     * the new cursor position is also returned.  If zero is returned, then
     * there are no key/value pairs that follow the cursor position and a
     * key/data pair is not returned.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * <p>Tombstones are treated as ordinary records (they are counted) by
     * this method. TODO: Provide a way to exclude tombstones.</p>
     *
     * @param maxCount the maximum number of key/data pairs to skip, i.e., the
     * maximum number by which the cursor should be moved; must be greater
     * than zero.
     *
     * @param key the key returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return the number of key/data pairs skipped, i.e., the number by which
     * the cursor has moved; if zero is returned, the cursor position is
     * unchanged and the key/data pair is not returned.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public long skipPrev(
        final long maxCount,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        checkOpenAndState(true);
        if (maxCount <= 0) {
            throw new IllegalArgumentException("maxCount must be positive: " +
                                               maxCount);
        }
        trace(Level.FINEST, "Cursor.skipPrev: ", lockMode);

        return skipInternal(
            maxCount, false /*forward*/, key, data, lockMode, false, null);
    }

    /**
     * Moves the cursor to the given key of the database, and returns the datum
     * associated with the given key.  If the matching key has duplicate
     * values, the first data item in the set of duplicates is returned.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#SEARCH}.</p>
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getSearchKey(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.SEARCH, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to the closest matching key of the database, and
     * returns the data item associated with the matching key.  If the matching
     * key has duplicate values, the first data item in the set of duplicates
     * is returned.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#SEARCH_GTE}.</p>
     *
     * <p>The returned key/data pair is for the smallest key greater than or
     * equal to the specified key (as determined by the key comparison
     * function), permitting partial key matches and range searches.</p>
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a> and returned as output.
     *
     * @param data the data returned as
     * <a href="DatabaseEntry.html#outParam">output</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes
     * are used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getSearchKeyRange(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.SEARCH_GTE, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to the specified key/data pair, where both the key and
     * data items must match.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#SEARCH_BOTH}.</p>
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getSearchBoth(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.SEARCH_BOTH, DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Moves the cursor to the specified key and closest matching data item of
     * the database.
     *
     * <p>Calling this method is equivalent to calling {@link
     * #get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)} with
     * {@link Get#SEARCH_BOTH_GTE}.</p>
     *
     * <p>In the case of any database supporting sorted duplicate sets, the
     * returned key/data pair is for the smallest data item greater than or
     * equal to the specified data item (as determined by the duplicate
     * comparison function), permitting partial matches and range searches in
     * duplicate data sets.</p>
     *
     * <p>In the case of databases that do not support sorted duplicate sets,
     * this method is equivalent to getSearchBoth.</p>
     *
     * @param key the key used as
     * <a href="DatabaseEntry.html#inParam">input</a>.
     *
     * @param data the data used as
     * <a href="DatabaseEntry.html#inParam">input</a> and returned as output.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getSearchBothRange(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        final OperationResult result = get(
            key, data, Get.SEARCH_BOTH_GTE,
            DbInternal.getReadOptions(lockMode));

        return result == null ?
            OperationStatus.NOTFOUND : OperationStatus.SUCCESS;
    }

    /**
     * Returns a count of the number of data items for the key to which the
     * cursor refers.
     *
     * <p>If the database is configured for duplicates, the database is scanned
     * internally, without taking any record locks, to count the number of
     * non-deleted entries.  Although the internal scan is more efficient under
     * some conditions, the result is the same as if a cursor were used to
     * iterate over the entries using {@link LockMode#READ_UNCOMMITTED}.</p>
     *
     * <p>If the database is not configured for duplicates, the count returned
     * is always zero or one, depending on the record at the cursor position is
     * deleted or not.</p>
     *
     * <p>The cost of this method is directly proportional to the number of
     * records scanned.</p>
     *
     * <p>Tombstones are treated as ordinary records (they are counted) by
     * this method. TODO: Provide a way to exclude tombstones.</p>
     *
     * @return A count of the number of data items for the key to which the
     * cursor refers.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     */
    public int count() {

        checkOpenAndState(true);
        trace(Level.FINEST, "Cursor.count: ", null);

        return countInternal(false);
    }

    /**
     * Returns a rough estimate of the count of the number of data items for
     * the key to which the cursor refers.
     *
     * <p>If the database is configured for duplicates, a quick estimate of the
     * number of records is computed using information in the Btree.  Because
     * the Btree is unbalanced, in some cases the estimate may be off by a
     * factor of two or more.  The estimate is accurate when the number of
     * records is less than the configured {@link
     * DatabaseConfig#setNodeMaxEntries NodeMaxEntries}.</p>
     *
     * <p>If the database is not configured for duplicates, the count returned
     * is always zero or one, depending on the record at the cursor position is
     * deleted or not.</p>
     *
     * <p>The cost of this method is fixed, rather than being proportional to
     * the number of records scanned.  Because its accuracy is variable, this
     * method should normally be used when accuracy is not required, such as
     * for query optimization, and a fixed cost operation is needed. For
     * example, this method is used internally for determining the index
     * processing order in a {@link JoinCursor}.</p>
     *
     * <p>Tombstones are treated as ordinary records (they are counted) by
     * this method. TODO: Provide a way to exclude tombstones.</p>
     *
     * @return an estimate of the count of the number of data items for the key
     * to which the cursor refers.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     */
    public long countEstimate() {

        checkOpenAndState(true);
        trace(Level.FINEST, "Cursor.countEstimate: ", null);

        return countEstimateInternal(false);
    }

    /**
     * Perform an optimized search and deletion. Secondary associations are not
     * handled, and therefore this method may only be used for replay and
     * secondaries. It could also be used for primaries with no associations,
     * but blind deletions are not supported in that case.
     *
     * @param deleteKey is the primary key or two-part secondary key.
     *
     * @param allowBlindDelete is true when the key is known to be present.
     *
     * @return non-null if the key is found and a regular deletion is
     * performed, or a blind deletion is performed. Returns null if the key
     * does not exist, indicating an error when allowBlindDeletions is true.
     *
     * @see CursorImpl#searchAndDelete for details.
     */
    OperationResult searchAndDelete(
        final DatabaseEntry deleteKey,
        final ReplayPreprocessor preprocessor,
        final long modificationTime,
        final long oldExpirationTime,
        final ReplicationContext repContext,
        final CacheMode cacheMode,
        final boolean allowBlindDelete,
        final BeforeImageContext bImgCtx) {

        final boolean hasUserTriggers = (dbImpl.getTriggers() != null);
        final DatabaseEntry oldData =
            hasUserTriggers ? new DatabaseEntry() : null;

        synchronized (getTxnSynchronizer()) {
            beginMoveCursor(false /*samePosition*/, cacheMode);
            OperationResult result = null;
            try {
                result = cursorImpl.searchAndDelete(
                    deleteKey, preprocessor, modificationTime,
                    oldExpirationTime, repContext,
                    (cacheMode != null) ? cacheMode : defaultCacheMode,
                    allowBlindDelete, oldData, bImgCtx);
                if (result != null && result.getBeforeImageDBEntry() != null) {
                    dbImpl.getEnv().getBeforeImageIndex()
                        .put(result.getBeforeImageDBEntry());
                }

            } finally {
                endMoveCursor(result != null);
            }
            if (includeInOpStats && result != null) {
                dbImpl.getEnv().incDeleteOps(dbImpl);
            }
            if (result != null && hasUserTriggers) {
                TriggerManager.runDeleteTriggers(
                    cursorImpl.getLocker(), dbImpl, deleteKey, oldData);
            }
            return result;
        }
    }

    OperationResult searchAndDelete(
        final DatabaseEntry deleteKey,
        final ReplayPreprocessor preprocessor,
        final long modificationTime,
        final long oldExpirationTime,
        final ReplicationContext repContext,
        final CacheMode cacheMode,
        final boolean allowBlindDelete) {
           return searchAndDelete(deleteKey, preprocessor, modificationTime,
                           oldExpirationTime, repContext, cacheMode,
                           allowBlindDelete, null);
    }

    /**
     * Version of deleteInternal that allows specifying ReplicationContext.
     * Notifies triggers.
     */
    OperationResult deleteWithRepContext(final ReplicationContext repContext) {
        return deleteNotify(repContext, null, 0L /*modificationTime*/, null);
    }

    /**
     * Internal version of delete() that does no parameter checking.  Notify
     * triggers, update secondaries and enforce foreign key constraints.
     */
    OperationResult deleteInternal(final ReplicationContext repContext,
                                   final CacheMode cacheMode,
                                   final BeforeImageContext bImgCtx) {
        checkUpdatesAllowed();
        return deleteNotify(repContext, cacheMode, 0L /*modificationTime*/,
                            bImgCtx);
    }

    /**
     * Implementation of deleteInternal that does not check disk limits.
     *
     * Note that this algorithm is duplicated in Database and Cursor for
     * efficiency reasons: in Cursor delete we must separately fetch the key
     * and data, while in Database delete we know the key and have to search
     * anyway so we can get the old data when we search.  The two algorithms
     * need to be kept in sync.
     */
    private OperationResult deleteNotify(final ReplicationContext repContext,
                                         final CacheMode cacheMode,
                                         final long modificationTime,
                                         final BeforeImageContext bImgCtx) {

        final boolean hasUserTriggers = (dbImpl.getTriggers() != null);
        final boolean hasAssociations = (dbHandle != null) &&
            dbHandle.hasSecondaryOrForeignKeyAssociations();

        if (hasAssociations) {
            dbImpl.getEnv().acquireSecondaryAssociationsReadLock();
        }
        try {
            /* The key is needed if there are secondaries or triggers. */
            final DatabaseEntry key;
            if (hasAssociations || hasUserTriggers) {
                key = new DatabaseEntry();
                key.setData(cursorImpl.getCurrentKey());
            } else {
                key = null;
            }

            /*
             * Get secondaries from the association and determine whether the
             * old data is needed.
             */
            final Collection<SecondaryDatabase> secondaries;
            final Collection<SecondaryDatabase> fkSecondaries;
            final boolean needOldData;
            if (hasAssociations) {
                secondaries = dbHandle.secAssoc.getSecondaries(key);
                fkSecondaries = dbHandle.foreignKeySecondaries;
                needOldData = hasUserTriggers ||
                    SecondaryDatabase.needOldDataForDelete(secondaries);
            } else {
                secondaries = null;
                fkSecondaries = null;
                needOldData = hasUserTriggers;
            }

            /*
             * Get old data only if needed.  Even if the old data is not
             * needed, if there are associations we must lock the record with
             * RMW before calling onForeignKeyDelete.
             */
            final DatabaseEntry oldData =
                needOldData ? (new DatabaseEntry()) : null;

            final OperationResult readResult;
            long oldModificationTime = 0;
            int oldStorageSize = 0;

            if (needOldData || hasAssociations) {
                readResult = getCurrentInternal(
                    key, oldData, LockMode.RMW, false /*excludeTombstones*/,
                    cacheMode);

                if (readResult == null) {
                    return null;
                }
                oldModificationTime = readResult.getModificationTime();
                oldStorageSize = readResult.getStorageSize();
            } else {
                readResult = null;
            }

            /*
             * Enforce foreign key constraints first, so that
             * ForeignKeyDeleteAction.ABORT is applied before deletions.
             */
            final Locker locker = cursorImpl.getLocker();
            if (fkSecondaries != null) {
                for (final SecondaryDatabase secDb : fkSecondaries) {
                    secDb.onForeignKeyDelete(locker, key, cacheMode);
                }
            }

            /*
             * The actual deletion.
             */
            final OperationResult deleteResult =
                deleteNoNotify(cacheMode, modificationTime, repContext,
                               bImgCtx);

            if (deleteResult == null) {
                return null;
            }

            /*
             * Update secondaries after actual deletion, so that replica replay
             * will lock the primary before the secondaries. This locking order
             * is required for secondary deadlock avoidance.
             */
            if (secondaries != null) {
                int nWrites = 0;

                for (final SecondaryDatabase secDb : secondaries) {
                    nWrites += secDb.updateSecondary(
                        locker, null /*cursor*/, dbImpl, cursorImpl, key,
                        oldData, null /*newData*/, cacheMode,
                        0 /*newModificationTime*/,
                        oldModificationTime,
                        0 /*expirationTime*/,
                        false /*expirationUpdated*/,
                        readResult.getExpirationTime(),
                        0 /*storageSize*/,
                        oldStorageSize,
                        readResult.isTombstone(),
                        false /*newTombstone*/);
                }

                cursorImpl.setNSecondaryWrites(nWrites);
            }

            /* Run triggers after actual deletion. */
            if (hasUserTriggers) {
                TriggerManager.runDeleteTriggers(locker, dbImpl, key, oldData);
            }

            return deleteResult;
        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        } finally {
            if (hasAssociations) {
                dbImpl.getEnv().releaseSecondaryAssociationsReadLock();
            }
        }
    }
    OperationResult deleteNoNotify(final CacheMode cacheMode,
                                   final ReplicationContext repContext) {

        return deleteNoNotify(cacheMode, 0L /*modificationTime*/,
                repContext, null);
    }

    /**
     * Delete at current position.   Does not notify triggers (does not perform
     * secondary updates).
     */
    private OperationResult deleteNoNotify(
        final CacheMode cacheMode,
        final long modificationTime,
        final ReplicationContext repContext,
        final BeforeImageContext bImgCtx ) {

        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            final EnvironmentImpl envImpl = dbImpl.getEnv();

            /* This operation does not change the cursor position. */
            beginUseCurrentPosition(cacheMode);

            final OperationResult result = cursorImpl.deleteCurrentRecord(
                modificationTime, repContext, false /*latched*/, bImgCtx);

            if (result !=null && result.getBeforeImageDBEntry() != null) {
                envImpl.getBeforeImageIndex()
                    .put(result.getBeforeImageDBEntry());
            }

            if (result != null && includeInOpStats) {
                envImpl.incDeleteOps(dbImpl);
            }

            endUseCurrentPosition();
            return result;
       }
    }

    /**
     * Version of putInternal that allows passing an existing LN, does not
     * interpret duplicates, and does not check disk limits.  Used for
     * replication stream replay.
     *
     * Notifies triggers. Replay triggers are used by KVS. Note that although
     * putNotify is called, secondaries are not populated because this cursor
     * is internal and has no associated Database handle.
     */
    OperationResult putForReplay(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LNLogEntry lnEntry,
        final ReplayPreprocessor preprocessor,
        final PutMode putMode,
        final ReplicationContext repContext) {

        assert putMode != PutMode.CURRENT;

        WriteParams writeParams = null;
        
        /**
         * Before Image Support for Replay
         */
        if (lnEntry instanceof BeforeImageLNLogEntry) {
            writeParams = new WriteParams(null /* cacheMode */, preprocessor,
              repContext, lnEntry.getExpiration(),
              lnEntry.isExpirationInHours(), true /* updateExpiration */,
              lnEntry.getModificationTime(), lnEntry.isTombstone(), null,
              null, null, lnEntry.isBeforeImageEnabled(),
              ((BeforeImageLNLogEntry) lnEntry)
              .getBeforeImageExpiration(),
              ((BeforeImageLNLogEntry) lnEntry)
              .isBeforeImageExpirationInHours());
        } else {
            writeParams = new WriteParams(null /* cacheMode */, preprocessor,
              repContext, lnEntry.getExpiration(),
              lnEntry.isExpirationInHours(), true /* updateExpiration */,
              lnEntry.getModificationTime(), lnEntry.isTombstone(),
              null/* allIndexDbNames */, null/* allIndexIds */,
              null/* indexesToUpdate */, lnEntry.isBeforeImageEnabled());
        }

        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            return putNotify(key, data, lnEntry.getLN(), writeParams, putMode);
        }
    }

    OperationResult putWithRepContext(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final PutMode putMode,
        final ReplicationContext repContext) {

        synchronized (getTxnSynchronizer()) {
            checkTxnState();

            return putNotify(
                key, data, null /*lnForReplay*/,
                new WriteParams(repContext), putMode);
        }
    }

    /**
     * Internal version of put that does no parameter checking.  Interprets
     * duplicates and notifies triggers.
     */
    OperationResult putInternal(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final WriteParams writeParams,
        final PutMode putMode) {

        checkUpdatesAllowed(writeParams);

        synchronized (getTxnSynchronizer()) {
            checkTxnState();

            if (dbImpl.getSortedDuplicates()) {
                return putHandleDups(key, data, writeParams, putMode);
            }

            if (putMode == PutMode.NO_DUP_DATA) {
                throw new UnsupportedOperationException(
                    "Database is not configured for duplicate data.");
            }

            return putNoDups(key, data, writeParams, putMode);
        }
    }

    /**
     * Interpret duplicates for the various 'putXXX' operations.
     */
    private OperationResult putHandleDups(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final WriteParams writeParams,
        final PutMode putMode) {

        if (putMode == PutMode.NO_OVERWRITE) {
            throw new UnsupportedOperationException(
                "NO_OVERWRITE not allowed for a duplicates DB.");
        }

        if (writeParams.tombstone) {
            throw new IllegalArgumentException(
                "Tombstone property must be false for a put op " +
                    "in a duplicates DB.");
        }
        if (writeParams.modificationTime != 0) {
            throw new IllegalArgumentException(
                "modificationTime must be zero for a put op.");
        }

        switch (putMode) {
        case OVERWRITE:
            return dupsPutOverwrite(key, data, writeParams);
        case NO_DUP_DATA:
            return dupsPutNoDupData(key, data, writeParams);
        case CURRENT:
            return dupsPutCurrent(data, writeParams);
        default:
            throw EnvironmentFailureException.unexpectedState(
                putMode.toString());
        }
    }

    /**
     * Interpret duplicates for the put() operation.
     */
    private OperationResult dupsPutOverwrite(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final WriteParams writeParams) {

        final DatabaseEntry twoPartKey = DupKeyData.combine(key, data);
        
        return putNoDups(
            twoPartKey, EMPTY_DUP_DATA, writeParams, PutMode.OVERWRITE);
    }

    /**
     * Interpret duplicates for putNoDupData operation.
     */
    private OperationResult dupsPutNoDupData(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final WriteParams writeParams) {

        final DatabaseEntry twoPartKey = DupKeyData.combine(key, data);
        
        return putNoDups(
            twoPartKey, EMPTY_DUP_DATA, writeParams, PutMode.NO_OVERWRITE);
    }

    /**
     * Interpret duplicates for putCurrent operation.
     *
     * Get old key/data, replace data portion, and put new key/data.
     *
     * Arguably we could skip the replacement if there is no user defined
     * comparison function and the new data is the same.
     */
    private OperationResult dupsPutCurrent(
        final DatabaseEntry newData,
        final WriteParams writeParams) {

        final DatabaseEntry oldTwoPartKey =
            new DatabaseEntry(cursorImpl.getCurrentKey());

        final DatabaseEntry key = new DatabaseEntry();
        DupKeyData.split(oldTwoPartKey, key, null);

        final DatabaseEntry newTwoPartKey = DupKeyData.combine(key, newData);
        
        return putNoDups(
            newTwoPartKey, EMPTY_DUP_DATA, writeParams, PutMode.CURRENT);
    }

    /**
     * Eventually, all insertions/updates are happening via this method.
     */
    private OperationResult putNoDups(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final WriteParams writeParams,
        final PutMode putMode) {

        return putNotify(
            key, data, null /*lnForReplay*/, writeParams, putMode);
    }

    /**
     * This single method is used for all put operations in order to notify
     * triggers and perform secondary updates in one place. Does not interpret
     * duplicates.
     *
     * WARNING: When the cursor has no Database handle, which is true when
     * called from the replication replayer, this method notifies user triggers
     * but does not do secondary updates.  This is correct for replication
     * because secondary updates are part of the replication stream.  However,
     * it is fragile because other operations, when no Database handle is used,
     * will not perform secondary updates.  This isn't currently a problem
     * because a Database handle is present for all user operations.  But it is
     * fragile and needs work.
     *
     * @param putMode One of OVERWRITE, NO_OVERWITE, CURRENT. (NO_DUPS_DATA
     * has been converted to NO_OVERWRITE).  Note: OVERWRITE may perform an
     * insertion or an update, NO_OVERWRITE performs insertion only, and
     * CURRENT updates the slot where the cursor is currently positioned at.
     *
     * @param key The new key value for the BIN slot S to be inserted/updated.
     * Cannot be partial. For a no-dups DB, it is null if the putMode is
     * CURRENT. For dups DBs it is a 2-part key: if the putMode is CURRENT,
     * it combines the current primary key of slot S with the original,
     * user-provided data; for OVERWRITE and NO_OVERWRITE, it combines the
     * original, user-provided key and data. In case of update, "key" must
     * compare equal to S.key (otherwise DuplicateDataException is thrown),
     * but the 2 keys may not be identical if custom comparators are used.
     * So, S.key will actually be replaced by "key".
     *
     * @param data The new data for the LN associated with the BIN slot. For
     * dups DBs it is EMPTY_DUPS_DATA. Note: for dups DBs the original,
     * user-provided "data" must not be partial.
     *
     * @param lnForReplay LN to be inserted or updated during replica replay.
     */
    private OperationResult putNotify(
        DatabaseEntry key,
        final DatabaseEntry data,
        final LN lnForReplay,
        final WriteParams writeParams,
        final PutMode putMode) {

        final boolean hasUserTriggers = (dbImpl.getTriggers() != null);
        final boolean hasAssociations = (dbHandle != null) &&
            dbHandle.hasSecondaryOrForeignKeyAssociations();
        
        if (hasAssociations) {
            dbImpl.getEnv().acquireSecondaryAssociationsReadLock();
        }

        try {
            final OperationResult result;
            DatabaseEntry replaceKey = null;

            if (putMode == PutMode.CURRENT) {
                if (key == null) {
                    /*
                     * This is a no-dups DB. The slot key will not be affected
                     * by the update. However, if there are indexes/triggers,
                     * the value of the key is needed to update/apply the
                     * indexes/triggers after the update. So, it must be
                     * returned by the putCurrentNoNotify() call below.
                     * Furthermore, for indexes, the value of the key is needed
                     * before the update as well, to determine which indexes
                     * actually must be updated and whether the old data is
                     * also needed to do the index updates. So, we read the
                     * value of the key here by what is effectively a
                     * dirty-read.
                     */
                    if (hasAssociations || hasUserTriggers) {
                        key = new DatabaseEntry();
                        /*
                         * Latch this.bin and make "key" point to the
                         * slot key; then unlatch this.bin. 
                         */
                        key.setData(cursorImpl.getCurrentKey());
                    }
                } else {
                    /*
                     * This is a dups DB. The slot key must be replaced by the
                     * given 2-part key. We don't need the pre-update slot key. 
                     */
                    replaceKey = key;
                }
            }

            /*
             * - oldData: if needed, will be set to the LN data before the
             *   update.
             * - newData: if needed, will be set to the full LN data after
             *   the update; may be different than newData only if newData
             *   is partial. 
             */
            DatabaseEntry oldData = null;
            DatabaseEntry newData = null;

            /*
             * Get secondaries from the association and determine whether the
             * old data and new data is needed.
             */
            Collection<SecondaryDatabase> secondaries = null;

            if (hasAssociations || hasUserTriggers) {

                if (data.getPartial()) {
                    newData = new DatabaseEntry();
                }

                if (hasUserTriggers) {
                    /*
                     * TODO: Perhaps the old data is not needed for all
                     *  triggers and this can be made optional.
                     */
                    oldData = new DatabaseEntry();
                }

                if (hasAssociations) {
                    secondaries =
                        (writeParams.getAllIndexDbNames() == null ?
                         dbHandle.secAssoc.getSecondaries(key) :
                         dbHandle.secAssoc.getSecondaries(
                             key,
                             writeParams.getAllIndexDbNames(),
                             writeParams.getAllIndexIds(),
                             writeParams.getIndexesToUpdate(),
                             writeParams.getExpirationUpdated(),
                             writeParams.tombstone));
                    if (oldData == null &&
                        (SecondaryDatabase.needOldDataForUpdate(secondaries)
                         || writeParams.tombstone)) {
                        oldData = new DatabaseEntry();
                    }
                }
            }

            /* Perform the actual put operation. */
            if (putMode == PutMode.CURRENT) {

                result = putCurrentNoNotify(
                    replaceKey, data, oldData, newData, writeParams);

            } else {

                result = putNoNotify(
                    key, data, lnForReplay, writeParams, putMode, oldData);
            }

            if (result == null) {
                return null;
            }

            /*
             * If returned data is null, then
             * 1. this is an insertion not an update, or
             * 2. an expired LN was purged and the data could not be fetched.
             *
             * The latter case is acceptable because the old data is needed
             * only to delete secondary records, and if the LN expired then
             * those secondary records will also expire naturally. The old
             * expirationTime is passed to updateSecondary below, which will
             * prevent secondary integrity errors.
             */
            if (oldData != null && oldData.getData() == null) {
                oldData = null;
            }

            if (newData == null) {
                newData = data;
            }

            /*
             * Update secondaries and notify triggers.  Pass newData, not data,
             * since data may be partial.
             */
            final Locker locker = cursorImpl.getLocker();

            if (secondaries != null) {
                int nWrites = 0;

                for (final SecondaryDatabase secDb : secondaries) {

                    /* If updateMayChangeSecondary is false, and expiration is
                     * updated by an update operation, this immutable secondary
                     * index needs to update expiration. Hence the condition on
                     * writeParams.getExpirationUpdated(). Result's expiration
                     * time is tested to make sure that update operation on
                     * primary has updated expiration time.
                     * Immutable secondary indexes must also be updated if a
                     * tombstone is inserted.
                     * Note that the modification time and storage size are
                     * passed to the updateSecondary() method, but they are
                     * not actually stored in the secondary db; they are only
                     * needed for index key extraction in case the index is on
                     * the modification time or the starage size of the row. */
                    if (!result.isUpdate() ||
                        secDb.updateMayChangeSecondary() ||
                        (writeParams.getExpirationUpdated()) ||
                        writeParams.tombstone) {

                        nWrites += secDb.updateSecondary(
                            locker, null, dbImpl, cursorImpl, key,
                            oldData, newData, writeParams.cacheMode,
                            result.getModificationTime(),
                            result.getOldModificationTime(),
                            result.getExpirationTime(),
                            writeParams.getExpirationUpdated(),
                            writeParams.getOldExpirationTime(),
                            result.getStorageSize(),
                            result.getOldStorageSize(),
                            writeParams.getOldTombstone(),
                            writeParams.tombstone);
                    }
                }

                cursorImpl.setNSecondaryWrites(nWrites);
            }

            if (hasUserTriggers) {
                TriggerManager.runPutTriggers(
                    locker, dbImpl, key, oldData, newData);
            }

            return result;

        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        } finally {
            if (hasAssociations) {
                dbImpl.getEnv().releaseSecondaryAssociationsReadLock();
            }
        }
    }

    /**
     * Search for the key and perform insertion or update. Does not notify
     * triggers or perform secondary updates.
     *
     * @param putMode is either OVERWRITE, NO_OEVERWRITE, or BLIND_INSERTION
     *
     * @param key The new key value for the BIN slot S to be inserted/updated.
     * Cannot be partial. For dups DBs it is a 2-part key combining the
     * original, user-provided key and data. In case of update, "key" must
     * compare equal to S.key (otherwise DuplicateDataException is thrown),
     * but the 2 keys may not be identical if custom comparators are used.
     * So, S.key will actually be replaced by "key".
     *
     * @param data In case of update, the new data to (perhaps partially)
     * replace the data of the LN associated with the BIN slot. For dups DBs
     * it is EMPTY_DUPS_DATA. Note: for dups DBs the original, user-provided
     * "data" must not be partial.
     *
     * @param lnForReplay LN to be inserted or updated during replica replay.
     *
     * @param returnOldData To receive, in case of update, the old LN data
     * (before the update). It is needed only by DBs with indexes/triggers;
     * will be null otherwise.

     * @return OperationResult where isUpdate() distinguishes insertions and
     * updates.
     */
    private OperationResult putNoNotify(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LN lnForReplay,
        final WriteParams writeParams,
        final PutMode putMode,
        final DatabaseEntry returnOldData) {
        
        assert key != null;
        assert lnForReplay != null || data != null;
        assert putMode != null;
        assert putMode != PutMode.CURRENT;

        OperationResult result = null;
        boolean beginMove = false;
        boolean success = false;

        try {
            final EnvironmentImpl envImpl = dbImpl.getEnv();
            beginMoveCursor(false /*samePosition*/, writeParams.cacheMode);
            beginMove = true;

            /* Perform operation. */
            result = cursorImpl.insertOrUpdateRecord(
                key, data, lnForReplay, writeParams, putMode, returnOldData);

            if (result != null && result.getBeforeImageDBEntry() != null) {
                envImpl.getBeforeImageIndex()
                    .put(result.getBeforeImageDBEntry());
            }

            if (includeInOpStats) {
                if (result == null) {
                    if (putMode == PutMode.NO_OVERWRITE) {
                        envImpl.incInsertFailOps(dbImpl);
                    }
                } else {
                    if (!result.isUpdate()) {
                        envImpl.incInsertOps(dbImpl);
                    } else {
                        envImpl.incUpdateOps(dbImpl);
                    }
                }
            }

            /* Note that status is used in the finally. */
            success = true;
            return result;

        } finally {

            try {
                if (beginMove) {
                    endMoveCursor(result != null);
                }
            } catch (Exception e) {
                if (success) {
                    throw e;
                } else {
                    /*
                     * Log the exception thrown by the cleanup actions and
                     * allow the original exception to be thrown
                     */
                    LoggerUtils.traceAndLogException(
                        dbImpl.getEnv(), "Cursor", "putNoNotify", "", e);
                }
            }
        }
    }

    /**
     * Update the data at the current position. Does not interpret duplicates.
     *
     * @param key The new key value for the BIN slot S to be updated. Cannot
     * be partial. For a no-dups DB, it is null. For dups DBs it is a 2-part
     * key combining the current primary key of slot S with the original,
     * user-provided data. "key" (if not null) must compare equal to S.key
     * (otherwise DuplicateDataException is thrown), but the 2 keys may not
     * be identical if custom comparators are used. So, S.key will actually
     * be replaced by "key".
     *
     * @param data The new data to (perhaps partially) replace the data of the
     * LN associated with the BIN slot. For dups DBs it is EMPTY_DUPS_DATA.
     * Note: for dups DBs the original, user-provided "data" must not be
     * partial.
     *
     * @param returnOldData To receive the old LN data (before the update).
     * It is needed only by DBs with indexes/triggers; will be null otherwise.
     *
     * @param returnNewData To receive the full data of the updated LN.
     * It is needed only by DBs with indexes/triggers and only if "data" is
     * partial; will be null otherwise. Note: "returnNewData" may be different
     * than "data" only if "data" is partial.
     */
    private OperationResult putCurrentNoNotify(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final DatabaseEntry returnOldData,
        final DatabaseEntry returnNewData,
        final WriteParams writeParams) {
        
        assert data != null;

        beginUseCurrentPosition(writeParams.cacheMode);

        final OperationResult result = cursorImpl.updateCurrentRecord(
            key, data, writeParams, returnOldData, returnNewData);

        if (result != null && includeInOpStats) {
            dbImpl.getEnv().incUpdateOps(dbImpl);
        }

        endUseCurrentPosition();
        return result;
    }

    /**
     * Returns the current key and data.
     */
    OperationResult getCurrentInternal(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {
        
        synchronized (getTxnSynchronizer()) {

            checkTxnState();

            if (dbImpl.getSortedDuplicates()) {
                return getCurrentHandleDups(
                    key, data, lockMode, excludeTombstones, cacheMode);
            }

            return getCurrentNoDups(
                key, data, lockMode, excludeTombstones, cacheMode);
        }
    }

    /**
     * Used to lock without returning key/data.  When called with
     * LockMode.READ_UNCOMMITTED, it simply checks for a deleted record.
     */
    OperationResult checkCurrent(final LockMode lockMode,
                                 final boolean excludeTombstones,
                                 final CacheMode cacheMode) {
        return getCurrentNoDups(
            null, null, lockMode, excludeTombstones, cacheMode);
    }

    /**
     * Interpret duplicates for getCurrent operation.
     */
    private OperationResult getCurrentHandleDups(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {

        final DatabaseEntry twoPartKey = new DatabaseEntry();

        final OperationResult result = getCurrentNoDups(
            twoPartKey, NO_RETURN_DATA, lockMode, excludeTombstones,
            cacheMode);

        if (result == null) {
            return null;
        }

        DupKeyData.split(twoPartKey, key, data);
        return result;
    }

    private OperationResult getCurrentNoDups(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {

        boolean success = false;

        beginUseCurrentPosition(cacheMode);

        final LockType lockType = getLockType(lockMode);

        try {
            final OperationResult result = cursorImpl.lockAndGetCurrent(
                key, data, lockType, excludeTombstones,
                lockMode == LockMode.READ_UNCOMMITTED_ALL,
                false /*isLatched*/, false /*unlatch*/);

            success = true;
            return result;

        } finally {

            if (success &&
                !dbImpl.isInternalDb() &&
                cursorImpl.getBIN() != null &&
                cursorImpl.getBIN().isBINDelta()) {
                dbImpl.getEnv().incBinDeltaGets();
            }

            cursorImpl.releaseBIN();
            endUseCurrentPosition();
        }
    }

    /**
     * Internal version of getFirst/getLast that does no parameter checking.
     * Interprets duplicates.
     */
    OperationResult position(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final boolean first) {
        
        synchronized (getTxnSynchronizer()) {

            checkTxnState();

            final OperationResult result;

            if (dbImpl.getSortedDuplicates()) {
                result = positionHandleDups(
                    key, data, lockMode, excludeTombstones, cacheMode, first);
            } else {
                result = positionNoDups(
                    key, data, lockMode, excludeTombstones, cacheMode, first);
            }

            if (result != null && includeInOpStats) {
                dbImpl.getEnv().incPositionOps(dbImpl);
            }

            return result;
        }
    }

    /**
     * Interpret duplicates for getFirst and getLast operations.
     */
    private OperationResult positionHandleDups(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final boolean first) {
        
        final DatabaseEntry twoPartKey = new DatabaseEntry();

        final OperationResult result = positionNoDups(
            twoPartKey, NO_RETURN_DATA, lockMode, excludeTombstones,
            cacheMode, first);

        if (result == null) {
            return null;
        }

        DupKeyData.split(twoPartKey, key, data);
        return result;
    }

    /**
     * Does not interpret duplicates.
     */
    private OperationResult positionNoDups(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final boolean first) {

        assert (key != null && data != null);

        OperationResult result = null;

        beginMoveCursor(false /*samePosition*/, cacheMode);

        try {
            /* Search for first or last slot. */
            if (!cursorImpl.positionFirstOrLast(first)) {
                /* Tree is empty. */
                if (LatchSupport.TRACK_LATCHES) {
                    LatchSupport.expectBtreeLatchesHeld(0);
                }
            } else {
                /*
                 * Found and latched first/last BIN in this tree.
                 * BIN may be empty.
                 */
                if (LatchSupport.TRACK_LATCHES) {
                    LatchSupport.expectBtreeLatchesHeld(1);
                }

                final LockType lockType = getLockType(lockMode);

                final boolean dirtyReadAll =
                    lockMode == LockMode.READ_UNCOMMITTED_ALL;

                result = cursorImpl.lockAndGetCurrent(
                    key, data, lockType, excludeTombstones, dirtyReadAll,
                    true /*isLatched*/, false /*unlatch*/);

                if (result == null) {
                    /*
                     * The BIN may be empty or the slot we're pointing at may
                     * be deleted.
                     */
                    result = cursorImpl.getNext(
                        key, data, lockType, excludeTombstones, dirtyReadAll,
                        first, true /*isLatched*/, null /*rangeConstraint*/);
                }
            }
        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        } finally {
            cursorImpl.releaseBIN();
            endMoveCursor(result != null);
        }
        return result;
    }

    /**
     * Retrieves the next or previous record.
     */
    OperationResult retrieveNext(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final GetMode getMode) {

        synchronized (getTxnSynchronizer()) {
            final OperationResult result;

            if (dbImpl.getSortedDuplicates()) {
                result = retrieveNextHandleDups(
                    key, data, lockMode, excludeTombstones, cacheMode,
                    getMode);
            } else {
                result = retrieveNextNoDups(
                    key, data, lockMode, excludeTombstones, cacheMode,
                    getMode);
            }

            if (result != null && includeInOpStats) {
                dbImpl.getEnv().incPositionOps(dbImpl);
            }

            return result;
        }
    }

    /**
     * Interpret duplicates for getNext/Prev/etc operations.
     */
    private OperationResult retrieveNextHandleDups(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final GetMode getMode) {
        
        switch (getMode) {
        case NEXT:
        case PREV:
            return dupsGetNextOrPrev(
                key, data, lockMode, excludeTombstones, cacheMode, getMode);
        case NEXT_DUP:
            return dupsGetNextOrPrevDup(
                key, data, lockMode, excludeTombstones, cacheMode,
                GetMode.NEXT);
        case PREV_DUP:
            return dupsGetNextOrPrevDup(
                key, data, lockMode, excludeTombstones, cacheMode,
                GetMode.PREV);
        case NEXT_NODUP:
            return dupsGetNextNoDup(
                key, data, lockMode, excludeTombstones, cacheMode);
        case PREV_NODUP:
            return dupsGetPrevNoDup(
                key, data, lockMode, excludeTombstones, cacheMode);
        default:
            throw EnvironmentFailureException.unexpectedState(
                getMode.toString());
        }
    }

    /**
     * Interpret duplicates for getNext and getPrev.
     */
    private OperationResult dupsGetNextOrPrev(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final GetMode getMode) {
        
        final DatabaseEntry twoPartKey = new DatabaseEntry();

        final OperationResult result = retrieveNextNoDups(
            twoPartKey, NO_RETURN_DATA, lockMode, excludeTombstones,
            cacheMode, getMode);

        if (result == null) {
            return null;
        }
        DupKeyData.split(twoPartKey, key, data);
        return result;
    }

    /**
     * Interpret duplicates for getNextDup and getPrevDup.
     *
     * Move the cursor forward or backward by one record, and check the key
     * prefix to detect going out of the bounds of the duplicate set.
     */
    private OperationResult dupsGetNextOrPrevDup(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final GetMode getMode) {
        
        final byte[] currentKey = cursorImpl.getCurrentKey();

        try (final Cursor c = dup(true /*samePosition*/)) {
            setPrefixConstraint(c, currentKey);
            final DatabaseEntry twoPartKey = new DatabaseEntry();

            final OperationResult result = c.retrieveNextNoDups(
                twoPartKey, NO_RETURN_DATA, lockMode, excludeTombstones,
                cacheMode, getMode);

            if (result == null) {
                return null;
            }
            DupKeyData.split(twoPartKey, key, data);
            swapCursor(c);
            return result;
        }
    }

    /**
     * Interpret duplicates for getNextNoDup.
     *
     * Using a special comparator, search for first duplicate in the duplicate
     * set following the one for the current key.  For details see
     * DupKeyData.NextNoDupComparator.
     */
    private OperationResult dupsGetNextNoDup(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {

        final byte[] currentKey = cursorImpl.getCurrentKey();
        final DatabaseEntry twoPartKey = DupKeyData.removeData(currentKey);

        try (final Cursor c = dup(false /*samePosition*/)) {

            final OperationResult result = c.searchNoDups(
                twoPartKey, NO_RETURN_DATA, null, lockMode, excludeTombstones,
                cacheMode, SearchMode.SET_RANGE, getNextNoDupComparator());

            if (result == null) {
                return null;
            }

            DupKeyData.split(twoPartKey, key, data);

            swapCursor(c);
            return result;
        }
    }

    /**
     * Used to perform the getNextNoDup operation.
     *
     * Compares the left parameter (the key parameter in a user-initiated
     * search operation) as:
     *  - less than a right operand with a prefix which is less than the
     *    prefix of the left operand.  This is standard.
     *  - greater than a right operand with a prefix which is greater than the
     *    prefix of the left operand.  This is standard.
     *  - greater than a right operand with a prefix equal to the prefix of
     *    the left operation.  This is non-standard.
     *
     * The last property causes the range search to find the first duplicate in
     * the duplicate set following the duplicate set of the left operand.
     */
    private InternalComparator getNextNoDupComparator() {

        return (key1, key1Offset, key1length,
                key2Prefix, key2PrefixOffset, key2PrefixLength,
                key2Suffix, key2SuffixOffset, key2SuffixLength) -> {

            final int cmp = dbImpl.getMainKeyComparator().compare(
                key1, key1Offset, key1length,
                key2Prefix, key2PrefixOffset, key2PrefixLength,
                key2Suffix, key2SuffixOffset, key2SuffixLength);

            return (cmp != 0) ? cmp : 1;
        };
    }

    /**
     * Interpret duplicates for getPrevNoDup.
     *
     * Move the cursor to the first duplicate in the duplicate set, then to the
     * previous record. If this fails because all dups at the current position
     * have been deleted, move the cursor backward to find the previous key.
     */
    private OperationResult dupsGetPrevNoDup(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {
        
        final byte[] currentKey = cursorImpl.getCurrentKey();
        final DatabaseEntry twoPartKey = DupKeyData.removeData(currentKey);
        Cursor c = dup(false /*samePosition*/);
        try {
            setPrefixConstraint(c, currentKey);

            OperationResult result = c.searchNoDups(
                twoPartKey, NO_RETURN_DATA, null, lockMode, excludeTombstones,
                cacheMode, SearchMode.SET_RANGE, null /*comparator*/);

            if (result != null) {
                c.rangeConstraint = null;

                result = c.retrieveNextNoDups(
                    twoPartKey, NO_RETURN_DATA, lockMode, excludeTombstones,
                    cacheMode, GetMode.PREV);

                if (result == null) {
                    return null;
                }

                DupKeyData.split(twoPartKey, key, data);
                swapCursor(c);
                return result;
            }
        } finally {
            c.close();
        }

        c = dup(true /*samePosition*/);

        try {
            while (true) {
                final OperationResult result =
                    c.retrieveNextNoDups(
                        twoPartKey, NO_RETURN_DATA, lockMode,
                        excludeTombstones, cacheMode, GetMode.PREV);

                if (result == null) {
                    return null;
                }

                if (!haveSameDupPrefix(twoPartKey, currentKey)) {
                    DupKeyData.split(twoPartKey, key, data);
                    swapCursor(c);
                    return result;
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Does not interpret duplicates.
     */
    private OperationResult retrieveNextNoDups(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final GetMode getModeParam) {

        final GetMode getMode;
        switch (getModeParam) {
        case NEXT_DUP:
        case PREV_DUP:
            return null;
        case NEXT_NODUP:
            getMode = GetMode.NEXT;
            break;
        case PREV_NODUP:
            getMode = GetMode.PREV;
            break;
        default:
            getMode = getModeParam;
        }

        try {
            assert (getMode == GetMode.NEXT || getMode == GetMode.PREV);

            beginMoveCursor(true /*samePosition*/, cacheMode);

            OperationResult result = null;
            try {
                result = cursorImpl.getNext(
                    key, data, getLockType(lockMode),
                    excludeTombstones,
                    lockMode == LockMode.READ_UNCOMMITTED_ALL,
                    getMode.isForward(), false /*isLatched*/,
                    rangeConstraint);

                return result;
            } finally {
                endMoveCursor(result != null);
            }
        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        }
    }

    private long skipInternal(
        final long maxCount,
        final boolean forward,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {

        final LockType lockType = getLockType(lockMode);

        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            while (true) {
                /*
                 * Clone the cursor since we may need to restart the operation
                 * at the previous position.
                 */
                try (final Cursor c = dup(true /*samePosition*/)) {
                    final long count = c.cursorImpl.skip(
                        forward, maxCount, null /*rangeConstraint*/);
                    if (count <= 0) {
                        return 0;
                    }
                    final OperationResult result = getCurrentWithCursorImpl(
                        c.cursorImpl, key, data, lockType, excludeTombstones);

                    if (result == null) {
                        /* Retry if deletion occurs while unlatched. */
                        continue;
                    }
                    swapCursor(c);
                    return count;
                }
            }
        }
    }

    /**
     * Convenience method that does lockAndGetCurrent, with and without dups,
     * using a CursorImpl.  Does no setup or save/restore of cursor state.
     */
    private OperationResult getCurrentWithCursorImpl(
        final CursorImpl c,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockType lockType,
        final boolean excludeTombstones) {
        
        if (!dbImpl.getSortedDuplicates()) {
            return c.lockAndGetCurrent(key, data, lockType, excludeTombstones);
        }

        final DatabaseEntry twoPartKey = new DatabaseEntry();

        final OperationResult result = c.lockAndGetCurrent(
            twoPartKey, NO_RETURN_DATA, lockType, excludeTombstones);

        if (result == null) {
            return null;
        }

        DupKeyData.split(twoPartKey, key, data);
        return result;
    }

    /**
     * Performs search by key, data, or both.
     */
    OperationResult search(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        SearchMode searchMode,
        final boolean countOpStat) {

        synchronized (getTxnSynchronizer()) {
            final OperationResult result;

            checkTxnState();

            if (dbImpl.getSortedDuplicates()) {

                switch (searchMode) {
                case SET:
                    result = dupsGetSearchKey(
                        key, data, lockMode, excludeTombstones, cacheMode);
                    break;
                case SET_RANGE:
                    result = dupsGetSearchKeyRange(
                        key, data, lockMode, excludeTombstones, cacheMode);
                    break;
                case BOTH:
                    result = dupsGetSearchBoth(
                        key, data, lockMode, excludeTombstones, cacheMode);
                    break;
                case BOTH_RANGE:
                    result = dupsGetSearchBothRange(
                        key, data, lockMode, excludeTombstones, cacheMode);
                    break;
                case ANY_RANGE:
                    result = dupsGetSearchAnyRange(
                        key, data, lockMode, excludeTombstones, cacheMode);
                    break;
                default:
                    throw EnvironmentFailureException.unexpectedState(
                        searchMode.toString());
                }
            } else {
                if (searchMode == SearchMode.BOTH_RANGE) {
                    searchMode = SearchMode.BOTH;
                } else if (searchMode == SearchMode.ANY_RANGE) {
                    searchMode = SearchMode.SET_RANGE;
                }
                result = searchNoDups(
                    key, data, null, lockMode, excludeTombstones, cacheMode,
                    searchMode, null /*comparator*/);
            }

            if (countOpStat && includeInOpStats) {
                if (result != null) {
                    dbImpl.getEnv().incSearchOps(dbImpl);
                } else {
                    dbImpl.getEnv().incSearchFailOps(dbImpl);
                }
            }

            return result;
        }
    }

    /**
     * Interpret duplicates for getSearchKey operation.
     *
     * Use key as prefix to find first duplicate using a range search.  Compare
     * result to prefix to see whether we went out of the bounds of the
     * duplicate set, i.e., whether NOTFOUND should be returned.
     *
     * Even if the user-provided "key" exists in the DB, the twoPartKey built
     * here out of "key" compares < any of the BIN-slot keys that comprise the
     * duplicates-set of "key". So there is no way to get an exact key match
     * by a BTree search. Instead, we do a constrained range search: we forbid
     * the cursor to advance past the duplicates-set of "key" by using an
     * appropriate range constraint.
     */
    private OperationResult dupsGetSearchKey(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {

        final DatabaseEntry twoPartKey = new DatabaseEntry(
            DupKeyData.makePrefixKey(key.getData(),
                                     key.getOffset(),
                                     key.getSize()));

        final RangeConstraint savedRangeConstraint = rangeConstraint;

        try {
            setPrefixConstraint(this, twoPartKey);

            final OperationResult result = searchNoDups(
                twoPartKey, NO_RETURN_DATA, null, lockMode, excludeTombstones,
                cacheMode, SearchMode.SET_RANGE, null /*comparator*/);

            if (result == null) {
                return null;
            }

            DupKeyData.split(twoPartKey, key, data);

            return result;
        } finally {
            rangeConstraint = savedRangeConstraint;
        }
    }

    /**
     * Interpret duplicates for getSearchKeyRange operation.
     *
     * Do range search for key prefix.
     */
    private OperationResult dupsGetSearchKeyRange(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {

        final DatabaseEntry twoPartKey = new DatabaseEntry(
            DupKeyData.makePrefixKey(key.getData(),
                                     key.getOffset(),
                                     key.getSize()));

        final OperationResult result = searchNoDups(
            twoPartKey, NO_RETURN_DATA, null, lockMode, excludeTombstones,
            cacheMode, SearchMode.SET_RANGE, null /*comparator*/);

        if (result == null) {
            return null;
        }

        DupKeyData.split(twoPartKey, key, data);
        return result;
    }

    /**
     * Interpret duplicates for getSearchBoth operation.
     *
     * Do exact search for combined key.
     */
    private OperationResult dupsGetSearchBoth(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {

        final DatabaseEntry twoPartKey = DupKeyData.combine(key, data);

        final OperationResult result = searchNoDups(
            twoPartKey, NO_RETURN_DATA, null, lockMode, excludeTombstones,
            cacheMode, SearchMode.BOTH, null /*comparator*/);

        if (result == null) {
            return null;
        }

        DupKeyData.split(twoPartKey, key, data);
        return result;
    }

    /**
     * Interpret duplicates for getSearchBothRange operation.
     *
     * Do range search for combined key.  Compare result to prefix to see
     * whether we went out of the bounds of the duplicate set, i.e., whether
     * null should be returned.
     */
    private OperationResult dupsGetSearchBothRange(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {

        final DatabaseEntry twoPartKey = DupKeyData.combine(key, data);

        final RangeConstraint savedRangeConstraint = rangeConstraint;

        try {
            setPrefixConstraint(this, twoPartKey);

            final OperationResult result = searchNoDups(
                twoPartKey, NO_RETURN_DATA, null, lockMode, excludeTombstones,
                cacheMode, SearchMode.SET_RANGE, null /*comparator*/);

            if (result == null) {
                return null;
            }

            DupKeyData.split(twoPartKey, key, data);

            return result;
        } finally {
            rangeConstraint = savedRangeConstraint;
        }
    }

    /**
     * Interpret duplicates for Get.SEARCH_ANY_GTE operation.
     *
     * Do range search for combined key.
     */
    private OperationResult dupsGetSearchAnyRange(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode) {

        final DatabaseEntry twoPartKey = DupKeyData.combine(key, data);

        final OperationResult result = searchNoDups(
            twoPartKey, NO_RETURN_DATA, null, lockMode, excludeTombstones,
            cacheMode, SearchMode.SET_RANGE, null /*comparator*/);

        if (result == null) {
            return null;
        }

        DupKeyData.split(twoPartKey, key, data);
        return result;
    }

    /**
     * Does not interpret duplicates.
     */
    private OperationResult searchNoDups(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final ReplayPreprocessor preprocessor,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final SearchMode searchMode,
        final InternalComparator comparator) {

        /*
         * searchMode cannot be BOTH_RANGE, because for non-dups DBs BOTH_RANGE
         * is converted to BOTH, and for dup DBs BOTH_RANGE is converted to
         * SET_RANGE.
         */
        assert(searchMode != SearchMode.BOTH_RANGE);

        /*
         * searchMode cannot be ANY_RANGE, because ANY_RANGE is always
         * converted to SET_RANGE.
         */
        assert(searchMode != SearchMode.ANY_RANGE);

        try {
            if (searchMode.isExactSearch()) {

                assert(comparator == null);

                return searchExact(
                    key, data, preprocessor, lockMode, excludeTombstones,
                    cacheMode, searchMode);
            }

            while (true) {
                try {
                    return searchRange(
                        key, data, lockMode, excludeTombstones, cacheMode,
                        comparator);
                } catch (RangeRestartException e) {
                    // continue
                }
            }
        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        }
    }

    /**
     * Search for a "valid" BIN slot whose key is equal to the given "key".
     * A slot is "valid" only if after locking it, neither its PD nor it KD
     * flags are set. If no slot exists, return NOTFOUND. Otherwise, copy
     * the key and the LN of the found slot into "key" and "data" respectively
     * (if "key"/"data" request so) and return either NOTFOUND if searchMode
     * == BOTH and "data" does not match the LN of the found slot, or SUCCESS
     * otherwise.
     *
     * Note: On return from this method no latches are held by this cursor.
     *
     * Note: If the method returns NOTFOUND or raises an exception, any non-
     * transactional locks acquired by this method are released.
     *
     * Note: On SUCCESS, any non-transactional locks held by this cursor
     * before calling this method are released.
     *
     * @param key It is used as the search key, as well as to receive the key
     * of the BIN slot found by this method, if any. If the DB contains
     * duplicates, the key is in the "two-part-key" format (see
     * dbi/DupKeyData.java) so that it can be compared with the two-part keys
     * stored in the BTree (which contain both a primary key and a data
     * portion). The search key itself may or may not contain a data portion. 
     *
     * @param data A DatabaseEntry to compare against the LN of the slot found
     * by the search (if searchMode == BOTH) as well as to receive the data of
     * that LN. If the DB contains duplicates, it is equal to NO_RETURN_DATA,
     * because the LN will be emtpy (the full record is contained in the key).
     *
     * @param searchMode Either SET or BOTH.
     *
     * @return NOTFOUND if (a) no valid slot exists with a key == the search
     * key, or (b) searchMode == BOTH and "data" does not match the LN of the
     * found slot. SUCCESS otherwise.
     */
    private OperationResult searchExact(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final ReplayPreprocessor preprocessor,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        final SearchMode searchMode) {
        
        assert(key != null && data != null);
        assert(searchMode == SearchMode.SET || searchMode == SearchMode.BOTH);

        boolean success = false;
        OperationResult result = null;

        final DatabaseEntry origData = new DatabaseEntry(
            data.getData(), data.getOffset(), data.getSize());

        final boolean dataRequested =
            !data.getPartial() || data.getPartialLength() != 0;

        final LockType lockType = getLockType(lockMode);

        final boolean dirtyReadAll =
            lockMode == LockMode.READ_UNCOMMITTED_ALL;

        beginMoveCursor(false /*samePosition*/, cacheMode);

        try {
            /*
             * Search for a BIN slot whose key is == the search key. If such a
             * slot is found, lock it and check whether it is valid.
             */
            if (cursorImpl.searchExact(
                    key, preprocessor, lockType, excludeTombstones,
                    dirtyReadAll, dataRequested) == null) {
                success = true;
                return null;
            }

            /*
             * The search found and locked a valid BIN slot whose key is
             * equal to the search key. Copy into "data" the LN of this
             * slot (if "data" requests so). Also copy into "key" the key of
             * the found slot if a partial key comparator is used, since then
             * it may be different than the given key.
             */
            result = cursorImpl.getCurrent(
                dbImpl.allowsKeyUpdates() ? key : null, data);

            /* Check for data match, if asked so. */
            if (result != null &&
                searchMode == SearchMode.BOTH &&
                !checkDataMatch(origData, data)) {
                result = null;
            }

            success = true;

        } finally {

            if (success &&
                !dbImpl.isInternalDb() &&
                cursorImpl.getBIN() != null &&
                cursorImpl.getBIN().isBINDelta()) {
                dbImpl.getEnv().incBinDeltaGets();
            }

            cursorImpl.releaseBIN();
            endMoveCursor(result != null);
        }
    
        return result;
    }

    /**
     * Search for the 1st "valid" BIN slot whose key is in the range [K1, K2),
     * where (a) K1 is a given key, (b) K2 is determined by
     * this.rangeConstraint, or is +INFINITY if this.rangeConstraint == null,
     * and (c) a slot is "valid" only if after locking it, neither its PD nor
     * its KD flags are set.
     *
     * If such a slot is found, copy its key and its associated LN into "key"
     * and "data" respectively (if "key"/"data" request so). Note that the
     * fact that the slot is valid implies that it has been locked.
     *
     * Note: On return from this method no latches are held by this cursor.
     *
     * Note: If the method returns NOTFOUND or raises an exception, any non-
     * transactional locks acquired by this method are released.
     *
     * Note: On SUCCESS, any non-transactional locks held by this cursor
     * before calling this method are released.
     *
     * @param key It is used as the search key, as well as to receive the key
     * of the BIN slot found by this method, if any. If the DB contains
     * duplicates, the key is in the "two-part-key" format (see
     * dbi/DupKeyData.java) so that it can be compared with the two-part keys
     * stored in the BTree (which contain both a primary key and a data
     * portion). The search key itself may or may not contain a data portion. 
     *
     * @param data A DatabaseEntry to receive the data of the LN associated
     * with the found slot, if any. If the DB contains duplicates, it is equal
     * to NO_RETURN_DATA, because the LN will be empty (the full record is
     * contained in the key).
     *
     * @param comparator Comparator to use to compare the search key against
     * the BTree keys.
     *
     * @return NOTFOUND if no valid slot exists in the [K1, K2) range; SUCCESS
     * otherwise.
     *
     * @throws RangeRestartException if the search should be restarted by the
     * caller.
     */
    private OperationResult searchRange(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode,
        final boolean excludeTombstones,
        final CacheMode cacheMode,
        InternalComparator comparator)
        throws RangeRestartException {

        assert(key != null && data != null);

        boolean success = false;
        boolean incStats = !dbImpl.isInternalDb();
        OperationResult result = null;

        final LockType lockType = getLockType(lockMode);

        final boolean dirtyReadAll =
            lockMode == LockMode.READ_UNCOMMITTED_ALL;

        beginMoveCursor(false /*samePosition*/, cacheMode);

        try {
            /* Search for a BIN slot whose key is the max key <= K1. */
            final int searchResult = cursorImpl.searchRange(key, comparator);

            if ((searchResult & CursorImpl.FOUND) == 0) {
                /* The tree is completely empty (has no nodes at all) */
                success = true;
                return null;
            }

            /*
             * The search positioned cursorImpl on the BIN that should contain
             * K1 and this BIN is now latched. If the BIN does contain K1,
             * cursorImpl.index points to K1's slot. Otherwise,
             * cursorImpl.index points to the right-most slot whose key is < K1
             * (or cursorImpl.index is -1 if K1 is < than all keys in the BIN).
             * Note: if foundLast is true, cursorImpl is positioned on the very
             * last slot of the BTree.
             */
            final boolean exactKeyMatch =
                ((searchResult & CursorImpl.EXACT_KEY) != 0);
            final boolean foundLast =
                ((searchResult & CursorImpl.FOUND_LAST) != 0);

            /*
             * If we found K1, lock the slot and check whether it is valid.
             * If so, copy out its key and associated LN.
             */
            if (exactKeyMatch) {
                result = cursorImpl.lockAndGetCurrent(
                    key, data, lockType, excludeTombstones, dirtyReadAll,
                    true /*isLatched*/, false /*unlatch*/);
            }

            /*
             * If K1 is not in the BTree or its slot is not valid, advance
             * cursorImpl until (a) the rangeConstraint (if any) returns false,
             * or (b) there are no more slots, or (c) we find a valid slot. If
             * (c), check whether the slot key is < K1. This can happen if K1
             * was not in the BTree (so cursorImpl is now on a key K0 < K1) and
             * another txn inserted new keys < K1 while we were trying to
             * advance cursorImpl. If so, a RestartException is thrown.
             * Otherwise, the slot key and LN are copied into "key" and "data" (if
             * "key"/"data" request so).
             */
            if (!exactKeyMatch || result == null) {
                result = null;
                if (!foundLast) {
                    result = searchRangeAdvanceAndCheckKey(
                        key, data, lockType, excludeTombstones,
                        dirtyReadAll, comparator, rangeConstraint);

                    /*
                     * Don't inc thput stats because the bin is released by
                     * searchRangeAdvanceAndCheckKey(). This is ok because
                     * searchRangeAdvanceAndCheckKey() will cause mutation
                     * to full bin anyway.
                     */
                    incStats = false;
                }
            }

            success = true;

        } finally {

            if (success &&
                incStats &&
                cursorImpl.getBIN() != null &&
                cursorImpl.getBIN().isBINDelta()) {
                dbImpl.getEnv().incBinDeltaGets();
            }

            cursorImpl.releaseBIN();
            endMoveCursor(result != null);
        }

        return result;
    }

    @SuppressWarnings("serial")
    private static class RangeRestartException extends InternalException
        implements NotSerializable {

        public RangeRestartException() {
            super();
        }
    }

    /**
     * @throws RangeRestartException if the search should be restarted by the
     * caller.
     */
    private OperationResult searchRangeAdvanceAndCheckKey(
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockType lockType,
        final boolean excludeTombstones,
        final boolean dirtyReadAll,
        InternalComparator comparator,
        final RangeConstraint rangeConstraint)
        throws RangeRestartException {

        if (comparator == null) {
            comparator = dbImpl.getKeyComparator();
        }

        DatabaseEntry origKey = new DatabaseEntry(
            key.getData(), key.getOffset(), key.getSize());

        DatabaseEntry nextKey = key;
        if (key.getPartial()) {
            nextKey = new DatabaseEntry(
                key.getData(), key.getOffset(), key.getSize());
        }

        OperationResult result = cursorImpl.getNext(
            nextKey, data, lockType, excludeTombstones, dirtyReadAll,
            true /*forward*/, true /*isLatched*/, rangeConstraint);

        /*
         * Check whether the cursorImpl.getNext() landed on slot whose key is
         * < K1. This can happen if K1 was not in the BTree (so before
         * cursorImpl.getNext() is called, cursorImpl is on a key K0 < K1) and
         * another txn inserted new keys < K1 while we were trying to advance
         * cursorImpl. Such an insertion is possible because if cursorImpl must
         * move to the next BIN, it releases all latches for a while, so the
         * inserter can come in, split the current BIN and insert its keys on
         * the right split-sibling. Finally, cursorImpl moves to the right
         * split-sibling and lands on a wrong slot.
         */
        if (result != null) {
            int c = comparator.compare(nextKey, origKey);
            if (c < 0) {
                key.setData(origKey.getData(),
                            origKey.getOffset(),
                            origKey.getSize());

                throw new RangeRestartException();

            } else if (key.getPartial()) {
                LN.setEntry(key, nextKey);
            }
        }

        return result;
    }

    /**
     * For a non-duplicates database, the data must match exactly when
     * getSearchBoth or getSearchBothRange is called.
     */
    private boolean checkDataMatch(
        DatabaseEntry data1,
        DatabaseEntry data2) {

        final int size1 = data1.getSize();
        final int size2 = data2.getSize();
        if (size1 != size2) {
            return false;
        }
        return Key.compareUnsignedBytes(
            data1.getData(), data1.getOffset(), size1,
            data2.getData(), data2.getOffset(), size2) == 0;
    }

    /**
     * Counts duplicates without parameter checking.
     */
    int countInternal(final boolean excludeTombstones) {
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            if (dbImpl.getSortedDuplicates()) {
                return countHandleDups(excludeTombstones);
            }
            return countNoDups(excludeTombstones);
        }
    }

    /**
     * Count duplicates by skipping over the entries in the dup set key range.
     */
    private int countHandleDups(final boolean excludeTombstones) {
        final byte[] currentKey = cursorImpl.getCurrentKey();
        final DatabaseEntry twoPartKey = DupKeyData.removeData(currentKey);

        try (final Cursor c = dup(false /*samePosition*/)) {
            setPrefixConstraint(c, currentKey);

            /* Move cursor to first key in this dup set. */
            OperationResult result = c.searchNoDups(
                twoPartKey, NO_RETURN_DATA, null, LockMode.READ_UNCOMMITTED,
                excludeTombstones, CacheMode.UNCHANGED, SearchMode.SET_RANGE,
                null /*comparator*/);

            if (result == null) {
                return 0;
            }

            /* Skip over entries in the dup set. */
            long count = 1 + c.cursorImpl.skip(
                true /*forward*/, 0 /*maxCount*/, c.rangeConstraint);

            if (count > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                    "count exceeded integer size: " + count);
            }

            return (int) count;
        }
    }

    /**
     * When there are no duplicates, the count is either 0 or 1, and is very
     * cheap to determine.
     */
    private int countNoDups(final boolean excludeTombstones) {
        try {
            beginUseCurrentPosition(CacheMode.UNCHANGED);

            final OperationResult result = cursorImpl.lockAndGetCurrent(
                null /*foundKey*/, null /*foundData*/, LockType.NONE,
                excludeTombstones);

            endUseCurrentPosition();

            return (result != null) ? 1 : 0;
        } catch (Error E) {
            dbImpl.getEnv().invalidate(E);
            throw E;
        }
    }

    /**
     * Estimates duplicate count without parameter checking.
     */
    long countEstimateInternal(final boolean excludeTombstones) {
        if (dbImpl.getSortedDuplicates()) {
            return countEstimateHandleDups(excludeTombstones);
        }
        return countNoDups(excludeTombstones);
    }

    /**
     * Estimate duplicate count using the end point positions.
     */
    private long countEstimateHandleDups(final boolean excludeTombstones) {
        final byte[] currentKey = cursorImpl.getCurrentKey();
        final DatabaseEntry twoPartKey = DupKeyData.removeData(currentKey);

        try (final Cursor c1 = dup(false /*samePosition*/)) {
            setPrefixConstraint(c1, currentKey);

            /* Move cursor 1 to first key in this dup set. */
            OperationResult result = c1.searchNoDups(
                twoPartKey, NO_RETURN_DATA, null, LockMode.READ_UNCOMMITTED,
                excludeTombstones, CacheMode.UNCHANGED, SearchMode.SET_RANGE,
                null /*comparator*/);

            if (result == null) {
                return 0;
            }

            /* Move cursor 2 to first key in the following dup set. */
            try (Cursor c2 = c1.dup(true /*samePosition*/)) {

                result = c2.dupsGetNextNoDup(
                    twoPartKey, NO_RETURN_DATA, LockMode.READ_UNCOMMITTED,
                    excludeTombstones, CacheMode.UNCHANGED);

                final boolean c2Inclusive;
                if (result != null) {
                    c2Inclusive = false;
                } else {
                    c2Inclusive = true;

                    /*
                     * There is no following dup set.  Go to the last record in
                     * the database.  If we land on a newly inserted dup set,
                     * go to the prev record until we find the last record in
                     * the original dup set.
                     */
                    result = c2.positionNoDups(
                        twoPartKey, NO_RETURN_DATA, LockMode.READ_UNCOMMITTED,
                        excludeTombstones, CacheMode.UNCHANGED,
                        false /*first */);

                    if (result == null) {
                        return 0;
                    }

                    while (!haveSameDupPrefix(twoPartKey, currentKey)) {
                        result = c2.retrieveNextNoDups(
                            twoPartKey, NO_RETURN_DATA,
                            LockMode.READ_UNCOMMITTED, excludeTombstones,
                            CacheMode.UNCHANGED, GetMode.PREV);

                        if (result == null) {
                            return 0;
                        }
                    }
                }

                /* Estimate the count between the two cursor positions. */
                return CountEstimator.count(
                    dbImpl, c1.cursorImpl, true, c2.cursorImpl, c2Inclusive);

            }
        }
    }

    /**
     * Reads the primary data for a primary key that was retrieved from a
     * secondary DB via this secondary cursor ("this" may also be a regular
     * Cursor in the role of a secondary cursor).  This method is in the
     * Cursor class, rather than in SecondaryCursor, to support joins with
     * plain Cursors [#21258].
     * <p>
     * When a non-null status is returned by this method, the caller should
     * return a successful result. When null is returned, the caller should
     * treat this as a deleted record and either skip the record (in the case
     * of position, search, and retrieveNext) or return failure/null (in the
     * case of getCurrent). Null can be returned only when read-uncommitted
     * is used or the primary record has expired.
     * <p>
     * If no data is requested
     *   (data.getPartial() && data.getPartialLength() == 0)
     * then in most cases it doesn't make sense to call this method because
     * a lock on the secondary has been acquired (if the caller did not
     * specify dirty-read). However, for btree verification we need to check
     * whether the primary record exists without requesting the data.
     * Therefore if no data is requested then verifyPrimary must be true.
     *
     * @param priDb primary database as input.
     *
     * @param key secondary key as input.
     *
     * @param pKey key as input.
     *
     * @param data the data returned as output.
     *
     * @param lockMode the lock mode to use for the primary read; if null, use
     * the default lock mode.
     *
     * @param secDirtyRead whether we used dirty-read for reading the secondary
     * record.  It is true if the user's configured isolation mode (or lockMode
     * param) is dirty-read, or we used dirty-read for the secondary read to
     * avoid deadlocks (this is done when the user's isolation mode is
     * READ_COMMITTED or REPEATABLE_READ).
     *
     * @param lockPrimaryOnly If false, then we are not using dirty-read for
     * secondary deadlock avoidance.  If true, this secondary cursor's
     * reference to the primary will be checked after the primary record has
     * been locked.
     *
     * @param verifyPrimary If true, we are only checking integrity. We read
     * the primary even though the data is not requested, and do not
     * invalidate the secondary DB when throwing SecondaryIntegrityException.
     *
     * @param locker is the Locker to use for accessing the primary record.
     *
     * @param secDb is the Database handle of the secondary database. Note
     * that the dbHandle field may be null and should not be used by this
     * method.
     * 
     * @param secAssoc is the SecondaryAssociation of the secondary database.
     * It is used to check whether the secondary database is still in the
     * SecondaryAssociation before throwing SecondaryIntegrityException. If
     * not, we will not throw SecondaryIntegrityException.
     *
     * @return the result of a successful primary read, or null in one of
     * the following cases:
     *  + When using read-uncommitted and the primary has been deleted.
     *  + When using read-uncommitted and the primary has been updated and no
     *    longer contains the secondary key.
     *  + When the primary record has expired (whether or not read-uncommitted
     *    is used).
     *
     * @throws SecondaryIntegrityException to indicate a corrupt secondary
     * reference if the primary record is deleted (as opposed to expired) and
     * read-uncommitted is not used.
     */
    OperationResult readPrimaryAfterGet(
        final Database priDb,
        final DatabaseEntry key,
        final DatabaseEntry pKey,
        DatabaseEntry data,
        final LockMode lockMode,
        final boolean secDirtyRead,
        final boolean lockPrimaryOnly,
        final boolean verifyPrimary,
        final Locker locker,
        final Database secDb,
        final SecondaryAssociation secAssoc) {

        final boolean priDirtyRead = isReadUncommittedMode(lockMode);
        final DatabaseImpl priDbImpl = priDb.getDbImpl();

        /*
         * If we only lock the primary (and check the sec cursor), we must be
         * using sec dirty-read for deadlock avoidance (whether or not the user
         * requested dirty-read). Otherwise, we should be using sec dirty-read
         * iff the user requested it.
         */
        if (lockPrimaryOnly) {
            assert secDirtyRead && !priDirtyRead;
        } else {
            assert secDirtyRead == priDirtyRead;
        }

        final boolean dataRequested =
            !data.getPartial() || data.getPartialLength() > 0;

        assert dataRequested || verifyPrimary;

        /*
         * If partial data is requested along with read-uncommitted, then we
         * must read all data in order to call the key creator below. [#14966]
         */
        DatabaseEntry copyToPartialEntry = null;

        if (priDirtyRead && data.getPartial()) {
            copyToPartialEntry = data;
            data = new DatabaseEntry();
        }

        /*
         * Do not release non-transactional locks when reading the primary
         * cursor.  They are held until all locks for this operation are
         * released by the secondary cursor [#15573].
         */
        final CursorImpl priCursor = new CursorImpl(
            priDbImpl, locker, true /*retainNonTxnLocks*/,
            false /*isSecondaryCursor*/);

        try {
            final LockType priLockType = getLockType(lockMode);

            final boolean dirtyReadAll =
                lockMode == LockMode.READ_UNCOMMITTED_ALL;

            /* Exclude tombstones to account for updates and dirty-read. */
            LockStanding priLockStanding = priCursor.searchExact(
                pKey, null, priLockType, true /*excludeTombstones*/,
                dirtyReadAll, dataRequested);

            OperationResult result = null;
            try {
                if (priLockStanding != null) {
                    result = priCursor.getCurrent(null, data);
                    if (result == null) {
                        priCursor.revertLock(priLockStanding);
                        priLockStanding = null;
                    }
                }
            } finally {
                priCursor.releaseBIN();
            }

            if (priLockStanding != null && lockPrimaryOnly) {
                if (!ensureReferenceToPrimary(pKey, priLockType)) {
                    priCursor.revertLock(priLockStanding);
                    priLockStanding = null;
                }
            }

            if (priLockStanding == null) {
                /*
                 * If using read-uncommitted and the primary is deleted, the
                 * primary must have been deleted after reading the secondary.
                 * We cannot verify this by checking if the secondary is
                 * deleted, because it may have been reinserted.  [#22603]
                 *
                 * If the secondary is expired (within TTL clock tolerance),
                 * then the record must have expired after the secondary read.
                 *
                 * In either case, return false to skip this record.
                 */
                if (secDirtyRead || cursorImpl.isProbablyExpired()) {
                    return null;
                }

                /*
                 * TODO: whether we need to do the following check for all
                 *       usage scenarios of readPrimaryAfterGet. If true, we
                 *       may get the SecondaryAssociation by the secDb.
                 *
                 * If secDb has been removed from SecondaryAssociation, the
                 * operations on the primary database after removing it
                 * may cause an inconsistency between the secondary record and
                 * the corresponding primary record. For this case, just return
                 * false to skip this record.
                 */
                if (secAssoc != null) {
                    boolean stillExist = false;
                    for (SecondaryDatabase db : secAssoc.getSecondaries(pKey)) {
                        if (db == secDb) {
                            stillExist = true;
                            break;
                        }
                    }
                    if (!stillExist) {
                        return null;
                    }
                }

                /*
                 * When the primary is deleted, secondary keys are deleted
                 * first.  So if the above check fails, we know the secondary
                 * reference is corrupt.
                 */
                throw secDb.secondaryRefersToMissingPrimaryKey(
                    !verifyPrimary, locker, priDb, key, pKey,
                    cursorImpl.getExpirationTime());
            }

            /*
             * If using read-uncommitted and the primary was found, check to
             * see if primary was updated so that it no longer contains the
             * secondary key. If it has been, return null to skip the record.
             *
             * checkForPrimaryUpdate is not called for tombstones (excluded
             * above). We cannot pass a tombstone to a secondary key creator.
             */
            if (priDirtyRead &&
                checkForPrimaryUpdate(key, pKey, data,
                                      result.getModificationTime(),
                                      result.getExpirationTime(),
                                      cursorImpl.getStorageSize())) {
                return null;
            }

            /*
             * When a partial entry was requested but we read all the data,
             * copy the requested partial data to the caller's entry. [#14966]
             */
            if (copyToPartialEntry != null) {
                LN.setEntry(copyToPartialEntry, data.getData());
            }

            /* Copy primary cursor info/associations to secondary cursor. */
            cursorImpl.setPriInfo(priCursor);
            locker.transferCursorLocks(priCursor, cursorImpl);

            if (includeInOpStats) {
                priDbImpl.getEnv().incSearchOps(priDbImpl);
            }

            return result;
        } finally {
            priCursor.close();
        }
    }

    /**
     * Checks whether this secondary cursor still refers to the primary key,
     * and locks the secondary record if necessary.
     *
     * This is used for deadlock avoidance with secondary DBs.  The initial
     * secondary index read is done without locking.  After the primary has
     * been locked, we check here to insure that the primary/secondary
     * relationship is still in place. There are two cases:
     *
     * 1. If the secondary DB has duplicates, the key contains the sec/pri
     *    relationship and the presence of the secondary record (that is not
     *    deleted) is sufficient to insure the sec/pri relationship.
     *
     * 2. If the secondary DB does not allow duplicates, then the primary key
     *    (the data of the secondary record) must additionally be compared to
     *    the original search key. This detects the case where the secondary
     *    record was updated to refer to a different primary key.
     *
     * In addition, this method locks the secondary record if it would expire
     * within {@link EnvironmentParams#ENV_TTL_MAX_TXN_TIME}. This is needed to
     * support repeatable-read. The lock prevents expiration of the secondary.
     */
    private boolean ensureReferenceToPrimary(
        final DatabaseEntry matchPriKey,
        final LockType lockType) {

        assert lockType != LockType.NONE;

        /*
         * To check whether the reference is still valid, because the primary
         * is locked and the secondary can only be deleted after locking the
         * primary, it is sufficient to check whether the secondary PD and KD
         * flags are set. There is no need to lock the secondary, because it is
         * protected from changes by the lock on the primary.
         */
        cursorImpl.latchBIN();
        try {
            final BIN bin = cursorImpl.getBIN();
            final int index = cursorImpl.getIndex();

            if (bin.isDeleted(index)) {
                return false;
            }

            final EnvironmentImpl envImpl = dbImpl.getEnv();

            /* Additionally, lock the secondary if it expires soon. */
            final long expirationTime = TTL.expirationToSystemTime(
                bin.getExpiration(index), bin.isExpirationInHours());

            if (envImpl.expiresWithin(
                expirationTime, envImpl.getTtlMaxTxnTime())) {
                cursorImpl.lockLN(
                    lockType, false /*excludeTombstones*/,
                    false /*allowUncontended*/, false /*noWait*/);
            }
        } finally {
            cursorImpl.releaseBIN();
        }

        /*
         * If there are no duplicates, check the secondary data (primary key).
         * No need to actually lock (use LockType.NONE) since the primary lock
         * protects the secondary from changes.
         */
        if (!cursorImpl.hasDuplicates()) {
            final DatabaseEntry secData = new DatabaseEntry();

            if (cursorImpl.lockAndGetCurrent(
                null, secData, LockType.NONE,
                false /*excludeTombstones*/) == null) {
                return false;
            }

            if (!secData.equals(matchPriKey)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks for a secondary corruption caused by a primary record update
     * during a read-uncommitted read.  Checking in this method is not possible
     * because there is no secondary key creator available.  It is overridden
     * by SecondaryCursor.
     *
     * This method is in the Cursor class, rather than in SecondaryCursor, to
     * support joins with plain Cursors [#21258].
     */
    boolean checkForPrimaryUpdate(
        final DatabaseEntry key,
        final DatabaseEntry pKey,
        final DatabaseEntry data,
        final long modificationTime,
        final long expirationTime,
        final int storageSize) {
        return false;
    }

    /**
     * Returns whether the two keys have the same prefix.
     *
     * @param twoPartKey1 combined key.
     *
     * @param keyBytes2 combined key byte array.
     */
    private boolean haveSameDupPrefix(
        final DatabaseEntry twoPartKey1,
        final byte[] keyBytes2) {

        return dbImpl.getMainKeyComparator().compare(
            twoPartKey1.getData(),
            twoPartKey1.getOffset(),
            twoPartKey1.getSize(),
            null, 0, 0,
            keyBytes2, 0, keyBytes2.length) == 0;
    }

    /**
     * Called to start an operation that potentially moves the cursor.
     *
     * If the cursor is not initialized already, the method simply returns.
     *
     * If the cursor is initialized, the actions taken here are:
     * - If same position is true, (1) the current LN (if any) is evicted, if
     *   the cachemode so dictates, and (2) non-txn locks are released, if
     *   retainNonTxnLocks is false. this.cursorImpl remains registered at its
     *   current BIN.
     * - If same position is false, this.cursorImpl is "reset", i.e., (1) it is
     *   deregistered from its current position, (2) cachemode eviction is
     *   performed, (3) non-txn locks are released, if retainNonTxnLocks is
     *   false, and (4) this.cursorImpl is marked uninitialized.
     * - this.cursorImpl is returned.
     *
     * Note: In cases where only non-transactional locks are held, releasing
     * them before the move prevents more than one lock from being held during
     * a cursor move, which helps to avoid deadlocks.
     *
     * Critical eviction is performed, if necessary, before the method
     * returns. This is done by CursorImpl.beforeMove()/reset(), or is
     * done here explicitly when the cursor is not cloned or reset.
     *
     * @param samePosition If true, this cursor's position is unchanged.
     * If false, the cursor will be is reset.
     *
     * @see CursorImpl performCacheModeEviction for a description of how the
     * cache mode is used.  This method ensures that the correct cache mode
     * is used before each operation.
     */
    private void beginMoveCursor(
        final boolean samePosition,
        final CacheMode cacheMode) {

        /* Must set cache mode before calling criticalEviction or reset. */
        cursorImpl.setCacheMode(
            cacheMode != null ? cacheMode : defaultCacheMode);

        if (cursorImpl.isNotInitialized()) {
            cursorImpl.criticalEviction();
            return;
        }

        if (samePosition) {
            cursorImpl.beforeMove();
        } else {
            cursorImpl.reset();
        }
    }

    /**
     * Called to end an operation that potentially moves the cursor.
     *
     * - If the op is successful, only critical eviction is done.
     * - If the op is not successful, this.cursorImpl is "reset", i.e.,
     *   (1) it is deregistered from its current position, (2) cachemode
     *   eviction is performed, (3) non-txn locks are released, if
     *   retainNonTxnLocks is false, and (4) this.cursorImpl is marked
     *   uninitialized.
     *
     * Critical eviction is performed after each cursor operation. This is
     * done by CursorImpl.reset() or afterMove().
     */
    private void endMoveCursor(final boolean success) {

        if (success) {
            cursorImpl.afterMove();
        } else {
            cursorImpl.reset();
        }
    }

    /**
     * Called to start an operation that does not move the cursor, and
     * therefore does not clone the cursor.  Either beginUseCurrentPosition /
     * endUseCurrentPosition or beginMoveCursor / endMoveCursor must be used
     * for each operation.
     */
    private void beginUseCurrentPosition(final CacheMode cacheMode) {
        /* Must set cache mode before calling criticalEviction. */
        cursorImpl.setCacheMode(
            cacheMode != null ? cacheMode : defaultCacheMode);
        cursorImpl.criticalEviction();
    }

    /**
     * Called to end an operation that does not move the cursor.
     */
    private void endUseCurrentPosition() {
        cursorImpl.criticalEviction();
    }

    /**
     * Swaps CursorImpl of this cursor and the other cursor given.
     */
    private void swapCursor(Cursor other) {
        final CursorImpl otherImpl = other.cursorImpl;
        other.cursorImpl = this.cursorImpl;
        this.cursorImpl = otherImpl;
    }

    private LockType getLockType(final LockMode lockMode) {

        if (isReadUncommittedMode(lockMode)) {
            return LockType.NONE;
        } else if (lockMode == null || lockMode == LockMode.DEFAULT) {
            return LockType.READ;
        } else if (lockMode == LockMode.RMW) {
            return LockType.WRITE;
        } else {
            assert false : lockMode;
            return LockType.NONE;
        }
    }

    /**
     * Returns whether the given lock mode will cause a read-uncommitted when
     * used with this cursor, taking into account the default cursor
     * configuration.
     */
    boolean isReadUncommittedMode(final LockMode lockMode) {

        return (lockMode == LockMode.READ_UNCOMMITTED ||
                lockMode == LockMode.READ_UNCOMMITTED_ALL ||
                (readUncommittedDefault &&
                 (lockMode == null || lockMode == LockMode.DEFAULT)));
    }

    private void checkUpdatesAllowed(final WriteParams writeParams) {

        checkUpdatesAllowed();

        if (dbImpl.isReplicated() &&
            writeParams != null && writeParams.expiration > 0) {

            /* Throws IllegalStateException if TTL is not available. */
            dbImpl.getEnv().checkTTLAvailable();
        }
    }

    private void checkUpdatesAllowed() {

        if (updateOperationsProhibited) {
            throw updatesProhibitedException(cursorImpl.getLocker());
        }

        if (!dbImpl.getDbType().isInternal()) {

            final String diskLimitViolation =
                dbImpl.getEnv().getDiskLimitViolation();

            if (diskLimitViolation != null) {
                throw new DiskLimitException(
                    cursorImpl.getLocker(), diskLimitViolation);
            }
        }
    }

    private UnsupportedOperationException updatesProhibitedException(
        final Locker locker) {

        final StringBuilder str = new StringBuilder(200);

        str.append("Write operation is not allowed because ");

        /* Be sure to keep this logic in sync with init(). */
        if (locker.isReadOnly()) {
            str.append("the Transaction is configured as read-only.");
        } else if (dbHandle != null && !dbHandle.isWritable()) {
            str.append("the Database is configured as read-only.");
        } else if (dbImpl.isTransactional() && !locker.isTransactional()) {
            str.append("a Transaction was not supplied to openCursor ");
            str.append("and the Database is transactional.");
        } else if (dbImpl.isReplicated() && locker.isLocalWrite() &&
                    !dbImpl.getDbType().isMixedReplication()) {
            str.append("the Database is replicated and Transaction is ");
            str.append("configured as local-write.");
        } else if (!dbImpl.isReplicated() && !locker.isLocalWrite()) {
            str.append("the Database is not replicated and the ");
            str.append("Transaction is not configured as local-write.");
        } else {
            assert false;
        }

        throw new UnsupportedOperationException(str.toString());
    }

    /**
     * Checks the cursor state.
     */
    void checkState(final boolean mustBeInitialized) {
        cursorImpl.checkCursorState(
            mustBeInitialized, false /*mustNotBeInitialized*/);
    }

    /**
     * Checks the environment, DB handle, and cursor state.
     */
    void checkOpenAndState(final boolean mustBeInitialized) {
        checkEnv();
        checkOpen();
        checkState(mustBeInitialized);
    }

    /**
     * Checks the environment and DB handle.
     */
    void checkOpen() {
        checkEnv();
        if (dbHandle != null) {
            dbHandle.checkOpen();
        }
    }

    /**
     * @throws EnvironmentFailureException if the underlying environment is
     * invalid.
     */
    void checkEnv() {
        cursorImpl.checkEnv();
    }

    /**
     * Returns an object used for synchronizing transactions that are used in
     * multiple threads.
     *
     * For a transactional locker, the Transaction is returned to prevent
     * concurrent access using this transaction from multiple threads.  The
     * Transaction.commit and abort methods are synchronized so they do not run
     * concurrently with operations using the Transaction.  Note that the Txn
     * cannot be used for synchronization because locking order is BIN first,
     * then Txn.
     *
     * For a non-transactional locker, 'this' is returned because no special
     * blocking is needed.  Other mechanisms are used to prevent
     * non-transactional usage access by multiple threads (see ThreadLocker).
     * In the future we may wish to use the getTxnSynchronizer for
     * synchronizing non-transactional access as well; however, note that a new
     * locker is created for each operation.
     */
    private Object getTxnSynchronizer() {
        return (transaction != null) ? transaction : this;
    }

    private void checkTxnState() {
        if (transaction == null) {
            return;
        }
        transaction.checkOpen();
        transaction.getTxn().checkState(false /*calledByAbort*/);
    }

    /**
     * Sends trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(
        final Level level,
        final String methodName,
        final String getOrPutType,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {

        if (logger.isLoggable(level)) {
            final StringBuilder sb = new StringBuilder();
            sb.append(methodName);
            sb.append(getOrPutType);
            traceCursorImpl(sb);
            if (key != null) {
                sb.append(" key=").append(key.dumpData());
            }
            if (data != null) {
                sb.append(" data=").append(data.dumpData());
            }
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            LoggerUtils.logMsg(
                logger, dbImpl.getEnv(), level, sb.toString());
        }
    }

    /**
     * Sends trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(
        final Level level,
        final String methodName,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LockMode lockMode) {
        
        if (logger.isLoggable(level)) {
            final StringBuilder sb = new StringBuilder();
            sb.append(methodName);
            traceCursorImpl(sb);
            if (key != null) {
                sb.append(" key=").append(key.dumpData());
            }
            if (data != null) {
                sb.append(" data=").append(data.dumpData());
            }
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            LoggerUtils.logMsg(
                logger, dbImpl.getEnv(), level, sb.toString());
        }
    }

    /**
     * Sends trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(
        final Level level,
        final String methodName,
        final LockMode lockMode) {
        
        if (logger.isLoggable(level)) {
            final StringBuilder sb = new StringBuilder();
            sb.append(methodName);
            traceCursorImpl(sb);
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            LoggerUtils.logMsg(
                logger, dbImpl.getEnv(), level, sb.toString());
        }
    }

    void trace(
        final Level level,
        final String methodName,
        final DatabaseEntry key,
        final DatabaseEntry pk,
        final String msg) {

        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(methodName);

            if (key != null) {
                sb.append(" key = ").append(DatabaseUtil.printByteArray(key.getData()));
            }
            if (pk != null) {
                sb.append(" pk = ").append(DatabaseUtil.printByteArray(pk.getData()));
            }

            sb.append(" : ").append(msg);

            LoggerUtils.logMsg(
                logger, getDatabaseImpl().getEnv(), level, sb.toString());
        }
    }

    private void traceCursorImpl(final StringBuilder sb) {
        sb.append(" locker=").append(cursorImpl.getLocker().getId());
        sb.append(" bin=").append(cursorImpl.getCurrentNodeId());
        sb.append(" idx=").append(cursorImpl.getIndex());
    }
}
