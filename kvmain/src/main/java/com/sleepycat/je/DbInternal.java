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

import java.util.Properties;

import com.sleepycat.je.beforeimage.BeforeImageContext;
import com.sleepycat.compat.DbCompat.OpReadOptions;
import com.sleepycat.compat.DbCompat.OpResult;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.PutMode;
import com.sleepycat.je.dbi.ReplayPreprocessor;
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.util.keyrange.KeyRange;
import com.sleepycat.util.keyrange.RangeCursor;

/**
 * @hidden
 * For internal use only. It serves to shelter methods that must be public to
 * be used by other BDB JE packages but that are not part of the public API
 * available to applications.
 */
public class DbInternal {

    /**
     * Used for internal ops that don't need any OperationResult properties,
     * and just need a pass/fail indication.
     */
    public static OperationResult DEFAULT_RESULT =
        new OperationResult(
            0 /*expirationTime*/, false /*update*/,
            0L /*modificationTime*/,
            0 /*storageSize*/,
            false /*tombstone*/);

    /**
     * Proxy to Database.invalidate()
     */
    public static void invalidate(final Database db) {
        db.invalidate();
    }

    /**
     * Proxy to Database.setPreempted()
     */
    public static void setPreempted(final Database db,
                                    final String dbName,
                                    final String msg) {
        db.setPreempted(dbName, msg);
    }

    /**
     * Proxy to Environment.getMaybeNullEnvImpl.
     *
     * This method does not check whether the returned envImpl is valid.
     *
     * WARNING: This method will be phased out over time and normally
     * getNonNullEnvImpl should be called instead.
     *
     * @return the non-null underlying EnvironmentImpl, or null if the env has
     * been closed.
     */
    public static EnvironmentImpl getEnvironmentImpl(final Environment env) {
        return env.getMaybeNullEnvImpl();
    }

    /**
     * Proxy to Environment.getNonNullEnvImpl
     *
     * This method is called to access the underlying EnvironmentImpl when an
     * env is expected to be open, to guard against NPE when the env has been
     * closed.
     *
     * This method does not check whether the env is valid.
     *
     * @return the non-null, underlying EnvironmentImpl.
     *
     * @throws IllegalStateException if the env has been closed.
     */
    public static EnvironmentImpl getNonNullEnvImpl(final Environment env) {
        return env.getNonNullEnvImpl();
    }

    /**
     * Proxy to Environment.initEnvImpl
     */
    public static void initEnvImpl(final Environment env) {
        env.initEnvImpl();
    }

    /**
     * Proxy to Environment.checkOpen
     */
    public static EnvironmentImpl checkOpen(final Environment env) {
        return env.checkOpen();
    }

    /**
     * Proxy to Environment.closeInternalHandle
     */
    public static void closeInternalHandle(final Environment env) {
        env.closeInternalHandle();
    }

    /**
     * Proxy to SecondaryDatabase.getSecondaryAssociation
     */
    public static SecondaryAssociation getSecondaryAssociation(
        final Database db) {
        return db.getSecondaryAssociation();
    }

    /**
     * Proxy to SecondaryDatabase.getPrivateSecondaryConfig
     */
    public static SecondaryConfig getPrivateSecondaryConfig(
        final SecondaryDatabase secDb) {
        return secDb.getPrivateSecondaryConfig();
    }

    /**
     * Proxy to Cursor.excludeFromOpStats
     */
    public static void excludeFromOpStats(final Cursor cursor) {
        cursor.excludeFromOpStats();
    }

    /**
     * Proxy to Cursor.readPrimaryAfterGet
     */
    public static OperationResult readPrimaryAfterGet(
        final Cursor cursor,
        final Database priDb,
        final DatabaseEntry key,
        final DatabaseEntry pKey,
        DatabaseEntry data,
        final LockMode lockMode,
        final boolean secDirtyRead,
        final boolean lockPrimaryOnly,
        final boolean allowNoData,
        final Locker locker,
        final SecondaryDatabase secDb,
        final SecondaryAssociation secAssoc) {

        return cursor.readPrimaryAfterGet(
            priDb, key, pKey, data, lockMode, secDirtyRead,
            lockPrimaryOnly, allowNoData, locker, secDb, secAssoc);
    }

    /**
     * Proxy to Cursor.deleteWithRepContext()
     */
    public static OperationResult deleteWithRepContext(
        final Cursor cursor,
        final ReplicationContext repContext) {

        return cursor.deleteWithRepContext(repContext);
    }

    /**
     * Proxy to Cursor.searchAndDelete()
     */
    public static OperationResult searchAndDelete(
        final Cursor cursor,
        final DatabaseEntry twoPartKey,
        final ReplayPreprocessor preprocessor,
        final long modificationTime,
        final long oldExpirationTime,
        final ReplicationContext repContext,
        final CacheMode cacheMode,
        final boolean allowBlindDelete,
        final BeforeImageContext bImgCtx) {

        return cursor.searchAndDelete(
            twoPartKey, preprocessor, modificationTime, oldExpirationTime,
            repContext, cacheMode, allowBlindDelete, bImgCtx);
    }

    /**
     * Proxy to Cursor.putForReplay()
     */
    public static OperationResult putForReplay(
        final Cursor cursor,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final LNLogEntry<?> lnEntry,
        final ReplayPreprocessor preprocessor,
        final PutMode putMode,
        final ReplicationContext repContext) {

        return cursor.putForReplay(
            key, data, lnEntry, preprocessor, putMode, repContext);
    }

    /**
     * Proxy to Cursor.putWithRepContext()
     */
    public static OperationResult putWithRepContext(
        final Cursor cursor,
        final DatabaseEntry key,
        final DatabaseEntry data,
        final PutMode putMode,
        final ReplicationContext repContext) {

        return cursor.putWithRepContext(key, data, putMode, repContext);
    }

    /**
     * Search mode used with the internal search and searchBoth methods.
     */
    public enum Search {

        /**
         * Match the smallest value greater than the key or data param.
         */
        GT,

        /**
         * Match the smallest value greater than or equal to the key or data
         * param.
         */
        GTE,

        /**
         * Match the largest value less than the key or data param.
         */
        LT,

        /**
         * Match the largest value less than or equal to the key or data param.
         */
        LTE,
    }

    /**
     * Finds the key according to the Search param. If dups are configured, GT
     * and GTE will land on the first dup for the matching key, while LT and
     * LTE will land on the last dup for the matching key.
     *
     * search() and searchBoth() in this class may eventually be exposed as
     * public JE Cursor methods, but this isn't practical now for the following
     * reasons:
     *
     *  + The API design needs more thought. Perhaps Search.EQ should be added.
     *    Perhaps existing Cursor methods should be deprecated.
     *
     *  + This implementation moves the cursor multiple times and does not
     *    release locks on the intermediate records.
     *
     *  + This could be implemented more efficiently using lower level cursor
     *    code. For example, an LTE search would actually more efficient than
     *    the existing GTE search (getSearchKeyRange and getSearchBothRange).
     *
     * These methods are used by KVStore.
     */
    public static OperationResult search(
        final Cursor cursor,
        final DatabaseEntry key,
        final DatabaseEntry pKey,
        final DatabaseEntry data,
        final Search searchMode,
        final ReadOptions options) {

        final DatabaseImpl dbImpl = cursor.getDatabaseImpl();
        KeyRange range = new KeyRange(
            dbImpl.getIntBtreeComparator().asJavaComparator());
        final boolean first;

        switch (searchMode) {
        case GT:
        case GTE:
            range = range.subRange(
                key, searchMode == Search.GTE, null, false);
            first = true;
            break;
        case LT:
        case LTE:
            range = range.subRange(
                null, false, key, searchMode == Search.LTE);
            first = false;
            break;
        default:
            throw EnvironmentFailureException.unexpectedState();
        }

        final RangeCursor rangeCursor = new RangeCursor(
            range, null, dbImpl.getSortedDuplicates(), (data == null), cursor);

        final OpReadOptions opReadOptions = OpReadOptions.make(options);

        final OpResult result = (first) ?
            rangeCursor.getFirst(key, pKey, data, opReadOptions) :
            rangeCursor.getLast(key, pKey, data, opReadOptions);

        /* RangeCursor should not have dup'd the cursor. */
        assert cursor == rangeCursor.getCursor();

        return result.jeResult;
    }

    /**
     * Searches with the dups for the given key and finds the dup matching the
     * pKey value, according to the Search param.
     *
     * See search() for more discussion.
     */
    public static OperationResult searchBoth(
        final Cursor cursor,
        final DatabaseEntry key,
        final DatabaseEntry pKey,
        final DatabaseEntry data,
        final Search searchMode,
        final ReadOptions options) {

        final DatabaseImpl dbImpl = cursor.getDatabaseImpl();
        KeyRange range = new KeyRange(
            dbImpl.getIntBtreeComparator().asJavaComparator());
        range = range.subRange(key);
        KeyRange pKeyRange = new KeyRange(
            dbImpl.getIntDupComparator().asJavaComparator());
        final boolean first;

        switch (searchMode) {
        case GT:
        case GTE:
            pKeyRange = pKeyRange.subRange(
                pKey, searchMode == Search.GTE, null, false);
            first = true;
            break;
        case LT:
        case LTE:
            pKeyRange = pKeyRange.subRange(
                null, false, pKey, searchMode == Search.LTE);
            first = false;
            break;
        default:
            throw EnvironmentFailureException.unexpectedState();
        }

        final RangeCursor rangeCursor = new RangeCursor(
            range, pKeyRange, dbImpl.getSortedDuplicates(), (data == null), cursor);

        final OpReadOptions opReadOptions = OpReadOptions.make(options);

        final OpResult result = (first) ?
            rangeCursor.getFirst(key, pKey, data, opReadOptions) :
            rangeCursor.getLast(key, pKey, data, opReadOptions);

        /* RangeCursor should not have dup'd the cursor. */
        assert cursor == rangeCursor.getCursor();

        return result.jeResult;
    }

    /**
     * Proxy to Cursor.getCursorImpl()
     */
    public static CursorImpl getCursorImpl(Cursor cursor) {
        return cursor.getCursorImpl();
    }

    /**
     * Create a Cursor for internal use from a DatabaseImpl.
     * The retainNonTxnLocks param is true.
     */
    public static Cursor makeCursor(final DatabaseImpl databaseImpl,
                                    final Locker locker,
                                    final CursorConfig cursorConfig) {
        return makeCursor(databaseImpl, locker, cursorConfig, true);
    }

    /**
     * Create a Cursor for internal use from a DatabaseImpl, specifying
     * retainNonTxnLocks.
     */
    public static Cursor makeCursor(final DatabaseImpl databaseImpl,
                                    final Locker locker,
                                    final CursorConfig cursorConfig,
                                    boolean retainNonTxnLocks) {
        return new Cursor(
            databaseImpl, locker, cursorConfig, retainNonTxnLocks);
    }

    /**
     * Create a Cursor from a DatabaseHandle.
     */
    public static Cursor makeCursor(final Database dbHandle,
                                    final Locker locker,
                                    final CursorConfig cursorConfig) {
        return makeCursor(dbHandle, locker, cursorConfig, false);
    }

    public static Cursor makeCursor(final Database dbHandle,
                                    final Locker locker,
                                    final CursorConfig cursorConfig,
                                    boolean retainNonTxnLocks) {
        return new Cursor(dbHandle, locker, cursorConfig, retainNonTxnLocks);
    }

    public static boolean isCorrupted(Database db) {
        return db.isCorrupted();
    }

    public static SecondaryIntegrityException getCorruptedCause(Database db) {
        return db.getCorruptedCause();
    }

    /**
     * Proxy to Database.getDbImpl()
     */
    public static DatabaseImpl getDbImpl(final Database db) {
        return db.getDbImpl();
    }

    /**
     * Proxy to JoinCursor.getSortedCursors()
     */
    public static Cursor[] getSortedCursors(final JoinCursor cursor) {
        return cursor.getSortedCursors();
    }

    /**
     * Proxy to EnvironmentConfig.setLoadPropertyFile()
     */
    public static void setLoadPropertyFile(final EnvironmentConfig config,
                                           final boolean loadProperties) {
        config.setLoadPropertyFile(loadProperties);
    }

    /**
     * Proxy to EnvironmentConfig.setCreateUP()
     */
    public static void setCreateUP(final EnvironmentConfig config,
                                   final boolean val) {
        config.setCreateUP(val);
    }

    /**
     * Proxy to EnvironmentConfig.getCreateUP()
     */
    public static boolean getCreateUP(final EnvironmentConfig config) {
        return config.getCreateUP();
    }

    /**
     * Proxy to EnvironmentConfig.setCreateEP()
     */
    public static void setCreateEP(final EnvironmentConfig config,
                                   final boolean val) {
        config.setCreateEP(val);
    }

    /**
     * Proxy to EnvironmentConfig.getCreateEP()
     */
    public static boolean getCreateEP(final EnvironmentConfig config) {
        return config.getCreateEP();
    }

    /**
     * Proxy to EnvironmentConfig.setCheckpointUP()
     */
    public static void setCheckpointUP(final EnvironmentConfig config,
                                       final boolean checkpointUP) {
        config.setCheckpointUP(checkpointUP);
    }

    /**
     * Proxy to EnvironmentConfig.getCheckpointUP()
     */
    public static boolean getCheckpointUP(final EnvironmentConfig config) {
        return config.getCheckpointUP();
    }
    
    /**
     * Proxy to EnvironmentConfig.setBImgIdx()
     */
    public static void setCreateBImgIdx(final EnvironmentConfig config,
                                       final boolean bImgIdx) {
        config.setCreateBImgIdx(bImgIdx);
    }
    
    /**
     * Proxy to EnvironmentConfig.getBImgIdx()
     */
    public static boolean getBImgIdx(final EnvironmentConfig config) {
        return config.getBImgIdx();
    }

    /**
     * Proxy to EnvironmentConfig.setTxnReadCommitted()
     */
    public static void setTxnReadCommitted(final EnvironmentConfig config,
                                           final boolean txnReadCommitted) {
        config.setTxnReadCommitted(txnReadCommitted);
    }

    /**
     * Proxy to EnvironmentConfig.setTxnReadCommitted()
     */
    public static boolean getTxnReadCommitted(final EnvironmentConfig config) {
        return config.getTxnReadCommitted();
    }

    /**
     * Proxy to EnvironmentMutableConfig.cloneMutableConfig()
     */
    public static EnvironmentMutableConfig
        cloneMutableConfig(final EnvironmentMutableConfig config) {
        return config.cloneMutableConfig();
    }

    /**
     * Proxy to EnvironmentMutableConfig.copyMutablePropsTo()
     */
    public static void
        copyMutablePropsTo(final EnvironmentMutableConfig config,
                           final EnvironmentMutableConfig toConfig) {
        config.copyMutablePropsTo(toConfig);
    }

    /**
     * Proxy to EnvironmentMutableConfig.validateParams.
     */
    public static void
        disableParameterValidation(final EnvironmentMutableConfig config) {
        config.setValidateParams(false);
    }

    /**
     * Proxy to EnvironmentMutableConfig.getProps
     */
    public static Properties getProps(final EnvironmentMutableConfig config) {
        return config.getProps();
    }

    /**
     * Proxy to DatabaseConfig.setUseExistingConfig()
     */
    public static void setUseExistingConfig(final DatabaseConfig config,
                                            final boolean useExistingConfig) {
        config.setUseExistingConfig(useExistingConfig);
    }

    /**
     * Proxy to DatabaseConfig.validate(DatabaseConfig()
     */
    public static void validate(final DatabaseConfig config1,
                                final DatabaseConfig config2)
        throws DatabaseException {

        config1.validate(config2);
    }

    /**
     * Proxy to Transaction.getLocker()
     */
    public static Locker getLocker(final Transaction txn)
        throws DatabaseException {

        return txn.getLocker();
    }

    /**
     * Proxy to Transaction.getEnvironment()
     */
    public static Environment getEnvironment(final Transaction txn)
        throws DatabaseException {

        return txn.getEnvironment();
    }

    /**
     * Proxy to Environment.getDefaultTxnConfig()
     */
    public static TransactionConfig
        getDefaultTxnConfig(final Environment env) {
        return env.getDefaultTxnConfig();
    }

    public static Transaction
        beginInternalTransaction(final Environment env,
                                 final TransactionConfig config) {
        return env.beginInternalTransaction(config);
    }

    public static ExceptionEvent makeExceptionEvent(final Exception e,
                                                    final String n) {
        return new ExceptionEvent(e, n);
    }

    public static Txn getTxn(final Transaction transaction) {
        return transaction.getTxn();
    }

    public static OperationResult makeResult(
        final long expirationTime,
        final long modificationTime,
        final int storageSize,
        final boolean tombstone) {

        return new OperationResult(
            expirationTime, false /*update*/,
            modificationTime, storageSize, tombstone);
    }

    public static OperationResult makeResult(
        final int expiration,
        final boolean expirationInHours,
        final boolean update,
        final long modificationTime,
        final int storageSize,
        final boolean tombstone) {

        return new OperationResult(
            TTL.expirationToSystemTime(expiration, expirationInHours),
            update, modificationTime, storageSize, tombstone);
    }

    public static ReadOptions getReadOptions(LockMode lockMode) {
        if (lockMode == null) {
            lockMode = LockMode.DEFAULT;
        }
        return lockMode.toReadOptions();
    }
}
