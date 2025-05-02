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

import com.sleepycat.je.Database;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.ReplicationContext;

/**
 * Factory of static methods for creating Locker objects.
 */
public class LockerFactory {

    /**
     * Get a locker for a write operation, checking whether the db and
     * environment is transactional or not. Must return a non null locker.
     */
    public static Locker getWritableLocker(final Environment env,
                                           final Transaction userTxn,
                                           final boolean isInternalDb,
                                           final boolean dbIsTransactional,
                                           final boolean autoTxnIsReplicated) {

        return getWritableLocker(
            env, userTxn, isInternalDb, dbIsTransactional,
            autoTxnIsReplicated, null /*autoCommitConfig*/);
    }

    /**
     * Get a locker for a write operation.
     *
     * @param autoTxnIsReplicated is true if this transaction is
     * executed on a rep group master, and needs to be broadcast.
     * Currently, all application-created transactions are of the type
     * com.sleepycat.je.txn.Txn, and are replicated if the parent
     * environment is replicated. Auto Txns are trickier because they may
     * be created for a local write operation, such as log cleaning.
     */
    public static Locker getWritableLocker(
        final Environment env,
        final Transaction userTxn,
        final boolean isInternalDb,
        final boolean dbIsTransactional,
        final boolean autoTxnIsReplicated,
        TransactionConfig autoCommitConfig) {

        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);
        final boolean envIsTransactional = envImpl.isTransactional();

        if (dbIsTransactional && userTxn == null) {

            if (autoCommitConfig == null) {
                autoCommitConfig = DbInternal.getDefaultTxnConfig(env);
            }

            return Txn.createAutoTxn(
                envImpl, autoCommitConfig,
                (autoTxnIsReplicated ?
                 ReplicationContext.MASTER :
                 ReplicationContext.NO_REPLICATE));

        }

        if (userTxn == null) {
            /* Non-transactional user operations use ThreadLocker. */
            return
                ThreadLocker.createThreadLocker(envImpl, autoTxnIsReplicated);
        }

        /*
         * The user provided a transaction, so the environment and the
         * database had better be opened transactionally.
         */
        if (!isInternalDb && !envIsTransactional) {
            throw new IllegalArgumentException(
                "A Transaction cannot be used because the"+
                " environment was opened non-transactionally");
        }
        if (!dbIsTransactional) {
            throw new IllegalArgumentException(
                "A Transaction cannot be used because the" +
                " database was opened non-transactionally");
        }

        /* Use the locker for the given transaction. */
        return DbInternal.getLocker(userTxn);
    }

    /**
     * Get a locker for a read or cursor operation.
     */
    public static Locker getReadableLocker(
        final Database dbHandle,
        final Transaction userTxn) {

        return getReadableLocker(
            dbHandle,
            (userTxn != null) ? DbInternal.getLocker(userTxn) : null);
    }

    /**
     * Get a locker for this database handle for a read or cursor operation.
     */
    public static Locker getReadableLocker(
        final Database dbHandle,
        Locker locker) {

        final DatabaseImpl dbImpl = DbInternal.getDbImpl(dbHandle);

        if (!dbImpl.isTransactional() &&
            locker != null &&
            locker.isTransactional()) {
            throw new IllegalArgumentException(
                "A Transaction cannot be used because the" +
                " database was opened non-transactionally");
        }

        /* Don't reuse a non-transactional locker. */
        if (locker != null && !locker.isTransactional()) { 
            locker = null;
        }

        final boolean autoTxnIsReplicated =
            dbImpl.isReplicated() &&
            dbImpl.getEnv().isReplicated();

        final Environment env = dbHandle.getEnvironment();
        final EnvironmentImpl envImpl = DbInternal.getNonNullEnvImpl(env);

        if (locker == null) {
            /* Non-transactional user operations use ThreadLocker. */
            return
                ThreadLocker.createThreadLocker(envImpl, autoTxnIsReplicated);
        }

        /* Use the given locker. */
        return locker;
    }

    /**
     * Get a non-transactional locker for internal database operations.  Always
     * non replicated.
     *
     * This method is not called for user txns and should not throw a Java
     * runtime exception (IllegalArgument, etc).
     */
    public static Locker getInternalReadOperationLocker(
        final EnvironmentImpl envImpl) {

        return BasicLocker.createBasicLocker(envImpl);
    }
}
