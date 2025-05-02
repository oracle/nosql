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

import com.sleepycat.je.ExtinctionFilter.ExtinctionStatus;
import com.sleepycat.je.txn.Locker;

/**
 * Thrown when an integrity problem is detected while accessing a secondary
 * database, including access to secondaries while writing to a primary
 * database. Secondary integrity problems are normally caused by the use of
 * secondaries without transactions.
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception. In addition, if
 * {@link EnvironmentConfig#TREE_SECONDARY_INTEGRITY_FATAL} is true (the
 * default) the corrupt index (secondary database) is marked as corrupt in
 * memory and all subsequent access to the index will throw
 * {@code SecondaryIntegrityException}. To correct the problem, the
 * application may perform a full restore (an HA {@link
 * com.sleepycat.je.rep.NetworkRestore} or restore from backup) or rebuild
 * the corrupt index.</p>
 *
 * <p>A secondary corruption may also be detected by Btree verification when
 * performed by the {@link Environment#verify} method, or by the {@link
 * EnvironmentConfig#ENV_RUN_VERIFIER background verifier}
 * (if {@link EnvironmentConfig#VERIFY_SECONDARIES} is set to true).</p>
 *
 * <p>Some possible causes of a secondary integrity exception are listed
 * below. </p>
 * <ol>
 *  <li>The use of non-transactional databases or stores can cause secondary
 *  corruption as described in <a
 *  href="SecondaryDatabase.html#transactions">Special considerations for using
 *  Secondary Databases with and without Transactions</a>.  Secondary databases
 *  and indexes should always be used in conjunction with transactional
 *  databases and stores.</li>
 *
 *  <li>Secondary corruption can be caused by an incorrectly implemented
 *  secondary key creator method, for example, one which uses mutable state
 *  information or is not properly synchronized.  When the DPL is not used, the
 *  application is responsible for correctly implementing the key creator.</li>
 *
 *  <li>Secondary corruption can be caused by failing to open a secondary
 *  database before writing to the primary database, by writing to a secondary
 *  database directly using a {@link Database} handle, or by truncating or
 *  removing primary database without also truncating or removing all secondary
 *  databases.  When the DPL is not used, the application is responsible for
 *  managing associated databases correctly.
 * </ol>
 *
 * @since 4.0
 */
public class SecondaryIntegrityException extends SecondaryReferenceException {
    private static final long serialVersionUID = 1L;

    /** 
     * For internal use only.
     * @hidden 
     */
    public SecondaryIntegrityException(Database secDb,
                                       boolean invalidateDb,
                                       Locker locker,
                                       String message,
                                       String secDbName,
                                       String priDbName,
                                       DatabaseEntry secKey,
                                       DatabaseEntry priKey,
                                       long priLsn,
                                       long expirationTime,
                                       ExtinctionStatus extinctionStatus) {
        super(locker, message, secDbName, priDbName, secKey, priKey,
            priLsn, expirationTime, extinctionStatus);

        if (secDb != null && invalidateDb &&
            secDb.getEnv().isSecondaryIntegrityErrorFatal()) {
            secDb.setCorrupted(this);
        }
    }

    /** 
     * Only for use by wrapSelf methods.
     */
    private SecondaryIntegrityException(String message,
                                        SecondaryIntegrityException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public OperationFailureException wrapSelf(
        String msg,
        OperationFailureException clonedCause) {

        return new SecondaryIntegrityException(
            msg, (SecondaryIntegrityException) clonedCause);
    }
}
