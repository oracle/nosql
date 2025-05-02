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
import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Base class for exceptions thrown when a read or write operation fails
 * because of a secondary constraint or integrity problem.  Provides accessors
 * for getting further information about the database and keys involved in the
 * failure.  See subclasses for more information.
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 *
 * @see <a href="SecondaryDatabase.html#transactions">Special considerations
 * for using Secondary Databases with and without Transactions</a>
 *
 * @since 4.0
 */
public abstract class SecondaryReferenceException
    extends OperationFailureException {

    private static final long serialVersionUID = 1;

    private final String secDbName;
    private final String priDbName;
    private final DatabaseEntry secKey;
    private final DatabaseEntry priKey;
    private final long priLsn;
    private final long expirationTime;

    /**
     * For internal use only.
     * @hidden
     */
    public SecondaryReferenceException(Locker locker,
                                       String message,
                                       String secDbName,
                                       String priDbName,
                                       DatabaseEntry secKey,
                                       DatabaseEntry priKey,
                                       long priLsn,
                                       long expirationTime,
                                       ExtinctionStatus extinctionStatus) {
        super(locker, true /*abortOnly*/, message, null /*cause*/);
        this.priDbName = priDbName;
        this.secDbName = secDbName;
        this.secKey = secKey;
        this.priKey = priKey;
        this.priLsn = priLsn;
        this.expirationTime = expirationTime;

        addErrorMessage(
            "secDbName=" + secDbName +
            " priDbName=" + priDbName +
            " expiration=" + ((expirationTime != 0) ?
                TTL.formatExpirationTime(expirationTime) : "none") +
            ((extinctionStatus != null) ? (" " + extinctionStatus) : "") +
            " priLsn=" + DbLsn.getNoFormatString(priLsn));

        if (locker.getEnvironment().getExposeUserData()) {
            addErrorMessage(
                "priKey=" +
                ((priKey != null) ? priKey.toString() : "unknown") +
                " secKey=" +
                ((secKey != null) ? secKey.toString() : "unknown"));
        }
    }

    /**
     * For internal use only.
     * @hidden
     */
    SecondaryReferenceException(String message,
                                SecondaryReferenceException cause) {
        super(message, cause);
        this.priDbName = cause.priDbName;
        this.secDbName = cause.secDbName;
        this.secKey = cause.secKey;
        this.priKey = cause.priKey;
        this.priLsn = cause.priLsn;
        this.expirationTime = cause.expirationTime;
    }

    /**
     * Returns the name of the secondary database being accessed during the
     * failure.
     */
    public String getSecondaryDatabaseName() {
        return secDbName;
    }

    /**
     * Returns the name of the primary database being accessed during the
     * failure.
     *
     * @since 18.2
     */
    public String getPrimaryDatabaseName() {
        return priDbName;
    }

    /**
     * Returns the secondary key being accessed during the failure. Note that
     * in some cases, the returned primary key can be null.
     */
    public DatabaseEntry getSecondaryKey() {
        return secKey;
    }

    /**
     * Returns the primary key being accessed during the failure. Note that
     * in some cases, the returned primary key can be null.
     */
    public DatabaseEntry getPrimaryKey() {
        return priKey;
    }

    /**
     * Returns the internal Log Sequence Number of the primary record being
     * accessed during the failure.
     *
     * @since 18.2
     */
    public long getPrimaryLsn() {
        return priLsn;
    }

    /**
     * Returns the expiration time of the record being accessed during the
     * failure.
     *
     * @since 7.0
     */
    public long getExpirationTime() {
        return expirationTime;
    }
}
