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
 * Thrown when an attempt to write a primary database record would insert a
 * secondary record with a key that does not exist in a foreign key database,
 * when the secondary key is configured as a foreign key.
 *
 * <p>When using the base API ({@code com.sleepycat.je}), this can occur when a
 * {@link SecondaryDatabase} is configured to be associated with a foreign key
 * database (see {@link SecondaryConfig#setForeignKeyDatabase}).</p>
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 *
 * @see <a href="SecondaryDatabase.html#transactions">Special considerations
 * for using Secondary Databases with and without Transactions</a>
 *
 * @since 4.0
 */
public class ForeignConstraintException extends SecondaryConstraintException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public ForeignConstraintException(Locker locker,
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
    }

    /** 
     * Only for use by wrapSelf methods.
     */
    private ForeignConstraintException(String message,
                                       ForeignConstraintException cause) {
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

        return new ForeignConstraintException(
            msg, (ForeignConstraintException) clonedCause);
    }
}
