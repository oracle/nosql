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
 * Base class for exceptions thrown when a write operation fails because of a
 * secondary constraint.  See subclasses for more information.
 *
 * <p>The {@link Transaction} handle is invalidated as a result of this
 * exception.</p>
 *
 * @see <a href="SecondaryDatabase.html#transactions">Special considerations
 * for using Secondary Databases with and without Transactions</a>
 *
 * @since 4.0
 */
public abstract class SecondaryConstraintException
    extends SecondaryReferenceException {

    private static final long serialVersionUID = 1L;

    /** 
     * For internal use only.
     * @hidden 
     */
    public SecondaryConstraintException(Locker locker,
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
     * For internal use only.
     * @hidden 
     */
    SecondaryConstraintException(String message,
                                 SecondaryConstraintException cause) {
        super(message, cause);
    }
}
