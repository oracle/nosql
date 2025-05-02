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

import com.sleepycat.je.txn.Locker;

/**
 * Thrown when a non-blocking operation fails to get a lock.  Non-blocking
 * transactions are configured using {@link TransactionConfig#setNoWait}.
 *
 * <p>The {@link Transaction} handle is <em>not</em> invalidated as a result of
 * this exception.</p>
 *
 * <p>Normally, applications should catch the base class {@link
 * LockConflictException} rather than catching one of its subclasses.  All lock
 * conflicts are typically handled in the same way, which is normally to abort
 * and retry the transaction.  See {@link LockConflictException} for more
 * information.</p>
 *
 * @since 4.0
 */
public class LockNotAvailableException extends LockConflictException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public LockNotAvailableException(Locker locker, String message) {
        /* Do not set abort-only for a no-wait lock failure. */
        super(message);
    }

    /** 
     * Only for use by wrapSelf methods.
     */
    private LockNotAvailableException(String message,
                                      LockNotAvailableException cause) {
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

        return new LockNotAvailableException(
            msg, (LockNotAvailableException) clonedCause);
    }
}
