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

package com.sleepycat.je.rep;

import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.txn.Locker;

/**
 * This exception indicates that an update operation or transaction commit
 * or abort was attempted while in the
 * {@link ReplicatedEnvironment.State#REPLICA} state. The transaction is marked
 * as being invalid.
 * <p>
 * The exception is the result of either an error in the application logic or 
 * the result of a transition of the node from Master to Replica while a 
 * transaction was in progress.
 * <p>
 * The application must abort the current transaction and redirect all
 * subsequent update operations to the Master.
 */
public class ReplicaWriteException extends StateChangeException {
    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public ReplicaWriteException(Locker locker,
                                 StateChangeEvent stateChangeEvent) {
        super(locker, stateChangeEvent);
    }

    /**
     * Only for use by wrapSelf methods.
     */
    private ReplicaWriteException(String message,
                                  ReplicaWriteException cause) {
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

        return new ReplicaWriteException(
            msg, (ReplicaWriteException) clonedCause);
    }

    /**
     * For internal use only.
     * @hidden
     * Allows standalone JE code to check for ReplicaWriteException.
     */
    @Override
    public boolean isReplicaWrite() {
        return true;
    }
}
