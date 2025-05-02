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

/**
 * This exception indicates that the application attempted an operation that is
 * not permitted when it is in the {@link ReplicatedEnvironment.State#MASTER}
 * state.
 */
public class MasterStateException extends StateChangeException {
    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public MasterStateException(StateChangeEvent stateChangeEvent) {
        super(null, stateChangeEvent);
    }

    /**
     * For internal use only.
     * @hidden
     */
    public MasterStateException(String message) {
        super(message, null);
    }

    /**
     * Only for use by wrapSelf methods.
     */
    private MasterStateException(String message,
                                 MasterStateException cause) {
        super(message, cause);
    }

    /**
     * For internal use only.
     * @hidden
     */
    @Override
    public MasterStateException wrapSelf(
        String msg,
        OperationFailureException clonedCause) {

        return new MasterStateException(
            msg, (MasterStateException) clonedCause);
    }
}
