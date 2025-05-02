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
 * Thrown by {@link ReplicatedEnvironment#transferMaster} if a Master Transfer
 * operation cannot be completed within the allotted time.
 */
public class MasterTransferFailureException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /** 
     * For internal use only.
     * @hidden 
     */
    public MasterTransferFailureException(String message) {
        super(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    /** 
     * Only for use by wrapSelf methods.
     */
    private MasterTransferFailureException
        (String message, OperationFailureException cause) {
        super(message, cause);
    }

    /** 
     * For internal use only.
     * @hidden 
     */
    @Override
    public MasterTransferFailureException wrapSelf(
        String message,
        OperationFailureException clonedCause) {

        return new MasterTransferFailureException(message, clonedCause);
    }
}
