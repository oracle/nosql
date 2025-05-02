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
 * @hidden internal, for use in disaster recovery [#23447]
 *
 * Thrown when an operation is performed on an active replication group member
 * but it requires that the member not be active.
 */
public class MemberActiveException extends OperationFailureException {
    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     */
    public MemberActiveException(String message) {
        super(null /*locker*/, false /*abortOnly*/, message, null /*cause*/);
    }

    /**
     * Only for use by wrapSelf methods.
     */
    private MemberActiveException(String message,
                                  OperationFailureException cause) {
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

        return new MemberActiveException(msg, clonedCause);
    }
}
