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
 * Thrown when a write operation cannot be performed because a disk limit has
 * been violated. It may also be thrown by {@link Environment#checkpoint}
 * {@link Environment#sync} and {@link Environment#close} (when it performs
 * a checkpoint).
 *
 * @see EnvironmentConfig#MAX_DISK
 * @see EnvironmentConfig#FREE_DISK
 * @since 7.5
 */
public class DiskLimitException extends OperationFailureException {

    private static final long serialVersionUID = 1;

    /**
     * For internal use only.
     * @hidden
     *
     * @param locker is non-null to mark the txn abort-only, or null in cases
     * where no txn/locker is involved.
     */
    public DiskLimitException(Locker locker, String message) {
        super(
            locker /*locker*/,
            locker != null /*abortOnly*/,
            message,
            null /*cause*/);
    }

    /**
     * Only for use by wrapSelf methods.
     */
    private DiskLimitException(String message,
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

        return new DiskLimitException(msg, clonedCause);
    }
}
