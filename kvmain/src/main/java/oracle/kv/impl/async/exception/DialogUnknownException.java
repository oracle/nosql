/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.async.exception;

/**
 * A dialog exception that is caused by unexpected reason.
 *
 * This exception indicates a bug in the code. There is nothing we can do
 * except logging and fixing it.  Operations that result in this exception
 * should not be retried.
 */
public class DialogUnknownException extends PersistentDialogException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param hasSideEffect {@code true} if the dialog incurs any side effect
     * on the remote.
     * @param fromRemote {@code true} if the exception is reported from the
     * remote
     * @param message the message of the exception
     * @param cause the cause of the exception
     */
    public DialogUnknownException(boolean hasSideEffect,
                                  boolean fromRemote,
                                  String message,
                                  Throwable cause) {
        super(hasSideEffect, fromRemote, message, cause);
    }

    /**
     * This exception is not expected because it represents a bug.
     */
    @Override
    public boolean isExpectedException() {
        return false;
    }
}
