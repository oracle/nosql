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
 * A dialog exception that should be handled by retrying the dialog.
 *
 * Upon seeing this exception, the layer managing the dialog could consider
 * retrying the dialog immediately since the error is likely to go away. The
 * exception does not in any way indicates the dialog ought be retried or can
 * be retired which should be determined based on the characteristics of the
 * dialog and the impact of side effect, etc., by the upper layer.
 *
 * @see ConnectionException#isPersistent
 */
public class TemporaryDialogException extends DialogException {

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
    public TemporaryDialogException(boolean hasSideEffect,
                                         boolean fromRemote,
                                         String message,
                                         Throwable cause) {
        super(hasSideEffect, fromRemote, message, cause);
    }

    /**
     * Callers can retry operations that resulted in this exception
     * immediately, without waiting.
     */
    @Override
    public boolean isPersistent() {
        return false;
    }
}
