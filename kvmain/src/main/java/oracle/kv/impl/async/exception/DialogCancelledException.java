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
 * A dialog exception that indicates the dialog is cancelled by the upper layer.
 *
 * This exception indicates that the dialog is cancelled by the upper layer for
 * some unknown reason possibly unexpected error or resource issue.
 */
public class DialogCancelledException extends DialogUnknownException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param hasSideEffect {@code true} if the dialog incurs any side effect
     * on the remote.
     * @param fromRemote {@code true} if the exception is reported from the
     * remote
     * @param cause the cause of the exception
     */
    public DialogCancelledException(boolean hasSideEffect,
                                    boolean fromRemote,
                                    Throwable cause) {
        super(hasSideEffect, fromRemote,
              "dialog cancelled by the upper layer", cause);
    }
}
