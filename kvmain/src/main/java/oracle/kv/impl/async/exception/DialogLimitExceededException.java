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
 * This exception is thrown because the local endpoint has already started the
 * maximum amount of dialogs when the dialog is being started.
 *
 * This is a {@link DialogUnknownException}. The layer managing the dialog,
 * e.g., the request dispatcher, should already be enforcing request limits, so
 * if we get this exception something has gone wrong.
 */
public class DialogLimitExceededException extends DialogUnknownException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param limit the dialog limit
     */
    public DialogLimitExceededException(int limit) {
        super(false /* no side effect */,
              false /* not from remote */,
              String.format("Dialog limit exceeded, the limit is %d", limit),
              null /* no cause */);
    }
}

