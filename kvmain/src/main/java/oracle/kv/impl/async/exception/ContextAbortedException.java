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

import oracle.kv.impl.async.DialogContext;

/**
 * This exception is thrown when {@link DialogContext#read} or {@link
 * DialogContext#write} is called after dialog is aborted.
 */
public class ContextAbortedException extends ContextException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param cause the cause of the exception
     */
    public ContextAbortedException(Throwable cause) {
        super("The dialog and the context is already aborted.", cause);
    }

    /**
     * Returns the cause, if it is non-null, or else an {@link
     * IllegalStateException} since this exception represents an unexpected
     * situation.
     */
    @Override
    public Throwable getUserException() {
        final Throwable cause = getCause();
        return (cause != null) ?
            cause :
            new IllegalStateException(getMessage(), this);
    }

    /**
     * This exception is unexpected because it represents an unexpected
     * situation.
     */
    @Override
    public boolean isExpectedException() {
        return false;
    }
}

