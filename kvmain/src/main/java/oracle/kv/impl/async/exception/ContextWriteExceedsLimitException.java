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
 * This exception is thrown when the size of the request/response message of
 * {@link DialogContext#write} exceeds the limit.
 */
public class ContextWriteExceedsLimitException extends ContextWriteException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param size the message size
     * @param limit the limit
     */
    public ContextWriteExceedsLimitException(int size, int limit) {
        super(String.format(
                    "Message size too large, " +
                    "writing message size is %d, limit is %d",
                    size, limit),
                null);
    }

    /**
     * Return an {@link IllegalStateException} since this exception represents
     * an unexpected situation and does not include an underlying exception
     * cause.
     */
    @Override
    public Throwable getUserException() {
        return new IllegalStateException(getMessage(), this);
    }
}
