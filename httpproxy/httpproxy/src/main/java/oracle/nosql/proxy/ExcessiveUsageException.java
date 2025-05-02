/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * An exception that indicates excessive usage. This may be from some
 * kind of denial-of-service attack, bad actor, or possibly a malprogrammed
 * client.
 * Exceptions of this type will always wrap a RequestException as its cause.
 */
public class ExcessiveUsageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /* there are no constructors without a RequestException to wrap */
    public ExcessiveUsageException(String msg,
                                   @NonNull RequestException cause) {
        /* create a throwable with no stack trace */
        super(msg, cause, true, false);
    }

    public RequestException getRequestException() {
        return (RequestException)getCause();
    }

    /* TODO: future: possible integer codes, other info? */

    /* disable stack traces for ExcessiveUsageExceptions */
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
