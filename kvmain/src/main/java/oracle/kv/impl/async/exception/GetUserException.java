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

import java.io.IOException;

import oracle.kv.FaultException;

/**
 * An interface implemented by internal async exceptions so that they can be
 * presented as user-visible exceptions. The idea is to convert the internal
 * exception types into public exceptions, either {@link IOException}s, for
 * network problems, or {@link RuntimeException}s, typically for internal
 * problems.
 */
public interface GetUserException {

    /**
     * Returns an exception to supply to users in place of this internal
     * exception. The returned exception should either be an {@link Exception}
     * or an {@link Error} defined as part of Java. An internal exception that
     * implements this interface, or another non-public exception, can appear
     * as the cause of the returned exception: it is only the top-level
     * exception that must be public. The exception should be either an {@link
     * IOException}, for network problems, or a {@link RuntimeException},
     * typically for internal problems. The caller will wrap any {@code
     * Exception} in {@link FaultException} before supplying it to users. Note
     * that {@code Error}s should be propagated unwrapped: they should not be
     * made the cause of an {@code Exception}, because we expect errors to be
     * treated specially.
     *
     * @return the associated exception to supply to users
     */
    Throwable getUserException();

    /**
     * Returns whether this is an expected or unexpected exception. An expected
     * exception represents a failure that can happen during normal operation
     * due to the sorts of failures that are expected in distributed systems.
     * Examples include I/O exceptions due to temporary network outages or
     * connectivity failures caused by host reboots. An unexpected exception
     * represents a failure that suggests a software bug, or some situation
     * that developers thought should not happen.
     *
     * <p>The default implementation returns true if and only if the associated
     * user exception is an {@link IOException}, which represents a network
     * failure.
     *
     * @return whether this exception is expected
     */
    default boolean isExpectedException() {
        return getUserException() instanceof IOException;
    }

    /**
     * Returns an exception to supply to users in place of an arbitrary
     * exception, calling {@link #getUserException()} if the argument
     * implements {@link GetUserException}.
     *
     * @param t the exception
     * @return the exception to supply to users
     */
    static Throwable getUserException(Throwable t) {
        return (t instanceof GetUserException) ?
            ((GetUserException) t).getUserException() :
            t;
    }
}
