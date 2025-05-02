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

import oracle.kv.impl.async.DialogHandler;

/**
 * Instances of this exception class are presented to {@link
 * DialogHandler#onAbort}.  Note that classes for all instantiated instances of
 * this class should inherit from either {@link PersistentDialogException} or
 * {@link TemporaryDialogException}.
 */
public abstract class DialogException extends RuntimeException
        implements GetUserException {

    private static final long serialVersionUID = 1L;

    private final boolean hasSideEffect;
    private final boolean fromRemote;

    /**
     * Constructs the exception.
     *
     * @param hasSideEffect {@code true} if the dialog incurs any side effect
     * on the remote
     * @param fromRemote {@code true} if the exception is reported from the
     * remote
     * @param message the message of the exception
     * @param cause the cause of the exception
     */
    protected DialogException(boolean hasSideEffect,
                              boolean fromRemote,
                              String message,
                              Throwable cause) {
        super(message, cause);
        this.hasSideEffect = hasSideEffect;
        this.fromRemote = fromRemote;
    }

    /**
     * Returns {@code true} if the problem is persistent.
     *
     * @see ConnectionException#isPersistent
     */
    public abstract boolean isPersistent();

    /**
     * Returns whether the dialog incurs any side effect on the remote.
     *
     * @return {@code true} if there is side effect
     */
    public boolean hasSideEffect() {
        return hasSideEffect;
    }

    /**
     * Returns whether the exception is reported by the remote.
     *
     * @return {@code true} if from remote
     */
    public boolean fromRemote() {
        return fromRemote;
    }

    /**
     * Returns the non-dialog layer or connection layer exception that
     * underlies this exception, if any, or else this exception.
     */
    public Throwable getUnderlyingException() {
        Throwable cause = getCause();

        /* Return the underlying cause if it is not the ConnectionException */
        if (cause instanceof ConnectionException) {
            cause = cause.getCause();
        }
        return (cause != null) ? cause : this;
    }

    /**
     * Return the user exception from the cause if the cause also implements
     * {@link GetUserException}, the cause if it is an {@link IOException} or
     * an {@link Error}, and otherwise an {@link IllegalStateException} since
     * other cases are not expected.
     */
    @Override
    public Throwable getUserException() {
        final Throwable cause = getCause();
        if (cause instanceof GetUserException) {
            return ((GetUserException) cause).getUserException();
        }
        if ((cause instanceof IOException) || (cause instanceof Error)) {
            return cause;
        }
        return new IllegalStateException(getMessage(), this);
    }

    /**
     * Return whether the exception is expected from the cause if the cause
     * also implements {@link GetUserException}.
     *
     * TODO: we probably should look at the semantics of expected error and
     * error handling in general again. [KVSTORE-441], [KVSTORE-458],
     * [KVSTORE-468].
     */
    @Override
    public boolean isExpectedException() {
        final Throwable cause = getCause();
        if (cause instanceof GetUserException) {
            return ((GetUserException) cause).isExpectedException();
        }
        return GetUserException.super.isExpectedException();
    }
}

