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

import oracle.kv.impl.async.dialog.ChannelDescription;

/**
 * This base exception representing the cause of a connection abort.
 *
 */
public abstract class ConnectionException extends RuntimeException
        implements GetUserException {

    private static final long serialVersionUID = 1L;

    private final boolean fromRemote;
    private final boolean isHandshakeDone;
    private volatile String messagePrefix;

    /**
     * Constructs the exception.
     *
     * @param fromRemote {@code true} if is aborted by the remote
     * @param isHandshakeDone {@code true} if the connection is aborted after
     * the handshake is done
     * @param channelDescription the description of the connection channel
     * @param message the message of the exception
     * @param cause the cause of the exception
     */
    public ConnectionException(boolean fromRemote,
                               boolean isHandshakeDone,
                               ChannelDescription channelDescription,
                               String message,
                               Throwable cause) {
        super((channelDescription == null)
              ? message
              : String.format(
                  "Problem with channel (%s): ", channelDescription.get())
                + message,
              cause);
        this.fromRemote = fromRemote;
        this.isHandshakeDone = isHandshakeDone;
    }

    /**
     * Returns {@code true} if the connection is aborted by the remote
     * endpoint.
     */
    public boolean fromRemote() {
        return fromRemote;
    }

    /**
     * Returns {@code true} if the connection is aborted after handshake is
     * done.
     */
    public boolean isHandshakeDone() {
        return isHandshakeDone;
    }

    /**
     * Returns a {@link DialogException} based on this exception.
     *
     * @param hasSideEffect {@code true} if has side effect on the remote
     * @return the dialog exception
     */
    public DialogException getDialogException(boolean hasSideEffect) {
        checkSideEffect(hasSideEffect);
        if (isPersistent()) {
            return new PersistentDialogException(
                    hasSideEffect, fromRemote(), getMessage(), this);
        }
        return new TemporaryDialogException(
                hasSideEffect, fromRemote(), getMessage(), this);
    }

    /**
     * Returns the cause, if it is non-null, or else an {@link IOException}.
     */
    @Override
    public Throwable getUserException() {
        final Throwable cause = getCause();
        return (cause != null) ? cause : new IOException(getMessage(), this);
    }

    /**
     * Checks whether the hasSideEffect value is permitted for this exception.
     * For example, some connection exceptions are guaranteed to have no side
     * effects, so the caller should always specify a false value for the
     * parameter.
     *
     * @param hasSideEffect whether the exception has side effects
     * @throws IllegalArgumentException if the value of the hasSideEffect
     * argument is not permitted
     */
    public void checkSideEffect(boolean hasSideEffect) {
        if (!isHandshakeDone() && hasSideEffect) {
            throw new IllegalArgumentException(
                "should not have side effect when handshake is not done");
        }
    }

    /**
     * Returns {@code true} if the problem is persistent.
     *
     * <p>
     * This method indicates that the problem of the exception cannot recover
     * in a short time such that retry immediately is not likely to succeed.
     * The problem may be such that (1) it will never go away with the current
     * setting or (2) it will recover after a period of time. An example for
     * the first scenario is the client side does not have the necessary
     * prerequisite to communicate with the server side, e.g., a cached handler
     * holds staled information. In this case, the upper layer should
     * invalidate the cache and try another server. An example for the second
     * scenario is the server side is over-utilized at the moment. In this
     * case, the upper layer could choose to backoff a period of time or try
     * another server. Currently we do not distinguish between the two
     * scenarios.
     *
     * <p>
     * For IO exceptions that indicates a network issue, even if we are not
     * really sure about the actual cause, this method returns {@code false}.
     *
     * <p>
     * For exceptions that indicates an unknown cause, this method returns
     * {@code true}.
     *
     * <p>
     * TODO: note that this interface reflects the current design of error
     * handling in the RequestHandlerImpl in which upon an exception we either
     * retry immediately or invalidates the cache to the current RN and tries
     * the next.  We only backoff after all the RNs are retried.  We could make
     * it more flexible if in the future we intend to redesign the retry
     * mechanism. For example, the server side could report more information
     * and the retry mechanism could backoff for the current RN without
     * invalidates the cache under applicable circumstances.
     */
    public abstract boolean isPersistent();

    /**
     * Specify a prefix to include the beginning of the value returned by
     * getMessage, or null for no message prefix, which is the default.
     *
     * @param messagePrefix the message prefix or null
     */
    public void setMessagePrefix(String messagePrefix) {
        this.messagePrefix = messagePrefix;
    }

    /**
     * Include the message prefix specified by the last call to
     * setMessagePrefix, if any.
     */
    @Override
    public String getMessage() {
        final String msg = super.getMessage();
        return (messagePrefix == null) ? msg : messagePrefix + msg;
    }
}
