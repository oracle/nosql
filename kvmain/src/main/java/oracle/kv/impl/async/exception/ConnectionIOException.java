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
import java.net.ConnectException;

import oracle.kv.impl.async.dialog.ChannelDescription;

/**
 * The exception is caused by IOException.
 */
public class ConnectionIOException extends ConnectionException {

    private static final long serialVersionUID = 1L;

    /**
     * Whether to backoff on connection retries.
     */
    private final boolean isPersistent;

    /**
     * Creates the exception based on the cause.
     *
     * @param isHandshakeDone {@code true} if the connection is aborted after
     * the handshake is done
     * @param channelDescription the description of the connection channel
     * @param cause the cause of the exception
     */
    public ConnectionIOException(boolean isHandshakeDone,
                                 ChannelDescription channelDescription,
                                 IOException cause) {
        this(isHandshakeDone, channelDescription, cause, false);
    }

    /**
     * Creates the exception based on the cause and specifying whether to
     * back off on connection retries.
     *
     * @param isHandshakeDone {@code true} if the connection is aborted after
     * the handshake is done
     * @param channelDescription the description of the connection channel
     * @param cause the cause of the exception
     * @param isPersistent whether to back off on connection retries
     */
    public ConnectionIOException(boolean isHandshakeDone,
                                 ChannelDescription channelDescription,
                                 IOException cause,
                                 boolean isPersistent) {
        super(false, isHandshakeDone, channelDescription,
              cause.getMessage(), cause);
        this.isPersistent = isPersistent;
    }

    /**
     * Creates the exception representing that the remote aborts the connection
     * due to IO exception. For example, the remote side may abort the
     * conneciton due to missing heartbeat.
     *
     * @param isHandshakeDone {@code true} if the connection is aborted after
     * the handshake is done
     * @param channelDescription the description of the connection channel
     * @param cause the cause of the exception
     */
    public ConnectionIOException(boolean isHandshakeDone,
                                 ChannelDescription channelDescription,
                                 String cause) {
        super(true, isHandshakeDone,
              channelDescription, cause, new IOException(cause));
        /*
         * We notify the dialog to backoff. The retionale is that since the
         * remote side usually choose to abort the connection, either because
         * itself or the network traffic is busy.
         */
        this.isPersistent = true;
    }

    @Override
    public boolean isPersistent() {
        return isPersistent;
    }

    /**
     * Add more information about the target address if the cause is a
     * {@link ConnectException}.
     */
    @Override
    public Throwable getUserException() {
        if (getCause() instanceof ConnectException) {
            final ConnectException connectException =
                new ConnectException(getMessage());
           connectException.initCause(this);
           return connectException;
        }
        return super.getUserException();
    }
}
