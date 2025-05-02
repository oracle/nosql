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

import oracle.kv.impl.async.dialog.ChannelDescription;

/**
 * The exception is caused by some unknwon fault.
 */
public class ConnectionUnknownException extends ConnectionException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception caused by local fault.
     *
     * @param isHandshakeDone {@code true} if the connection is aborted after
     * the handshake is done
     * @param channelDescription the description of the connection channel
     * @param cause the cause of the exception
     */
    public ConnectionUnknownException(boolean isHandshakeDone,
                                      ChannelDescription channelDescription,
                                      Throwable cause) {
        super(false /* not remote */,
              isHandshakeDone,
              channelDescription,
              cause.getMessage(),
              cause);
    }

    /**
     * Constructs the exception reported by remote.
     *
     * @param isHandshakeDone {@code true} if the connection is aborted after
     * the handshake is done
     * @param channelDescription the description of the connection channel
     * @param message the message of the exception
     */
    public ConnectionUnknownException(boolean isHandshakeDone,
                                      ChannelDescription channelDescription,
                                      String message) {
        super(true /* remote */,
              isHandshakeDone,
              channelDescription,
              message,
              null /* no cause */);
    }

    /**
     * Returns {@code true} since we do not know what happens.
     */
    @Override
    public boolean isPersistent() {
        return true;
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
     * This exception is unexpected because the cause of the problem is
     * unknown.
     */
    @Override
    public boolean isExpectedException() {
        return false;
    }
}
