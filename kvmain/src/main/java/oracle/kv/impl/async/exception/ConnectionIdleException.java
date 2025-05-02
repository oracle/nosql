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

import oracle.kv.impl.async.AsyncOption;
import oracle.kv.impl.async.dialog.ChannelDescription;

/**
 * The exception is caused by the connection timed out due to idle timeout
 * (specified by {@link AsyncOption#DLG_IDLE_TIMEOUT}).
 */
public class ConnectionIdleException extends ConnectionException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param fromRemote {@code true} if is aborted by the remote
     * @param isHandshakeDone {@code true} if the connection is aborted after
     * the handshake is done
     * @param channelDescription the description of the connection channel
     * @param message the message of the exception
     */
    public ConnectionIdleException(boolean fromRemote,
                                   boolean isHandshakeDone,
                                   ChannelDescription channelDescription,
                                   String message) {
        super(fromRemote, isHandshakeDone,
              channelDescription, message, null);
    }

    /**
     * Returns {@code false} since there is nothing wrong with the connection
     * itself.
     */
    @Override
    public boolean isPersistent() {
        return false;
    }

    /**
     * This exception is expected because closing idle connections is not
     * coordinated with other operations.
     */
    @Override
    public boolean isExpectedException() {
        return true;
    }
}
