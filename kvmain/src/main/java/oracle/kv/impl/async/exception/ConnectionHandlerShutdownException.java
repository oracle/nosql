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

import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.dialog.ChannelDescription;

/**
 * This exception is thrown when {@link CreatorEndpoint#startDialog} is called
 * but the underlying handler of the connection is being terminated for some
 * reason.
 */
public class ConnectionHandlerShutdownException extends ConnectionException {

    private static final String MESSAGE = "Connection handler is unavailable";
    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     * @param channelDescription the description of the connection channel
     */
    public ConnectionHandlerShutdownException(
        ChannelDescription channelDescription)
    {
        super(false /* not from remote */,
              false /* handshake not done */,
              channelDescription,
              MESSAGE,
              null /* no cause */);
    }

    /**
     * Returns {@code false} since a new connection handler will be created the
     * next time we start a dialog.
     */
    @Override
    public boolean isPersistent() {
        return false;
    }

    /**
     * Checks for side effect.
     */
    @Override
    public void checkSideEffect(boolean hasSideEffect) {
        if (hasSideEffect) {
            throw new IllegalArgumentException(
                "This exception should have no side effects");
        }
    }

    /**
     * This exception is expected since the shutdown may not be coordinated
     * with other operations.
     */
    @Override
    public boolean isExpectedException() {
        return true;
    }
}
