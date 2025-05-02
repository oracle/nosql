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

package oracle.kv.impl.async;

/**
 * An interface for handling errors for a listening channel.
 */
public interface ListeningChannelErrorHandler {

    /**
     * Called when some error/exception occurred to the listening channel. The
     * default implementation does nothing.
     *
     * @param listenConfig the listener config used for the channel when
     * listening
     * @param cause the cause
     * @param channelClosed {@code true} if the channel is closed
     */
    default void onChannelError(ListenerConfig listenConfig,
                                Throwable cause,
                                boolean channelClosed) { }
}
