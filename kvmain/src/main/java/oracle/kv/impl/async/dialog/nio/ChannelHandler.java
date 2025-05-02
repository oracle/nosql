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

package oracle.kv.impl.async.dialog.nio;

import java.nio.channels.Selector;

/**
 * Handling a channel of the establishing/established connection.
 *
 * The handler is always associated with a single-thread channel executor. The
 * handler stays in a loop of the channel executor thread with the following
 * steps:
 * <ol>
 * <li>{@link Selector#select}</li>
 * <li>If some interested operations are ready for the handler</li>
 *      <ol>
 *      <li>call {@link #onSelected}</li>
 *      <li>call {@link #onRead} and/or {@link #onWrite}</li>
 *      <li>call {@link #onProcessed}</li>
 *      </ol>
 * <li>Run other scheduled tasks</li>
 * </ol>
 */
interface ChannelHandler extends NioHandler {

    /**
     * Called when the connection is established.
     */
    void onConnected();

    /**
     * Called when the channel is ready for read.
     */
    void onRead();

    /**
     * Called when the channel is ready for write
     */
    void onWrite();

    /**
     * Called when the channel is selected for ready ops.
     */
    default void onSelected() { }

    /**
     * Called when the channel is processed for ready ops.
     */
    default void onProcessed() { }
}
