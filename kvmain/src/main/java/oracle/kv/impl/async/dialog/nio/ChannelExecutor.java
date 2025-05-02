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

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ScheduledExecutorService;

public interface ChannelExecutor extends ScheduledExecutorService {

    /**
     * Returns {@code true} when executed in the thread of the executor.
     *
     * @return {@code true} if in executor thread
     */
    boolean inExecutorThread();

    /**
     * Registers interest in socket accept operations on the channel.
     *
     * Server socket channels can register for accept interest.
     *
     * @param channel the server socket channel
     * @param handler the handler for preparing the socket channel after
     * accepting a connection
     * @throws IOException if there is an I/O error
     */
    void registerAcceptInterest(ServerSocketChannel channel,
                                ChannelAccepter handler)
        throws IOException;

    /**
     * Registers interest in socket connect operations on the channel.
     *
     * <p>The method will remove any other interest (e.g., read and write) of
     * the channel.
     *
     * @param channel the socket channel
     * @param handler the handler to call when connection is established
     * @throws IOException if there is an I/O error
     */
    void registerConnectInterest(SocketChannel channel, ChannelHandler handler)
        throws IOException;

    /**
     * Registers or clears interest in reading data from the channel.
     *
     * <p>The method will keep the state for any other interest (e.g., write
     * and connect) of the channel. The implementation must make sure that the
     * actual register procedure is executed inside the executor thread to
     * guarantee atomicity.
     *
     * @param channel the socket channel
     * @param handler the handler to call when some data is ready for read on
     * the channel
     * @param register {@code true} if should register
     */
    void setReadInterest(SocketChannel channel,
                         ChannelHandler handler,
                         boolean register)
        throws IOException;

    /**
     * Registers or clears interest in writing data on the channel.
     *
     * <p>The method will keep the state for any other interest (e.g., read and
     * connect) of the channel. The implementation must make sure that the
     * actual register procedure is executed inside the executor thread to
     * guarantee atomicity.
     *
     * @param channel the socket channel
     * @param handler the handler to call when the channel is ready for write
     * @param register {@code true} if should register
     */
    void setWriteInterest(SocketChannel channel,
                          ChannelHandler handler,
                          boolean register)
        throws IOException;

    /**
     * Registers interest for the specified set of operations on the channel.
     *
     * The parameter {@code ops} must be one of the constant values of {@link
     * SelectionKey}. The interest is set exactly as the {@code ops} specified,
     * i.e., other interests if registered are cleared.
     *
     * @param channel the socket channel
     * @param handler the handler to call when the channel is ready for write
     * @param ops the ops to set
     */
    void setInterest(SocketChannel channel, ChannelHandler handler, int ops)
        throws IOException;

    /**
     * Deregisters the channel.
     *
     * @param channel the socket channel
     */
    void deregister(SelectableChannel channel) throws IOException;
}
