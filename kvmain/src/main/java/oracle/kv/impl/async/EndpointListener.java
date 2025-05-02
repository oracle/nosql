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

import oracle.kv.impl.util.HostPort;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Basic interface for endpoint group listeners.
 */
public interface EndpointListener {

    /**
     * Shuts down this listener.
     *
     * @param force whether to shutdown active dialogs
     */
    void shutdown(boolean force);

    /**
     * Returns {@code true} if is shut down.
     */
    boolean isShutdown();

    /**
     * Returns the dialog handler factories.
     */
    DialogHandlerFactoryMap getDialogHandlerFactoryMap();

    /**
     * Returns the local address.
     *
     * The method is not expected to be called before the createChannel method
     * which bounds to a local address.
     *
     * @return the local address the endpoint is listening on
     * @throws IllegalStateException if not yet bound to a local address
     */
    HostPort getLocalAddress();

    /**
     * Returns the socket prepared handler, which may be null if sync
     * connection is not supported.
     */
    @Nullable SocketPrepared getSocketPrepared();

    /**
     * Called by the transport layer when error occurred to the listening
     * channel.
     */
    void onChannelError(Throwable t, boolean channelClosed);

    /**
     * Add a responder endpoint when it is accepted.
     */
    void acceptResponderEndpoint(AbstractResponderEndpoint endpoint);

    /**
     * Remove a responder endpoint when it is shut down.
     *
     * The endpoint also shuts down when the connection is not a dialog
     * connection. That is, we do not count for active non-dialog connections
     * when we do connection management.
     */
    void removeResponderEndpoint(AbstractResponderEndpoint endpoint);
}
