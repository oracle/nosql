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

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import oracle.kv.impl.async.exception.ConnectionNotEstablishedException;
import oracle.kv.impl.async.perf.DialogEndpointGroupPerfTracker;
import oracle.kv.impl.util.EventTrackersManager;
import oracle.kv.impl.util.HostPort;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Manages a group of endpoints on a host.
 *
 * <p>The endpoint group may create new endpoints and enable listening for new
 * connections.
 */
public interface EndpointGroup {

    /**
     * Returns a creator endpoint.
     *
     * <p>If there is an existing creator endpoint with an equivalent remote
     * address and endpoint configuration, the endpoint is shared. Otherwise, a
     * new endpoint will be created.
     *
     * @param perfName the name for performance monitoring
     * @param remoteAddress the network address of the remote endpoint
     * @param localAddress the network address of the local endpoint, or null
     * to use the default address
     * @param endpointConfig the endpoint configuration
     *
     * @return the endpoint
     */
    CreatorEndpoint
        getCreatorEndpoint(String perfName,
                           NetworkAddress remoteAddress,
                           @Nullable NetworkAddress localAddress,
                           EndpointConfig endpointConfig);

    /**
     * Returns a responder endpoint.
     *
     * <p>If there is an existing responder endpoint with an equivalent remote
     * address and listener configuration, the endpoint is shared. Otherwise,
     * the method returns an endpoint that always fails dialogs submitted to it
     * with {@link ConnectionNotEstablishedException}.
     *
     * @param remoteAddress the network address of the remote endpoint
     * @param listenerConfig the listener configuration
     *
     * @return the responder endpoint
     */
    ResponderEndpoint
        getResponderEndpoint(NetworkAddress remoteAddress,
                             ListenerConfig listenerConfig);

    /**
     * Listens for incoming async connections to respond to the specified type
     * of dialogs.
     *
     * <p>If there already exists a listening channel with an equivalent {@link
     * ListenerConfig}, the listening channel is shared (for both async and
     * sync). Otherwise, a new listening channel will be created.
     *
     * <p>Any current or future endpoints created by the listening channel are
     * enabled to respond to the specified type of dialog.
     *
     * @param listenerConfig the listener configuration
     * @param dialogType the dialog type of the handler to create
     * @param handlerFactory the factory to create the handler
     * @throws IOException if there is an I/O error
     * @return the listen handle
     */
    ListenHandle listen(ListenerConfig listenerConfig,
                        int dialogType,
                        DialogHandlerFactory handlerFactory)
        throws IOException;

    /**
     * Listens for incoming sync connections.
     *
     * <p>If there already exists a listening channel with an equivalent {@link
     * ListenerConfig}, the listening channel is shared (for both async and
     * sync). Otherwise, a new listening channel will be created.
     *
     * @param listenerConfig the listener configuration
     * @param handler the handler for when the socket is prepared
     * @throws IOException if there is an I/O error
     * @return the listen handle
     */
    ListenHandle listen(ListenerConfig listenerConfig,
                        SocketPrepared handler)
        throws IOException;

    /**
     * Shuts down all endpoints and channels in the group.
     *
     * @param force {@code false} if shut down gracefully.
     */
    void shutdown(boolean force);

    /**
     * Returns whether the endpoint group has started to shutdown.
     *
     * @return whether the endpoint group has started to shutdown
     */
    boolean getIsShutdown();

    /**
     * Returns one of the executor services associated with this group.
     *
     * @return the executor service
     */
    ScheduledExecutorService getSchedExecService();

    /**
     * Returns the back-up executor services associated with this group.
     *
     * @return the executor service
     */
    ScheduledExecutorService getBackupSchedExecService();

    /**
     * Returns the performance tracker.
     *
     * @return the perf tracker
     */
    DialogEndpointGroupPerfTracker getDialogEndpointGroupPerfTracker();

    /**
     * A handle for shutting down the listening operation for the {@link
     * ListenerConfig} with the dialog type.
     */
    public interface ListenHandle {

        /**
         * Gets the listener configuration bound to this handle.
         *
         * @return the listener configuration
         */
        ListenerConfig getListenerConfig();

        /**
         * Gets the local HostPort for the handle.
         *
         * @return the host-port the listener is listening on
         */
        HostPort getLocalAddress();

        /**
         * Disables responding to a dialog type for both future and current
         * connections.
         *
         * If the disabled dialog type is the last one, this method stops the
         * listening channel for the associated with the listener.
         *
         * If force equals to true, the existing connections accepted by the
         * listener are terminated when the method returns.
         *
         * If force equals to false, the existing connections accepted by the
         * listener are marked as gracefully shutdown such that existing
         * dialogs are allowed to finish, but new dialogs are not allowed.
         *
         * @param force {@code false} if existing dialogs on established
         * connections are allowed to finish.
         */
        void shutdown(boolean force);

        /**
         * Sets the maximum number of allowed active connections for accept.
         */
        void setAcceptMaxActiveConnections(int nconn);
    }

    /**
     * Returns the dialog resource manager for concurrent dialogs.
     */
    DialogResourceManager getDialogResourceManager();

    /**
     * Returns the event trackers manager.
     */
    EventTrackersManager getEventTrackersManager();
}
