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

import oracle.kv.RequestTimeoutException;

/**
 * A server-side endpoint that is defined by the remote hostname and the
 * listener channel factory.
 *
 * <p>The endpoint manages all the connections that are created by the listener
 * channel factory and connect to the remote host. The endpoint supports
 * starting dialogs if there is already a connection established.
 *
 * <p>This endpoint allows server side to initiate a dialog when the connection
 * is available. One use case is that a client sends a request to RN1, which
 * forwards the request to RN2; instead of sending the response back to RN1,
 * RN2 can directly send the response to the client if there is already a
 * connection.
 *
 * <p>To support this direct response mechanism, the desired endpoint can be
 * acquired from {@link EndpointGroup#getResponderEndpoint} with the desired
 * {@link NetworkAddress} and {@link ListenerConfig}. However, the upper layer
 * must have the knowledge of which {@link NetworkAddress} sets belongs to the
 * same clients. One way to do it is to piggyback a client ID with each request
 * and the server extract such information and maintains mapping of the client
 * IDs and network addresses.
 */
public interface ResponderEndpoint {

    /**
     * Starts a dialog.
     *
     * <p>The {@link DialogHandler#onStart} method will be called, possibly
     * inside this method or in the future. The calling layer should rely on
     * dialog handler callback methods (e.g., {@link DialogHandler#onAbort}) to
     * determine the state of the dialog.
     *
     * <p>The dialog will be alive for at most the specified {@code
     * timeoutMillis}, after which it will be aborted and {@link
     * DialogHandler#onAbort} will be called with {@link
     * RequestTimeoutException}.
     *
     * @param dialogType the dialog type
     * @param dialogHandler the dialog handler
     * @param timeoutMillis the timeout interval for keeping the dialog alive,
     * zero if no timeout
     */
    void startDialog(int dialogType,
                     DialogHandler dialogHandler,
                     long timeoutMillis);

    /**
     * Returns the endpoint group.
     */
    EndpointGroup getEndpointGroup();

    /**
     * Returns the address of the remote endpoint.
     *
     * @return the remote host name
     */
    NetworkAddress getRemoteAddress();

    /**
     * Returns the listener configuration of the listener that accepted the
     * endpoint.
     *
     * @return the listener configuration
     */
    ListenerConfig getListenerConfig();

    /**
     * Returns the limit on the number of dialogs this endpoint can
     * concurrently start.
     *
     * The limit is negotiated between this endpoint and its peer based on the
     * their {@link EndpointConfig}. Calling {@link #startDialog} after reaching
     * the limit will cause the dialog being aborted immediately.
     *
     * @return the limit, -1 if the limit is not negotiated yet
     */
    int getNumDialogsLimit();
}
