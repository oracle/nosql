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
 * A client-side endpoint that is defined by the remote address and the channel
 * factory.
 *
 * The endpoint supports starting dialogs and responding to a specific dialog
 * type if enabled.
 *
 * Subclasses should define content-based equals and hashCode methods.
 */
public interface CreatorEndpoint {
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
     * Enables responding to a dialog type.
     *
     * <p>The method enables responding to a dialog type on current and future
     * established connections.
     *
     * <p>Note that there might not be any connection established at the time
     * of the method call, and this method does not attempt to establish a
     * connection. Therefore, if there is no connection when the method
     * returns, the enabling will only take effect after the next successful
     * {@code startDialog} call.
     *
     * @param dialogType the dialog type
     * @param factory the dialog handler factory
     */
    void enableResponding(int dialogType, DialogHandlerFactory factory);

    /**
     * Disables responding to a dialog type.
     *
     * <p>The method has no effect if the dialog type is already disabled.
     *
     * @param dialogType the dialog type
     */
    void disableResponding(int dialogType);

    /**
     * Returns the endpoint group.
     */
    EndpointGroup getEndpointGroup();

    /**
     * Returns the network address of the remote endpoint.
     *
     * @return the remote network address
     */
    NetworkAddress getRemoteAddress();

    /**
     * Returns the configuration of the endpoint.
     *
     * @return the configuration
     */
    EndpointConfig getEndpointConfig();

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
