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

import java.util.concurrent.ScheduledExecutorService;

import oracle.kv.RequestTimeoutException;

/**
 * Represents the handler for an endpoint which handles for dialog contexts and
 * wire protocol.
 *
 * <p>The endpoint handler is also managed by endpoint handler managers.
 */
public interface EndpointHandler {

    /**
     * Returns the network address of the remote endpoint.
     *
     * @return the remote network address
     */
    NetworkAddress getRemoteAddress();

    /**
     * Returns the executor service associated with this context.
     *
     * @return the executor service
     */
    ScheduledExecutorService getSchedExecService();

    /**
     * Returns the statistically universal unique ID for the handler.
     *
     * @return the UUID
     */
    long getUUID();

    /**
     * Returns the statistically universal unique ID for the connection.
     *
     * @return the connection UUID, zero if not assigned yet
     */
    long getConnID();

    /**
     * Returns a string format of the handler ID.
     *
     * @return the string ID of the handler
     */
    String getStringID();

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

    /**
     * Shuts down the handler.
     *
     * <p>The handler will not start new dialog after this method returns.
     *
     * <p>If {@code force} is {@code false}, the handler shuts down gracefully,
     * i.e., currently active dialogs are allowed to run. If there is no active
     * dialogs, then the handler notifies the remote side of the shutdown
     * event, after which the handler is shut down and resources are cleaned
     * up.
     *
     * <p>If {@code force} is {@code true}, the handler is shuts down immediately,
     * i.e., all active dialogs are aborted. The handler will attempt to flush
     * the messages onto the connection and notifies the remote of the shutdown
     * event, but will not wait if the operations block, after which the
     * handler is shut down and resources are cleaned up.
     *
     * @param detail the description of the termination.
     * @param force {@code true} if shutdown immediately
     */
    void shutdown(String detail, boolean force);
}
