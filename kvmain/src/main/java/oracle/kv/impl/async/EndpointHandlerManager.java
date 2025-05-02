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

import oracle.kv.impl.async.perf.DialogEndpointGroupPerfTracker;

/**
 * Manages {@linkplain EndpointHandler endpoint handlers}.
 */
public interface EndpointHandlerManager {

    /**
     * Called when an endpoint handler is shut down.
     *
     * <p>There are two causes of the shutdown: (1) the manager is shutdown
     * which shuts down the handlers and invokes this callback when the handler
     * is terminated; (2) there is an error happened to the handler which
     * invokes this callback.
     *
     * <p>The implmenetation must expect this method being called more than
     * once, e.g., the handler is shutting down itself due to an error and in
     * the mean time the manager is shutting down the handler as well.
     *
     * @param handler the endpoint handler
     */
    void onHandlerShutdown(EndpointHandler handler);

    /**
     * Returns the endpoint group perf tracker.
     *
     * @return the endpoint group perf tracker
     */
    DialogEndpointGroupPerfTracker getEndpointGroupPerfTracker();

    /**
     * Shuts down this manager and the associated endpoint
     * handlers.
     *
     * @param detail the description of the termination.
     * @param force {@code true} if shuts down the handlers forcefully
     */
    void shutdown(String detail, boolean force);
}
