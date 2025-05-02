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
import java.util.concurrent.RejectedExecutionException;

import oracle.kv.impl.async.exception.ConnectionException;

/**
 * Represents a handler for an nio channel and interacts with {@link
 * NioChannelExecutor}.
 */
interface NioHandler {

    /**
     * Signals the handler to terminate.
     *
     * <p>This method is used by the {@link NioChannelExecutor} to notify the
     * handler that errors have happened when processing events directed to the
     * handler.
     */
    void cancel(Throwable t);

    /**
     * Rethrows an error occurred while processing an IO event.
     *
     * <p>A common routine for the event handler to throw out errors that it
     * cannot handle.
     *
     * @param error the thrown error
     */
    default void rethrowUnhandledError(Throwable error) {

        /* IOException is expected due to network operation */
        if (error instanceof IOException) {
            return;
        }

        /* ConnectionException is expected due to network operation */
        if (error instanceof ConnectionException) {
            return;
        }

        /*
         * RejectedExecutionException is expected due to the idling channel
         * executor being shutdown
         */
        if (error instanceof RejectedExecutionException) {
            return;
        }

        /*
         * Java Error class indicates a serious problem that the process should
         * not continue
         */
        if (error instanceof Error) {
            throw (Error) error;
        }

        /*
         * IllegalStateException indicates an illegal state such as coding error
         * which has undefined behavior.
         */
        if (error instanceof IllegalStateException) {
            throw (IllegalStateException) error;
        }

        throw new RuntimeException(error);
    }
}
