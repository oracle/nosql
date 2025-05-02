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

import java.util.concurrent.Executor;
import java.util.logging.Logger;

import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * A dialog handler factory for use in responders (servers) that produces a
 * dialog handler given a service implementation, an executor, and a logger.
 *
 * @param <S> the type of the service
 */
public interface ResponderDialogHandlerFactory<S extends VersionedRemote> {

    /**
     * Creates a dialog handler for use in a responder.
     *
     * @param service the underlying service implementation
     * @param executor an executor to run the service operation asynchronously
     * @param logger a debug logger
     * @return the dialog handler
     */
    DialogHandler createDialogHandler(S service,
                                      Executor executor,
                                      Logger logger);
}
