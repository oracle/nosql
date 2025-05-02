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

package oracle.kv.impl.api;

import java.util.logging.Logger;
import java.util.concurrent.CompletableFuture;

import oracle.kv.impl.async.AsyncVersionedRemoteInitiator;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.StandardDialogTypeFamily;

/**
 * An initiator (client-side) implementation of {@link AsyncRequestHandler}.
 *
 * @see AsyncRequestHandlerAPI
 */
public class AsyncRequestHandlerInitiator extends AsyncVersionedRemoteInitiator
    implements AsyncRequestHandler {

    public AsyncRequestHandlerInitiator(CreatorEndpoint endpoint,
                                        DialogType dialogType,
                                        Logger logger) {
        super(endpoint, dialogType, logger);
        if (dialogType.getDialogTypeFamily() !=
            StandardDialogTypeFamily.REQUEST_HANDLER_TYPE_FAMILY) {
            throw new IllegalArgumentException(
                "Dialog type should have dialog type family" +
                " ASYNC_REQUEST_HANDLER, found: " +
                dialogType.getDialogTypeFamily());
        }
    }

    @Override
    protected GetSerialVersionCall getSerialVersionCall() {
        return new GetSerialVersionCall();
    }

    @Override
    public CompletableFuture<Response> execute(final Request request,
                                               final long timeoutMillis) {
        return startDialog(request.getSerialVersion(), request, timeoutMillis);
    }
}
