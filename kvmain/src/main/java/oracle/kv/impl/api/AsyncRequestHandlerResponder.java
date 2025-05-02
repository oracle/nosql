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

import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;

import java.util.logging.Logger;

import oracle.kv.impl.api.AsyncRequestHandler.GetSerialVersionCall;
import oracle.kv.impl.api.AsyncRequestHandler.RequestMethodOp;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.StandardDialogTypeFamily;

/**
 * A responder (server-side) dialog handler for {@link AsyncRequestHandler}
 * dialogs.
 *
 * @see AsyncRequestHandler
 */
public class AsyncRequestHandlerResponder
        extends AsyncVersionedRemoteDialogResponder {

    private final AsyncRequestHandler server;

    public AsyncRequestHandlerResponder(AsyncRequestHandler server,
                                        Logger logger) {
        super(StandardDialogTypeFamily.REQUEST_HANDLER_TYPE_FAMILY, logger);
        this.server = server;
    }

    @Override
    protected MethodOp getMethodOp(int methodOpVal) {
        return RequestMethodOp.valueOf(methodOpVal);
    }

    @Override
    protected void handleRequest(final short serialVersion,
                                 final MethodCall<?> methodCall,
                                 final long timeoutMillis,
                                 final DialogContext context) {
        try {
            final RequestMethodOp methodOp =
                (RequestMethodOp) methodCall.getMethodOp();
            switch (methodOp) {
            case GET_SERIAL_VERSION:
                getSerialVersion(serialVersion,
                                 (GetSerialVersionCall) methodCall,
                                 timeoutMillis, server);
                break;
            case EXECUTE:
                execute(serialVersion, (Request) methodCall, context);
                break;
            default:
                throw new IllegalArgumentException(
                    "Unexpected method op: " + methodOp);
            }
        } catch (Throwable e) {
            sendException(e, serialVersion);
        }
    }

    private void execute(final short serialVersion,
                         final Request request,
                         final DialogContext context) {

        /*
         * There isn't an easy way to pass the dialog context to the server
         * execute implementation, so store it in thread local
         */
        withThreadDialogContext(context,
                                () -> server.execute(
                                    request, request.getTimeout()))
            .whenComplete(
                unwrapExceptionVoid(
                    getResponseConsumer(serialVersion, request)));
    }
}
