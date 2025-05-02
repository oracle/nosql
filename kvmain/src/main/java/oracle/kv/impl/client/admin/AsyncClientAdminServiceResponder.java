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

package oracle.kv.impl.client.admin;

import static oracle.kv.impl.async.FutureUtils.unwrapExceptionVoid;
import static oracle.kv.impl.async.StandardDialogTypeFamily.CLIENT_ADMIN_SERVICE_TYPE_FAMILY;

import java.util.concurrent.Executor;
import java.util.logging.Logger;

import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.async.AsyncVersionedRemoteDialogResponder;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.VersionedRemoteAsyncServerImpl;
import oracle.kv.impl.client.admin.AsyncClientAdminService.ServiceMethodCall;
import oracle.kv.impl.client.admin.AsyncClientAdminService.ServiceMethodOp;

/**
 * A responder (server-side) dialog handler for {@link
 * AsyncClientAdminService}.
 */
public class AsyncClientAdminServiceResponder
        extends AsyncVersionedRemoteDialogResponder {

    private final AsyncClientAdminService server;

    public AsyncClientAdminServiceResponder(ClientAdminService server,
                                            Executor executor,
                                            Logger logger) {
        super(CLIENT_ADMIN_SERVICE_TYPE_FAMILY, logger);
        this.server = VersionedRemoteAsyncServerImpl.createProxy(
            AsyncClientAdminService.class, server, executor, logger);
    }

    @Override
    protected MethodOp getMethodOp(int methodOpValue) {
        return ServiceMethodOp.valueOf(methodOpValue);
    }

    @Override
    protected void handleRequest(final short serialVersion,
                                 final MethodCall<?> methodCall,
                                 final long timeoutMs,
                                 final DialogContext context) {
        makeCall(serialVersion, (ServiceMethodCall<?>) methodCall,
                 timeoutMs, context);
    }

    private <R, M extends ServiceMethodCall<R>>
        void makeCall(final short serialVersion,
                      final M call,
                      final long timeoutMs,
                      final DialogContext context) {
        withThreadDialogContext(context,
                                () -> call.callService(
                                    serialVersion, timeoutMs, server))
            .whenComplete(
                unwrapExceptionVoid(getResponseConsumer(serialVersion, call)));
    }
}
