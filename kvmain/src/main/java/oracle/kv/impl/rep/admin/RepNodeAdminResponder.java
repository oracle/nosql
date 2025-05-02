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

package oracle.kv.impl.rep.admin;

import java.util.concurrent.Executor;
import java.util.logging.Logger;

import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.JavaSerialMethodTable;
import oracle.kv.impl.async.JavaSerialResponder;
import oracle.kv.impl.rep.admin.AsyncClientRepNodeAdmin.ServiceMethodCall;

/**
 * A responder (server-side) async dialog handler for both RepNodeAdmin and
 * AsyncClientRepNodeAdmin. Since there are two clients for this server that
 * can make requests in different formats, this server supports both.
 */
public class RepNodeAdminResponder extends AsyncClientRepNodeAdminResponder {
    private final RepNodeAdmin rmiServer;
    private final Executor executor;
    private final JavaSerialMethodTable methodTable;

    public RepNodeAdminResponder(RepNodeAdmin server,
                                 Executor executor,
                                 Logger logger) {
        super(server, executor, logger);
        rmiServer = server;
        this.executor = executor;
        methodTable = JavaSerialMethodTable.getTable(RepNodeAdmin.class);
    }

    @Override
    protected MethodOp getMethodOp(int methodOpValue) {
        final MethodOp methodOp = methodTable.getMethodOpOrNull(methodOpValue);
        return (methodOp == null) ?
            super.getMethodOp(methodOpValue) :
            methodOp;
    }

    @Override
    protected void handleRequest(final short serialVersion,
                                 final MethodCall<?> methodCall,
                                 final long timeoutMs,
                                 final DialogContext context) {
        if (methodCall instanceof ServiceMethodCall) {
            super.handleRequest(serialVersion, methodCall, timeoutMs,
                                context);
        } else {
            JavaSerialResponder.handleRequest(serialVersion, methodCall,
                                              context, rmiServer, executor,
                                              this);
        }
    }
}
