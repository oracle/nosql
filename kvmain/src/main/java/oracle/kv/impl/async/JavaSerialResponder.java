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
import java.util.function.BiConsumer;
import java.util.logging.Logger;

import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.async.AsyncVersionedRemote.MethodOp;
import oracle.kv.impl.async.JavaSerialMethodTable.JavaSerialMethodCall;
import oracle.kv.impl.util.registry.VersionedRemote;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A responder (server) for async services that use {@link
 * JavaSerialMethodTable} to map methods.
 */
public class JavaSerialResponder<S extends VersionedRemote>
        extends AsyncVersionedRemoteDialogResponder {

    private final S server;
    private final Executor executor;
    private final JavaSerialMethodTable methodTable;

    public JavaSerialResponder(S server,
                               Class<S> serviceInterface,
                               Executor executor,
                               DialogTypeFamily dialogTypeFamily,
                               Logger logger) {
        super(dialogTypeFamily, logger);
        this.server = server;
        this.methodTable = JavaSerialMethodTable.getTable(serviceInterface);
        this.executor = executor;
    }

    @Override
    protected MethodOp getMethodOp(int methodOpValue) {
        return methodTable.getMethodOp(methodOpValue);
    }

    @Override
    protected void handleRequest(final short serialVersion,
                                 final MethodCall<?> methodCall,
                                 final long timeoutMs,
                                 final DialogContext context) {
        handleRequest(serialVersion, methodCall, context, server, executor,
                      this);
    }

    /**
     * Handle an async request by using the executor to call the appropriate
     * method on the associated RMI server.
     *
     * @param <S> the type of the RMI server
     * @param serialVersion the serial version for the call
     * @param methodCall the object representing the call
     * @param context the dialog context for the call
     * @param server the RMI server
     * @param executor an executor to execute the RMI server call
     * @param responder the responder to use to send the result
     */
    public static <S extends VersionedRemote>
        void handleRequest(short serialVersion,
                           MethodCall<?> methodCall,
                           DialogContext context,
                           S server,
                           Executor executor,
                           AsyncVersionedRemoteDialogResponder responder) {
        final JavaSerialMethodCall javaSerialMethodCall =
            (JavaSerialMethodCall) methodCall;
        executor.execute(() ->
                         withThreadDialogContext(
                             context,
                             () -> callServer(serialVersion,
                                              javaSerialMethodCall,
                                              server,
                                              responder)));
    }

    private static <S extends VersionedRemote>
        @Nullable
        Void callServer(short serialVersion,
                        JavaSerialMethodCall methodCall,
                        S server,
                        AsyncVersionedRemoteDialogResponder responder)
    {
        final BiConsumer<Object, Throwable> responseConsumer =
            responder.getResponseConsumer(serialVersion, methodCall);
        try {
            responseConsumer.accept(
                methodCall.callService(serialVersion, server), null);
        } catch (Throwable t) {
            responseConsumer.accept(null, t);
        }
        return null;
    }
}
