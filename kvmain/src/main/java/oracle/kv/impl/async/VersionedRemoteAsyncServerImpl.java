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

import static java.util.stream.Collectors.joining;
import static oracle.kv.impl.async.FutureUtils.checked;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.unwrapException;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import java.util.stream.Stream;

import oracle.kv.impl.util.AbstractInvocationHandler;
import oracle.kv.impl.util.registry.VersionedRemote;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A utility class for implementing an async versioned remote server using a
 * dynamic proxy that makes corresponding calls to an RMI versioned remote
 * server. Use this class to build server-side support for an async network
 * interface interface on top of an RMI server implementation.
 *
 * To support reflection, the class of the server needs to be public so that
 * the server method can be called reflectively from code in another package.

 * @param <T> the type of the async versioned remote interface
 */
public class VersionedRemoteAsyncServerImpl<T extends AsyncVersionedRemote>
        extends AbstractInvocationHandler {

    private static final Class<?>[] EMPTY_CLASSES = { };

    private final VersionedRemote rmiServer;
    private final Executor executor;
    /* Keep this around so we can add logging easily */
    @SuppressWarnings("unused")
    private final Logger logger;
    private final Map<Method, Method> methodMap = new ConcurrentHashMap<>();

    private VersionedRemoteAsyncServerImpl(VersionedRemote rmiServer,
                                           Executor executor,
                                           Logger logger) {
        this.rmiServer = rmiServer;
        this.executor = executor;
        this.logger = logger;
    }

    /**
     * Returns a dynamic proxy that implements the specified async versioned
     * remote interface by making corresponding calls to an RMI versioned
     * remote server in a separate thread and returning a future for obtaining
     * the result. This method makes it possible to implement an async server
     * interface using the associated RMI server.
     *
     * @param <T> the type of the async versioned remote interface
     * @param asyncInterface the async versioned remote interface
     * @param rmiServer the associated RMI server
     * @param executor for executing server operations
     * @param logger for logging
     * @return a dynamic proxy that implements the async versioned remote
     * interface
     */
    public static <T extends AsyncVersionedRemote>
        T createProxy(Class<T> asyncInterface,
                      VersionedRemote rmiServer,
                      Executor executor,
                      Logger logger)
    {
        return asyncInterface.cast(
            Proxy.newProxyInstance(
                rmiServer.getClass().getClassLoader(),
                new Class<?>[] { asyncInterface },
                new VersionedRemoteAsyncServerImpl<T>(rmiServer, executor,
                                                      logger)));
    }

    @Override
    protected Object invokeNonObject(Method method,
                                     Object @Nullable[] args) {
        return executeServer(
            methodMap.computeIfAbsent(method, this::makeHandler),
            checkNull("args", args));
    }

    private Method makeHandler(Method method) {
        final Class<?>[] params = method.getParameterTypes();
        if (params.length < 2) {
            throw new IllegalArgumentException(
                "AsyncVersionedRemote method must have at least two" +
                " parameters");
        }
        final Class<?>[] newParams;
        if ("getSerialVersion".equals(method.getName())) {

            /* VersionedRemote.getSerialVersion has no parameters */
            newParams = EMPTY_CLASSES;
        } else {

            /*
             * Otherwise move remove timeoutMillis from the end and move
             * serialVersion from first to last
             */
            newParams = new Class<?>[params.length - 1];
            newParams[params.length - 2] = params[0];
            if (params.length > 2) {
                System.arraycopy(params, 1, newParams, 0, params.length - 2);
            }
        }

        try {
            return rmiServer.getClass()
                .getMethod(method.getName(), newParams);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                "Method " + method.getName() +
                Stream.of(newParams).map(Object::toString)
                .collect(joining(", ", "(", ")")) +
                " not found when attempting to convert method " + method +
                " to a method on server " + rmiServer,
                e);
        }
    }

    private CompletableFuture<Object> executeServer(Method handler,
                                                    Object[] args) {
        final Object[] newArgs;
        if ("getSerialVersion".equals(handler.getName())) {
            newArgs = null;
        } else {

            /*
             * Remove timeoutMillis from the end and move serialVersion from
             * first to last
             */
            newArgs = new Object[args.length - 1];
            newArgs[args.length - 2] = args[0];
            if (args.length > 2) {
                System.arraycopy(args, 1, newArgs, 0, args.length - 2);
            }
        }
        try {
            final DialogContext context =
                AsyncVersionedRemoteDialogResponder.getThreadDialogContext();
            final CompletableFuture<Object> future = new CompletableFuture<>();
            executor.execute(() -> callServer(handler, newArgs, context,
                                              future));
            return future;
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    private void callServer(Method handler,
                            Object @Nullable[] newArgs,
                            @Nullable DialogContext context,
                            CompletableFuture<Object> future) {
        try {
            future.complete(
                AsyncVersionedRemoteDialogResponder.withThreadDialogContext(
                    context,
                    checked(() -> invokeMethod(rmiServer, handler, newArgs))));
        } catch (Throwable e) {
            future.completeExceptionally(unwrapException(e));
        }
    }

    @Override
    public String toString() {
        return String.format("VersionedRemoteAsyncServerImpl[%s]",
                             rmiServer);
    }

    @Override
    public int hashCode() {
        return rmiServer.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object object) {
        if (!(object instanceof VersionedRemoteAsyncServerImpl)) {
            return false;
        }
        final VersionedRemoteAsyncServerImpl<?> other =
            (VersionedRemoteAsyncServerImpl<?>) object;
        return rmiServer.equals(other.rmiServer);
    }
}
