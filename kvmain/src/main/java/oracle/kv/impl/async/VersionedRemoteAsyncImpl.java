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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static oracle.kv.KVStoreConfig.DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import oracle.kv.impl.util.AbstractInvocationHandler;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.VersionedRemote;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A utility class for implementing a RMI versioned remote interface using a
 * dynamic proxy that makes corresponding calls to an async versioned remote
 * server. Use this class to build client-side support for an RMI interface on
 * top of an async network server.
 *
 * To support reflection, the class of the server needs to be public so that
 * the server method can be called reflectively from code in another package.
 *
 * @param <T> the type of the RMI versioned remote interface
 */
public class VersionedRemoteAsyncImpl<T extends VersionedRemote>
        extends AbstractInvocationHandler {

    private final AsyncVersionedRemote asyncServer;
    private final long timeoutMs;
    private final Map<Method, Method> methodMap = new ConcurrentHashMap<>();
    private final long futureTimeoutMs;

    private VersionedRemoteAsyncImpl(AsyncVersionedRemote asyncServer,
                                     long timeoutMs) {
        this.asyncServer = asyncServer;
        this.timeoutMs = timeoutMs;

        /*
         * Add the network roundtrip timeout to the time we wait for the call
         * to complete and check for wraparound.
         */
        final long value = timeoutMs + DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT;
        futureTimeoutMs = (value > 0) ? value : Long.MAX_VALUE;
    }

    /**
     * Returns a dynamic proxy that implements the specified versioned remote
     * interface by making corresponding calls to an async versioned remote
     * server and waiting for the result. This method makes it possible to
     * implement an RMI interface using the associated async server interface.
     *
     * @param <T> the type of the RMI versioned remote interface
     * @param remoteInterface the RMI versioned remote interface
     * @param asyncServer the associated async server
     * @param timeoutMs the timeout in milliseconds for all remote calls made
     * to the proxy
     * @return a dynamic proxy that implements the versioned remote interface
     * @throws IllegalArgumentException if timeoutMs is 0 or less
     */
    public static <T extends VersionedRemote>
        T createProxy(Class<T> remoteInterface,
                      AsyncVersionedRemote asyncServer,
                      long timeoutMs) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException(
                "The timeoutMs must be greater than 0, found " +  timeoutMs);
        }
        return remoteInterface.cast(
            Proxy.newProxyInstance(
                asyncServer.getClass().getClassLoader(),
                new Class<?>[] { remoteInterface },
                new VersionedRemoteAsyncImpl<T>(asyncServer, timeoutMs)));
    }

    @Override
    protected Object invokeNonObject(Method method,
                                     Object @Nullable[] args)
        throws Exception {

        return callServer(methodMap.computeIfAbsent(method, this::makeHandler),
                          args);
    }

    private Method makeHandler(Method method) {
        final Class<?>[] params = method.getParameterTypes();
        final Class<?>[] newParams;
        if ("getSerialVersion".equals(method.getName())) {

            /*
             * VersionedRemote.getSerialVersion has no parameters, but the
             * AsyncVersionedRemote version has serialVersion and timeoutMillis
             * parameters
             */
            newParams = new Class<?>[] { Short.TYPE, Long.TYPE };
        } else {

            /*
             * Otherwise move serial version to the front and add a long to the
             * end for the timeout.
             */
            newParams = new Class<?>[params.length + 1];
            newParams[0] = Short.TYPE;
            if (params.length > 1) {
                System.arraycopy(params, 0, newParams, 1, params.length - 1);
            }
            newParams[params.length] = Long.TYPE;
        }

        try {
            return asyncServer.getClass()
                .getMethod(method.getName(), newParams);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                "Method " + method.getName() +
                Stream.of(newParams).map(Object::toString)
                .collect(joining(", ", "(", ")")) +
                " not found when attempting to convert method " + method +
                " to a method on server " + asyncServer,
                e);
        }
    }

    private Object callServer(Method handler, Object @Nullable[] args)
        throws Exception {

        final Object[] newArgs;
        if (args == null) {
            if (!"getSerialVersion".equals(handler.getName())) {
                throw new IllegalStateException(
                    "Expected getSerialVersion, found: " + handler.getName());
            }

            /* Pass serial version and timeout for getSerialVersion */
            newArgs = new Object[] { SerialVersion.CURRENT, timeoutMs };
        } else {

            /*
             * Move serialVersion from last to first and add timeoutMillis to
             * the end
             */
            newArgs = new Object[args.length + 1];
            newArgs[0] = args[args.length - 1];
            System.arraycopy(args, 0, newArgs, 1, args.length - 1);
            newArgs[args.length] = timeoutMs;
        }

        try {
            final CompletableFuture<?> future = (CompletableFuture<?>)
                invokeMethod(asyncServer, handler, newArgs);
            try {
                return future.get(futureTimeoutMs, MILLISECONDS);
            } catch (TimeoutException e) {
                throw new RemoteException("Operation timed out: " + e, e);
            } catch (Throwable t) {
                final Exception exception;
                try {
                    exception = FutureUtils.handleFutureGetException(t);
                } catch (RuntimeException e) {
                    throw AsyncRegistryUtils.convertToRemoteException(e);
                }
                if (exception instanceof RemoteException) {
                    throw (RemoteException) exception;
                }
                /* Other typed exceptions are not expected */
                throw new IllegalStateException(
                    "Unexpected exception: " + exception, exception);
            }
        } catch (RuntimeException e) {
            throw AsyncRegistryUtils.convertToRemoteException(e);
        }
    }

    @Override
    public String toString() {
        return String.format("VersionedRemoteAsyncImpl[%s]",
                             asyncServer);
    }

    /** Returns the hash code of the server. */
    @Override
    public int hashCode() {
        return asyncServer.hashCode();
    }

    /**
     * Returns true if the asyncServer and timeoutMs fields are equal. Ignores
     * the other fields since they are derived.
     */
    @Override
    public boolean equals(@Nullable Object object) {
        if (!(object instanceof VersionedRemoteAsyncImpl)) {
            return false;
        }
        final VersionedRemoteAsyncImpl<?> other =
            (VersionedRemoteAsyncImpl<?>) object;
        return asyncServer.equals(other.asyncServer) &&
            (timeoutMs == other.timeoutMs);
    }
}
