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
import static oracle.kv.KVStoreConfig.DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT;

import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import oracle.kv.impl.async.AsyncVersionedRemote.MethodCall;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.VersionedRemote;
import oracle.kv.impl.util.SerialVersion;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A proxy that implements a {@link VersionedRemote} interface using a {@link
 * JavaSerialMethodTable} to map methods.
 */
public class JavaSerialInitiatorProxy extends AbstractAsyncInitiatorProxy {
    private final JavaSerialMethodTable methodTable;
    private final long timeoutMs;
    private final long futureTimeoutMs;

    private JavaSerialInitiatorProxy(Class<? extends VersionedRemote>
                                     serviceInterface,
                                     JavaSerialMethodTable methodTable,
                                     CreatorEndpoint endpoint,
                                     DialogType dialogType,
                                     long timeoutMs,
                                     Logger logger) {
        super(serviceInterface,
              computeGetSerialVersionCall(serviceInterface, methodTable),
              endpoint, dialogType, logger);
        this.methodTable = methodTable;
        this.timeoutMs = timeoutMs;

        /*
         * Add the network roundtrip timeout to the time we wait for the call
         * to complete and check for wraparound.
         */
        final long value = timeoutMs + DEFAULT_NETWORK_ROUNDTRIP_TIMEOUT;
        futureTimeoutMs = (value > 0) ? value : Long.MAX_VALUE;
    }

    private static MethodCall<Short> computeGetSerialVersionCall(
        Class<? extends VersionedRemote> serviceInterface,
        JavaSerialMethodTable methodTable)
    {
        try {
            final Method method =
                serviceInterface.getMethod("getSerialVersion");
            if (method.getReturnType() != Short.TYPE) {
                throw new IllegalStateException(
                    "Expected 'short' return type for method: " + method);
            }
            /* We made the necessary runtime check */
            @SuppressWarnings("unchecked")
            final MethodCall<Short> result =
                (MethodCall<Short>) getMethodCall(methodTable, method, null);
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Unexpected exception: " + e, e);
        }
    }

    /**
     * Creates a dynamic proxy that implements the specified versioned remote
     * service interface by sending async requests to the specified endpoint.
     * Each remote method of the service interface will get an ID generated via
     * a hash of the method name, parameter types, and return type. That ID,
     * along with Java serialization for method arguments, return values, and
     * thrown exceptions, will be used to serialize the call.
     *
     * @param <T> the type of the versioned remote interface
     * @param serviceInterface the versioned remote interface
     * @param expectedDialogTypeFamily the dialog type family that the dialog
     * type should belong to
     * @param endpoint the associated endpoint
     * @param dialogType the dialog type
     * @param timeoutMs the timeout for async operations on the proxy
     * @param logger the logger
     * @return the proxy
     * @throws IllegalArgumentException if the dialog type has the wrong type
     * family
     */
    public static <T extends VersionedRemote>
        T createProxy(Class<T> serviceInterface,
                      DialogTypeFamily expectedDialogTypeFamily,
                      CreatorEndpoint endpoint,
                      DialogType dialogType,
                      long timeoutMs,
                      Logger logger)
    {
        final JavaSerialInitiatorProxy initiator =
            new JavaSerialInitiatorProxy(
                serviceInterface,
                JavaSerialMethodTable.getTable(serviceInterface), endpoint,
                dialogType, timeoutMs, logger);
        return initiator.createProxy(serviceInterface,
                                     expectedDialogTypeFamily, dialogType);
    }

    /**
     * Converts a synchronous remote method to an asynchronous call on the
     * underlying server. The expected arguments are:
     * <ul>
     * <li> method-specific arguments
     * <li> short serialVersion (but not for getSerialVersion)
     * </ul>
     *
     * @throws RemoteException if the operation times out locally
     */
    @Override
    protected Object invokeNonObject(Method method,
                                     Object @Nullable[] args)
        throws Exception
    {
        final short serialVersion;
        final Object[] methodCallArgs;
        if (args == null) {
            if (!"getSerialVersion".equals(method.getName())) {
                throw new IllegalArgumentException(
                    "Null arguments not permitted in call to " + method);
            }
            serialVersion = SerialVersion.CURRENT;
            methodCallArgs = null;
        } else {
            if (args.length == 0) {
                throw new IllegalArgumentException(
                    "Empty arguments not permitted in call to " + method);
            }
            if (!(args[args.length - 1] instanceof Short)) {
                throw new IllegalArgumentException(
                    "Last argument must be Short in call to " + method +
                    ": " + Arrays.toString(args));
            }
            serialVersion = (Short) args[args.length - 1];
            methodCallArgs = (args.length > 1) ?
                Arrays.copyOfRange(args, 0, args.length - 1) :
                null;
        }
        final MethodCall<?> methodCall = getMethodCall(method, methodCallArgs);

        try {
            final CompletableFuture<?> future =
                initiator.startDialog(serialVersion, methodCall, timeoutMs);
            try {
                return future.get(futureTimeoutMs, MILLISECONDS);
            } catch (TimeoutException e) {
                throw new RemoteException("Operation timed out after " +
                                          timeoutMs + " ms: " + e,
                                          e);
            } catch (Throwable t) {
                final Exception exception;
                try {
                    exception = FutureUtils.handleFutureGetException(t);
                } catch (RuntimeException e) {
                    throw AsyncRegistryUtils.convertToRemoteException(e);
                }
                throw new IllegalStateException(
                    "Unexpected exception: " + exception, exception);
            }
        } catch (RuntimeException e) {
            throw AsyncRegistryUtils.convertToRemoteException(e);
        }
    }

    /**
     * Finds the MethodCall object associated with the specified method and
     * method arguments using the MethodCallClass annotation.
     */
    @Override
    MethodCall<?> getMethodCall(Method method,
                                Object @Nullable[] methodCallArgs)
        throws Exception
    {
        return getMethodCall(methodTable, method, methodCallArgs);
    }

    private static
        MethodCall<?> getMethodCall(JavaSerialMethodTable methodTable,
                                    Method method,
                                    Object @Nullable[] methodCallArgs)
        throws Exception
    {
        return methodTable.getMethodCall(method, methodCallArgs);
    }

    /*
     * Checker doesn't understand that object is not null if super returns true
     */
    @SuppressWarnings("null")
    @Override
    public boolean equals(@Nullable Object object) {
        if (!super.equals(object)) {
            return false;
        }
        final JavaSerialInitiatorProxy other =
            (JavaSerialInitiatorProxy) object;
        return methodTable.equals(other.methodTable) &&
            (timeoutMs == other.timeoutMs) &&
            (futureTimeoutMs == other.futureTimeoutMs);
    }
}
