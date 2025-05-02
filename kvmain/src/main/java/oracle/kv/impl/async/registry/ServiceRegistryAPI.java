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

package oracle.kv.impl.async.registry;

import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.whenComplete;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.async.AsyncVersionedRemoteAPIWithTimeout;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.NetworkAddress;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The API for the service registry, which maps service names to service
 * endpoints.  The service registry is the asynchronous replacement for the RMI
 * registry used for synchronous operations.  This is the API that clients use
 * to register, unregister, lookup, and list the available services.  The
 * {@link ServiceRegistry} interface represents the remote interface that is
 * used to communicate requests over the network.  The {@link
 * ServiceRegistryImpl} class provides the server-side implementation.  The
 * {@link ServiceEndpoint} class is used to represent information about an
 * available remote service.
 *
 * @see ServiceRegistry
 * @see ServiceRegistryImpl
 * @see ServiceEndpoint
 */
public class ServiceRegistryAPI extends AsyncVersionedRemoteAPIWithTimeout {
    private final ServiceRegistry proxyRemote;

    /**
     * Creates a new instance.
     */
    protected ServiceRegistryAPI(short serialVersion,
                                 long defaultTimeoutMs,
                                 ServiceRegistry remote) {
        super(serialVersion, defaultTimeoutMs);
        proxyRemote = remote;
    }

    /**
     * Makes an asynchronous request to create an instance of this class,
     * returning the result as a future. If specified, the client ID is used
     * when deserializing the client socket factory associated with the service
     * endpoint returned by calls to the lookup method.
     *
     * @param endpoint the remote endpoint representing the server
     * @param clientId the client ID or null
     * @param timeoutMs the timeout in milliseconds for obtaining the API
     * @param defaultTimeoutMs the default timeout in milliseconds for
     * operations on the API
     * @param logger for debug logging
     * @return the future
     */
    public static
        CompletableFuture<ServiceRegistryAPI> wrap(CreatorEndpoint endpoint,
                                                   @Nullable ClientId clientId,
                                                   long timeoutMs,
                                                   long defaultTimeoutMs,
                                                   Logger logger) {
        return Factory.INSTANCE.wrap(endpoint, clientId, timeoutMs,
                                     defaultTimeoutMs, logger);
    }

    /**
     * A factory class for obtain ServiceRegistryAPI instances.
     */
    public static class Factory {

        static final Factory INSTANCE = new Factory();

        /**
         * Create a ServiceRegistryAPI instance.
         */
        protected ServiceRegistryAPI createAPI(short serialVersion,
                                               long defaultTimeoutMs,
                                               ServiceRegistry remote,
                                               @SuppressWarnings("unused")
                                               NetworkAddress remoteAddress) {
            return new ServiceRegistryAPI(serialVersion, defaultTimeoutMs,
                                          remote);
        }

        /**
         * Makes an asynchronous request to create a ServiceRegistryAPI
         * instance, returning the result as a future.
         *
         * @param endpoint the remote endpoint representing the server
         * @param clientId the client ID or null
         * @param timeoutMs the timeout in milliseconds for obtaining the API
         * @param defaultTimeoutMs the default timeout in milliseconds for
         * operations on the API
         * @param logger for debug logging
         * @return the future
         */
        public CompletableFuture<ServiceRegistryAPI>
            wrap(CreatorEndpoint endpoint,
                 @Nullable ClientId clientId,
                 long timeoutMs,
                 long defaultTimeoutMs,
                 final Logger logger) {
            try {
                logger.finest("Getting async ServiceRegistryAPI");
                final ServiceRegistry initiator =
                    new ServiceRegistryInitiator(endpoint, clientId, logger);
                return computeSerialVersion(initiator, timeoutMs)
                    .thenApply(serialVersion ->
                               createAPI(serialVersion, defaultTimeoutMs,
                                         initiator,
                                         endpoint.getRemoteAddress()));
            } catch (Throwable e) {
                return failedFuture(e);
            }
        }
    }

    /**
     * Look up an entry in the registry.
     *
     * @param name the name of the entry
     * @param dialogTypeFamily the expected dialog type family, or null to
     * support returning any type
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @return a future to receive the service endpoint with the requested
     * dialog type family, if specified, or {@code null} if the entry is not
     * found
     */
    public CompletableFuture<ServiceEndpoint>
        lookup(String name,
               @Nullable DialogTypeFamily dialogTypeFamily,
               long timeoutMillis) {

        try {
            return whenComplete(
                proxyRemote.lookup(getSerialVersion(), name, timeoutMillis),
                (serviceEndpoint, e) -> {
                    if (e == null) {
                        checkServiceEndpointResult(serviceEndpoint,
                                                   dialogTypeFamily);
                    }
                });
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    private void checkServiceEndpointResult(ServiceEndpoint result,
                                            @Nullable
                                            DialogTypeFamily dialogTypeFamily)
    {
        if ((result != null) && (dialogTypeFamily != null)) {
            final DialogTypeFamily serviceDialogTypeFamily =
                result.getDialogType().getDialogTypeFamily();
            if (serviceDialogTypeFamily != dialogTypeFamily) {
                throw new IllegalStateException(
                    "Unexpected dialog type family for service" +
                    " endpoint.  Expected: " + dialogTypeFamily +
                    ", found: " + serviceDialogTypeFamily);
            }
        }
    }

    /**
     * Set an entry in the registry.
     *
     * @param name the name of the entry
     * @param endpoint the endpoint to associate with the name
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @return a future for when the operation is complete
     */
    public CompletableFuture<Void> bind(String name,
                                        ServiceEndpoint endpoint,
                                        long timeoutMillis) {
        return proxyRemote.bind(getSerialVersion(), name, endpoint,
                                timeoutMillis);
    }

    /**
     * Remove an entry from the registry.
     *
     * @param name the name of the entry
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @return a future for when the operation is complete
     */
    public CompletableFuture<Void> unbind(String name,
                                          long timeoutMillis) {
        return proxyRemote.unbind(getSerialVersion(), name, timeoutMillis);
    }

    /**
     * List the entries in the registry.
     *
     * @param timeoutMillis the timeout for the operation in milliseconds
     * @return a future to return the entries
     */
    public CompletableFuture<List<String>> list(long timeoutMillis) {
        return proxyRemote.list(getSerialVersion(), timeoutMillis);
    }
}
