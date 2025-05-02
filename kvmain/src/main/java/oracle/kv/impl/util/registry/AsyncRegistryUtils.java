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

package oracle.kv.impl.util.registry;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT;
import static oracle.kv.impl.admin.param.GlobalParams.CLIENT_ADMIN_SERVICE_NAME;
import static oracle.kv.impl.async.FutureUtils.checked;
import static oracle.kv.impl.async.FutureUtils.failedFuture;
import static oracle.kv.impl.async.FutureUtils.handleFutureGetException;
import static oracle.kv.impl.async.FutureUtils.unwrapException;
import static oracle.kv.impl.async.StandardDialogTypeFamily.CLIENT_ADMIN_SERVICE_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REP_NODE_ADMIN_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REQUEST_HANDLER_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.SERVICE_REGISTRY_DIALOG_TYPE;
import static oracle.kv.impl.async.StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLHandshakeException;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.FaultException;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.api.AsyncRequestHandlerAPI;
import oracle.kv.impl.api.AsyncRequestHandlerInitiator;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.async.CreatorEndpoint;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.DialogType;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.StandardDialogTypeFamily;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;
import oracle.kv.impl.async.exception.ConnectionEndpointShutdownException;
import oracle.kv.impl.async.exception.ConnectionException;
import oracle.kv.impl.async.exception.DialogException;
import oracle.kv.impl.async.exception.DialogNoSuchTypeException;
import oracle.kv.impl.async.exception.GetUserException;
import oracle.kv.impl.async.exception.InitialConnectIOException;
import oracle.kv.impl.async.registry.ServiceEndpoint;
import oracle.kv.impl.async.registry.ServiceRegistry;
import oracle.kv.impl.async.registry.ServiceRegistryAPI;
import oracle.kv.impl.async.registry.ServiceRegistryImpl;
import oracle.kv.impl.client.admin.AsyncClientAdminServiceAPI;
import oracle.kv.impl.client.admin.AsyncClientAdminServiceInitiator;
import oracle.kv.impl.fault.AsyncEndpointGroupFaultHandler;
import oracle.kv.impl.rep.admin.AsyncClientRepNodeAdminAPI;
import oracle.kv.impl.rep.admin.AsyncClientRepNodeAdminInitiator;
import oracle.kv.impl.security.login.AsyncUserLoginAPI;
import oracle.kv.impl.security.login.AsyncUserLoginInitiator;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.ServiceResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.NonNullByDefault;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.client.ClientLoggerUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utilities for managing asynchronous services.
 */
@NonNullByDefault
public class AsyncRegistryUtils extends AsyncControl {

    /**
     * The endpoint group, for connecting to dialog-layer-based services, or
     * null if not initialized.  Synchronize on the class when accessing this
     * field.
     */
    private static volatile @Nullable EndpointGroup endpointGroup = null;

    /**
     * The host name to use when registering async service endpoints for
     * services created on this host, or null if not yet set.
     *
     * @see #getServerHostName
     * @see #setServerHostName
     */
    private static @Nullable String serverHostName = null;

    /** A ServiceRegistryAPI factory that wraps some exceptions. */
    private static final ServiceRegistryAPI.Factory registryFactory =
        new TranslatingExceptionsRegistryFactory();

    /**
     * If non-null, an overriding ServiceRegistryAPI factory to use instead,
     * for testing.
     */
    private static volatile
        ServiceRegistryAPI.@Nullable Factory testRegistryFactory;

    /** Track if we've logged that async is disabled */
    private static volatile boolean loggedServerAsyncDisabled;

    /**
     * The next value to make dialog types unique for multiple stores using
     * different registry ports. Use 0 for the first registry port, and
     * non-zero values to make values unique for other ports. Multiple stores
     * are only used within the same VM during testing.
     */
    private static final AtomicInteger dialogTypeNextRegistryPortValue =
        new AtomicInteger(0);

    /**
     * A map from registry ports to unique values to use to make dialog types
     * unique for stores using particular registry ports.
     */
    private static final ConcurrentMap<Integer, Integer>
        dialogTypeRegistryPortValues = new ConcurrentHashMap<>();

    /**
     * A test hook that is called at the start of getRequestHandler with the RN
     * ID of the handler to get. If the hook throws a runtime exception, then
     * that exception will be returned as the result of the call.
     */
    public static volatile @Nullable TestHook<RepNodeId> getRequestHandlerHook;

    /**
     * A test hook that is called by createInitiatorProxy with the
     * EndpointConfigBuilder used to create the configuration for the
     * initiator's creator endpoint.
     */
    public static volatile @Nullable TestHook<EndpointConfigBuilder>
        createInitiatorProxyEndpointConfigBuilderHook;

    private AsyncRegistryUtils() {
        throw new AssertionError();
    }

    /**
     * Returns the endpoint group to use for asynchronous operations.  Returns
     * the currently initialized endpoint group, if present, or otherwise
     * configures and returns a default endpoint group.
     *
     * @return the endpoint group
     * @throws RuntimeException if a problem occurs initializing the endpoint
     * group
     */
    public static synchronized EndpointGroup getEndpointGroup() {
        if (endpointGroup != null) {
            return endpointGroup;
        }

        /*
         * Configure default endpoint group. This case should only be used for
         * utilities: clients and services should initialize the endpoint group
         * explicitly so it is configured properly.
         */
        return assureEndpointGroup(
            ClientLoggerUtils.getLogger(EndpointGroup.class, "endpoint-group"),
            KVStoreFactory.getEndpointGroupNumThreads(),
            KVStoreFactory.getEndpointGroupMaxQuiescentSeconds(),
            /*
             * forClient is true for utilities. Services should explicitly
             * initialized the endpoint group so that they can set this value
             * to false.
             */
            true,
            /* maxPermits is not limited for utilities */
            Integer.MAX_VALUE,
            AsyncEndpointGroupFaultHandler.DEFAULT,
            true /* failIfPresent */);
    }

    /**
     * Returns the endpoint group to use for asynchronous operations, or null
     * if the endpoint group is not available.
     *
     * @return the endpoint group or null
     */
    public static @Nullable EndpointGroup getEndpointGroupOrNull()
    {
        return endpointGroup;
    }

    /**
     * Creates a new endpoint group to use for asynchronous operations, if one
     * does not already exist, using the default fault handler and failing if
     * forClient is false and the group already exists. Note that maintaining a
     * single endpoint group for the life of the JVM is necessary so that
     * server sockets shared with RMI continue to be usable, since RMI expects
     * server sockets to remain open until it closes them.
     *
     * @param logger the logger to be used by the endpoint group
     * @param numThreads the number of threads
     * @param maxQuiescentSeconds the maximum time in seconds an executor is in
     * quiescence before it is shut down
     * @param forClient true if the endpoint group is needed for use for a
     * client call, else false for server side use. If false, then the endpoint
     * group will only be created if async is enabled for the server.
     * @param maxPermits the maximum number of permits that the group's dialog
     * resource manager should grant, which controls the maximum number of
     * concurrent async calls
     * @throws IllegalStateException if forClient is false and the endpoint
     * group has already been created
     * @throws RuntimeException if a problem occurs creating the endpoint group
     */
    public static synchronized
        void initEndpointGroup(Logger logger,
                               int numThreads,
                               int maxQuiescentSeconds,
                               boolean forClient,
                               int maxPermits) {
        initEndpointGroup(
            logger, numThreads, maxQuiescentSeconds, forClient,
            maxPermits, AsyncEndpointGroupFaultHandler.DEFAULT);
    }

    /**
     * Creates a new endpoint group to use for asynchronous operations, if one
     * does not already exist, using the specified fault handler and failing if
     * forClient is false and the group already exists. Note that maintaining a
     * single endpoint group for the life of the JVM is necessary so that
     * server sockets shared with RMI continue to be usable, since RMI expects
     * server sockets to remain open until it closes them.
     *
     * @param logger the logger to be used by the endpoint group
     * @param numThreads the number of threads
     * @param maxQuiescentSeconds the maximum time in seconds an executor is in
     * quiescence before it is shut down
     * @param forClient true if the endpoint group is needed for use for a
     * client call, else false for server side use. If false, then the endpoint
     * group will only be created if async is enabled for the server.
     * @param maxPermits the maximum number of permits that the group's dialog
     * resource manager should grant, which controls the maximum number of
     * concurrent async calls
     * @param faultHandler the fault handler
     * @throws IllegalStateException if forClient is false and the endpoint
     * group has already been created
     * @throws RuntimeException if a problem occurs creating the endpoint group
     */
    public static synchronized
        void initEndpointGroup(Logger logger,
                               int numThreads,
                               int maxQuiescentSeconds,
                               boolean forClient,
                               int maxPermits,
                               AsyncEndpointGroupFaultHandler faultHandler) {
        initEndpointGroup(logger, numThreads, maxQuiescentSeconds,
                          forClient, maxPermits, faultHandler,
                          true /* failIfPresent */);
    }

    /**
     * Creates a new endpoint group to use for asynchronous operations, if one
     * does not already exist, using the specified fault handler and failing if
     * the group already exists and forClient is false, failIfPresent is true,
     * and TestStatus.isActive is false. Note that maintaining a single
     * endpoint group for the life of the JVM is necessary so that server
     * sockets shared with RMI continue to be usable, since RMI expects server
     * sockets to remain open until it closes them.
     *
     * @param logger the logger to be used by the endpoint group
     * @param numThreads the number of threads
     * @param maxQuiescentSeconds the maximum time in seconds an executor is in
     * quiescence before it is shut down
     * @param forClient true if the endpoint group is needed for use for a
     * client call, else false for server side use. If false, then the endpoint
     * group will only be created if async is enabled for the server.
     * @param maxPermits the maximum number of permits that the group's dialog
     * resource manager should grant, which controls the maximum number of
     * concurrent async calls
     * @param faultHandler the fault handler
     * @param failIfPresent whether to fail if forClient is false and the
     * endpoint group is already created
     * @throws IllegalStateException if the endpoint group has already been
     * created, and forClient is false, failIfPresent is true, and
     * TestStatus.isActive is false
     * @throws RuntimeException if a problem occurs creating the endpoint group
     */
    public static synchronized
        void initEndpointGroup(Logger logger,
                               int numThreads,
                               int maxQuiescentSeconds,
                               boolean forClient,
                               int maxPermits,
                               AsyncEndpointGroupFaultHandler faultHandler,
                               boolean failIfPresent) {
        checkNull("logger", logger);
        if (!forClient && !serverUseAsync) {
            if (!loggedServerAsyncDisabled) {
                logger.info("Async is disabled on the server");
                loggedServerAsyncDisabled = true;
            }
            return;
        }
        assureEndpointGroup(logger, numThreads, maxQuiescentSeconds,
                            forClient, maxPermits, faultHandler,
                            failIfPresent);
    }

    /**
     * Like initEndpointGroup, but always returns an endpoint group.
     */
    private static EndpointGroup
        assureEndpointGroup(Logger logger,
                            int numThreads,
                            int maxQuiescentSeconds,
                            boolean forClient,
                            int maxPermits,
                            AsyncEndpointGroupFaultHandler faultHandler,
                            boolean failIfPresent) {

        if (endpointGroup != null) {
            final EndpointGroup nonNullEndpointGroup = endpointGroup;
            /* Use the existing endpoint group for clients or if requested */
            if (forClient || !failIfPresent) {
                return nonNullEndpointGroup;
            }

            /*
             * Also use the existing group during tests since some tests create
             * multiple SNAs in the same JVM
             */
            if (TestStatus.isActive()) {
                return nonNullEndpointGroup;
            }

            throw new IllegalStateException(
                "Server endpoint group was already created");
        }
        try {
            /*
             * TODO: Use a log-only handler for now. The same behavior is
             * applied to clients. Ideally, we should apply the behavior of
             * RepNodeService.ThreadExceptionHandler for the server-side, but
             * that is not modularized to use separately.
             */
            final UncaughtExceptionHandler uncaughtExceptionHandler =
                new LogOnlyHandler(logger);
            final EndpointGroup newEndpointGroup = new NioEndpointGroup(
                logger, forClient, numThreads, maxQuiescentSeconds,
                maxPermits, faultHandler, uncaughtExceptionHandler);
            endpointGroup = newEndpointGroup;
            return newEndpointGroup;
        } catch (Exception e) {
            throw new IllegalStateException(
                "Unexpected exception creating the async endpoint group: " +
                e.getMessage(),
                e);
        }
    }

    private static class LogOnlyHandler implements UncaughtExceptionHandler {
        final Logger logger;

        public LogOnlyHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void uncaughtException(@Nullable Thread t,
                                      @Nullable Throwable e) {
            if (t != null) {
                logger.log(Level.SEVERE,
                           "Uncaught exception in thread:" + t.getName(), e);
            }
        }
    }

    /**
     * Explicitly set the endpoint group, for testing.
     */
    public static
        void setEndpointGroup(@Nullable EndpointGroup endpointGroup)
    {
        AsyncRegistryUtils.endpointGroup = endpointGroup;
    }

    /**
     * Returns the host name to use when registering async service endpoints
     * for services created on this host.  Returns the value specified by the
     * most recent call to setServerHostName, if any, or else returns the host
     * name of the address returned by {@link InetAddress#getLocalHost}.
     *
     * This is a blocking method since InetAddress#getHostName may need to
     * resolve with a DNS.
     *
     * @return the host name
     * @throws UnknownHostException if there is a problem obtaining the host
     * name of the local host
     */
    public static synchronized String getServerHostName()
        throws UnknownHostException {

        if (serverHostName != null) {
            return serverHostName;
        }
        return InetAddress.getLocalHost().getHostName();
    }

    /**
     * Sets the host name for use when registering remote objects, either RMI
     * or async ones, created on this host.
     *
     * @param hostName the server host name
     */
    public static synchronized void setServerHostName(String hostName) {

        /* Set the name used for async services */
        serverHostName = checkNull("hostName", hostName);

        /* Set the hostname to be associated with RMI stubs */
        if (!EmbeddedMode.isEmbedded()) {
            System.setProperty("java.rmi.server.hostname", hostName);
        }
    }

    /**
     * Get the API wrapper for the async request handler for the RN identified
     * by {@code repNodeId}. Returns {@code null} if the RN is not present in
     * the topology, and otherwise returns exceptionally if the request handler
     * cannot be obtained within the requested timeout. Callers should include
     * the resource ID of the caller, or null if none is available.
     */
    public static CompletableFuture<AsyncRequestHandlerAPI>
        getRequestHandler(Topology topology,
                          RepNodeId repNodeId,
                          @Nullable ResourceId callerId,
                          long timeoutMs,
                          Logger logger) {

        try {
            if (timeoutMs <= 0) {
                return completedFuture(null);
            }

            final long stopTime = System.currentTimeMillis() + timeoutMs;

            assert TestHookExecute.doHookIfSet(getRequestHandlerHook,
                                               repNodeId);

            final RepNode repNode = topology.get(repNodeId);
            if (repNode == null) {
                return completedFuture(null);
            }
            final StorageNode sn = topology.get(repNode.getStorageNodeId());
            final String bindingName =
                RegistryUtils.bindingName(topology.getKVStoreName(),
                                          repNodeId.getFullName(),
                                          RegistryUtils.InterfaceType.MAIN);
            return getRegistry(topology.getKVStoreName(),
                               sn.getHostname(), sn.getRegistryPort(),
                               getClientId(callerId), timeoutMs, logger)
                .thenCompose(registry ->
                             getServiceEndpoint(
                                 registry,
                                 stopTime - System.currentTimeMillis(),
                                 bindingName,
                                 REQUEST_HANDLER_TYPE_FAMILY))
                .thenCompose(endpoint ->
                             getRequestHandler(
                                 endpoint, repNodeId,
                                 stopTime - System.currentTimeMillis(),
                                 logger));
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    private static @Nullable ClientId getClientId(@Nullable ResourceId rid) {
        return (rid instanceof ClientId) ? (ClientId) rid : null;
    }

    private static CompletableFuture<ServiceEndpoint>
        getServiceEndpoint(ServiceRegistryAPI registry,
                           long timeoutMs,
                           String bindingName,
                           @Nullable DialogTypeFamily dialogTypeFamily) {
        try {
            checkNull("registry", registry);
            if (timeoutMs <= 0) {
                return completedFuture(null);
            }
            return registry.lookup(bindingName, dialogTypeFamily, timeoutMs);
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    private static CompletableFuture<AsyncRequestHandlerAPI>
        getRequestHandler(@Nullable ServiceEndpoint serviceEndpoint,
                          RepNodeId repNodeId,
                          long timeoutMs,
                          Logger logger) {
        try {
            return
                createInitiatorProxy(
                    asInitiatorProxyFactory(AsyncRequestHandlerInitiator::new),
                    serviceEndpoint, repNodeId.getFullName(), timeoutMs,
                    logger)
                .thenCompose(
                    (initiator) ->
                    (initiator == null)
                    ? completedFuture(null)
                    : AsyncRequestHandlerAPI.wrap(initiator, timeoutMs));
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    /**
     * Creates a client side initiator proxy using the specified factory and
     * service endpoint, using the timeout specified by the service endpoint's
     * client socket factory, and returning null if the service endpoint is
     * null.
     */
    static <T> CompletableFuture<T> createInitiatorProxy(
        InitiatorProxyFactory<T> proxyFactory,
        @Nullable ServiceEndpoint serviceEndpoint,
        String bindingName,
        Logger logger)
        throws IOException
    {
        if (serviceEndpoint == null) {
            return completedFuture(null);
        }
        final ClientSocketFactory csf =
            serviceEndpoint.getClientSocketFactory();
        return createInitiatorProxy(proxyFactory, serviceEndpoint,
                                    bindingName, getTimeoutMs(csf), logger);
    }

    /**
     * Creates an client side initiator proxy using the specified factory,
     * service endpoint, and timeout, and returning null if the service
     * endpoint is null or the timeout is not greater than 0.
     */
    private static <T>
        CompletableFuture<T> createInitiatorProxy(
            InitiatorProxyFactory<T> proxyFactory,
            @Nullable ServiceEndpoint serviceEndpoint,
            String bindingName,
            long timeoutMs,
            Logger logger)
        throws IOException
    {
        if ((serviceEndpoint == null) || (timeoutMs <= 0)) {
            return completedFuture(null);
        }
        final ClientSocketFactory csf =
            serviceEndpoint.getClientSocketFactory();
        final EndpointConfigBuilder endpointConfigBuilder =
            csf.getEndpointConfigBuilder();
        assert TestHookExecute.doHookIfSet(
            createInitiatorProxyEndpointConfigBuilderHook,
            endpointConfigBuilder);
        final EndpointConfig endpointConfig =
            endpointConfigBuilder.build();
        final NetworkAddress networkAddress =
            serviceEndpoint.getNetworkAddress();
        /* Specify the local address, but only for TCP addresses */
        boolean needAddressResolution = false;
        final InetSocketAddress localAddr = csf.getLocalAddr();
        if (networkAddress.isInetAddress()) {
            if (localAddr != null) {
                needAddressResolution = true;
            }
        }
        final CompletableFuture<NetworkAddress> addressResolution =
            needAddressResolution && (localAddr != null)
            ? NetworkAddress.convert(localAddr)
            : CompletableFuture.completedFuture(null);
        return addressResolution.thenApply(
            (localNetworkAddress) ->
            proxyFactory.createProxy(
                new RetryingCreatorEndpoint(
                    bindingName, networkAddress, localNetworkAddress,
                    endpointConfig, logger),
                serviceEndpoint.getDialogType(), timeoutMs, logger));
    }

    /**
     * A CreatorEndpoint that recreates the endpoint and retries starting the
     * dialog if the initial attempt to start a dialog is aborted because the
     * endpoint was shutdown due to a race condition.
     *
     * TODO: We should modify EndpointGroup and CreatorEndpoint to handle this
     * problem transparently, since the work around seems messy.
     */
    private static class RetryingCreatorEndpoint implements CreatorEndpoint {
        private final String perfName;
        private final NetworkAddress remoteAddress;
        private final @Nullable NetworkAddress localAddress;
        private final EndpointConfig endpointConfig;
        private final Logger logger;
        private volatile CreatorEndpoint endpoint;
        RetryingCreatorEndpoint(String perfName,
                                NetworkAddress remoteAddress,
                                @Nullable NetworkAddress localAddress,
                                EndpointConfig endpointConfig,
                                Logger logger) {
            this.perfName = perfName;
            this.remoteAddress = remoteAddress;
            this.localAddress = localAddress;
            this.endpointConfig = endpointConfig;
            this.logger = logger;
            endpoint = getEndpoint();
        }
        @Override
        public void startDialog(int dialogType,
                                DialogHandler dialogHandler,
                                long timeoutMillis) {
            endpoint.startDialog(
                dialogType,
                new WrappedDialogHandler(dialogType, dialogHandler,
                                         timeoutMillis, this),
                timeoutMillis);
        }
        boolean updateEndpoint() {
            final CreatorEndpoint newEndpoint = getEndpoint();
            if (newEndpoint == endpoint) {
                return false;
            }
            endpoint = newEndpoint;
            return true;
        }
        private CreatorEndpoint getEndpoint() {
            return AsyncRegistryUtils.getEndpointGroup()
                .getCreatorEndpoint(perfName, remoteAddress, localAddress,
                                    endpointConfig);
        }
        @Override
        public void enableResponding(int dialogType,
                                     DialogHandlerFactory factory) {
            endpoint.enableResponding(dialogType, factory);
        }
        @Override
        public void disableResponding(int dialogType) {
            endpoint.disableResponding(dialogType);
        }
        @Override
        public EndpointGroup getEndpointGroup() {
            return endpoint.getEndpointGroup();
        }
        @Override
        public NetworkAddress getRemoteAddress() {
            return endpoint.getRemoteAddress();
        }
        @Override
        public EndpointConfig getEndpointConfig() {
            return endpoint.getEndpointConfig();
        }
        @Override
        public int getNumDialogsLimit() {
            return endpoint.getNumDialogsLimit();
        }
        @Override
        public String toString() {
            return "RetryingCreatorEndpoint[" + endpoint + "]";
        }
        @Override
        public int hashCode() {
            return endpoint.hashCode();
        }
        @Override
        public boolean equals(@Nullable Object object) {
            if (!(object instanceof RetryingCreatorEndpoint)) {
                return false;
            }
            final RetryingCreatorEndpoint other =
                (RetryingCreatorEndpoint) object;
            return endpoint.equals(other.endpoint);
        }
    }

    private static class WrappedDialogHandler implements DialogHandler {
        private final int dialogType;
        private final DialogHandler dialogHandler;
        private final long stopTimeMillis;
        private final RetryingCreatorEndpoint retryEndpoint;
        private volatile boolean onStartAborted;
        WrappedDialogHandler(int dialogType,
                             DialogHandler dialogHandler,
                             long timeoutMillis,
                             RetryingCreatorEndpoint retryEndpoint) {
            this.dialogType = dialogType;
            this.dialogHandler = dialogHandler;
            stopTimeMillis = System.currentTimeMillis() + timeoutMillis;
            this.retryEndpoint = retryEndpoint;
        }
        @Override
        public void onStart(DialogContext context, boolean aborted) {
            if (!aborted) {
                dialogHandler.onStart(context, aborted);
            } else {
                /*
                 * Wait to start the dialog in case the endpoint needs to be
                 * replaced
                 */
                onStartAborted = true;
            }
        }
        @Override
        public void onCanWrite(DialogContext context) {
            if (onStartAborted) {
                throw new IllegalStateException("Illegal write after abort");
            }
            dialogHandler.onCanWrite(context);
        }
        @Override
        public void onCanRead(DialogContext context, boolean finished) {
            if (onStartAborted) {
                throw new IllegalStateException("Illegal read after abort");
            }
            dialogHandler.onCanRead(context, finished);
        }
        @Override
        public void onAbort(DialogContext context, Throwable cause) {

            /* If onStart was aborted... */
            if (onStartAborted) {

                /*
                 * ... check to see if the exception means the connection was
                 * shutdown prior to the completion of the handshake. If we can
                 * update the endpoint, and there is still time, then try again
                 * with the updated endpoint, to handle the case where the
                 * endpoint needed to be replaced.
                 * TODO: perhaps this logic can be moved into the dialog layer,
                 * [KVSTORE-1242].
                 */
                if ((cause instanceof DialogException) &&
                    (cause.getCause() instanceof
                     ConnectionEndpointShutdownException) &&
                    !((ConnectionException) cause.getCause())
                    .isHandshakeDone() &&
                    retryEndpoint.updateEndpoint()) {
                    final long updatedTimeoutMillis =
                        stopTimeMillis - System.currentTimeMillis();
                    if (updatedTimeoutMillis > 0) {

                        /* Try again with new endpoint */
                        retryEndpoint.logger.fine(
                            "Retrying call with new creator endpoint");
                        retryEndpoint.endpoint.startDialog(
                            dialogType, dialogHandler, updatedTimeoutMillis);
                        return;
                    }
                }

                /* We delayed starting the dialog, so start it now */
                dialogHandler.onStart(context, true);
            }

            dialogHandler.onAbort(context, cause);
        }
        @Override
        public String toString() {
            return String.format("WrappedDialogHandler[%s, %s]",
                                 dialogType, dialogHandler);
        }
    }

    /**
     * A factory interface for creating client side initiator proxies.
     */
    public interface InitiatorProxyFactory<T> {
        T createProxy(CreatorEndpoint creatorEndpoint,
                      DialogType dialogType,
                      long timeoutMs,
                      Logger logger);
    }

    /**
     * A factory interface for creating client side initiator proxies where the
     * factory method does not have a timeout parameter.
     */
    public interface InitiatorProxyFactoryNoTimeout<T>
            extends InitiatorProxyFactory<T> {
        T createProxy(CreatorEndpoint creatorEndpoint,
                      DialogType dialogType,
                      Logger logger);
        @Override
        default T createProxy(CreatorEndpoint creatorEndpoint,
                              DialogType dialogType,
                              long timeoutMs,
                              Logger logger) {
            return createProxy(creatorEndpoint, dialogType, logger);
        }
    }

    /**
     * A convenience method for casting a InitiatorProxyFactoryNoTimeout to an
     * InitiatorProxyFactory.
     */
    private static <T> InitiatorProxyFactory<T>
        asInitiatorProxyFactory(InitiatorProxyFactoryNoTimeout<T> factory)
    {
        return factory;
    }

    /**
     * Get an AsyncClientAdminServiceAPI for the admin running on the specified
     * host and port. If the server is not running with a sufficiently recent
     * serial version that it supports the async version of the service, throws
     * InsufficientSerialVersionException.
     *
     * @return the API or null
     * @throws InsufficientSerialVersionException if the async service is not
     * supported
     * @throws RemoteException if there is a network error
     */
    static @Nullable AsyncClientAdminServiceAPI
        getClientAdminService(String storeName,
                              String hostName,
                              int registryPort,
                              @Nullable LoginHandle loginHandle,
                              @Nullable ClientId clientId,
                              Logger logger)
        throws RemoteException
    {
        return getRemoteService(storeName, hostName, registryPort,
                                CLIENT_ADMIN_SERVICE_NAME, clientId,
                                endpoint ->
                                getClientAdminService(endpoint, loginHandle,
                                                      logger),
                                CLIENT_ADMIN_SERVICE_TYPE_FAMILY,
                                SerialVersion.MINIMUM, logger);
    }

    /**
     * Check that the serial version of the service registry meets the version
     * required to support the specified service.
     */
    private static
        ServiceRegistryAPI checkRegistryVersion(ServiceRegistryAPI registry,
                                                short requiredVersion,
                                                String operationName) {
        final short serviceVersion = registry.getSerialVersion();
        if (serviceVersion < requiredVersion) {
            throw new InsufficientSerialVersionException(
                "Required serial version " + requiredVersion +
                ", found " + serviceVersion +
                " while performing operation: " + operationName);
        }
        return registry;
    }

    /**
     * Thrown when the serial version of a service is not high enough to
     * support the requested async operation.
     */
    static class InsufficientSerialVersionException
            extends RuntimeException {
        private static final long serialVersionUID = 1;
        InsufficientSerialVersionException(String msg) {
            super(msg);
        }
    }

    /**
     * Wait for an object being returned from a future, converting dialog
     * exceptions to remote exceptions as needed. This method does not supply
     * its own timeout because it assumes that the future does its own
     * timeouts. Since those timeouts can depend in some cases on values
     * obtained during the chain of operations, there is no easy way for the
     * caller to do the timing out.
     */
    private static <T> T getWithRemoteExceptions(CompletableFuture<T> future,
                                                 String operationName)
        throws RemoteException
    {
        return getWithRemoteExceptions(future, operationName, Long.MAX_VALUE);
    }

    /**
     * Same as two argument overloading, but specify a timeout.
     */
    private static <T> T getWithRemoteExceptions(CompletableFuture<T> future,
                                                 String operationName,
                                                 long timeoutMillis)
        throws RemoteException
    {
        try {
            return getWithTimeout(future, operationName, timeoutMillis);
        } catch (RuntimeException e) {
            throw convertToRemoteException(e);
        }
    }

    /**
     * Takes a runtime exception, converting dialog exceptions to the
     * associated RemoteException as appropriate.
     *
     * <p>If the exception implements GetUserException, get the associated user
     * exception or else work with the original exception.
     *
     * <p>If the exception is an Error, throw it.
     *
     * <p>If the original exception is a DialogException with
     * ConnectionEndpointShutdownException or InitialConnectIOException as the
     * cause, return a ConnectException, which represent the case that the
     * initial network connection failed.
     *
     * <p>If the original exception is a DialogNoSuchTypeException, return a
     * NoSuchObjectException, which is the RMI equivalent for a problem
     * involving not finding the requested service.
     *
     * <p>Otherwise, if the original exception represents an
     * ConnectionException with isHandshakeDone=false, return a
     * ConnectIOException, which is the RMI equivalent for failing to make an
     * initial connection.
     *
     * <p>If the exception is an IOException or RequestTimeoutException, return
     * a RemoteException that wraps it.
     *
     * <p>If the exception is a RuntimeException, throw it.
     *
     * <p>Otherwise, throw an IllegalStateException.
     */
    public static
        RemoteException convertToRemoteException(RuntimeException e) {

        final Throwable userThrowable = GetUserException.getUserException(e);
        if (userThrowable instanceof Error) {
            throw (Error) userThrowable;
        }
        if (!(userThrowable instanceof Exception)) {
            throw new IllegalStateException(
                "Unexpected exception: " + userThrowable, userThrowable);
        }
        final Exception userException = (Exception) userThrowable;

        /*
         * A DialogException with a ConnectionEndpointShutdownException or
         * InitialConnectIOException cause means that the server was not
         * reachable on the network.
         */
        if ((e instanceof DialogException) &&
            ((e.getCause() instanceof ConnectionEndpointShutdownException) ||
             (e.getCause() instanceof InitialConnectIOException))) {
            return new ConnectException(userException.getMessage(),
                                        userException);
        }

        /*
         * A DialogNoSuchTypeException means the requested dialog type was not
         * found, most likely because the server was not running.
         */
        if (e instanceof DialogNoSuchTypeException) {

            /* Can't specify cause for NoSuchObjectException */
            return new NoSuchObjectException(userException.toString());
        }

        /*
         * Otherwise, if the problem was during the initial async connection
         * negotiation, then throw ConnectIOException, which is the
         * RemoteException that best captures this situation. Some callers
         * treat ConnectIOException differently from other RemoteExceptions
         * because it means the RMI service is not even available, and so the
         * problem may be more permanent than a temporary network glitch that
         * might result in regular IOException.
         */
        if ((e instanceof DialogException) &&
            (e.getCause() instanceof ConnectionException) &&
            !((ConnectionException) e.getCause()).isHandshakeDone()) {
            return new ConnectIOException(userException.getMessage(),
                                          userException);
        }

        if (userException instanceof IOException) {
            return new RemoteException(e.getMessage(), e);
        }

        /*
         * getWithTimeout throws RequestTimeoutException for timeouts, which
         * should be thrown as a RemoteException
         */
        if (e instanceof RequestTimeoutException) {
            return new RemoteException(e.getMessage(), e);
        }
        if (userException instanceof RuntimeException) {
            throw (RuntimeException) userException;
        }
        throw new IllegalStateException(
            "Unexpected exception: " + userException, userException);
    }

    static CompletableFuture<AsyncClientAdminServiceAPI>
        getClientAdminService(@Nullable ServiceEndpoint serviceEndpoint,
                              @Nullable LoginHandle loginHandle,
                              Logger logger) {
        try {
            if (serviceEndpoint == null) {
                return completedFuture(null);
            }
            final ClientSocketFactory csf =
                serviceEndpoint.getClientSocketFactory();
            final long timeoutMs = getTimeoutMs(csf);
            return
                createInitiatorProxy(
                    asInitiatorProxyFactory(
                        AsyncClientAdminServiceInitiator::createProxy),
                    serviceEndpoint, CLIENT_ADMIN_SERVICE_NAME, logger)
                .thenCompose(
                    (initiator) ->
                    (initiator == null)
                    ? completedFuture(null)
                    : AsyncClientAdminServiceAPI.wrap(
                        initiator, loginHandle, timeoutMs, timeoutMs));
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    /**
     * Return the operation timeout to use for operations associated with the
     * specified client socket factory. Uses the factory's read timeout if it
     * is greater than zero, treating zero as meaning an infinite timeout.
     * Returns the default registry timeout if the factory is not provided.
     */
    private static long getTimeoutMs(@Nullable ClientSocketFactory csf) {
        if (csf == null) {
            return DEFAULT_REGISTRY_READ_TIMEOUT;
        }
        final long readTimeoutMs = csf.getReadTimeoutMs();
        return readTimeoutMs > 0 ? readTimeoutMs : Long.MAX_VALUE;
    }

    /**
     * Get the AsyncClientRepNodeAdminAPI registered with the specified service
     * binding. If the server is not running with a sufficiently recent serial
     * version that it supports the async version of the service, throws
     * InsufficientSerialVersionException.
     *
     * If ignoreWrongType is true, then treats the service as missing if its
     * type is wrong.
     *
     * @return the API or null
     * @throws InsufficientSerialVersionException if the async service is not
     * supported
     * @throws RemoteException if there is a network error
     */
    static AsyncClientRepNodeAdminAPI
        getClientRepNodeAdmin(String storeName,
                              String hostName,
                              int registryPort,
                              String bindingName,
                              @Nullable LoginHandle loginHandle,
                              @Nullable ClientId clientId,
                              boolean ignoreWrongType,
                              Logger logger)
        throws RemoteException
    {
        return getRemoteService(
            storeName, hostName, registryPort, bindingName, clientId,
            ep -> getClientRepNodeAdmin(ep, bindingName, loginHandle, logger),
            !ignoreWrongType ? REP_NODE_ADMIN_TYPE_FAMILY : null,
            SerialVersion.MINIMUM, logger);
    }

    static CompletableFuture<AsyncClientRepNodeAdminAPI>
        getClientRepNodeAdmin(@Nullable ServiceEndpoint serviceEndpoint,
                              String bindingName,
                              @Nullable LoginHandle loginHandle,
                              Logger logger) {
        try {
            if (serviceEndpoint == null) {
                return completedFuture(null);
            }
            final ClientSocketFactory csf =
                serviceEndpoint.getClientSocketFactory();
            final long timeoutMs = getTimeoutMs(csf);
            return
                createInitiatorProxy(
                    asInitiatorProxyFactory(
                        AsyncClientRepNodeAdminInitiator::createProxy),
                    serviceEndpoint, bindingName, logger)
                .thenCompose(
                    (initiator) ->
                    (initiator == null)
                    ? completedFuture(null)
                    : AsyncClientRepNodeAdminAPI.wrap(
                        initiator, loginHandle, timeoutMs, timeoutMs));
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    /**
     * Get the AsyncUserLoginAPI registered with the specified service binding.
     * If the server is not running with a sufficiently recent serial version
     * that it supports the async version of the service, throws
     * InsufficientSerialVersionException.
     *
     * If ignoreWrongType is true, then treats the service as missing if its
     * type is wrong.
     *
     * @return the API or null
     * @throws InsufficientSerialVersionException if the async service is not
     * supported
     * @throws RemoteException if there is a network error
     */
    static AsyncUserLoginAPI getUserLogin(String storeName,
                                          String hostName,
                                          int registryPort,
                                          String bindingName,
                                          @Nullable LoginHandle loginHandle,
                                          @Nullable ClientId clientId,
                                          boolean ignoreWrongType,
                                          Logger logger)
        throws RemoteException
    {
        return getRemoteService(
            storeName, hostName, registryPort, bindingName, clientId,
            ep -> getUserLogin(ep, bindingName, loginHandle, logger),
            !ignoreWrongType ? USER_LOGIN_TYPE_FAMILY : null,
            SerialVersion.MINIMUM, logger);
    }

    static CompletableFuture<AsyncUserLoginAPI>
        getUserLogin(@Nullable ServiceEndpoint serviceEndpoint,
                     String bindingName,
                     @Nullable LoginHandle loginHandle,
                     Logger logger) {
        try {
            if (serviceEndpoint == null) {
                return completedFuture(null);
            }
            final ClientSocketFactory csf =
                serviceEndpoint.getClientSocketFactory();
            final long timeoutMs = getTimeoutMs(csf);
            return
                createInitiatorProxy(
                    asInitiatorProxyFactory(AsyncUserLoginInitiator::createProxy),
                    serviceEndpoint, bindingName, logger)
                .thenCompose(
                    (initiator) ->
                    (initiator == null)
                    ? completedFuture(null)
                    : AsyncUserLoginAPI.wrap(
                        initiator, loginHandle, timeoutMs, timeoutMs));
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }

    private static EndpointConfig
        getRegistryEndpointConfig(@Nullable ClientSocketFactory csf)
        throws IOException {

        if (csf != null) {
            return csf.getEndpointConfig();
        }
        return ClientSocketFactory.getEndpointConfigBuilder(
            KVStoreConfig.DEFAULT_REGISTRY_OPEN_TIMEOUT,
            KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT)
            .build();
    }

    /**
     * Returns a list of the names in the async service registry. If the
     * registry server is not running a version where client services are
     * registered with the async registry, throws
     * InsufficientSerialVersionException.
     *
     * @return a list of service names
     * @throws InsufficientSerialVersionException if the registry is not at
     * high enough version to support client async services
     * @throws RemoteException if there is a network failure
     */
    static List<String> getServiceNames(@Nullable String storeName,
                                        String hostName,
                                        int registryPort,
                                        @Nullable ClientId clientId,
                                        Logger logger)
        throws RemoteException
    {
        final String opName = "list services";
        return getWithRemoteExceptions(
            getRegistry(storeName, hostName, registryPort, clientId, logger)
            .thenApply(registry ->
                       checkRegistryVersion(
                           registry, SerialVersion.MINIMUM,
                           opName))
            .thenCompose(registry ->
                         registry.list(registry.getDefaultTimeoutMs())),
            opName);
    }

    /**
     * Returns a client side initiator for the specified binding, or null if
     * the initiator is not found.
     */
    static <T> T
        getRemoteService(@Nullable String storeName,
                         String hostName,
                         int registryPort,
                         String bindingName,
                         @Nullable ClientId clientId,
                         Function<ServiceEndpoint, CompletableFuture<T>>
                         createProxy,
                         @Nullable DialogTypeFamily dialogTypeFamily,
                         short requiredSerialVersion,
                         Logger logger)
        throws RemoteException
    {
        final String opName = "get service for binding name " + bindingName;
        return getWithRemoteExceptions(
            getRegistry(storeName, hostName, registryPort, clientId, logger)
            .thenApply(registry ->
                       checkRegistryVersion(registry, requiredSerialVersion,
                                            opName))
            .thenCompose(registry ->
                         getServiceEndpoint(registry,
                                            registry.getDefaultTimeoutMs(),
                                            bindingName, dialogTypeFamily))
            .thenCompose(createProxy),
            opName);
    }

    /**
     * Returns an async service registry running on the specified host using
     * the default timeout.
     */
    public static CompletableFuture<ServiceRegistryAPI>
        getRegistry(@Nullable String storeName,
                    String hostName,
                    int registryPort,
                    @Nullable ClientId clientId,
                    Logger logger)
    {
        final ClientSocketFactory csf =
            RegistryUtils.getRegistryCSF(storeName, clientId);
        return getRegistry(hostName, registryPort, csf, clientId,
                           getTimeoutMs(csf), logger);
    }

    /**
     * Returns an async service registry running on the specified host using
     * the specified timeout.
     */
    private static CompletableFuture<ServiceRegistryAPI>
        getRegistry(String storeName,
                    String hostName,
                    int registryPort,
                    @Nullable ClientId clientId,
                    long timeout,
                    Logger logger) {
        final ClientSocketFactory csf =
            RegistryUtils.getRegistryCSF(storeName, clientId);
        return getRegistry(hostName, registryPort, csf, clientId, timeout,
                           logger);
    }

    public static CompletableFuture<ServiceRegistryAPI>
        getRegistry(String hostName,
                    int registryPort,
                    @Nullable ClientSocketFactory csf,
                    @Nullable ClientId clientId,
                    long timeout,
                    Logger logger) {
        try {
            final NetworkAddress networkAddress =
                NetworkAddress.createNetworkAddress(hostName, registryPort);
            /* Specify the local address, but only for TCP addresses */
            boolean needAddressResolution = false;
            /* This CSF can be null in tests */
            final InetSocketAddress localAddr =
                (csf != null) ? csf.getLocalAddr() : null;
            if (networkAddress.isInetAddress()) {
                if (localAddr != null) {
                    needAddressResolution = true;
                }
            }
            final CompletableFuture<NetworkAddress> addressResolution =
                needAddressResolution && (localAddr != null)
                ? NetworkAddress.convert(localAddr)
                : CompletableFuture.completedFuture(null);
            final EndpointConfig config = getRegistryEndpointConfig(csf);
            return addressResolution.thenCompose(
                (localNetworkAddress) ->
                getRegistryFactory().wrap(
                    new RetryingCreatorEndpoint(
                        String.format("getRegistry:%s", hostName),
                        networkAddress, localNetworkAddress,
                        config, logger),
                    clientId, timeout, getTimeoutMs(csf), logger)
                .exceptionally(
                    unwrapException(
                        checked(e -> {
                                throw translateRegistryException(
                                    e, hostName, registryPort);
                        }))));
        } catch (Throwable t) {
            return failedFuture(
                translateRegistryException(t, hostName, registryPort));
        }
    }

    /**
     * Returns the standard registry factory.
     */
    public static ServiceRegistryAPI.Factory getStandardRegistryFactory() {
        return registryFactory;
    }

    /**
     * Sets or clears the test registry factory.
     */
    public static void
        setTestRegistryFactory(ServiceRegistryAPI.@Nullable Factory factory)
    {
        testRegistryFactory = factory;
    }

    private static ServiceRegistryAPI.Factory getRegistryFactory() {
        return (testRegistryFactory != null) ?
            testRegistryFactory :
            registryFactory;
    }

    /** A ServiceRegistryAPI factory that translates exceptions. */
    private static class TranslatingExceptionsRegistryFactory
            extends ServiceRegistryAPI.Factory {
        @Override
        protected ServiceRegistryAPI createAPI(short serialVersion,
                                               long defaultTimeoutMs,
                                               ServiceRegistry remote,
                                               NetworkAddress remoteAddress) {
            return new ExceptionWrappingServiceRegistryAPI(
                serialVersion, defaultTimeoutMs, remote, remoteAddress);
        }
    }

    /**
     * Translates an underlying SSLHandshakeException into an
     * AuthenticationFailureException. Also includes information about the
     * storage node agent host and port for
     * ConnectionEndpointShutdownException and InitialConnectIOException.
     */
    private static
        Throwable translateRegistryException(Throwable exception,
                                             String hostName,
                                             int registryPort) {
        if (exception instanceof DialogException) {
            final DialogException dialogException =
                (DialogException) exception;
            final Throwable underlyingException =
                dialogException.getUnderlyingException();
            if (underlyingException instanceof SSLHandshakeException) {
                return new AuthenticationFailureException(underlyingException);
            }
            final Throwable cause = dialogException.getCause();
            if ((cause instanceof ConnectionEndpointShutdownException) ||
                (cause instanceof InitialConnectIOException)) {
                final ConnectionException connectionException =
                    (ConnectionException) dialogException.getCause();
                connectionException.setMessagePrefix(
                    "Unable to connect to the storage node agent at" +
                    " host " + hostName + ", port " + registryPort +
                    ", which may not be running. ");
            }
        }
        return exception;
    }

    /**
     * A ServiceRegistryAPI that provides better exceptions, performing the
     * same alterations as RegistryUtils.ExceptionWrappingRegistry.
     */
    private static class ExceptionWrappingServiceRegistryAPI
            extends ServiceRegistryAPI {
        private final NetworkAddress remoteAddress;
        ExceptionWrappingServiceRegistryAPI(short serialVersion,
                                            long defaultTimeoutMs,
                                            ServiceRegistry remote,
                                            NetworkAddress remoteAddress) {
            super(serialVersion, defaultTimeoutMs, remote);
            this.remoteAddress = remoteAddress;
         }
        @Override
        public CompletableFuture<ServiceEndpoint>
            lookup(String name,
                   @Nullable DialogTypeFamily family,
                   long timeout) {
            try {
                return translate(super.lookup(name, family, timeout));
            } catch (Throwable e) {
                return failedFuture(e);
            }
        }
        @Override
        public CompletableFuture<Void>
            bind(String name,
                 ServiceEndpoint endpoint,
                 long timeout) {
            try {
                return translate(super.bind(name, endpoint, timeout));
            } catch (Throwable e) {
                return failedFuture(e);
            }
        }
        @Override
        public CompletableFuture<Void> unbind(String name, long timeout) {
            try {
                return translate(super.unbind(name, timeout));
            } catch (Throwable e) {
                return failedFuture(e);
            }
        }
        @Override
        public CompletableFuture<List<String>> list(long timeout) {
            try {
                return translate(super.list(timeout));
            } catch (Throwable e) {
                return failedFuture(e);
            }
        }
        private <T> CompletableFuture<T>
            translate(CompletableFuture<T> future)
        {
            return future.exceptionally(
                unwrapException(
                    checked(e -> {
                            throw translateRegistryException(
                                e, remoteAddress.getHostName(),
                                remoteAddress.getPort());
                        })));
        }
    }

    /**
     * Create an async service registry.
     *
     * This is a blocking method as it calls AbstractEndpointGroup#listen.
     */
    public static ListenHandle createRegistry(String hostName,
                                              int registryPort,
                                              ServerSocketFactory ssf,
                                              Logger logger)
        throws IOException {

        checkNull("ssf", ssf);

        if (registryPort != 0) {
            ssf = ssf.newInstance(registryPort);
        }

        final ServiceRegistryImpl server = new ServiceRegistryImpl(logger);
        class ServiceRegistryDialogHandlerFactory
            implements DialogHandlerFactory {
            @Override
            public DialogHandler create() {
                return server.createDialogHandler();
            }
        }
        return getEndpointGroup().listen(
            ssf.getListenerConfig(hostName),
            SERVICE_REGISTRY_DIALOG_TYPE.getDialogTypeId(),
            new ServiceRegistryDialogHandlerFactory());
    }

    /**
     * Bind a service entry in the async service registry, throwing
     * RequestTimeoutException if the operation times out. Returns the
     * ListenHandle if the rebinding succeeded, otherwise throws an exception
     * and makes sure that the ListenHandle, if created, is shutdown.
     *
     * This is a blocking method as it calls rebind.
     */
    public static @Nullable ListenHandle
        rebind(String hostName,
               int registryPort,
               final String serviceName,
               final ServiceResourceId serviceId,
               final DialogTypeFamily dialogTypeFamily,
               final DialogHandlerFactory dialogHandlerFactory,
               final ClientSocketFactory clientSocketFactory,
               final ServerSocketFactory serverSocketFactory,
               Logger logger)
        throws RemoteException
    {
        if (!serverUseAsync ||
            ((dialogTypeFamily != REQUEST_HANDLER_TYPE_FAMILY) &&
             !RegistryUtils.serverMiscAsyncServices)) {
            return null;
        }
        final ClientSocketFactory registryCSF =
            RegistryUtils.getRegistryCSF(null /* storeName */,
                                         null /* clientId */);
        final long registryTimeout = getTimeoutMs(registryCSF);

        return rebind(hostName, registryPort, registryCSF, serviceName,
                      serviceId, dialogTypeFamily, dialogHandlerFactory,
                      clientSocketFactory, serverSocketFactory,
                      registryTimeout, logger);
    }

    /**
     * Binds a service entry in the async service registry.
     *
     * This is a blocking method as it calls listenAndRebind.
     */
    private static ListenHandle
        rebind(String hostName,
               int registryPort,
               @Nullable ClientSocketFactory registryCSF,
               String serviceName,
               ServiceResourceId serviceId,
               DialogTypeFamily dialogTypeFamily,
               DialogHandlerFactory dialogHandlerFactory,
               ClientSocketFactory clientSocketFactory,
               ServerSocketFactory serverSocketFactory,
               long registryTimeoutMillis,
               Logger logger)
        throws RemoteException
    {
        final long ts = System.currentTimeMillis();
        final CompletableFuture<ServiceRegistryAPI> cf =
            getRegistry(hostName, registryPort, registryCSF,
                        null /* clientId */, registryTimeoutMillis, logger);
        final long passed = System.currentTimeMillis() - ts;
        final long left = registryTimeoutMillis - passed;
        if (left <= 0) {
            throw new RequestTimeoutException(
                (int) registryTimeoutMillis,
                "AsyncRegistryUtils.rebind.getRegistry",
                null, false /* isRemote */);
        }
        final ServiceRegistryAPI registry =
            getWithRemoteExceptions(
                cf, "AsyncRegistryUtils.rebind.getRegistry",
                left);
        return listenAndRebind(serviceName,
                               serviceId,
                               registryPort,
                               dialogTypeFamily,
                               dialogHandlerFactory,
                               registry,
                               registryTimeoutMillis,
                               clientSocketFactory,
                               serverSocketFactory,
                               logger);
    }

    /**
     * Gets the value of a future, waiting for the specified amount of time.
     * Callers should make sure that timeoutMillis is greater than 0,
     * converting 0 to either a RequestTimeoutException or to a default timeout
     * as appropriate.
     *
     * @param <T> the type of the result
     * @param future the future to wait for
     * @param operation a description of the operation being performed
     * @param timeoutMillis the number of milliseconds to wait for the result
     * @return the result
     * @throws IllegalArgumentException if timeoutMillis is not greater than 0
     * @throws RuntimeException an unchecked exception thrown while attempting
     * to perform the operation will be rethrown
     * @throws FaultException if the future was canceled or interrupted, or if
     * a checked exception is thrown while attempting to perform the operation
     * @throws RequestTimeoutException if the timeout is reached before the
     * operation completes
     */
    public static <T> T getWithTimeout(CompletableFuture<T> future,
                                       String operation,
                                       long timeoutMillis) {
        return getWithTimeout(
            future, () -> operation, timeoutMillis, timeoutMillis,
            timeoutException -> new RequestTimeoutException(
                (int) timeoutMillis,
                "Timed out while waiting for result from: " + operation,
                timeoutException, false));
    }

    /**
     * Gets the value of a future, waiting for the specified amount of time,
     * specifying the original timeout, and providing a function that will
     * compute the timeout exception to throw. Callers should make sure that
     * timeoutMillis is greater than 0, converting 0 to either a
     * RequestTimeoutException or to a default timeout as appropriate.
     *
     * @param <T> the type of the result
     * @param future the future to wait for
     * @param operation a supplier of the description of the operation being
     * performed
     * @param originalTimeoutMillis the original number of milliseconds
     * specified for waiting
     * @param timeoutMillis the number of milliseconds to wait for the result
     * @param getTimeoutException a function that takes a TimeoutException and
     * returns the RuntimeException that should be thrown if a timeout is
     * detected
     * @return the result
     * @throws IllegalArgumentException if timeoutMillis is not greater than 0
     * @throws RuntimeException an unchecked exception thrown while attempting
     * to perform the operation will be rethrown
     * @throws FaultException if the future was canceled or interrupted, or if
     * a checked exception is thrown while attempting to perform the operation
     * @throws RequestTimeoutException if the timeout is reached before the
     * operation completes
     */
    public static <T>
        T getWithTimeout(CompletableFuture<T> future,
                         Supplier<String> operation,
                         long originalTimeoutMillis,
                         long timeoutMillis,
                         Function<TimeoutException, RuntimeException>
                         getTimeoutException) {
        if (originalTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                "Timeout should be greater than zero, found: " +
                originalTimeoutMillis);
        }
        try {
            try {
                return future.get(timeoutMillis, MILLISECONDS);
            } catch (Throwable t) {
                final Exception e = handleFutureGetException(t);
                throw new FaultException("Unexpected exception: " + e, e,
                                         false);
            }
        } catch (CancellationException e) {
            throw new FaultException(
                "Canceled while waiting for result from: " + operation.get(),
                e, false);
        } catch (InterruptedException e) {
            throw new FaultException(
                "Interrupted while waiting for result from: " +
                operation.get(),
                e, false);
        } catch (TimeoutException e) {
            throw getTimeoutException.apply(e);
        } catch (RuntimeException|Error e) {

            /*
             * Append the current stack trace so that exceptions originally
             * thrown in other threads can include the current thread's stack
             * trace, to help with debugging.
            */
            CommonLoggerUtils.appendCurrentStack(e);
            throw e;
        }
    }

    /**
     * Establish a listener for the service and bind the service endpoint in
     * the async service registry.
     *
     * This is a blocking method which calls getServerHostName, Future#get and
     * AbstractEndpointGroup#listen.
     */
    private static ListenHandle
        listenAndRebind(String serviceName,
                        ServiceResourceId serviceId,
                        int registryPort,
                        DialogTypeFamily dialogTypeFamily,
                        DialogHandlerFactory dialogHandlerFactory,
                        ServiceRegistryAPI registry,
                        long registryTimeoutMillis,
                        ClientSocketFactory csf,
                        ServerSocketFactory ssf,
                        Logger logger)
        throws RemoteException
    {
        logger.finest(() -> "AsyncRegistryUtils.listenAndRebind" +
                      " serviceName=" + serviceName +
                      " serviceId=" + serviceId +
                      " registryPort=" + registryPort +
                      " dialogTypeFamily=" + dialogTypeFamily);
        ListenHandle listenHandle = null;
        try {
            final long ts = System.currentTimeMillis();
            final String hostName = getServerHostName();
            final DialogType dialogType =
                new DialogType(dialogTypeFamily,
                               getDialogTypeNumber(serviceId, registryPort));
            listenHandle = getEndpointGroup().listen(
                ssf.getListenerConfig(hostName),
                dialogType.getDialogTypeId(),
                dialogHandlerFactory);
            final HostPort localAddr =
                listenHandle.getLocalAddress();
            final int port = (localAddr != null) ? localAddr.port() : 0;
            final NetworkAddress addr =
                NetworkAddress.createNetworkAddress(hostName, port);
            final CompletableFuture<Void> cf = registry.bind(serviceName,
                          new ServiceEndpoint(addr, dialogType, csf),
                          registryTimeoutMillis);
            final long passed = System.currentTimeMillis() - ts;
            final long left = registryTimeoutMillis - passed;
            if (left <= 0) {
                throw new RequestTimeoutException(
                    (int) registryTimeoutMillis,
                    "AsyncRegistryUtils.listenAndRebind", null,
                    false /* isRemote */);
            }
            getWithRemoteExceptions(
                cf, "AsyncRegistryUtils.listenAndRebind", left);
            cf.get(left, TimeUnit.MILLISECONDS);
            if (logger.isLoggable(Level.FINEST)) {
                logger.finest("AsyncRegistryUtils.listenAndRebind" +
                              " serviceName=" + serviceName +
                              " serviceId=" + serviceId +
                              " registryPort=" + registryPort +
                              " dialogTypeFamily=" + dialogTypeFamily +
                              " listenHandle=" + listenHandle);
            }
            return listenHandle;
        } catch (Throwable ex) {
            logger.fine(() -> "AsyncRegistryUtils.listenAndRebind" +
                        " serviceName=" + serviceName +
                        " serviceId=" + serviceId +
                        " registryPort=" + registryPort +
                        " dialogTypeFamily=" + dialogTypeFamily +
                        " exception=" + ex);
            if (listenHandle != null) {
                listenHandle.shutdown(false);
            }
            if (ex instanceof RemoteException) {
                throw (RemoteException) ex;
            }
            throw new RemoteException(
                "Can't rebind " + serviceName +
                " csf: " + csf + " ssf: " + ssf,
                ex);
        }
    }

    /**
     * Construct a unique type number from the given service ID and registry
     * port.
     *
     * The DialogType class already reserves a 100-valued field out of the
     * int-valued type number for the dialog family number, so we have
     * Integer.MAX_VALUE / 100 = 21,474,836 values left for this number. We
     * will use 20,000,000 of them.
     *
     * For services that are shard-based (RNs and arbiters), the service ID's
     * group ID represents the shard number and the node ID represents the
     * replica within the shard. As a practical matter, we probably can't
     * support more than 1,000 shards. The store replication factor is
     * typically 3, rarely greater than 5, and limited to 20, so a limit of 50
     * is sufficient. 100 would produce nicer output, but we don't have enough
     * space for that.
     *
     * For services that are not shard-based (admins and SNAs), the group ID is
     * 0. Limiting the total number of instances to 1,000 shouldn't be a
     * practical problem.
     *
     * Most dialog type families are limited to a single service type, but some
     * such as REMOTE_TEST_INTERFACE_TYPE_FAMILY can be used for all service
     * types. To make the dialog type number unique for all 4 possible service
     * types, include a 10-valued field for the service type.
     *
     * Tests sometimes create more than one store in the same VM, with each
     * store having its own registry port. Include a unique value for the
     * registry port if more than one registry port is being used. It seems
     * that our tests create at most 2 stores in the same VM, but we may need
     * to support more ports for tests with multiple test methods. In
     * particular, the ElasticityStress test creates 30 SNs in the same JVM, so
     * support 40 different ports. In most cases, if that number is not
     * sufficient, the test probably needs to be modified to make sure to
     * unexport services at the end of each test method.
     *
     * For shard-based services:
     *
     *           40       Registry Port
     *      X 1,000       Group ID
     *        X  50       Node number
     *         X 10       Service type
     *   ----------
     *   20,000,000       Total
     *
     * For non-shard-based services:
     *
     *           40       Registry Port
     *        X  50       (Unused)
     *      X 1,000       Node number
     *         X 10       Service type
     *  -----------
     *   20,000,000       Total
     *
     * Note that these numbers have been chosen for their (relative)
     * readability by using different digits for different subfields. It would
     * be possible to compress these numbers more by doing less rounding if
     * needed in the future.
     */
    static int getDialogTypeNumber(ServiceResourceId serviceId,
                                   int registryPort) {
        final int groupId = serviceId.getGroupId();
        if (groupId < 0) {
            throw new IllegalArgumentException(
                "Group ID is negative: " + serviceId);
        }
        final int nodeNum = serviceId.getNodeNum();
        if (nodeNum < 0) {
            throw new IllegalArgumentException(
                "Node number is negative: " + serviceId);
        }
        final int portNum = dialogTypeRegistryPortValues.computeIfAbsent(
            registryPort,
            k -> dialogTypeNextRegistryPortValue.getAndIncrement());
        final int serviceType = getServiceTypeNumber(serviceId);
        if (groupId == 0) {
            return (checkedMod(portNum, 40) * 500_000) +
                (checkedMod(nodeNum, 1_000) * 10) +
                checkedMod(serviceType, 10);
        }
        return (checkedMod(portNum, 40) * 500_000) +
            (checkedMod(groupId, 1_000) * 500) +
            (checkedMod(nodeNum, 50) * 10) +
            checkedMod(serviceType, 10);
    }

    /** Produce a descriptive string for a dialog type number, for testing. */
    public static String describeDialogTypeNumber(int dialogTypeNumber) {
        final int dialogFamilyNum =
            dialogTypeNumber % DialogType.MAX_TYPE_FAMILIES;
        dialogTypeNumber /= DialogType.MAX_TYPE_FAMILIES;
        final DialogTypeFamily dialogFamily =
            StandardDialogTypeFamily.getOrNull(dialogFamilyNum);
        final int serviceId = dialogTypeNumber % 10;
        dialogTypeNumber /= 10;
        final ResourceType serviceType = getServiceResourceType(serviceId);
        final int groupId;
        final int nodeNum;
        switch (serviceType) {
        case STORAGE_NODE:
        case ADMIN:
            nodeNum = dialogTypeNumber % 1000;
            dialogTypeNumber /= 1000;
            groupId = 0;
            dialogTypeNumber /= 50;
            break;
        case REP_NODE:
        case ARB_NODE:
            nodeNum = dialogTypeNumber % 50;
            dialogTypeNumber /= 50;
            groupId = dialogTypeNumber % 1000;
            dialogTypeNumber /= 1000;
            break;
        default:
            throw new AssertionError();
        }
        final int portNum = dialogTypeNumber % 40;
        return String.format(
            "dialogTypeNumber[port=%d group=%d node=%d service=%s(%d)" +
            " family=%s]",
            portNum,
            groupId,
            nodeNum,
            serviceType,
            serviceId,
            (dialogFamily != null) ? dialogFamily : dialogFamilyNum);
    }

    /**
     * Returns the service resource ID ResourceType for a service type number
     * value encoded in a dialog type number.
     */
    static ResourceType getServiceResourceType(int serviceTypeNumber) {
        switch (serviceTypeNumber) {
        case 0:
            return ResourceType.STORAGE_NODE;
        case 1:
            return ResourceType.REP_NODE;
        case 2:
            return ResourceType.ADMIN;
        case 3:
            return ResourceType.ARB_NODE;
        default:
            throw new IllegalArgumentException(
                "Service type not found for service type number: " +
                serviceTypeNumber);
        }
    }

    /**
     * Returns the service type number value to encode in a dialog type number
     * for a service resource ID.
     */
    static int getServiceTypeNumber(ServiceResourceId serviceId) {
        switch (serviceId.getType()) {
        case STORAGE_NODE:
            return 0;
        case REP_NODE:
            return 1;
        case ADMIN:
            return 2;
        case ARB_NODE:
            return 3;
        default:
            throw new AssertionError();
        }
    }

    /**
     * Compute value % mod, but check during testing that the value was in the
     * range [0, mod). We don't expect the value to fall outside that range. If
     * it does, we want to just truncate in production, because things might
     * still work, but fail during testing, so we can adjust the type number
     * encoding.
     */
    private static int checkedMod(int value, int mod) {
        final int result = value % mod;
        assert !TestStatus.isActive() ||
            ((result == value) && (value >= 0))
            : "value=" + value + " mod=" + mod;
        return result;
    }

    /** Reset port values, for testing */
    static void resetDialogTypeRegistryPortValues() {
        dialogTypeNextRegistryPortValue.set(0);
        dialogTypeRegistryPortValues.clear();
    }

    /**
     * Unbind a service entry in the async service registry and shutdown the
     * ListenHandle, if not null, throwing RequestTimeoutException if the
     * operation times out. Does nothing if async is disabled for the server.
     */
    public static void unbind(String hostName,
                              int registryPort,
                              final String serviceName,
                              @Nullable ListenHandle listenHandle,
                              Logger logger)
        throws RemoteException
    {
        if (!serverUseAsync) {
            return;
        }
        logger.finest(() ->
                      "AsyncRegistryUtils.unbind" +
                      " hostName=" + hostName +
                      " registryPort=" + registryPort +
                      " serviceName=" + serviceName +
                      " listenHandle=" + listenHandle);
        final ClientSocketFactory csf =
            RegistryUtils.getRegistryCSF(null /* storeName */,
                                         null /* clientId */);
        final long registryTimeout = getTimeoutMs(csf);

        /*
         * Two registry operations -- get registry and unbind -- and check for
         * overflow
         */
        long twiceTimeout = 2 * registryTimeout;
        if (twiceTimeout <= 0) {
            twiceTimeout = Long.MAX_VALUE;
        }

        getWithRemoteExceptions(
            getRegistry(hostName, registryPort, csf, null /* clientId */,
                        registryTimeout, logger)
            .thenCompose(registry ->
                         unbind(registry, serviceName, registryTimeout))
            .whenComplete((ignoreResult, e) -> {
                    logger.finest(() -> "AsyncRegistryUtils.unbind" +
                                  " hostName=" + hostName +
                                  " registryPort=" + registryPort +
                                  " serviceName=" + serviceName +
                                  " exception=" + e);

                    /*
                     * Also attempt to shutdown the listen handle if non-null.
                     * Specify force=false, which for async means to allow
                     * current calls to finish.
                     */
                    if (listenHandle != null) {
                        listenHandle.shutdown(false);
                    }
                }),
            "AsyncRegistryUtils.unbind",
            twiceTimeout);
    }

    private static CompletableFuture<Void>
        unbind(ServiceRegistryAPI registry,
               String serviceName,
               long timeout) {
        try {
            checkNull("registry", registry);
            return registry.unbind(serviceName, timeout);
        } catch (Throwable e) {
            return failedFuture(e);
        }
    }
}
