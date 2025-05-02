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

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT;
import static oracle.kv.impl.admin.param.GlobalParams.COMMAND_SERVICE_NAME;
import static oracle.kv.impl.async.FutureUtils.checked;
import static oracle.kv.impl.async.StandardDialogTypeFamily.ARB_NODE_ADMIN_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.CLIENT_ADMIN_SERVICE_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.COMMAND_SERVICE_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.MONITOR_AGENT_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REMOTE_TEST_INTERFACE_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REP_NODE_ADMIN_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.REQUEST_HANDLER_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.TRUSTED_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.RemoteObject;
import java.rmi.server.RemoteRef;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLHandshakeException;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.admin.CommandService;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.CommandServiceInitiator;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.RequestHandler;
import oracle.kv.impl.api.RequestHandlerAPI;
import oracle.kv.impl.arb.admin.ArbNodeAdmin;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.arb.admin.ArbNodeAdminInitiator;
import oracle.kv.impl.async.DialogHandlerFactory;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.registry.ServiceEndpoint;
import oracle.kv.impl.client.admin.ClientAdminService;
import oracle.kv.impl.client.admin.ClientAdminServiceAPI;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.monitor.MonitorAgent;
import oracle.kv.impl.monitor.MonitorAgentAPI;
import oracle.kv.impl.monitor.MonitorAgentInitiator;
import oracle.kv.impl.rep.admin.ClientRepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminInitiator;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.TrustedLogin;
import oracle.kv.impl.security.login.TrustedLoginAPI;
import oracle.kv.impl.security.login.TrustedLoginInitiator;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.security.login.UserLoginAPI;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeAgentInterface;
import oracle.kv.impl.sna.StorageNodeAgentInterfaceInitiator;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.test.RemoteTestInterfaceInitiator;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.ServiceResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.NonNullByDefault;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils.InitiatorProxyFactory;
import oracle.kv.impl.util.registry.AsyncRegistryUtils.InsufficientSerialVersionException;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.ssl.SSLClientSocketFactory;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * RegistryUtils encapsulates the operations around registry lookups and
 * registry bindings. It's responsible for producing RMI binding names for the
 * various remote interfaces used by the store, and wrapping them in an
 * appropriate API class.
 * <p>
 * All KV store registry binding names are of the form:
 * <p>
 * { kvstore name } : { component full name } :
 *                         [ main | monitor | admin | login | trusted_login ]
 * <p>
 * This class is responsible for the construction of these names. Examples:
 * FooStore:sn1:monitor names the monitor interface of the SNA.
 * Foostore:rg5-rn12:admin names the admin interface of the RN rg5-rn2.
 *
 * In future we may make provisions for making a version be a part of the name,
 * e.g. FooStore:sn1:monitor:5, for version 5 of the SNA monitor interface.
 * <p>
 * Note that all exception handling is done by the invokers of the get/rebind
 * methods.
 * <p>
 * While server processes only access a single store, applications may access
 * multiple stores. To support applications with multiple stores, this class
 * uses storeToRegistryCSFMap to look up right registry for a given store. Look
 * up by client ID is best because client IDs are unique, and that is now the
 * intended way for registries to be found. Although we recommend that stores
 * have unique names, we do not enforce uniqueness for store names, so we can't
 * depend on them being unique.
 * <p>
 * But there are complications when testing because test environments often
 * include both client and server facilities in the same process. To handle
 * this situation in cases where tests use multiple stores, the tests were
 * written to depend on store-name-based lookups to access the proper
 * registries, and used unique store names for each store. To continue to
 * support these tests, registry lookup continues to support lookup by store
 * name. It also permits a single registry supplied for server use to be found
 * by clients, and a single client-supplied registry to be found be servers,
 * since that behavior was already present and depended upon by existing tests.
 * When tests are written that use multiple stores with the same name, they
 * need to isolate server components by store in their own processes, as seen
 * in MultiStoreTest.
 *
 * @see VersionedRemote
 */
@NonNullByDefault
public class RegistryUtils {

    /**
     * The exception message used in a ConnectIOException that is thrown when
     * there may be a security mismatch between the client and the server.
     */
    public static final String POSSIBLE_SECURITY_MISMATCH_MESSAGE =
        "Problem connecting to the storage node agent, which may be caused" +
        " by a security mismatch between the client and server, or by" +
        " another network connectivity issue";

    /**
     * The exception message used in a ConnectIOException that is thrown when
     * there may be an Async vs. RMI mismatch between the client and the
     * server.
     */
    public static final String POSSIBLE_ASYNC_MISMATCH_MESSAGE =
        "Problem connecting to the storage node agent, which may be caused" +
        " by a client using the async network protocol with a server that" +
        " does not support that protocol, or by another network" +
        " connectivity issue";

    /** Test hook called on registry lookup with the name of the entry */
    public static volatile @Nullable TestHook<String> registryLookupHook;

    /*
     * A test hook that is called at the start of getRequestHandler with the RN
     * ID of the handler to get.
     */
    public static volatile @Nullable TestHook<RepNodeId> getRequestHandlerHook;

    /**
     * The name of an internal system property that specifies whether to use
     * asynchronous versions of miscellaneous services used by clients,
     * services, and utilities.
     */
    public static final String USE_MISC_ASYNC_SERVICES =
        "oracle.kv.use.misc.async.services";

    /**
     * Whether to initiate calls to asynchronous versions of miscellaneous
     * services from clients, services, and utilities.
     */
    public static final boolean useMiscAsyncServices =
        Boolean.parseBoolean(
            System.getProperty(USE_MISC_ASYNC_SERVICES,
                               String.valueOf(AsyncControl.serverUseAsync)));

    /**
     * The name of an internal system property that specifies whether to enable
     * async servers for miscellaneous services.
     */
    public static final String SERVER_MISC_ASYNC_SERVICES =
        "oracle.kv.server.misc.async.services";

    /**
     * Whether to enable async servers for miscellaneous services.
     */
    public static final boolean serverMiscAsyncServices =
        Boolean.parseBoolean(
            System.getProperty(SERVER_MISC_ASYNC_SERVICES,
                               String.valueOf(AsyncControl.serverUseAsync)));

    /**
     * The name of the system property that specifies whether to use RMI on the
     * server side to receive calls.
     */
    public static final String SERVER_USE_RMI = "oracle.kv.rmi.server";

    /**
     * Whether to enable RMI by default on the server side to receive calls.
     */
    private static final boolean serverUseRmiDefault = true;

    /**
     * If true, enables RMI on the server side to receive calls.
     */
    public static volatile boolean serverUseRmi =
        (System.getProperty(SERVER_USE_RMI) == null) ?
        serverUseRmiDefault :
        Boolean.getBoolean(SERVER_USE_RMI);

    /**
     * The global client socket factory, used in server contexts where only a
     * single store is being accessed, or null if not yet set. Synchronize on
     * the class when accessing this field.
     */
    private static @Nullable ClientSocketFactory globalRegistryCSF = null;

    /**
     * Map from store client ID or store name to registry ClientSocketFactory.
     * Synchronize on the class when accessing this field.
     */
    private static final Map<Object, ClientSocketFactory>
        storeToRegistryCSFMap = new HashMap<>();

    /**
     * A regular expression used to parse the port number from the string
     * representation of remote object, which may contain the endpoint info:
     * endpoint:[host:port<,RMIServerSocketFactory><,RMIClientSocketFactory>]
     * where the info enclosed with <> are optional
     */
    private static final Pattern PARSE_PORT_REGEX =
        Pattern.compile("endpoint:\\[.*:(\\d{1,5})[,\\]]");

    /**
     * A regular expression that matches a RepNodeAdmin service, for example
     * storename:rg1-rn1:ADMIN. For consistency with previous behavior, the RN
     * name is case insensitive.
     */
    private static final Pattern REP_NODE_ADMIN_REGEX =
        Pattern.compile(".*:[Rr][Gg]\\d+-[Rr][Nn]\\d+:ADMIN");

    /**
     * A logger to use for debugging, even though we don't have access to the
     * real logger.
     */
    private static final Logger debugLogger =
        ClientLoggerUtils.getLogger(RegistryUtils.class, "debugging");

    /** Logging level for debug logging, to be adjusted during debugging. */
    private static final Level debugLoggerLevel = Level.FINEST;

    /** Cache of remote service objects. */
    private static final ServiceCache serviceCache = new ServiceCache();

    /* Used to separate the different name components */
    private static final String BINDING_NAME_SEPARATOR = ":";

    /* The topology that is the basis for generation of binding names. */
    private final Topology topology;

    /*
     * The login manager to be used for initializing APIs or null if
     * non-secure
     */
    private final @Nullable LoginManager loginMgr;

    /** The client ID of the associated client, or null. */
    private final @Nullable ClientId clientId;

    private final Logger logger;

    /*
     * The different interface types associated with a remotely accessed
     * topology component. The component, depending upon its function, may only
     * implement a subset of the interfaces listed below.
     */
    public enum InterfaceType {

        MAIN,     /* The main interface. */
        MONITOR,  /* The monitoring interface. */
        ADMIN,    /* The administration interface. */
        TEST,     /* The test interface. */
        LOGIN,    /* The standard login interface */
        TRUSTED_LOGIN; /* The trusted login interface */

        public String interfaceName() {
            return name().toLowerCase();
        }
    }

    /**
     * Create an instance of this class for use in a server context.
     *
     * @param topology the topology used as the basis for operations
     * @param loginMgr a null-allowable LoginManager to be used for
     *  authenticating remote access.  When accessing a non-secure instance
     *  or when executing only operations that do not require authentication,
     *  this may be null.
     * @param logger for debug logging
     */
    public RegistryUtils(Topology topology,
                         @Nullable LoginManager loginMgr,
                         Logger logger) {
        this(topology, loginMgr, null /* clientId */, logger);
    }

    /**
     * Create an instance of this class, passing a client ID if needed for a
     * client context.
     *
     * @param topology the topology used as the basis for operations
     * @param loginMgr a null-allowable LoginManager to be used for
     *  authenticating remote access.  When accessing a non-secure instance
     *  or when executing only operations that do not require authentication,
     *  this may be null.
     * @param clientId the resource ID of the client, may be null in a server
     * context
     * @param logger for debug logging
     */
    public RegistryUtils(Topology topology,
                         @Nullable LoginManager loginMgr,
                         @Nullable ClientId clientId,
                         Logger logger) {
        this.topology = topology;
        this.loginMgr = loginMgr;
        this.clientId = clientId;
        this.logger = checkNull("logger", logger);
    }

    /** Returns the topology supplied to the constructor. */
    public Topology getTopology() {
        return topology;
    }

    /**
     * Returns the login manager supplied to the constructor, possibly null.
     */
    public @Nullable LoginManager getLoginManager() {
        return loginMgr;
    }

    /** Returns the client ID supplied to the constructor, possibly null. */
    public @Nullable ClientId getClientId() {
        return clientId;
    }

    /**
     * Returns the API wrapper for the RMI stub associated with request handler
     * for the RN identified by <code>repNodeId</code>. If the RN is not
     * present in the topology <code>null</code> is returned.
     */
    public @Nullable RequestHandlerAPI getRequestHandler(RepNodeId repNodeId)
        throws RemoteException, NotBoundException {

        assert TestHookExecute.doHookIfSet(getRequestHandlerHook,
                                           repNodeId);

        final RepNode repNode = topology.get(repNodeId);

        if (!serverUseRmi) {
            throw new IllegalStateException(
                "Attempt to use RMI request handler when RMI is disabled");
        }

        return (repNode == null) ?
                null :
                RequestHandlerAPI.wrap
                       ((RequestHandler) lookup(repNodeId.getFullName(),
                                                InterfaceType.MAIN,
                                                repNode.getStorageNodeId()));
    }

    /**
     * Returns the API wrapper for the remote handle associated with the
     * administration of the RN identified by <code>repNodeId</code>.
     */
    public RepNodeAdminAPI getRepNodeAdmin(RepNodeId repNodeId)
        throws RemoteException, NotBoundException {

        final RepNode repNode = topology.get(repNodeId);
        final StorageNodeId storageNodeId = repNode.getStorageNodeId();
        final StorageNode storageNode = topology.get(storageNodeId);
        return getRepNodeAdmin(topology.getKVStoreName(),
                               storageNode.getHostname(),
                               storageNode.getRegistryPort(),
                               repNodeId, loginMgr, logger);
    }

    /**
     * Returns the API of the client admin interface for the RN with the
     * specified ID.
     */
    public ClientRepNodeAdminAPI getClientRepNodeAdmin(RepNodeId repNodeId,
                                                       Protocols protocols)
        throws RemoteException, NotBoundException {

        final String storeName = topology.getKVStoreName();
        final RepNode repNode = topology.get(repNodeId);
        final String bindingName =
            RegistryUtils.bindingName(storeName, repNodeId.getFullName(),
                                      RegistryUtils.InterfaceType.ADMIN);
        final StorageNode sn = topology.get(repNode.getStorageNodeId());
        return getClientRepNodeAdmin(storeName,
                                     sn.getHostname(),
                                     sn.getRegistryPort(),
                                     bindingName,
                                     loginMgr,
                                     clientId,
                                     protocols,
                                     false /* ignoreWrongType */,
                                     logger);
    }

    /**
     * Returns the API of the RN client admin interface registered with the
     * specified service binding.
     *
     * If ignoreWrongType is true, then treats the service as missing if its
     * type is wrong.
     */
    public static ClientRepNodeAdminAPI
        getClientRepNodeAdmin(@Nullable String storeName,
                              String hostName,
                              int registryPort,
                              String bindingName,
                              @Nullable LoginManager loginMgr,
                              @Nullable ClientId clientId,
                              Protocols protocols,
                              boolean ignoreWrongType,
                              Logger logger)
        throws RemoteException, NotBoundException {

        final LoginHandle loginHandle =
            getLogin(loginMgr, hostName, registryPort, ResourceType.REP_NODE);
        final RepNodeAdmin rnAdmin = getRemoteService(
            storeName, hostName, registryPort, bindingName, clientId,
            checked(endpoint -> AsyncRegistryUtils.getClientRepNodeAdmin(
                        endpoint, bindingName, loginHandle, logger)
                    .thenApply(api ->
                               (api == null) ? null : api.createSyncProxy())),
            !ignoreWrongType ? REP_NODE_ADMIN_TYPE_FAMILY : null,
            SerialVersion.MINIMUM, protocols, logger);
        return ClientRepNodeAdminAPI.wrap(rnAdmin, loginHandle);
    }

    /**
     * A RepNodeAdmin interface service name will look like this:
     * storename:rg1-rn1:ADMIN
     */
    public static boolean isRepNodeAdmin(String serviceName) {
        return REP_NODE_ADMIN_REGEX.matcher(serviceName).matches();
    }

    /**
     * Returns the API of the user login interface for the RN with the
     * specified ID.
     */
    public UserLoginAPI getRepNodeLogin(RepNodeId repNodeId,
                                        Protocols protocols)
        throws RemoteException, NotBoundException {

        final RepNode repNode = topology.get(repNodeId);
        final StorageNode storageNode =
            topology.get(repNode.getStorageNodeId());

        return getRepNodeLogin(topology.getKVStoreName(),
                               storageNode.getHostname(),
                               storageNode.getRegistryPort(),
                               repNodeId,
                               loginMgr,
                               clientId,
                               protocols,
                               logger);
    }

    /**
     * Check whether the service name appears to be a valid RepNode UserLogin
     * interface for some kvstore.
     * @return the store name from the RN login entry if this appears
     * to be a valid rep node login and null otherwise.
     */
    public static @Nullable String isRepNodeLogin(String serviceName) {
        /*
         * A RepNode UserLogin interface will look like this:
         * storename:rg1-rn1:LOGIN
         */
        final int firstSep = serviceName.indexOf(BINDING_NAME_SEPARATOR);
        if (firstSep < 0) {
            return null;
        }
        if (serviceName.toLowerCase().indexOf(
                RepGroupId.getPrefix(), firstSep) > 0 &&
                serviceName.endsWith(
                    BINDING_NAME_SEPARATOR + InterfaceType.LOGIN.toString())) {
            return serviceName.substring(0, firstSep);
        }
        return null;
    }

    /**
     * Returns the API wrapper for the remote handle associated with the test
     * interface of the RN identified by <code>repNodeId</code>.
     */
    public RemoteTestAPI getRepNodeTest(RepNodeId repNodeId)
        throws RemoteException, NotBoundException {

        final RepNode repNode = topology.get(repNodeId);
        return getRemoteTest(repNodeId.getFullName(),
                             repNode.getStorageNodeId());
    }

    private RemoteTestAPI getRemoteTest(String serviceName,
                                        StorageNodeId storageNodeId)
        throws RemoteException, NotBoundException {

        final StorageNode storageNode = topology.get(storageNodeId);
        final String bindingName =
            bindingName(serviceName, InterfaceType.TEST);
        return getRemoteTest(topology.getKVStoreName(),
                             storageNode.getHostname(),
                             storageNode.getRegistryPort(),
                             bindingName,
                             logger);
    }

    public static RemoteTestAPI getRemoteTest(@Nullable String storeName,
                                              String hostName,
                                              int registryPort,
                                              String bindingName,
                                              Logger logger)
        throws RemoteException, NotBoundException {

        final RemoteTestInterface test =
            getRemoteService(storeName, hostName, registryPort, bindingName,
                             RemoteTestInterfaceInitiator::createProxy,
                             REMOTE_TEST_INTERFACE_TYPE_FAMILY, logger);
        return RemoteTestAPI.wrap(test);
    }

    /**
     * Returns the API wrapper for the remote handle associated with the
     * monitor for the service identified by <code>resourceId</code>. This
     * should only be called for components that support monitoring. An attempt
     * to obtain an agent for a non-monitorable component will result in an
     * {@link IllegalStateException}.
     */
    public static MonitorAgentAPI getMonitor(String storeName,
                                             String hostName,
                                             int registryPort,
                                             ResourceId resourceId,
                                             @Nullable LoginManager loginMgr,
                                             Logger logger)
        throws RemoteException, NotBoundException {

        final String bindingName = bindingName(
            storeName, resourceId.getFullName(), InterfaceType.MONITOR);
        final MonitorAgent monitorAgent =
            getRemoteService(storeName, hostName, registryPort,
                             bindingName, MonitorAgentInitiator::createProxy,
                             MONITOR_AGENT_TYPE_FAMILY, logger);
        return MonitorAgentAPI.wrap(monitorAgent,
                                    getLogin(loginMgr, hostName, registryPort,
                                             resourceId.getType()));
    }

    /**
     * For compatibility with httpproxy. Remove when the proxy uses the logger
     * overloading introduced in 21.3.
     */
    public static CommandServiceAPI getAdmin(String hostName,
                                             int registryPort,
                                             @Nullable LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        return getAdmin(hostName, registryPort, loginMgr,
                        ClientLoggerUtils.getLogger(RegistryUtils.class,
                                                    "httpproxy"));
    }

    /**
     * Looks up the Admin (CommandService) in the registry without benefit of
     * the Topology. Used to bootstrap storage node registration.
     * @throws RemoteException
     * @throws NotBoundException
     */
    public static CommandServiceAPI getAdmin(String hostName,
                                             int registryPort,
                                             @Nullable LoginManager loginMgr,
                                             Logger logger)
        throws RemoteException, NotBoundException {

        return getAdmin(null /* storeName */, hostName, registryPort,
                        loginMgr, Protocols.getDefault(), logger);
    }

    /**
     * Look up an Admin (CommandService) when a Topology is available.
     * @throws RemoteException
     * @throws NotBoundException
     */
    public CommandServiceAPI getAdmin(StorageNodeId snid)
        throws RemoteException, NotBoundException {

        final StorageNode sn = topology.get(snid);
        return getAdmin(topology.getKVStoreName(), sn.getHostname(),
                        sn.getRegistryPort(), loginMgr, Protocols.getDefault(),
                        logger);
    }

    /** Look up an Admin and specify protocols */
    public static CommandServiceAPI getAdmin(@Nullable String storeName,
                                             String hostName,
                                             int registryPort,
                                             @Nullable LoginManager loginMgr,
                                             Protocols protocols,
                                             Logger logger)
        throws RemoteException, NotBoundException {

        final CommandService admin =
            getRemoteService(storeName, hostName, registryPort,
                             COMMAND_SERVICE_NAME,
                             CommandServiceInitiator::createProxy,
                             COMMAND_SERVICE_TYPE_FAMILY, protocols, logger);
        return CommandServiceAPI.wrap(admin,
                                      getLogin(loginMgr, hostName,
                                               registryPort,
                                               ResourceType.ADMIN));
    }

    public RemoteTestAPI getAdminTest(StorageNodeId snid)
        throws RemoteException, NotBoundException {

        final StorageNode storageNode = topology.get(snid);
        return getAdminTest(storageNode.getHostname(),
                            storageNode.getRegistryPort(),
                            logger);
    }

    public static RemoteTestAPI getAdminTest(String hostName,
                                             int registryPort,
                                             Logger logger)
        throws RemoteException, NotBoundException {

        return getRemoteTest(null /* storeName */, hostName, registryPort,
                            GlobalParams.COMMAND_SERVICE_TEST_NAME, logger);
    }

    public static UserLoginAPI getAdminLogin(String hostName,
                                             int registryPort,
                                             @Nullable LoginManager loginMgr,
                                             Logger logger)
        throws RemoteException, NotBoundException {

        return getUserLogin(null /* storeName */, hostName, registryPort,
                            GlobalParams.ADMIN_LOGIN_SERVICE_NAME,
                            ResourceType.ADMIN, loginMgr, null /* clientId */,
                            SerialVersion.MINIMUM,
                            Protocols.getDefault(),
                            false /* ignoreWrongType */, logger);
    }

    /**
     * Returns the API of the client interface for the master Admin on the
     * specified host, for use without benefit of the Topology. Used to execute
     * DDL operations.
     *
     * @throws RemoteException
     * @throws NotBoundException
     */
    public static ClientAdminServiceAPI
        getClientAdminService(@Nullable String storeName,
                              String hostName,
                              int registryPort,
                              @Nullable LoginManager loginMgr,
                              @Nullable ClientId clientId,
                              Protocols protocols,
                              Logger logger)
        throws RemoteException, NotBoundException {

        final LoginHandle loginHandle =
            getLogin(loginMgr, hostName, registryPort, ResourceType.ADMIN);
        final ClientAdminService adminService = getRemoteService(
            storeName, hostName, registryPort,
            GlobalParams.CLIENT_ADMIN_SERVICE_NAME, clientId,
            checked(endpoint -> AsyncRegistryUtils.getClientAdminService(
                        endpoint, loginHandle, logger)
                    .thenApply(api ->
                               (api == null) ? null : api.createSyncProxy())),
            CLIENT_ADMIN_SERVICE_TYPE_FAMILY,
            SerialVersion.MINIMUM, protocols, logger);
        return ClientAdminServiceAPI.wrap(adminService, loginHandle);
    }

    /**
     * Returns the API of the client interface for the master Admin associated
     * with the specified SN, for use when a Topology is available. Used to
     * execute DDL operations.
     */
    public ClientAdminServiceAPI getClientAdminService(StorageNodeId snid,
                                                       Protocols protocols)
        throws RemoteException, NotBoundException {

        final StorageNode storageNode = topology.get(snid);
        return getClientAdminService(topology.getKVStoreName(),
                                     storageNode.getHostname(),
                                     storageNode.getRegistryPort(),
                                     loginMgr,
                                     clientId,
                                     protocols,
                                     logger);
    }

    /**
     * Returns an API wrapper for the remote handle to the RepNodeAdmin.
     */
    public static
        RepNodeAdminAPI getRepNodeAdmin(String storeName,
                                        String hostName,
                                        int registryPort,
                                        RepNodeId rnId,
                                        @Nullable LoginManager loginMgr,
                                        Logger logger)
        throws RemoteException, NotBoundException {

        final String bindingName =
            bindingName(storeName, rnId.getFullName(), InterfaceType.ADMIN);
        return getRepNodeAdmin(storeName, hostName, registryPort, bindingName,
                               loginMgr, logger);
    }

    public static
        RepNodeAdminAPI getRepNodeAdmin(@Nullable String storeName,
                                        String hostName,
                                        int registryPort,
                                        String bindingName,
                                        @Nullable LoginManager loginMgr,
                                        Logger logger)
        throws RemoteException, NotBoundException {

        final RepNodeAdmin repNodeAdmin =
            getRemoteService(storeName, hostName, registryPort, bindingName,
                             RepNodeAdminInitiator::createProxy,
                             REP_NODE_ADMIN_TYPE_FAMILY, logger);
        return RepNodeAdminAPI.wrap(repNodeAdmin,
                                    getLogin(loginMgr, hostName, registryPort,
                                             ResourceType.REP_NODE));
    }

    /**
     * Returns an API wrapper for the remote handle to the ArbNodeAdmin.
     */
    public static
        ArbNodeAdminAPI getArbNodeAdmin(String storeName,
                                        String hostName,
                                        int registryPort,
                                        ArbNodeId arbId,
                                        @Nullable LoginManager loginMgr,
                                        Logger logger)
        throws RemoteException, NotBoundException {

        final String bindingName =
            bindingName(storeName, arbId.getFullName(), InterfaceType.ADMIN);
        final ArbNodeAdmin arbNodeAdmin =
            getRemoteService(storeName, hostName, registryPort, bindingName,
                             ArbNodeAdminInitiator::createProxy,
                             ARB_NODE_ADMIN_TYPE_FAMILY, logger);
        return ArbNodeAdminAPI.wrap(arbNodeAdmin,
                                    getLogin(loginMgr, hostName, registryPort,
                                             ResourceType.ARB_NODE));
    }

    /**
     * Returns the API of the user login interface for the RN with the
     * specified ID, using the default for whether to attempt to use the async
     * network protocol and a default logger. This method is provided for
     * compatibility for the HTTP proxy and show only be used for that purpose.
     * Since different loggers are typically used for different KVStore
     * instances, this method should only be used in cases where there is only
     * one KVStore active.
     */
    public static UserLoginAPI getRepNodeLogin(String storeName,
                                               String hostName,
                                               int registryPort,
                                               RepNodeId rnId,
                                               @Nullable LoginManager loginMgr)
        throws RemoteException, NotBoundException {

        return getRepNodeLogin(storeName, hostName, registryPort, rnId,
                               loginMgr,
                               ClientLoggerUtils.getLogger(RegistryUtils.class,
                                                           "UnknownClient"));
    }

    /**
     * Returns the API of the user login interface for the RN with the
     * specified ID, using the default for whether to attempt to use the async
     * network protocol.
     */
    public static UserLoginAPI getRepNodeLogin(String storeName,
                                               String hostName,
                                               int registryPort,
                                               RepNodeId rnId,
                                               @Nullable LoginManager loginMgr,
                                               Logger logger)
        throws RemoteException, NotBoundException {

        return getRepNodeLogin(storeName, hostName, registryPort,
                               rnId, loginMgr, null /* clientId */,
                               Protocols.getDefault(), logger);
    }

    /**
     * Returns the API of the user login interface for the RN with the
     * specified ID.
     */
    public static UserLoginAPI getRepNodeLogin(String storeName,
                                               String hostName,
                                               int registryPort,
                                               RepNodeId rnId,
                                               @Nullable LoginManager loginMgr,
                                               @Nullable ClientId clientId,
                                               Protocols protocols,
                                               Logger logger)
        throws RemoteException, NotBoundException {

        final String bindingName = bindingName(
            storeName, rnId.getFullName(), InterfaceType.LOGIN);
        return getRepNodeLogin(storeName, hostName, registryPort,
                               bindingName, loginMgr, clientId, protocols,
                               false /* ignoreWrongType */, logger);
    }

    /**
     * Returns the API of the RN user login interface registered with the
     * specified service binding.
     *
     * If ignoreWrongType is true, then treats the service as missing if its
     * type is wrong.
     */
    public static UserLoginAPI getRepNodeLogin(@Nullable String storeName,
                                               String hostName,
                                               int registryPort,
                                               String bindingName,
                                               @Nullable LoginManager loginMgr,
                                               @Nullable ClientId clientId,
                                               Protocols protocols,
                                               boolean ignoreWrongType,
                                               Logger logger)
        throws RemoteException, NotBoundException {

        return getUserLogin(storeName, hostName, registryPort, bindingName,
                            ResourceType.REP_NODE, loginMgr, clientId,
                            protocols, ignoreWrongType, logger);
    }

    public static UserLoginAPI getUserLogin(@Nullable String storeName,
                                            String hostName,
                                            int registryPort,
                                            String bindingName,
                                            ResourceType resourceType,
                                            @Nullable LoginManager loginMgr,
                                            @Nullable ClientId clientId,
                                            Protocols protocols,
                                            boolean ignoreWrongType,
                                            Logger logger)
        throws RemoteException, NotBoundException {

        return getUserLogin(storeName, hostName, registryPort, bindingName,
                            resourceType, loginMgr,
                            clientId, SerialVersion.MINIMUM,
                            protocols, ignoreWrongType, logger);
    }

    private static UserLoginAPI getUserLogin(@Nullable String storeName,
                                             String hostName,
                                             int registryPort,
                                             String bindingName,
                                             ResourceType resourceType,
                                             @Nullable LoginManager loginMgr,
                                             @Nullable ClientId clientId,
                                             short requiredSerialVersion,
                                             Protocols protocols,
                                             boolean ignoreWrongType,
                                             Logger logger)
        throws RemoteException, NotBoundException {

        return getUserLogin(storeName, hostName, registryPort, bindingName,
                            getLogin(loginMgr, hostName, registryPort,
                                     resourceType),
                            clientId, requiredSerialVersion, protocols,
                            ignoreWrongType, logger);
    }

    public static UserLoginAPI getUserLogin(@Nullable String storeName,
                                            String hostName,
                                            int registryPort,
                                            String bindingName,
                                            @Nullable LoginHandle loginHandle,
                                            @Nullable ClientId clientId,
                                            Protocols protocols,
                                            boolean ignoreWrongType,
                                            Logger logger)
        throws RemoteException, NotBoundException {

        return getUserLogin(storeName, hostName, registryPort, bindingName,
                            loginHandle, clientId,
                            SerialVersion.MINIMUM, protocols,
                            ignoreWrongType, logger);
    }

    private static UserLoginAPI getUserLogin(@Nullable String storeName,
                                             String hostName,
                                             int registryPort,
                                             String bindingName,
                                             @Nullable LoginHandle loginHandle,
                                             @Nullable ClientId clientId,
                                             short requiredSerialVersion,
                                             Protocols protocols,
                                             boolean ignoreWrongType,
                                             Logger logger)
        throws RemoteException, NotBoundException {

        final UserLogin userLogin =
            getRemoteService(
                storeName, hostName, registryPort, bindingName, clientId,
                checked(endpoint -> AsyncRegistryUtils.getUserLogin(
                            endpoint, bindingName, loginHandle, logger)
                        .thenApply(api -> (api == null) ?
                                   null :
                                   api.createSyncProxy())),
                !ignoreWrongType ? USER_LOGIN_TYPE_FAMILY : null,
                requiredSerialVersion, protocols, logger);
        return UserLoginAPI.wrap(userLogin, loginHandle);
    }

    /**
     * If a startup problem is found for this resource, throw an exception with
     * the information, because that's the primary problem.
     */
    public static void checkForStartupProblem(String storeName,
                                              String hostName,
                                              int registryPort,
                                              ResourceId rId,
                                              StorageNodeId snId,
                                              @Nullable LoginManager loginMgr,
                                              Logger logger) {

        StringBuilder startupProblem = null;

        try {
            final StorageNodeAgentAPI sna = getStorageNodeAgent(storeName,
                                                                hostName,
                                                                registryPort,
                                                                snId,
                                                                loginMgr,
                                                                logger);
            startupProblem =  sna.getStartupBuffer(rId);
        } catch (Exception secondaryProblem) {

            /*
             * We couldn't get to the SNA to check. Eat this secondary problem
             * so the first problem isn't hidden.
             */
        }

        if (startupProblem != null) {
            throw new OperationFaultException("Problem starting process for " +
                                              rId + ":" +
                                              startupProblem.toString());
        }
    }

    /**
     * Looks up a StorageNode in the registry without benefit of the
     * Topology. Used to bootstrap storage node registration.
     * @throws RemoteException
     * @throws NotBoundException
     */
    public static StorageNodeAgentAPI
        getStorageNodeAgent(String hostName,
                            int registryPort,
                            String serviceName,
                            @Nullable LoginManager loginMgr,
                            Logger logger)
        throws RemoteException, NotBoundException {

        return getStorageNodeAgent(null /* storeName */, hostName,
                                   registryPort, serviceName, loginMgr,
                                   logger);
    }

    private static StorageNodeAgentAPI
        getStorageNodeAgent(@Nullable String storeName,
                            String hostName,
                            int registryPort,
                            String bindingName,
                            @Nullable LoginManager loginMgr,
                            Logger logger)
        throws RemoteException, NotBoundException {

        final StorageNodeAgentInterface snai =
            getRemoteService(storeName, hostName, registryPort, bindingName,
                             StorageNodeAgentInterfaceInitiator::createProxy,
                             STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY,
                             logger);
        final LoginHandle loginHdl = getLogin(loginMgr, hostName, registryPort,
                                              ResourceType.STORAGE_NODE);
        return StorageNodeAgentAPI.wrap(snai, loginHdl);
    }

    /** Get a non-bootstrap SNA. */
    public StorageNodeAgentAPI getStorageNodeAgent(StorageNodeId storageNodeId)
        throws RemoteException, NotBoundException {

        final StorageNode storageNode = topology.get(storageNodeId);
        return getStorageNodeAgent(topology.getKVStoreName(),
                                   storageNode.getHostname(),
                                   storageNode.getRegistryPort(),
                                   storageNodeId, loginMgr, logger);
    }

    /**
     * Returns the API wrapper for the remote handle associated with the
     * administration of the ARB identified by <code>arbNodeId</code>.
     */
    public ArbNodeAdminAPI getArbNodeAdmin(ArbNodeId arbNodeId)
        throws RemoteException, NotBoundException {

        final ArbNode arbNode = topology.get(arbNodeId);
        final StorageNodeId storageNodeId = arbNode.getStorageNodeId();
        final StorageNode storageNode = topology.get(storageNodeId);
        return getArbNodeAdmin(topology.getKVStoreName(),
                               storageNode.getHostname(),
                               storageNode.getRegistryPort(),
                               arbNodeId, loginMgr, logger);
    }

    /**
     * Returns the API wrapper for the remote handle associated with the test
     * interface of the AN identified by <code>arbNodeId</code>.
     */
    public RemoteTestAPI getArbNodeTest(ArbNodeId arbNodeId)
        throws RemoteException, NotBoundException {

        final ArbNode arbNode = topology.get(arbNodeId);
        return getRemoteTest(arbNodeId.getFullName(),
                             arbNode.getStorageNodeId());
    }

    public static RemoteTestAPI getStorageNodeAgentTest(String storeName,
                                                        String hostName,
                                                        int registryPort,
                                                        StorageNodeId snid,
                                                        Logger logger)
        throws RemoteException, NotBoundException {

        final String bindingName =
            bindingName(storeName, snid.getFullName(), InterfaceType.TEST);
        return getRemoteTest(storeName, hostName, registryPort, bindingName,
                             logger);
    }

    public RemoteTestAPI getStorageNodeAgentTest(StorageNodeId storageNodeId)
        throws RemoteException, NotBoundException {

        return getRemoteTest(storageNodeId.getFullName(),
                             storageNodeId);
    }

    /**
     * Get the SNA trusted login
     */
    public static TrustedLoginAPI getStorageNodeAgentLogin(String hostName,
                                                           int registryPort,
                                                           Logger logger)
        throws RemoteException, NotBoundException {

        return getTrustedLogin(hostName, registryPort,
                               GlobalParams.SNA_LOGIN_SERVICE_NAME, logger);
    }

    /**
     * Get the specified trusted login
     */
    public static TrustedLoginAPI getTrustedLogin(String hostName,
                                                  int registryPort,
                                                  String bindingName,
                                                  Logger logger)
        throws RemoteException, NotBoundException {

        final TrustedLogin trustedLogin =
            getRemoteService(null /* storeName */, hostName, registryPort,
                             bindingName, TrustedLoginInitiator::createProxy,
                             TRUSTED_LOGIN_TYPE_FAMILY, logger);
        return TrustedLoginAPI.wrap(trustedLogin);
    }

    /**
     * Checks whether the service name appears to be a valid StorageNodeAgent
     * TrustedLogin interface.
     */
    public static boolean isStorageNodeAgentLogin(String serviceName) {
        /*
         * A StorageAgentNode TrustedLogin interface will look like this:
         * SNA:TRUSTED_LOGIN
         */
        return serviceName.equals(GlobalParams.SNA_LOGIN_SERVICE_NAME);
    }

    /**
     * Get the non-bootstrap SNA interface without using Topology.
     */
    public static StorageNodeAgentAPI
        getStorageNodeAgent(String storeName,
                            String hostName,
                            int registryPort,
                            StorageNodeId snId,
                            @Nullable LoginManager loginMgr,
                            Logger logger)
        throws RemoteException, NotBoundException {

        final String bindingName =
            bindingName(storeName, snId.getFullName(), InterfaceType.MAIN);
        return getStorageNodeAgent(storeName, hostName, registryPort,
                                   bindingName, loginMgr, logger);
    }

    /**
     * Get the non-bootstrap SNA interface without using Topology.
     */
    public static StorageNodeAgentAPI
        getStorageNodeAgent(String storeName,
                            StorageNode sn,
                            @Nullable LoginManager loginMgr,
                            Logger logger)
        throws RemoteException, NotBoundException {

        return getStorageNodeAgent(storeName,
                                   sn.getHostname(),
                                   sn.getRegistryPort(),
                                   sn.getResourceId(),
                                   loginMgr,
                                   logger);
    }

    /**
     * Get the non-bootstrap SNA interface using the given Parameters object.
     */
    public static StorageNodeAgentAPI
        getStorageNodeAgent(Parameters parameters,
                            StorageNodeId snId,
                            @Nullable LoginManager loginMgr,
                            Logger logger)
        throws RemoteException, NotBoundException {
        final String storename = parameters.getGlobalParams().getKVStoreName();
        return getStorageNodeAgent(storename, parameters.get(snId),
                                   snId, loginMgr, logger);
    }

    public static StorageNodeAgentAPI
        getStorageNodeAgent(String storeName,
                            StorageNodeParams snp,
                            StorageNodeId snId,
                            @Nullable LoginManager loginMgr,
                            Logger logger)
        throws RemoteException, NotBoundException {

        return getStorageNodeAgent(storeName, snp.getHostname(),
                                   snp.getRegistryPort(), snId, loginMgr,
                                   logger);
    }

    /**
     * Rebinds the object associated with request handler for the RN identified
     * by <code>repNodeId</code>
     *
     * @return the listen handle if using async, else null
     */
    public @Nullable ListenHandle
        rebind(RepNodeId repNodeId,
               RequestHandler requestHandler,
               ClientSocketFactory csf,
               ServerSocketFactory ssf,
               DialogHandlerFactory dialogHandlerFactory)
        throws RemoteException {

        final RepNode repNode = topology.get(repNodeId);
        return rebind(repNodeId, InterfaceType.MAIN,
                      repNode.getStorageNodeId(), requestHandler, csf, ssf,
                      REQUEST_HANDLER_TYPE_FAMILY, dialogHandlerFactory);
    }

    /**
     * Rebinds the object associated with monitor agent for the RN identified
     * by <code>repNodeId</code>.
     *
     * @return the listen handle if using async, else null
     */
    public @Nullable ListenHandle
        rebind(RepNodeId repNodeId,
               MonitorAgent monitorAgent,
               ClientSocketFactory csf,
               ServerSocketFactory ssf,
               DialogHandlerFactory dialogHandlerFactory)
        throws RemoteException {

        final RepNode repNode = topology.get(repNodeId);
        return rebind(repNodeId, InterfaceType.MONITOR,
                      repNode.getStorageNodeId(), monitorAgent, csf, ssf,
                      MONITOR_AGENT_TYPE_FAMILY, dialogHandlerFactory);
    }

    /**
     * Rebinds the object associated with administration of the RN identified
     * by <code>repNodeId</code>.
     *
     * @return the listen handle if using async, else null
     */
    public @Nullable ListenHandle
        rebind(RepNodeId repNodeId,
               RepNodeAdmin repNodeAdmin,
               ClientSocketFactory csf,
               ServerSocketFactory ssf,
               DialogHandlerFactory dialogHandlerFactory)
        throws RemoteException {

        final RepNode repNode = topology.get(repNodeId);
        return rebind(repNodeId, InterfaceType.ADMIN,
                      repNode.getStorageNodeId(), repNodeAdmin, csf, ssf,
                      REP_NODE_ADMIN_TYPE_FAMILY, dialogHandlerFactory);
    }

    /**
     * Rebinds the object in the registry after assembling the binding name
     * from its service resource ID and suffix if any.
     *
     * @param serviceId the resource ID of the service
     * @param interfaceType the suffix if any to be appended to the baseName
     * @param storageNodeId the storage node used to locate the registry
     * @param object the remote object to be bound
     * @param csf the client socket factory
     * @param ssf the server socket factory
     * @param dialogTypeFamily the async dialog type family
     * @param dialogHandlerFactory a factory to create the dialog handler
     *
     * @return listen handle if using async, else null
     *
     * @throws RemoteException
     */
    private @Nullable ListenHandle
        rebind(ServiceResourceId serviceId,
               InterfaceType interfaceType,
               StorageNodeId storageNodeId,
               VersionedRemote object,
               ClientSocketFactory csf,
               ServerSocketFactory ssf,
               DialogTypeFamily dialogTypeFamily,
               DialogHandlerFactory dialogHandlerFactory)
        throws RemoteException {

        final StorageNode storageNode = topology.get(storageNodeId);
        return rebind(storageNode.getHostname(), storageNode.getRegistryPort(),
                      getTopology().getKVStoreName(), serviceId,
                      interfaceType, object, csf, ssf,
                      dialogTypeFamily, dialogHandlerFactory, logger);
    }

    public static @Nullable ListenHandle
        rebind(String hostName,
               int registryPort,
               String storeName,
               ServiceResourceId serviceId,
               InterfaceType interfaceType,
               VersionedRemote object,
               ClientSocketFactory csf,
               ServerSocketFactory ssf,
               DialogTypeFamily dialogTypeFamily,
               DialogHandlerFactory dialogHandlerFactory,
               Logger logger)
        throws RemoteException {

        final String serviceName =
            bindingName(storeName, serviceId.getFullName(), interfaceType);
        return rebind(hostName, registryPort, serviceName, serviceId, object,
                      csf, ssf, dialogTypeFamily, dialogHandlerFactory,
                      logger);
    }

    /**
     * Rebinds the object in the registry using the specified service name.
     * This form of rebind is used by the StorageNodeAgent before it is
     * registered with a Topology.
     *
     * @param hostName the hostName associated with the registry
     * @param registryPort the registry port
     * @param serviceName the binding name for the entry
     * @param serviceId the resource ID of the service
     * @param object the remote object to be bound
     *
     * @return listen handle if using async, else null
     *
     * @throws RemoteException
     */
    public static @Nullable ListenHandle
        rebind(String hostName,
               int registryPort,
               String serviceName,
               ServiceResourceId serviceId,
               VersionedRemote object,
               ClientSocketFactory csf,
               ServerSocketFactory ssf,
               DialogTypeFamily dialogTypeFamily,
               DialogHandlerFactory dialogHandlerFactory,
               Logger logger)
        throws RemoteException {

        if (serverUseRmi) {
            try {
                exportAndRebind(serviceName, object,
                                getRegistry(hostName, registryPort), csf, ssf);
            } catch (RemoteException re) {
                throw new RemoteException(
                    "Can't rebind " + serviceName +
                    " at " + hostName + ":" + registryPort +
                    " csf: " + csf + " ssf: " + ssf,
                    re);
            }
        }

        return AsyncRegistryUtils.rebind(hostName, registryPort, serviceName,
                                         serviceId, dialogTypeFamily,
                                         dialogHandlerFactory, csf, ssf,
                                         logger);
    }

    /**
     * Provide a way to clear the service cache so that old entries are not
     * returned even if they have not been unexported. Some tests create real
     * admins after creating bootstrap ones, and should call this method to
     * make sure that the bootstrap admins are removed from the cache.
     */
    public static void clearServiceCache() {
        serviceCache.clear();
    }

    /**
     * Unbinds the object in the registry after assembling the binding name. It
     * also unexports the object after letting all active operations finish.
     *
     * @param hostName the hostName associated with the registry
     * @param registryPort the registry port
     * @param storeName the name of the KV store
     * @param baseName the base name
     * @param interfaceType the suffix if any to be appended to the baseName
     * @param object the object being unbound
     * @param asyncListenHandle the listen handle if using async, else null
     * @param logger debug logger
     *
     * @throws RemoteException
     */
    public static void unbind(String hostName,
                              int registryPort,
                              String storeName,
                              String baseName,
                              InterfaceType interfaceType,
                              Remote object,
                              @Nullable ListenHandle asyncListenHandle,
                              Logger logger)
        throws RemoteException {

        unbind(hostName, registryPort,
               bindingName(storeName, baseName, interfaceType), object,
               asyncListenHandle, logger);
    }

    /**
     * Static unbind method for non-topology object.
     */
    public static void unbind(String hostName,
                              int registryPort,
                              String serviceName,
                              Remote object,
                              @Nullable ListenHandle asyncListenHandle,
                              Logger logger)
        throws RemoteException {

        RemoteException re = null;
        if (serverUseRmi) {
            try {
                final Registry registry = getRegistry(hostName, registryPort);
                registry.unbind(serviceName);
            } catch (RemoteException e) {
                re = e;
            } catch (NotBoundException e) {
            }

            /*
             * Even if the object was not in the registry, make sure it is
             * unexported. Unexport is needed in cases where an attempt to
             * create a new service failed, and the failure cleanup caused the
             * existing service to be unbound in the registry, even though it
             * is still exported. It's not clear if this situation can occur in
             * production or only happens in tests, but seems safest to account
             * for it, and the unexport should be harmless.
             */
            try {
                UnicastRemoteObject.unexportObject(object, true);
            } catch (NoSuchObjectException e) {
                /* The object must not have been exported after all */
            }
        }

        if (asyncListenHandle != null) {
            AsyncRegistryUtils.unbind(hostName, registryPort, serviceName,
                                      asyncListenHandle, logger);
        }

        if (re != null) {
            throw re;
        }
    }

    /**
     * Returns the binding name used in the registry.
     */
    public static String bindingName(String storeName,
                                     String baseName,
                                     InterfaceType interfaceType) {
        return storeName + BINDING_NAME_SEPARATOR +
            baseName  + BINDING_NAME_SEPARATOR + interfaceType;
    }

    /**
     * Initialize socket policies with default client socket factory timeout
     * settings for client utility use (e.g. admin client, ping, etc.).
     *
     * Whether to use SSL is determined from system properties.
     * TBD: the reference to system property usage could be invalidated by
     * pending proposed change to the security configuration API.
     */
    public static void initRegistryCSF() {
        initRegistryCSF(KVStoreConfig.DEFAULT_REGISTRY_OPEN_TIMEOUT,
                        KVStoreConfig.DEFAULT_REGISTRY_READ_TIMEOUT);
    }

    /**
     * Initialize socket policies with given client socket factory timeout
     * settings for client utility use (e.g. admin client, ping, etc.).
     *
     * Whether to use SSL is determined from system properties.
     * TBD: the reference to system property usage could be invalidated by
     * pending proposed change to the security configuration API.
     *
     * @param openTimeoutMs client socket factory open timeout
     * @param readTimeoutMs client socket factory read timeout
     */
    public static void initRegistryCSF(int openTimeoutMs, int readTimeoutMs) {
        final RMISocketPolicy rmiPolicy =
            ClientSocketFactory.ensureRMISocketPolicy();
        final String registryCsfName =
            ClientSocketFactory.registryFactoryName();

        final SocketFactoryArgs args = new SocketFactoryArgs().
            setCsfName(registryCsfName).
            setCsfConnectTimeout(openTimeoutMs).
            setCsfReadTimeout(readTimeoutMs);

        setRegistryCSF(rmiPolicy.getRegistryCSF(args), null, null);
    }

    /**
     * Set socket timeouts for ClientSocketFactory.
     * Only for KVStore client usage.
     * Whether to use SSL is determined from system properties, if not yet
     * configured.
     */
    public static
        void setRegistrySocketTimeouts(int connectMs,
                                       int readMs,
                                       @Nullable String storeName,
                                       @Nullable ClientId clientId) {

        final RMISocketPolicy rmiPolicy =
            ClientSocketFactory.ensureRMISocketPolicy();
        final String registryCsfName =
            ClientSocketFactory.registryFactoryName();
        final SocketFactoryArgs args = new SocketFactoryArgs().
            setKvStoreName(storeName).
            setCsfName(registryCsfName).
            setCsfConnectTimeout(connectMs).
            setCsfReadTimeout(readMs).
            setClientId(clientId);

        setRegistryCSF(rmiPolicy.getRegistryCSF(args), storeName, clientId);
    }

    /**
     * Set the supplied CSF for use during registry lookups within the
     * KVStore server.
     */
    public static void setServerRegistryCSF(ClientSocketFactory csf) {

        setRegistryCSF(csf, null, null);
    }

    /**
     * Sets the client socket factory to use for registry lookups for the
     * specified store name or client ID, if non-null, and also the global
     * client socket factory.
     */
    public static synchronized
        void setRegistryCSF(ClientSocketFactory csf,
                            @Nullable String storeName,
                            @Nullable ClientId clientId)
    {
        checkNull("csf", csf);
        globalRegistryCSF = csf;
        if (storeName != null) {
            storeToRegistryCSFMap.put(storeName, csf);
        }
        if (clientId != null) {
            storeToRegistryCSFMap.put(clientId, csf);
        }
        debugLogger.log(debugLoggerLevel,
                        () -> String.format("RegistryUtils.setRegistryCSF" +
                                            " storeName:%s clientId:%s csf:%s",
                                            storeName, clientId, csf));
    }

    /**
     * Reset all client socket factory settings. For use by tests.
     */
    public static synchronized void clearRegistryCSF() {
        globalRegistryCSF = null;
        SSLClientSocketFactory.clearUserSSLControlMap();
        storeToRegistryCSFMap.clear();
    }

    /**
     * Returns the number of entries in the client socket factory map. For
     * testing.
     */
    public static synchronized int getRegistryCSFMapSize() {
        return storeToRegistryCSFMap.size();
    }

    /**
     * Reset the client socket factory settings for the specified client ID
     * when a KVStoreImpl is closed.
     *
     * @param clientId the client ID of the KVStoreImpl
     */
    public static synchronized
        void clearRegistryCSF(@Nullable ClientId clientId) {

        checkNull("clientId", clientId);
        storeToRegistryCSFMap.remove(clientId);
    }

    /**
     * Reset registry client socket factory after resetting the RMI socket
     * policy.  These resets will allow future connections to take advantage of
     * updated truststore certificates, if any.
     */
    public static synchronized
        void resetRegistryCSF(@Nullable String storeName,
                              @Nullable ClientId clientId)
    {
        checkNull("storeName", storeName);
        final RMISocketPolicy rmiPolicy =
            ClientSocketFactory.resetRMISocketPolicy(storeName, clientId,
                                                     debugLogger);
        ClientSocketFactory csf =
            (clientId != null) ? storeToRegistryCSFMap.get(clientId) : null;
        if (csf == null) {
            csf = storeToRegistryCSFMap.get(storeName);
        }
        if (debugLogger.isLoggable(debugLoggerLevel)) {
            debugLogger.log(debugLoggerLevel,
                            String.format("RegistryUtils.resetRegistryCSF" +
                                          " storeName:%s clientId:%s csf:%s",
                                          storeName, clientId, csf));
        }
        if (csf == null) {
            return;
        }

        /*
         * Using the same socket factory argument to reset client socket
         * factory
         */
        final SocketFactoryArgs args = new SocketFactoryArgs().
            setKvStoreName(storeName).
            setCsfName(csf.name).
            setCsfConnectTimeout(csf.getConnectTimeoutMs()).
            setCsfReadTimeout(csf.getReadTimeoutMs()).
            setClientId(csf.getClientId());
        setRegistryCSF(rmiPolicy.getRegistryCSF(args), storeName, clientId);
    }

    /**
     * Returns a list of the names of all of the services registered with the
     * registry for the specified storage node.
     *
     * If protocols.useAsync() is true, attempts to look up service names in
     * the async registry. Values will only be returned from the async registry
     * if the async registry is running and the server is running a version
     * where all client services will be present in the async registry.
     */
    public List<String> getServiceNames(StorageNodeId snId)
        throws RemoteException
    {
        final StorageNode sn = topology.get(snId);
        return getServiceNames(topology.getKVStoreName(), sn.getHostname(),
                               sn.getRegistryPort(), Protocols.getDefault(),
                               clientId, logger);
    }

    /**
     * Returns a list of the names of all of the services registered with the
     * registry on the specified host.
     *
     * If protocols.useAsync() is true, attempts to look up service names in
     * the async registry. Values will only be returned from the async registry
     * if the async registry is running and the server is running a version
     * where all client services will be present in the async registry.
     */
    public static
        List<String> getServiceNames(@Nullable String storeName,
                                     String hostName,
                                     int registryPort,
                                     Protocols protocols,
                                     @Nullable ClientId clientId,
                                     Logger logger)
        throws RemoteException
    {
        if (protocols.useAsync() && useMiscAsyncServices) {
            try {
                return AsyncRegistryUtils.getServiceNames(
                    storeName, hostName, registryPort, clientId, logger);
            } catch (InsufficientSerialVersionException e) {
                /*
                 * The service does not support async for the specified serial
                 * version
                 */
            } catch (RemoteException e) {
                /*
                 * We weren't able to access the async registry, but we don't
                 * know if that means we're talking to an older version which
                 * isn't running async, or that there is a more general network
                 * problem. Continue if we can try RMI instead.
                 */
                if (!protocols.useRmi()) {
                    throw e;
                }
            }
        }

        if (!protocols.useRmi()) {
            return emptyList();
        }

        final String[] array =
            getRegistry(hostName, registryPort, storeName, clientId)
            .list();
        final List<String> list = new ArrayList<>(array.length);
        Collections.addAll(list, array);
        return list;
    }

    /**
     * Returns a client side remote service instance for the specified binding,
     * using the proxy factory to create the async instance, not checking the
     * dialog type family, checking for the SerialVersion.MINIMUM serial
     * version, and using async by default.
     */
    public static <T extends Remote>
        T getRemoteService(@Nullable String storeName,
                           String hostName,
                           int registryPort,
                           String bindingName,
                           InitiatorProxyFactory<T> proxyFactory,
                           Logger logger)
        throws RemoteException, NotBoundException
    {
        return getRemoteService(storeName, hostName, registryPort, bindingName,
                                proxyFactory, null /* dialogTypeFamily */,
                                logger);
    }

    /**
     * Returns a client side remote service instance for the specified binding,
     * using the proxy factory to create the async instance, and checking the
     * dialog type family if the specified value is not null, checking for the
     * SerialVersion.MINIMUM serial version, and using async by default.
     */
    private static <T extends Remote>
        T getRemoteService(@Nullable String storeName,
                           String hostName,
                           int registryPort,
                           String bindingName,
                           InitiatorProxyFactory<T> proxyFactory,
                           @Nullable DialogTypeFamily dialogTypeFamily,
                           Logger logger)
        throws RemoteException, NotBoundException
    {
        return getRemoteService(storeName, hostName, registryPort, bindingName,
                                proxyFactory, dialogTypeFamily,
                                Protocols.getDefault(), logger);
    }

    /** Overloading to specify protocols */
    private static <T extends Remote>
        T getRemoteService(@Nullable String storeName,
                           String hostName,
                           int registryPort,
                           String bindingName,
                           InitiatorProxyFactory<T> proxyFactory,
                           @Nullable DialogTypeFamily dialogTypeFamily,
                           Protocols protocols,
                           Logger logger)
        throws RemoteException, NotBoundException
    {
        return getRemoteService(
            storeName, hostName, registryPort, bindingName, null /* clientId */,
            checked(endpoint ->
                    AsyncRegistryUtils.createInitiatorProxy(
                        proxyFactory, endpoint, bindingName, logger)),
            dialogTypeFamily, SerialVersion.MINIMUM, protocols, logger);
    }

    /**
     * Returns a client side remote service instance for the specified binding,
     * using the proxy factory to create the async instance, checking the
     * dialog type family if the specified value is not null, checking the
     * specified serial version, and using async only if requested and
     * available.
     */
    private static <T extends Remote>
        T getRemoteService(@Nullable String storeName,
                           String hostName,
                           int registryPort,
                           String bindingName,
                           @Nullable ClientId clientId,
                           Function<ServiceEndpoint, CompletableFuture<T>>
                           createProxy,
                           @Nullable DialogTypeFamily dialogTypeFamily,
                           short requiredSerialVersion,
                           Protocols protocols,
                           Logger logger)
        throws RemoteException, NotBoundException
    {
        if (protocols.useAsync() && useMiscAsyncServices) {
            try {
                final T remoteService = AsyncRegistryUtils.getRemoteService(
                    storeName, hostName, registryPort, bindingName, clientId,
                    createProxy, dialogTypeFamily, requiredSerialVersion,
                    logger);
                if (remoteService == null) {
                    throw new NotBoundException(
                        "Service not found:" +
                        " hostName=" + hostName +
                        " registryPort=" + registryPort +
                        " bindingName=" + bindingName);
                }
                return remoteService;
            } catch (InsufficientSerialVersionException e) {
                /*
                 * The service does not support async for the specified serial
                 * version
                 */
            } catch (RemoteException e) {
                /*
                 * We weren't able to access the async registry, but we don't
                 * know if that means we're talking to an older version which
                 * isn't running async, or that there is a more general network
                 * problem. Continue if we can try RMI instead.
                 */
                if (!protocols.useRmi()) {
                    throw e;
                }
            }
        }
        if (!protocols.useRmi()) {
            throw new NotBoundException("Service not found: " + bindingName);
        }

        /* RMI result type is unchecked */
        @SuppressWarnings("unchecked")
        T result = (T) registryLookup(hostName, registryPort, storeName,
                                      bindingName, clientId);
        return result;
    }

    /**
     * Creates an RMI registry. Throws an exception if serverUseRmi is false.
     */
    public static Registry createRegistry(int port,
                                          @Nullable ClientSocketFactory csf,
                                          @Nullable ServerSocketFactory ssf)
        throws RemoteException {

        if (!serverUseRmi) {
            throw new IllegalStateException(
                "Attempt to create RMI registry when RMI is disabled");
        }
        return LocateRegistry.createRegistry(port, csf, ssf);
    }

    /**
     * Common function to get a Registry reference using a pre-configured CSF
     * with an optional store name or client ID as a CSF lookup key, for cases
     * where multiple stores are accessed concurrently. This method is used in
     * various contexts, with one both or neither of the store name or the
     * client ID specified. The store name should be available on the client
     * side because it is specified in the KVStoreConfig. The client ID should
     * also be available on clients since it is assigned when the KVStore
     * instance is created. Servers and other non-client contexts won't have
     * either value.
     */
    private static Registry getRegistry(String hostName,
                                        int port,
                                        @Nullable String storeName,
                                        @Nullable ClientId clientId)
        throws RemoteException {

        return new ExceptionWrappingRegistry(
            hostName, port, getRegistryCSF(storeName, clientId));
    }

    /**
     * Returns the client socket factory for the specified store name or client
     * ID, or the global client socket factory if the parameters are null or
     * the factory is not found.
     */
    static synchronized @Nullable ClientSocketFactory
        getRegistryCSF(@Nullable String storeName,
                       @Nullable ClientId clientId)
    {
        ClientSocketFactory csf =
            (clientId != null) ? storeToRegistryCSFMap.get(clientId) : null;
        if ((csf == null) && (storeName != null)) {
            csf = storeToRegistryCSFMap.get(storeName);
        }
        if (csf == null) {
            csf = globalRegistryCSF;
        }
        if (debugLogger.isLoggable(debugLoggerLevel)) {
            debugLogger.log(debugLoggerLevel,
                            String.format("RegistryUtils.getRegistryCSF" +
                                          " storeName:%s clientId:%s csf:%s",
                                          storeName, clientId, csf));
        }
        return csf;
    }

    /**
     * Convenience version of the method above, where the storeName is not
     * specified.  This is suitable for all calls within a KVStore
     * component and for calls by applications that have no ability to access
     * multiple stores concurrently.
     */
    private static Registry getRegistry(String hostName, int port)
        throws RemoteException {

        return getRegistry(hostName, port, null, null);
    }

    public static int findFreePort(int start, int end, String hostName) {
        ServerSocket serverSocket = null;
        for (int current = start; current <= end; current++) {
            try {
                /*
                 * Try a couple different methods to be sure that the port is
                 * truly available.
                 */
                serverSocket = new ServerSocket(current);
                serverSocket.close();

                /**
                 * Now using the hostname.
                 */
                serverSocket = new ServerSocket();
                final InetSocketAddress sa =
                    new InetSocketAddress(hostName, current);
                serverSocket.bind(sa);
                serverSocket.close();
                return current;
            } catch (IOException e) /* CHECKSTYLE:OFF */ {
                /* Try the next port number. */
            } /* CHECKSTYLE:ON */
        }
        return 0;
    }

    /**
     * Get the service port on which the specified remote object is exported.
     * This method attempts to use a known format specified in PARSE_PORT_REGEX
     * to parse port number from the string that represents the remote object.
     * If the attempt fails, return 0.
     *
     * @param remote remote object
     * @return a positive port number on which the specified remote object is
     * exported, or 0 if the port number cannot be retrieved
     */
    private static int getServicePort(Remote remote) {
        String str = remote.toString();
        if (remote instanceof RemoteObject) {
            final RemoteRef remoteRef = ((RemoteObject) remote).getRef();
            if (remoteRef != null) {
                str = remoteRef.remoteToString();
            }
        }

        if (str != null) {
            final Matcher m = PARSE_PORT_REGEX.matcher(str);
            if (m.find()) {
                final String port = m.group(1);
                try {
                    return Integer.parseInt(port);
                } catch (NumberFormatException ex) {
                    return 0;
                }
            }
        }
        return 0;
    }

    /**
     * Log the service name and port associated with the specified RMI service
     * name if RMI is enabled.
     */
    public static void logServiceNameAndPort(String name,
                                             String hostName,
                                             int registryPort,
                                             Logger logger)
        throws RemoteException, NotBoundException
    {
        if (!Protocols.getDefault().useRmi()) {
            return;
        }

        final Registry registry =
            RegistryUtils.getRegistry(hostName, registryPort);
        final Remote remote = registry.lookup(name);
        logServiceNameAndPort(name, remote, logger);
    }

    /**
     * Log service name and port associated with the specified remote object.
     * This method attempts to use a known format to parse port number from the
     * string that represents the remote object.  If the attempt fails, log the
     * string as it may contain useful info to figure out the port number.
     *
     * @param name the name to which the specified remote object is bound
     * @param remote remote object
     * @param logger logger
     */
    private static void logServiceNameAndPort(String name,
                                              Remote remote,
                                              Logger logger) {

        /*
         * None of these values should be null, but we want to really be sure
         * since it is OK to skip logging in an unexpected case
         */
        final boolean hasNulls =
            (logger == null) || (remote == null) || (name == null);

        if (hasNulls || !logger.isLoggable(Level.INFO)) {
            return;
        }

        final int port = getServicePort(remote);
        if (port > 0) {
            logger.info(name + " service port: " + port);
        } else {
            String str = remote.toString();
            if (remote instanceof RemoteObject) {
                final RemoteRef remoteRef = ((RemoteObject) remote).getRef();
                if (remoteRef != null) {
                    str = remoteRef.remoteToString();
                }
            }
            logger.info("Remote object: " + str + " is bound to " + name);
        }
    }

    /* Internal Utility methods. */

    /**
     * Export the specified object and bind the resulting stub to the name in
     * the registry.  This method takes special steps to leave out any
     * specified client socket factories when exporting the object so that
     * objects with unequal client socket factories can still be exported on
     * the same port.  The client socket factories are then added back when
     * binding the client-side stub in the registry.
     *
     * @param name the name to bind
     * @param object the object to export
     * @param registry the registry
     * @param csf the client socket factory or null
     * @param ssf the server socket factory or null
     */
    private static void exportAndRebind(String name,
                                        Remote object,
                                        Registry registry,
                                        @Nullable ClientSocketFactory csf,
                                        @Nullable ServerSocketFactory ssf)
        throws RemoteException {

        if (!serverUseRmi) {
            throw new IllegalStateException(
                "Attempt to export RMI server when RMI is disabled");
        }

        int port = 0;
        ServerSocket ss = null;
        if (ssf != null) {
            try {
                /*
                 * Allow the ssf to determine in advance what server socket
                 * should be used.  This allows us to specify the port number
                 * when exporting the object, which is necessary for RMI to
                 * close the server socket when all objects exported on this
                 * port are unexported.
                 */
                ss = ssf.preallocateServerSocket();
                if (ss != null) {
                    port = ss.getLocalPort();
                }
            } catch (IOException ioe) {
                throw new ExportException(
                    "Unable to create ServerSocket for export", ioe);
            }
        }

        final Remote remote;
        try {

            /*
             * Export the object using a ReplaceableRMIClientSocketFactory so
             * that the actual client socket factory does not prevent port
             * sharing.
             */
            remote = UnicastRemoteObject.exportObject(
                object, port, ReplaceableRMIClientSocketFactory.INSTANCE, ssf);

            /*
             * The prepared server socket, if any, should have been consumed,
             * so null out ss to suppress the call to discardServerSocket
             * below.
             */
            ss = null;
        } finally {
            /* If the export did not succeed, we may need to clean up */
            if (ss != null && ssf != null) {
                ssf.discardServerSocket(ss);
            }
        }

        /*
         * Specify a replacement client socket factory when binding the stub in
         * the registry.  The call will serialize the stub, which will cause
         * the desired replacement client socket factory to be substituted for
         * the temporary one.
         */
        try {
            if (csf != null) {
                ReplaceableRMIClientSocketFactory.setReplacement(csf);
            }
            registry.rebind(name, remote);
        } catch (RemoteException re) {
            UnicastRemoteObject.unexportObject(object, true);
            throw re;
        } finally {
            if (csf != null) {
                ReplaceableRMIClientSocketFactory.setReplacement(null);
            }
        }

        if (ssf != null) {
            logServiceNameAndPort(name, remote, ssf.getConnectionLogger());
        }
    }

    /**
     * Returns the binding name used in the registry.
     */
    private String bindingName(String baseName, InterfaceType interfaceType) {
        return bindingName(topology.getKVStoreName(), baseName, interfaceType);
    }

    /**
     * Looks up an object in the registry after assembling the binding name
     * from its base name and suffix if any.
     *
     * @param baseName the base name
     * @param interfaceType the suffix if any to be appended to the baseName
     * @param storageNodeId the storage node used to locate the registry
     *
     * @return the stub associated with the remote object
     *
     * @throws RemoteException
     * @throws NotBoundException
     */
    private Remote lookup(String baseName,
                          InterfaceType interfaceType,
                          StorageNodeId storageNodeId)
        throws RemoteException, NotBoundException {

        final String bindingName = bindingName(baseName, interfaceType);
        logger.finer(() -> String.format("RegistryUtils.lookup" +
                                         " bindingName=%s clientId=%s",
                                         bindingName, clientId));
        return registryLookup(storageNodeId, bindingName);
    }

    /**
     * Look up an entry in the RMI registry associated with a storage node ID.
     */
    private Remote registryLookup(StorageNodeId storageNodeId,
                                  String bindingName)
        throws RemoteException, NotBoundException {

        final StorageNode storageNode = topology.get(storageNodeId);

        return registryLookup(storageNode.getHostname(),
                              storageNode.getRegistryPort(),
                              topology.getKVStoreName(),
                              bindingName, clientId);
    }

    /** A key associated with a service in the service cache. */
    private static class ServiceKey {
        final String hostName;
        final int port;
        final @Nullable String storeName;
        final String bindingName;
        final @Nullable ClientId clientId;
        ServiceKey(String hostName,
                   int port,
                   @Nullable String storeName,
                   String bindingName,
                   @Nullable ClientId clientId) {
            this.hostName = hostName;
            this.port = port;
            this.storeName = storeName;
            this.bindingName = bindingName;
            this.clientId = clientId;
        }
        @Override
        public boolean equals(@Nullable Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof ServiceKey)) {
                return false;
            }
            final ServiceKey otherKey = (ServiceKey) other;
            return Objects.equals(hostName, otherKey.hostName) &&
                port == otherKey.port &&
                Objects.equals(storeName, otherKey.storeName) &&
                Objects.equals(bindingName, otherKey.bindingName) &&
                Objects.equals(clientId, otherKey.clientId);
        }
        @Override
        public int hashCode() {
            int result = 79;
            result = (result * 31) + Objects.hashCode(hostName);
            result = (result * 31) + port;
            result = (result * 31) + Objects.hashCode(storeName);
            result = (result * 31) + Objects.hashCode(bindingName);
            result = (result * 31) + Objects.hashCode(clientId);
            return result;
        }
        @Override
        public String toString() {
            return "ServiceKey[" +
                "hostName=" + hostName +
                " port=" + port +
                " storeName=" + storeName +
                " bindingName=" + bindingName +
                " clientId=" + clientId +
                "]";
        }
    }

    /**
     * A cache for remote services. The cache is needed to avoid concurrent
     * creation of RMI remote stub instances, which can result in duplicate RMI
     * RenewClean threads, presumably due to a concurrency bug in the RMI DGC
     * code. [KVSTORE-493]
     *
     * Before returning a cached entry, the cache calls isValid, which probes
     * the remote object by calling getSerialVersion. If the probe fails, which
     * it should if the old object has been unexported, the entry is removed
     * from the cache before a new entry is fetched. Code, including tests,
     * that binds a new object in the registry should make sure to unexport the
     * old one so that the cache will know to remove its stale entry.
     */
    private static class ServiceCache {

        /**
         * Map from key to a future that holds or will hold the associated
         * value. Synchronize on the cache when accessing this field.
         */
        private final Map<ServiceKey, CompletableFuture<VersionedRemote>> map
            = new HashMap<>();

        /**
         * Gets a future that returns the remote RMI stub for the service
         * associated with the specified parameters.
         */
        CompletableFuture<VersionedRemote> get(String hostName,
                                               int port,
                                               @Nullable String storeName,
                                               String bindingName,
                                               @Nullable ClientId clientId) {
            final ServiceKey key = new ServiceKey(hostName, port, storeName,
                                                  bindingName, clientId);
            CompletableFuture<VersionedRemote> future;
            synchronized (this) {
                future = map.get(key);
                if (future == null) {

                    /* Add a future for a new try */
                    future = new CompletableFuture<>();
                    map.put(key, future);
                } else if (!future.isDone()) {

                    /*
                     * This value is being refreshed, which makes sure it is
                     * valid or else arranges to throw an exception.
                     */
                    return future;
                } else if (future.isCompletedExceptionally()) {

                    /* The previous refresh failed -- try again */
                    future = new CompletableFuture<>();
                    map.put(key, future);
                }
            }

            /*
             * We already checked for an exceptional result, so this call
             * should not throw
             */
            VersionedRemote value = future.getNow(null);

            if (value != null) {

                /* Return if the value is valid */
                if (isValid(value)) {
                    return future;
                }

                synchronized (this) {
                    final CompletableFuture<VersionedRemote> currentFuture =
                        map.get(key);

                    /*
                     * If a new future has been created since the one we
                     * checked, it is a newly refreshed future, so return it
                     */
                    if (currentFuture != future) {
                        return currentFuture;
                    }

                    future = new CompletableFuture<>();
                    map.put(key, future);
                }
            }
            refresh(key, future);
            return future;
        }

        /**
         * Check to be sure that the cached remote RMI stub is still valid. We
         * are depending on services that export RMI servers to unexport them
         * when binding a new object in the registry. We can make the trivial
         * getSerialVersion remote call to the remote object to confirm that it
         * has not be unexported.
         */
        private boolean isValid(VersionedRemote remote) {
            try {
                remote.getSerialVersion();
                return true;
            } catch (RemoteException e) {
                return false;
            }
        }

        /**
         * Obtain a new value for the associated key, updating the future
         * either with the new value or with an exception that represents a
         * failure to obtain the value.
         */
        private void refresh(ServiceKey key,
                             CompletableFuture<VersionedRemote> future) {

            /*
             * TODO: Log entries for refreshes at INFO to help debug any
             * problems. Use the logger provided when KVSTORE-531 is fixed.
             */
            try {
                final VersionedRemote remote = (VersionedRemote)
                    registryLookupInternal(key.hostName, key.port,
                                           key.storeName, key.bindingName,
                                           key.clientId);
                future.complete(remote);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }

        /** Clear the cache. */
        synchronized void clear() {
            map.clear();
        }
    }

    /**
     * Look up an entry in the RMI registry with the specified client ID bound
     * during the lookup.
     */
    private static Remote registryLookup(String hostName,
                                         int port,
                                         @Nullable String storeName,
                                         String bindingName,
                                         @Nullable ClientId clientId)
        throws RemoteException, NotBoundException {

        final CompletableFuture<VersionedRemote> future =
            serviceCache.get(hostName, port, storeName, bindingName, clientId);
        final ClientSocketFactory csf = getRegistryCSF(storeName, clientId);
        final long timeoutMs = (csf != null) ?
            csf.getReadTimeoutMs() :
            DEFAULT_REGISTRY_READ_TIMEOUT;
        try {
            return future.get(timeoutMs, MILLISECONDS);
        } catch (CancellationException e) {
            /*
             * TODO: This would happen if we canceled pending requests on
             * shutdown
             */
            throw new RemoteException("Canceled by shutdown", e);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RemoteException) {
                throw (RemoteException) cause;
            }
            if (cause instanceof NotBoundException) {
                throw (NotBoundException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException("Unexpected exception: " + e, e);
        } catch (InterruptedException e) {
            /*
             * This happens if a request is interrupted during a shutdown
             */
            throw new RemoteException("Interrupted", e);
        } catch (TimeoutException e) {
            throw new RemoteException("Request timed out", e);
        }
    }

    /**
     * Do an RMI registry lookup after no cached value was found.
     */
    private static Remote registryLookupInternal(String hostName,
                                                 int port,
                                                 @Nullable String storeName,
                                                 String bindingName,
                                                 @Nullable ClientId clientId)
        throws RemoteException, NotBoundException {

        final Registry registry =
            getRegistry(hostName, port, storeName, clientId);

        /*
         * Bind the client ID while performing the registry lookup, which makes
         * the client ID available while deserializing the underlying client
         * socket factory
         */
        ClientSocketFactory.setCurrentClientId(clientId);
        try {
            return registry.lookup(bindingName);
        } finally {
            ClientSocketFactory.setCurrentClientId(null);
        }
    }

    private static
        @Nullable LoginHandle getLogin(@Nullable LoginManager loginMgr,
                                       String hostName,
                                       int registryPort,
                                       ResourceType rtype) {

        if (loginMgr == null) {
            return null;
        }

        return loginMgr.getHandle(new HostPort(hostName, registryPort),
                                  rtype);
    }

    /**
     * A decorator class for Registry. When messages of some exception in
     * Registry need to be improved to show a clearer instruction, this class
     * gives a chance to do that.
     * <p>
     * Currently we wrap the {@link ConnectIOException} to tell users to check
     * the security configurations on client and server ends when this
     * exception is seen.
     */
    private static class ExceptionWrappingRegistry implements Registry {
        private final String hostName;
        private final int registryPort;
        private final Registry registry;

        private ExceptionWrappingRegistry(String hostName,
                                          int registryPort,
                                          @Nullable ClientSocketFactory csf)
            throws RemoteException {

            this.hostName = hostName;
            this.registryPort = registryPort;
            registry = LocateRegistry.getRegistry(hostName, registryPort, csf);
            debugLogger.log(debugLoggerLevel, "Supplying RMI registry");
        }

        @Override
        public Remote lookup(@Nullable String name)
            throws RemoteException,
                   NotBoundException,
                   AccessException,
                   AuthenticationFailureException {

            debugLogger.log(debugLoggerLevel,
                            () -> "RMI registry lookup: " + name);
            assert TestHookExecute.doHookIfSet(registryLookupHook, name);

            try {
                return registry.lookup(name);
            } catch (RemoteException re) {
                rethrow(re);
            }
            /* Unreachable code */
            throw new AssertionError("Unreachable");
        }

        @Override
        public void bind(@Nullable String name, @Nullable Remote obj)
            throws RemoteException,
                   AlreadyBoundException,
                   AccessException,
                   AuthenticationFailureException {
            try {
                registry.bind(name, obj);
            } catch (RemoteException re) {
                rethrow(re);
            }
        }

        @Override
        public void unbind(@Nullable String name)
            throws RemoteException,
                   NotBoundException,
                   AccessException,
                   AuthenticationFailureException {
            try {
                registry.unbind(name);
            } catch (RemoteException re) {
                rethrow(re);
            }
        }

        @Override
        public void rebind(final @Nullable String name,
                           final @Nullable Remote obj)
            throws RemoteException,
                   AccessException,
                   AuthenticationFailureException {
            try {
                registry.rebind(name, obj);
            } catch (RemoteException re) {
                rethrow(re);
            }
        }

        @Override
        public String[] list()
            throws RemoteException,
                   AccessException,
                   AuthenticationFailureException{

            debugLogger.log(debugLoggerLevel, "RMI registry list");
            try {
                return registry.list();
            } catch (RemoteException re) {
                rethrow(re);
            }
            /* Unreachable code */
            throw new AssertionError("Unreachable");
        }

        /*
         * Note: Keep this method in sync with
         * AsyncRegistryUtils.TranslateExceptions.onResult and
         * AsyncRequestHandler.handleDialogException
         */
        private void rethrow(RemoteException re)
            throws RemoteException, AuthenticationFailureException {

            /* Wraps the CIOE to give a clearer message to users */
            if (re instanceof ConnectIOException) {

                /*
                 * If CIOE caused by the SSL handshake error, wraps as an
                 * AuthenticationFailureException
                 */
                final Throwable cause = re.getCause();
                if (cause instanceof SSLHandshakeException) {
                    throw new AuthenticationFailureException(cause);
                }
                throw new ConnectIOException(
                    POSSIBLE_SECURITY_MISMATCH_MESSAGE, re);
            }
            if (re instanceof ConnectException) {
                throw new ConnectException(
                    "Unable to connect to the storage node agent at" +
                    " host " + hostName + ", port " + registryPort +
                    ", which may not be running",
                    re);
            }
            throw re;
        }
    }

    /**
     * Define an RMI client socket factory that will replace itself with
     * another client factory during serialization, if one is specified, or
     * else with null, to use the default.  All instances of this class are
     * equal to each other.  Use this factory when exporting an RMI server so
     * that different client socket factories can be used on the client side
     * without requiring the use of a different server socket for each export.
     *
     * <p>If you want to use a non-standard client socket factory, call the
     * setReplacement method around an operation that serializes the
     * client-side stub, which will cause the instance of this class to be
     * replaced by the desired client-side client socket factory.  [#24708]
     */
    public static class ReplaceableRMIClientSocketFactory
            implements Serializable, RMIClientSocketFactory {

        /** Use this default instance since all instances are equal. */
        public static final ReplaceableRMIClientSocketFactory INSTANCE =
            new ReplaceableRMIClientSocketFactory();

        /**
         * Use this ClientSocketFactory instance as the replacement if none is
         * specified. Because of a regression in Java 1.8.0_241, 11.0.6, and
         * 13.0.2, specifying a null replacement is producing a
         * NullPointerException in those Java versions. See:
         * https://bugs.openjdk.java.net/browse/JDK-8237368
         */
        private static final ClientSocketFactory NULL_REPLACEMENT =
            new ClientSocketFactory("nullReplacement", 0, 0, null);

        private static final long serialVersionUID = 1;

        /**
         * Stores the replacement client socket factory for the current thread.
         */
        private static final ThreadLocal<RMIClientSocketFactory>
            replacement = new ThreadLocal<RMIClientSocketFactory>();

        @Override
        public boolean equals(@Nullable Object object) {
            return object instanceof ReplaceableRMIClientSocketFactory;
        }

        @Override
        public int hashCode() {
            return 424242;
        }

        /**
         * Call the default RMI socket factory if this instance has not been
         * replaced.
         */
        @Override
        public Socket createSocket(@Nullable String host, int port)
            throws IOException {

            final RMIClientSocketFactory defaultFactory =
                RMISocketFactory.getDefaultSocketFactory();
            return defaultFactory.createSocket(host, port);
        }

        /**
         * Specify the client socket factory that should replace this instance
         * during serialization.  Make sure to clear the setting to null after
         * serialization is complete, to avoid holding a reference to the
         * replacement.
         */
        static void setReplacement(@Nullable RMIClientSocketFactory csf) {
            replacement.set(csf);
        }

        /**
         * Replace this instance with the replacement, if not null.  Otherwise,
         * replace this instance with null, which will in turn be replaced by
         * the default RMI socket factory when it is deserialized.
         */
        private Object writeReplace() {
            final RMIClientSocketFactory csf = replacement.get();
            if (csf != null) {
                return csf;
            }
            return NULL_REPLACEMENT;
        }

        /**
         * Instances of this class should never be deserialized because they
         * are always replaced during serialization.
         */
        @SuppressWarnings("unused")
        private void readObject(ObjectInputStream in) {
            throw new AssertionError();
        }
    }
}
