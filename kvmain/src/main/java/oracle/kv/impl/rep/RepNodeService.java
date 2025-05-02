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

package oracle.kv.impl.rep;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.param.ParameterState.COMMON_HIDE_USERDATA;

import java.io.File;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.AggregateThroughputTracker.RWKB;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.KVStoreInternalFactory;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.api.ops.ResourceTracker;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.ResponderDialogHandlerFactory;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.ServiceFaultHandler;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterTracker;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.RepNode.ReplicationStateListener;
import oracle.kv.impl.rep.admin.RepNodeAdmin;
import oracle.kv.impl.rep.admin.RepNodeAdminImpl;
import oracle.kv.impl.rep.login.RepNodeLoginService;
import oracle.kv.impl.rep.monitor.MonitorAgentImpl;
import oracle.kv.impl.rep.stats.KeyStatsCollector;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater;
import oracle.kv.impl.security.metadata.SecurityMDListener;
import oracle.kv.impl.security.metadata.SecurityMDUpdater;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.oauth.IDCSOAuthAuthenticator;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.GCUtils;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.ServiceCrashDetector;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.VersionedRemote;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.TaskCoordinator;

/**
 * This is the "main" that represents the RepNode. It handles startup and
 * houses all the pieces of software that share a JVM such as a request
 * handler, the administration support and the monitor agent.
 */
public class RepNodeService
    implements ConfigurableService, ParameterListener {

    /**
     * Timeout for shutdown of threads and thread pools so the process doesn't
     * hang.
     */
    public static final long SHUTDOWN_TIMEOUT_MS = 10 * 1000;

    /**
     * A test hook that is called by the start method after obtaining the
     * startStopLock -- for testing.
     */
    public volatile static TestHook<RepNodeService> startTestHook;

    /**
     * A test hook that is called by the stop method after it records that a
     * stop is requested -- for testing.
     */
    public volatile static TestHook<Void> stopRequestedTestHook;

    /**
     * The total number of async services in the RN:
     * AsyncClientRepNodeAdmin
     * AsyncUserLogin
     * MonitorAgent
     * RepNodeTestInterface
     */
    private static final int NUM_ASYNC_SERVICES = 4;

    /**
     * The number of seconds to keep threads alive in the async executor.
     */
    private static final int ASYNC_EXECUTOR_KEEP_ALIVE_SECS = 30;

    private RepNodeId repNodeId;
    private Params params;

    /**
     * The components that make up the service.
     */
    private RequestHandlerImpl reqHandler = null;
    private RepNodeSecurity rnSecurity = null;
    private RepNodeAdminImpl admin = null;
    private final Map<InterfaceType, ListenHandle> listenHandlesMap =
        new HashMap<>();
    private MonitorAgentImpl monitorAgent = null;
    private RepNodeLoginService loginService = null;
    private RepNode repNode = null;
    private RequestDispatcher requestDispatcher = null;

    /**
     * Creator to create store handle on RepNode service.
     */
    private KVStoreCreator kvStoreCreator = null;

    /**
     * The thread that monitors other RNs looking for updates to metadata.
     */
    private MetadataUpdateThread metadataUpdateThread = null;

    /**
     *  The status of the service
     */
    private final ServiceStatusTracker statusTracker =
        new ServiceStatusTracker(null /* logger */);

    /**
     * Statistics tracker for user operations
     */
    private OperationsStatsTracker opStatsTracker;

    /**
     * Parameter change tracker
     */
    private final ParameterTracker parameterTracker;

    /**
     * Global parameter change tracker
     */
    private final ParameterTracker globalParameterTracker;

    /**
     * The object used to coordinate concurrent requests to start and stop the
     * rep node service.
     */
    private final Object startStopLock = new Object();

    /**
     * Whether the stop method has been called, even if the actual stop is
     * being delayed because of other operations.
     */
    private volatile boolean stopRequested;

    /**
     * The fault handler associated with the service.
     */
    private final ServiceFaultHandler faultHandler;

    /**
     * True if running in a thread context.
     */
    private final boolean usingThreads;

    /**
     * The collector that gathers statistics of key distribution within this
     * RepNode.
     */
    private KeyStatsCollector keyStatsCollector;

    /* The task coordinator. */
    private volatile TaskCoordinator taskCoordinator;

    /**
     * For executing miscellaneous async operations, not for the request
     * handler. Synchronize on asyncMiscExecutorLock when accessing.
     */
    private ThreadPoolExecutor asyncMiscExecutor;
    private final Object asyncMiscExecutorLock = new Object();

    /**
     * The number of async services started -- used to confirm that
     * NUM_ASYNC_SERVICES is set properly.
     */
    private int numAsyncServicesStarted;

    protected Logger logger;

    /**
     * An additional handler to call for uncaught exceptions during testing, or
     * null.
     */
    private final UncaughtExceptionHandler testExceptionHandler;

    /**
     * An executor that provides threads as needed for executing SNA callbacks.
     */
    private final ExecutorService snaCallbackThreadPool =
        Executors.newCachedThreadPool(
            new KVThreadFactory("RnSnaCallbacks", logger));

    /**
     * The constructor. Nothing interesting happens until the call to
     * newProperties.
     */
    public RepNodeService() {
        this(false);
    }

    public RepNodeService(boolean usingThreads) {
        this(usingThreads, null);
    }

    public RepNodeService(boolean usingThreads,
                          UncaughtExceptionHandler testExceptionHandler) {
        super();
        faultHandler = new ServiceFaultHandler(this,
                                               null, /* logger */
                                               ProcessExitCode.RESTART);
        this.usingThreads = usingThreads;
        parameterTracker = new ParameterTracker();
        globalParameterTracker = new ParameterTracker();
        this.testExceptionHandler = testExceptionHandler;
    }

    /**
     * Initialize a RepNodeService.  This must be invoked before start() is
     * called.
     */
    public void initialize(SecurityParams securityParams,
                           RepNodeParams repNodeParams,
                           LoadParameters lp) {

        GlobalParams globalParams =
            new GlobalParams(lp.getMap(ParameterState.GLOBAL_TYPE));

        noteKVStoreName(globalParams.getKVStoreName());

        StorageNodeParams storageNodeParams =
            new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));

        /* construct the Params from its components */
        params = new Params(securityParams, globalParams,
                            storageNodeParams, repNodeParams);

        repNodeId = repNodeParams.getRepNodeId();

        /*
         * The AgentRepository is the buffered monitor data and belongs to the
         * monitorAgent, but is instantiated outside to take care of
         * initialization dependencies. Don't instantiate any loggers before
         * this is in place, because constructing the AgentRepository registers
         * it in LoggerUtils, and ensures that loggers will tie into the
         * monitoring system.
         */
        AgentRepository monitorBuffer =
            new AgentRepository(globalParams.getKVStoreName(), repNodeId,
                                repNodeParams.getMonitorAgentRepoSize());
        addParameterListener(monitorBuffer.new RNParamsListener());

        logger = LoggerUtils.getLogger(this.getClass(), params);
        securityParams.initRMISocketPolicies(logger);

        faultHandler.setLogger(logger);
        statusTracker.setLogger(logger);

        /*
         * Any socket timeouts observed by the ClientSocketFactory in this
         * process will be logged to this logger.
         */
        ClientSocketFactory.setTimeoutLogger(logger);

        rnSecurity = new RepNodeSecurity(this, logger);

        /* Initialize the async endpoint group before the request dispatcher */
        if (!usingThreads) {
            AsyncRegistryUtils.initEndpointGroup(
                logger,
                storageNodeParams.calcEndpointGroupNumThreads(),
                storageNodeParams.getEndpointGroupMaxQuiescentSeconds(),
                false /* forClient */,
                repNodeParams.getAsyncMaxConcurrentRequests(),
                faultHandler);
        }

        requestDispatcher =
            RequestDispatcherImpl.createForRN(globalParams,
                                              params.getRepNodeParams(),
                                              rnSecurity.getLoginManager(),
                                              new ThreadExceptionHandler(),
                                              logger);

        /*
         * Initialize KVStoreCreator,
         * 1. The creator is relying on the request dispatcher in
         * RepNodeService to create the store. The handle of request dispatcher
         * need to be initialized on RepNodeService before doing any create
         * action.
         * 2. The creator handle on RepNodeService itself need to be
         * initialized before the RepNode Security set the dispatcher, this will
         * allow session manager in RepNode Security to acquire the not null
         * creator handle for later usage.
         * 3. The creator handle on RepNodeService itself need to be
         * initialized before the KeyStatsCollector's initialization, this will
         * allow KeyStatsCollector to acquire the not null creator handle for
         * later usage.
         */
        kvStoreCreator = new KVStoreCreator(this, logger);

        rnSecurity.setDispatcher(requestDispatcher);

        statusTracker.addListener(monitorBuffer);
        statusTracker.setCrashDetector(
            new ServiceCrashDetector(repNodeId, getLoggingDir(), logger));

        addParameterListener(this);

        repNode =  new RepNode(params, requestDispatcher, this);
        rnSecurity.setTopologyManager(requestDispatcher.getTopologyManager());
        reqHandler = new RequestHandlerImpl(requestDispatcher, faultHandler,
                                            rnSecurity.getAccessChecker()) ;

        repNode.initialize(reqHandler.getListenerFactory());
        addParameterListener(repNode.getRepEnvManager());

        opStatsTracker =  new OperationsStatsTracker
            (this, params.getRepNodeParams().getMap(),
             params.getGlobalParams().getMap(), monitorBuffer);
        reqHandler.initialize(params, repNode, opStatsTracker);
        addParameterListener(opStatsTracker.getRNParamsListener());
        addGlobalParameterListener(opStatsTracker.getGlobalParamsListener());

        admin = new RepNodeAdminImpl(this, repNode);
        loginService = new RepNodeLoginService(this);
        monitorAgent = new MonitorAgentImpl(this, monitorBuffer);

        metadataUpdateThread = new MetadataUpdateThread(requestDispatcher,
                                                        repNode,
                                                        logger);
        metadataUpdateThread.start();

        addParameterListener(new RequestTypeUpdater(reqHandler, logger));

        keyStatsCollector = new KeyStatsCollector(repNode,
                                                  kvStoreCreator,
                                                  logger);
        addParameterListener(keyStatsCollector);

        /*
         * Log GC events. Trigger informational messages when young gen. > 3
         * seconds and old gen. > 10 seconds
         */
        GCUtils.monitorGC(3 * 1000, 10 * 1000, logger,
                          this.getClass().getClassLoader());


        /*
         * set UserDataControl based on existing policy params. Both
         * key and value hiding are controlled by the same parameter
         */
        boolean hideUserData =
            repNodeParams.getMap().
            getOrDefault(COMMON_HIDE_USERDATA).asBoolean();
        UserDataControl.setKeyHiding(hideUserData);
        UserDataControl.setValueHiding(hideUserData);

        addParameterListener(UserDataControl.getParamListener());

        final LoginUpdater loginUpdater = new LoginUpdater();
        final IDCSOAuthAuthenticator idcsAuthenticator =
            rnSecurity.getIDCSOAuthAuthenticator();

        loginUpdater.addServiceParamsUpdaters(rnSecurity);
        loginUpdater.addGlobalParamsUpdaters(rnSecurity);

        if (idcsAuthenticator != null) {
            loginUpdater.addGlobalParamsUpdaters(idcsAuthenticator);
        }

        loginUpdater.addServiceParamsUpdaters(loginService);
        loginUpdater.addGlobalParamsUpdaters(loginService);

        addParameterListener(loginUpdater.new ServiceParamsListener());
        addGlobalParameterListener(
            loginUpdater.new GlobalParamsListener());

        final SecurityMDUpdater secMDUpdater = new SecurityMDUpdater();
        secMDUpdater.addUserChangeUpdaters(rnSecurity);
        secMDUpdater.addRoleChangeUpdaters(rnSecurity);

        /* Register security metadata change update listener */
        addSecurityMDListener(loginService);
        addSecurityMDListener(secMDUpdater.new UserChangeListener());
        addSecurityMDListener(secMDUpdater.new RoleChangeListener());

        /* Set the server hostname for remote objects */
        AsyncRegistryUtils.setServerHostName(storageNodeParams.getHostname());

        /* Disable to allow for faster timeouts on failed connections. */
        if (!EmbeddedMode.isEmbedded()) {
            System.setProperty("java.rmi.server.disableHttp", "true");
        }

        taskCoordinator =
            new RNTaskCoordinator(logger,
                                  repNode.getRepEnvManager().envStorageType(),
                                  null /* Use the default permit policy */ ) {
            @Override
            RWKB getRWKB() {
                return reqHandler.getAggregateThroughputTracker().getRWKB();
            }
        };

        if (!usingThreads) {
            storageNodeParams.setRegistryCSF(securityParams);
        }
    }

    /**
     * Returns the executor to use to execute miscellaneous async operations.
     * This method is synchronized to make sure that multi-thread access doesn't
     * create multiple executors.
     */
    public Executor getAsyncMiscExecutor() {
        synchronized (asyncMiscExecutorLock) {
            if (asyncMiscExecutor == null) {
                asyncMiscExecutor = new ThreadPoolExecutor(
                    0 /* corePoolSize */, Integer.MAX_VALUE /* maxPoolSize */,
                    ASYNC_EXECUTOR_KEEP_ALIVE_SECS, SECONDS,
                    new SynchronousQueue<>(),
                    new KVThreadFactory("RepNodeService misc async executor",
                                        logger));
            }
            return asyncMiscExecutor;
        }
    }

    private void stopAsyncMiscExecutor() {
        synchronized (asyncMiscExecutorLock) {
            if (asyncMiscExecutor != null) {

                /*
                 * We can't wait for all threads to exit because the shutdown
                 * that does the waiting happens in one of those threads. Just
                 * tell the executor to exit any idle threads quickly, which
                 * should clean up right after the threads are done.
                 */
                asyncMiscExecutor.setKeepAliveTime(1, MILLISECONDS);
                asyncMiscExecutor.allowCoreThreadTimeOut(true);
            }
        }
    }

    /**
     * Notification that there are modified service parameters.
     */
    public void newParameters() {
        ParameterMap oldMap;
        ParameterMap newMap;
        synchronized(params) {
            oldMap = params.getRepNodeParams().getMap();
            StorageNodeParams snp = params.getStorageNodeParams();
            newMap = null;
            try {
                final StorageNodeId snid1 = snp.getStorageNodeId();
                StorageNodeAgentAPI snapi =
                    RegistryUtils.getStorageNodeAgent(
                        params.getGlobalParams().getKVStoreName(),
                        snp,
                        snid1,
                        rnSecurity.getLoginManager(),
                        logger);

                LoadParameters lp = snapi.getParams();
                newMap = lp.getMap(oldMap.getName(), oldMap.getType());
            } catch (NotBoundException | RemoteException e) {
                /* Ignore exception and read from file directly */
            }
            if (newMap == null) {
                /*
                 * If SN is not available, as a last resort read file directly.
                 */
                newMap =
                    ConfigUtils.getRepNodeMap(params.getStorageNodeParams(),
                                          params.getGlobalParams(),
                                          repNodeId, logger);
            }

            /* Do nothing if maps are the same */
            if (oldMap.equals(newMap)) {
                logger.info("newParameters are identical to old parameters");
                return;
            }
            logger.info("newParameters: refreshing parameters");
            params.setRepNodeParams(new RepNodeParams(newMap));
        }
        parameterTracker.notifyListeners(oldMap, newMap);
    }

    @Override
    public void newParameters(ParameterMap oldMap,
                                           ParameterMap newMap) {

        /* Make any requested updates related to async */
        final EndpointGroup endpointGroup =
            AsyncRegistryUtils.getEndpointGroupOrNull();
        if (endpointGroup == null) {
            return;
        }

        final RepNodeParams oldParams = new RepNodeParams(oldMap);
        final RepNodeParams newParams = new RepNodeParams(newMap);
        reqHandler.updateAsyncThreadPoolParams(oldParams, newParams);

        /* TODO: Update maxTrackedLatencyMillis if needed? */
    }

    /**
     * Notification that there are modified global parameters
     */
    public void newGlobalParameters() {
        ParameterMap oldMap;
        ParameterMap newMap;
        synchronized(params) {
            oldMap = params.getGlobalParams().getMap();
            newMap =
                ConfigUtils.getGlobalMap(params.getStorageNodeParams(),
                                         params.getGlobalParams(),
                                         logger);

            /* Do nothing if maps are the same */
            if (oldMap.equals(newMap)) {
                logger.info(
                    "newGlobalParameters are identical to old " +
                    "global parameters");
                return;
            }
            logger.info("newGlobalParameters: refreshing global parameters");
            params.setGlobalParams(new GlobalParams(newMap));
        }
        globalParameterTracker.notifyListeners(oldMap, newMap);
    }

    public void setReplicationStateListener(ReplicationStateListener listener) {
        repNode.setReplicationStateListener(listener);
    }

    public void addParameterListener(ParameterListener listener) {
        parameterTracker.addListener(listener);
    }

    private void addGlobalParameterListener(ParameterListener listener) {
        globalParameterTracker.addListener(listener);
    }

    private void addSecurityMDListener(SecurityMDListener listener) {
        repNode.getSecurityMDManager().addUpdateListener(listener);
    }

    /**
     * Starts the RepNodeService.
     *
     * @see #start(Topology)
     */
    @Override
    public void start() {

        start(null);
    }

    /**
     * Starts the RepNodeService.
     *
     * Invokes the startup method on each of the components constituting the
     * service. Proper sequencing of the startup methods is important and is
     * explained in the embedded method components.
     * <p>
     * The <code>topology</code> argument is non-null only in test situations
     * and is used to bypass the explicit initialization of the service by the
     * SNA. When a topology is passed in, the service does not need to wait for
     * the SNA to supply the topology since it's already available.
     *
     * @param topology the topology to use if non null.
     */
    public void start(Topology topology) {

        synchronized (startStopLock) {
            assert TestHookExecute.doHookIfSet(startTestHook, this);

            statusTracker.update(ServiceStatus.STARTING);

            try {
                logger.info("Starting RepNodeService");

                /* Reset the count of async services started */
                numAsyncServicesStarted = 0;

                /*
                 * Start the monitor agent first, so the monitor can report
                 * state and events.
                 */
                monitorAgent.startup();
                checkStopRequestedDuringStart();

                /*
                 * Start up admin.  Most requests will fail until the repNode
                 * is started but this is done early to allow ping() to
                 * function.
                 */
                admin.startup();
                checkStopRequestedDuringStart();

                /*
                 * Install listeners before starting the JE environment so that
                 * we send notifications for the initial JE state changes
                 */
                installListeners();
                checkStopRequestedDuringStart();

                /*
                 * Start up the repNode so that it's available for admin and
                 * user requests. The startup results in a replicated
                 * environment handle being established. This may take some
                 * time if there is a long recovery involved.
                 */
                final ReplicatedEnvironment repEnv = repNode.startup();
                checkStopRequestedDuringStart();

                topologyStartup(repEnv, topology);
                checkStopRequestedDuringStart();

                /*
                 * Starts the RepNodeSecurity, which relies on topology startup
                 */
                rnSecurity.startup();
                checkStopRequestedDuringStart();

                /*
                 * Start servicing requests last after the node has been
                 * initialized. It's at this point that the reqHandler appears
                 * in the registry and requests can be sent to it.
                 */
                reqHandler.startup();
                checkStopRequestedDuringStart();

                /*
                 * Start the login service.
                 */
                loginService.startup();
                checkStopRequestedDuringStart();

                /* Startup the statistics collector. */
                keyStatsCollector.startup(getRepNodeParams());
                checkStopRequestedDuringStart();

                if (numAsyncServicesStarted > NUM_ASYNC_SERVICES) {
                    throw new IllegalStateException(
                        "The number of async services started (" +
                        numAsyncServicesStarted +
                        ") exceeds NUM_ASYNC_SERVICES (" +
                        NUM_ASYNC_SERVICES + ")");
                }

                statusTracker.update(ServiceStatus.RUNNING);

                logger.info("Started RepNodeService");
            } catch (RemoteException re) {
                statusTracker.update(ServiceStatus.ERROR_NO_RESTART,
                                     re.toString());
                throw new IllegalStateException
                    ("RepNodeService startup failed", re);
            }
        }
    }

    private void checkStopRequestedDuringStart() {
        if (stopRequested) {
            throw new IllegalStateException(
                "RepNodeService startup failed because stop was requested");
        }
    }

    /**
     * Establishes topology during service startup.
     * <p>
     * For an established node, the Topology is stored in a non-replicated
     * database in the environment. If the topology is available there, no
     * further action is needed.
     * <p>
     * If the Topology is not available in the environment, and this is the
     * first node in the replication group, it waits for the SNA to supply the
     * Topology via a {@link RepNodeAdminImpl#configure call}.
     * <p>
     * For second and subsequent nodes in the replication group, it simply goes
     * ahead expecting the normal Topology propagation mechanism to discover it
     * has an empty Topology and therefore push the current Topology to it. Any
     * requests directed to the node while it's waiting for the Topology to be
     * pushed are rejected, and the request dispatcher redirects them.
     * <p>
     * Note that a newly created replica may get two configure calls, one from
     * the SNA and another redundant call from the Topology push mechanism.
     * This is routine and it simply uses the most current topology as usual.
     *
     * @param repEnv the startup environment handle
     *
     * @param testTopology non null if the topology is explicitly supplied as
     * part of a unit test
     */
    private void topologyStartup(ReplicatedEnvironment repEnv,
                                 Topology testTopology) {

        if (testTopology != null) {
            /*
             * Topology supplied (a test scenario) use it directly,
             * effectively simulating the call the SNA would have made.
             */
            logger.info("Test environment topology self-supplied.");
            final Set<Metadata<? extends MetadataInfo>> metadataSet =
                new HashSet<>(1);
            metadataSet.add(testTopology);
            admin.configure(metadataSet,
                            (AuthContext) null, SerialVersion.CURRENT);
            return;
        }

        if (repNode.getTopology() != null) {
            /* Have been initialized with topology, keep going. */
            return;
        }

        /*
         * Proceed to establish the request handler interface, so NOP requests
         * can be serviced, the empty topology detected and new Topology pushed
         * to this node.
         */
        logger.info("Topology needs to be pushed to this node.");
    }

    /**
     * Invokes the stop method on each of the components constituting the
     * service. Proper sequencing of the stop methods is important and is
     * explained in the embedded method comments.
     *
     * If the service has already been stopped the method simply returns.
     */
    @Override
    public void stop(boolean force, String reason) {
        stop(force, false, reason);
    }

    public void stop(boolean force, boolean checkStream, String reason) {
        stopRequested = true;
        assert TestHookExecute.doHookIfSet(stopRequestedTestHook, null);
        synchronized (startStopLock) {
            if (statusTracker.getServiceStatus().isTerminalState()) {
                /* If the service has already been stopped. */
                return;
            }

            if (checkStream && repNode.servStreamClient()) {
                repNode.waitForStream();
            }

            /* stop RN begins */
            statusTracker.update(ServiceStatus.STOPPING, reason);

            try {
                /*
                 * Stop the task coordinator -- no need to prioritize new
                 * tasks. Note that the task coordinator will hand out deficit
                 * permits after it is closed, so users of it will continue to
                 * behave reasonably while shutdown is underway.
                 */
                taskCoordinator.close();

                /*
                 * Stop key statistics collector, so we are no longer
                 * gathering statistics information.
                 */
                keyStatsCollector.shutdown();

                /*
                 * Stop the login handler first, so we are no longer
                 * accepting requests.
                 */
                loginService.stop();

                /*
                 * Stop the request handler next, so we are no longer
                 * accepting requests.
                 */
                reqHandler.stop();

                /*
                 * Stop the KVStore after stopping the request handler so that
                 * the request handler can stop itself gracefully first
                 */
                kvStoreCreator.stop();

                /*
                 * Push all stats out of the operation stats collector, to
                 * attempt to get them to the admin monitor or to the local log.
                 */
                opStatsTracker.pushStats();

                /* Then stop the tracker, so its threads get stopped */
                opStatsTracker.close();

                /*
                 * Stop the admin. Note that an admin shutdown request may be
                 * in progress, but the request should not be impacted by
                 * stopping the admin service. Admin services may be using the
                 * environment, so stop them first.
                 */
                admin.stop();

                /* Stop the executor for miscellaneous async operations */
                stopAsyncMiscExecutor();

                /* Stop the executor for SNA callbacks */
                snaCallbackThreadPool.shutdown();

                /*
                 * Close any internal kvstore handle that may be in use, and
                 * stop the token verifier
                 */
                rnSecurity.stop();

                /*
                 * Stop the metadata updater
                 */
                stopMetadataUpdateThread(false);

                /*
                 * Stop the rep node next.
                 */
                repNode.stop(force);

                /* Set the status to STOPPED */
                statusTracker.update(ServiceStatus.STOPPED, reason);

                /*
                 * Shutdown the monitor last.
                 */
                monitorAgent.stop();
            } finally {
                if (!usingThreads) {
                    /* Flush any log output. */
                    LoggerUtils.closeHandlers
                        ((params != null) ?
                         params.getGlobalParams().getKVStoreName() :
                         "UNKNOWN");
                }
            }
        }
    }

    @Override
    public boolean stopRequested() {
        return stopRequested;
    }

    public ServiceStatusTracker getStatusTracker() {
        return statusTracker;
    }

    @Override
    public void update(ServiceStatus status, String reason) {
        statusTracker.update(status, reason);
    }

    public RepNodeService.Params getParams() {
        return params;
    }

    /**
     * Returns the RepNodeId associated with the service
     */
    public RepNodeId getRepNodeId() {
        return repNode.getRepNodeId();
    }

    public StorageNodeId getStorageNodeId() {
        return params.getStorageNodeParams().getStorageNodeId();
    }

    public RepNodeParams getRepNodeParams() {
        return params.getRepNodeParams();
    }

    /**
     * Returns the fault handler associated with the service
     */
    public ProcessFaultHandler getFaultHandler() {
       return faultHandler;
    }

    /**
     * Returns the current SecurityMetadata
     */
    public SecurityMetadata getSecurityMetadata() {
        return repNode.getSecurityMDManager().getSecurityMetadata();
    }

    @Override
    public boolean getUsingThreads() {
        return usingThreads;
    }

    @Override
    public ServiceStatusTracker getServiceStatusTracker() {
        return statusTracker;
    }

    public TaskCoordinator getTaskCoordinator() {
        return taskCoordinator;
    }

    ResourceTracker getAggregateThroughputTracker() {
        return reqHandler.getAggregateThroughputTracker();
    }

    /**
     * Rebinds the remote component in the registry associated with this SN. If
     * dialogTypeFamily and createDialogHandler are non-null, also rebinds the
     * component in the async registry.
     */
    public <S extends VersionedRemote>
        void rebind(S remoteComponent,
                    InterfaceType type,
                    ClientSocketFactory csf,
                    ServerSocketFactory ssf,
                    DialogTypeFamily dialogTypeFamily,
                    ResponderDialogHandlerFactory<S> dialogHandlerFactory)
        throws RemoteException {

        final String serviceName = RegistryUtils.bindingName(
            params.getGlobalParams().getKVStoreName(),
            getRepNodeId().getFullName(), type);
        final StorageNodeParams snp = params.getStorageNodeParams();
        final ListenHandle listenHandle = RegistryUtils.rebind(
            snp.getHostname(), snp.getRegistryPort(), serviceName,
            repNodeId, remoteComponent, csf, ssf, dialogTypeFamily,
            () -> dialogHandlerFactory.createDialogHandler(
                remoteComponent, getAsyncMiscExecutor(), logger),
            logger);
        if (listenHandle != null) {
            numAsyncServicesStarted++;
            listenHandlesMap.put(type, listenHandle);
        }
    }

    /**
     * Unbinds the remote component in the registry associated with this SN
     */
    public void unbind(VersionedRemote remoteComponent, InterfaceType type)
        throws RemoteException {

        final StorageNodeParams snp = params.getStorageNodeParams();
        final String hostName = snp.getHostname();
        final int registryPort = snp.getRegistryPort();
        final String storeName =
            params.getGlobalParams().getKVStoreName();
        final String serviceName = RegistryUtils.bindingName(
            storeName, getRepNodeId().getFullName(), type);
        final ListenHandle listenHandle = listenHandlesMap.get(type);
        RegistryUtils.unbind(hostName, registryPort, serviceName,
                             remoteComponent, listenHandle, logger);
    }

    public RepNode getRepNode() {
        return repNode;
    }

    public RequestHandlerImpl getReqHandler() {
        return reqHandler;
    }

    public RepNodeAdmin getRepNodeAdmin() {
        return admin;
    }

    public RepNodeSecurity getRepNodeSecurity() {
        return rnSecurity;
    }

    public LoginManager getLoginManager() {
        return rnSecurity.getLoginManager();
    }

    public RequestDispatcher getRequestDispatcher() {
        return requestDispatcher;
    }

    public KVStoreCreator getKVStoreCreator() {
        return kvStoreCreator;
    }

    /**
     * Issue a BDBJE update of the target node's HA address.
     *
     * @param groupName replication group name
     * @param targetNodeName node name
     * @param targetHelperHosts helper hosts used to access RepGroupAdmin
     * @param newNodeHostPort list of new helpers
     * @return true if JEHA DB was modified, otherwise false
     */
    public boolean updateMemberHAAddress(String groupName,
                                      String targetNodeName,
                                      String targetHelperHosts,
                                      String newNodeHostPort) {
        return
            modifyMemberHAAddress(groupName, targetNodeName, targetHelperHosts,
                                  newNodeHostPort);
    }

    /**
     * Delete the target node from the JE HA group.
     *
     * @param groupName replication group name
     * @param targetNodeName node name
     * @param targetHelperHosts helper hosts used to access RepGroupAdmin
     * @return true if JEHA DB was modified, otherwise false
     */
    public boolean deleteMember(String groupName,
                             String targetNodeName,
                             String targetHelperHosts) {
        return
            modifyMemberHAAddress(groupName, targetNodeName, targetHelperHosts,
                                  null);
    }

    /**
     * Issue a BDBJE update of the target node's HA address or remove the
     * target from the group.
     *
     * @param groupName replication group name
     * @param targetNodeName node name
     * @param targetHelperHosts helper hosts used to access RepGroupAdmin
     * @param newNodeHostPort list of new helpers, if NULL complete entry is
     * removed from the rep group.
     * @returns true if JEHA DB was modified or the data is consistent with
     *          the passed in helper host values. otherwise false
     */
    private boolean modifyMemberHAAddress(String groupName,
                                       String targetNodeName,
                                       String targetHelperHosts,
                                       String newNodeHostPort) {
        /*
         * Setup the helper hosts to use for finding the master to execute this
         * update.
         */
        Set<InetSocketAddress> helperSockets = new HashSet<>();
        StringTokenizer tokenizer =
                new StringTokenizer(targetHelperHosts,
                                    ParameterUtils.HELPER_HOST_SEPARATOR);
        while (tokenizer.hasMoreTokens()) {
            String helper = tokenizer.nextToken();
            helperSockets.add(HostPortPair.getSocket(helper));
        }

        /*
         * Check the target node's HA address. If it is already changed
         * and if that node is alive, don't make any further changes. If the
         * target still has its old HA address, attempt an update.
         */
        ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(groupName, helperSockets,
                                      ((repNode != null) ?
                                       repNode.getRepNetConfig() : null));
        ReplicationGroup rg = rga.getGroup();
        com.sleepycat.je.rep.ReplicationNode jeRN =
            rg.getMember(targetNodeName);
        if (jeRN == null) {
            return false;
        }

        if (newNodeHostPort == null) {
            rga.deleteMember(targetNodeName);
            return true;
        }

        String newHostName = HostPortPair.getHostname(newNodeHostPort);
        int newPort = HostPortPair.getPort(newNodeHostPort);

        if ((jeRN.getHostName().equals(newHostName)) &&
            (jeRN.getPort() == newPort)) {

            /*
             * This node is already changed, nothing more to do. Do this
             * check in case the change has been made previously, and this
             * node is alive, as the updateAddress() call will incur an
             * exception if the node is alive.
             */
            return true;
        }
        rga.updateAddress(targetNodeName, newHostName, newPort);
        return true;
    }

    /* This has package access for unit testing */
    void stopMetadataUpdateThread(boolean wait) {
        if (metadataUpdateThread != null) {
            metadataUpdateThread.shutdown();

            if (wait) {
                try {
                    metadataUpdateThread.join(SHUTDOWN_TIMEOUT_MS);
                } catch (InterruptedException ex) {
                    /* ignore */
                }
            }
        }
    }
    /**
     * A convenience class to package all the parameter components used by
     * the Rep Node service
     */
    public static class Params {
        private final SecurityParams securityParams;
        private volatile GlobalParams globalParams;
        private final StorageNodeParams storageNodeParams;
        private volatile RepNodeParams repNodeParams;

        public Params(SecurityParams securityParams,
                      GlobalParams globalParams,
                      StorageNodeParams storageNodeParams,
                      RepNodeParams repNodeParams) {
            super();
            this.securityParams = securityParams;
            this.globalParams = globalParams;
            this.storageNodeParams = storageNodeParams;
            this.repNodeParams = repNodeParams;
        }

        public SecurityParams getSecurityParams() {
            return securityParams;
        }

        public GlobalParams getGlobalParams() {
            return globalParams;
        }

        public StorageNodeParams getStorageNodeParams() {
            return storageNodeParams;
        }

        public RepNodeParams getRepNodeParams() {
            return repNodeParams;
        }

        public void setRepNodeParams(RepNodeParams params) {
            repNodeParams = params;
        }

        public void setGlobalParams(GlobalParams params) {
            globalParams = params;
        }
    }

    /**
     * The uncaught exception handler associated with top level threads run by
     * the request dispatcher. In the RN service an unhandled exception results
     * in the process being restarted by the SNA.
     */
    private class ThreadExceptionHandler implements UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (logger != null) {
                logger.log(Level.SEVERE, "uncaught exception in thread", e);
            } else if (!EmbeddedMode.isEmbedded()){
                System.err.println("Uncaught exception:");
                e.printStackTrace(System.err);
            }

            if (e instanceof EnvironmentFailureException &&
                ((EnvironmentFailureException) e).isCorrupted()) {

                /* Shutdown without restart if environment is corrupted */
                faultHandler.queueShutdown(
                    e, ProcessExitCode.getNoRestart(e));
            } else {
                /* Have the SNA restart the service. */
                faultHandler.queueShutdown(e, ProcessExitCode.getRestart(e));
            }
            if (testExceptionHandler != null) {
                testExceptionHandler.uncaughtException(t, e);
            }
        }
    }

    /**
     * Create KVStore handle. If the the KVStore handle does not exist and
     * attempt to create it via APIs in KVStoreFactory. If it exists, return it.
     * The object of the class is used multiple times in different code.
     */
    public class KVStoreCreator {
        private KVStore kvstoreBasic;

        /* An internal kvstore handle. */
        private KVStore kvstore;
        /*
         * Notes whether an attempt to initialize has previously been attempted.
         */
        private boolean initializationAttempted = false;

        private final RepNodeService rnService;
        private final Logger loggerHandle;

        /*
         * The number of seconds that we should wait for topology information to
         * become available at startup time.
         */
        private static final int MAX_INIT_SECS = 5;

        public KVStoreCreator(RepNodeService rnService,
                              Logger logger) {
            this.rnService = rnService;
            this.loggerHandle = logger;

            this.initializationAttempted = false;
        }

        synchronized void stop() {
            if (kvstoreBasic != null) {
                kvstoreBasic.close();
                kvstoreBasic = null;
            }
        }

        /**
         * Get KVStore handle. Return it if the handle exists, or create it.
         * Before create a new KVStore handle, The dispatcher must have topology
         * initialized and partitions existing.
         * @return KVStore handle
         */
        public synchronized KVStore getKVStore() {
            /*
             * Callers normally check that kvstore is non-null before calling
             * this, but the kvstore variable may become non-null by the time
             * they enter this method.
             */
            if (kvstore != null) {
                return kvstore;
            }

            final RequestDispatcher dispatcher =
                rnService.getRequestDispatcher();
            /*
             * We can't get a KVStore until we have topology information. The
             * first time this is called we will wait a bit for the topology
             * to become available. We also need to check if the RegUtils of
             * dispatcher is available, otherwise an NPE would be met. Besides,
             * partitions should exist.
             */
            Topology topo = dispatcher.getTopologyManager().getTopology();
            if (topo == null || dispatcher.getRegUtils() == null ||
                    topo.getPartitionMap().getNPartitions() == 0) {

                if (initializationAttempted) {
                    return null;
                }

                loggerHandle.info("Topology not immediately available - " +
                                  "waiting");

                initializationAttempted = true;

                /*
                 * TODO: consider using postUpdateListener on TopologyManager to
                 * eliminate the fixed sleep period used here.
                 */
                for (int i = 0; i < MAX_INIT_SECS; i++) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                    } /* CHECKSTYLE:ON */

                    topo = dispatcher.getTopologyManager().getTopology();
                    if (topo != null && dispatcher.getRegUtils() != null &&
                            topo.getPartitionMap().getNPartitions() > 0) {
                        break;
                    }
                }

                topo = dispatcher.getTopologyManager().getTopology();
                if (topo == null || dispatcher.getRegUtils() == null ||
                        topo.getPartitionMap().getNPartitions() == 0) {

                    loggerHandle.info("Topology is not ready");
                    return null;
                }
            }

            try {
                /*
                 * Get a KVStore handle that imposes restrictions on access to
                 * the hidden namespace, and save it so we can close it.
                 */
                kvstoreBasic =
                    KVStoreInternalFactory.getStore(rnService, loggerHandle);
                /*
                 * Then upgrade to a kvstore that has access to the internal
                 * namespace.
                 */
                kvstore = KVStoreImpl.makeInternalHandle(kvstoreBasic);
                return kvstore;
            } catch (IllegalArgumentException iae) {
                throw new IllegalStateException(
                    "Unable to create internal KVStore", iae);
            }
        }
    }

    /**
     * Install listeners used to track status changes, stats, parameter
     * changes, and state changes.
     */
    private void installListeners() {
        try {
            final StorageNodeParams snParams = params.getStorageNodeParams();
            final StorageNodeId snId = snParams.getStorageNodeId();
            final StorageNodeAgentAPI sna =
                RegistryUtils.getStorageNodeAgent(
                    params.getGlobalParams().getKVStoreName(), snParams, snId,
                    rnSecurity.getLoginManager(), logger);

            statusTracker.addListenerSendCurrent(newStatus -> {
                    try {
                        sna.updateNodeStatus(repNodeId, newStatus);
                    } catch (RemoteException e) {
                        logger.info("Failure to deliver status update to" +
                                    " SNA: " + e.getMessage());
                    }
                });

            opStatsTracker.addListener(packet -> {
                    try {
                        sna.receiveStats(repNodeId, packet);
                    } catch (RemoteException e) {
                        logger.info("Failure to deliver perf stats to SNA: " +
                                    e.getMessage());
                    }
                });

            addParameterListener((oldMap, newMap) -> {
                    try {
                        sna.receiveNewParams(repNodeId, newMap);
                    } catch (RemoteException e) {
                        logger.info("Failure to deliver parameter change to" +
                                    " SNA: " + e.getMessage());
                    }
                });

            setReplicationStateListener(changeEvent -> {
                    snaCallbackThreadPool.execute(() -> {
                            try {
                                sna.updateReplicationState(repNodeId,
                                                           changeEvent);
                            } catch (RemoteException e) {
                                logger.info("Failure to deliver replication" +
                                            " state change to SNA: " +
                                            e.getMessage());
                            }
                        });
                });
        } catch (RemoteException|NotBoundException e) {
            logger.warning("Problem installing status listeners: " + e);
        }
    }

    /**
     * Returns the directory for logging files, either as specified by the
     * rnLogMountPoint parameter or the KV root directory.
     */
    private File getLoggingDir() {
        final File logDirectoryFile =
            params.getRepNodeParams().getLogDirectoryFile();
        if (logDirectoryFile != null) {
            return logDirectoryFile;
        }
        return FileNames.getLoggingDir(
            new File(params.getStorageNodeParams().getRootDirPath()),
            params.getGlobalParams().getKVStoreName());
    }
}
