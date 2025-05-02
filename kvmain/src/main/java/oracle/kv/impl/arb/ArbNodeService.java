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

package oracle.kv.impl.arb;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.arb.admin.ArbNodeAdmin;
import oracle.kv.impl.arb.admin.ArbNodeAdminImpl;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.ResponderDialogHandlerFactory;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.ServiceFaultHandler;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterTracker;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.EmbeddedMode;
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

import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;

/**
 * This is the "main" that represents the ArbNode. It handles startup and
 * houses all the pieces of software that share a JVM such as a request
 * handler, the administration support and the monitor agent.
 */
public class ArbNodeService
    implements ConfigurableService {

    /**
     * The total number of async services in the arbiter:
     * ArbNodeAdmin
     * MonitorAgent
     * RemoteTestInterface
     */
    private static final int NUM_ASYNC_SERVICES = 3;

    /**
     * The number of seconds to keep threads alive in the async executor.
     */
    private static final int ASYNC_EXECUTOR_KEEP_ALIVE_SECS = 30;

    private ArbNodeId arbNodeId;
    private Params params;

    /**
     * The components that make up the service.
     */
    private ArbNodeSecurity anSecurity = null;
    private ArbNodeAdminImpl admin = null;
    private MonitorAgentImpl monitorAgent = null;
    private ArbNode arbNode = null;
    private final Map<InterfaceType, ListenHandle> listenHandlesMap =
        new HashMap<>();

    /**
     *  The status of the service
     */
    private final ServiceStatusTracker statusTracker =
        new ServiceStatusTracker(null /* logger */);

    /**
     * Parameter change tracker
     */
    private final ParameterTracker parameterTracker;

    /**
     * Global parameter change tracker
     */
    private final ParameterTracker globalParameterTracker;

    private ArbStatsTracker arbStatsTracker;

    /**
     * The object used to coordinate concurrent request to start and stop the
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
    protected Logger logger;

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

    public ArbNodeService(boolean usingThreads) {
        super();
        faultHandler = new ServiceFaultHandler(this, logger,
                                               ProcessExitCode.RESTART);
        this.usingThreads = usingThreads;
        parameterTracker = new ParameterTracker();
        globalParameterTracker = new ParameterTracker();
    }

    /**
     * Initialize a ArbNodeService.  This must be invoked before start() is
     * called.
     */
    public void initialize(SecurityParams securityParams,
                           ArbNodeParams arbNodeParams,
                           LoadParameters lp) {

        GlobalParams globalParams =
            new GlobalParams(lp.getMap(ParameterState.GLOBAL_TYPE));

        noteKVStoreName(globalParams.getKVStoreName());

        StorageNodeParams storageNodeParams =
            new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));

        /* construct the Params from its components */
        params = new Params(securityParams, globalParams,
                            storageNodeParams, arbNodeParams);

        arbNodeId = arbNodeParams.getArbNodeId();

        /*
         * The AgentRepository is the buffered monitor data and belongs to the
         * monitorAgent, but is instantiated outside to take care of
         * initialization dependencies. Don't instantiate any loggers before
         * this is in place, because constructing the AgentRepository registers
         * it in LoggerUtils, and ensures that loggers will tie into the
         * monitoring system.
         */
        AgentRepository monitorBuffer =
            new AgentRepository(globalParams.getKVStoreName(), arbNodeId,
                                arbNodeParams.getMonitorAgentRepoSize());
        addParameterListener(monitorBuffer.new RNParamsListener());

        logger = LoggerUtils.getLogger(this.getClass(), params);
        securityParams.initRMISocketPolicies(logger);

        LoggerUtils.logSoftwareVersions(logger);
        faultHandler.setLogger(logger);
        statusTracker.setLogger(logger);

        /*
         * Any socket timeouts observed by the ClientSocketFactory in this
         * process will be logged to this logger.
         */
        ClientSocketFactory.setTimeoutLogger(logger);

        anSecurity = new ArbNodeSecurity(this, logger);

        statusTracker.addListener(monitorBuffer);
        statusTracker.setCrashDetector(
            new ServiceCrashDetector(
                arbNodeId,
                FileNames.getLoggingDir(
                    new File(params.getStorageNodeParams().getRootDirPath()),
                    globalParams.getKVStoreName()),
                logger));

        arbNode =  new ArbNode(params);
        addParameterListener(arbNode);

        arbStatsTracker =
            new ArbStatsTracker(this, params.getArbNodeParams().getMap(),
                                params.getGlobalParams().getMap(),
                                monitorBuffer);
        addParameterListener(arbStatsTracker.getARBParamsListener());
        addGlobalParameterListener(arbStatsTracker.getGlobalParamsListener());

        /* Initialize the async endpoint group before the ArbNodeAdmin */
        if (!usingThreads) {
            AsyncRegistryUtils.initEndpointGroup(
                logger,
                storageNodeParams.calcEndpointGroupNumThreads(),
                storageNodeParams.getEndpointGroupMaxQuiescentSeconds(),
                false /* forClient */,
                arbNodeParams.getMiscAsyncMaxConcurrentOps(),
                faultHandler);
        }

        admin = new ArbNodeAdminImpl(this, arbNode);
        monitorAgent = new MonitorAgentImpl(this, monitorBuffer);

        addParameterListener(UserDataControl.getParamListener());

        final LoginUpdater loginUpdater = new LoginUpdater();

        loginUpdater.addServiceParamsUpdaters(anSecurity);
        loginUpdater.addGlobalParamsUpdaters(anSecurity);

        addParameterListener(loginUpdater.new ServiceParamsListener());
        addGlobalParameterListener(
            loginUpdater.new GlobalParamsListener());

        /* Sets the server hostname for remote objects */
        AsyncRegistryUtils.setServerHostName(storageNodeParams.getHostname());

        /* Disable to allow for faster timeouts on failed connections. */
        if (!EmbeddedMode.isEmbedded()) {
            System.setProperty("java.rmi.server.disableHttp", "true");
        }

        if (!usingThreads) {
            storageNodeParams.setRegistryCSF(securityParams);
        }
    }

    /**
     * Returns the executor to use to execute miscellaneous async operations.
     * This method is synchronized to make sure that multi-thread access
     * doesn't create multiple executors.
     */
    public ThreadPoolExecutor getAsyncMiscExecutor() {
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
                 * We don't really know for sure if there some last async calls
                 * coming in, so just tell the executor to exit all idle
                 * threads quickly
                 */
                asyncMiscExecutor.setKeepAliveTime(1, MILLISECONDS);
                asyncMiscExecutor.allowCoreThreadTimeOut(true);
            }
        }
    }

    /**
     * Notification that there are modified service parameters.
     */
    synchronized public void newParameters() {
        ParameterMap newMap = null;
        ParameterMap oldMap = params.getArbNodeParams().getMap();
        StorageNodeParams snp = params.getStorageNodeParams();
        try {
            final StorageNodeId snid1 = snp.getStorageNodeId();
            StorageNodeAgentAPI snapi =
                RegistryUtils.getStorageNodeAgent(
                    params.getGlobalParams().getKVStoreName(),
                    snp,
                    snid1,
                    anSecurity.getLoginManager(),
                    logger);

            LoadParameters lp = snapi.getParams();
            newMap = lp.getMap(oldMap.getName(), oldMap.getType());
        } catch (NotBoundException | RemoteException e) {
            /* Ignore exception, will read directly from file */
        }

        if (newMap == null) {
            newMap =
                ConfigUtils.getArbNodeMap(params.getStorageNodeParams(),
                                          params.getGlobalParams(),
                                          arbNodeId, logger);
        }

        /* Do nothing if maps are the same */
        if (oldMap.equals(newMap)) {
            logger.info("newParameters are identical to old parameters");
            return;
        }
        logger.info("newParameters: refreshing parameters");
        params.setArbNodeParams(new ArbNodeParams(newMap));
        parameterTracker.notifyListeners(oldMap, newMap);
    }

    /**
     * Notification that there are modified global parameters
     */
    synchronized public void newGlobalParameters() {
        ParameterMap oldMap = params.getGlobalParams().getMap();
        ParameterMap newMap =
            ConfigUtils.getGlobalMap(params.getStorageNodeParams(),
                                     params.getGlobalParams(),
                                     logger);

        /* Do nothing if maps are the same */
        if (oldMap.equals(newMap)) {
            logger.info(
                "newGlobalParameters are identical to old global parameters");
            return;
        }
        logger.info("newGlobalParameters: refreshing global parameters");
        params.setGlobalParams(new GlobalParams(newMap));
        globalParameterTracker.notifyListeners(oldMap, newMap);
    }

    public void addParameterListener(ParameterListener listener) {
        parameterTracker.addListener(listener);
    }

    Logger getLogger() {
        return logger;
    }

    private void addGlobalParameterListener(ParameterListener listener) {
        globalParameterTracker.addListener(listener);
    }

    /**
     * Starts the ArbNodeService.
     *
     */
    @Override
    public void start() {

        synchronized (startStopLock) {
            statusTracker.update(ServiceStatus.STARTING);

            try {

                logger.info("Starting ArbNodeService");

                /* Reset the count of async services started */
                numAsyncServicesStarted = 0;

                /*
                 * Start the monitor agent first, so the monitor can report
                 * state and events.
                 */
                monitorAgent.startup();
                checkStopRequestedDuringStart();

                /*
                 * Start up admin.  Most requests will fail until the arbNode
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
                 * Start up the arbNode so that it's available for admin and
                 * user requests.
                 */
                arbNode.startup();
                checkStopRequestedDuringStart();

                if (numAsyncServicesStarted > NUM_ASYNC_SERVICES) {
                    throw new IllegalStateException(
                        "The number of async services started (" +
                        numAsyncServicesStarted +
                        ") exceeds NUM_ASYNC_SERVICES (" +
                        NUM_ASYNC_SERVICES + ")");
                }

                statusTracker.update(ServiceStatus.RUNNING);

                logger.info("Started ArbNodeService");
            } catch (RemoteException re) {
                statusTracker.update(ServiceStatus.ERROR_NO_RESTART,
                                     re.toString());
                throw new IllegalStateException
                    ("ArbNodeService startup failed", re);
            }
        }
    }

    private void checkStopRequestedDuringStart() {
        if (stopRequested) {
            throw new IllegalStateException(
                "ArbNodeService startup failed because stop was requested");
        }
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
        stopRequested = true;
        synchronized (startStopLock) {
            if (statusTracker.getServiceStatus().isTerminalState()) {
                /* If the service has already been stopped. */
                return;
            }
            statusTracker.update(ServiceStatus.STOPPING, reason);

            try {

                /*
                 * Push all stats out of the operation stats collector, to
                 * attempt to get them to the admin monitor or to the local log.
                 */
                arbStatsTracker.pushStats();

                /* Then stop the tracker, so its threads get stopped */
                arbStatsTracker.close();

                /*
                 * Stop the admin. Note that an admin shutdown request may be
                 * in progress, but the request should not be impacted by
                 * stopping the admin service. Admin services may be using the
                 * environment, so stop them first.
                 */
                admin.stop();

                /*
                 * Stop the arb node next.
                 */
                arbNode.stop();

                /* Stop the executor for miscellaneous async operations */
                stopAsyncMiscExecutor();

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

    public ArbNodeService.Params getParams() {
        return params;
    }

    /**
     * Returns the ArbNodeId associated with the service
     */
    public ArbNodeId getArbNodeId() {
        return arbNodeId;
    }

    public StorageNodeId getStorageNodeId() {
        return params.getStorageNodeParams().getStorageNodeId();
    }

    public ArbNodeParams getArbNodeParams() {
        return params.getArbNodeParams();
    }

    /**
     * Returns the fault handler associated with the service
     */
    public ProcessFaultHandler getFaultHandler() {
       return faultHandler;
    }

    @Override
    public boolean getUsingThreads() {
        return usingThreads;
    }

    @Override
    public ServiceStatusTracker getServiceStatusTracker() {
        return statusTracker;
    }

    /**
     * Rebinds the remote component in the registry associated with this SN
     */
    public <S extends VersionedRemote>
        void rebind(S remoteComponent,
                    RegistryUtils.InterfaceType type,
                    ClientSocketFactory csf,
                    ServerSocketFactory ssf,
                    DialogTypeFamily dialogTypeFamily,
                    ResponderDialogHandlerFactory<S> dialogHandlerFactory)
        throws RemoteException {

        final String serviceName = RegistryUtils.bindingName(
            params.getGlobalParams().getKVStoreName(),
            getArbNodeId().getFullName(), type);
        final StorageNodeParams snp = params.getStorageNodeParams();
        final ListenHandle listenHandle = RegistryUtils.rebind(
            snp.getHostname(), snp.getRegistryPort(), serviceName,
            getArbNodeId(), remoteComponent, csf, ssf, dialogTypeFamily,
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
    public void unbind(Remote remoteComponent, InterfaceType type)
        throws RemoteException {

        final StorageNodeParams snp = params.getStorageNodeParams();
        final String hostName = snp.getHostname();
        final int registryPort = snp.getRegistryPort();
        final String storeName =
            params.getGlobalParams().getKVStoreName();
        final String serviceName = RegistryUtils.bindingName(
            storeName, getArbNodeId().getFullName(), type);
        final ListenHandle listenHandle = listenHandlesMap.get(type);
        RegistryUtils.unbind(hostName, registryPort, serviceName,
                             remoteComponent, listenHandle, logger);
    }

    ArbNode getArbNode() {
        return arbNode;
    }

    public ArbNodeAdmin getArbNodeAdmin() {
        return admin;
    }

    public ArbNodeSecurity getArbNodeSecurity() {
        return anSecurity;
    }

    public LoginManager getLoginManager() {
        return anSecurity.getLoginManager();
    }

    /**
     * Issue a BDBJE update of the target node's HA address.
     *
     * @param groupName - Replication group name
     * @param targetNodeName - Node name
     * @param targetHelperHosts - Helper hosts used to access RepGroupAdmin
     * @param newNodeHostPort - List of new helpers, if NULL complete entry is
     *                          removed from the rep group.
     */
    public void updateMemberHAAddress(String groupName,
                                      String targetNodeName,
                                      String targetHelperHosts,
                                      String newNodeHostPort) {

        /*
         * Setup the helper hosts to use for finding the master to execute this
         * update.
         */
        Set<InetSocketAddress> helperSockets = new HashSet<InetSocketAddress>();
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
                                      ((arbNode != null) ?
                                       arbNode.getRepNetConfig() : null));
        ReplicationGroup rg = rga.getGroup();
        com.sleepycat.je.rep.ReplicationNode jeRN =
            rg.getMember(targetNodeName);
        if (jeRN == null) {
            throw new IllegalStateException
                (targetNodeName + " does not exist in replication group " +
                 groupName);
        }

        if (newNodeHostPort == null) {
            rga.deleteMember(targetNodeName);
            return;
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
            return;
        }

        rga.updateAddress(targetNodeName, newHostName, newPort);
    }

    /**
     * A convenience class to package all the parameter components used by
     * the Arb Node service
     */
    public static class Params {
        private final SecurityParams securityParams;
        private volatile GlobalParams globalParams;
        private final StorageNodeParams storageNodeParams;
        private volatile ArbNodeParams arbNodeParams;

        public Params(SecurityParams securityParams,
                      GlobalParams globalParams,
                      StorageNodeParams storageNodeParams,
                      ArbNodeParams arbNodeParams) {
            super();
            this.securityParams = securityParams;
            this.globalParams = globalParams;
            this.storageNodeParams = storageNodeParams;
            this.arbNodeParams = arbNodeParams;
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

        public ArbNodeParams getArbNodeParams() {
            return arbNodeParams;
        }

        public void setArbNodeParams(ArbNodeParams params) {
            arbNodeParams = params;
        }

        public void setGlobalParams(GlobalParams params) {
            globalParams = params;
        }
    }

    /**
     * Install listeners used to track status changes, stats, and parameter
     * changes.
     */
    private void installListeners() {
        try {
            final StorageNodeParams snParams = params.getStorageNodeParams();
            final StorageNodeId snId = snParams.getStorageNodeId();
            final StorageNodeAgentAPI sna =
                RegistryUtils.getStorageNodeAgent(
                    params.getGlobalParams().getKVStoreName(), snParams, snId,
                    anSecurity.getLoginManager(), logger);

            statusTracker.addListenerSendCurrent(newStatus -> {
                    try {
                        sna.updateNodeStatus(arbNodeId, newStatus);
                    } catch (RemoteException re) {
                        logger.info("Failure to deliver status update to" +
                                    " SNA: " + re.getMessage());
                    }
                });

            arbStatsTracker.addListener(packet -> {
                    try {
                        sna.receiveStats(arbNodeId, packet);
                    } catch (RemoteException re) {
                        logger.info("Failure to deliver perf stats to SNA: " +
                                    re.getMessage());
                    }
                });

            addParameterListener((oldMap, newMap) -> {
                    try {
                        sna.receiveNewParams(arbNodeId, newMap);
                    } catch (RemoteException re) {
                        logger.info("Failure to deliver parameter change to" +
                                    " SNA: " + re.getMessage());
                    }
                });
        } catch (RemoteException|NotBoundException e) {
            logger.warning("Problem installing status listeners: " + e);
        }
    }
}
