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

package oracle.kv.impl.admin;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.async.StandardDialogTypeFamily.CLIENT_ADMIN_SERVICE_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.COMMAND_SERVICE_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY;

import java.io.File;
import java.net.InetSocketAddress;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.PlanProgress;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.ResponderDialogHandlerFactory;
import oracle.kv.impl.client.admin.AsyncClientAdminServiceResponder;
import oracle.kv.impl.client.admin.ClientAdminService;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.ProcessFaultHandler.Procedure;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.RoleResolver;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.login.AsyncUserLoginResponder;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.UserLogin;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.PortRange;
import oracle.kv.impl.util.ServiceCrashDetector;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.VersionedRemote;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * AdminService houses the two services provided for administering a kv store
 * instance and providing clients with access to Admin API functions.
 */
public class AdminService implements ConfigurableService {

    /** The number of threads for the admin's async endpoint group. */
    private static final int ENDPOINT_GROUP_NUM_THREADS = 1;

    /**
     * The total number of async services in the admin:
     * AsyncClientAdminService
     * AsyncUserLogin
     * CommandService
     * CommandServiceTestInterface
     */
    private static final int NUM_ASYNC_SERVICES = 4;

    /** The number of seconds to keep threads alive in the async executor. */
    private static final int ASYNC_EXECUTOR_KEEP_ALIVE_SECS = 30;

    private AdminServiceParams params;
    private Admin admin = null;           /* Admin can be null at bootstrap. */
    private ServiceBinder<CommandService> commandService;
    private LoginService loginService;
    private ServiceBinder<UserLogin> userLogin;
    private ServiceBinder<ClientAdminService> clientAdminService;
    private AdminWebServiceManager webServiceManager;
    private ParameterListener parameterListener = null;
    private MgmtAgentPlanStateChangeTracker planStateListener = null;
    private MgmtAgentPlanProgressTracker planProgressListener = null;
    private AdminSecurity adminSecurity;

    private Logger logger;

    private AdminServiceFaultHandler faultHandler;
    private final boolean usingThreads;

    public static final int ADMIN_MIN_HEAP_MB =
        ParameterUtils.applyMinHeapMB(96);
    public static final int ADMIN_MAX_HEAP_MB =
        ParameterUtils.applyMinHeapMB(128);

    /* Default Java heap to 96M for now */
    public static final String DEFAULT_JAVA_ARGS =
        "-XX:+DisableExplicitGC " +
        "-Xms" + ADMIN_MIN_HEAP_MB + "M -Xmx" + ADMIN_MAX_HEAP_MB + "M " +
        "-server" +

        /*
         * Disable JE's requirement that helper host names be resolvable.  We
         * want nodes to be able to start up even if other nodes in the
         * replication group have been removed and no longer have DNS names.
         * [#23120]
         */
        " -Dje.rep.skipHelperHostResolution=true";

    /**
     * Track the status of the service.
     */
    private final ServiceStatusTracker statusTracker =
        new ServiceStatusTracker(null /* logger */);

    /* For unit test support, to determine if the service is up or down. */
    private boolean active = false;

    /**
     * Whether the stop method has been called, even if the actual stop is
     * being delayed because of other operations.
     */
    private volatile boolean stopRequested;

    /**
     * For executing async operations. Synchronize on asyncExecutorLock when
     * accessing.
     */
    private ThreadPoolExecutor asyncExecutor;
    private final Object asyncExecutorLock = new Object();

    /**
     * The number of async services started -- used to confirm that
     * NUM_ASYNC_SERVICES is set properly.
     */
    private int numAsyncServicesStarted;

    /** The SNA that manages the admin, for delivering JMX info. */
    private volatile StorageNodeAgentAPI sna = null;

    /** Whether the current SNA is the bootstrap SNA. */
    private volatile boolean isBootstrapSna;

    /**
     * An executor that provides threads as needed for executing SNA callbacks.
     */
    private final ExecutorService snaCallbackThreadPool =
        Executors.newCachedThreadPool(
            new KVThreadFactory("AdminSnaCallbacks", logger));

    /**
     * Creates a non-bootstrap AdminService.  The initialize() method must
     * be called before start().
     */
    public AdminService(boolean usingThreads) {
        this.usingThreads = usingThreads;
        faultHandler = null;
    }

    /**
     * Creates a bootstrap AdminService.  The initialize() method should not
     * be called before start(). The bootstrap service is always created as
     * a PRIMARY Admin.
     */
    public AdminService(BootstrapParams bp,
                        SecurityParams sp,
                        boolean usingThreads) {
        this.usingThreads = usingThreads;
        deriveASParams(bp, sp);

        /*
         * No need to worry about RMI Socket Policies and Registry CSF here
         * because ManagedBootstrapAdmin takes care of that for us, if needed.
         */

        /* When kvStoreName is null, we are starting in bootstrap mode. */
        if (params.getGlobalParams().getKVStoreName() == null) {
            logger =
                LoggerUtils.getBootstrapLogger(bp.getRootdir(),
                                               FileNames.BOOTSTRAP_ADMIN_LOG,
                                               "BootstrapAdmin");
        } else {
            logger = LoggerUtils.getLogger(this.getClass(), params);
        }
        faultHandler = new AdminServiceFaultHandler(logger, this);
        adminSecurity = new AdminSecurity(this, logger);
    }

    /**
     * Initialize an AdminService.  This must be called before start() if not
     * created via the BootStrap constructor.
     */
    public void initialize(SecurityParams securityParams,
                           AdminParams adminParams,
                           LoadParameters lp) {

        GlobalParams globalParams =
            new GlobalParams(lp.getMap(ParameterState.GLOBAL_TYPE));

        noteKVStoreName(globalParams.getKVStoreName());

        StorageNodeParams storageNodeParams =
            new StorageNodeParams(lp.getMap(ParameterState.SNA_TYPE));
        /*
         * Initializing storage node params with admin mount point before
         * start. If admin mount is not specified then kvroot directory will
         * host admin env files as per current design.
         *
         * TODO: Need to check if admin directory should be part of AdminParams
         * like RN Log Directories are part of RepNodeParams.
         */
        storageNodeParams.setAdminDirMap(
            lp.getMap(ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS));

        params = new AdminServiceParams
            (securityParams, globalParams, storageNodeParams, adminParams);

        logger = LoggerUtils.getLogger(this.getClass(), params);
        securityParams.initRMISocketPolicies(logger);

        LoggerUtils.logSoftwareVersions(logger);

        statusTracker.setLogger(logger);

        if (faultHandler == null) {
            faultHandler = new AdminServiceFaultHandler(logger, this);
        }

        if (!usingThreads) {
            storageNodeParams.setRegistryCSF(securityParams);
        }

        if (adminSecurity != null) {
            throw new IllegalStateException(
                "Calling initialize for bootstrap AdminService not permitted");
        }
        adminSecurity = new AdminSecurity(this, logger);
    }

    /**
     * Start and stop are synchronized to avoid having stop() run before
     * start() is done.  This comes up in testing.
     */
    @Override
    public synchronized void start() {
        getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

                @Override
                public void execute() {
                    startInternal();
                }
             });
    }

    private void startInternal() {
        final String kvStoreName = params.getGlobalParams().getKVStoreName();
        final StorageNodeParams snParams = params.getStorageNodeParams();

        /*
         * If the store is configured, then register the crash detector, which
         * will use the configured log directory
         */
        if (kvStoreName != null) {
            statusTracker.setCrashDetector(
                new ServiceCrashDetector(
                    params.getAdminParams().getAdminId(),
                    FileNames.getLoggingDir(
                        new File(snParams.getRootDirPath()), kvStoreName),
                    logger));
        }
        statusTracker.update(ServiceStatus.STARTING);

        final String hostName = snParams.getHostname();

        /* Sets the server hostname for remote objects */
        AsyncRegistryUtils.setServerHostName(hostName);

        /*
         * Initialize the async endpoint group before starting any RMI or async
         * services so it is available to support server sockets
         */
        if (!usingThreads) {
            final int maxConcurrentOps =
                params.getAdminParams().getMiscAsyncMaxConcurrentOps();
            AsyncRegistryUtils.initEndpointGroup(
                logger,
                ENDPOINT_GROUP_NUM_THREADS,
                snParams.getEndpointGroupMaxQuiescentSeconds(),
                false, /* forClient */
                NUM_ASYNC_SERVICES * maxConcurrentOps, /* maxPermits */
                faultHandler);
        }

        /* Disable to allow for faster timeouts on failed connections. */
        if (!EmbeddedMode.isEmbedded()) {
            System.setProperty("java.rmi.server.disableHttp", "true");
        }

        /*
         * If kvStoreName is null, then we are starting in bootstrap mode.
         * The Admin can't be created yet.
         */
        if (kvStoreName == null) {
            logger.info("Starting in bootstrap mode");
        } else {
            logger.info("Starting AdminService");
            admin = new Admin(params, this);
        }

        if (sna == null) {
            getSna(kvStoreName);
        }

        /* Reset the count of async services started */
        numAsyncServicesStarted = 0;

        /*
         * Create the UserLoginImpl instance and bind it in the registry if
         * security is enabled.
         */
        final boolean isSecure = params.getSecurityParams().isSecure();
        final RMISocketPolicy rmiSocketPolicy =
            params.getSecurityParams().getRMISocketPolicy();
        if (isSecure) {
            loginService = new LoginService(this);
            userLogin =
                new ServiceBinder<>(GlobalParams.ADMIN_LOGIN_SERVICE_NAME,
                                    loginService.getUserLogin(),
                                    snParams.getAdminLoginSFP(rmiSocketPolicy,
                                                              kvStoreName),
                                    USER_LOGIN_TYPE_FAMILY,
                                    AsyncUserLoginResponder::new);
            /* Install the login updater now */
            if (admin != null) {
                admin.installSecurityUpdater();
            }
        }

        /* Create the CommandService instance and bind it in the registry */
        commandService =
            new ServiceBinder<>(GlobalParams.COMMAND_SERVICE_NAME,
                                new CommandServiceImpl(this),
                                snParams.getAdminCommandServiceSFP(
                                    rmiSocketPolicy, kvStoreName,
                                    null /* clientId */),
                                COMMAND_SERVICE_TYPE_FAMILY,
                                CommandServiceResponder::new);

        /* Create the ClientAdminService for handling DDL and DML statements */
        clientAdminService =
            new ServiceBinder<>(GlobalParams.CLIENT_ADMIN_SERVICE_NAME,
                                new ClientAdminServiceImpl(this, logger),
                                snParams.getAdminCommandServiceSFP(
                                    rmiSocketPolicy, kvStoreName,
                                    null /* clientId */),
                                CLIENT_ADMIN_SERVICE_TYPE_FAMILY,
                                AsyncClientAdminServiceResponder::new);

        startWebService();

        if (numAsyncServicesStarted > NUM_ASYNC_SERVICES) {
            throw new IllegalStateException(
                "The number of async services started (" +
                numAsyncServicesStarted +
                ") exceeds NUM_ASYNC_SERVICES (" + NUM_ASYNC_SERVICES + ")");
        }

        synchronized (this) {
            active = true;
            this.notifyAll();
        }
        logger.info("Started AdminService");
    }

    @Override
    public void stop(boolean force, String reason) {
        stopRequested = true;
        synchronized (this) {
            logger.info("Shutting down AdminService instance" +
                        (force ? " (force)" : ""));

            try {
                update(ServiceStatus.STOPPING, reason);
            } catch (RuntimeException e) {
                logger.warning("Exception when noting service stopped" +
                               " status: " + e);
            }
            if (webServiceManager != null) {
                webServiceManager.shutdown();
            }

            if (commandService != null) {
                ((CommandServiceImpl) commandService.getInsecureService())
                    .stopRemoteTestInterface(logger);
                commandService.shutdownService();
                commandService = null;
            }

            if (clientAdminService != null) {
                clientAdminService.shutdownService();
                clientAdminService = null;
            }

            if (userLogin != null) {
                userLogin.shutdownService();
                userLogin = null;
                loginService = null;
            }

            if (admin != null) {
                logger.info("Shutting down Admin");
                admin.shutdown(force, reason);
                admin = null;
            } else {
                /*
                 * Admin.shutdown updates the status tracker, so update it here
                 * if there is no admin.
                 */
                statusTracker.update(ServiceStatus.STOPPED, reason);
            }

            stopAsyncExecutor();
            snaCallbackThreadPool.shutdown();

            adminSecurity.stop();

            active = false;
            this.notifyAll();
        }
    }

    @Override
    public boolean stopRequested() {
        return stopRequested;
    }

    /**
     *  Wait for the service to be started or stopped.
     */
    public synchronized void waitForActive(boolean desiredState)
        throws InterruptedException {

        while (active != desiredState) {
            this.wait();
        }
    }

    /**
     * Accessor for admin, which can be null.
     */
    public Admin getAdmin() {
        return admin;
    }

    /**
     * Accessor for params
     */
    public AdminServiceParams getParams() {
        return params;
    }

    /**
     * Accessor for security info
     */
    public AdminSecurity getAdminSecurity() {
        return adminSecurity;
    }

    /**
     * Accessor for login services
     */
    public LoginService getLoginService() {
        return loginService;
    }

    /**
     * Accessor for internal login manager
     */
    public InternalLoginManager getLoginManager() {
        return (adminSecurity == null) ? null : adminSecurity.getLoginManager();
    }

    /**
     * Configure the store name and then create the Admin instance, which
     * creates the Admin database.  This method can be used only when the
     * AdminService is running in bootstrap/configuration mode.
     */
    public void configure(String storeName) {
        assert admin == null;
        params.getGlobalParams().setKVStoreName(storeName);

        /*
         * Since we are bootstrapping, there is a chicken-egg problem regarding
         * the HA service port.  The bootstrap parameters have an HA port range
         * configured for the SNA.  We know that none of these are in use at
         * this time, so we will commandeer the first port in this range for
         * now.  Later, when this #1 admin is officially deployed via the
         * deployment plan, we will note the use of this port in the Admin's
         * parameter record, thereby reserving its use with the PortTracker.
         */
        final StorageNodeParams snp = params.getStorageNodeParams();

        final int haPort = PortRange.getRange(snp.getHAPortRange()).get(0);
        AdminParams ap = params.getAdminParams();
        ap.setJEInfo(snp.getHAHostname(), haPort, snp.getHAHostname(), haPort);

        admin = new Admin(params, this);

        /* Now we can use the real log configuration. */
        logger.info("Changing log files to log directory for store " +
                    storeName);
        logger = LoggerUtils.getLogger(this.getClass(), params);
        LoggerUtils.logSoftwareVersions(logger);
        statusTracker.setLogger(logger);
        faultHandler.setLogger(logger);
        adminSecurity.configure(storeName);
        if (loginService != null) {
            loginService.resetLogger(logger);
        }
        logger.info("Configured Admin for store: " + storeName);

        /* We can install the login updater now */
        final SecurityParams securityParams = params.getSecurityParams();
        if (securityParams != null) {
            admin.installSecurityUpdater();
        }

        startWebService();
    }

    /**
     * Subordinate services (e.g. CommandService) use this
     * method when logging.  AdminService's logger can change during
     * bootstrap.
     */
    public Logger getLogger() {
        return logger;
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

    /**
     * Initialize our AdminServiceParams member based on the contents of a
     * BootParams instance.
     */
    private void deriveASParams(BootstrapParams bp, SecurityParams sp) {

        String storeName = bp.getStoreName();
        StorageNodeId snid =
            new StorageNodeId(storeName == null ? 1 : bp.getId());

        GlobalParams gp = new GlobalParams(storeName);

        /*
         * Pass user-defined external authentication method to global parameter
         * so that bootstrap admin can recognize this setting automatically.
         */
        final String externalAuths = bp.getUserExternalAuth();
        gp.setUserExternalAuthMethods(externalAuths);

        /*
         * In this release, once the IDCS OAuth authentication mechanism is
         * enabled, the whole store won't allow session extension.
         *
         * TODO: Remove this restriction by distinguishing sessions created
         * after successfully login using different authentication methods.
         */
        if (SecurityUtils.hasIDCSOAuth(externalAuths)) {
            gp.setSessionExtendAllow("false");
        }

        StorageNodeParams snp =
            new StorageNodeParams(snid, bp.getHostname(), bp.getRegistryPort(),
                                  "Admin Bootstrap");

        snp.setRootDirPath(bp.getRootdir());
        /*
         * Setting Admin Directory property for configure store command
         * support
         */
        snp.setAdminDirMap(bp.getAdminDirMap());
        snp.setHAHostname(bp.getHAHostname());
        snp.setHAPortRange(bp.getHAPortRange());
        snp.setServicePortRange(bp.getServicePortRange());
        snp.setAdminWebPort(bp.getAdminWebServicePort());
        String adminStorageDir = bp.getRootdir();

        if (!snp.getAdminDirPaths().isEmpty()) {
            adminStorageDir = snp.getAdminDirPaths().get(0);
        }

        final AdminParams ap = new AdminParams(new AdminId(1),
                                               snp.getStorageNodeId(),
                                               AdminType.PRIMARY,
                                               adminStorageDir);
        params = new AdminServiceParams(sp, gp, snp, ap);
    }

    /**
     * Issue a BDBJE update of the target node's HA address.
     */
    public void updateMemberHAAddress(AdminId targetId,
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

        String storeName = params.getGlobalParams().getKVStoreName();
        String groupName = Admin.getAdminRepGroupName(storeName);

        /* Change the target node's HA address. */
        logger.info("Updating rep group " + groupName + " using helpers " +
                    targetHelperHosts + " to change " + targetId + " to " +
                    newNodeHostPort);

        String targetNodeName = Admin.getAdminRepNodeName(targetId);

        /*
         * Figure out the right ReplicationNetworkConfig to use.  If there's
         * an admin present, we just use that config.  Otherwise (not sure
         * why it wouldn't be if we are part of a replicated config),
         * construct a DataChannelFactory from the SecurityParams, if
         * present.
         */
        final ReplicationNetworkConfig repNetConfig;
        if (admin != null) {
            repNetConfig = admin.getRepNetConfig();
        } else if (params.getSecurityParams() == null) {
            repNetConfig = null;
        } else {
            final Properties haProps =
                params.getSecurityParams().getJEHAProperties();
            logger.info("DataChannelFactory: " +
                        haProps.getProperty(
                            ReplicationNetworkConfig.CHANNEL_TYPE));
            repNetConfig = ReplicationNetworkConfig.create(haProps);
        }

        ReplicationGroupAdmin rga =
            new ReplicationGroupAdmin(groupName, helperSockets, repNetConfig);

        ReplicationGroup rg = rga.getGroup();
        com.sleepycat.je.rep.ReplicationNode jeRN =
            rg.getMember(targetNodeName);
        if (jeRN == null) {
            throw new IllegalStateException
                (targetNodeName + " does not exist in replication group " +
                 groupName);
        }

        String newHostName =  HostPortPair.getHostname(newNodeHostPort);
        int newPort =  HostPortPair.getPort(newNodeHostPort);

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
     * Looks up the SNA in the registry and stores it in the sna field,
     * returning true if the SNA was retrieved successfully.
     */
    private boolean getSna(String storeName) {
        try {
            final StorageNodeParams snParams = params.getStorageNodeParams();

            /* Use the bootstrap SNA if there is no store name */
            if (storeName == null) {
                sna = RegistryUtils.getStorageNodeAgent(
                    snParams.getHostname(), snParams.getRegistryPort(),
                    GlobalParams.SNA_SERVICE_NAME,
                    adminSecurity.getLoginManager(), logger);
                isBootstrapSna = true;
            } else {
                sna = RegistryUtils.getStorageNodeAgent(
                    storeName, snParams, snParams.getStorageNodeId(),
                    adminSecurity.getLoginManager(), logger);
                isBootstrapSna = false;
            }

            /* When there is a new SNA, report our initial status */
            final ServiceStatus newStatus = (admin == null) ?
                /* We're unconfigured; report waiting for deployment. */
                ServiceStatus.WAITING_FOR_DEPLOY :
                /*
                 * Otherwise, if we're up and servicing calls, we are running
                 */
                ServiceStatus.RUNNING;
            updateAdminStatus(admin, newStatus, null /* reason */);

            return true;
        } catch (RemoteException|NotBoundException e) {
            logger.warning("Failed to find SNA: " + e);
            return false;
        }
    }

    @Override
    public void update(ServiceStatus newStatus, String reason) {
       updateAdminStatus(admin, newStatus, reason);
    }

    void updateAdminStatus(Admin a, ServiceStatus newStatus, String reason) {
        updateStatusTracker(newStatus, reason);

        if (sna == null) {
            return;
        }

        /*
         * During bootstrapping, we have no Admin, so we can't reliably add the
         * ParameterListener when installing the receiver.  This method is
         * called at receiver installation time, and also when the Admin's
         * status changes.  When it changes from bootstrap to configured mode,
         * we can install the listener here.  Also, the Admin is given as an
         * argument to this method because, during bootstrapping, this method
         * is called before AdminService's admin instance variable is assigned.
         */
        if (a != null && parameterListener == null) {
            final AdminParams adminParams = a.getParams().getAdminParams();
            parameterListener =
                new ParameterChangeListener(adminParams.getAdminId());
            a.addParameterListener(parameterListener);
            /* Prime the pump with the first newParameters call. */
            parameterListener.newParameters(null, adminParams.getMap());
        }

        /*
         * Use the same pattern as ParameterListener to add a plan state and
         * plan progress listener at the point when the Admin changes from
         * bootstrap to configured mode.
         */
        if (planStateListener == null) {
            planStateListener = new MgmtAgentPlanStateChangeTracker();
        }
        if (planProgressListener == null) {
            planProgressListener = new MgmtAgentPlanProgressTracker();
        }
        if (a != null) {
            /* Monitor listener set can filter out duplicated listener */
            a.getMonitor().trackPlanStateChange(planStateListener);
            a.getMonitor().trackPlanProgress(planProgressListener);
        }

        State adminState = (a == null ? null : a.getReplicationMode());

        boolean isMaster =
            (adminState == null || adminState != State.MASTER ? false : true);

        withAsyncSnaCallback(() -> sna.updateAdminStatus(
                                 new ServiceStatusChange(newStatus), isMaster),
                             "send status update of " + newStatus);
    }

    /** Tell the status tracker about a status change. */
    void updateStatusTracker(ServiceStatus newStatus, String reason) {
        /*
         * Skip the update if the current status is STOPPED, since it is a
         * terminal state and doesn't permit further updates. Testing found
         * cases during an orderly shutdown where the service set its status to
         * STOPPED, but then encountered an exception after that and wanted to
         * update the status to ERROR_RESTARTING or ERROR_NO_RESTART. There is
         * special code to set these error statuses based on what exception was
         * detected, but sometimes the exception is detected after the status
         * has moved to STOPPED. We don't check here for all terminal states,
         * though, because at least one isTerminal value appears to be
         * incorrect, so handle STOPPED specially.
         */
        if (statusTracker.getServiceStatus() != ServiceStatus.STOPPED) {
            statusTracker.update(newStatus, reason);
        }
    }

    @Override
    public ServiceStatusTracker getServiceStatusTracker() {
        return statusTracker;
    }

    /**
     * Make the specified SNA callback asynchronously, to avoid deadlocks,
     * refreshing the SNA and retrying if needed, and logging errors unless
     * opDesc is null.
     */
    private void withAsyncSnaCallback(Procedure<RemoteException> op,
                                      String opDesc) {
        snaCallbackThreadPool.execute(() -> doSnaCallback(op, opDesc));
    }

    /** Helper method for SNA callbacks. */
    private void doSnaCallback(Procedure<RemoteException> op, String opDesc) {
        RemoteException re = null;
        try {
            op.execute();
            return;
        } catch (NoSuchObjectException nsoe) {

            /*
             * If the SNA was not found, and we're using the bootstrap SNA,
             * have a store name, and can successfully get the non-bootstrap
             * SNA, then try the operation again. When the store is being
             * configured, the bootstrap admin first communicates with the
             * bootstrap SNA, and switches to the standard SNA after the SN is
             * deployed. If we were using the bootstrap SNA, the admin will
             * first be reconfigured to have a store name while the bootstrap
             * SNA is still working, and will then notice that the SNA has been
             * replaced, so this check should result in switching to the new
             * SNA. Because the SNA is local, calls to it shouldn't really fail
             * otherwise.
             */
            if (isBootstrapSna) {
                final String storeName =
                    params.getGlobalParams().getKVStoreName();
                if (storeName != null) {
                    if (getSna(storeName)) {
                        try {
                            op.execute();
                            return;
                        } catch (RemoteException e) {
                        }
                    }
                }
            }

            /* Otherwise log the original exception */
            re = nsoe;
        } catch (RemoteException e) {
            re = e;
        }
        if (opDesc != null) {
            logger.log(Level.WARNING,
                       "Failed to " + opDesc + " to MgmtAgent",
                       re);
        }
    }

    void receiveStats(StatsPacket p) {
        if (sna == null) {
            return;
        }
        withAsyncSnaCallback(() -> sna.receiveAdminStats(p),
                             /* Don't report if stats not updated */
                             null);
    }

    public RoleResolver getRoleResolver() {
        return (adminSecurity == null) ? null : adminSecurity.getRoleResolver();
    }

    private void startWebService() {

        if (webServiceManager != null) {
            /*
             * When the first admin turned from bootstrap to normal admin.
             * The logger location will change, reset the logger handle
             * inside SkLogger so that all loggers can be refreshed.
             */
            webServiceManager.resetLogger(logger);
            return;
        }

        final StorageNodeParams snp = params.getStorageNodeParams();
        int httpPort = snp.getAdminWebPort();

        if (httpPort <= 0) {
            logger.info("Admin web service is not starting up, web port: " +
                        httpPort);
            return;
        }

        final SecurityParams secParams = params.getSecurityParams();

        SslContext ctx = null;
        if (secParams != null && secParams.isSecure() &&
            !secParams.allTransportSSLDisabled()) {
            try {
                KeyManagerFactory kmf = secParams.createSSLKeyManagerFactory();
                SslContextBuilder builder = SslContextBuilder.forServer(kmf);
                ctx = builder.build();
            } catch (Exception e) {
                logger.severe("Fail to build SSL context: " + e);
                throw new IllegalStateException(e);
            }
        }

        int httpsPort = 0;
        if (ctx != null) {
            httpsPort = httpPort;
            httpPort = 0;
        }

        webServiceManager =
            new AdminWebServiceManager(logger,
                                       commandService.getSecureService(),
                                       loginService, snp, httpPort, httpsPort,
                                       ctx);
    }

    /**
     * The plan state change tracker for updating the status receiver.
     */
    private class MgmtAgentPlanStateChangeTracker
        implements ViewListener<PlanStateChange> {

        @Override
        public void newInfo(ResourceId resourceId, PlanStateChange planState) {
            withAsyncSnaCallback(
                () -> sna.updatePlanStatus(planState.toJsonString()),
                "deliver plan state change");
        }
    }

    /**
     * The plan progress tracker for updating the status receiver.
     */
    private class MgmtAgentPlanProgressTracker
        implements ViewListener<PlanProgress> {

        @Override
        public void newInfo(ResourceId resourceId, PlanProgress planProgress) {
            withAsyncSnaCallback(
                () -> sna.updatePlanStatus(planProgress.toJsonString()),
                "deliver plan progress");
        }
    }

    /**
     * The parameter listener for updating the status receiver.
     */
    private class ParameterChangeListener implements ParameterListener {
        private final AdminId adminId;
        ParameterChangeListener(AdminId adminId) {
            this.adminId = adminId;
        }
        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            withAsyncSnaCallback(() -> sna.receiveNewParams(adminId, newMap),
                                 "deliver parameter change");
        }
    }

    private Executor getAsyncExecutor() {
        synchronized (asyncExecutorLock) {
            if (asyncExecutor == null) {
                asyncExecutor = new ThreadPoolExecutor(
                    0 /* corePoolSize */, Integer.MAX_VALUE /* maxPoolSize */,
                    ASYNC_EXECUTOR_KEEP_ALIVE_SECS, SECONDS,
                    new SynchronousQueue<>(),
                    new KVThreadFactory("AdminService misc async executor",
                                        logger));
            }
            return asyncExecutor;
        }
    }

    private void stopAsyncExecutor() {
        synchronized (asyncExecutorLock) {
            if (asyncExecutor != null) {

                /*
                 * We don't really know for sure if there are some last async
                 * calls coming in, so just tell the executor to exit all idle
                 * threads quickly
                 */
                asyncExecutor.setKeepAliveTime(1, MILLISECONDS);
                asyncExecutor.allowCoreThreadTimeOut(true);
            }
        }
    }

    /**
     * Rebinds the remote component in the registry associated with this SN,
     * returning the listen handle, or null if async is disabled.
     */
    <S extends VersionedRemote> ListenHandle
        rebind(S remoteComponent,
               String bindingName,
               ClientSocketFactory csf,
               ServerSocketFactory ssf,
               DialogTypeFamily dialogTypeFamily,
               ResponderDialogHandlerFactory<S> dialogHandlerFactory)
        throws RemoteException {

        final StorageNodeParams snp = params.getStorageNodeParams();
        final String hostName = snp.getHostname();
        final int registryPort = snp.getRegistryPort();

        logger.info("Starting " + bindingName + " on " + hostName + ":" +
                    registryPort);

        final ListenHandle listenHandle = RegistryUtils.rebind(
            hostName, registryPort, bindingName,
            params.getAdminParams().getAdminId(), remoteComponent, csf, ssf,
            dialogTypeFamily,
            () -> dialogHandlerFactory.createDialogHandler(
                remoteComponent, getAsyncExecutor(), logger),
            logger);
        if (listenHandle != null) {
            numAsyncServicesStarted++;
        }
        return listenHandle;
    }

    void unbind(String svcName,
                VersionedRemote secureService,
                ListenHandle asyncListenHandle)
        throws RemoteException {

        logger.info("Unbinding " + svcName);
        final StorageNodeParams snParams = params.getStorageNodeParams();
        final String hostName = snParams.getHostname();
        final int registryPort = snParams.getRegistryPort();
        try {
            RegistryUtils.unbind(hostName, registryPort, svcName,
                                 secureService, asyncListenHandle, logger);
        } catch (RemoteException re) {
            logger.warning("Problem unbinding " + svcName +
                           LoggerUtils.getStackTrace(re));
            throw re;
        }
    }

    /**
     * Helper class to manage RMI and async services.
     *
     * @param <S> the service interface
     */
    private class ServiceBinder<S extends VersionedRemote> {
        private final String svcName;
        private final S insecureService;
        private final SocketFactoryPair sfp;
        private S secureService;
        private ListenHandle asyncListenHandle;

        /**
         * Export a secure version of RMI and async services.
         *
         * @param svcName the service name
         * @param insecureService the non-secure RMI service
         * @param sfp the socket factories
         * @param dialogTypeFamily the async dialog type family, or null if no
         * async service should be exported
         * @param dialogHandlerFactory factory to create the dialog handler
         * given a secure RMI service, executor, and logger
         */
        ServiceBinder(String svcName,
                      S insecureService,
                      SocketFactoryPair sfp,
                      DialogTypeFamily dialogTypeFamily,
                      ResponderDialogHandlerFactory<S> dialogHandlerFactory) {
            this.svcName = svcName;
            this.insecureService = insecureService;
            this.sfp = sfp;

            /* create and wrap the services */
            initService(dialogTypeFamily, dialogHandlerFactory);
        }

        /**
         * Returns the insure version of the RMI service.
         */
        S getInsecureService() {
            return insecureService;
        }

        /**
         * Returns a wrapped version of the RMI service that implements secure
         * authorization and authentication checks.
         */
        S getSecureService() {
            return secureService;
        }

        /**
         * Wraps the service with the appropriate security proxy, and exports
         * and registers it as appropriate.
         */
        private void
            initService(DialogTypeFamily dialogTypeFamily,
                        ResponderDialogHandlerFactory<S> dialogHandlerFactory)
        {
            /* Wrap the insecure service in a secure proxy */
            try {
                secureService =
                    SecureProxy.create(insecureService,
                                       adminSecurity.getAccessChecker(),
                                       faultHandler);
                logger.info("Successfully created a secure proxy for " +
                            svcName);
            } catch (ConfigurationException ce) {
                throw new IllegalStateException
                    ("Unable to create a secure proxy for " + svcName, ce);
            }

            if (sfp.getServerFactory() != null) {
                sfp.getServerFactory().setConnectionLogger(logger);
            }

            try {
                asyncListenHandle = rebind(secureService, svcName,
                                           sfp.getClientFactory(),
                                           sfp.getServerFactory(),
                                           dialogTypeFamily,
                                           dialogHandlerFactory);
            } catch (RemoteException re) {
                final String msg = "Starting " + svcName + " failed";
                logger.severe(msg + ": " + LoggerUtils.getStackTrace(re));
                throw new IllegalStateException(msg, re);
            }
        }

        /** Stops the RMI and async services. */
        void shutdownService() {
            try {
                unbind(svcName, secureService, asyncListenHandle);
            } catch (RemoteException e) {
                /* Ignore */
            }
        }
    }
}
