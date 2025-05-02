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

package oracle.kv.impl.sna;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.KVVersion.CURRENT_VERSION;
import static oracle.kv.impl.async.StandardDialogTypeFamily.STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.TRUSTED_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.param.ParameterState.COMMON_MGMT_CLASS;
import static oracle.kv.impl.param.ParameterState.INETADDR_CACHE_NEGATIVE_POLICY_PROP;
import static oracle.kv.impl.param.ParameterState.INETADDR_CACHE_POLICY_PROP;
import static oracle.kv.impl.util.CommonLoggerUtils.exceptionString;
import static oracle.kv.impl.util.FileNames.RETRIEVE_SECURITY_UPDATES_FILE;
import static oracle.kv.impl.util.FileNames.SECURITY_UPDATES_DIR;
import static oracle.kv.impl.util.FileUtils.computeSha256Hash;
import static oracle.kv.impl.util.FileUtils.getFormattedFileModTime;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.file.NoSuchFileException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.KVStoreConfig;
import oracle.kv.KVVersion;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.SecurityParams.KrbPrincipalInfo;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.async.DialogTypeFamily;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.ResponderDialogHandlerFactory;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.measurement.ServiceStatusChange;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.mgmt.MgmtAgent;
import oracle.kv.impl.mgmt.MgmtAgentFactory;
import oracle.kv.impl.mgmt.MgmtUtil;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterTracker;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.ConfigurationException;
import oracle.kv.impl.security.SecureProxy;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater;
import oracle.kv.impl.security.login.TrustedLoginHandler;
import oracle.kv.impl.security.login.TrustedLoginImpl;
import oracle.kv.impl.security.login.TrustedLoginResponder;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.StorageNodeAgentAPI.CredentialHashes;
import oracle.kv.impl.sna.collector.CollectorService;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager.SNInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManagerInterface;
import oracle.kv.impl.sna.masterBalance.MasterBalancingInterface.MDInfo;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.ServiceCrashDetector;
import oracle.kv.impl.util.ServiceStatusTracker;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.SnapshotFileUtils;
import oracle.kv.impl.util.SnapshotFileUtils.SnapshotConfigTask;
import oracle.kv.impl.util.SnapshotFileUtils.SnapshotOp;
import oracle.kv.impl.util.SnapshotFileUtils.SnapshotTaskHandler;
import oracle.kv.impl.util.SnapshotFileUtils.UpdateConfigType;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils.InterfaceType;
import oracle.kv.impl.util.registry.ServerSocketFactory;
import oracle.kv.impl.util.registry.VersionedRemote;
import oracle.kv.impl.util.server.LoggerUtils;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * The class that does the work of the Storage Node Agent (SNA).  It is
 * mostly controlled by StorageNodeAgentImpl.
 */
public final class StorageNodeAgent {

    /* External commands, for "java -jar" usage. */
    public static final String START_COMMAND_NAME = "start";
    public static final String START_COMMAND_DESC =
        "starts StorageNodeAgent (and if configured, store) in kvroot";
    public static final String STOP_COMMAND_NAME = "stop";
    public static final String STOP_COMMAND_DESC =
        "stops StorageNodeAgent and services related to kvroot";
    public static final String STATUS_COMMAND_NAME = "status";
    public static final String STATUS_COMMAND_DESC =
        "status for StorageNodeAgent and services related to kvroot";
    public static final String RESTART_COMMAND_NAME = "restart";
    public static final String RESTART_COMMAND_DESC =
        "combines stop and start commands into one";
    public static final String CONFIG_FLAG = "-config";
    public static final String DISABLE_SERVICES_FLAG = "-disable-services";

    public static final String COMMAND_ARGS =
        CommandParser.getRootUsage() + " " +
        CommandParser.optional(CONFIG_FLAG + " <bootstrapFileName>") + " " +
        CommandParser.optional(DISABLE_SERVICES_FLAG);
    public static final String DEFAULT_CONFIG_FILE = "config.xml";
    public static final String DEFAULT_SECURITY_DIR = "security";
    public static final String DEFAULT_SECURITY_FILE = "security.xml";

    /*
     * Additional hidden args. The -shutdown flag is added by "java -jar" when
     * stop and restart commands are used.
     */
    public static final String SHUTDOWN_FLAG = "-shutdown";
    public static final String STATUS_FLAG = "-status";
    public static final String THREADS_FLAG = "-threads";
    public static final String LINK_COMMAND = "ln";
    public static final String RESTORE_FROM_SNAPSHOT =
        "-restore-from-snapshot";
    public static final String UPDATE_CONFIG_FLAG = "-update-config";
    public static final String ADDITIONAL_RESTORE_ARGS =
        CommandParser.optional(
             RESTORE_FROM_SNAPSHOT + " " +
             CommandParser.optional(UPDATE_CONFIG_FLAG + " <true|false>"));
    private static final String IBM_VENDOR_PREFIX = "IBM";

    /* The disable-services command has been removed */
    public static final String DISABLE_SERVICES_COMMAND_NAME =
        "disable-services";
    public static final String DISABLE_SERVICES_COMMAND_MSG =
        ", use the -disable-services" +
        "\noption with the " + START_COMMAND_NAME + ", " + STOP_COMMAND_NAME +
        ", or " + RESTART_COMMAND_NAME + " commands";

    /* We assume the admin guide's 256MB recommendation is used. */
    public static final int SN_ASSUMED_HEAP_MB =
        ParameterUtils.applyMinHeapMB(256);

    /*
     * RMI registry security properties name and required value,
     * introduced since Java 8 version 1.8.0_121.
     */
    public static final String RMI_REGISTRY_FILTER_NAME =
        "sun.rmi.registry.registryFilter";
    public static final String[] RMI_REGISTRY_FILTER_REQUIRED = {
        "oracle.kv.**", "java.lang.Enum"
    };
    public static final String RMI_REGISTRY_FILTER_DELIMITER = ";";

    /**
     * The total number of async services in the SNA (not including the
     * ServiceRegistry):
     * MonitorAgent
     * StorageNodeAgentInterface
     * TrustedLogin
     * StorageNodeAgentTestInterface
     */
    private static final int NUM_ASYNC_SERVICES = 4;

    /** The number of seconds to keep threads alive in the async executor. */
    private static final int ASYNC_EXECUTOR_KEEP_ALIVE_SECS = 30;

    /**
     * Timeout for shutdown of threads and thread pools so the process doesn't
     * hang.
     */
    public static final long SHUTDOWN_TIMEOUT_MS = 10 * 1000;

    private int jsonVersion = -1;

    private final StorageNodeAgentImpl snai;
    private StorageNodeAgentInterface exportableSnaif;
    /*
     * Many of these members are technically final once set but they cannot be
     * set in the constructor.
     */
    private String bootstrapDir;
    private String bootstrapFile;
    private File securityDir;
    private String securityConfigFile;
    private BootstrapParams bp;
    private SecurityParams sp;
    private GlobalParams globalParams;
    private File kvRoot;
    private File snRoot;
    private File kvConfigPath;
    private Registry registry;
    private String snaName;
    private StorageNodeId snid;
    private int serviceWaitMillis;
    private int repnodeWaitSecs;
    private int maxLink;
    private int linkExecWaitSecs;
    private boolean isWindows;
    private Boolean isLoopback;
    private boolean isVerbose;

    /* Used for configuration restore */
    private String restoreSnapshotName;
    private UpdateConfigType isUpdateConfig;

    /* SNP Information cached for reporting to mgmt. */
    int capacity;
    int logFileLimit;
    int logFileCount;
    int numCPUs;
    int memoryMB;
    private String storageDirectoriesString;
    private String rnLogDirectoriesString;
    private String adminDirectoryString;

    /**
     * The service status associated with the SNA. Note that only the following
     * subset of states: STARTING, WAITING_FOR_DEPLOY, and RUNNING are
     * currently relevant to the SNA.
     */
    private boolean createBootstrapAdmin;
    private final ServiceStatusTracker statusTracker;
    private volatile SnaLoggingStatsTracker loggingStatsTracker;
    private boolean useThreads;
    private Logger logger;
    private final Map<String, ServiceManager> repNodeServices;
    private final Map<String, ServiceManager> arbNodeServices;
    private ServiceManager adminService;
    private MonitorAgentImpl monitorAgent;
    private MgmtAgent mgmtAgent;
    private TrustedLoginImpl trustedLogin;
    private SNASecurity snaSecurity;
    private CollectorService collectorService;
    private final ParameterTracker snParameterTracker;
    private final ParameterTracker globalParameterTracker;

    /* SNAParser handles the command line arguments when issuing commands to
     * the SNA. The parser should be a member of the class so there's greater
     * resiliency in having some parsed results even if some of the arguments
     * throw errors.
     */
    private SNAParser snaParser;

    /* The master Balance manager component in the SNA. */
    private MasterBalanceManagerInterface masterBalanceManager;

    private String customProcessStartupPrefix;

    /**
     * The listening handle obtained when initiating shared listening
     * operations with the dialog layer, or null.
     */
    private ListenHandle asyncRegistryListenHandle;

    /**
     * Test only.  restart*Hook exists to "fake" exiting the process at various
     * times to test how things go when it is restarted.  It works by creating
     * a new SNA and shutting down the running one.  Note that the two RN hooks
     * apply to RNs and ANs.
     */
    private TestHook<StorageNodeAgent> restartRNHook;
    private TestHook<StorageNodeAgent> restartAdminHook;
    private TestHook<StorageNodeAgent> stopRNHook;

    /* Hook to inject failures at different points in SN execution */
    public static TestHook<Integer> FAULT_HOOK;

    /*
     * Test post hook invoked immediately after the MasterBalanceManager is
     * shutdown. The hook is static and fires for all SNs. It can be customized
     * if it only needs to fire for a specific SN.
     */
    static private TestHook<StorageNodeAgent> mbmPostShutdownHook;

    /** Test hook invoked before attempting to shutdown an RN */
    static volatile TestHook<Void> rnShutdownHook;

    /* Time when the StorageNodeAgent instance was created. */
    private final long serviceStartTime;

    /**
     * For executing miscellaneous async operations. Synchronize on
     * asyncMiscExecutorLock when accessing.
     */
    private ThreadPoolExecutor asyncMiscExecutor;
    private final Object asyncMiscExecutorLock = new Object();

    /**
     * The number of async services started -- used to confirm that
     * NUM_ASYNC_SERVICES is set properly.
     */
    private int numAsyncServicesStarted;

    private final Map<InterfaceType, ListenHandle> listenHandlesMap =
        new HashMap<>();

    /**
     * A constructor that allows the caller to indicate that the bootstrap
     * admin service should or should not be started.
     */
    StorageNodeAgent(StorageNodeAgentImpl snai, boolean createBootstrapAdmin) {

        this.snai = snai;
        bootstrapDir = null;
        bootstrapFile = null;
        securityDir = null;
        securityConfigFile = null;
        kvRoot = null;
        snRoot = null;
        kvConfigPath = null;
        registry = null;
        snaName = GlobalParams.SNA_SERVICE_NAME;
        snid = new StorageNodeId(0);
        logger = null;
        statusTracker = new ServiceStatusTracker(null /* logger */);
        mgmtAgent = null;
        useThreads = false;
        this.createBootstrapAdmin = createBootstrapAdmin;
        isLoopback = null;
        snParameterTracker = new ParameterTracker();
        globalParameterTracker = new ParameterTracker();

        repNodeServices = new HashMap<>();
        arbNodeServices = new HashMap<>();

        adminService = null;
        final String os = System.getProperty("os.name");
        if (os.indexOf("Windows") != -1) {
            isWindows = true;
        } else {
            isWindows = false;
        }
        restoreSnapshotName = null;
        isUpdateConfig = UpdateConfigType.UNKNOWN;
        serviceStartTime = System.currentTimeMillis();

    }

    public MasterBalanceManagerInterface getMasterBalanceManager() {
        return masterBalanceManager;
    }

    void setRNTestHook(TestHook<StorageNodeAgent> hook) {
        restartRNHook = hook;
    }

    void setStopRNTestHook(TestHook<StorageNodeAgent> hook) {
        stopRNHook = hook;
    }

    void setAdminTestHook(TestHook<StorageNodeAgent> hook) {
        restartAdminHook = hook;
    }

    public static void setMBMPostShutdownHook(TestHook<StorageNodeAgent>
                                               mbmPostShutdownHook) {

        StorageNodeAgent.mbmPostShutdownHook = mbmPostShutdownHook;
    }

    /**
     * For testing.
     */
    void setRepNodeWaitSecs(int seconds) {
        repnodeWaitSecs = seconds;
    }

    SNAParser getSNAParser () {
        return snaParser;
    }

    class SNAParser extends CommandParser {
        private boolean shutdown;
        private boolean status;
        private boolean disableServices;
        private String command = "start SNA";

        SNAParser(String[] args) {
            super(args);
        }

        public boolean getShutdown() {
            return shutdown;
        }

        public boolean getStatus() {
            return status;
        }

        public boolean getDisableServices() {
            return disableServices;
        }

        public String getCommand() {
            return command;
        }

        @Override
        protected void verifyArgs() {
            if (getRootDir() == null) {
                missingArg(ROOT_FLAG);
            } else {
                final String reason =
                        FileUtils.verifyDirectory(new File(getRootDir()));
                if (reason != null) {
                    final String msg = "Root directory " + reason;
                    throw new IllegalArgumentException(msg);
                }
            }
            if (bootstrapFile == null) {
                bootstrapFile = DEFAULT_CONFIG_FILE;
            }
            if (securityConfigFile == null) {
                securityConfigFile = DEFAULT_SECURITY_FILE;
            }

            if (restoreSnapshotName == null &&
                isUpdateConfig != UpdateConfigType.UNKNOWN) {
                usage(UPDATE_CONFIG_FLAG + " can only specify with " +
                      RESTORE_FROM_SNAPSHOT + " option");
            }

            isVerbose = getVerbose();
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(CONFIG_FLAG)) {
                bootstrapFile = nextArg(arg);
                return true;
            }
            if (arg.equals(SHUTDOWN_FLAG)) {
                command = "stop SNA";
                shutdown = true;
                return true;
            }
            if (arg.equals(STATUS_FLAG)) {
                command = "status SNA";
                status = true;
                return true;
            }
            if (arg.equals(DISABLE_SERVICES_FLAG)) {
                command = "stop SNA and disable services";
                disableServices = true;
                return true;
            }
            if (arg.equals(THREADS_FLAG)) {
                useThreads = true;
                return true;
            }
            if (arg.equals(RESTORE_FROM_SNAPSHOT)) {
                restoreSnapshotName = nextArg(arg);
                return true;
            }
            if (arg.equals(UPDATE_CONFIG_FLAG)) {
                final String updateConfig = nextArg(arg);
                if (updateConfig.equals(Boolean.toString(true))) {
                    isUpdateConfig = UpdateConfigType.TRUE;
                    return true;
                } else if (updateConfig.equals(Boolean.toString(false))) {
                    isUpdateConfig = UpdateConfigType.FALSE;
                    return true;
                } else {
                    usage("Flag " + arg + " requires boolean value");
                }
            }
            return false;
        }

        @Override
        public void usage(String errorMsg) {
            throw new IllegalArgumentException
                ((errorMsg == null ? "" : errorMsg + "\n") +
                 KVSTORE_USAGE_PREFIX + "\n\t<" +
                 START_COMMAND_NAME + " | " +
                 STOP_COMMAND_NAME + " | " +
                 STATUS_COMMAND_NAME + " | " +
                 RESTART_COMMAND_NAME + ">\n\t" +
                 COMMAND_ARGS +
                 ADDITIONAL_RESTORE_ARGS);
        }
    }

    SNAParser parseArgs(String args[]) {

        snaParser = new SNAParser(args);
        try {
            snaParser.parseArgs();
        } catch (IllegalArgumentException re) {
            jsonVersion = snaParser.getJsonVersion();
            throw re;
        }
        bootstrapDir = snaParser.getRootDir();
        jsonVersion = snaParser.getJsonVersion();

        return snaParser;
    }

    StorageNodeAgentAPI getRunningAgent(int openTimeout, int readTimeout)
        throws NotBoundException, RemoteException {

        final File configPath = new File(bootstrapDir, bootstrapFile);
        logger = LoggerUtils.getBootstrapLogger
            (bootstrapDir, FileNames.BOOTSTRAP_SNA_LOG, snaName);
        bp = ConfigUtils.getBootstrapParams(configPath, logger);

        initSecurity();
        sp.initRMISocketPolicies(logger);

        /*
         * TODO: We probably should be calling snp.setRegistryCSF if
         * it is available to us.  However, it only affects whether we use
         * the default timeouts or configured timeouts, so it's not
         * critical.
         */
        BootstrapParams.initRegistryCSF(sp);

        snaSecurity = new SNASecurity(this, bp, sp, null /* gp */,
                                      null /* snp */, logger);

        final StorageNodeAgentAPI snai1;
        if (bp.getStoreName() != null) {
            StorageNodeId snid1 = new StorageNodeId(bp.getId());
            String bn =
                RegistryUtils.bindingName(bp.getStoreName(),
                                          snid1.getFullName(),
                                          RegistryUtils.InterfaceType.MAIN);
            String csfName =
                ClientSocketFactory.factoryName(bp.getStoreName(),
                                                StorageNodeId.getPrefix(),
                                                InterfaceType.MAIN.
                                                interfaceName());

            final KVStoreConfig config = new KVStoreConfig(bp.getStoreName(),
                new HostPort(bp.getHostname(),
                             bp.getRegistryPort()).toString()).
                setSocketOpenTimeout(openTimeout, TimeUnit.MILLISECONDS).
                setSocketReadTimeout(readTimeout, TimeUnit.MILLISECONDS);

            /*
             * Specify the local address, which is needed to select the right
             * network for multi-homed TCP networks. Not needed for Unix domain
             * sockets.
             */
            final String hostname = bp.getHostname();
            if (NetworkAddress.isInetHostname(hostname)) {
                config.setLocalAddress(new InetSocketAddress(hostname, 0));
            }

            ClientSocketFactory.configure(csfName, config,
                                          null /* clientId */);

            snai1 = RegistryUtils.getStorageNodeAgent
                (bp.getHostname(), bp.getRegistryPort(), bn,
                 getLoginManager(), logger);
        } else {
            snai1 = RegistryUtils.getStorageNodeAgent
                (bp.getHostname(), bp.getRegistryPort(),
                 GlobalParams.SNA_SERVICE_NAME, getLoginManager(), logger);
        }
        return snai1;
    }

    /**
     * Using the bootstrap config file, attempt to stop the SNA process that it
     * using it.  Throws an exception if it fails.
     */
    public void stopRunningAgent() {
        try {
            /**
             * Set generous open and read timeouts when communicating with
             * the SNA to allow time for long RN shutdown times resulting
             * from checkpoints.
             */
            getRunningAgent(60000, 2 * 60 * 60 *1000)
                .shutdown(true /* stopServices */, false /* force */,
                          "SNA shutdown requested");
        } catch (RemoteException re) {
            throw new IllegalStateException(
                "Exception shutting down Storage Node Agent: " +
                re.getMessage(),
                re);
        } catch (NotBoundException nbe) {
            throw new IllegalStateException(
                "Unable to contact Storage Node Agent: " + nbe.getMessage(),
                nbe);
        }
    }

    /**
     * Using the bootstrap config file, attempt to get the SNA process status
     * Throws an exception if it fails.
     */
    ServiceStatus getRunningAgentStatus() {

        try {
            return getRunningAgent(60000, 5 * 60000).ping().getServiceStatus();
        } catch (RemoteException | NotBoundException re) {
            return ServiceStatus.UNREACHABLE;
        }
    }

    /**
     * Set the securityDir and sp fields, and check that the expected security
     * configuration file exists if the configuration calls for it.  The parser
     * should have been called, and the bp field set, before this method is
     * called.
     *
     * @throws IllegalStateException if a problem is found
     */
    private void initSecurity() {
        final String relSecurityDir = bp.getSecurityDir();
        if (relSecurityDir != null) {
            securityDir = new File(bootstrapDir, relSecurityDir);

            final File securityConfigPath = new File(securityDir,
                                                     securityConfigFile);
            if (securityConfigPath.exists()) {
                logger.info("Loading security configuration: " +
                            securityConfigPath);
                sp = ConfigUtils.getSecurityParams(securityConfigPath, logger);
            } else {
                securityDir = null;
                throw new IllegalStateException(
                    "Configuration declares that security should be " +
                    "present, but it was not found at " +
                    securityConfigPath);
            }
        } else {
            securityDir = null;
            sp = SecurityParams.makeDefault();
        }
    }

    /**
     * Start an instance of the Storage Node Agent.  It needs to know the
     * startup directory and configuration file.  The configuration file must
     * have this information as well:
     * 1.  Is the SNA registered or not (kvName is set)
     * 2.  Initial port for RMI and default service name
     * 3.  KV root directory
     */
    void start()
        throws IOException {

        /* Get bootstrap logger */
        logger = LoggerUtils.getBootstrapLogger
            (bootstrapDir, FileNames.BOOTSTRAP_SNA_LOG, snaName);
        /*
         * Create the status tracker here, but wait to log STARTING status
         * until we can update to the non-bootstrap logger
         */
        loggingStatsTracker = new SnaLoggingStatsTracker(this, logger);

        checkForConfigRecovery();

        final File configPath = new File(bootstrapDir, bootstrapFile);
        logger.info("Starting, configuration file: " + configPath);
        bp = ConfigUtils.getBootstrapParams(configPath, logger);
        /*
         * Sets the dns cache ttl values according to the BootstrapParams or
         * StorageNodeParams
         * There is no need to set dns cache ttl values if the store is in the
         * embedded mode.
         */
        if (!EmbeddedMode.isEmbedded()) {
            ConfigUtils.applyDnsCacheTTL(bp, logger);
        }
        setRMIRegistryFilterProperty();

        try {
            initSecurity();
        } catch (IllegalStateException e) {
            logger.severe(e.getMessage());
            throw new IllegalStateException(
                e.getMessage() + "\nUnable to continue without security");
        }
        sp.initRMISocketPolicies(logger);

        /*
         * Check software compatibility with the current store version if it
         * is set. Versions that differ only in the patch number are assumed
         * to be compatible.
         */
        final String storeVersionString = getStoreVersionString();
        if (storeVersionString != null) {
            final KVVersion storeVersion =
                    KVVersion.parseVersion(storeVersionString);
            /*
             * Compare the versions, ignoring the patch number.
             * comp ==
             * -1 - we are older than store version
             *  0 - same
             *  1 - we are newer than store version
             */
            final int comp =
                    VersionUtil.compareMinorVersion(CURRENT_VERSION,
                                                    storeVersion);
            if (comp < 0) {

                /* Version older than store version is not supported */
                throw new IllegalStateException(
                    "The store requires all nodes to run version " +
                    storeVersion.getNumericVersionString() +
                    " or later, but this node is running the older version " +
                    CURRENT_VERSION.getNumericVersionString() +
                    ", which is not supported");

            } else if (comp > 0) {
                /* Newer version - throws ISE if prerequisite check fails */
                VersionUtil.checkUpgrade(storeVersion, "store");
            } /* else equal, OK */
        }

        /*
         * Check software compatibility with the previously running SN. Versions
         * that differ only in the patch number are assumed to be compatible.
         */
        final KVVersion previousVersion = bp.getSoftwareVersion();
        assert previousVersion != null;

        boolean updateConfigFile = false;

        /*
         * Compare the versions, ignoring the patch number.
         * comp ==
         * -1 - we are older than previous version
         *  0 - same (ignore patch)
         *  1 - we are newer than previous version
         */
        final int comp = VersionUtil.compareMinorVersion(CURRENT_VERSION,
                                                         previousVersion);
        if (comp < 0) {
            /* Downgrade is not supported */
            throw new IllegalStateException("The previous version " +
                                 previousVersion.getNumericVersionString() +
                                 " is newer than " +
                                 CURRENT_VERSION.getNumericVersionString() +
                                 " and cannot be downgraded");
        } else if (comp > 0) {

            /* Upgrade - throws ISE if upgrade cannot be done */
            VersionUtil.checkUpgrade(previousVersion, "previous");

            logger.log(Level.INFO,
                       "Upgrade software version from {0} to {1}",
                       new Object[] {
                             previousVersion.getNumericVersionString(),
                             CURRENT_VERSION.getNumericVersionString()
                       });

            bp.setSoftwareVersion(CURRENT_VERSION);
            updateConfigFile = true;

        } else if (!previousVersion.equals(CURRENT_VERSION)) {
            /* Change in patch version (up or down) */
            logger.log(Level.INFO,
                       "Update software patch version from {0} to {1}",
                       new Object[] {
                             previousVersion.getNumericVersionString(),
                             CURRENT_VERSION.getNumericVersionString()
                       });

            bp.setSoftwareVersion(CURRENT_VERSION);
            updateConfigFile = true;
        } /* else equal, OK */

        if (bp.getRootdir() == null) {
            bp.setRootdir(bootstrapDir);
            updateConfigFile = true;
        }
        kvRoot = new File(bp.getRootdir());

        checkSecurityViolations(previousVersion);

        if (updateConfigFile) {
            ConfigUtils.createBootstrapConfig(bp, configPath, logger);
        }

        try {
            if (kvRoot.exists() && isRegistered()) {
                /* SNA is registered, do registered startup */
                startupRegistered();
            } else {
                /* SNA is not registered, do un-registered startup */
                startupUnregistered();
            }
        } catch (IOException e) {
            cleanupRegistry();
            throw e;
        }

        /*
         * At this point, both the SNASecurity and TrustedLoginImpl should have
         * been initialized, we add them as parameter listeners.
         */
        if (sp.isSecure()) {
            final LoginUpdater loginUpdater = new LoginUpdater();
            loginUpdater.addGlobalParamsUpdaters(snaSecurity);
            loginUpdater.addServiceParamsUpdaters(snaSecurity);
            if (trustedLogin != null) {
                loginUpdater.addGlobalParamsUpdaters(trustedLogin);
                loginUpdater.addServiceParamsUpdaters(trustedLogin);
            }

            snParameterTracker.addListener(
                loginUpdater.new ServiceParamsListener());
            globalParameterTracker.addListener(
                loginUpdater.new GlobalParamsListener());
        }
    }

    private String getStoreVersionString() {
        /*
         * Check for config file if exists, to read the SN config. If the
         * config file is not present, SN is not running.
         * If the store is not registered, SN is not deployed yet and so it is
         * not an upgrade scenario.
         * We can skip the prerequisite version check if the above condition
         * is not satisfied.
         */
        if (isRegistered()) {
            try {
                final StorageNodeId id = new StorageNodeId(bp.getId());
                File snaConfigFile = FileNames.getSNAConfigFile(bootstrapDir,
                                                                getStoreName(),
                                                                id);
                if (!snaConfigFile.exists()) {
                    logger.log(Level.WARNING,
                               "Storage node config not found.");
                    return null;
                }
                final GlobalParams gp =
                    ConfigUtils.getGlobalParams(snaConfigFile, logger);
                return gp.getStoreVersion();

            }catch (IllegalStateException ex) {
                /*
                 Ignore if the SNs config file is not found.
                 Not an upgrade scenario and do need prerequisite check.
                */
                if (ex.getCause() != null &&
                    (ex.getCause() instanceof IOException)) {
                    logger.log(Level.INFO,
                               "Storage node config could not be read " +
                               "with exception: {0}", ex);
                } else {
                    throw ex;
                }
            }
        }

        return null;
    }

    private void checkSecurityViolations(KVVersion previousVersion) {
        final boolean isSecureStore = (sp != null) && sp.isSecure();

        /* Check if  a secure registered SN break the security requirement */
        if (kvRoot.exists() && isRegistered() && isSecureStore) {

            /* initialize store path to find path of storage node config */
            initStorePaths();

            final StorageNodeParams snp;
            try {
                snp = ConfigUtils.getStorageNodeParams(kvConfigPath, logger);
            } catch (IllegalStateException e) {
                throw new IllegalStateException(
                    "Exception reading config file: " + e.getMessage());
            }

            /*
             * ensure that secure store cannot start up if FTS is in insecure
             * mode
             */
            if (snp != null) {
                final String esCluster =  snp.getSearchClusterName();
                final boolean esSecured = snp.isSearchClusterSecure();
                if (esCluster != null && !esCluster.isEmpty() && !esSecured) {
                    final boolean isUpgrade =
                                    !previousVersion.equals(CURRENT_VERSION);
                    throw new IllegalStateException(
                        "Secure store is not allowed if there is a " +
                        "registered ES cluster " + esCluster + ".  " +
                        (isUpgrade ?
                         "Please restart the storage node agent with the " +
                         "previous version (" +
                         previousVersion.getNumericVersionString() +
                         ") and either configure this secure store as a " +
                         "non-secure store to register an Elasticsearch " +
                         "cluster, or first deregister Elasticsearch Cluster" +
                         " in non secure store, start secure store and " +
                         "register secure ES Cluster." :
                         "Please configure this secure store as a " +
                         "non-secure store and deregister ES Cluster." +
                         "Then start the secure store and register a " +
                         "secure Elasticsearch Cluster." ));
                }
            }
        }
    }

    /**
     * Set required patterns for RMI registry filter.
     *
     * Since Java 8u121, RMI registry filter sun.rmi.registry.registryFilter is
     * enabled by default if using SSL and RMI, which is configured as a
     * sequence of patterns. It is required to add our filter patterns,
     * so our serialized classes won't be rejected by RMI registry. If there
     * is an existing filter pattern set in the system or security property,
     * add our required filter patterns to the existing set.
     */
    void setRMIRegistryFilterProperty() {
        /* TODO : after RMI is disabled, add code to disable setting
         * system property in embedded mode.
         */
        final String existingSecProperty =
            Security.getProperty(RMI_REGISTRY_FILTER_NAME);
        final String existingSysProperty =
            System.getProperty(RMI_REGISTRY_FILTER_NAME);

        /*
         * If users configured filter patterns in system property, the patterns
         * specified in security property won't take effect. For simplicity,
+         * always add our required patterns to filter set in system property.
         */
        String existingPatterns = null;
        if (existingSysProperty != null) {
            existingPatterns = existingSysProperty;
        } else if (existingSecProperty != null) {
            existingPatterns = existingSecProperty;
        }

        String patternSet = getRMIFilterSet(existingPatterns);
        if (patternSet != null) {
            System.setProperty(RMI_REGISTRY_FILTER_NAME, patternSet);
        }

        /*
         * Get system property first, if null, means it's not setting filter
         * in system property so the filter patterns in security property
         * should be applied on this node.
         */
        patternSet = System.getProperty(RMI_REGISTRY_FILTER_NAME);
        if (patternSet == null) {
            patternSet = existingSecProperty;
        }
        logger.info("RMI registry serial filter is configured as: " +
                    RMI_REGISTRY_FILTER_NAME + "=" + patternSet);
    }

    /**
     * Get RMI registry filter patterns. If specified property is null, return
     * required filter patterns. If it is not null, check if required patterns
     * exist in property value, if not, add them to the existing patterns. If
     * existing patterns contains required filter patterns, return null.
     */
    String getRMIFilterSet(String existingPatterns) {
        final String requiredPatterns =
            Arrays.stream(RMI_REGISTRY_FILTER_REQUIRED).
                collect(Collectors.joining(RMI_REGISTRY_FILTER_DELIMITER));

        if (existingPatterns == null) {
            return requiredPatterns;
        }
        final List<String> patterns = Arrays.asList(
            existingPatterns.split(RMI_REGISTRY_FILTER_DELIMITER));
        if (!Arrays.stream(RMI_REGISTRY_FILTER_REQUIRED).parallel().
            allMatch(s -> patterns.contains(s))) {
            return  requiredPatterns +
                    RMI_REGISTRY_FILTER_DELIMITER +
                    existingPatterns;
        }
        return null;
    }

    /**
     * Available for testing.  During testing, we allow the SNA to cohabitate
     * with the client, and the client may alter the socket policies.  This
     * resets them to standard configuration.
     */
    public void resetRMISocketPolicies() {
        sp.initRMISocketPolicies(logger);
        if (isRegistered()) {
            StorageNodeParams snp =
                ConfigUtils.getStorageNodeParams(kvConfigPath, logger);
            snp.setRegistryCSF(sp);
        } else {
            BootstrapParams.initRegistryCSF(sp);
        }
    }

    private void logwarning(String msg, Exception e) {
        logger.log(Level.WARNING, msg, e);
    }

    private void logsevere(String msg, Exception e) {
        logger.log(Level.SEVERE, msg, e);
    }

    private void revertToBootstrap() {

        try {
            File configPath = new File(bootstrapDir, bootstrapFile);
            bp.setStoreName(null);
            bp.setHostingAdmin(false);
            bp.setId(1);
            ConfigUtils.createBootstrapConfig(bp, configPath);
            snaName = GlobalParams.SNA_SERVICE_NAME;
            snid = new StorageNodeId(0);
        } catch (Exception e) {
            logsevere("Cannot revert to bootstrap configuration", e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Unregistered startup:
     * 1. Create a default registry
     * 2. Kill any running processes (bootstrap admin).
     * 3. Start up a bootstrap admin instance
     */
    private void startupUnregistered()
        throws IOException {

        statusTracker.update(ServiceStatus.STARTING);
        logStartup();

        /* Reset the count of async services started */
        numAsyncServicesStarted = 0;

        createRegistry(null);
        BootstrapParams.initRegistryCSF(sp);
        createAsyncRegistry(null);

        snaSecurity = new SNASecurity(this, bp, sp, null /* gp */,
                                      null /* snp */, logger);

        bindUnregisteredSNA();
        bindUnregisteredTrustedLogin();

        /*
         * Start the mgmt agent AFTER the RMI registry exists.  The JMX
         * implementation uses this registry.
         */
        mgmtAgent = MgmtAgentFactory.getAgent(this, null, statusTracker);

        capacity = bp.getCapacity();
        numCPUs = bp.getNumCPUs();
        memoryMB = bp.getMemoryMB();
        storageDirectoriesString = joinStringList(bp.getStorageDirPaths(), ",");
        rnLogDirectoriesString = joinStringList(bp.getRNLogDirPaths(), ",");
        adminDirectoryString = joinStringList(bp.getAdminDirPath(), ",");

        /**
         * The fault handler needs a logger in the event an exception occurs
         * prior to, or during registration.
         */
        snai.getFaultHandler().setLogger(logger);

        /**
         * If restarted without being registered the bootstrap admin needs to
         * be killed if present, otherwise it will fail to start up because its
         * http port will be in use.  In the event that there is a new config
         * file with a different port also match based on kvhome and configfile
         * name.
         */
        ManagedService.killManagedProcesses
            (getStoreName(), makeBootstrapAdminName(), getLogger());

        ManagedService.killManagedProcesses
            (bootstrapDir, bootstrapFile, getLogger());

        startBootstrapAdmin();

        if (numAsyncServicesStarted > NUM_ASYNC_SERVICES) {
            throw new IllegalStateException(
                "The number of async services started (" +
                numAsyncServicesStarted +
                ") exceeds NUM_ASYNC_SERVICES (" +
                NUM_ASYNC_SERVICES + ")");
        }

        statusTracker.update(ServiceStatus.WAITING_FOR_DEPLOY);
    }

    private synchronized void startupRegistered()
        throws IOException {

        initStorePaths();
        logger.info("Registered startup, config file: " + kvConfigPath);

        /* Reset the count of async services started */
        numAsyncServicesStarted = 0;

        /*
         * Ensure store directories exist. This is necessary for the purpose of
         * store upgrade, i.e., a directory created during register() for the
         * upgraded code does not exist in the old store.
         */
        ensureStoreDirectory();
        StorageNodeParams snp = null;
        GlobalParams gp = null;
        try {
            snp = ConfigUtils.getStorageNodeParams(kvConfigPath, logger);
            gp = ConfigUtils.getGlobalParams(kvConfigPath, logger);
        } catch (IllegalStateException e) {
            logger.info("Exception reading config file: " + e.getMessage());
        }
        if (snp == null || gp == null) {
            logger.info("Could not get required parameters, reverting to " +
                        "unregistered state");
            revertToBootstrap();
            cleanupRegistry();

            /* This is a recursive call */
            start();
            return;
        }
        globalParams = gp;

        /* Verify the SN ParameterMap before startup any service. */
        try {
            checkSNParams(snp.getMap(), gp.getMap());
        } catch (IllegalArgumentException e) {
            logger.severe(e.toString());
        }
        try {
            checkGlobalParams(gp.getMap(), snp.getMap());
        } catch (IllegalArgumentException e) {
            logger.severe(e.toString());
        }
        /**
         * Set up monitoring and reset logger to use the real snid.  Monitor
         * must be set up before the logger is created because the
         * AgentRepository registers itself in LoggerUtils.
         */
        setupMonitoring(gp, snp);
        final File loggingDir =
            FileNames.getLoggingDir(kvRoot, getStoreName());
        logger.info("Changing log files to directory: " + loggingDir);
        logger = LoggerUtils.getLogger(StorageNodeAgentImpl.class, gp, snp);
        statusTracker.setLogger(logger);
        statusTracker.setCrashDetector(
            new ServiceCrashDetector(snp.getStorageNodeId(), loggingDir,
                                     logger));
        statusTracker.update(ServiceStatus.STARTING);
        snai.getFaultHandler().setLogger(logger);
        loggingStatsTracker.setLogger(logger);

        /*
         * If snaSecurity was already created, clean up it's token cache thread
         */
        if (snaSecurity != null) {
            snaSecurity.stop();
        }
        snaSecurity = new SNASecurity(this, bp, sp, gp, snp, logger);

        /*
         * Any socket timeouts observed by the ClientSocketFactory in this
         * process will be logged to this logger.
         */
        ClientSocketFactory.setTimeoutLogger(logger);

        logger.info("Starting StorageNodeAgent for " + getStoreName());
        logStartup();
        statusTracker.setLogger(logger);

        /**
         * The Registry is required for services to start.
         */
        if (registry == null) {
            createRegistry(snp);
        }

        if (asyncRegistryListenHandle == null) {
            createAsyncRegistry(snp);
        }

        /**
         * Initialize state from parameters.
         */
        snid = snp.getStorageNodeId();
        initSNParams(snp);

        /* Set up the CSF that will be used to access services. */
        snp.setRegistryCSF(sp);

        snai.startTestInterface();

        /**
         * Kill leftover managed processes and start new ones.
         */
        cleanupRunningComponents();

        /*
         * Bind the Trusted Login interface so that components can access it
         * when they start up.
         */
        unbind(trustedLogin, GlobalParams.SNA_LOGIN_SERVICE_NAME,
               InterfaceType.TRUSTED_LOGIN);
        bindRegisteredTrustedLogin(gp, snp);

        try {
            /*
             *  Note: when security is enabled and mgmtClass is JmxAgent, the
             *  default client socket factory will be initialized with SNA
             *  store truststore in JmxAgent and used by collector service.
             */
            collectorService = new CollectorService(snp, gp, sp,
                                                    getLoginManager(),
                                                    logger);
            globalParameterTracker.addListener(
                collectorService.getGlobalParamsListener());
            snParameterTracker.addListener(
                collectorService.getSNParamsListener());
            logger.info("Create collector service and register parameter "
                + "listeners successfully.");
        } catch (Exception e) {
            logger.severe("Failed to create collector service: " +
                           LoggerUtils.getStackTrace(e));
        }
        /*
         * Must precede the startup of the RN components, so their state can be
         * tracked.
         */
        startMasterBalanceManager(snp.getMasterBalance());
        startComponents();

        monitorAgent.startup();

        /**
         * Rebind using new name.  Unbind, change name, rebind.
         */
        unbind(exportableSnaif, snaName, InterfaceType.MAIN);
        snaName = snid.getFullName();

        bindRegisteredSNA(snp);

        if (adminService != null) {
            adminService.registered(this);
        }

        if (numAsyncServicesStarted > NUM_ASYNC_SERVICES) {
            throw new IllegalStateException(
                "The number of async services started (" +
                numAsyncServicesStarted +
                ") exceeds NUM_ASYNC_SERVICES (" +
                NUM_ASYNC_SERVICES + ")");
        }

        statusTracker.update(ServiceStatus.RUNNING);

        logger.info("Started StorageNodeAgent for " + getStoreName());
    }

    /** Log information about service startup */
    private void logStartup() {
        LoggerUtils.logSoftwareVersions(logger);
        ProcessHandle.current().info().commandLine().ifPresent(
            cmdline -> logger.info("SNA process command line: " + cmdline));
    }

    private void bindUnregisteredSNA()
        throws RemoteException {

        final RMISocketPolicy policy = sp.getRMISocketPolicy();
        final SocketFactoryPair sfp = bp.getStorageNodeAgentSFP(policy);

        initExportableSnaif();

        if (sfp.getServerFactory() != null) {
            sfp.getServerFactory().setConnectionLogger(logger);
        }

        rebind(exportableSnaif, snaName, InterfaceType.MAIN,
               sfp.getClientFactory(), sfp.getServerFactory(),
               STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY,
               StorageNodeAgentInterfaceResponder::new);
        logger.info("Bound to registry port " + getRegistryPort() +
                    " using name " + snaName +
                    " with SSF:" + sfp.getServerFactory());
    }

    <S extends VersionedRemote>
        void rebind(S remoteComponent,
                    String serviceName,
                    InterfaceType type,
                    ClientSocketFactory csf,
                    ServerSocketFactory ssf,
                    DialogTypeFamily dialogTypeFamily,
                    ResponderDialogHandlerFactory<S> dialogHandlerFactory)
        throws RemoteException {

        final ListenHandle listenHandle = RegistryUtils.rebind(
            getHostname(), getRegistryPort(), serviceName,
            snid, remoteComponent, csf, ssf, dialogTypeFamily,
            () -> dialogHandlerFactory.createDialogHandler(
                remoteComponent, getAsyncMiscExecutor(), logger),
            logger);
        if (listenHandle != null) {
            numAsyncServicesStarted++;
            listenHandlesMap.put(type, listenHandle);
        }
    }

    void unbind(VersionedRemote remoteComponent,
                String serviceName,
                InterfaceType type)
        throws RemoteException {

        final String hostName = getHostname();
        final int registryPort = getRegistryPort();
        final ListenHandle listenHandle = listenHandlesMap.get(type);
        RegistryUtils.unbind(hostName, registryPort, serviceName,
                             remoteComponent, listenHandle, logger);
    }

    private void bindUnregisteredTrustedLogin()
        throws RemoteException {

        final RMISocketPolicy trustedPolicy = sp.getTrustedRMISocketPolicy();

        if (trustedPolicy != null) {
            final SocketFactoryPair tsfp =
                bp.getSNATrustedLoginSFP(trustedPolicy);
            final String snaTLName = GlobalParams.SNA_LOGIN_SERVICE_NAME;
            final SNAFaultHandler faultHandler =
                new SNAFaultHandler(logger, statusTracker);
            final TrustedLoginHandler loginHandler =
                new TrustedLoginHandler(snid, true /* localId */);
            trustedLogin = new TrustedLoginImpl(faultHandler, loginHandler,
                                                logger);

            if (tsfp.getServerFactory() != null) {
                tsfp.getServerFactory().setConnectionLogger(logger);
            }

            rebind(trustedLogin, snaTLName, InterfaceType.TRUSTED_LOGIN,
                   tsfp.getClientFactory(), tsfp.getServerFactory(),
                   TRUSTED_LOGIN_TYPE_FAMILY,
                   TrustedLoginResponder::new);
            logger.info("Bound trusted login to registry port " +
                        getRegistryPort() + " using name " + snaTLName +
                        " with SSF:" + tsfp.getServerFactory());
        }
    }

    private void bindRegisteredSNA(StorageNodeParams snp)
        throws RemoteException {

        final String csfName =
            ClientSocketFactory.factoryName(getStoreName(),
                                            StorageNodeId.getPrefix(),
                                            RegistryUtils.InterfaceType.
                                            MAIN.interfaceName());
        final RMISocketPolicy rmiPolicy = sp.getRMISocketPolicy();
        final SocketFactoryPair sfp =
            snp.getStorageNodeAdminSFP(rmiPolicy, csfName);

        initExportableSnaif();

        if (sfp.getServerFactory() != null) {
            sfp.getServerFactory().setConnectionLogger(logger);
        }

        final String serviceName = RegistryUtils.bindingName(
            getStoreName(), snaName, InterfaceType.MAIN);
        rebind(exportableSnaif, serviceName, InterfaceType.MAIN,
               sfp.getClientFactory(), sfp.getServerFactory(),
               STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY,
               StorageNodeAgentInterfaceResponder::new);
        logger.info("Rebound to registry port " + getRegistryPort() +
                    " using name " + snaName + " with SSF:" +
                    sfp.getServerFactory());
    }

    private void bindRegisteredTrustedLogin(GlobalParams gp,
                                            StorageNodeParams snp)
        throws RemoteException {

        final RMISocketPolicy trustedPolicy =
            sp.getTrustedRMISocketPolicy();
        if (trustedPolicy != null) {
            final SocketFactoryPair tsfp =
                bp.getSNATrustedLoginSFP(trustedPolicy);
            final SNAFaultHandler faultHandler =
                new SNAFaultHandler(logger, statusTracker);
            final long sessionTimeout =
                gp.getSessionTimeoutUnit().toMillis(gp.getSessionTimeout());
            final int sessionLimit = snp.getSessionLimit();
            final TrustedLoginHandler loginHandler =
                new TrustedLoginHandler(snid, false /* localId */,
                                        sessionTimeout, sessionLimit);
            final String snaTLName = GlobalParams.SNA_LOGIN_SERVICE_NAME;
            trustedLogin = new TrustedLoginImpl(
                faultHandler, loginHandler, logger);

            if (tsfp.getServerFactory() != null) {
                tsfp.getServerFactory().setConnectionLogger(logger);
            }

            rebind(trustedLogin, snaTLName, InterfaceType.TRUSTED_LOGIN,
                   tsfp.getClientFactory(), tsfp.getServerFactory(),
                   TRUSTED_LOGIN_TYPE_FAMILY,
                   TrustedLoginResponder::new);
            logger.info("Bound trusted login to registry port " +
                        getRegistryPort() + " using name " + snaTLName +
                        " with SSF:" + tsfp.getServerFactory());
        }
    }

    void checkRegistered(String method)
        throws IllegalStateException {

        if (getStoreName() == null) {
            throw new IllegalStateException
                (method + ": Storage Node Agent is not registered");
        }
    }

    private void initExportableSnaif() {
        try {
            exportableSnaif =
                SecureProxy.create(snai, snaSecurity.getAccessChecker(),
                                   snai.getFaultHandler());
            logger.info(
                "Successfully created secure proxy for the storage node agent");
        } catch (ConfigurationException ce) {
            throw new IllegalStateException("Unabled to create proxy", ce);
        }
    }

    @SuppressWarnings("unused")
    private StorageNodeAgentImpl getImpl() {
        return snai;
    }

    StorageNodeStatus getStatus(AuthContext authCtx,
                                short serialVersion) {
        try {
            MasterBalanceManagerInterface mbm = getMasterBalanceManager();
            final Boolean isMasterBalanced;
            if (mbm == null) {
                isMasterBalanced = null;
            } else {
                final MDInfo mdInfo = mbm.getMDInfo(authCtx, serialVersion);
                isMasterBalanced =
                    (mdInfo == null) ? null : !mdInfo.hasExcessMasters();
            }

            return new StorageNodeStatus(statusTracker.getServiceStatus(),
                                         isMasterBalanced, serviceStartTime);
        } catch (RemoteException e) {
            /* No RMI calls are involved. */
           throw new IllegalStateException("RMI exception is impossible:" + e);
        }
    }

    MonitorAgentImpl getMonitorAgent() {
        return monitorAgent;
    }

    /**
     * Initialize store variables.
     */
    private File initStorePaths() {
        snRoot = ConfigUtils.getSNRootDir(bp);
        kvConfigPath = ConfigUtils.getSNConfigPath(bp);
        return FileNames.getKvDir(kvRoot.toString(), getStoreName());
    }

    /**
     * Ensure that the store directory exists, has a security policy file and a
     * security properties directory.
     */
    private void ensureStoreDirectory() {

        final File kvDir = initStorePaths();
        if (!snRoot.isDirectory()) {
            if (FileNames.makeDir(snRoot)) {
                logger.info("Created a new store directory: " + snRoot);
            }
        }

        /**
         * Make sure there's a Java security policy file and if not, copy it
         * from the bootstrap directory.  This file goes into the store
         * directory.
         */
        final File javaSecPolicy = FileNames.getSecurityPolicyFile(kvDir);
        if (!javaSecPolicy.exists()) {
            logger.log(Level.FINE, "Creating security policy file: {0}",
                       javaSecPolicy);
            final File fromFile =
                new File(bootstrapDir, FileNames.JAVA_SECURITY_POLICY_FILE);
            if (!fromFile.exists()) {
                throw new IllegalStateException
                    ("Cannot find bootstrap security file " + fromFile);
            }
            try {
                FileUtils.copyFile(fromFile, javaSecPolicy);
            } catch (IOException ie) {
                throw new IllegalStateException
                    ("Could not create policy file", ie);
            }
        }

        FileNames.makeSecurityPropertyDir(snRoot);
    }

    protected Registry getRegistry() {
        return registry;
    }

    public int getRegistryPort() {
        return bp.getRegistryPort();
    }

    public String getServiceName() {
        return snaName;
    }

    public StorageNodeId getStorageNodeId() {
        return snid;
    }

    /**
     * These next few are for testing purposes.
     */
    protected int getServiceWaitMillis() {
        return serviceWaitMillis;
    }
    protected int getRepnodeWaitSecs() {
        return repnodeWaitSecs;
    }

    protected int getMaxLink() {
        return maxLink;
    }

    protected int getLinkExecWaitSecs() {
        return linkExecWaitSecs;
    }

    /**
     * Create a Registry if not already done.  Because the SNA does not change
     * the registry port when it is "registered" the bootstrap registry can
     * remain unmodified.
     * @param snp
     */
    private void createRegistry(StorageNodeParams snp)
        throws RemoteException {

        /*
         * Initialize the endpoint group first, since it is needed to create
         * shared server sockets.
         */
        final int numEndpointGroupThreads =

            /* No parameters -- just use the minimum number of threads */
            (snp == null) ?
            Integer.parseInt(
                ParameterState.SN_ENDPOINT_GROUP_THREADS_FLOOR_DEFAULT) :

            /*
             * Running services in processes -- use the minimum number of
             * threads for the endpoint group since this process will only host
             * the SNA
             */
            (!useThreads) ?
            snp.getEndpointGroupThreadsFloor() :

            /*
             * Running services in threads in this process -- use the number of
             * threads appropriate for an RN
             */
            snp.calcEndpointGroupNumThreads();

        final int maxQuiescentSeconds =
            (snp == null) ?
            ParameterState.
            SN_ENDPOINT_GROUP_MAX_QUIESCENT_SECONDS_VALUE_DEFAULT:
            snp.getEndpointGroupMaxQuiescentSeconds();

        AsyncRegistryUtils.initEndpointGroup(
            logger, numEndpointGroupThreads, maxQuiescentSeconds,
            false /* forClient */,
            /*
             * Use the RN default for async max concurrent requests as the
             * limit for the SN registry
             */
            ParameterState.RN_RH_ASYNC_MAX_CONCURRENT_REQUESTS_DEFAULT,
            new SNAFaultHandler(logger, statusTracker),

            /*
             * Don't fail if the endpoint group already exists since kvlite,
             * and lots of tests, sometimes run multiple SNAs in the same JVM
             */
            false /* failIfPresent */);

        /* Set the server hostname for remote objects */
        AsyncRegistryUtils.setServerHostName(getHostname());

        final RMISocketPolicy rmiPolicy = sp.getRMISocketPolicy();
        final ServerSocketFactory ssf = (snp == null) ?
            StorageNodeParams.getDefaultRegistrySSF(rmiPolicy) :
            snp.getRegistrySSF(rmiPolicy);

        if (!RegistryUtils.serverUseRmi) {
            logger.info("RMI disabled: not creating RMI registry");
            return;
        }

        logger.info("Creating a Registry on " +
                    getHostname() + ":" + getRegistryPort() +
                    " server socket factory:" + ssf);

        /* Note that no CSF is supplied. */

        ExportException throwEE = null ;
        /* A little over 2 min (the CLOSE_WAIT timeout.) */
        final int limitMs = 128000;
        final int retryPeriodMs = 1000;
        int totalWaitMs = 0;
        do {
            try {
                throwEE = null;
                registry = RegistryUtils.createRegistry(getRegistryPort(),
                                                        null, ssf);
                return;
            } catch (ExportException ee) {
                throwEE = ee;
                if (TestStatus.isActive() &&
                    (ee.getCause() instanceof BindException)) {

                    logger.info("Registry bind exception:" +
                                ee.getCause().getMessage() +
                                " Registry port:" + getRegistryPort());
                    try {
                        Thread.sleep(retryPeriodMs);
                    } catch (InterruptedException e) {
                        throw throwEE;
                    }
                    continue;
                }
                throw throwEE;
            }
        } while ((totalWaitMs += retryPeriodMs) <= limitMs);

        if (TestStatus.isActive() &&
            (throwEE.getCause() instanceof BindException)) {

            /*
             * Log information to help identify the process currently binding
             * the required port.
             */
            /*
             * Log all java processes and their args. Assumes jps is available
             * on the search path.
             */
            logger.info(RepUtils.exec("jps", "-v"));
            /*
             * Log all processes binding tcp ports. Note that the args are
             * linux-specific.
             */
            logger.info(RepUtils.exec("netstat", "-lntp"));
        }

        /* Timed out after retries in test. */
        throw throwEE;
    }

    private void cleanupRegistry() {
        /**
         * Unbind this object, trusted login if it exists,  and clean up
         * registry.  Don't throw.
         */
        try {
            if (isRegistered()) {
                try {
                    unbind(trustedLogin, GlobalParams.SNA_LOGIN_SERVICE_NAME,
                           InterfaceType.TRUSTED_LOGIN);
                } catch (Exception e) {
                    /* Ignore */
                }
                try {
                    unbind(exportableSnaif, snaName, InterfaceType.MAIN);
                } catch (Exception e) {
                    /* Ignore */
                }
                if (monitorAgent != null) {
                    try {
                        monitorAgent.stop();
                    } catch (Exception e) {
                        /* Ignore */
                    }
                }
                try {
                    snai.stopTestInterface();
                } catch (Exception e) {
                    /* Ignore */
                }
            } else {
                try {
                    unbind(trustedLogin, GlobalParams.SNA_LOGIN_SERVICE_NAME,
                           InterfaceType.TRUSTED_LOGIN);
                } catch (Exception e) {
                    /* ignore */
                }
                try {
                    unbind(exportableSnaif, snaName, InterfaceType.MAIN);
                } catch (Exception e) {
                    /* ignore */
                }
            }
            if (registry != null) {
                try {
                    UnicastRemoteObject.unexportObject(registry, true);
                } catch (Exception e) {
                    /* Ignore */
                }
            }
        } catch (Exception ignored) {
        } finally {
            registry = null;
        }
        cleanupAsyncRegistry();
    }

    private void createAsyncRegistry(StorageNodeParams snp)
        throws IOException {

        if (!AsyncRegistryUtils.serverUseAsync) {
            return;
        }
        final RMISocketPolicy rmiPolicy = sp.getRMISocketPolicy();
        final ServerSocketFactory ssf = (snp == null) ?
            StorageNodeParams.getDefaultRegistrySSF(rmiPolicy) :
            snp.getRegistrySSF(rmiPolicy);
        asyncRegistryListenHandle = AsyncRegistryUtils.createRegistry(
            getHostname(), getRegistryPort(), ssf, logger);
    }

    private void cleanupAsyncRegistry() {
        if (asyncRegistryListenHandle != null) {
            asyncRegistryListenHandle.shutdown(false);
            asyncRegistryListenHandle = null;
        }
    }

    /*
     * Set system information:
     * 1. The number of CPUs based on reality
     * 2. If available, the amount of real memory in the system
     * NOTE: if using threads for managing services setting memoryMB can
     * result in over-allocation of RepNode parameters (e.g. je.maxMemory)
     * without direct control over the corresponding Java heap, so in threads
     * mode do not set memoryMB.
     */
    private void setSystemInfo() {
        OperatingSystemMXBean bean =
            ManagementFactory.getOperatingSystemMXBean();
        logger.info("System architecture is " + bean.getArch());
        int ncpu = bp.getNumCPUs();
        if (ncpu == 0) {
            ncpu = bean.getAvailableProcessors();
            logger.info("Setting number of CPUs to " + ncpu);
            bp.setNumCPUs(ncpu);
        }
        /*
         * Don't set memoryMB for threads, or in unit tests where we use many
         * RNs on a single machine.
         */
        if ((!useThreads) && !TestStatus.manyRNs()) {
            int mb = bp.getMemoryMB();
            if (mb == 0) {
                long bytes = JVMSystemUtils.ZING_JVM ?
                    JVMSystemUtils.getSystemZingMemorySize() :
                    getTotalPhysicalMemorySize(bean);
                if (bytes != 0) {
                    mb = (int) (bytes >> 20);
                    logger.info("Setting memory MB to " + mb);
                    bp.setMemoryMB(mb);
                } else {
                    logger.info("Cannot get memory size");
                }
            }
        }
    }

   /**
    * Get the get the total physical memory available on the machine if the
    * mbean implements the getTotalPhysicalMemorySize method (the Sun mbean
    * does). If the mbean does not implement the method, it returns 0.
    * It is a little more complicated than above... if running a 32-bit JVM
    * and capacity 1 the memoryMB should not be > 2G or the JVM will fail to
    * start.  Logic is added for this situation.
    */
    private long getTotalPhysicalMemorySize(OperatingSystemMXBean bean) {
        Class<?> beanClass = bean.getClass();
        try {
            final int maxInt = Integer.MAX_VALUE;
            final String jvmVendor = System.getProperty("java.vendor");
            final Method m;
            if (jvmVendor != null && jvmVendor.startsWith(IBM_VENDOR_PREFIX)) {
                m = beanClass.getMethod("getTotalPhysicalMemory");
            } else {

                /*
                 * Specify the interface explicitly so we call the interface
                 * method, not the class method, because it is the interface
                 * that is exported by the jdk.management module.
                 */
                beanClass =
                    Class.forName("com.sun.management.OperatingSystemMXBean");
                m = beanClass.getMethod("getTotalPhysicalMemorySize");
            }
            m.setAccessible(true); /* Since it's a native method. */
            long mem = (Long)m.invoke(bean);

            /*
             * This call will work because if the above worked we are likely
             * using a Sun JVM.
             */
            String bits = System.getProperty("sun.arch.data.model");
            if (bits == null) {
                return mem;
            }
            int intBits = Integer.parseInt(bits);
            if (intBits == 32) {
                int cap = bp.getCapacity();
                if (mem / cap > maxInt) {
                    logger.info("Reducing total memory from " +
                                mem + " to " + (maxInt * cap) + " bytes");
                    mem = maxInt * cap;
                }
            }
            return mem;
        } catch (Exception e) {
            logger.warning("Unable to obtain physical memory size: " + e);
            return 0;
        }
    }

    /**
     * Provide a shutdown hook so that if the SNA is killed externally it can
     * attempt to cleanly shut down managed services.  There is no reason to
     * unbind from the RMI Registry since the process is exiting.  This hook
     * will only be installed for registered SNA instances.
     *
     * NOTE: if/when we allow RepNodes and Admin instances to stay up and
     * re-register themselves if the SNA dies, this code must change.
     *
     * NOTE: the Logger instance used here may already be shut down and the
     * information not logged.
     */
    private class ShutdownThread extends Thread {

        @Override
        public void run() {
            /* if statusTracker is null there is nothing to shut down */
            if ((statusTracker != null) &&
                ((statusTracker.getServiceStatus() == ServiceStatus.RUNNING) ||
                 (statusTracker.getServiceStatus() ==
                  ServiceStatus.WAITING_FOR_DEPLOY))) {
                logger.info("Shutdown thread running, stopping services");
                try {
                    shutdown(true /* stopService */, false /* force */,
                             "Storage node exit");
                } finally {
                    logger.info("Shutdown thread exiting");
                }
            }
        }
    }

    void addShutdownHook() {
        if (logger != null) {
            logger.fine("Adding shutdown hook");
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
    }

    public BootstrapParams getBootstrapParams() {
        return bp;
    }

    public String getStoreName() {
        return bp.getStoreName();
    }

    public String getHostname() {
        return bp.getHostname();
    }

    public String getHAHostname() {
        return bp.getHAHostname();
    }

    boolean isLoopbackAddress() {
        if (isLoopback == null) {
            isLoopback = checkLoopback(getHAHostname());
        }
        return isLoopback;
    }

    public String getHAPortRange() {
        return bp.getHAPortRange();
    }

    public String getServicePortRange() {
        return bp.getServicePortRange();
    }

    public Logger getLogger() {
        return logger;
    }

    public String getBootstrapDir() {
        return bootstrapDir;
    }

    public String getBootstrapFile() {
        return bootstrapFile;
    }

    public File getSecurityDir() {
        return securityDir;
    }

    public String getSecurityConfigFile() {
        return securityConfigFile;
    }

    /**
     * Return Kerberos principal configuration information from security
     * parameter.
     */
    KrbPrincipalInfo getKrbPrincipalInfo() {
        return sp.getKerberosPrincipalInfo();
    }

    public File getKvConfigFile() {
        return kvConfigPath;
    }

    /**
     * Return the value of the processStartPrefix property.
     */
    String getCustomProcessStartupPrefix() {
        return customProcessStartupPrefix;
    }

    boolean verbose() {
        return isVerbose;
    }

    /**
     * Advisory interface indicating whether the RepNode is running.  This is
     * good for testing but should not be trusted 100%.
     */
    boolean isRunning(RepNodeId rnid) {
        ServiceManager mgr = repNodeServices.get(rnid.getFullName());
        if (mgr != null) {
            return mgr.isRunning();
        }
            return false;
    }

    /*
     * Useful for testing
     */
    public ServiceManager getServiceManager(RepNodeId rnid) {
        return repNodeServices.get(rnid.getFullName());
    }

    public ServiceManager getAdminServiceManager() {
        return adminService;
    }

    public static boolean checkLoopback(String host) {
        InetSocketAddress isa = new InetSocketAddress(host, 0);
        return isa.getAddress().isLoopbackAddress();
    }

    /**
     * Start all components that are configured.
     */
    private void startComponents() {

        /**
         * Admins are arbitrarily started first. A future feature
         * having Admins push metadata to the SN may benefit from
         *  Admins being started first.
         * Start the Admin if this SN is hosting it. It may already be running.
         * This will be the case during registration of the SNA that is hosting
         * the Admin.
         */
        if (adminService == null) {
            AdminParams ap = ConfigUtils.getAdminParams(kvConfigPath, logger);
            if (ap != null && !ap.isDisabled()) {
                startAdminInternal(ap, bp.isHostingAdmin());
            }
        }

        List<ParameterMap> repNodes =
            ConfigUtils.getRepNodes(kvConfigPath, logger);

        /**
         * Start RepNodes.
         */

        /*
         * Log exceptions from starting a single RepNode and continue
         * to start the next RepNode. The administrator will have to do
         * 'verify-configuration' to obtain details on the failures
         */
        for (ParameterMap map : repNodes) {
            RepNodeParams rn = new RepNodeParams(map);
            if (!rn.isDisabled()) {
            	try {
                    startRepNodeInternal(rn);
            	} catch (RuntimeException re) {
                     logsevere((rn.getRepNodeId().getFullName() +
                               ": Unable to start a RepNode on " +
                               rn.getStorageNodeId().getFullName()),
                              re);
               	}
            } else {
                logger.info(rn.getRepNodeId().getFullName() +
                            ": Skipping automatic start of stopped RepNode ");
            }
        }

        List<ParameterMap> arbNodes =
            ConfigUtils.getArbNodes(kvConfigPath, logger);
        for (ParameterMap map : arbNodes) {
            ArbNodeParams an = new ArbNodeParams(map);
            if (!an.isDisabled()) {
                startArbNodeInternal(an);
            } else {
                logger.info(an.getArbNodeId().getFullName() +
                            ": Skipping automatic start of stopped ArbNode ");
            }
        }


    }

    private boolean stopAdminService(boolean stopService,
                                     boolean force,
                                     String reason) {
        if (adminService == null) {
            return false;
        }
        boolean stopped = false;
        String serviceName = adminService.getService().getServiceName();
        try {

            logger.info(serviceName + ": Stopping AdminService");
            /**
             * Make sure the service won't automatically restart.
             */
            adminService.dontRestart();

            /**
             * Try clean shutdown first.  If that fails for any reason use a
             * bigger hammer to be sure the service is gone.  Give the admin
             * service a few seconds to come up if it's not already.  Stopping
             * the service while it's not yet up can be problematic.
             */
            if (stopService && !adminService.forceOK(force)) {
                CommandServiceAPI admin = ServiceUtils.waitForAdmin
                    (getHostname(), getRegistryPort(), getLoginManager(),
                     5, ServiceStatus.RUNNING, logger);
                admin.stop(force, reason);
                adminService.waitFor(serviceWaitMillis);
                stopped = true;
            }
        } catch (Exception e) {

            /**
             * Eat the exception but log the problem and make sure that the
             * service is really stopped.
             */
            logwarning("Exception stopping Admin service", e);
        }
        if (!stopped) {
            adminService.stop();
        }
        logger.info(serviceName + ": Stopped AdminService");
        unbindService(GlobalParams.COMMAND_SERVICE_NAME);
        unbindService(GlobalParams.ADMIN_LOGIN_SERVICE_NAME);
        mgmtAgent.removeAdmin();
        adminService = null;
        return true;
    }

    /**
     * Stops all running RN ServiceManagers and optionally rep node services in
     * parallel. It does this by creating an executor service and shutting down
     * each RN in a thread associated with the service.
     *
     * @param stopService if true stop the service as well as the manager.
     * @param force if true, stop the service by killing the process/thread.
     * Note that if force is true, the service is stopped even if stopServices
     * is false.
     * @param reason the reason for stopping the services, or null
     */
    private void stopRepNodeServices(boolean stopService,
                                     boolean force,
                                     String reason) {

        if (repNodeServices.isEmpty()) {
            return;
        }

        /*
         * Check if any of the RNs managed by the SNA is a master for its
         * replication group. If an RN is a master, the SNA should cause a
         * master-transfer before shutting down the RN.
         */
        masterBalanceManager.transferMastersForShutdown();

        final ExecutorService pool =
            Executors.newFixedThreadPool(repNodeServices.size(),
                                         new KVThreadFactory("RnShutDownThread",
                                                              logger));
        for (ServiceManager mgr : repNodeServices.values()) {
            pool.execute(new RNShutdownThread(this, mgr, serviceWaitMillis,
                                              stopService, force, reason));
        }

        pool.shutdown();

        /*
         * Overhead to provide the thread an opportunity to implement the
         * timeout and minimize the chance of a race.
         */
        final long overheadMs = 10000;
        try {
            pool.awaitTermination(serviceWaitMillis + overheadMs,
                                  TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warning("stopRepNodeservices: Unexpected interrupt" );
            return;
        }

        final List<Runnable> residual = pool.shutdownNow();
        for (Runnable t : residual) {
            /* Kill the processes, if the RN did not terminate gracefully. */
            ((RNShutdownThread)t).getMgr().stop();
        }

        repNodeServices.clear();
    }


    /**
     * Stops all running ARB ServiceManagers and optionally arb node services in
     * parallel. It does this by creating an executor service and shutting down
     * each AN in a thread associated with the service.
     *
     * @param stopService if true stop the service as well as the manager.
     * @param force if true, stop the service by killing the process/thread.
     * Note that if force is true, the service is stopped even if stopServices
     * is false.
     * @param reason the reason for stopping the services, or null
     */
    private void stopArbNodeServices(boolean stopService,
                                     boolean force,
                                     String reason) {

        if (arbNodeServices.isEmpty()) {
            return;
        }

        final ExecutorService pool =
            Executors.newFixedThreadPool(arbNodeServices.size(),
                                         new KVThreadFactory("AnShutDownThread",
                                                              logger));
        for (ServiceManager mgr : arbNodeServices.values()) {
            pool.execute(new RNShutdownThread(this, mgr, serviceWaitMillis,
                                              stopService, force, reason));
        }

        pool.shutdown();

        /*
         * Overhead to provide the thread an opportunity to implement the
         * timeout and minimize the chance of a race.
         */
        final long overheadMs = 10000;
        try {
            pool.awaitTermination(serviceWaitMillis + overheadMs,
                                  TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warning("stopArbNodeservices: Unexpected interrupt" );
            return;
        }

        final List<Runnable> residual = pool.shutdownNow();
        for (Runnable t : residual) {
            /* Kill the processes, if the AN did not terminate gracefully. */
            ((RNShutdownThread)t).getMgr().stop();
        }

        arbNodeServices.clear();
    }

    /**
     * Unbind a managed service from this SNA's registry.
     *
     * This is done to ensure that even on forcible stop the service is
     * cleaned out of the registry.  Errors are expected (already unbound)
     * and ignored.
     */
    void unbindService(String serviceName) {
        if (registry != null) {
            try {
                registry.unbind(serviceName);
            } catch (NotBoundException | RemoteException nbe) {
                /* ignore */
            }
        }
        try {
            AsyncRegistryUtils.unbind(getHostname(), getRegistryPort(),
                                      serviceName, null /* listenHandle */,
                                      logger);
        } catch (RemoteException e) {
            /* ignore */
        }
    }

    /**
     * Returns the executor to use to execute miscellaneous async operations.
     * This method is synchronized to make sure that multi-thread access doesn't
     * create multiple executors.
     */
    ThreadPoolExecutor getAsyncMiscExecutor() {
        synchronized (asyncMiscExecutorLock) {
            if (asyncMiscExecutor == null) {
                asyncMiscExecutor = new ThreadPoolExecutor(
                    0 /* corePoolSize */, Integer.MAX_VALUE /* maxPoolSize */,
                    ASYNC_EXECUTOR_KEEP_ALIVE_SECS, SECONDS,
                    new SynchronousQueue<>(),
                    new KVThreadFactory("StorageNodeAgent misc async executor",
                                        logger));
            }
        }
        return asyncMiscExecutor;
    }

    private void stopAsyncMiscExecutor() {
        synchronized (asyncMiscExecutorLock) {
            if (asyncMiscExecutor != null) {

                /*
                 * We don't really know for sure if there are some last async
                 * calls coming in, so just tell the executor to exit all idle
                 * threads quickly
                 */
                asyncMiscExecutor.setKeepAliveTime(1, MILLISECONDS);
                asyncMiscExecutor.allowCoreThreadTimeOut(true);
            }
        }
    }

    /**
     * Starts the master balance manager component
     *
     * @param enabled if true master balancing is enabled. If false it's
     * disabled; the SNA will not initiate any rebalancing at this node and
     * will decline to participate in rebalancing requests from other SNAs.
     */
    private void startMasterBalanceManager(boolean enabled) {
        /* Cleanup any existing MBM */
        stopMasterBalanceManager();

        /* Now start up the master balance manager. */
        final SNInfo snInfo =
                new MasterBalanceManager.SNInfo(getStoreName(), snid,
                                                getHostname(),
                                                getRegistryPort());

        masterBalanceManager =
            MasterBalanceManager.create(enabled, snInfo, logger,
                                        getLoginManager());
    }

    /**
     * Stops the master balance manager component
     */
    private void stopMasterBalanceManager() {
        if (masterBalanceManager == null) {
            return;
        }

        masterBalanceManager.shutdown();

        /*
         * Leave the iv non-null in the shutdown state, so it can provide
         * appropriate responses to MBM requests and so we don't need null
         * check guards.
         */
    }

    /**
     * Detect and kill running processes for this store.
     */
    private void cleanupRunningComponents() {

        /**
         * Kill RepNodes
         */
        List<ParameterMap> repNodes =
            ConfigUtils.getRepNodes(kvConfigPath, logger);
        for (ParameterMap map : repNodes) {
            RepNodeParams rn = new RepNodeParams(map);
            ManagedService.killManagedProcesses
                (getStoreName(), rn.getRepNodeId().getFullName(), getLogger());
        }

        /**
         * Admin may be named as the bootstrap admin or via the config
         * file. Try both.  Don't kill a currently-managed bootstrap admin, as
         * indicated by a non-null adminService.
         */
        if (adminService == null) {
            ManagedService.killManagedProcesses
                (getStoreName(), makeBootstrapAdminName(), getLogger());
        }
        AdminParams ap = ConfigUtils.getAdminParams(kvConfigPath, logger);
        if (ap != null) {
            ManagedService.killManagedProcesses
                (getStoreName(), ap.getAdminId().getFullName(), getLogger());
        }
    }

    /**
     * Make sure that the storage directory, if specified, exists and is a
     * directory.
     * TODO: should symlinks be created in the SNs directory to point to the
     * RepNode directories?
     */
    private File validateRepNodeDirectory(RepNodeParams rnp) {
        final File file = rnp.getStorageDirectoryFile();
        return validateDirectory(file,
                                 rnp.getRepNodeId(),
                                 rnp.getStorageDirectorySize());
    }

    private File validateArbNodeDirectory(ArbNodeParams arp) {
        final File mp =
            FileNames.getEnvDir(kvRoot.getAbsolutePath(),
                                getStoreName(),
                                null,
                                snid,
                                arp.getArbNodeId());
        if (mp.exists()) {
            validateDirectory(mp, arp.getArbNodeId(), 0L);
        }
        return mp;
    }

    private File validateDirectory(File file,
                                   ResourceId id,
                                   long requiredSize) {
        if (file == null) {
            return null;
        }
        final String reason = FileUtils.verifyDirectory(file, requiredSize);
        if (reason != null) {
            final String msg = "Directory specified for "+ id.getFullName() +
                               " " + reason;
            logger.info(msg);
            throw new IllegalArgumentException(msg);
        }
        return file;
    }

    /**
     * Internal method to start a RepNode, shared by external and internal
     * callers.
     *
     * @return true if the rep node was successfully started, or false if an
     * exception that is not a RuntimeException is raised
     * @throws RuntimeException if a problem occurred validating the rep node
     * directory
     */
    private boolean startRepNodeInternal(RepNodeParams rnp) {

        RepNodeId rnid = rnp.getRepNodeId();
        String serviceName = rnid.getFullName();
        logger.info(serviceName + ": Starting RepNode");
        File repNodeDir;

        try {
            repNodeDir = validateRepNodeDirectory(rnp);
        } catch (RuntimeException re) {
            /* log the exception and throw it so that it reaches the caller */
            logsevere(serviceName + ": Runtime Exception starting RepNode",
                      re);
            throw re;
        }

        try {
            /**
             * Create a ManagedService object used by the ServiceManager to
             * start the service.
             */
            ManagedRepNode ms = new ManagedRepNode
                (sp, rnp, kvRoot, snRoot, getStoreName());

            ServiceManager mgr = repNodeServices.get(serviceName);
            if (mgr != null) {
                /* The service may be running */
                boolean isRunning = mgr.isRunning();
                if (isRunning) {
                    logger.info(serviceName +
                                ": Attempt to start a running RepNode.");
                    return true;
                }
                logger.info(serviceName + " exists but is not runnable." +
                            "  Attempt to stop it and restart.");
                stopRepNode(rnid, true /* force */, false /* checkStream */,
                            "Stopping old service as part of restart");
                /*
                 * recurse back to startRepNode to make sure state gets set
                 * correctly.
                 */
                return startRepNode(rnid);
            }
            if (useThreads) {
                /* start in thread */
                mgr = new ThreadServiceManager(this, ms);
            } else {
                /* start in process */
                mgr = new ProcessServiceManager(this, ms);
            }
            checkForRecovery(rnid, repNodeDir);
            mgmtAgent.addRepNode(rnp, mgr);

            mgr.start();

            /**
             * Add service to map of running services.
             */
            repNodeServices.put(serviceName, mgr);
            logger.info(serviceName + ": Started RepNode");
        } catch (Exception e) {
            logsevere((serviceName + ": Exception starting RepNode"), e);
            return false;
        }
        return true;
    }

    /**
     * Internal method to start an ArbNode, shared by external and internal
     * callers.
     */
    private boolean startArbNodeInternal(ArbNodeParams anp) {

        ArbNodeId arbid = anp.getArbNodeId();
        String serviceName = arbid.getFullName();
        logger.info(serviceName + ": Starting ArbNode");
        try {

            /**
             * Create a ManagedService object used by the ServiceManager to
             * start the service.
             */
            ManagedArbNode ms = new ManagedArbNode
                (sp, anp, kvRoot, snRoot, getStoreName());
            File arbNodeDir = validateArbNodeDirectory(anp);

            ServiceManager mgr = arbNodeServices.get(serviceName);
            if (mgr != null) {
                /* The service may be running */
                boolean isRunning = mgr.isRunning();
                if (isRunning) {
                    logger.info(serviceName +
                                ": Attempt to start a running ArbNode.");
                    return true;
                }
                logger.info(serviceName + " exists but is not runnable." +
                            "  Attempt to stop it and restart.");
                stopArbNode(arbid, true);
                /*
                 * recurse back to startArbNode to make sure state gets set
                 * correctly.
                 */
                return startArbNode(arbid);
            }
            if (useThreads) {
                /* start in thread */
                mgr = new ThreadServiceManager(this, ms);
            } else {
                /* start in process */
                mgr = new ProcessServiceManager(this, ms);
            }
            checkForRecovery(arbid, arbNodeDir);
            mgmtAgent.addArbNode(anp, mgr);

            mgr.start();

            /**
             * Add service to map of running services.
             */
            arbNodeServices.put(serviceName, mgr);
            logger.info(serviceName + ": Started ArbNode");
        } catch (Exception e) {
            logsevere((serviceName + ": Exception starting ArbNode"), e);
            return false;
        }
        return true;
    }


    /**
     * Utility method for waiting until a RepNode reaches one of the given
     * states.  Primarily here for the exception handling.
     */
    public RepNodeAdminAPI waitForRepNodeAdmin(RepNodeId rnid,
                                               ServiceStatus[] targets) {
        return waitForRepNodeAdmin(rnid, targets, repnodeWaitSecs);
    }

    private RepNodeAdminAPI waitForRepNodeAdmin(RepNodeId rnid,
                                                ServiceStatus[] targets,
                                                int waitSecs) {
        final RepNodeAdminAPI rnai;
        try {
            rnai = ServiceUtils.waitForRepNodeAdmin
                (getStoreName(), getHostname(), getRegistryPort(), rnid, snid,
                 getLoginManager(), waitSecs, targets, logger);
        } catch (Exception e) {
            File logDir = FileNames.getLoggingDir(kvRoot, getStoreName());
            String logName =
                logDir + File.separator + rnid.toString() + "*.log";
            String msg = "Failed to attach to RepNodeService for " +
                rnid + " after waiting " + waitSecs +
                " seconds; see log, " + logName + ", on host " +
                getHostname() + " for more information.";
            logsevere(msg, e);

            /*
             * Check if the process didn't actually start up, and throw an
             * exception if so. That's different from a timeout exception, and
             * it would be better to propagate that information.
             */
            RegistryUtils.checkForStartupProblem(getStoreName(), getHostname(),
                                                 getRegistryPort(), rnid, snid,
                                                 getLoginManager(), logger);
            return null;
        }
        return rnai;
    }

    /**
     * Utility method for waiting until the Admin reaches the given state.
     */
    public CommandServiceAPI waitForAdmin(ServiceStatus target,
                                          int timeoutSecs) {

        CommandServiceAPI cs = null;
        try {
            cs = ServiceUtils.waitForAdmin
                (getHostname(), getRegistryPort(), getLoginManager(),
                 timeoutSecs, target, logger);
        } catch (Exception e) {

            String msg = "Failed to attach to AdminService for after waiting " +
                timeoutSecs + " seconds.";
            logger.severe(msg);
            throw new IllegalStateException(msg, e);
        }
        return cs;
    }

    /**
     * Remove the data directory for the resource.  This method does not deal
     * with storage directories and should not be called for any service that
     * is using one.
     */
    void removeDataDir(ResourceId rid) {
        File dataDir = FileNames.getServiceDir(kvRoot.toString(),
                                               getStoreName(),
                                               null,
                                               snid,
                                               rid);
        logger.info("Removing data directory for resource " +
                    rid + ": " + dataDir);
        if (dataDir.exists()) {
            removeFiles(dataDir);
        }
    }

    /**
     * Stop a running RepNode -- convenience function for testing
     */
    public boolean stopRepNode(RepNodeId rnid, boolean force) {
        return stopRepNode(rnid, force, false /* checkStream */,
                           "For testing");
    }

    /*
     * This function should never fail to stop a RepNode if the RepNode exists
     * and is running.  The last resort is to forcibly kill the RN, even if the
     * force boolean is false.
     */
    public boolean stopRepNode(RepNodeId rnid,
                               boolean force,
                               boolean checkStream,
                               String reason) {

        boolean stopped = false;
        String serviceName = rnid.getFullName();
        logger.info(serviceName + ": stopRepNode called");
        ServiceManager mgr = repNodeServices.get(serviceName);
        boolean isRunning = true;
        if (mgr != null) {
            isRunning = mgr.isRunning();
        }
        if (mgr == null || !isRunning) {
            logger.info(serviceName + ": RepNode is not running");
        }
        if (mgr == null) {
            return false;
        }

        /*
         * Set the service state in the config file to disabled.  Once this is
         * done the RN will not be automatically restarted.
         */
        setServiceStoppedState
            (serviceName, ParameterState.COMMON_DISABLED, true);
        try {

            /*
             * Make sure the service won't automatically restart.
             */
            mgr.dontRestart();

            /*
             * If force is true, skip directly to killing the service.
             */
            if (isRunning && !mgr.forceOK(force)) {
                ManagedRepNode mrn = (ManagedRepNode) mgr.getService();
                RepNodeAdminAPI rna = mrn.getRepNodeAdmin(this);

                assert TestHookExecute.doHookIfSet(rnShutdownHook, null);
                rna.shutdown(force, checkStream, reason);

                /*
                 * Wait for the execution context (process or thread).
                 */
                mgr.waitFor(serviceWaitMillis);
                stopped = true;
                logger.info(serviceName + ": Stopped RepNode");
            }
        } catch (RuntimeException | RemoteException e) {
            logwarning((serviceName + ": Exception stopping RepNode"), e);
        } finally {

            /*
             * Ask the ServiceManager to stop it if shutdown failed.
             */
            if (!stopped) {
                mgr.stop();
            }

            /*
             * Remove service and set active state in RNP to false.
             */
            unbindService(makeRepNodeBindingName(serviceName));
            repNodeServices.remove(serviceName);
            try {
                mgmtAgent.removeRepNode(rnid);
            } catch (RuntimeException ce) {
                logwarning
                    ((serviceName + ": Exception removing RepNode from mgmt" +
                      " agent"), ce);
            }
        }
        return isRunning;
    }

    String makeRepNodeBindingName(String fullName) {
        return RegistryUtils.bindingName(getStoreName(),
                                         fullName,
                                         RegistryUtils.InterfaceType.ADMIN);
    }

    /**
     * Stop a running ArbNode -- convenience function for testing
     */
    public boolean stopArbNode(ArbNodeId rnid, boolean force) {
        return stopArbNode(rnid, force, "For testing");
    }

    /*
     * This function should never fail to stop a ArbNode if the ArbNode exists
     * and is running.  The last resort is to forcibly kill the AN, even if the
     * force boolean is false.
     */
    boolean stopArbNode(ArbNodeId arbid, boolean force, String reason) {

        boolean stopped = false;
        String serviceName = arbid.getFullName();
        logger.info(serviceName + ": stopArbNode called");
        ServiceManager mgr = arbNodeServices.get(serviceName);
        boolean isRunning = true;
        if (mgr != null) {
            isRunning = mgr.isRunning();
        }
        if (mgr == null || !isRunning) {
            logger.info(serviceName + ": ArbNode is not running");
        }
        if (mgr == null) {
            return false;
        }

        /*
         * Set the service state in the config file to disabled.  Once this is
         * done the AN will not be automatically restarted.
         */
        setServiceStoppedState
            (serviceName, ParameterState.COMMON_DISABLED, true);
        try {

            /*
             * Make sure the service won't automatically restart.
             */
            mgr.dontRestart();

            /*
             * If force is true, skip directly to killing the service.
             */
            if (isRunning && !mgr.forceOK(force)) {
                ManagedArbNode man = (ManagedArbNode) mgr.getService();
                ArbNodeAdminAPI ana = man.getArbNodeAdmin(this);
                ana.shutdown(force, reason);

                /*
                 * Wait for the execution context (process or thread).
                 */
                mgr.waitFor(serviceWaitMillis);
                stopped = true;
                logger.info(serviceName + ": Stopped ArbNode");
            }
        } catch (RuntimeException | RemoteException e) {
            logwarning((serviceName + ": Exception stopping ArbNode"), e);
        } finally {

            /*
             * Ask the ServiceManager to stop it if shutdown failed.
             */
            if (!stopped) {
                mgr.stop();
            }

            /*
             * Remove service and set active state in ANP to false.
             */
            unbindService(makeRepNodeBindingName(serviceName));
            arbNodeServices.remove(serviceName);
            try {
                mgmtAgent.removeArbNode(arbid);
            } catch (RuntimeException ce) {
                logwarning
                    ((serviceName + ": Exception removing ArbNode from mgmt" +
                      " agent"), ce);
            }
        }
        return isRunning;
    }

    /**
     * Modify the configuration to mark all services as disabled, failing if it
     * can determine that the SNA or any of its services are currently running.
     * Makes no configuration changes if the SNA is not registered.
     *
     * @throws RuntimeException if fails to disable services
     */
    public void disableServices() {
        logger = LoggerUtils.getBootstrapLogger
            (bootstrapDir, FileNames.BOOTSTRAP_SNA_LOG, snaName);
        final File configPath = new File(bootstrapDir, bootstrapFile);
        bp = ConfigUtils.getBootstrapParams(configPath, logger);

        /* Check for correct security configuration */
        boolean securityOK = false;
        try {
            initSecurity();
            securityOK = true;
        } catch (RuntimeException e) {
            logger.info(e.getMessage());
        }

        /* Check for a running SNA if the security configuration was correct */
        if (securityOK) {
            sp.initRMISocketPolicies(logger);
            BootstrapParams.initRegistryCSF(sp);
            snaSecurity = new SNASecurity(
                this, bp, sp, null /* gp */, null /* snp */, logger);
            final String serviceName;
            if (isRegistered()) {
                final StorageNodeId snid1 = new StorageNodeId(bp.getId());
                serviceName = RegistryUtils.bindingName(
                    bp.getStoreName(), snid1.getFullName(),
                    RegistryUtils.InterfaceType.MAIN);
            } else {
                serviceName = GlobalParams.SNA_SERVICE_NAME;
            }
            try {
                RegistryUtils.getStorageNodeAgent(
                    bp.getHostname(), bp.getRegistryPort(), serviceName,
                    getLoginManager(), logger);
                throw new IllegalStateException(
                    "Attempt to disable services when the storage node" +
                    " agent is running");
            } catch (RemoteException | NotBoundException ignore) {
            }
        }
        logger.info("SNA not found");

        /* There are no services to disable if the SNA is unregistered */
        if (!isRegistered()) {
            return;
        }

        /*
         * Check for services that might be running even though the SNA was not
         * found
         */
        final String rootPath = CommandParser.ROOT_FLAG + " " + bootstrapDir;
        final String configFile = CONFIG_FLAG + " " + bootstrapFile;
        final List<Integer> snaProcesses = ManagedService.findManagedProcesses(
            "StorageNodeAgentImpl", rootPath, configFile, logger);
        final List<Integer> managedProcesses =
            ManagedService.findManagedProcesses(
                getStoreName(), rootPath, configFile, logger);
        if (!snaProcesses.isEmpty() || !managedProcesses.isEmpty()) {
            throw new IllegalStateException(
                "Attempt to disable services when the storage node agent or" +
                " some services are running");
        }
        logger.info("Services not found");

        /* Initialize more fields in preparation for disabling services */
        kvRoot = new File(bp.getRootdir());
        initStorePaths();
        final StorageNodeParams snp =
            ConfigUtils.getStorageNodeParams(kvConfigPath, logger);
        final GlobalParams gp =
            ConfigUtils.getGlobalParams(kvConfigPath, logger);
        logger = LoggerUtils.getLogger(StorageNodeAgentImpl.class, gp, snp);

        /* Disable RNs */
        for (final ParameterMap map :
                 ConfigUtils.getRepNodes(kvConfigPath, logger)) {
            final RepNodeParams rn = new RepNodeParams(map);
            if (!rn.isDisabled()) {
                rn.setDisabled(true);
                replaceRepNodeParams(rn);
            }
        }

        /* Disable admin */
        final AdminParams ap =
            ConfigUtils.getAdminParams(kvConfigPath, logger);
        if ((ap != null) && !ap.isDisabled()) {
            ap.setDisabled(true);
            replaceAdminParams(ap.getAdminId(), ap.getMap());
        }

        logger.info("Disabled all services");
    }

    private void startBootstrapAdmin() {
        if (bp.isHostingAdmin()) {
            createBootstrapAdmin = true;
        }
        if (createBootstrapAdmin) {
            startAdminInternal(null, true);
        } else {
            logger.info
                ("isHostingAdmin is false; not starting Bootstrap Admin");
        }
    }

    /**
     * Used by SNAImpl.  It will never pass a null AdminParams.
     */
    public boolean startAdmin(AdminParams ap) {

        /**
         * Set active state in AP.  This must be done before the service is
         * started to avoid conflict on the file.
         */
        setServiceStoppedState
            (ap.getAdminId().getFullName(),
             ParameterState.COMMON_DISABLED, false);
        return startAdminInternal(ap, getBootstrapParams().isHostingAdmin());
    }

    /**
     * Internal method to start an AdminService, shared by external and
     * internal callers.
     */
    private boolean startAdminInternal(AdminParams ap, boolean isBootstrap) {

        if (ap == null && !isBootstrap) {
            throw new IllegalStateException
                ("Params for admin do not exist, should have been created.");
        }

        String serviceName = (ap != null ?
                              ap.getAdminId().getFullName() :
                              ManagedService.BOOTSTRAP_ADMIN_NAME);

        try {

            /**
             * Create a ManagedService object used by the ServiceManager to
             * start the service.
             */
            if (adminService != null) {
                /* The service may be running */
                boolean isRunning = adminService.isRunning();
                if (isRunning) {
                    logger.info(serviceName +
                                ": Attempt to start a running AdminService");
                    return true;
                }
                logger.info(serviceName + " exists but is not runnable." +
                            "  Attempt to stop it and restart.");
                stopAdminService(true /* stopService */, true /* force */,
                                 "Stopping old service as part of restart");
                /* fall through to start */
            }
            ManagedAdmin ms;
            logger.info(serviceName + ": Starting AdminService");
            if (ap != null) {
                ms = new ManagedAdmin
                    (sp, ap, kvRoot, snRoot, getStoreName());
            } else {
                ms = new ManagedBootstrapAdmin(this);
            }
            final ServiceManager mgr;
            if (useThreads) {
                /* start in thread */
                mgr = new ThreadServiceManager(this, ms);
            } else {
                /* start in process */
                mgr = new ProcessServiceManager(this, ms);
            }
            if (ap != null) {
                if (adminDirectoryString == null ||
                    adminDirectoryString.isEmpty()) {
                    checkForRecovery(ap.getAdminId(), null);
                } else if (adminDirectoryString.split(",").length > 1) {
                    throw new IllegalStateException
                        ("Cannot set more than one admin directory");
                } else {
                    checkForRecovery(ap.getAdminId(),
                                     new File(adminDirectoryString));
                }
            }

            mgmtAgent.addAdmin(ap, mgr);

            mgr.start();
            adminService = mgr;

            logger.info(serviceName + ": Started AdminService");
        } catch (Exception e) {
            logsevere("Exception starting AdminService: ", e);
            return false;
        }
        return true;
    }

    synchronized boolean isRegistered() {
        return getStoreName() != null;
    }

    /**
     * Make a service name for the BootstrapAdmin.  It must include the
     * registry port to uniquely identify the bootstrap admin process in the
     * event there is more than one SN on the host.
     */
    public String makeBootstrapAdminName() {
        return ManagedService.BOOTSTRAP_ADMIN_NAME + "." + getRegistryPort();
    }

    /**
     * Setup all that is needed for logging and monitoring, for registered
     * SNAs.
     */
    private void setupMonitoring(GlobalParams globalParams,
                                 StorageNodeParams snp) {

        if (monitorAgent != null) {
            return;
        }

        /*
         * The AgentRepository is the buffered monitor data and belongs
         * to the monitorAgent, but is instantiated outside to take care of
         * initialization dependencies.
         */
        AgentRepository monitorBuffer =
            new AgentRepository(globalParams.getKVStoreName(),
                                snp.getStorageNodeId(),
                                snp.getMonitorAgentRepoSize());
        snParameterTracker.addListener(monitorBuffer.new SNParamsListener());
        statusTracker.addListener(monitorBuffer);

        loggingStatsTracker.initialize(globalParams.getMap());
        globalParameterTracker.addListener(
            loggingStatsTracker.getGlobalParamsListener());

        monitorAgent = new MonitorAgentImpl(this,
                                            globalParams,
                                            snp,
                                            sp,
                                            monitorBuffer,
                                            statusTracker);
    }

    /**
     * Send Storage Node and bootstrap information back as a reply to the
     * register() call
     */
    public static class RegisterReturnInfo {
        final private List<ParameterMap> maps;
        private ParameterMap bootMap;
        private ParameterMap storageDirMap;
        private ParameterMap adminDirMap;
        private ParameterMap rnLogDirMap;

        public RegisterReturnInfo(StorageNodeAgent sna) {
            final BootstrapParams bp = sna.getBootstrapParams();
            maps = new ArrayList<>();
            bootMap = bp.getMap().copy();
            bootMap.setParameter(ParameterState.GP_ISLOOPBACK,
                                 Boolean.toString(sna.isLoopbackAddress()));
            bootMap.setParameter(ParameterState.SN_SOFTWARE_VERSION,
                                 CURRENT_VERSION.getNumericVersionString());
            storageDirMap = bp.getStorageDirMap().copy();
            adminDirMap = bp.getAdminDirMap().copy();
            rnLogDirMap = bp.getRNLogDirMap().copy();
            maps.add(bootMap);
            maps.add(storageDirMap);
            maps.add(adminDirMap);
            maps.add(rnLogDirMap);
        }

        public RegisterReturnInfo(List<ParameterMap> maps) {
            this.maps = maps;
            bootMap = null;
            storageDirMap = null;
            adminDirMap = null;
            rnLogDirMap = null;
            for (ParameterMap pmap : maps) {
                if (pmap.getName().equals(ParameterState.BOOTSTRAP_PARAMS)) {
                    bootMap = pmap;
                }
                if (pmap.getName().equals
                    (ParameterState.BOOTSTRAP_MOUNT_POINTS)) {
                    storageDirMap = pmap;
                }
                if (pmap.getName().equals
                    (ParameterState.BOOTSTRAP_ADMIN_MOUNT_POINTS)) {
                    adminDirMap = pmap;
                }
                if (pmap.getName().equals
                    (ParameterState.BOOTSTRAP_RNLOG_MOUNT_POINTS)) {
                    rnLogDirMap = pmap;
                }
            }
        }

        public List<ParameterMap> getMaps() {
            return maps;
        }

        public ParameterMap getBootMap() {
            return bootMap;
        }

        public ParameterMap getStorageDirMap() {
            return storageDirMap;
        }

        public ParameterMap getAdminDirMap() {
            return adminDirMap;
        }

        public ParameterMap getRNLogDirMap() {
            return rnLogDirMap;
        }

        public boolean getIsLoopback() {
            if (bootMap == null) {
                throw new IllegalStateException
                    ("BootMap cannot be null when asking for loopback info");
            }
            Parameter p = bootMap.get(ParameterState.GP_ISLOOPBACK);
            if (p == null) {
                throw new IllegalStateException
                    ("GP_ISLOOPBACK parameter is not set in boot map");
            }
            return p.asBoolean();
        }
    }

    /**
     * Implementation methods for StorageNodeAgentInterface.  These are
     * called by the public interface methods that wrap the calls with a
     * ProcessFaultHandler object for consistent exception handling.
     */
    List<ParameterMap> register(GlobalParams gp,
                                StorageNodeParams snp,
                                boolean hostingAdmin) {

        logger.info("Register: root: " + kvRoot + ", store: " +
                    gp.getKVStoreName() + ", hostingAdmin: " + hostingAdmin);

        /**
         * Allow retries to succeed.
         */
        if (isRegistered()) {
            logger.info("Register: Storage Node Agent is already registered " +
                        "to " + getStoreName());
            return new RegisterReturnInfo(this).getMaps();
        }

        if (gp.isLoopbackSet()) {
            if (gp.isLoopback() != isLoopbackAddress()) {
                String msg="Register: Cannot mix loopback and non-loopback " +
                    "addresses in the same store.  The store value " +
                    (gp.isLoopback() ? "is" : "is not") +
                     " configured to use loopback addresses but storage node " +
                    snp.getHostname() + ":" + snp.getRegistryPort() + " " +
                    (isLoopbackAddress() ? "is" : "is not") +
                    " a loopback address.";

                logger.info(msg);
                throw new IllegalStateException(msg);
            }
        } else {
            gp.setIsLoopback(isLoopbackAddress());
        }

        /*
         * Make sure that system information, such as number of CPUs, is set if
         * it is available.  It is returned in RegisterReturnInfo and set in
         * snp.setInstallationInfo().
         */
        setSystemInfo();

        /**
         * Create the ultimate return object and initialize fields in the
         * StorageNodeParams that the SNA owns.
         */
        RegisterReturnInfo rri = new RegisterReturnInfo(this);

        snp.setInstallationInfo
            (rri.getBootMap(), rri.getStorageDirMap(), rri.getAdminDirMap(),
             rri.getRNLogDirMap(), hostingAdmin, logger);

        /**
         * Initialize state from parameters.
         */
        initSNParams(snp);

        try {

            /**
             * There is a test-only race where the bootstrap admin may still be
             * coming up so make sure it is running before continuing.
             */
            if (adminService != null) {
                ServiceUtils.waitForAdmin(getHostname(), getRegistryPort(),
                                          getLoginManager(), 40,
                                          ServiceStatus.RUNNING, logger);
            }

            /**
             * If not hosting the admin, shut down the bootstrap admin.
             */
            if (!hostingAdmin) {
                stopAdminService(true /* stopService */, true /* force */,
                                 "Replace bootstrap service with registered" +
                                 " service");
            }

            /**
             * Rewrite the bootstrap config file with the KV Store name and Id
             * for this SN.  Do this after testing kvConfigPath above to make
             * reverting to bootstrap state automatic.
             */
            File configPath = new File(bootstrapDir, bootstrapFile);
            bp.setStoreName(gp.getKVStoreName());
            bp.setStoreVersion(gp.getStoreVersion());
            bp.setId(snp.getStorageNodeId().getStorageNodeId());
            bp.setHostingAdmin(hostingAdmin);
            ConfigUtils.createBootstrapConfig(bp, configPath, logger);

            /**
             * Make sure the kvstore directory has been created.  This call
             * uses state set in the BootstrapParams above.
             */
            ensureStoreDirectory();

            /**
             * If there is a configuration file already this is bad.  It means
             * the SNA was not registered but there is state that appears
             * otherwise.  kvConfigPath is set in ensureStoreDirectory().
             */
            if (kvConfigPath.exists()) {
                String msg = "Configuration file was not expected in store " +
                    "directory: " + kvConfigPath;
                throw new IllegalStateException(msg);
            }

            /**
             * Change Logger instances now that this class has an identity.
             */
            String newName = snp.getStorageNodeId().getFullName();
            logger = LoggerUtils.getLogger(StorageNodeAgentImpl.class, gp, snp);
            logger.log(Level.FINE,
                       "Storage Node named {0} Registering to store {1}",
                       new Object[]{newName, getStoreName()});
            statusTracker.setLogger(logger);
            snai.getFaultHandler().setLogger(logger);
            if (adminService != null) {
                adminService.resetLogger(logger);
            }

            /**
             * Create a new KVstore config file for the store.
             */
            LoadParameters lp = new LoadParameters();
            lp.addMap(snp.getMap());
            if (snp.getStorageDirMap() != null) {
                lp.addMap(snp.getStorageDirMap());
            }
            if (snp.getAdminDirMap() != null) {
                lp.addMap(snp.getAdminDirMap());
            }
            if (snp.getRNLogDirMap() != null) {
                lp.addMap(snp.getRNLogDirMap());
            }
            lp.addMap(gp.getMap());
            lp.saveParameters(kvConfigPath, logger);

            /**
             * Restart as if coming up for the first time but registered this
             * time.
             */
            start();
        } catch (Exception e) {

            /**
             * Any exceptions from register() will revert the SNA to bootstrap
             * mode.
             */
            String msg = "Register failed: " + exceptionString(e);
            revertToBootstrap();
            if (adminService == null) {
                startBootstrapAdmin();
            }
            logger.log(Level.SEVERE, msg, e);
            throw new IllegalStateException(msg, e);
        }
        return rri.getMaps();
    }

    /** Shut down service -- for testing. */
    public void shutdown(boolean stopServices, boolean force) {
        shutdown(stopServices, force, "For testing");
    }

    public void shutdown(boolean stopServices, boolean force, String reason) {
        statusTracker.update(ServiceStatus.STOPPING, reason);

        /**
         * Stop running services.  Minimally cause the SNA to not attempt to
         * restart.
         */
        stopRepNodeServices(stopServices, force, reason);
        stopMasterBalanceManager();

        stopArbNodeServices(stopServices, force, reason);

        assert TestHookExecute.doHookIfSet(mbmPostShutdownHook, this);

        stopAdminService(stopServices, force, reason);

        cleanupRegistry();

        stopAsyncMiscExecutor();

        snaSecurity.stop();

        loggingStatsTracker.close();
        logger.info(snaName + ": Shutdown complete");
        statusTracker.update(ServiceStatus.STOPPED, reason);
        if (collectorService != null) {
            collectorService.shutdown();
        }
        mgmtAgent.shutdown();
    }

    boolean createAdmin(AdminParams adminParams) {

        checkRegistered("createAdmin");
        final AdminId adminId = adminParams.getAdminId();
        final String adminName = adminId.getFullName();
        logger.info(adminName + ": Creating AdminService");


        /**
         * Creation of RepNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {

            /**
             * Add the new Admin to configuration.
             */
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);

            /**
             * Does the Admin already exist?
             */
            if (lp.getMap(adminName) != null) {
                String msg = adminName +
                    ": AdminService exists in config file";
                logger.info(msg);
            } else {
                lp.addMap(adminParams.getMap());
                lp.saveParameters(kvConfigPath, logger);
                LoggerUtils.logServiceMonitorCreateEvent(logger, adminId);
            }

            assert TestHookExecute.doHookIfSet(restartAdminHook, this);

            /**
             * If this SN is hosting the admin, don't start it; it is already
             * running.  It is possible that "isHostingAdmin()" will be true
             * and there is no bootstrap admin running.  This may be a
             * test-only case, but handle it anyway.
             */
            if (bp.isHostingAdmin() && adminService != null) {
                ManagedAdmin ma = (ManagedAdmin) adminService.getService();
                if (ma instanceof ManagedBootstrapAdmin) {
                    ((ManagedBootstrapAdmin) ma).resetAsManagedAdmin(
                        adminParams, kvRoot, securityDir, snRoot,
                        getStoreName(), logger);
                }

                /*
                 * Give the admin a chance to read potentially modified
                 * parameters
                 */
                try {
                    ma.getAdmin(this).newParameters();
                    ma.getAdmin(this).newGlobalParameters();
                } catch (RemoteException e) {
                    String msg = adminName +
                        ": Failed to contact bootstrap AdminService";
                    logger.info(msg);
                    throw new IllegalStateException(msg, e);
                }
                logger.info(adminName + ": Created AdminService");
                return true;
            }
            return startAdminInternal(adminParams, false);
        }
    }

    /** Shut down service -- for testing. */
    public boolean stopAdmin(AdminId adminId, boolean force) {
        return stopAdmin(adminId, force, "For testing");
    }

    /**
     * TODO: eventually use adminId if multiple admins are supported.
     */
    public boolean stopAdmin(@SuppressWarnings("unused") AdminId adminId,
                             boolean force,
                             String reason) {

        if (adminService == null) {
            String msg = "Stopping AdminService: service is not running";

            /**
             * Throw an exception if the service has not been created.
             */
            if (ConfigUtils.getAdminParams(kvConfigPath, logger) == null) {
                msg += "; service does not exist";
                throw new IllegalStateException(msg);
            }
            logger.warning(msg);
            return false;
        }
        String adminName = adminService.getService().getServiceName();
        boolean retval = stopAdminService(true, force, reason);

        /**
         * Set disabled to true in config file
         */
        setServiceStoppedState
            (adminName, ParameterState.COMMON_DISABLED, true);
        return retval;
    }

    /**
     * Idempotent -- if the service does not exist, it is fine.
     * NOTE: for now the adminId is ignored as the SNA can only support a
     * single admin instance.  In order to fully support multiple admins
     * the name of the ParameterMap stored in the SNA's config file must
     * change to use the admin ID vs "adminParams" in order to uniquely
     * identify the correct instance to remove.  This is an upgrade issue.
     */
    boolean destroyAdmin(AdminId adminId, boolean deleteData, String reason) {

        boolean retval = false;

        if (adminService == null) {
            logger.warning("Destroying AdminService: service is not running");
        } else {
            String serviceName = adminService.getService().getServiceName();
            logger.info(serviceName + ": Destroying AdminService");
            retval = stopAdminService(true /* stopService */,
                                      true /* force */,
                                      reason);
        }

        /**
         * Set bootstrap configuration to indicate that the Admin is no longer
         * hosted, if it was previously.
         */
        if (bp.isHostingAdmin()) {
            bp.setHostingAdmin(false);
            File configPath = new File(bootstrapDir, bootstrapFile);
            ConfigUtils.createBootstrapConfig(bp, configPath, logger);
        }

        /**
         * Remove admin from the config file.  This happens even if the
         * stopAdminService() call above failed.
         */
        removeConfigurable(adminId, ParameterState.ADMIN_TYPE, deleteData);

        return retval;
    }

    /**
     * Remove a RepNode or Admin from the config file.
     */
    boolean removeConfigurable(ResourceId rid, String type,
                               boolean deleteData) {
        ParameterMap map =
            ConfigUtils.removeComponent(kvConfigPath, rid, type, logger);
        LoggerUtils.logServiceMonitorDestroyEvent(logger, rid);
        if (deleteData && map != null) {
            /**
             * Determine data dir.  If this is not a RepNode it will not
             * have a storage directory and the data dir will be in the default
             * location.
             */
            if (map.getType().equals(ParameterState.REPNODE_TYPE)) {
                RepNodeParams rnp = new RepNodeParams(map);
                File file = rnp.getStorageDirectoryFile();
                if (file != null) {
                    File rnDir = new File(file, rid.getFullName());
                    if (rnDir.exists()) {
                        logger.info("Removing data directory for RepNode " +
                                    rid + ": " + rnDir);
                        removeFiles(rnDir);
                    }
                    return true;
                }
            }

            /**
             * If here, remove the data dir in the default location.  It may be
             * either a RepNode or Admin.
             */
            removeDataDir(rid);
        }
        return (map != null);
    }

    StringBuilder getStartupBuffer(ResourceId rid) {
        if (rid instanceof RepNodeId) {
            ServiceManager mgr = repNodeServices.get(rid.getFullName());
            if (mgr != null) {
                return mgr.getService().getStartupBuffer();
            }
        } else {
            ManagedAdmin ma = (ManagedAdmin) adminService.getService();
            if (rid.equals(ma.getResourceId())) {
                return ma.getStartupBuffer();
            }
        }
        throw new IllegalStateException
            ("Resource " + rid + " is not running on this storage node");
    }

    void updateNodeStatus(ResourceId serviceId,
                          ServiceStatusChange newStatus) {
        if (mgmtAgent != null) {
            mgmtAgent.updateNodeStatus(serviceId, newStatus);
        }
    }

    void updateAdminStatus(ServiceStatusChange newStatus, boolean isMaster) {
        if (mgmtAgent != null) {
            mgmtAgent.updateAdminStatus(newStatus, isMaster);
        }
    }

    void receiveStats(ResourceId serviceId, StatsPacket packet) {
        if (mgmtAgent != null) {
            mgmtAgent.receiveStats(serviceId, packet);
        }
    }

    void receiveAdminStats(StatsPacket packet) {
        if (mgmtAgent != null) {
            mgmtAgent.receiveAdminStats(packet);
        }
    }

    void receiveNewParams(ResourceId serviceId, ParameterMap newMap) {
        if (mgmtAgent != null) {
            mgmtAgent.receiveNewParams(serviceId, newMap);
        }
    }

    void updatePlanStatus(String planStatus) {
        if (mgmtAgent != null) {
            mgmtAgent.updatePlanStatus(planStatus);
        }
    }

    void updateReplicationState(RepNodeId rnId, StateChangeEvent changeEvent) {
        if (mgmtAgent != null) {
            mgmtAgent.updateReplicationState(rnId, changeEvent);
        }
    }

    /**
     * Utility method used only by createRepNode to configure a newly-created
     * RepNode.   This method will never fail.  If the call to configure fails
     * it is logged (as SEVERE) but the RepNode will still exist as far as the
     * SNA is concerned.  If the RepNode does not manage to acquire a topology
     * that is a problem for the administrator.
     */
    private void
        configureRepNode(Set<Metadata<? extends MetadataInfo>> metadataSet,
                         RepNodeId rnid) {

        /**
         * When creating the RepNode it needs to be configured using the
         * Topology.  Don't wait all that long.  If the service is going
         * to start up and it's taking a while it will eventually get the
         * configuration information from other sources.
         */

        ServiceStatus[] targets =
            {ServiceStatus.WAITING_FOR_DEPLOY, ServiceStatus.RUNNING};

        /* This will log a failure to get the interface */
        RepNodeAdminAPI rnai = waitForRepNodeAdmin(rnid, targets,
                                                   repnodeWaitSecs);
        if (rnai != null) {
            try {
                rnai.configure(metadataSet);
            } catch (RemoteException re) {
                logsevere((rnid + ": Failed to configure RepNodeService"), re);
            }
        }
    }

    private void setServiceStoppedState(String serviceName,
                                        String paramName,
                                        boolean state) {

        LoadParameters lp =
            LoadParameters.getParameters(kvConfigPath, logger);
        ParameterMap map = lp.getMap(serviceName);
        if (map != null) {
            map.setParameter(paramName, Boolean.toString(state));
            lp.saveParameters(kvConfigPath, logger);
        }
    }

    RepNodeParams lookupRepNode(RepNodeId rnid) {

        return ConfigUtils.getRepNodeParams(kvConfigPath, rnid, logger);
    }

    boolean createRepNode(RepNodeParams repNodeParams,
                          Set<Metadata<? extends MetadataInfo>> metadataSet) {

        /**
         * Creation of RepNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            RepNodeId rnid = repNodeParams.getRepNodeId();
            String serviceName = rnid.getFullName();
            logger.info(serviceName + ": Creating RepNode");

            /**
             * Add the new RepNode to configuration.
             */
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);

            /**
             * The RepNode may be in the config file but not configured.  This
             * would happen if the SNA exited after writing the config file but
             * before the RepNode configure() method succeeded.
             *
             * So, allow the RepNode to exist and be running and if it's not
             * yet deployed, deploy (configure) it.  If it is deployed then
             * this call was in error so return false.
             */
            boolean startService = true;
            RepNodeParams rnp = null;
            ParameterMap map = lp.getMap(serviceName);
            if (map != null) {
                ServiceManager mgr = repNodeServices.get(serviceName);
                rnp = new RepNodeParams(map);
                if (mgr != null) {
                    /**
                       The service may have been created.  Don't start it.
                    */
                    String msg = serviceName +
                        ": RepNode exists, not starting process";
                    logger.info(msg);
                    startService = false;
                } else {
                    String msg = serviceName +
                        ": RepNode exists but is not running, will attempt " +
                        "to start it";
                    logger.info(msg);
                }
            } else {
                lp.addMap(repNodeParams.getMap());
                lp.saveParameters(kvConfigPath, logger);
                LoggerUtils.logServiceMonitorCreateEvent(logger, rnid);
                rnp = ConfigUtils.getRepNodeParams(kvConfigPath, rnid, logger);
            }

            /*
             * Test: exit here between creation and startup.
             */
            assert TestHookExecute.doHookIfSet(restartRNHook, this);
            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 0);

            /*
             * Start the service.
             */
            if (startService) {
                if (!startRepNodeInternal(rnp)) {
                    return false;
                }
            }

            /*
             * Test exit after start, before configure.
             */
            assert TestHookExecute.doHookIfSet(stopRNHook, this);
            logger.info(serviceName + ": Configuring RepNode");
            configureRepNode(metadataSet, rnid);
            logger.info(serviceName + ": Created RepNode");
            return true;
        }
    }

    /**
     * Start an already created RepNode.  Public to support testing.
     */
    public boolean startRepNode(RepNodeId repNodeId) {

        String serviceName = repNodeId.getFullName();
        RepNodeParams rnp =
            ConfigUtils.getRepNodeParams(kvConfigPath, repNodeId, logger);
        if (rnp == null) {
            String msg = serviceName + ": RepNode has not been created";
            logger.info(msg);

            /* This rep node should have been created by this point. */
            throw new IllegalStateException(msg);
        }

        /**
         * Set active state in RNP.
         */
        setServiceStoppedState
            (serviceName, ParameterState.COMMON_DISABLED, false);
        return startRepNodeInternal(rnp);
    }

    /**
     * Utility method for waiting until a ArbNode reaches one of the given
     * states.  Primarily here for the exception handling.
     */
    public ArbNodeAdminAPI waitForArbNodeAdmin(ArbNodeId anid,
                                               ServiceStatus[] targets) {
        return waitForArbNodeAdmin(anid, targets, repnodeWaitSecs);
    }

    private ArbNodeAdminAPI waitForArbNodeAdmin(ArbNodeId anid,
                                                ServiceStatus[] targets,
                                                int waitSecs) {
        ArbNodeAdminAPI anai = null;
        try {
            anai = ServiceUtils.waitForArbNodeAdmin(
                getStoreName(), getHostname(), getRegistryPort(), anid, snid,
                getLoginManager(), waitSecs, targets, logger);
        } catch (Exception e) {
            File logDir = FileNames.getLoggingDir(kvRoot, getStoreName());
            String logName =
                logDir + File.separator + anid.toString() + "*.log";
            String msg = "Failed to attach to ArbNodeService for " +
                anid + " after waiting " + waitSecs +
                " seconds; see log, " + logName + ", on host " +
                getHostname() + " for more information.";
            logsevere(msg, e);

            /*
             * Check if the process didn't actually start up, and throw an
             * exception if so. That's different from a timeout exception, and
             * it would be better to propagate that information.
             */
            RegistryUtils.checkForStartupProblem(getStoreName(), getHostname(),
                                                 getRegistryPort(), anid, snid,
                                                 getLoginManager(), logger);
            return null;
        }
        return anai;
    }

    ArbNodeParams lookupArbNode(ArbNodeId arid) {

        return ConfigUtils.getArbNodeParams(kvConfigPath, arid, logger);
    }

    boolean createArbNode(ArbNodeParams arbNodeParams) {

        /**
         * Creation of ArbNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            ArbNodeId anid = arbNodeParams.getArbNodeId();
            String serviceName = anid.getFullName();
            logger.info(serviceName + ": Creating ArbNode");

            /**
             * Add the new ArbNode to configuration.
             */
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);

            /**
             * The ArbNode may be in the config file but not configured.  This
             * would happen if the SNA exited after writing the config file but
             * before the ArbNode configure() method succeeded.
             *
             * So, allow the ArbNode to exist and be running and if it's not
             * yet deployed, deploy (configure) it.  If it is deployed then
             * this call was in error so return false.
             */
            boolean startService = true;
            ArbNodeParams anp = null;
            ParameterMap map = lp.getMap(serviceName);
            if (map != null) {
                ServiceManager mgr = arbNodeServices.get(serviceName);
                anp = new ArbNodeParams(map);
                if (mgr != null) {
                    /**
                       The service may have been created.  Don't start it.
                    */
                    String msg = serviceName +
                        ": ArbNode exists, not starting process";
                    logger.info(msg);
                    startService = false;
                } else {
                    String msg = serviceName +
                        ": ArbNode exists but is not running, will attempt " +
                        "to start it";
                    logger.info(msg);
                }
            } else {
                lp.addMap(arbNodeParams.getMap());
                lp.saveParameters(kvConfigPath, logger);
                LoggerUtils.logServiceMonitorCreateEvent(logger, anid);
                anp = ConfigUtils.getArbNodeParams(kvConfigPath, anid, logger);
            }

            /*
             * Test: exit here between creation and startup.
             */
            assert TestHookExecute.doHookIfSet(restartRNHook, this);
            assert TestHookExecute.doHookIfSet(FAULT_HOOK, 0);

            /*
             * Start the service.
             */
            if (startService) {
                if (!startArbNodeInternal(anp)) {
                    return false;
                }
            }

            /*
             * Test exit after start, before waiting
             */
            assert TestHookExecute.doHookIfSet(stopRNHook, this);

            ServiceStatus[] targets =
                {ServiceStatus.WAITING_FOR_DEPLOY, ServiceStatus.RUNNING};

            waitForArbNodeAdmin(anid, targets, repnodeWaitSecs);
            logger.info(serviceName + ": Created ArbNode");
            return true;
        }
    }

    /**
     * Start an already created ArbNode.
     */
    public boolean startArbNode(ArbNodeId arbNodeId) {

        String serviceName = arbNodeId.getFullName();
        ArbNodeParams arp =
            ConfigUtils.getArbNodeParams(kvConfigPath, arbNodeId, logger);
        if (arp == null) {
            String msg = serviceName + ": ArbNode has not been created";
            logger.info(msg);

            /* This arb node should have been created by this point. */
            throw new IllegalStateException(msg);
        }

        /**
         * Set active state in arp.
         */
        setServiceStoppedState
            (serviceName, ParameterState.COMMON_DISABLED, false);
        return startArbNodeInternal(arp);
    }

    /**
     * Test-oriented interface to wait for the RepNode to exit.
     */
    protected boolean waitForRepNodeExit(RepNodeId rnid, int timeoutSecs) {
        String serviceName = rnid.getFullName();
        ServiceManager mgr = repNodeServices.get(serviceName);
        if (mgr != null) {
            mgr.waitFor(timeoutSecs * 1000);
            return true;
        }
        return false;
    }

    /**
     * Checks the specified parameters for correctness. Throws an
     * IllegalArgumentException if a parameter is found to be invalid. If
     * id is non-null then the parameters are for that service. Otherwise
     * the global parameters are checked.
     *
     * @param params parameter map to check
     * @param id the service associated with the parameters or null
     *
     * @throws IllegalArgumentException if an invalid parameter is found
     */
    void checkParams(ParameterMap params, ResourceId id) {
        if (id == null) {
            checkGlobalParams(params,
                              ConfigUtils.getStorageNodeParams(
                                  kvConfigPath).getMap());
        } else if (id instanceof StorageNodeId) {
            checkSNParams(params,
                          ConfigUtils.getGlobalParams(kvConfigPath).getMap());
        }
    }

    /**
     * Checks Global components parameters. Throws an IllegalArgumentException
     * if a parameter is found to be invalid.
     */
    public static void checkGlobalParams(ParameterMap params,
                                         ParameterMap snParams) {
        final List<String> errors = new ArrayList<>();

        /*
         * Loop through each parameter. If a problem is found set reason to
         * a non-null value which will be used in the IllegalArgumentException
         * message.
         */
        for (Parameter p : params) {
            String reason = null;

            switch (p.getName()) {
            case ParameterState.GP_COLLECTOR_ENABLED:
                final boolean collectorEnabled = params.getOrDefault(
                    ParameterState.GP_COLLECTOR_ENABLED).asBoolean();
                final String mgmtClass =
                    snParams.getOrDefault(COMMON_MGMT_CLASS).asString();
                reason = checkCollectorViolations(mgmtClass, collectorEnabled);
                break;
            }
            if (reason != null) {
                errors.add(p.getName() + ": " + reason);
            }
        }
        if (!errors.isEmpty()) {
            final StringBuilder sb = new StringBuilder();
            if (errors.size() == 1) {
                sb.append("Invalid parameter: ");
                sb.append(errors.get(0));
            } else {
                sb.append("Invalid parameters:");
                for (String error : errors) {
                   sb.append("\n\t").append(error);
                }
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }

    /**
     * Checks SN parameters. Throws an IllegalArgumentException if a parameter
     * is found to be invalid.
     */
    public static void checkSNParams(ParameterMap params,
                                     ParameterMap gParams) {

        assert params != null;
        assert gParams != null;

        final List<String> errors = new ArrayList<>();

        /* Special case storage directories */
        if (isStorageDirMap(params)) {
            for (Parameter p : params) {
                final String reason =
                            FileUtils.verifyDirectory(new File(p.getName()),
                                                      SizeParameter.getSize(p));
                if (reason != null) {
                    errors.add(reason);
                }
            }
            if (!errors.isEmpty()) {
                final StringBuilder sb = new StringBuilder();
                if (errors.size() == 1) {
                    sb.append("Invalid storage directory: ");
                    sb.append(errors.get(0));
                } else {
                    sb.append("Invalid storage directories:");
                    for (String error : errors) {
                       sb.append("\n\tdirectory: ").append(error);
                    }
                }
                throw new IllegalArgumentException(sb.toString());
            }
            return;
        }

        /*
         * Loop through each parameter. If a problem is found set reason to
         * a non-null value which will be used in the IllegalArgumentException
         * message.
         */
        for (Parameter p : params) {
            String reason = null;

            switch (p.getName()) {
            case ParameterState.SN_ROOT_DIR_PATH:
                reason = FileUtils.verifyDirectory(new File(p.asString()));
                break;

            case ParameterState.SN_ROOT_DIR_SIZE:
                final String rootDir =
                params.getOrDefault(ParameterState.SN_ROOT_DIR_PATH).asString();
                reason = FileUtils.verifyDirectory(new File(rootDir),
                                                   SizeParameter.getSize(p));
                break;
            case ParameterState.COMMON_MGMT_CLASS:
                final boolean collectorEnabled =
                    gParams.getOrDefault(
                      ParameterState.GP_COLLECTOR_ENABLED).asBoolean();
                final String mgmtClass = params.getOrDefault(
                    ParameterState.COMMON_MGMT_CLASS).asString();
                reason = checkCollectorViolations(mgmtClass, collectorEnabled);
            }
            if (reason != null) {
                errors.add(p.getName() + ": " + reason);
            }
        }
        if (!errors.isEmpty()) {
            final StringBuilder sb = new StringBuilder();
            if (errors.size() == 1) {
                sb.append("Invalid parameter: ");
                sb.append(errors.get(0));
            } else {
                sb.append("Invalid parameters:");
                for (String error : errors) {
                   sb.append("\n\t").append(error);
                }
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }

    public static boolean isFileSystemCheckRequired(ParameterMap pmap) {
        if (isStorageDirMap(pmap)) {
            return true;
        }

        for (Parameter p : pmap) {
            switch (p.getName()) {
            case ParameterState.SN_ROOT_DIR_PATH:
            case ParameterState.SN_ROOT_DIR_SIZE:
                return true;
            }
        }
        return false;
    }


    private static String checkCollectorViolations(String mgmtClass,
                                            boolean collectorEnabled) {
        if (collectorEnabled &&
            !MgmtUtil.MGMT_JMX_IMPL_CLASS.equals(mgmtClass)) {
                return "Can't enable collector service because SN JMX is " +
                    "disabled.";
        }
        return null;
    }

    void replaceRepNodeParams(RepNodeParams repNodeParams) {

        /**
         * Creation of RepNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            String serviceName = repNodeParams.getRepNodeId().getFullName();
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);
            if (lp.removeMap(serviceName) == null) {
                throw new IllegalStateException
                       ("replaceRepNodeParams: RepNode service " + serviceName +
                        " is not managed by this Storage Node: " + snaName);
            }
            lp.addMap(repNodeParams.getMap());
            lp.saveParameters(kvConfigPath, logger);
        }
    }

    void replaceArbNodeParams(ArbNodeParams arbNodeParams) {

        /**
         * Creation of ArbNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            String serviceName = arbNodeParams.getArbNodeId().getFullName();
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);
            if (lp.removeMap(serviceName) == null) {
                throw new IllegalStateException
                       ("replaceArbNodeParams: ArbNode service " + serviceName +
                        " is not managed by this Storage Node: " + snaName);
            }
            lp.addMap(arbNodeParams.getMap());
            lp.saveParameters(kvConfigPath, logger);
        }
    }
    void replaceAdminParams(AdminId adminId, ParameterMap params) {

        /**
         * Creation of RepNodes, admins and parameter changes are synchronized
         * in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            String serviceName = adminId.getFullName();
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);
            if (lp.removeMap(serviceName) == null) {
                if (lp.removeMapByType(ParameterState.ADMIN_TYPE) == null) {
                    throw new IllegalStateException
                           ("replaceAdminParams: Admin service " + serviceName +
                            " is not managed by this Storage Node: " +
                         snaName);
                }
            }
            lp.addMap(params);
            lp.saveParameters(kvConfigPath, logger);
        }
    }

    void replaceGlobalParams(GlobalParams gp) {

        /**
         * Creation of RepNodes, admins and global parameter changes are
         * synchronized in order to coordinate changes to the config file(s).
         */
        synchronized(this) {
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);
            if (lp.removeMapByType(ParameterState.GLOBAL_TYPE) == null) {
                logger.warning("Missing GlobalParams on Storage Node: " +
                               snaName);
            }
            lp.addMap(gp.getMap());
            lp.saveParameters(kvConfigPath, logger);
            globalParams = gp;
            globalParameterTracker.notifyListeners(
                null, globalParams.getMap());
        }
    }

    /**
     * Only a restricted set of parameters can be changed via this mechanism.
     * Because there are 2 files changed sequentially there's a slight chance
     * of inconsistency in the face of a failure.  There's nothing to be done
     * for that other than making the call again.
     *
     * The ParameterMap is one of two types -- normal parameters or a map of
     * storage directories.  They are handled differently.  Normal parameters
     * are *merged* into the current map.  This mechanism does not allow actual
     * removal of parameters, which is the desired semantic.  The map
     * containing storage directories is replaced, allowing removal.
     *
     * Because the map of storage directories and some of the SNA parameters are
     * also part of the bootstrap state, those parameters are changed as well.
     */
    void replaceStorageNodeParams(ParameterMap params) {
        synchronized(this) {

            /**
             * Creation of RepNodes, admins and parameter changes are synchronized
             * in order to coordinate changes to the config file(s).
             */
            checkSNParams(params,
                          ConfigUtils.getGlobalParams(kvConfigPath).getMap());

            /**
             * Apply any changes to BootstrapParams first
             */
            changeBootstrapParams(params);

            boolean isModified = false;
            LoadParameters lp =
                LoadParameters.getParameters(kvConfigPath, logger);
            ParameterMap curMap = lp.getMap(ParameterState.SNA_TYPE);
            if (curMap == null) {
                throw new IllegalStateException
                    ("Could not get StorageNodeParams from file: " +
                     kvConfigPath);
            }
            StorageNodeParams snp = new StorageNodeParams(curMap);
            ParameterMap oldMap = curMap.copy();

            /**
             * Change params if there are new params
             */
            if (!isStorageDirMap(params) &&
                !isRNLogDirMap(params)) {
                ParameterMap diff = curMap.diff(params, false);
                logger.log(Level.INFO,
                           "replaceStorageNodeParams: changing: {0}", diff);

                /**
                 * Merge the new/modified values into the current map.  This
                 * relies on the fact that the changeable bootstrap params are
                 * a subset of StorageNodeParams.
                 */
                if (curMap.merge(params, true) > 0) {
                    isModified = true;
                    if (lp.removeMap(curMap.getName()) == null) {
                        throw new IllegalStateException
                            ("Failed to remove StorageNodeParams from file");
                    }
                    lp.addMap(curMap);
                }
            } else if (isStorageDirMap(params)) {

                /**
                 * If the storage directory map is new or modifies the existing
                 * one, apply the change.  The way to entirely clear a storage
                 * directory map to pass an empty map rather than a null entry.
                 * Null means that there is no directory change.
                 */
                ParameterMap currentMap =
                    lp.getMap(ParameterState.BOOTSTRAP_MOUNT_POINTS);
                if (currentMap == null || !currentMap.equals(params)) {
                    isModified = true;
                    if (currentMap != null) {
                        lp.removeMap(currentMap.getName());
                    }
                    logger.log(Level.INFO,
                               "replaceStorageNodeParams: changing storage " +
                               "directory map:\n" +
                               "    from: {0}\n" +
                               "    to: {1}",
                               new Object[]{currentMap, params});
                    lp.addMap(params);
                }
            } else if (isRNLogDirMap(params)) {

                /**
                 * If the rn log directory map is new or modifies the existing
                 * one, apply the change.  The way to entirely clear a rn log
                 * directory map to pass an empty map rather than a null entry.
                 * Null means that there is no directory change.
                 */
                ParameterMap currentMap =
                    lp.getMap(ParameterState.BOOTSTRAP_RNLOG_MOUNT_POINTS);
                if (currentMap == null || !currentMap.equals(params)) {
                    isModified = true;
                    if (currentMap != null) {
                        lp.removeMap(currentMap.getName());
                    }
                    logger.log(Level.INFO,
                               "replaceStorageNodeParams: changing rn log " +
                               "directory map:\n" +
                               "    from: {0}\n" +
                               "    to: {1}",
                               new Object[]{currentMap, params});
                    lp.addMap(params);
                }
            }

            /**
             * Apply changes to the config file
             */
            if (isModified) {
                lp.saveParameters(kvConfigPath, logger);
                initSNParams(snp);
            }

            /* Update login policy according to new SNA params */
            if (isModified &&
                (!isStorageDirMap(params) && !isRNLogDirMap(params))) {
                snParameterTracker.notifyListeners(oldMap, params);
            }

            /* Verify the dns cache ttl values */
            if (!(snp.verifyDnsCacheTTLValues())) {
                logger.info(
                    () ->
                    String.format(
                        "SN restart needed after " +
                        "updating the DNS cache TTL parameters. " +
                        "The current values are %s=%s, %s=%s. " +
                        "The set values are %s=%s, %s=%s. ",
                        INETADDR_CACHE_POLICY_PROP,
                        Security.getProperty(INETADDR_CACHE_POLICY_PROP),
                        INETADDR_CACHE_NEGATIVE_POLICY_PROP,
                        Security.getProperty(
                            INETADDR_CACHE_NEGATIVE_POLICY_PROP),
                        INETADDR_CACHE_POLICY_PROP,
                        snp.getDnsCacheTTL(),
                        INETADDR_CACHE_NEGATIVE_POLICY_PROP,
                        snp.getDnsCacheNegativeTTL()));
            }
        }
    }

    /**
     * Return the SNA's notion of its current StorageNodeParams and
     * GlobalParams, for verification.
     */
    LoadParameters getParams() {
        LoadParameters lp =  LoadParameters.getParameters(kvConfigPath, logger);
        return lp;
    }

    /**
     * Returns current information from the SN.
     */
    StorageNodeInfo getInfo() {
        final StorageNodeInfo sni = new StorageNodeInfo();
        final List<String> paths = new ArrayList<>();
        paths.addAll(bp.getStorageDirPaths());
        paths.addAll(bp.getRNLogDirPaths());
        paths.addAll(bp.getAdminDirPath());

        /*
         * If there are no explicit storage, rnlog and admin  directories,
         * this node is using the root directory which we don't report on.
         */
        for (String path : paths) {
            long size;
            try {
                size = FileUtils.getDirectorySize(path);
            } catch (IllegalArgumentException iae) {
                logger.log(Level.SEVERE,
                           "Exception verifying directory size for " +
                           path, iae);
                size = -1;
            }
            sni.addStorageDirectory(path, size);
        }
        return sni;
    }

    /**
     * Initialize locally cached values from the StorageNodeParams.  This is
     * called at registration time, during startupRegistered, and from
     * newParams, so that the new parameters can take effect.
     */
    private void initSNParams(StorageNodeParams snp) {
        serviceWaitMillis = snp.getServiceWaitMillis();
        repnodeWaitSecs = snp.getRepNodeStartSecs();
        maxLink = snp.getMaxLinkCount();
        linkExecWaitSecs = snp.getLinkExecWaitSecs();
        capacity = snp.getCapacity();
        logFileLimit = snp.getLogFileLimit();
        logFileCount = snp.getLogFileCount();
        numCPUs = snp.getNumCPUs();
        memoryMB = snp.getMemoryMB();
        storageDirectoriesString =
                                joinStringList(snp.getStorageDirPaths(), ",");
        rnLogDirectoriesString =
                                joinStringList(snp.getRNLogDirPaths(), ",");
        adminDirectoryString =
                                joinStringList(snp.getAdminDirPaths(), ",");

        customProcessStartupPrefix = snp.getProcessStartupPrefix();

        /*
         * Start the management agent here.  This covers the cases of
         * startupRegistered and newParams.  If nothing regarding the
         * configuration of the MgmtAgent has changed, then the factory will
         * return the currently established MgmtAgent.
         */
        final MgmtAgent newMgmtAgent =
            MgmtAgentFactory.getAgent(this, snp, statusTracker);

        if (newMgmtAgent != mgmtAgent) {
            mgmtAgent = newMgmtAgent;

            /*
             * If any RepNodes are running at this time, let the new MgmtAgent
             * know about them.
             */
            for (ServiceManager mgr : repNodeServices.values()) {
                /* Update ServiceManager to reference the new MgmtAgent */
                mgr.reloadSNParams();
                ManagedRepNode mrn = (ManagedRepNode) mgr.getService();
                try {
                    mgmtAgent.addRepNode(mrn.getRepNodeParams(), mgr);
                } catch (Exception e) {
                    String msg = mrn.getResourceId().getFullName() +
                        ": Exception adding RepNode to MgmtAgent: " +
                        e.getMessage();
                    throw new IllegalStateException(msg, e);
                }
            }

            /* If there is an admin, announce its existence.*/
            if (adminService != null) {
                /* Update ServiceManager to reference the new MgmtAgent */
                adminService.reloadSNParams();
                ManagedAdmin ma = (ManagedAdmin) adminService.getService();
                try {
                    mgmtAgent.addAdmin(ma.getAdminParams(), adminService);
                } catch (Exception e) {
                    String msg = "Exception adding Admin to MgmtAgent: " +
                        e.getMessage();
                    throw new IllegalStateException(msg, e);
                }
            }

            /*
             * If any ArbNodes are running at this time, let the new MgmtAgent
             * know about them.
             */
            for (ServiceManager mgr : arbNodeServices.values()) {
                /* Update ServiceManager to reference the new MgmtAgent */
                mgr.reloadSNParams();
                ManagedArbNode man = (ManagedArbNode) mgr.getService();
                try {
                    mgmtAgent.addArbNode(man.getArbNodeParams(), mgr);
                } catch (Exception e) {
                    String msg = man.getResourceId().getFullName() +
                        ": Exception adding ArbNode to MgmtAgent: " +
                        e.getMessage();
                    throw new IllegalStateException(msg, e);
                }
            }
        }
    }

    /* Merge List of String into one String, with delimiter. */
    private static String joinStringList(List<String> a, String delimiter) {
        String r = "";
        if (a != null) {
            int n = 0;
            for (String s : a) {
                if (n++ > 0) {
                    r += delimiter;
                }
                r += s;
            }
        }
        return r;
    }

    /**
     * Change bootstrap config file.  Filter out non-bootstrap parameters and
     * possibly replace the storage directory map.  The map is *either* a
     * normal parameter map *or* a map of storage directories.
     */
    private void changeBootstrapParams(ParameterMap params) {
        boolean isModified = false;
        File configPath = new File(bootstrapDir, bootstrapFile);
        bp = ConfigUtils.getBootstrapParams(configPath, logger);
        if (!isStorageDirMap(params)) {
            ParameterMap curMap = bp.getMap();
            ParameterMap bmap =
                params.filter(EnumSet.of(ParameterState.Info.BOOT));
            if (!bmap.isEmpty()) {
                if (curMap.merge(bmap, true) > 0) {
                    isModified = true;
                }
            }
        } else {
            ParameterMap currentMap = bp.getStorageDirMap();
            if (currentMap == null || !currentMap.equals(params)) {
                isModified = true;
                bp.setStorageDirMap(params);
            }
        }
        if (isModified) {
            ConfigUtils.createBootstrapConfig(bp, configPath, logger);
        }
    }

    private static boolean isStorageDirMap(ParameterMap pmap) {
        String name = pmap.getName();
        return (name != null &&
                name.equals(ParameterState.BOOTSTRAP_MOUNT_POINTS));
    }

    private static boolean isRNLogDirMap(ParameterMap pmap) {
        String name = pmap.getName();
        return (name != null &&
                name.equals(ParameterState.BOOTSTRAP_RNLOG_MOUNT_POINTS));
    }

    /*
     * TODO : Check if isRNLogDirMap and isAdminDirMap functionality
     * is needed ?
     */

    /**
     * Functions for snapshot implementation
     */

    /**
     * List the snapshots on the SN.  Assume that if there is more than one
     * managed service they all have the same list, so pick the first one
     * found.
     */
    String [] listSnapshots() {

        ResourceId rid = null;
        File serviceDir = null;

        AdminParams ap = ConfigUtils.getAdminParams(kvConfigPath, logger);
        if (ap != null) {
            rid = ap.getAdminId();
            /*
             * Supporting adminDir in list snapshot
             */
            final ParameterMap adminMountMap = bp.getAdminDirMap();
            final List <String> adminPath =
                BootstrapParams.getStorageDirPaths(adminMountMap);
            if (!adminPath.isEmpty()){
                serviceDir = new File(adminPath.get(0));
            }
        } else {
            List<ParameterMap> repNodes =
                ConfigUtils.getRepNodes(kvConfigPath, logger);
            for (ParameterMap map : repNodes) {
                RepNodeParams rn = new RepNodeParams(map);
                rid = rn.getRepNodeId();
                serviceDir = rn.getStorageDirectoryFile();
                break;
            }
        }
        if (rid == null) {
            logger.warning("listSnapshots: Unable to find managed services");
            return new String[0];

        }
        File snapDir = FileNames.getSnapshotDir(kvRoot.toString(),
                                                getStoreName(),
                                                serviceDir,
                                                snid,
                                                rid);
        if (snapDir.isDirectory()) {
            File [] snapFiles = snapDir.listFiles();
            String [] snaps = new String[snapFiles.length];
            for (int i = 0; i < snaps.length; i++) {
                snaps[i] = snapFiles[i].getName();
            }
            return snaps;
        }
                return new String[0];
    }

    String snapshotAdmin(AdminId aid, String name)
        throws RemoteException {

        if (adminService == null) {
            String msg = "AdminService " + aid + " is not running";
            logger.warning(msg);
            throw new IllegalStateException(msg);
        }
        try {
            ManagedAdmin ma = (ManagedAdmin) adminService.getService();
            /*
             * Supporting adminDir in snapshot create
             */
            final ParameterMap adminMountMap = bp.getAdminDirMap();
            final List <String> adminPath =
                BootstrapParams.getStorageDirPaths(adminMountMap);
            File adminDir = null;
            if (!adminPath.isEmpty()){
                adminDir = new File(adminPath.get(0));
            }
            CommandServiceAPI cs = ma.getAdmin(this);
            String[] files = cs.startBackup();
            String path = null;
            try {
                path = snapshot(aid, adminDir, name, files);
            } finally {
                cs.stopBackup();
            }
            return path;
        } catch (RemoteException re) {
            logwarning(("Exception attempting to snapshot Admin " + aid), re);
            throw re;
        }
    }

    String snapshotRepNode(RepNodeId rnid, String name)
        throws RemoteException {

        String serviceName = rnid.getFullName();
        ServiceManager mgr = repNodeServices.get(serviceName);
        if (mgr == null) {
            String msg = rnid + ": RepNode is not running";
            logger.warning(msg);
            throw new IllegalStateException(msg);
        }
        try {
            ManagedRepNode mrn = (ManagedRepNode) mgr.getService();
            RepNodeAdminAPI rna = mrn.getRepNodeAdmin(this);
            File repNodeDir = mrn.getRepNodeParams().getStorageDirectoryFile();
            String[] files = rna.startBackup();
            String path = null;
            try {
                path = snapshot(rnid, repNodeDir, name, files);
            } finally {
                rna.stopBackup();
            }
            return path;
        } catch (RemoteException re) {
            logwarning
                (("Exception attempting to snapshot RepNode " + rnid), re);
            throw re;
        }
    }

    /**
     * Shared code for RN and Admin snapshot removal.
     *
     * @param rid the ResourceId used to find the snapshot directory
     * @param name the name of the snapshot to remove, if null, remove all
     * snapshots
     */
    void removeSnapshot(ResourceId rid, String name) {

        File serviceDir = null;
        if (rid instanceof RepNodeId) {
            RepNodeParams rnp =
                ConfigUtils.getRepNodeParams
                (kvConfigPath, (RepNodeId) rid, logger);
            if (rnp == null) {
                String msg = rid.getFullName() + ": RepNode has not been created";
                logger.info(msg);
                /* This rep node should have been created by this point. */
                throw new IllegalStateException(msg);
            }
            serviceDir = rnp.getStorageDirectoryFile();
        }

        /*
         * Supporting adminDir in snapshot remove
         */
        if (rid instanceof AdminId) {
            final ParameterMap adminMountMap = bp.getAdminDirMap();
            final List <String> adminPath =
                BootstrapParams.getStorageDirPaths(adminMountMap);
            if (!adminPath.isEmpty()){
                serviceDir = new File(adminPath.get(0));
            }
        }

        File snapshotDir =
            FileNames.getSnapshotDir(kvRoot.toString(),
                                     getStoreName(),
                                     serviceDir,
                                     snid,
                                     rid);
        if (name != null) {
            removeFiles(new File(snapshotDir, name));
        } else {
            if (snapshotDir.isDirectory()) {
                for (File file : snapshotDir.listFiles()) {
                    logger.info(rid + ": Removing snapshot " + file.getName());
                    removeFiles(file);
                }
            }
        }
    }

    /**
     * Make hard links from the array of file names in files in the source
     * directory to the destination directory.  On *nix and Solaris systems the
     * ln command can take the form:
     *   ln file1 file2 file3 ... fileN destinationDir
     * This allows a single exec to create links for a number of files,
     * amortizing the cost of the exec.  Issues to be aware of:
     * -- max number of args in a single command line
     * -- max length of command line
     */
    private void makeLinks(File srcBase, File destDir, String[] files) {
        if (isWindows) {
            for (String file : files) {
                File src = new File(srcBase, file);
                File dest = new File(destDir, file);
                windowsMakeLink(src, dest);
            }
            return;
        }
        int nfiles = 0;
        List<String> command = new ArrayList<>();
        command.add(LINK_COMMAND);
        command.add("-f"); /* overwrites any existing target */
        for (String file : files) {
            command.add(new File(srcBase, file).toString());
            if (++nfiles >= maxLink) {
                /*
                 * Perform the operation and reset.
                 */
                command.add(destDir.toString());
                execute(command);
                command = new ArrayList<>();
                command.add(LINK_COMMAND);
                command.add("-f"); /* overwrites any existing target */
                nfiles = 0;
            }
        }
        /*
         * Execute the command for the remaining files
         */
        if (command.size() > 2) {
            command.add(destDir.toString());
            execute(command);
        }
    }

    /**
     * Make a hard link from src (existing) to dest (the new link) This
     * executes "ln" in a new process to perform the link.  Ideally it'd be
     * built into Java but that's not the case (yet).
     */
    private void windowsMakeLink(File src, File dest) {
        if (!isWindows) {
            throw new IllegalStateException
                ("Function should only be called on Windows");
        }
        List<String> command = new ArrayList<>();
        command.add("fsutil");
        command.add("hardlink");
        command.add("create");
        command.add(dest.toString());
        command.add(src.toString());
        execute(command);
    }

    private void execute(List<String> command) {
        /**
         * Leave off Logger argument.  It makes the log output too verbose
         */
        ProcessMonitor pm =
            new ProcessMonitor(command, 0, "snapshot", null);
        try {
            pm.startProcess();
            if (!pm.waitProcess(linkExecWaitSecs * 1000)) {
                throw new IllegalStateException
                    ("Timeout waiting for ln process to complete");
            }
        } catch (Exception e) {
            logger.info("Snapshot failed to make links with command: " +
                        command + ": " + e);
            throw new SNAFaultException(e);
        }
    }

    /**
     * The guts of snapshot shared by RepNode and Admin backup.  Assumes that
     * DbBackup has been called on the service to get the file array.
     * 1.  make the target directory
     * 2.  for each log file in the list, make a hard link from it
     *     to the target directory
     *
     * @return the full path to the new snapshot directory
     */
    private String snapshot(ResourceId rid,
                            File serviceDir,
                            String name,
                            String [] files) {

        File srcBase = FileNames.getEnvDir(kvRoot.toString(),
                                           getStoreName(),
                                           serviceDir,
                                           snid,
                                           rid);
        File destBase = FileNames.getSnapshotDir(kvRoot.toString(),
                                                 getStoreName(),
                                                 serviceDir,
                                                 snid,
                                                 rid);
        File destDir = new File(destBase, name);
        logger.info("Creating snapshot of " + rid + ": " + name + " (" +
                    files.length + " files)");

        /**
         * Create the snapshot directory, make sure it does not exist first.
         */
        if (destDir.exists()) {
            String msg =
                "Snapshot directory exists, cannot overwrite: " + destDir;
            logger.warning(msg);
            throw new IllegalStateException(msg);
        }
        FileNames.makeDir(destDir);

        makeLinks(srcBase, destDir, files);
        logger.info("Completed snapshot of " + rid + ": " + name);
        return destDir.toString();
    }

    /**
     * Recursive delete.  Danger!
     */
    private void removeFiles(File target) {
        if (target.isDirectory()) {
            for (File f : target.listFiles()) {
                removeFiles(f);
            }
        }
        if (target.exists() && !target.delete()) {
            String msg = "Unable to remove file or directory " + target;
            logger.warning(msg);
            throw new IllegalStateException(msg);
        }
    }

    /**
     * This method must not be called for a running service.  See if there is
     * an environment ready to be used for recovery of the service.  If so,
     * replace the existing environment, if any, with the new one.
     */
    private void checkForRecovery(ResourceId rid, File dir) {
        File recoveryDir = FileNames.getRecoveryDir(kvRoot.toString(),
                                                    getStoreName(),
                                                    dir, snid, rid);

        checkForRestoreFromSnapshot(recoveryDir);

        if (recoveryDir.isDirectory()) {
            File[] files = recoveryDir.listFiles();
            if (files.length != 1) {
                logger.info(rid + ": only one file is allowed in recovery " +
                            " directory " + recoveryDir + ", not recovering");
                return;
            }
            if (!files[0].isDirectory()) {
                logger.info("Recovery file " + files[0] + " is not a directory"
                            + ", cannot use it for recovery");
                return;
            }

            File envDir = FileNames.getEnvDir(kvRoot.toString(),
                                              getStoreName(),
                                              dir, snid, rid);
            logger.info(rid + ": recovering from " + files[0] +
                        " to environment directory " + envDir);

            /**
             * First rename the old env dir if present.  TODO: maybe remove it.
             */
            if (envDir.isDirectory()) {
                File target = new File(envDir.toString() + ".old");
                if (target.exists()) {
                    removeFiles(target);
                }
                if (!envDir.renameTo(target)) {
                    logger.warning(rid + ": failed to rename old env dir");
                    return;
                }
            }
            if (!files[0].renameTo(envDir)) {
                logger.warning(rid + ": failed to rename recovery directory");
            }
        }
    }

    /*
     * Make recovery directory from the restore snapshot.
     * This method is run when user explicitly specify to restore from
     * snapshot. If recovery directory already exist, override
     * the content with snapshot. Make new file links in the recovery
     * directory.
     */
    private void checkForRestoreFromSnapshot(File recoveryDir) {

        if (restoreSnapshotName == null) {
            return;
        }

        File snapshotDir = FileNames.getSnapshotNamedDir(
            recoveryDir.getParentFile(), restoreSnapshotName);

        if (!snapshotDir.exists()) {
            throw new IllegalStateException(
                "Fail to find snapshot directory: " + snapshotDir.toString());
        }

        /* Replace the recovery directory */
        if (recoveryDir.exists()) {
            FileUtils.deleteDirectory(recoveryDir);
        }

        final File destDir = new File(recoveryDir, restoreSnapshotName);
        destDir.mkdirs();
        makeLinks(snapshotDir, destDir, snapshotDir.list());
    }

    /**
     * Restore configurations from snapshot config. The method will try to
     * restore following configurations:
     * KVROOT/config.xml
     * KVROOT/security.policy
     * KVROOT/security (Only in secure store)
     * KVROOT/STORENAME/security.policy
     * KVROOT/STORENAME/SN/config.xml
     */
    private void checkForConfigRecovery() {

        if (restoreSnapshotName == null) {
            return;
        }

        final File snapshotDir =
            FileNames.getSnapshotNamedDir(bootstrapDir, restoreSnapshotName);

        /**
         * Fix for KVSTORE-1786: Start with unknown snapshot hangs silently
         * Throw IllegalStateException if the snapshot directory does not
         * exit. Do this when -update-config is both true or false
         */
        if (!snapshotDir.exists()) {
            throw new IllegalStateException(
                "Failed to find snapshot directory: " + snapshotDir);
        }

        if (isUpdateConfig == UpdateConfigType.FALSE) {
            return;
        }

        final SnapshotTaskHandler handler =
            new SnapshotFileUtils.SnapshotLockHandler(snapshotDir);

        handler.handleTask((SnapshotConfigTask)() -> {

            /* mark start snapshot operation */
            SnapshotFileUtils.snapshotOpStart(
                SnapshotOp.RESTORE, snapshotDir);

            /* restore bootstrap config file */
            SnapshotFileUtils.restoreSnapshotConfig(
                new File(bootstrapDir, bootstrapFile),
                snapshotDir, isUpdateConfig);

            /* restore security policy file on kvroot */
            SnapshotFileUtils.restoreSnapshotConfig(
               FileNames.getSecurityPolicyFile(new File(bootstrapDir)),
               snapshotDir, isUpdateConfig);

            /* Load bootstrap parameters */
            final File configPath = new File(bootstrapDir, bootstrapFile);
            BootstrapParams bootParam =
                ConfigUtils.getBootstrapParams(configPath, logger);

            final File snapshotStoreDir = new
               File(snapshotDir, bootParam.getStoreName());

            final File snapshotSNDir =
                FileNames.getStorageNodeDir(
                    snapshotStoreDir, new StorageNodeId(bootParam.getId()));

            /* restore security config */
            if (bootParam.getSecurityDir() != null) {
                SnapshotFileUtils.restoreSnapshotConfig(
                    new File(bootstrapDir, bootParam.getSecurityDir()),
                    snapshotDir, isUpdateConfig);
            }

            /* restore security policy file on store directory */
            SnapshotFileUtils.restoreSnapshotConfig(
                FileNames.getSecurityPolicyFile(
                    FileNames.getKvDir(bootstrapDir,
                                       bootParam.getStoreName())),
                snapshotStoreDir, isUpdateConfig);

            /* restore SNA config file */
            final StorageNodeId id = new StorageNodeId(bootParam.getId());
            SnapshotFileUtils.restoreSnapshotConfig(
                FileNames.getSNAConfigFile(
                    bootstrapDir, bootParam.getStoreName(), id),
                snapshotSNDir, isUpdateConfig);

            /* mark snapshot operation complete */
            SnapshotFileUtils.snapshotOpComplete(
                SnapshotOp.RESTORE, snapshotDir);
        });
    }

    public MgmtAgent getMgmtAgent() {
        return mgmtAgent;
    }

    public SecurityParams getSecurityParams() {
        return sp;
    }

    public SnaLoggingStatsTracker getSnaLoggingStatsTracker() {
        final SnaLoggingStatsTracker result = loggingStatsTracker;
        if (result == null) {
            throw new IllegalStateException(
                "SnaLoggingStatsTracker is not yet initialized");
        }
        return result;
    }

    public Integer getCapacity() {
        return capacity;
    }

    public int getLogFileLimit() {
        return logFileLimit;
    }

    public int getLogFileCount() {
        return logFileCount;
    }

    public int getNumCpus() {
        return numCPUs;
    }

    public int getMemoryMB() {
        return memoryMB;
    }

    public String getMountPointsString() {
        return storageDirectoriesString;
    }

    public String getRNLogMountPointsString() {
        return rnLogDirectoriesString;
    }

    public String getAdminMountPointsString() {
        return adminDirectoryString;
    }

    public SNASecurity getSNASecurity() {
        return snaSecurity;
    }

    public LoginManager getLoginManager() {
        return snaSecurity.getLoginManager();
    }

    ProcessFaultHandler getFaultHandler() {
        return snai.getFaultHandler();
    }

    public long getCollectorInterval() {
        if (globalParams != null) {
            return globalParams.getCollectorInterval();
        }
        DurationParameter dp = (DurationParameter) ParameterState.lookup(
            ParameterState.GP_COLLECTOR_INTERVAL).getDefaultParameter();
        return dp.toMillis();
    }

    public int getJsonVersion() {
        return jsonVersion;
    }

    /**
     * Create snapshot of configurations. The following configurations will be
     * snapshot:
     * KVROOT/config.xml
     * KVROOT/security.policy
     * KVROOT/security (Only in secure store)
     * KVROOT/STORENAME/security.policy
     * KVROOT/STORENAME/SN/config.xml
     *
     * @param snapshotName full name of the snapshot.
     */
    void createSnapshotConfig(String snapshotName) {

        final File snapshotDir =
            FileNames.getSnapshotNamedDir(kvRoot, snapshotName);

        if (!snapshotDir.exists()) {
            snapshotDir.mkdirs();
        }

        final File snapshotStoreDir = new File(snapshotDir, getStoreName());

        if (!snapshotStoreDir.exists()) {
            snapshotStoreDir.mkdirs();
        }

        final File snapshotSNDir =
            FileNames.getStorageNodeDir(snapshotStoreDir, getStorageNodeId());

        if (!snapshotSNDir.exists()) {
            snapshotSNDir.mkdirs();
        }

        final SnapshotTaskHandler handler =
            new SnapshotFileUtils.SnapshotLockHandler(snapshotDir);

        handler.handleTask((SnapshotConfigTask)() -> {

            /* mark start snapshot operation */
            SnapshotFileUtils.snapshotOpStart(
                SnapshotOp.SNAPSHOT, snapshotDir);

            /* Snapshot bootstrap config file */
            SnapshotFileUtils.snapshotConfig(
                new File(kvRoot, bootstrapFile), snapshotDir);

            /* Snapshot security policy file on root directory */
            SnapshotFileUtils.snapshotConfig(
                FileNames.getSecurityPolicyFile(kvRoot),
                snapshotDir);

            /* Snapshot security configs */
            if (getSecurityDir() != null) {
                SnapshotFileUtils.snapshotConfig(
                    getSecurityDir(), snapshotDir);
            }

            /* Snapshot security policy file on store directory */
            SnapshotFileUtils.snapshotConfig(
                FileNames.getSecurityPolicyFile(
                    FileNames.getKvDir(kvRoot.toString(), getStoreName())),
                snapshotStoreDir);

            /* Snapshot SNA config file */
            SnapshotFileUtils.snapshotConfig(
                kvConfigPath, snapshotSNDir);

            /* mark snapshot operation complete */
            SnapshotFileUtils.snapshotOpComplete(
                SnapshotOp.SNAPSHOT, snapshotDir);
        });
    }

    /**
     * Remove the snapshot of configurations. If snapshotName is null, all the
     * snapshots under base snapshot directory will be removed.
     * @param snapshotName full name of snapshot.
     */
    public void removeSnapshotConfig(String snapshotName) {

        /* remove snapshot under root */
        SnapshotFileUtils.removeSnapshotConfig(kvRoot, snapshotName);
    }

    /** Returns the service status tracker for the service. */
    public ServiceStatusTracker getServiceStatusTracker() {
        return statusTracker;
    }

    /** Get information about TLS credentials in JSON format. */
    String getTlsCredentialsInfo() {
        checkSecurityEnabled();

        final String keystore = sp.getKeystoreFile();
        final String truststore = sp.getTruststoreFile();

        final ObjectNode resultNode = JsonUtils.createObjectNode();
        final ObjectNode installed =
            resultNode.putObject("installedCredentials");
        ObjectNode info = getFileInfo(keystore);
        if (info != null) {
            installed.put("keystore", info);
        }
        info = getFileInfo(truststore);
        if (info != null) {
            installed.put("truststore", info);
        }
        final ObjectNode updates =
            resultNode.putObject("pendingUpdates");
        info = getFileInfo(SECURITY_UPDATES_DIR + File.separator + keystore);
        if (info != null) {
            updates.put("keystore", info);
        }
        info = getFileInfo(SECURITY_UPDATES_DIR + File.separator + truststore);
        if (info != null) {
            updates.put("truststore", info);
        }
        return resultNode.toString();
    }

    /**
     * Check that the SNA is registered, that security-related fields have
     * been initialized, and that the store has security enabled.
     */
    private void checkSecurityEnabled() {
        if (!isRegistered()) {
            throw new IllegalStateException("SNA should be registered");
        }
        initSecurity();
        if (securityDir == null) {
            throw new IllegalStateException(
                "Operation not supported for non-secure store");
        }
    }

    /**
     * Get information about a file in JSON format, including flagging errors.
     * Returns null if the file does not exist.
     */
    private ObjectNode getFileInfo(String localPath) {
        final File file = new File(sp.getConfigDir(), localPath);
        if (!file.exists()) {
            return null;
        }
        final ObjectNode node = JsonUtils.createObjectNode();
        node.put("file", localPath);
        String modTime = null;
        try {
            modTime = getFormattedFileModTime(file);
        } catch (IOException e) {
            modTime = e.toString();
        }
        if (modTime != null) {
            node.put("modTime", modTime);
        }
        String hash = null;
        try {
            hash = computeSha256Hash(file);
        } catch (NoSuchFileException e) {
            hash = null;
        } catch (IOException e) {
            hash = e.toString();
        }
        if (hash != null) {
            node.put("hash", hash);
        }
        return node;
    }

    /**
     * Call the retrieve script, if any, with the current directory set to the
     * update directory, to check for credential updates. Throws
     * IllegalStateException if the retrieve script is found and is not
     * executable, if there is an I/O error attempting to create the process to
     * run the script, if the script runs for more than 5 minutes, or if the
     * script exits with a non-zero error code.
     */
    String retrieveTlsCredentials() {
        checkSecurityEnabled();

        final File updatesDir =
            new File(sp.getConfigDir(), SECURITY_UPDATES_DIR);
        final File retrieve =
            new File(updatesDir, RETRIEVE_SECURITY_UPDATES_FILE);
        final String retrievePath = retrieve.getAbsolutePath();
        if (!retrieve.exists()) {
            return "Not retrieving updates because there is no retrieve" +
                " command file";
        }

        final File keystore = new File(updatesDir, sp.getKeystoreFile());
        final long keystoreModTime = keystore.lastModified();
        final File truststore = new File(updatesDir, sp.getTruststoreFile());
        final long truststoreModTime = truststore.lastModified();

        final File retrieveOutput;
        try {
            retrieveOutput = File.createTempFile("retrieve", "out");
        } catch (IOException e) {
            throw new IllegalStateException(
                "Problem creating temp file to record retrieve command" +
                " output: " + e,
                e);
        }
        retrieveOutput.deleteOnExit();
        try {
            final Process process;
            try {
                process = new ProcessBuilder(retrievePath)
                    .redirectErrorStream(true)
                    .redirectOutput(retrieveOutput)
                    .directory(updatesDir)
                    .start();
            } catch (IOException e) {
                /*
                 * Note that attempting to execute the retrieve command will
                 * fail if the file is not executable. That is an unexpected
                 * situation that will be reported here.
                 */
                throw new IllegalStateException(
                    "Problem executing TLS credentials retrieve command: " +
                    e,
                    e);
            }
            String failure = null;
            Throwable cause = null;
            try {
                if (!process.waitFor(5, MINUTES)) {
                    final long pid = process.pid();
                    process.destroyForcibly();
                    failure = "Retrieve command did not complete" +
                        " successfully within the 5 minute time limit," +
                        " killed process " + pid;
                }
            } catch (InterruptedException e) {
                process.destroyForcibly();
                failure = "Retrieve command was interrupted";
                cause = e;
            }
            if ((failure == null) && (process.exitValue() != 0)) {
                failure = "Retrieve command exited with non-zero exit" +
                    " code: " + process.exitValue();
            }
            if (failure != null) {
                final AtomicBoolean wasTruncated = new AtomicBoolean();
                final StringBuilder msg = new StringBuilder(failure);
                try {
                    final String output = FileUtils.readFileWithMaxSize(
                        retrieveOutput, 800, wasTruncated);
                    msg.append(": ").append(output);
                } catch (IOException e) {
                }
                if (wasTruncated.get()) {
                    msg.append(" [output truncated after 800 characters]");
                }
                throw new IllegalStateException(msg.toString(), cause);
            }
        } finally {
            retrieveOutput.delete();
        }
        final boolean keystoreUpdated =
            keystoreModTime != keystore.lastModified();
        final boolean truststoreUpdated =
            truststoreModTime != truststore.lastModified();
        if (!keystoreUpdated && !truststoreUpdated) {
            return "No TLS credential updates found";
        }
        return "Found TLS credential updates: " +
            (keystoreUpdated  ? "keystore " : "") +
            (truststoreUpdated ? "truststore " : "");
    }

    /**
     * Verify that any TLS credential updates are consistent either with each
     * other or with installed credentials. Throws an IllegalStateException if
     * a problem is detected. If force is true, ignore credentials problems
     * within the keystore.
     */
    CredentialHashes verifyTlsCredentialUpdates(boolean force) {
        checkSecurityEnabled();
        return SecurityUtils.verifyTlsCredentialUpdates(sp.getConfigDir(),
                                                        force, logger);
    }

    /**
     * Check for a truststore file in the update directory. If it is present,
     * make sure that the credentials in that file are present in the installed
     * truststore, and update the client truststore. Throws a RuntimeException
     * if an unexpected problem occurs.
     */
    String addTruststoreUpdates() {
        checkSecurityEnabled();
        return SecurityUtils.addTruststoreUpdates(sp.getConfigDir(), logger);
    }

    /**
     * Install the updated keystore. Throws a RuntimeException if an unexpected
     * problem occurs.
     */
    String installKeystoreUpdate(String keystoreHash) {
        checkSecurityEnabled();
        return SecurityUtils.installKeystoreUpdate(keystoreHash,
                                                   sp.getConfigDir());
    }

    /**
     * Install the updated truststore and update the client truststore. Throws
     * a RuntimeException if an unexpected problem occurs.
     */
    String installTruststoreUpdate(String truststoreHash) {
        checkSecurityEnabled();
        return SecurityUtils.installTruststoreUpdate(
            truststoreHash, sp.getConfigDir());
    }
}
