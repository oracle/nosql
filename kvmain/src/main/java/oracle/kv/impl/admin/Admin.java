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

import static oracle.nosql.common.contextlogger.ContextUtils.fineWithCtx;
import static oracle.nosql.common.contextlogger.ContextUtils.isLoggableWithCtx;
import static oracle.nosql.common.contextlogger.ContextUtils.warningWithCtx;
import static oracle.nosql.common.json.JsonUtils.getAsText;

import java.io.File;
import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.impl.admin.client.PoolCommand;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.criticalevent.EventRecorder;
import oracle.kv.impl.admin.criticalevent.EventRecorder.LatestEventTimestamps;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.ArbNodeParams;
import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.plan.DeploymentInfo;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.PlanRun;
import oracle.kv.impl.admin.plan.Planner;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.admin.plan.task.UpdateMetadata;
import oracle.kv.impl.admin.topo.RealizedTopology;
import oracle.kv.impl.admin.topo.Rules;
import oracle.kv.impl.admin.topo.Rules.Results;
import oracle.kv.impl.admin.topo.TopologyBuilder;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.admin.topo.TopologyDiff;
import oracle.kv.impl.admin.topo.Validations.RulesProblem;
import oracle.kv.impl.api.KVStoreInternalFactory;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.client.admin.ExecutionInfo;
import oracle.kv.impl.client.admin.ExecutionInfoImpl;
import oracle.kv.impl.fault.DatabaseNotReadyException;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.MonitorKeeper;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterTracker;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.security.AccessChecker;
import oracle.kv.impl.security.ClientProxyCredentials;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.RoleResolver;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.LoginUpdater;
import oracle.kv.impl.security.metadata.SecurityMDListener;
import oracle.kv.impl.security.metadata.SecurityMDTracker;
import oracle.kv.impl.security.metadata.SecurityMDUpdater;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.security.oauth.IDCSOAuthAuthenticator;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationManager;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.StateTracker;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.TxnUtil;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.JENotifyHooks.LogRewriteListener;
import oracle.kv.impl.util.server.JENotifyHooks.RecoveryListener;
import oracle.kv.impl.util.server.JENotifyHooks.RedirectHandler;
import oracle.kv.impl.util.server.JENotifyHooks.SyncupListener;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.ErrorMessage;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Durability.ReplicaAckPolicy;
import com.sleepycat.je.Durability.SyncPolicy;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicaConsistencyException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.je.utilint.StoppableThread;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An instance of the Admin class provides the general API underlying the
 * administrative interface.  It also owns the global Topology, System
 * Metadata, Parameters and Monitor instances, and provides interfaces
 * for fetching and storing them.
 */
public class Admin
        implements MonitorKeeper, ParameterListener, StateChangeListener {

    public static final String CAUSE_CREATE = "after create";
    public static final String CAUSE_APPROVE = "after approval";
    private static final String CAUSE_CANCEL = "after cancel";
    private static final String CAUSE_INTERRUPT = "after interrupt";
    public static final String CAUSE_INTERRUPT_REQUEST =
        "after interrupt requested";
    public static final String CAUSE_EXEC = "after execution";
    private static final long ADMIN_JE_CACHE_SIZE = 0;
    private final AdminId adminId;
    private AdminId masterId;

    /* For testing only */
    public static TestHook<Admin> EXECUTE_HOOK;

    /**
     * For testing only.  Called at the start of the shutdown method with the
     * admin object as an argument.
     */
    public static volatile TestHook<Admin> beginShutdownHook;

    /**
     * This is the maximum number of change history objects that are allowed
     * to be stored with each metadata objects in the Admin database.
     */
    private static final int MAX_MD_CHANGE_HISTORY = 1000;

    /**
     * The maximum number of in-place retries for writing topology history
     * record into system table.
     */
    private static final int MAX_STORE_WRITE_RETRIES = 10;

    /*
     * The parameters used to configure the Admin instance.
     */
    private final AdminServiceParams myParams;

    /*
     * Reference to planner. The planner is present only when the Admin is the
     * master, otherwise the field is null. Access must be synchronized
     * on the Admin instance.
     */
    private volatile Planner planner;

    /*
     * Track listeners for parameter changes.
     */
    private final ParameterTracker parameterTracker;

    /*
     * Track listeners for global parameter changes.
     */
    private final ParameterTracker globalParameterTracker;

    /*
     * Track listeners for security metadata change.
     */
    private final SecurityMDTracker securityMDTracker;

    /*
     * Track listeners for topology and parameter changes.
     */
    private final SNParameterChangeTracker parameterChangeTracker;

    /*
     * Monitoring services in the store.
     */
    private Monitor monitor;

    /*
     * SN consistency checker
     */
    private ParamConsistencyChecker metadataAdminChecker;
    private SoftwareVersionUpdater versionUpdater;

    /*
     * Subsystem for recording significant monitor events.
     */
    private EventRecorder eventRecorder;

    private final Logger logger;

    private Parameters parameters;

    private Memo memo;

    private final EnvironmentConfig envConfig;
    private final ReplicationConfig repConfig;
    private final File envDir;
    private final File snapshotDir;
    private final StateTracker stateTracker;
    private final ReplicatedEnvironment environment;

    private final AdminService owner; /* May be null. */

    private final UncaughtExceptionHandler exceptionHandler;

    /*
     * startupStatus is used to coordinate admin initialization between the
     * current thread and the thread that has been spawned to asynchronously do
     * the longer-running init tasks.
     */
    private final StartupStatus startupStatus;

    /**
     * Whether the admin is shutting down. Only modify this field while
     * synchronized on closingLock. If this field is true, new enterMode calls
     * will return immediately without taking effect.
     */
    private volatile boolean closing = false;

    /**
     * Whether the admin is in an enterMode call. Only access this field while
     * synchronized on closingLock. If this field is true, calls to shutdown
     * will wait for any pending enterMode call to set the field to false on
     * exit before completing the shutdown.
     */
    private boolean enteringMode;

    /**
     * Synchronizer for access to the closing and enteringMode fields.
     * This lock is always the innermost lock to hold.
     */
    private final Object closingLock = new Object();

    /**
     * Synchronizer for starting/stopping Admin threads. Code that modifies
     * Admin threads must hold this lock, this includes:
     *  - shutdown
     *  - replication mode change
     *  - parameter change
     *
     * The locking order is that the threadLock must be locked outside the
     * Admin monitor. This is because that the stopping thread callback
     * may take the Admin monitor lock.
     *
     */
    private final Object threadLock = new Object();

    private int eventStoreCounter = 0;
    private int eventStoreAgingFrequency = 100;

    /*
     * Cached global store version parameter.
     * This is the minimum version that the store is operating at. This
     * starts at the current release prerequisite since this admin should
     * not be able to be installed if the prereq was not met.
     */
    private volatile KVVersion storeVersion = KVVersion.PREREQUISITE_VERSION;

    /*
     * This is the minimum version the whole admin group is operating at.
     */
    private volatile KVVersion adminVersion = KVVersion.PREREQUISITE_VERSION;

    private final AdminStores stores;

    /* Thread to monotor Admin upgrade. */
    private StoppableThread upgradeMonitor;

    /* Thread to create/upgrade system tables. */
    private SysTableMonitor sysTableMonitor;

    private final AdminStatsTracker adminStatsTracker;

    /**
     * Admin je.maxDisk default value. This is an estimated maximum based on
     * creation of a large number of plans and tables. If it is insufficient
     * it can be overridden by specifying configParameters for the admin. In
     * the future we expect to reduce this size using metadata pruning.
     */
    private static final long ADMIN_DEFAULT_MAX_DISK =
        3 * 1024L * 1024L * 1024L;

    /* An internal kvstore handle. */
    private volatile KVStore kvstore;

    private final VerificationManager verificationManager;

    private final MRTManager mrtServiceManager;

    /* Time when the Admin instance was created. */
    private final long serviceStartTime;

    /*
     * Time when the Admin had it's last state change event, or 0 if no event
     * has been seen.
     */
    private volatile long stateChangeTime;

    /*
     * Gate to prevent writing to system tables if this is a K/V store or
     * the store is not yet created.
     *
     * We prevent the Admin from writing system tables to avoid collisions
     * with the Key/Value keyspace. Once there are user tables, we allow
     * writes since the user warned against mixing table and K/V operations.
     *
     * Writes are delayed until the store is initialized to avoid exceptions.
     *
     * Synchronize on Admin instance when setting.
     */
    private volatile boolean systemTablesEnabled = false;

    private volatile boolean systemTablesReady = false;

    /*
     * Public constructor for unit test.
     */
    public Admin(AdminServiceParams params) {
        this(params, null, null, null, null);
    }

    /**
     * Constructor.
     */
    Admin(AdminServiceParams params, AdminService owner) {
        this(params, owner, null, null, null);
    }

    Admin(AdminServiceParams params, AdminService owner,
          final ReplicatedEnvironment testEnvironment,
          final StartupStatus testStartupStatus,
          final AdminStores testAdminStores) {

        this.owner = owner;

        parameters = null;
        myParams = params;
        exceptionHandler = new AdminExceptionHandler();
        parameterTracker = new ParameterTracker();
        globalParameterTracker = new ParameterTracker();
        securityMDTracker = new SecurityMDTracker();
        parameterChangeTracker = new SNParameterChangeTracker();

        final AdminParams adminParams = myParams.getAdminParams();
        final StorageNodeParams snParams = myParams.getStorageNodeParams();

        this.adminId = adminParams.getAdminId();

        /*
         * adminMountMap contains single entry corresponding to
         * individual admin storage directory.
         */
        ParameterMap adminMountMap = snParams.getAdminDirMap();
        String adminDirName = null;
        long adminDirSize = 0L;
        if (adminMountMap != null) {
            for (Parameter adminDir : adminMountMap) {
                adminDirName = adminDir.getName();
                adminDirSize = SizeParameter.getSize(adminDir);
            }
        }

        /*
         * TODO : We are doing validate[Rep/Arb]NodeDirectory and also
         * raising warnings for not mentioning size before following steps
         * for storage node. These checks need to be considered for admindir.
         */
        if (adminDirName != null) {
            /*
             * We have dedicated admin mount point specified.
             */
            envDir = FileNames.getAdminEnvDir(adminDirName, adminId);
            snapshotDir = FileNames.getAdminSnapshotDir(adminDirName, adminId);
        } else {
            envDir =
                FileNames.getEnvDir
                    (snParams.getRootDirPath(),
                     myParams.getGlobalParams().getKVStoreName(),
                     null, snParams.getStorageNodeId(), adminId);

            snapshotDir =
                FileNames.getSnapshotDir
                    (snParams.getRootDirPath(),
                     myParams.getGlobalParams().getKVStoreName(),
                     null, snParams.getStorageNodeId(), adminId);
        }

        final boolean created = FileNames.makeDir(envDir);

        /*
         * Add a log handler to send logging messages on the Admin process to
         * the Monitor. This must happen before the first monitor is
         * instantiated.  This registration lasts the lifetime of the Admin.
         */
        LoggerUtils.registerMonitorAdminHandler
            (myParams.getGlobalParams().getKVStoreName(), adminId, this);

        /*
         * Monitor should exist before the planner, and preferably before
         * any loggers, so that Admin logging can be sent to the store-wide
         * view.
         */
        monitor = new Monitor(myParams, this, getLoginManager());

        /*
         * By creating the event recorder early in the bootstrapping process, we
         * put a stake in the ground with the Trackers to avoid the pruning of
         * early events.  However, we can't start up the recorder until after
         * the persistent environment is viable.
         */
        eventRecorder = new EventRecorder(this);

        addParameterListener(eventRecorder);
        addParameterListener(monitor);
        addParameterListener(UserDataControl.getParamListener());
        addParameterListener(this);
        addParameterListener(new KVMetadataAdminThreadParameterListenter());
        addParameterChangeListener(new KVAdminParameterListener());

        /* Logger must be created after the registering the monitor handler */
        logger = LoggerUtils.getLogger(this.getClass(), myParams);

        /*
         * Any socket timeouts observed by the ClientSocketFactory in this
         * process will be logged to this logger.
         */
        ClientSocketFactory.setTimeoutLogger(logger);

        /*
         * TODO - The global parameter should only be set on a new store.
         * Should we somehow check whether this was an upgrade and the
         * parameter was accidentally set?
         */
        logger.info("Initializing " + myParams.getAdminParams().getType() +
                " Admin for store: " +
                myParams.getGlobalParams().getKVStoreName());
        if (created) {
            logger.info("Created new admin environment dir: " + envDir);
        }

        /*
         * maxDisk should always be set for Admin.
         *
         * If admindirsize is not specified, then check diskSize for admindir,
         * if specified, or kvroot, if admindir is not specified.  If the
         * kvroot or admindir diskSize is more than 3 GB, then set upper 3 GB
         * limit as maxDisk.
         */
        if (adminDirSize == 0L) {
            adminDirSize = (adminDirName == null) ?
                FileUtils.getDirectorySize(snParams.getRootDirPath()) :
                FileUtils.getDirectorySize(adminDirName);
            if (adminDirSize > ADMIN_DEFAULT_MAX_DISK) {
                adminDirSize = ADMIN_DEFAULT_MAX_DISK;
            }
        }

        Properties props = new ParameterUtils(adminParams.getMap()).
            createProperties(true, true, adminDirSize);

        logger.info("JVM Runtime maxMemory (bytes): " +
                    JVMSystemUtils.getRuntimeMaxMemory());
        logger.info("Non-default JE properties for environment: " + props);

        envConfig = createEnvironmentConfig(props);

        /*
         * Use the service name as the JE log file prefix so that multiple
         * services can store their log files in the same directory
         */
        envConfig.setConfigParam
            (EnvironmentConfig.FILE_LOGGING_PREFIX,
             adminId.getFullName());

        /*
         * Logging je.info, je.stat, je.config file in kvroot
         * log directory.
         */
        String dirName = myParams.getStorageNodeParams().getRootDirPath() +
                         "/" + myParams.getGlobalParams().getKVStoreName() +
                         "/log";
        envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_DIRECTORY,
                                 dirName);

        repConfig = createReplicationConfig();

        stateTracker = new AdminStateTracker(logger);
        stateTracker.start();

        maybeResetRepGroup();

        if (testEnvironment != null && testStartupStatus != null &&
            testAdminStores != null) {
            /* Non-null values are input for these arguments only when running
             * the unit tests that employ mocked instances of these objects;
             * for increasing test coverage. Note that in this case, unlike
             * the case where mocks are not used, there is no call to the
             * ReplicatedEnvironment.setStateChangeListener() method or to
             * StartupStatus.waitForIsReady(). This is because adding those
             * calls to the mocked environment very complicated and difficult
             * to navigate. Doing so requires setting a listener in the mocked
             * JE ReplicatedEnvironment; which then involves simulating/mocking
             * the threads that are created, and then waiting for notify
             * calls that can't easily be mocked. It also may require diving
             * deeper into the JE code to mock various JE classes that are
             * not currently imported by the Admin. Since this has to do with
             * the coordination of the Admin startup process, a process that
             * is already covered by previous unit tests for the Admin, it
             * is probably not worth the effort to try to simulate something
             * that is already tested by other tests.
             */
            environment = testEnvironment;
            stores = testAdminStores;
            startupStatus = testStartupStatus;
        } else {
            environment = openEnv();
            stores = new AdminStores(this);

            /*
             * Some initialization of the Admin takes place in a separate thread
             * triggered by the StateChangeListener callback.  We'll wait here
             * to be notified that the initialization is complete.
             */
            startupStatus = new StartupStatus();
            environment.setStateChangeListener(this);
            startupStatus.waitForIsReady(this);
        }

        logger.info("Replicated environment handle established." +
                    " Cache size: " + environment.getConfig().getCacheSize() +
                    ", State: " + environment.getState());

        verificationManager =
            new VerificationManager(myParams.getAdminParams().getAdminId(),
                                    logger);
        verificationManager.setEnv(getEnv());

        mrtServiceManager = new MRTManager(this);

        adminStatsTracker =
            new AdminStatsTracker(this, myParams.getGlobalParams().getMap());

        addGlobalParameterListener(adminStatsTracker.getGlobalParamsListener());
        adminStatsTracker.addListener(new AdminStatsListener());
        serviceStartTime = System.currentTimeMillis();
    }

    public Logger getLogger() {
        return logger;
    }

    /**
     * Gets the Admin's exception handler.
     *
     * @return the Admin's exception handler
     */
    public UncaughtExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    /**
     * Check whether the Admin is master or not. If the admin is not master,
     * AdminNotReadyException is thrown.
     */
    private void checkIfReadonlyAdmin() {
        if (parameters == null) {
            /* This Admin is not master when parameter is null */
            throw new AdminNotReadyException
            ("Cannot service request, admin is not the master");
        }
    }

    /**
     * Create an EnvironmentConfig specific for the Admin.
     */
    private EnvironmentConfig createEnvironmentConfig(Properties props) {
        final EnvironmentConfig config = new EnvironmentConfig(props);
        config.setAllowCreate(true);
        config.setTransactional(true);
        config.setCacheSize(ADMIN_JE_CACHE_SIZE);
        return config;
    }

    /**
     * Create a ReplicationConfig specific for the Admin.
     */
    private ReplicationConfig createReplicationConfig() {
        final AdminParams ap = myParams.getAdminParams();

        final ReplicationConfig config =
            new ParameterUtils(ap.getMap()).getAdminRepEnvConfig();

        config.setGroupName
           (getAdminRepGroupName(myParams.getGlobalParams().getKVStoreName()));

        config.setNodeName(getAdminRepNodeName(ap.getAdminId()));
        config.setNodeType(getNodeType(ap.getType()));
        config.setElectableGroupSizeOverride(
            ap.getElectableGroupSizeOverride());
        config.setLogFileRewriteListener
            (new AdminLogRewriteListener(snapshotDir, myParams));

        if (myParams.getSecurityParams() != null) {
            final Properties haProps =
                myParams.getSecurityParams().getJEHAProperties();
            logger.info("DataChannelFactory: " +
                        haProps.getProperty(
                            ReplicationNetworkConfig.CHANNEL_TYPE));
            config.setRepNetConfig(ReplicationNetworkConfig.create(haProps));
        }

        /**
         * Reduce the message queue size, to ensure that the Admin can operate
         * in a small heap.
         */
        config.setConfigParam(RepParams.REPLICA_MESSAGE_QUEUE_SIZE.getName(),
                              "10");

        return config;
    }

    /*
     * Converts the specified Admin type into a JE replication node type.
     */
    private NodeType getNodeType(AdminType type) {
        switch (type) {
        case PRIMARY : return NodeType.ELECTABLE;
        case SECONDARY : return NodeType.SECONDARY;
        }
        throw new IllegalStateException("Unknown Admin type: " + type);
    }

    ReplicationNetworkConfig getRepNetConfig() {
        return repConfig.getRepNetConfig();
    }

    public static String getAdminRepGroupName(String kvstoreName) {
        return kvstoreName + "Admin";
    }

    public static String getAdminRepNodeName(AdminId adminId) {
        return (Integer.toString(adminId.getAdminInstanceId()));
    }

    /** Reset the JE replication group, if requested. */
    private void maybeResetRepGroup() {
        final AdminParams adminParams = myParams.getAdminParams();
        if (!adminParams.getResetRepGroup()) {
            return;
        }

        logger.info("Resetting replication group");
        final EnvironmentConfig resetEnvConfig = envConfig.clone();
        final ReplicationConfig resetRepConfig = repConfig.clone();

        /*
         * Make the reset retain the current rep group UUID so that existing
         * nodes can rejoin the group when they restart
         */
        resetRepConfig.setConfigParam(
            RepParams.RESET_REP_GROUP_RETAIN_UUID.getName(), "true");

        DatabaseUtils.resetRepGroup(envDir, resetEnvConfig, resetRepConfig);
    }

    /**
     * Returns a replicated environment handle, dealing with any recoverable
     * exceptions in the process.
     *
     * @return the replicated environment handle. The handle may be in the
     * Master, Replica, or Unknown state.
     */
    private ReplicatedEnvironment openEnv() {

        final boolean networkRestoreDone = false;

        /*
         * Plumb JE environment logging and progress listening output to
         * KVStore monitoring.
         */
        envConfig.setLoggingHandler(new AdminRedirectHandler(myParams));
        envConfig.setRecoveryProgressListener
            (new AdminRecoveryListener(myParams));
        repConfig.setSyncupProgressListener(new AdminSyncupListener(myParams));

        while (true) {
            try {
                return new ReplicatedEnvironment(envDir, repConfig, envConfig);
            } catch (InsufficientLogException ile) {
                if (networkRestoreDone) {

                    /*
                     * Should have made progress after the earlier network
                     * restore, propagate the exception to the caller so it
                     * can be logged and propagated back to the client.
                     */
                    throw ile;
                }
                final NetworkRestore networkRestore = new NetworkRestore();
                final NetworkRestoreConfig config = new NetworkRestoreConfig();
                config.setLogProviders(null);
                networkRestore.execute(ile, config);
                continue;
            } catch (RollbackException rbe) {
                final Long time = rbe.getEarliestTransactionCommitTime();
                logger.info("Rollback exception retrying: " +
                            rbe.getMessage() +
                            ((time ==  null) ?
                             "" :
                             " Rolling back to: " + new Date(time)));
                continue;
            } catch (Throwable t) {
                final String msg = "unexpected exception creating environment";
                logger.log(Level.WARNING, msg, t);
                throw new IllegalStateException(msg, t);
            }
        }
    }

    @Override
    public String toString() {
        return "Admin " + adminId;
    }

    /**
     * A custom Handler for JE log handling.  This class provides a unique
     * class scope for the purpose of logger creation.
     */
    private static class AdminRedirectHandler extends RedirectHandler {
        AdminRedirectHandler(AdminServiceParams adminServiceParams) {
            super(LoggerUtils.getLogger(AdminRedirectHandler.class,
                                        adminServiceParams));
        }
    }

    /**
     * A custom Handler for JE recovery recovery notification.  This class
     * exists only to provide a unique class scope for the purpose of logger
     * creation.
     */
    private static class AdminRecoveryListener extends RecoveryListener {
        AdminRecoveryListener(AdminServiceParams adminServiceParams) {
            super(LoggerUtils.getLogger(AdminRecoveryListener.class,
                                        adminServiceParams));
        }
    }

    /**
     * A custom Handler for JE syncup progress notification.  This class
     * provides a unique class scope for the purpose of logger creation.
     */
    private static class AdminSyncupListener extends SyncupListener {
        AdminSyncupListener(AdminServiceParams adminServiceParams) {
            super(LoggerUtils.getLogger(AdminSyncupListener.class,
                                        adminServiceParams));
        }
    }

    /**
     * A custom Handler for JE log rewrite notification.  This class provides
     * a unique class scope for the purpose of logger creation.
     */
    private static class AdminLogRewriteListener extends LogRewriteListener {
        AdminLogRewriteListener(File snapshotDir,
                                AdminServiceParams adminServiceParams) {
            super(snapshotDir,
                  LoggerUtils.getLogger(AdminLogRewriteListener.class,
                                        adminServiceParams));
        }
    }

    /**
     * Initialize the Admin database.  If it already exists, read its contents
     * into the in-memory caches.  If not, create the structures that it needs.
     *
     * TODO: we need to check for the situation where an environment exists,
     * and the parameters passed in to the admin do not match those that are
     * already stored, which means that the config.xml is not consistent with
     * the database. This could happen if there is a failure during:
     *  - a change to admin params (which is not yet implemented)
     *  - there is a failure when the admin is deployed.
     * The database should be considered the point of authority, which means
     * that changing params needs to be carefully orchestrated with changes
     * to the database.

     * Open may be called multiple times on the same environment.  It should be
     * idempotent.  It is called from the StateChangeListener when the
     * environment changes state.
     */
    private void open() {
        assert Thread.holdsLock(this);

        new AdminTransaction<Void>(RunTransaction.sync) {

            @Override
            protected Void doTransaction(Transaction txn) {

                final Topology topology = stores.getTopology(txn);
                parameters = readParameters(txn);
                memo = readMemo(txn);

                if (topology == null || parameters == null || memo == null) {
                    if (!(topology == null &&
                          parameters == null &&
                          memo == null)) {
                        throw new IllegalStateException
                            ("Inconsistency in Admin database: " +
                             "One of Topology, Parameters, or Memo is missing");
                    }

                    /* We are populating the database for the first time. */
                    logger.info("Initializing Admin database");

                    final String storeName =
                        myParams.getGlobalParams().getKVStoreName();
                    stores.putTopology(txn, new RealizedTopology(storeName));

                    logger.fine("Creating Parameters");

                    /* Seed new parameters with info from myParams. */
                    parameters = new Parameters(storeName);
                    updateAndNotify(myParams.getGlobalParams());
                    stores.putParameters(txn, parameters);

                    logger.fine("Creating Memo");
                    memo = new Memo();
                    stores.putMemo(txn, memo);

                    logger.info("Admin database initialized");

                } else {
                    logger.info("Using existing Admin database");

                    // TODO: add verification that the params passed into the
                    // constructor match what is in the database.
                }
                return null;
            }
        }.run();

        /*
         * Now that the persistent environment is established, we can start
         * recording events.  Until now, any recordable events that occurred
         * would be buffered in the Trackers.  Now we can pull them out, record
         * them, and allow the Trackers to prune them.
         */
        eventRecorder.start(memo.getLatestEventTimestamps());

        planner = new Planner(this, myParams, getNextId());
    }

    /*
     * The StopAdmin task needs a way to shut down the whole service, hence
     * this entry point.
     */
    public void stopAdminService(boolean force, String reason) {
        if (owner != null) {
            /* Eventually calls shutdown(force) */
            owner.stop(force, reason);
        } else {
            /* If there is no service, just treat this as shutdown. */
            shutdown(force, reason);
        }
    }

    /**
     * The force boolean means don't bother to cleanly shutdown JE.
     */
    public void shutdown(boolean force, String reason) {
        assert TestHookExecute.doHookIfSet(beginShutdownHook, this);
        synchronized (closingLock) {

            /* Done if already closing */
            if (closing) {
                return;
            }

            /* Note closing and notify enterMode waiters */
            closing = true;
            closingLock.notifyAll();

            /* Wait for any current enterMode call to complete */
            while (enteringMode) {
                try {
                    closingLock.wait();
                } catch (InterruptedException e) {

                    /* Done if interrupted */
                    return;
                }
            }
        }
        adminStatsTracker.pushStats();
        adminStatsTracker.close();
        shutdownThreads(force);
        shutdownInternal(force, reason);
    }

    /**
     * Shuts downs Admin threads without holding the Admin monitor. This
     * prevents deadlock as thread may call back to the Admin during shutdown.
     */
    private void shutdownThreads(boolean force) {
        assert !Thread.holdsLock(this) ||
               (Thread.holdsLock(this) && Thread.holdsLock(threadLock));
        assert closing || enteringMode;

        synchronized (threadLock) {
            shutdownSysTableMonitor();
            eventRecorder.shutdown();
            monitor.shutdown();
            removeParameterListener(monitor);

            /* Shutdown planner. If closing, wait for plans to finish */
            if (planner != null) {
                planner.shutdown(force, closing);
                planner = null;
            }
            shutdownKVMetadataAdminThread();
            shutdownVersionUpdaterThread(force);

            if (closing) {
                stateTracker.shutdown();

                if (upgradeMonitor != null) {
                    upgradeMonitor.shutdownThread(logger);
                }
                mrtServiceManager.shutdown();

                final KVStore store = kvstore;
                if (store != null) {
                    store.close();
                }
            }
        }
    }

    /* Internal method to perform shutdown while synchronized. */
    private synchronized void shutdownInternal(boolean force, String reason) {

        if (force) {
            try {
                stores.close();
            } catch (Exception possible) /* CHECKSTYLE:OFF */ {
                /*
                 * Ignore exceptions that come from a forced close of the
                 * environment.
                 */
            }/* CHECKSTYLE:ON */
            /* Use an internal interface to close without checkpointing */
            final EnvironmentImpl envImpl =
                DbInternal.getEnvironmentImpl(environment);
            if (envImpl != null) {
                envImpl.close(false);
            }
        } else {
            stores.close();
            environment.close();
        }

        /*
         * Update STOPPED status before closing the loggers so that the
         * status change gets logged.
         */
        if (owner != null) {
            owner.updateStatusTracker(ServiceStatus.STOPPED, reason);
        }

        /* Release all files. */
        if ((owner == null) || !owner.getUsingThreads()) {
            LoggerUtils.closeHandlers
                (myParams.getGlobalParams().getKVStoreName());
        }
    }

    /**
     * Notification that there are modified service parameters.
     */
    public void newParameters() {
        assert !Thread.holdsLock(this);
        synchronized (threadLock) {
            if (closing) {
                return;
            }
            final ParameterMap oldMap = myParams.getAdminParams().getMap();
            ParameterMap newMap = null;
            StorageNodeParams snp = myParams.getStorageNodeParams();
            GlobalParams gp = myParams.getGlobalParams();
            String serviceName = null;
            try {
                final StorageNodeId snid1 = snp.getStorageNodeId();

                serviceName = RegistryUtils.bindingName(
                    gp.getKVStoreName(), snid1.getFullName(),
                    RegistryUtils.InterfaceType.MAIN);

                StorageNodeAgentAPI snapi =
                    RegistryUtils.getStorageNodeAgent(snp.getHostname(),
                                                      snp.getRegistryPort(),
                                                      serviceName,
                                                      getLoginManager(),
                                                      logger);
                LoadParameters lp = snapi.getParams();
                newMap = lp.getMap(oldMap.getName(), oldMap.getType());
            } catch (NotBoundException | RemoteException e) {
                /*
                 * Ignore and read directly from file.
                 */
            }
            if (newMap == null) {
                newMap =
                    ConfigUtils.getAdminMap(adminId,
                                            myParams.getStorageNodeParams(),
                                            myParams.getGlobalParams(),
                                            logger);
            }

            /* Do nothing if maps are the same */
            if (oldMap.equals(newMap)) {
                return;
            }
            myParams.setAdminParams(new AdminParams(newMap));

            /* Change for JE parameters */
            ParameterUtils.setJEMutableParams(
                oldMap, newMap, environment, logger);

            parameterTracker.notifyListeners(oldMap, newMap);
        }
    }

    /**
     * Notification that there are modified global parameters
     */
    public void newGlobalParameters() {
        assert !Thread.holdsLock(this);
        synchronized (threadLock) {
            if (closing) {
                return;
            }
            ParameterMap oldMap = myParams.getGlobalParams().getMap();
            ParameterMap newMap =
            ConfigUtils.getGlobalMap(myParams.getStorageNodeParams(),
                                              myParams.getGlobalParams(),
                                              logger);

            /* Do nothing if maps are the same */
            if (oldMap.equals(newMap)) {
                logger.info("newGlobalParameters are identical to" +
                            " old global parameters");
                return;
            }
            logger.info("newGlobalParameters: refreshing global parameters");
            myParams.setGlobalParams(new GlobalParams(newMap));
            globalParameterTracker.notifyListeners(oldMap, newMap);
        }
    }

    /**
     * Notification that there is new security metadata change.
     */
    public void newSecurityMDChange() {
        final SecurityMetadata secMd = getMetadata(SecurityMetadata.class,
                                                   MetadataType.SECURITY);
        if (secMd == null) {
            throw new IllegalStateException(
                "newSecurityMDChange: no security metadata");
        }
        securityMDTracker.notifyListeners(secMd.getLatestChange());
        logger.info("newSecurityMDChange: update with the " +
                    "latest security metadata change");
    }

    public void addParameterListener(ParameterListener pl) {
        parameterTracker.addListener(pl);
    }

    public void removeParameterListener(ParameterListener pl) {
        parameterTracker.removeListener(pl);
    }

    private void addGlobalParameterListener(ParameterListener pl) {
        globalParameterTracker.addListener(pl);
    }

    private void addSecurityMDListener(SecurityMDListener secMdListener) {
        securityMDTracker.addListener(secMdListener);
    }

    private void addParameterChangeListener(SNParameterChangeListener cl) {
        parameterChangeTracker.addListener(cl);
    }

    public ReplicatedEnvironment getEnv() {
        return environment;
    }

    /**
     * Returns a clone of the current Topology.
     *
     * The concurrency protocol for Topology is that we always give out copies.
     * Updates to the One True Topology happen in the course of Plan execution
     * via savePlanResults, in which a new Topology replaces the old one.  The
     * Planner bears the responsibility for serializing such updates.
     */
    public Topology getCurrentTopology() {
        return new AdminTransaction<Topology>(RunTransaction.readOnly) {

            @Override
            protected Topology doTransaction(Transaction txn) {
                return stores.getTopology(txn);
            }
        }.run();
    }

    /**
     * The concurrency protocol for Parameters is that we give out read-only
     * copies.  Updates happen only through methods on the Admin instance.
     *
     * Updates to parameters objects that occur in the course of plan execution
     * happen in the various flavors of the savePlanResults method.  These
     * updates are serialized with respect to each other by the Planner.
     *
     * Parameters updates that are not controlled by the Planner include
     * changes to StorageNodePools, which occur directly through the public
     * Admin interface, in response to operator actions.  The Planner requires
     * that the contents of a StorageNodePool remain unchanged during the
     * course of plan execution that involves the pool.  This constraint is
     * enforced by a read/write lock on each StorageNodePool object.
     */
    public Parameters getCurrentParameters() {
        return new AdminTransaction<Parameters>(RunTransaction.readOnly) {

            @Override
            protected Parameters doTransaction(Transaction txn) {
                return readParameters(txn);
            }
        }.run();
    }

    public AdminServiceParams getParams() {
        return myParams;
    }

    /**
     * Saves this value for use as the next plan id. This value is retrieved
     * from the database and given to the Planner at construction.
     */
    public void saveNextId(int nextId) {
        memo.setPlanId(nextId);
        logger.log(Level.FINE, "Storing Memo, planId = {0}", nextId);

        new AdminTransaction<Void>(RunTransaction.sync) {

            @Override
            protected Void doTransaction(Transaction txn) {
                stores.putMemo(txn, memo);
                return null;
            }
        }.run();
    }

    public int getNextId() {
        if (memo == null) {
            return 0;
        }
        return memo.getPlanId();
    }

    /**
     * A convenience for the methods that all save the plan using CAUSE_CREATE
     * and return the id.  This is public to allow Planner to use it.
     */
    public int saveCreatePlan(final Plan plan) {
        savePlan(plan, CAUSE_CREATE);
        return plan.getId();
    }

    /**
     * Persists the plan.  It may or may not be finished. Plans can be saved
     * concurrently by concurrently executing tasks, so this method requires
     * synchronization.
     */
    public void savePlan(final Plan plan, String cause) {

        logger.log(Level.FINE, "Saving {0} {1}", new Object[]{plan, cause});

        /*
         * Assert that this plan was either terminal, or in the Planner's
         * cache, before storing it.
         */
        if (getPlanner().getCachedPlan(plan.getId()) == null &&
            !plan.getState().isTerminal()) {
            throw new IllegalStateException
                ("Attempting to save plan that is not in the Planner's " +
                 "cache " + plan);
        }

        new AdminTransaction<Void>(RunTransaction.sync) {

            @Override
            protected Void doTransaction(Transaction txn) {
                synchronized(plan) {
                    stores.putPlan(txn, plan);
                }
                return null;
            }
        }.run();
    }

    /**
     * Notes on topology saving methods.
     * 1.  all new topologies must have version numbers greater than the current
     * topology.
     * 2.  all updates to Topology must be transactional and atomic.  The means
     * that updating methods must be synchronized and database updates are
     * performed in a single transaction.
     */

    /**
     * Verifies that the version of the new Topology is higher than that of
     * the current Topology. The topology version should ascend with every save.
     * If it does not, something unexpected is happening.
     */
    private void checkTopoVersion(Topology newTopo) {
        final Topology current = getCurrentTopology();
        if (newTopo.getSequenceNumber() <= current.getSequenceNumber()) {
            throw new IllegalStateException
                ("Only save newer topologies. Current version=" +
                 current.getSequenceNumber() + " new version=" +
                 newTopo.getSequenceNumber());
        }
    }

    /**
     * Persists a new topology after an elasticity plan runs.
     * See note above regarding synchronization.
     */
    public synchronized void saveTopo(Topology topology,
                                      DeploymentInfo info,
                                      Plan plan) {
        checkTopoVersion(topology);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * Persists a new topology and ArbNode params after deploying a single ARB.
     * See note above regarding synchronization.
     */
    public synchronized
        void saveTopoAndARBParam(Topology topology,
                                 DeploymentInfo info,
                                 ArbNodeParams anp,
                                 Plan plan) {
        checkIfReadonlyAdmin();
        checkTopoVersion(topology);
        parameters.add(anp);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * Persists a new topology and RepNode params after deploying a single RN.
     * See note above regarding synchronization.
     */
    public synchronized void saveTopoAndRNParam(Topology topology,
                                                DeploymentInfo info,
                                                RepNodeParams rnp,
                                                Plan plan) {
        checkIfReadonlyAdmin();
        checkTopoVersion(topology);
        parameters.add(rnp);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * Persists a new topology after a DeployDatacenterPlan runs.
     * See note above regarding synchronization.
     */
    public synchronized void saveTopoAndParams(Topology topology,
                                               DeploymentInfo info,
                                               DatacenterParams params,
                                               Plan plan) {
        checkIfReadonlyAdmin();
        checkTopoVersion(topology);
        parameters.add(params);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * Persists a new topology and params after a DeploySNPlan runs.
     * See note above regarding synchronization.
     */
    public synchronized void saveTopoAndParams(Topology topology,
                                               DeploymentInfo info,
                                               StorageNodeParams snp,
                                               GlobalParams gp,
                                               Plan plan) {
        checkIfReadonlyAdmin();
        checkTopoVersion(topology);
        parameters.add(snp);
        if (gp != null) {
            updateAndNotify(gp);
        }

        /* Add the new SN to the pool of all storage nodes */
        final StorageNodePool pool =
            parameters.getStorageNodePool(Parameters.DEFAULT_POOL_NAME);
        pool.add(snp.getStorageNodeId());

        storeTopoAndParams(topology, info, plan);
    }

    /**
     * Persists a new topology and removes an SN from the params and storage
     * node pools, as one atomic action.   Because our policy is to restrict
     * modifying the Parameters instance to the Admin, we can only supply
     * a StorageNodeId and ask the Admin to do the work.
     *
     * See note above regarding synchronization.
     */
    public synchronized void saveTopoAndRemoveSN(Topology topology,
                                                 DeploymentInfo info,
                                                 StorageNodeId target,
                                                 Plan plan) {
        checkIfReadonlyAdmin();
        checkTopoVersion(topology);

        /* Remove the snId from the params and all pools. */
        parameters.remove(target);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * Persists a new topology and removes the specified RepNode from params.
     * See note above regarding synchronization.
     */
    public synchronized void saveTopoAndRemoveRN(Topology topology,
                                                 DeploymentInfo info,
                                                 RepNodeId targetRN,
                                                 Plan plan) {
        checkIfReadonlyAdmin();
        checkTopoVersion(topology);

        /* Remove the rnId from the params and all pools. */
        RepNodeParams rnp = parameters.remove(targetRN);
        storeTopoAndParams(topology, info, plan);
        parameterChangeTracker.notifyListeners(rnp.getStorageNodeId());
    }


    /**
     * Persists a new topology and removes the specified ArbNode from params.
     * See note above regarding synchronization.
     */
    public synchronized void saveTopoAndRemoveAN(Topology topology,
                                                 DeploymentInfo info,
                                                 ArbNodeId targetAN,
                                                 Plan plan) {
        checkIfReadonlyAdmin();
        checkTopoVersion(topology);

        /* Remove the anId from the params and all pools. */
        parameters.remove(targetAN);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * Persists a new topology and params after RemoveDatacenterPlan runs.
     * See note above regarding synchronization.
     */
    public synchronized void saveTopoAndRemoveDatacenter(Topology topology,
                                                         DeploymentInfo info,
                                                         DatacenterId targetId,
                                                         Plan plan) {
        checkIfReadonlyAdmin();
        checkTopoVersion(topology);

        /* Remove the targetId from the params. */
        parameters.remove(targetId);
        storeTopoAndParams(topology, info, plan);
    }

    /**
     * Persists a new topology and params after a MigrateStorageNode plan runs.
     * See note above regarding synchronization.
     */
    public synchronized void saveTopoAndParams(Topology topology,
                                               DeploymentInfo info,
                                               Set<RepNodeParams> repNodeParams,
                                               Set<AdminParams> adminParams,
                                               Set<ArbNodeParams> arbNodeParams,
                                               Plan plan) {
        checkIfReadonlyAdmin();
        checkTopoVersion(topology);

        /*
         * Use parameters.update rather than add, because we may be
         * re-executing this upon plan retry.
         */
        for (RepNodeParams rnParams : repNodeParams) {
            updateAndNotify(rnParams);
        }

        for (AdminParams ap : adminParams) {
            parameters.update(ap);
        }

        for (ArbNodeParams arbParams : arbNodeParams) {
            updateAndNotify(arbParams);
        }

        storeTopoAndParams(topology, info, plan);
    }

    /**
     * Persists a new set of params atomically after a MigrateStorageNode plan
     * runs.
     */
    public synchronized void saveParams(Set<RepNodeParams> repNodeParams,
                                        Set<AdminParams> adminParams,
                                        Set<ArbNodeParams> arbNodeParams) {
        checkIfReadonlyAdmin();

        /*
         * Use parameters.update rather than add, because we may be
         * re-executing this upon plan retry.
         */
        for (RepNodeParams rnParams : repNodeParams) {
            updateAndNotify(rnParams);
        }

        for (AdminParams ap : adminParams) {
            parameters.update(ap);
        }

        for (ArbNodeParams arbParams : arbNodeParams) {
            updateAndNotify(arbParams);
        }

        storeParameters();
    }

    /**
     * Updates the current topology stored within the AdminDB to reflect
     * the migration of the specified partition to a new shard. Done atomically
     * within a single transaction because partition migration can be executed
     * in parallel.
     *
     * If the topology is updated true is returned, otherwise false is returned.
     */
    public synchronized boolean updatePartition(final PartitionId partitionId,
                                                final RepGroupId targetRGId,
                                                final DeploymentInfo info,
                                                final Plan plan) {
        return new AdminTransaction<Boolean>(RunTransaction.sync) {

            @Override
            protected Boolean doTransaction(Transaction txn) {
                /*
                 * Be sure to take the plan mutex before any JE locks are
                 * acquired in this transaction, per the synchronization
                 * hierarchy rules.
                 */
                Topology t = null;
                synchronized (plan) {
                    t = stores.getTopology(txn);

                    /*
                     * If the partition is already at the target, there is no
                     * need to update the topology.
                     */
                    if (t.get(partitionId).getRepGroupId().equals(targetRGId)){
                        return false;
                    }

                    t.updatePartition(partitionId, targetRGId);

                    /*
                     * Now that the topo has been modified, inform the plan that
                     * the topology is being updated. Save the plan if needed.
                     */
                    if (plan.updatingMetadata(t)) {
                        stores.putPlan(txn, plan);
                    }
                }

                stores.putTopology(txn, new RealizedTopology(t, info));
                return true;
            }
        }.run();
    }

    /**
     * Persists new RepNodeParams following a ChangeParamsPlan.
     */
    public synchronized void updateParams(RepNodeParams rnp) {
        checkIfReadonlyAdmin();
        updateAndNotify(rnp);
        storeParameters();
    }


    /**
     * Persists new ArbNodeParams following a ChangeParamsPlan.
     */
    public synchronized void updateParams(ArbNodeParams anp) {
        checkIfReadonlyAdmin();
        updateAndNotify(anp);
        storeParameters();
    }

    /**
     * Persists new params following successful deployment of a Storage Node.
     */
    public synchronized void updateParams(StorageNodeParams snp,
                                          GlobalParams gp) {
        checkIfReadonlyAdmin();
        updateAndNotify(snp);
        if (gp != null) {
            updateAndNotify(gp);
        }
        storeParameters();
    }

    /**
     * Persists updated AdminParams after a successful deployment.
     */
    public synchronized void updateParams(AdminParams ap) {
        checkIfReadonlyAdmin();
        parameters.update(ap);
        storeParameters();
    }

    /**
     * Persists new GlobalParams following a ChangeGlobalSecurityPlan.
     */
    public synchronized void updateParams(GlobalParams gp) {
        checkIfReadonlyAdmin();
        updateAndNotify(gp);
        storeParameters();
    }

    /**
     * Persists new AdminParams following successful deployment of a new
     * Admin.  The admin must not yet exist in the params.
     */
    public synchronized void addAdminParams(AdminParams ap) {
        checkIfReadonlyAdmin();
        final int id = ap.getAdminId().getAdminInstanceId();

        logger.log(Level.FINE, "Saving new AdminParams[{0}]", id);

        final int nAdmins = getAdminCount();
        if (id <= nAdmins) {
            throw new NonfatalAssertionException
                ("Attempting to add an AdminParams " +
                 "for an existing Admin. Id=" + id + " nAdmins=" + nAdmins);
        }

        parameters.add(ap);
        storeParameters();
    }

    /**
     * Removes the parameters associated with the specified AdminId.
     */
    public synchronized void removeAdminParams(AdminId aid) {

        checkIfReadonlyAdmin();
        logger.log(Level.FINE, "Removing AdminParams[{0}]", aid);
        if (parameters.get(aid) == null) {
            throw new MemberNotFoundException
                ("Removing nonexistent params for admin " + aid);
        }

        final int nAdmins = getAdminCount();
        if (nAdmins == 1) {
            throw new NonfatalAssertionException
                ("Attempting to remove the sole Admin instance" + aid);
        }

        parameters.remove(aid);
        storeParameters();
    }

    /**
     * Removes a RepNode and save changed topology.Done atomically  within a
     * single transaction because removing RepNode can be executed in parallel.
     *
     * If the topology is updated true is returned, otherwise false is returned.
     */
    public synchronized boolean removeRepNodeAndSaveTopo(
                                            final RepNodeId target,
                                            final DeploymentInfo info,
                                            final Plan plan) {
        checkIfReadonlyAdmin();
        return new AdminTransaction<Boolean>(RunTransaction.sync) {

            @Override
            protected Boolean doTransaction(Transaction txn) {
                /*
                 * Be sure to take the plan mutex before any JE locks are
                 * acquired in this transaction, per the synchronization
                 * hierarchy rules.
                 */
                Topology topo = null;
                synchronized (plan) {
                    topo = stores.getTopology(txn);

                    try {
                        /* Remove RepNode from topology. */
                        topo.remove(target);
                    } catch (IllegalArgumentException mnfe) {
                        /*
                         * This would happen if the plan was interrupted and
                         * re-executed.
                         */
                        logger.fine("The RN " + target +
                                    " was not found in the repgroup.");
                    }

                    try {
                        /* Remove relevant parameters */
                        RepNodeParams rnp = parameters.remove(target);
                        storeParameters();
                        parameterChangeTracker.notifyListeners(
                            rnp.getStorageNodeId());
                    } catch (NonfatalAssertionException nae) {
                        /*
                         * This would happen if the plan was interrupted and
                         * re-executed.
                         */
                        logger.fine("Attempt to remove a nonexistent " +
                                    "RepNodesParams for RepNode " + target);
                    }

                    /*
                     * Now that the topo has been modified, inform the plan that
                     * the topology is being updated. Save the plan if needed.
                     */
                    if (plan.updatingMetadata(topo)) {
                        stores.putPlan(txn, plan);
                    }
                }

                stores.putTopology(txn, new RealizedTopology(topo, info));
                return true;
            }
        }.run();
    }

    /**
     * Adds a new storage node pool using the specified name.
     */
    public synchronized void addStorageNodePool(String name) {
        checkIfReadonlyAdmin();
        if (parameters.getStorageNodePool(name) != null) {
            return;
        }

        parameters.addStorageNodePool(name);
        logger.info("Created Storage Node Pool: " + name);
        storeParameters();
    }

    /**
     * Clone an existing storage node pool as a new storage node pool.
     *
     * @param name the name of the new storage node pool
     * @param source the name of the source storage node pool
     */
    synchronized void cloneStorageNodePool(String name, String source) {
        checkIfReadonlyAdmin();
        if (parameters.getStorageNodePool(name) != null) {
            throw new IllegalCommandException
            ("Target already exists: " + name, ErrorMessage.NOSQL_5200,
             CommandResult.NO_CLEANUP_JOBS);
        }

        final StorageNodePool pool = parameters.addStorageNodePool(name);
        final StorageNodePool sourcePool =
                parameters.getStorageNodePool(source);
        if (sourcePool == null) {
            throw new IllegalCommandException("No such Storage Node Pool: "  +
                                              source, ErrorMessage.NOSQL_5200,
                                              CommandResult.NO_CLEANUP_JOBS);
        }

        for (StorageNodeId snId : sourcePool) {
            pool.add(snId);
        }

        logger.info("Cloned Storage Node Pool " + source + " as: " + name);
        storeParameters();
    }

    /**
     * Removes the named storage node pool.  It must exist.
     */
    public synchronized void removeStorageNodePool(String name) {
        checkIfReadonlyAdmin();
        if (parameters.getStorageNodePool(name) == null) {
            throw new IllegalCommandException
                ("Attempt to remove a nonexistent StorageNodePool.");
        }

        parameters.removeStorageNodePool(name);
        logger.info("Removed Storage Node Pool: " + name);
        storeParameters();
    }

    /**
     * Adds the specified storage node to the named pool.  Both must exist.
     */
    public synchronized void addStorageNodeToPool(String name,
                                                  StorageNodeId snId) {
        checkIfReadonlyAdmin();
        final StorageNodePool pool = parameters.getStorageNodePool(name);
        if (pool == null) {
            throw new IllegalCommandException("No such Storage Node Pool: " +
                                              name, ErrorMessage.NOSQL_5200,
                                              CommandResult.NO_CLEANUP_JOBS);
        }

        /* Verify that the storage node exists. */
        final Topology current = getCurrentTopology();
        final StorageNode sn = current.get(snId);
        if (sn == null) {
            throw new IllegalCommandException
                ("Attempt to add nonexistent StorageNode " + snId +
                 " to a pool.",
                 ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
        }

        logger.info("Added Storage Node " + snId.toString() + " to pool: " +
                    name);
        pool.add(snId);
        storeParameters();
    }

    /**
     * Removes the specified storage node from the named pool. Both must exist.
     */
    synchronized void removeStorageNodeFromPool(String name,
                                                StorageNodeId snId) {
        checkIfReadonlyAdmin();
        final StorageNodePool pool = parameters.getStorageNodePool(name);
        if (pool == null) {
            throw new IllegalCommandException("No such Storage Node Pool: " +
                                              name, ErrorMessage.NOSQL_5200,
                                              CommandResult.NO_CLEANUP_JOBS);
        }

        /* Verify that the storage node exists. */
        final Topology current = getCurrentTopology();
        final StorageNode sn = current.get(snId);
        if (sn == null) {
            throw new IllegalCommandException
                ("Attempt to remove nonexistent StorageNode " + snId +
                 " to a pool.",
                 ErrorMessage.NOSQL_5200, CommandResult.NO_CLEANUP_JOBS);
        }

        logger.info("Removed Storage Node " + snId.toString() + " to pool: " +
                    name);
        pool.remove(snId);
        storeParameters();
    }

    /**
     * Replaces all of the storage nodes in the named pool with the storage
     * nodes specified in the list of StorageNodeIds.  The pool, and all
     * referenced storage nodes, must exist.
     */
    public synchronized void replaceStorageNodePool(String name,
                                                    List<StorageNodeId> ids) {
        checkIfReadonlyAdmin();
        final StorageNodePool pool = parameters.getStorageNodePool(name);
        if (pool == null) {
            throw new IllegalCommandException("No such Storage Node Pool: " +
                                              name);
        }

        final Topology current = getCurrentTopology();
        for (StorageNodeId id : ids) {
            /* Verify that the storage node exists. */
            final StorageNode sn = current.get(id);
            if (sn == null) {
                throw new IllegalCommandException
                    ("Attempt to add nonexistent StorageNode to a pool.");
            }
        }

        pool.clear();

        for (StorageNodeId id : ids) {
            pool.add(id);
        }

        storeParameters();
    }

    /**
     * Create a storage node pool that contains all of the SNs in the current
     * topology.
     *
     * @param topology the current topology
     * @param targetSNPool the target storage node pool
     */
    synchronized StorageNodePool createTargetStorageNodePool(
                                        final Topology topology,
                                        final String targetSNPool) {
        checkIfReadonlyAdmin();
        StorageNodePool pool = parameters.addStorageNodePool(targetSNPool);
        List<StorageNodeId> snIds = topology.getStorageNodeIds();
        for (StorageNodeId snId : snIds) {
            pool.add(snId);
        }

        logger.info("Created Target Storage Node Pool");
        storeParameters();
        return pool;
    }

    /**
     * Adds a named topology candidate that has been created by a copy
     * operation. Relies on the transaction to avoid duplicate inserts.
     *
     * @param candidateName the name of the new candidate
     * @param newTopo the topology to copy
     * @throws IllegalCommandException if an existing topology candidate is
     * found with the specified name
     * @throws DatabaseException if there is a problem modifying the database
     */
    public void addTopoCandidate(final String candidateName,
                                 final Topology newTopo) {

        new AdminTransaction<Void>(RunTransaction.writeNoSync) {

            @Override
            protected Void doTransaction(Transaction txn) {
                if (stores.getTopologyStore().candidateExists(txn,
                                                              candidateName)) {
                    throw new IllegalCommandException
                        (candidateName +
                         " already exists and can't be added as a new" +
                         " topology candidate.");
                }
                stores.getTopologyStore().
                        putCandidate(txn,
                                 new TopologyCandidate(candidateName, newTopo));
                return null;
            }
        }.run();
    }

    /**
     * Adds a named topology candidate that has been created by a copy of
     * an existing candidate. Relies on the transaction to avoid duplicate
     * inserts.
     */
    void addTopoCandidate(final String candidateName,
                          final String sourceCandidateName) {
        final Topology source =
            getCandidate(sourceCandidateName).getTopology();
        addTopoCandidate(candidateName, source);
    }

    /**
     * Deletes a topology candidate.
     *
     * @param candidateName the name of the candidate to delete
     * @throws IllegalCommandException if @param name is not associated with
     * a topology.
     * @throws DatabaseException if there is a problem with the deletion or
     * if a foreign key constraint is violated because the candidate is
     * referenced by a deployed topology. TODO: how to capture that case, and
     * present the error differently?
     */
    public void deleteTopoCandidate(final String candidateName) {

        new AdminTransaction<Void>(RunTransaction.writeNoSync) {

            @Override
            protected Void doTransaction(Transaction txn) {
                final TopologyStore topoStore = stores.getTopologyStore();
                if (!topoStore.candidateExists(txn, candidateName)) {
                      throw new IllegalCommandException
                          (candidateName + " doesn't exist");
                }
                topoStore.deleteCandidate(txn, candidateName);
                return null;
            }
        }.run();
    }

    /**
     * Create a new, initial layout, based on the SNs available in the SN pool.
     * @return a String with a status message suitable for the CLI.
     */
    public String createTopoCandidate(final String topologyName,
                                      final String snPoolName,
                                      final int numPartitions,
                                      final boolean json,
                                      final short jsonVersion) {
        checkIfReadonlyAdmin();
        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {
                final StorageNodePool pool =
                    parameters.getStorageNodePool(snPoolName);
                if (pool == null) {
                    throw new IllegalCommandException
                        ("Storage Node Pool " + snPoolName + " not found.");
                }

                Parameters currentParams = getCurrentParameters();
                TopologyCandidate candidate = buildCandidate(txn,
                                                             topologyName,
                                                             pool,
                                                             numPartitions,
                                                             currentParams);
                if (json) {
                    if (jsonVersion ==
                        SerialVersion.ADMIN_CLI_JSON_V1_VERSION) {
                        return candidate.displayAsJson(currentParams);
                    }
                    return TopologyPrinter.printTopologyJson(
                        candidate.getTopology(), currentParams,
                        TopologyPrinter.all, false).toString();
                }
                return "Created: " + candidate.getName();
            }
        }.run();
    }

    private TopologyCandidate buildCandidate(Transaction txn,
                                             String topologyName,
                                             StorageNodePool pool,
                                             int numPartitions,
                                             Parameters currentParams) {
        final TopologyBuilder tb =
            new TopologyBuilder(getCurrentTopology(),
                                topologyName,
                                pool,
                                numPartitions,
                                currentParams,
                                myParams);

        final TopologyCandidate candidate = tb.build();

        final TopologyStore topoStore = stores.getTopologyStore();
        if (topoStore.candidateExists(txn, topologyName)) {
            final TopologyCandidate preexistingCandidate =
                getCandidate(txn, topologyName);

            if (!preexistingCandidate.getTopology().
                    layoutEquals(candidate.getTopology())) {
                throw new IllegalCommandException(
                    "Topology candidate " + topologyName + " exists " +
                    "but with a different layout");
            }
            if (!preexistingCandidate.getDirectoryAssignments().equals(
                    candidate.getDirectoryAssignments())) {
                throw new IllegalCommandException(
                    "Topology candidate " + topologyName + " exists " +
                    "but with different mount point assignment");
            }
        } else {
            topoStore.putCandidate(txn, candidate);
        }
        return candidate;
    }

    /**
     * Retrieves the named topology candidate from the topo store.
     * @throws IllegalCommandException if the candidate does not exist.
     */
    public TopologyCandidate getCandidate(final String candidateName) {
        return new AdminTransaction<TopologyCandidate>(
                                                RunTransaction.readOnly) {

            @Override
            protected TopologyCandidate doTransaction(Transaction txn) {
                return getCandidate(txn, candidateName);
            }
        }.run();
    }

    /**
     * Check that the specified topology exists, throw IllegalCommand if
     * it doesn't.
     */
    private TopologyCandidate getCandidate(Transaction txn,
                                           String candidateName) {
        final TopologyCandidate candidate =
             stores.getTopologyStore().getCandidate(txn, candidateName);
        if (candidate == null) {
            throw new IllegalCommandException
                ("Topology " + candidateName + " does not exist. Use " +
                 " topology list to see all available candidate");
        }

        return candidate;
    }

    /**
     * Present a list of candidate names.
     */
    public List<String> listTopoCandidates() {
        return new AdminTransaction<List<String>>(RunTransaction.readOnly) {

            @Override
            protected List<String> doTransaction(Transaction txn) {
                return stores.getTopologyStore().getCandidateNames(txn);
            }
        }.run();
    }

    /**
     * Execute the redistribution algorithms on a given topo candidate,
     * updating its layout to use as much of the available store SNs as
     * possible.
     * @param candidateName the name of the target candidate.
     * @param snPoolName represents the resources to be used by in the new
     * layout.
     * @return a message indicating success or failure, to be reported  by the
     * CLI.
     */
    public String redistributeTopology(final String candidateName,
                                       final String snPoolName) {

        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                /* check that the target candidate exists.*/
                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool = freezeSNPool(snPoolName);

                try {
                    /* Create and save a new layout against the specified SNs */
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate, pool,
                                            getCurrentParameters(),
                                            myParams);
                    final TopologyCandidate newCandidate = tb.build();
                    stores.getTopologyStore().putCandidate(txn, newCandidate);
                    String message = "Redistributed: " + newCandidate.getName();
                    return getSuccessMessageWithNotes(message, tb.getNotes());
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    public String createFailoverTopology(final String candidateName,
                                         final String snPoolName,
                                         Set<DatacenterId> newPrimaryZones,
                                         Set<DatacenterId> offlineZones) {
        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                final StorageNodePool pool = freezeSNPool(snPoolName);

                try {
                    return doFailoverProcessing(candidateName,
                                                pool,
                                                newPrimaryZones,
                                                offlineZones,
                                                txn);
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    /**
     * Execute the contraction algorithms on a given topo candidate, updating
     * its layout to use as much of the available store SNs as possible.
     * @param candidateName the name of the target candidate.
     * @param snPoolName represents the resources to be used by in the new
     * layout.
     * @return a message indicating success or failure, to be reported  by the
     * CLI.
     */
    String contractTopology(final String candidateName,
                            final String snPoolName) {

        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                /* check that the target candidate exists.*/
                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool = freezeSNPool(snPoolName);

                try {
                    /* Create and save a new layout against the specified SNs */
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate,
                                            pool,
                                            getCurrentParameters(),
                                            myParams);
                    final TopologyCandidate newCandidate = tb.contract();

                    /* Save topology candidate */
                    stores.getTopologyStore().putCandidate(txn, newCandidate);

                    return "Contracted: " + newCandidate.getName();
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    /**
     * Execute the remove shard on source topology with mentioned failed
     * shard argument and generate new topology layout to remove RN's and
     * SNs if any associated with the failed shard.
     * @param failedShard the Id of failed shard
     * @param candidateName the name of target topology
     * @return a message indicating success or failure, to be reported  by the
     * CLI.
     */
    String removeFailedShard(final RepGroupId failedShard,
                             final String candidateName) {

       return new AdminTransaction<String>(RunTransaction.writeNoSync) {

           @Override
           protected String doTransaction(Transaction txn) {

               /* Generate a cloned target candidate topology */
               final Topology topology = getCurrentTopology();
               addTopoCandidate(candidateName,topology);

               /* Check that the target candidate exists.*/
               final TopologyCandidate candidate =
                   getCandidate(txn, candidateName);

               /* Generated new snPool from topology. We need to make sure
                * that targetSNPool name is unique from already existing
                * pools if customer runs new remove-shard commands, but use the
                * same pool if the same plan is restarted.  Each plan has a
                * unique topology candidate name, so that suffix will give each
                * plan a unique pool.
                *
                */
               String poolname = PoolCommand.INTERNAL_NAME_PREFIX +
                   "remove-shard-" + candidateName;
               StorageNodePool targetSNpool = createTargetStorageNodePool(
                                                  topology, poolname);

               /* Removing SN's from StorageNodePool associated with failed
                * shard if there are no other running RNs and capacity = 1
                */
               List<StorageNodeId> removeSNIds = new ArrayList<StorageNodeId>();
               List<StorageNodeId> snIds = topology.getStorageNodeIds();
               for (StorageNodeId snId : snIds) {
                   Set<RepNodeId> snRNIds = topology.getHostedRepNodeIds(snId);

                   /* If snRNId belong to failed shard and snId capacity = 1
                    * then add snId to remove list from storage node pool.
                    *
                    */
                   if (snRNIds.size() > 1) {
                       /* SN has capacity greater than 1 hence not
                        * adding it to remove SN pool list even
                        * if SN has RN from failed shard.
                        */
                       continue;
                   }

                   for (RepNodeId snRNId : snRNIds) {
                       if (snRNId.getGroupId() == failedShard.getGroupId()) {
                           removeSNIds.add(snId);
                       }
                   }
               }

               for (StorageNodeId snId : removeSNIds) {
                   removeStorageNodeFromPool(poolname, snId);
               }

               targetSNpool = freezeSNPool(poolname);
               try {
                   /* Create and save a new layout against the specified SNs */
                   final TopologyBuilder tb =
                           new TopologyBuilder(candidate,
                                               targetSNpool,
                                               getCurrentParameters(),
                                               myParams);

                   final TopologyCandidate newCandidate =
                           tb.removeFailedShard(failedShard);
                   /* Save topology candidate */
                   stores.getTopologyStore().putCandidate(txn, newCandidate);
                   return "Removed failed shard " + failedShard + " in " +
                       newCandidate.getName();
               } finally {
                   targetSNpool.thaw();
               }
           }
       }.run();
    }

    /**
     * Make the specified topology compliant with all the topology rules.
     */
    public String rebalanceTopology(final String candidateName,
                                    final String snPoolName,
                                    final DatacenterId dcId) {

        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool = freezeSNPool(snPoolName);

                try {
                    /* Create and save a new layout against the specified SNs */
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate, pool,
                                            getCurrentParameters(), myParams);

                    final TopologyCandidate newCandidate = tb.rebalance(dcId);
                    stores.getTopologyStore().putCandidate(txn, newCandidate);
                    String message = "Rebalanced: " + newCandidate.getName();
                    return getSuccessMessageWithNotes(message, tb.getNotes());
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    /**
     * Check that the specified snPool exists, and freeze it so it is stable
     * while the topology transformation is going on.
     */
    private StorageNodePool freezeSNPool(String snPoolName) {
        checkIfReadonlyAdmin();
        final StorageNodePool pool = parameters.getStorageNodePool(snPoolName);
        if (pool == null) {
            throw new IllegalCommandException
                ("Storage Node Pool " + snPoolName + " not found.");
        }
        pool.freeze();
        return pool;
    }

    /**
     * Get a new copy of the Parameters from the database.
     */
    private Parameters readParameters(Transaction txn) {
        final Parameters params = stores.getParameters(txn);
        if (params == null) {
            logger.fine("Parameters not found");
        } else {
            logger.fine("Parameters fetched");
        }

        return params;
    }

    /**
     * Get a new copy of the Memo from the database.
     */
    private Memo readMemo(Transaction txn) {
        final Memo m = stores.getMemo(txn);
        if (m == null) {
            logger.fine("Memo not found");
        } else {
            logger.fine("Memo fetched");
        }

        return m;
    }

    /**
     * Save a new bunch of event records, and the latest timestamp, in the same
     * transaction.
     */
    public void storeEvents(final List<CriticalEvent> events,
                            final LatestEventTimestamps let) {

        /*
         * Use a lightweight commit for critical-event-storing transactions.
         */

        RunTransaction<Void> transaction =
        new AdminTransaction<Void>(RunTransaction.noSync) {

            /* Supply handlers to propagate the the exceptions. */

            @Override
            void handler(UnknownMasterException ume) {
                throw ume;
            }

            @Override
            void handler(ReplicaWriteException rwe) {
                throw rwe;
            }

            @Override
            protected Void doTransaction(Transaction txn) {
                final EventStore eventStore = stores.getEventStore();

                for (CriticalEvent pe : events) {
                    eventStore.putEvent(txn, pe);

                    /*
                     * Age the store periodically.
                     */
                    if (eventStoreCounter++ % eventStoreAgingFrequency == 0) {
                        eventStore.ageStore(txn, getEventExpiryAge());
                    }
                }

                /*
                 * The memo is being modified without synchronization, which is
                 * ok because the method is guaranteed to be called by a single
                 * eventRecorder thread.
                 */
                memo.setLatestEventTimestamps(let);
                stores.putMemo(txn, memo);
                return null;
            }
        };

        try {
            transaction.run();
        } catch (AdminNotReadyException anre) {
            /*
             * ANRE is thrown during upgrade from DPL to non-DPL stores while
             * the Admin is in read-only mode
             */
            logger.info(anre.getMessage());
        } catch (ReplicaWriteException | UnknownMasterException e) {
            logger.log(Level.FINE,
                       "Ignoring exception resulting from write operation " +
                       "when not the master: {0}",
                       e.getMessage());
        } catch (ThreadInterruptedException | IllegalStateException e) {
            /**
             * The thread can be interrupted while closing, or an
             * IllegalStateException wrapping, say an
             * InsufficientReplicasException, could be thrown by
             * RunTransaction.
             *
             * Future: may be better to handle InsufficientReplicasException
             * (and perhaps other exceptions) on shutdown in RunTransaction
             * itself.
             */
            if (!closing) {
                throw e;
            }
            logger.log(Level.FINE,
                       "Ignoring interrupt exception storing events " +
                       "while shutting down: {0}",
                       e.getMessage());
        }
    }

    /**
     * Return a list of event records satisfying the given criteria.
     * For endTime, zero means the current time.
     * For type, null means all types.
     */
    public List<CriticalEvent> getEvents(final long startTime,
                                         final long endTime,
                                         final CriticalEvent.EventType type) {

        return new AdminTransaction<List<CriticalEvent>>
                                            (RunTransaction.readOnly) {

            @Override
            protected List<CriticalEvent> doTransaction(Transaction txn) {
                return stores.getEventStore().getEvents(txn, startTime, endTime,
                                                        type);
            }
        }.run();
    }

    /**
     * Return a single event that has the given database key.
     */
    public CriticalEvent getOneEvent(final String eventId) {

        return new AdminTransaction<CriticalEvent>(RunTransaction.readOnly) {

            @Override
            protected CriticalEvent doTransaction(Transaction txn) {
                return stores.getEvent(txn, eventId);
            }
        }.run();
    }

    /**
     * Approve the given plan for execution.
     */
    public synchronized void approvePlan(int id) {
        final Plan p = getAndCheckPlan(id);
        planner.approvePlan(id);
        savePlan(p, CAUSE_APPROVE);
        logger.info(p + " approved");
    }

    /**
     * Cancel the given plan.
     */
    public synchronized void cancelPlan(int id) {
        final Plan p = getAndCheckPlan(id);
        planner.cancelPlan(id);
        savePlan(p, CAUSE_CANCEL);
        logger.info(p + " canceled");
    }

    /**
     * Interrupt the given plan. Must be synchronized because
     * planner.interruptPlan() will take the plan mutex, and then may
     * attempt to save the plan, which requires the Admin mutex. Since the
     * lock hierarchy is Admin&rarr;plan, we need to take the Admin mutex now.
     * [#22161]
     */
    public synchronized void interruptPlan(int id) {
        final Plan p = getAndCheckPlan(id);
        planner.interruptPlan(id);
        savePlan(p, CAUSE_INTERRUPT);
        logger.info(p + " interrupted");
    }

    /**
     * Start the execution of a plan. The plan will proceed asynchronously.
     *
     * To check the status and final result of the plan, the caller should
     * examine the state and execution history. AwaitPlan() can be used to
     * synchronously wait for the end of the plan, and to learn the plan status.
     * AssertSuccess() can be used to generate an exception if the plan
     * fails, which will throw a wrapped version of the original exception
     * @param force TODO
     * @throws IllegalCommandException if there is a problem with executing the
     * plan.
     */
    public void executePlan(int id, boolean force) {
        try {
            executePlanInternalRetry(id, force, false);
        } catch (PlanLocksHeldException e) {
            throw new IllegalCommandException(e.getMessage(),
                ErrorMessage.NOSQL_5400,
                CommandResult.PLAN_CANCEL);
        }
    }

    /**
     * Executes specified plan. If there is a lock conflict retry the execution.
     *
     * @param id - Id of plan to execute
     * @param force - plan force flag
     * @param doMatchProcessing - if true, a lock conflict caused by a plan that
     *                            is logically the same will return the plan
     *                            id of the running plan.
     * @return plan id of executing plan.
     * @throws PlanLocksHeldException
     */
    private int executePlanInternalRetry(int id,
                                         boolean force,
                                         boolean doMatchProcessing)
        throws PlanLocksHeldException {

        final int MAX_RETRY_COUNT = 20;
        final long RETRY_WAIT_MS = 1000;

        int retryCount = 0;
        while (true) {
            try {
                return executePlanInternal(id, force, doMatchProcessing);
            } catch (PlanLocksHeldException e) {
                retryCount++;
                if (retryCount > MAX_RETRY_COUNT) {
                    throw e;
                }
                try {
                    Thread.sleep(RETRY_WAIT_MS);
                } catch (InterruptedException d) {

                }
            }
        }
    }

    private synchronized int executePlanInternal(int id,
                                                 boolean force,
                                                 boolean doMatchProcessing)
        throws PlanLocksHeldException {
        final Plan p = getAndCheckPlan(id);
        try {
            planner.executePlan(p, force);
            return id;
        } catch (PlanLocksHeldException e) {
            planner.clearLocks(id);
            if (doMatchProcessing) {
                int runningPlanId = e.getOwningPlanId();
                Plan runningPlan = getPlanById(runningPlanId);
                if (runningPlan.logicalCompare(p)) {
                    /*
                     * Cancel the duplicate plan and return the ID of the
                     * running plan.
                     */
                    cancelPlan(id);
                    return runningPlanId;
                }
            }
            throw e;
        }
    }

    /**
     * Start the execution of a plan. The focus is on getting a set of actions
     * to occur, so if there s a conflicting plan which already owns the
     * plan catalog locks needed by this plan, take extra steps to handle
     * the lock conflict.
     *
     * If there is a lock conflict, see if that conflicting plan is the logical
     * equivalent of the original plan. If it is, return the id of the
     * conflicting plan, which serves as an adequate proxy for the desired
     * actions.
     *
     * @return the id of whichever plan is going to carry out the desired
     * action, be that the original plan, or the conflicting plan.
     * @throws PlanLocksHeldException the plan cannot obtain its catalog locks.
     */
    public int executePlanOrFindMatch(int id)
        throws PlanLocksHeldException {
        return executePlanInternalRetry(id, false, true);
    }

    /**
     * Waits for the specified plan to finish. If the timeout period is
     * specified and exceeded or the wait is interrupted, the call will end.
     *
     * @return the current plan state.
     * @throws AdminNotReadyException if the Admin cannot service the request
     */
    public Plan.State awaitPlan(int id, int timeout, TimeUnit timeoutUnit) {

        final Plan p = getAndCheckPlan(id);
        final PlanWaiter waiter = new PlanWaiter();
        p.addWaiter(waiter);
        logger.log(Level.FINE, "Waiting for {0}, timeout={1} {2}",
                   new Object[]{p, timeout, timeoutUnit});

        try {
            if (timeout == 0) {

                /* Wait until the plan finishes or the planner is shutdown */
                while (!waiter.waitForPlanEnd(10, TimeUnit.SECONDS)) {

                    /*
                     * Get the planner from the plan, since the Admin's planner
                     * may have changed.
                     */
                    if (p.getPlanner().isShutdown()) {
                        throw new AdminNotReadyException("Cannot service " +
                                                         "request, admin " +
                                                         "is shutdown");
                    }
                }

                /*
                 * If the plan has ended in the RUNNING state, then the plan
                 * needed to restart this Admin. Throw an exception and let the
                 * caller retry at the new master. The plan should resume
                 * execution there.
                 */
                if (p.getState() == Plan.State.RUNNING) {
                    throw new AdminNotReadyException("Cannot service " +
                                                     "request, admin " +
                                                     "is shutting down");
                }
            } else {

                if (!waiter.waitForPlanEnd(timeout, timeoutUnit)) {
                    logger.log(Level.INFO, "Timed out (timeout {0} ms)" +
                               " waiting for {1} to finish, state={2} ",
                               new Object[] {timeoutUnit.toMillis(timeout),
                                             p, p.getState()});
                }
            }
        } catch (InterruptedException e) {
            logger.log
                (Level.INFO,
                 "Interrupted while waiting for {0} to finish, end state={1}",
                 new Object[] {p, p.getState()});
        } finally {
            p.removeWaiter(waiter);
            logger.log(Level.FINE, "Waiting for {0} ended, state={1}",
                       new Object[] {p, p.getState()});
        }
        return p.getState();
    }

    /**
     * Throw an operation fault exception if the plan did not succeed.
     */
    public void assertSuccess(int planId) {
        final Plan p = getAndCheckPlan(planId);
        final Plan.State status = p.getState();
        if (status == Plan.State.SUCCEEDED) {
            return;
        }

        final ExceptionTransfer transfer = p.getLatestRunExceptionTransfer();
        final String msg = p + " ended with " + status;
        if (transfer != null) {
            if (transfer.getFailure() != null) {
                final Throwable failure = transfer.getFailure();
                final OperationFaultException newEx =
                    new OperationFaultException(msg + ": " +
                                                transfer.getDescription(),
                                                failure);
                newEx.setStackTrace(failure.getStackTrace());
                throw newEx;
            } else if (transfer.getDescription() != null) {
                /* No exception saved, but there is some error information */
                throw new OperationFaultException(msg + ": " +
                                                  transfer.getDescription());
            }
        }
        throw new OperationFaultException(msg);
    }

    /**
     * The concurrency protocol for getPlanbyId is that we give out references
     * to the cached Plan of record, if the Plan is in a non-terminal, or
     * active, state.  If the plan is active, then it will be found in the
     * Planner's catalog, which is consulted first in this method.
     *
     * If the plan is not active, we read it straight from the database.  For
     * such plans, it's possible for this method to produce multiple instances
     * of the same plan.  We consider inactive plans to be read-only, so this
     * should not create any synchronization problems.  The read-only-ness of
     * inactive plans is enforced in Admin.savePlan, which is the only means by
     * which a plan is updated in the database.  The terminal-state-ness of
     * uncached plans is asserted below, also.
     *
     * public for unit test support
     */
    public Plan getPlanById(final int id) {
        final Plan p = getPlanner().getCachedPlan(id);
        if (p != null) {
            return p;
        }

        return new AdminTransaction<Plan>(RunTransaction.readOnly) {
            @Override
            protected Plan doTransaction(Transaction txn) {
                final Plan uncachedPlan = stores.getPlanStore().getPlanById
                    (id, txn, getPlanner(), myParams);
                if (uncachedPlan == null) {
                    return null;
                }
                if (!uncachedPlan.getState().isTerminal()) {
                    throw new IllegalStateException
                        ("Found non-terminal plan that is not cached. " +
                         uncachedPlan);
                }
                return uncachedPlan;
            }
        }.run();
    }

    /**
     * Get the plan, throw an exception if it doesn't exist.
     */
    Plan getAndCheckPlan(int id) {
        final Plan p = getPlanById(id);
        if (p == null) {
            throw new IllegalCommandException(String.format(
                "Plan id %s doesn't exist, possibly being pruned. " +
                "Latest plan-pruning info: %s",
                id, stores.getPlanStore().getLatestPruningInfo()));
        }
        return p;
    }

    /**
     * Retrieve the beginning and ending plan ids that satisfy the request.
     *
     * Returns an array of two integers indicating a range of plan id
     * numbers. [0] is the first id in the range, and [1] is the last,
     * inclusive.
     *
     * Operates in three modes:
     *
     *    mode A requests n plans ids following startTime
     *    mode B requests n plans ids preceding endTime
     *    mode C requests a range of plan ids from startTime to endTime.
     *
     *    mode A is signified by endTime == 0
     *    mode B is signified by startTime == 0
     *    mode C is signified by neither startTime nor endTime being == 0.
     *    N is ignored in mode C.
     */
    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int n,
                                final String planOwnerId) {
        return new AdminTransaction<int[]>(RunTransaction.readOnly) {

            @Override
            protected int[] doTransaction(Transaction txn) {
                return stores.getPlanStore().getPlanIdRange(txn,
                                                   startTime, endTime, n,
                                                   planOwnerId);
            }
        }.run();
    }

    /**
     * Returns a map of plans starting at firstPlanId.  The number of plans in
     * the map is the lesser of howMany, MAXPLANS, or the number of extant
     * plans with id numbers following firstPlanId.  The range is not
     * necessarily fully populated; while plan ids are mostly sequential, it is
     * possible for values to be skipped.
     *
     * The plan instances will only be valid for display, and will not
     * be executable, because they will be stripped of memory heavy objects.
     */
    public synchronized Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                                     final int howMany,
                                                     final String planOwnerId) {
        if (planner == null) {
            return Collections.emptyMap();
        }

        return new AdminTransaction<Map<Integer, Plan>>
                                        (RunTransaction.readOnly) {

            @Override
            protected Map<Integer, Plan> doTransaction(Transaction txn) {
                return stores.getPlanStore().getPlanRange(txn,
                                                 planner,
                                                 myParams,
                                                 firstPlanId, howMany,
                                                 planOwnerId);
            }
        }.run();
    }

    public boolean isClosing() {
        return closing;
    }

    /**
     * Store the current parameters.
     */
    private void storeParameters() {
        logger.fine("Storing Parameters");

        new AdminTransaction<Void>(RunTransaction.sync) {

            @Override
            protected Void doTransaction(Transaction txn) {
                stores.putParameters(txn, parameters);
                return null;
            }
        }.run();
    }

    /**
     * Convenience method to store the current topology and parameters in the
     * same transaction
     */
    private void storeTopoAndParams(final Topology topology,
                                    final DeploymentInfo info,
                                    final Plan plan) {
        assert Thread.holdsLock(this);

        logger.log(Level.FINE,
                   "Storing parameters and topology with sequence #: {0}",
                   topology.getSequenceNumber());

        new AdminTransaction<Void>(RunTransaction.sync) {

            @Override
            protected Void doTransaction(Transaction txn) {
                /*
                 * Inform the plan that the topology is being updated. Save
                 * the plan if needed. Be sure to take the plan mutex before
                 * any JE locks are acquired in this transaction, per the
                 * synchronization hierarchy.
                 */
                synchronized (plan) {
                   if (plan.updatingMetadata(topology)) {
                       stores.putPlan(txn, plan);
                   }
                }

                stores.putTopology(txn, new RealizedTopology(topology, info));
                stores.putParameters(txn, parameters);
                stores.putMemo(txn, memo);
                return null;
            }
        }.run();
    }

    /**
     * Returns the params for the specified storage node.
     */
    public StorageNodeParams getStorageNodeParams(StorageNodeId targetSNId) {
        checkIfReadonlyAdmin();
        return parameters.get(targetSNId);
    }

    /**
     * Creates a copy of the policy map so that it can be updated.
     */
    public ParameterMap copyPolicy() {
        checkIfReadonlyAdmin();
        return parameters.copyPolicies();
    }

    /**
     * Replaces the current policy map with a new one.  The pattern for updating
     * the policy map is:
     * 1.  make a copy using copyPolicy()
     * 2.  update the copy
     * 3.  replace the policy map using setPolicy().
     */
    public void setPolicy(ParameterMap policyParams) {
        checkIfReadonlyAdmin();
        logger.info("Setting new policy parameters: " +
                    parameters.getPolicies().diff(
                        policyParams, true /* notReadOnly */));
        parameters.setPolicies(policyParams);
        storeParameters();
    }

    public long getEventExpiryAge() {
        return myParams.getAdminParams().getEventExpiryAge();
    }

    public long getMaxEvents() {
        return myParams.getAdminParams().getMaxEvents();
    }

    /**
     * Returns the polling interval for the event recorder.
     */
    public long getEventRecorderPollingIntervalMs() {
        return myParams.getAdminParams().getEventRecorderPollingIntervalMs();
    }

    /**
     * This method is used only for testing.
     */
    public void setEventStoreAgingFrequency(int f) {
        eventStoreAgingFrequency = f;
    }

    /**
     * Generates a new AdminId for deploying a new Admin to the system.
     */
    public AdminId generateAdminId() {
        checkIfReadonlyAdmin();
        return parameters.getNextAdminId();
        /*
         * The new value of parameters.nextAdminId will be persisted when the
         * DeployAdminPlan completes and calls
         * savePlanResults(List<AdminParams>).
         */
    }

    /**
     * Returns the number of Admin instances in the system.
     */
    public int getAdminCount() {
        checkIfReadonlyAdmin();
        return parameters.getAdminCount();
    }

    /**
     * Returns the params for the specified RepNode.
     */
    public RepNodeParams getRepNodeParams(RepNodeId targetRepNodeId) {
        checkIfReadonlyAdmin();
        return parameters.get(targetRepNodeId);
    }

    /**
     * Returns the params for the specified ArbNode.
     */
    public ArbNodeParams getArbNodeParams(ArbNodeId targetArbNodeId) {
        checkIfReadonlyAdmin();
        return parameters.get(targetArbNodeId);
    }


    /**
     * Returns GlobalParams for the system.
     */
    public GlobalParams getGlobalParams() {
        return parameters.getGlobalParams();
    }

    /*
     * Initializes the Admin stores, checking the schema version and
     * performing updates as needed.
     */
    private synchronized void initStores(final boolean isMaster) {

        final AdminSchemaVersion schemaVersion =
                    new AdminSchemaVersion(Admin.this, logger);
        new AdminTransaction<Void>(RunTransaction.sync) {

            @Override
            protected Void doTransaction(Transaction txn) {

                /*
                 * Check that this release is compatible with the Admin db
                 * schema version, should it already exist.
                 */
                schemaVersion.checkAndUpdateVersion(txn, isMaster, stores);
                return null;
            }
        }.run();
        stores.init(schemaVersion.openAndReadSchemaVersion(), false /*force */);
    }

    /*
     * Enter a new replication mode. Called in the ReplicationStateChange
     * thread.
     */
    private void enterMode(final StateChangeEvent sce) {
        synchronized (closingLock) {
            while (true) {

                /* If we are shutting down, we don't need to do anything. */
                if (closing) {
                    return;
                }

                /* Wait if there is another enterMode caller */
                if (enteringMode) {
                    try {
                        closingLock.wait();
                        continue;
                    } catch (InterruptedException e) {

                        /* Done if interrupted */
                        return;
                    }
                }

                /* Note call to enterMode */
                enteringMode = true;
                break;
            }
        }
        try {
            /* Shutdown all threads befoe changing modes. */
            synchronized (threadLock) {
                shutdownThreads(false);
                enterModeInternal(sce);
            }
        } finally {

            /* Note that enterMode call is done and notify waiters */
            synchronized (closingLock) {
                enteringMode = false;
                closingLock.notifyAll();
            }
        }
    }

    /* Internal method to enter mode while synchronized */
    private synchronized void enterModeInternal(final StateChangeEvent sce) {
        final State state = sce.getState();
        switch (state) {
        case MASTER:
            enterMasterMode();
            break;
        case REPLICA:
            enterReplicaMode(state,
                     new AdminId(Integer.parseInt(sce.getMasterNodeName())));
            break;
        case DETACHED:
            if (environment.isClosed()) {
                logger.info
                    ("Admin replica is detached; envImpl is closed");
            } else {
                EnvironmentFailureException efe =
                    environment.getInvalidatingException();
                if (efe != null) {
                    throw efe;
                }
                logger.info("Admin replica is detached; env is valid.");
            }
            /*$FALL-THROUGH$*/
        case UNKNOWN:
            enterDetachedMode(state);
            break;
        }
    }

    /*
     * Transition to master mode.  This happens when the master starts up from
     * scratch, and when a new master is elected.  Called only from the
     * synchronized method enterMode, above.
     */
    private void enterMasterMode() {
        assert Thread.holdsLock(this);
        try {
            startupStatus.setUnready(Admin.this);
            masterId = adminId;
            initStores(true /* isMaster */);
            monitor = new Monitor(myParams, Admin.this, getLoginManager());
            eventRecorder = new EventRecorder(Admin.this);
            open();     // Sets planner and memo
            monitor.setupExistingAgents(getCurrentTopology());
            addParameterListener(monitor);
            restartPlans();
            startSysTableMonitor();
            AdminParams ap = myParams.getAdminParams();
            if (ap.isMetadataAdminThreadEnabled()) {
                startMetadataAdminThread(
                    ap.getKVMetadataAdminCheckInterval().toMillis(),
                    ap.getKVMetadataAdminMaxPlanWait().toMillis());
            }
            if (ap.isVersionThreadEnabled()) {
                startVersionUpdaterThread(
                    ap.getVersionCheckInterval().toMillis());
            }
        } catch (RuntimeException re) {
            logger.log(Level.SEVERE, "RuntimeException while entering " +
                       State.MASTER.name() + " mode, " + re.getMessage(), re);
            startupStatus.setError(Admin.this, re);
        }
    }

    /*
     * Transition to replica mode.  This happens on startup of a replica.
     * Called only from the synchronized method enterMode, above.
     */
    private void enterReplicaMode(State state, final AdminId newMaster) {
        assert Thread.holdsLock(this);
        try {
            startupStatus.setUnready(Admin.this);
            masterId = newMaster;
            initStores(false /* isMaster */);
            startupStatus.setReady(Admin.this);
        } catch (RuntimeException re) {
            logger.log(Level.SEVERE, "RuntimeException while entering " +
                       state.name() + " mode, " + re.getMessage(), re);
            startupStatus.setError(Admin.this, re);
        }
    }

    /*
     * Transition to a mode in which we don't really know what's going on, and
     * hope that it's a temporary situation.
     */
    private void enterDetachedMode(State state) {
        enterReplicaMode(state, null);
    }

    /**
     * Restart plans that were RUNNING before the Admin failed over.
     */
    private void restartPlans() {
        assert Thread.holdsLock(this);
        final Set<Integer> restartSet = readAndRecoverPlans();
        startupStatus.setReady(Admin.this);

        for (Integer planId : restartSet) {
            logger.info("Restarting plan " + planId);

            try {
                executePlan(planId, false);
            } catch (IllegalCommandException ice) {
                logger.log(Level.WARNING,
                          "Unable to restart plan " + planId, ice);
            }
        }
    }

    /**
     * Returns the set of plans that were not in a terminal state when the Admin
     * failover happened.
     *
     * From Planner.recover():
     *
     * 1.RUNNING plans ->INTERRUPT_REQUESTED -> INTERRUPTED, and
     *    will be restarted.
     * 2.INTERRUPT_REQUESTED plans -> INTERRUPTED and are not restarted. The
     *    failover is as if the cleanup phase was interrupted by the user.
     * 3.INTERRUPTED and APPROVED plans are left as is.
     */
    private Set<Integer> readAndRecoverPlans() {
        assert Thread.holdsLock(this);
        assert planner != null;

        final Map<Integer, Plan> activePlans =
            new AdminTransaction<Map<Integer, Plan>>(RunTransaction.readOnly) {

                @Override
                protected Map<Integer, Plan> doTransaction(Transaction txn) {
                    final Map<Integer, Plan> plans =
                            stores.getPlanStore().getActivePlans(txn,
                                                                   planner,
                                                                   myParams);
                    stores.getTopologyStore().initCachedStartTime(txn);
                    logger.log(Level.FINE, "Fetched {0} plans.", plans.size());

                    return plans;
                }

            }.run();

        /* Recover any plans that are not in a terminal state. */
        final Set<Integer> restartSet = new HashSet<>();

        for (Plan p : activePlans.values()) {
            final Plan restart = planner.recover(p);
            if (restart != null) {
                restartSet.add(restart.getId());
            }
        }

        return restartSet;
    }

    public synchronized ReplicatedEnvironment.State getReplicationMode() {
        return environment.getState();
    }

    public synchronized AdminStatus getAdminStatus() {
        return new AdminStatus(
            startupStatus.getStatus(this),
            environment.getState(),
            getIsAuthoritativeMaster(),
            environment.getRepStats(StatsConfig.DEFAULT),
            environment.getStats(StatsConfig.DEFAULT).getAvailableLogSize(),
            serviceStartTime,
            stateChangeTime);
    }

    /**
     * Returns whether the underlying JE environment is known to be the
     * authoritative master.
     */
    private synchronized boolean getIsAuthoritativeMaster() {
        final RepImpl repImpl = RepInternal.getRepImpl(environment);
        if (repImpl == null) {
            return false;
        }
        final com.sleepycat.je.rep.impl.node.RepNode repNode =
            repImpl.getRepNode();
        if (repNode == null) {
            return false;
        }
        return repNode.isAuthoritativeMaster();
   }

    public synchronized URI getMasterRmiAddress() {
        final AdminId currentMaster = masterId;
        if (currentMaster == null) {
            return null;
        }
        final Parameters p = getCurrentParameters();
        final AdminParams ap = p.get(currentMaster);
        if (ap == null) {
            return null;
        }
        final StorageNodeParams snp = p.get(ap.getStorageNodeId());

        try {
            return
                new URI("rmi", null, snp.getHostname(), snp.getRegistryPort(),
                        null, null, null);
        } catch (URISyntaxException e) {
            throw new NonfatalAssertionException
            ("Unexpected bad URL: " + e.getMessage(), e);
        }
    }

    /*
     * Try to build the admin master's web service URI. If fail return null.
     */
    public synchronized URI getMasterWebServiceAddress() {

        final AdminId currentMaster = masterId;
        if (currentMaster == null) {
            return null;
        }

        final Parameters p = getCurrentParameters();
        final AdminParams ap = p.get(currentMaster);
        if (ap == null) {
            return null;
        }
        final StorageNodeParams snp = p.get(ap.getStorageNodeId());

        /* Return null if there is not web service on admin master */
        final int adminWebPort = snp.getAdminWebPort();
        if (adminWebPort <= 0) {
            return null;
        }

        try {
            /* Check that whether we should use https */
            String schema = "http";
            if (myParams != null &&
                myParams.getSecurityParams() != null &&
                myParams.getSecurityParams().isSecure() &&
                !myParams.getSecurityParams().allTransportSSLDisabled()) {
                schema = "https";
            }

            return new URI(schema, null, snp.getHostname(),
                           snp.getAdminWebPort(),
                           null, null, null);
        } catch (URISyntaxException e) {
            throw new NonfatalAssertionException
            ("Unexpected bad URL: " + e.getMessage(), e);
        }
    }

    /**
     * Dump the deployed transaction history.
     */
    public List<String> displayRealizedTopologies(final boolean concise) {
        return new AdminTransaction<List<String>>(RunTransaction.readOnly) {

            @Override
            protected List<String> doTransaction(Transaction txn) {
                return stores.getTopologyStore().displayHistory(txn, concise);
            }
        }.run();
    }

    /**
     * Ensures that the proposed start time for a new plan which creates a
     * RealizedTopology is a value that is &gt; the deployment time of the
     * current topology.
     *
     * Since the start time is the primary key for the historical collection,
     * the start time for any new realized topology must be greater than
     * the start time recorded for the current topology. Due to clock skew in
     * the HA rep group, conceivably the start time could fail to advance if
     * there is admin rep node failover.
     */
    public
    long validateStartTime(long proposedStartTime) {
        return stores.getTopologyStore().validateStartTime(proposedStartTime);
    }

    /**
     * Provide information about the plan's current or
     * last execution run. Meant for display purposes.
     */
    String getPlanStatus(int planId, long options, boolean json) {
        final Plan p = getAndCheckPlan(planId);
        final StatusReport report = new StatusReport(p, options);
        if (json) {
            ObjectNode jsonTop = report.displayAsJson();
            CommandJsonUtils.updateNodeWithResult(jsonTop, p.getOperation(),
                                                  p.getCommandResult());
            return CommandJsonUtils.toJsonString(jsonTop);
        }
        return report.display();
    }

    /**
     * Admin.Memo is a persistent class for storing singleton information that
     * Admin needs to keep track of.
     */
    public static class Memo implements Serializable {
        private static final long serialVersionUID = 1;

        private int planId;
        private LatestEventTimestamps latestEventTimestamps;

        /*
         * Flag indicating that the store has not yet been deployed. Cleared
         * on successful execution of deploy topology plan. The field will be
         * false on an upgrade, but it is assumed that the upgrade was made on
         * a deployed store.
         */
        private boolean needsDeployment;

        private Memo() {
            planId = 1;
            latestEventTimestamps = new LatestEventTimestamps(0L, 0L, 0L);
            needsDeployment = true;
        }

        private int getPlanId() {
            return planId;
        }

        private void setPlanId(int nextId) {
            planId = nextId;
        }

        private LatestEventTimestamps getLatestEventTimestamps() {
            return latestEventTimestamps;
        }

        private void setLatestEventTimestamps(LatestEventTimestamps let) {
            latestEventTimestamps = let;
        }

        private boolean needsDeployment() {
            return needsDeployment;
        }

        /*
         * Clears the needsDeployment flag. Returns the previous value of the
         * flag.
         */
        private synchronized boolean deploymentSucceeded() {
            boolean previous = needsDeployment;
            needsDeployment = false;
            return previous;
        }

        @Override
        public String toString() {
            return "Memo[" + planId +
                    (needsDeployment ? " needsDeployment" : "") +
                    "]";
        }
    }

    public LoadParameters getAllParams() {
        final LoadParameters ret = new LoadParameters();
        ret.addMap(myParams.getGlobalParams().getMap());
        ret.addMap(myParams.getStorageNodeParams().getMap());
        ret.addMap(myParams.getAdminParams().getMap());
        return ret;

    }

    /**
     * Gets the planner. If the admin is not the master or the Admin is not
     * yet initialized an AdminNotReadyException is thrown.
     *
     * public for unit test support
     *
     * @return the planner
     * @throws AdminNotReadyException if the Admain is not initialized or not
     * a master
     */
    public Planner getPlanner() {
        final Planner p = planner;
        if (p == null) {
            throw new AdminNotReadyException("Cannot service request, admin " +
                                             "is not the master");
        }
        return p;
    }

    public void syncEventRecorder() {
        eventRecorder.sync();
    }

    /* -- From StateChangeListener -- */

    /**
     * Takes action based upon the state change. The actions
     * must be simple and fast since they are performed in JE's thread of
     * control.
     */
    @Override
    public void stateChange(final StateChangeEvent sce) {

        /* If we are shutting down, we don't need to do anything. */
        if (closing) {
            return;
        }

        LoggerUtils.logStateChangeEvent(
            logger, sce, myParams.getAdminParams().getType());
        stateTracker.noteStateChange(sce);
        stateChangeTime = System.currentTimeMillis();
    }

    /**
     * Thread to manage replicated environment state changes.
     */
    private class AdminStateTracker extends StateTracker {

        AdminStateTracker(Logger logger) {
            super(AdminStateTracker.class.getSimpleName(), adminId, logger,
                  exceptionHandler);
        }

        @Override
        protected void doNotify(StateChangeEvent sce) {
            enterMode(sce);
        }
    }

    /**
     * Uncaught exception handler for Admin threads.
     */
    private class AdminExceptionHandler implements UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.log(Level.SEVERE,
                       "Admin shutting down for fault in thread " + t.getName(),
                       e);
            if (owner != null) {
                ProcessExitCode exitCode = ProcessExitCode.getRestart(e);
                if (e instanceof EnvironmentFailureException &&
                    ((EnvironmentFailureException) e).isCorrupted()) {

                    /* Do not restart if environment is corrupted */
                    exitCode = ProcessExitCode.getNoRestart(e);
                }
                owner.getFaultHandler().queueShutdown(e, exitCode);
            } else {
                /* This would be true only in a test environment. */
                new StoppableThread("StopAdminForForeignThreadFault") {
                    @Override
                    public void run() {
                        shutdown(true,
                                 "Exception in thread " + t.getName() +
                                 ": " + e);
                    }
                    @Override
                    protected Logger getLogger() {
                        return Admin.this.getLogger();
                    }
                }.start();
            }
        }
    }

    /**
     * Transaction runner that shuts down when the Admin is shutdown.
     */
    private abstract class AdminTransaction<T> extends RunTransaction<T> {

        AdminTransaction(TransactionConfig config) {
            super(environment, config, logger);
        }

        @Override
        protected boolean shuttingDown() {
            return closing;
        }
    }

    /**
     * A class to wrap the invocation of a database transaction and to handle
     * all the exceptions that might arise.  Derived from the je.rep.quote
     * example in the BDB JE distribution.
     */
    public abstract static class RunTransaction<T> {
        public static volatile int TRANSACTION_RETRY_MAX = 10;
        private static final int RETRY_WAIT = 3 * 1000;

        /*
         * For reads only. Read-committed is used because there are currently
         * no read-only ops that need repeatable-read isolation.
         */
        public static final TransactionConfig readOnly =
            new TransactionConfig().setReadOnly(true).setReadCommitted(true);

        /* For critical writes requiring a high level of durability */
        public static final TransactionConfig sync =
            new TransactionConfig().setDurability
            (new Durability(SyncPolicy.SYNC, SyncPolicy.SYNC,
                            ReplicaAckPolicy.SIMPLE_MAJORITY));

        /*
         * For writes which need to be lightweight and their loss would not
         * be critical, such as event logging writes.
         */
        public static final TransactionConfig noSync =
            new TransactionConfig().setDurability
            (new Durability(SyncPolicy.NO_SYNC,
                            SyncPolicy.NO_SYNC,
                            ReplicaAckPolicy.NONE));

        /* Non-synced durable writes */
        public static final TransactionConfig writeNoSync =
            new TransactionConfig().setDurability
            (new Durability(SyncPolicy.WRITE_NO_SYNC,
                            SyncPolicy.NO_SYNC,
                            ReplicaAckPolicy.SIMPLE_MAJORITY));

        private final ReplicatedEnvironment env;
        private final Logger logger;
        private final TransactionConfig config;

        public RunTransaction(ReplicatedEnvironment env,
                              TransactionConfig config,
                              Logger logger) {
            this.env = env;
            this.logger = logger;

            /*
             * For unit tests, use local WRITE_NO_SYNC rather than SYNC, for
             * improved performance.
             */
            if (TestStatus.isWriteNoSyncAllowed() &&
                !config.getReadOnly() &&
                config.getDurability().getLocalSync() == SyncPolicy.SYNC) {
                final Durability newDurability =
                    new Durability(SyncPolicy.WRITE_NO_SYNC,
                                   config.getDurability().getReplicaSync(),
                                   config.getDurability().getReplicaAck());
                this.config = config.clone().setDurability(newDurability);
            } else {
                this.config = config;
            }
        }

        /**
         * Predicate to help determine whether the runner of this transaction
         * is shutting down.
         */
        abstract protected boolean shuttingDown();

        public T run() {

            long sleepMillis = 0;
            T retVal = null;

            RuntimeException retryException = null;

            for (int i = 0;
                 !shuttingDown() && (i < TRANSACTION_RETRY_MAX); i++) {

                /* Sleep before retrying. */
                if (sleepMillis != 0) {
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException ignored) {
                        }
                    sleepMillis = 0;
                }

                if (shuttingDown()) {
                    break;
                }

                Transaction txn = null;
                try {
                    txn = env.beginTransaction(null, config);
                    retVal = doTransaction(txn);
                    /* The txn may have been closed in doTransaction */
                    TxnUtil.localCommit(txn, logger);
                    return retVal;
                } catch (InsufficientReplicasException ire) {
                    retryException = ire;
                    logger.log
                        (Level.INFO, "Retrying transaction after " +
                         "InsufficientReplicasException", ire);
                    sleepMillis = RETRY_WAIT;
                    /* Loop and retry. */

                } catch (ReplicaWriteException rwe) {
                    retryException = rwe;
                    handler(rwe);

                } catch (UnknownMasterException ume) {
                    retryException = ume;
                    handler(ume);

                } catch (LockConflictException lce) {
                    retryException = lce;
                    /*
                     * This should not ever happen, because Admin is the sole
                     * user of this database.  Nonetheless, we'll code to retry
                     * this operation.
                     */
                    logger.log
                        (Level.SEVERE,
                         "Retrying transaction after LockConflictException. " +
                         " lock owner transaction ids " +
                         Arrays.toString(lce.getOwnerTxnIds()) +
                         " lock waiter transaction ids " +
                         Arrays.toString(lce.getWaiterTxnIds()),
                         lce);
                    sleepMillis = RETRY_WAIT;
                    /* Loop and retry. */

                } catch (ReplicaConsistencyException rce) {
                    retryException = rce;
                    /*
                     * Retry the transaction to see if the replica becomes
                     * consistent.
                     */
                    logger.log(Level.WARNING,
                               "Retrying transaction after " +
                               "ReplicaConsistencyException", rce);
                    sleepMillis = RETRY_WAIT;
                    /* Loop and retry. */

                } catch (DatabaseException rre) {

                    /*
                     * If this happens, we will let the ProcessFaultHandler
                     * exit the Admin process so that the StorageNodeAgent can
                     * restart it.
                     */
                    throw(rre);

                } catch (DatabaseNotReadyException dnre) {
                    retryException = dnre;

                    final EnvironmentImpl envImpl =
                        DbInternal.getEnvironmentImpl(env);
                    if (envImpl.getDiskLimitViolation() != null) {
                        throw new DBOperationFailedException(
                            "Disk limit violated in Admin environment" +
                            " directory",
                            retryException);
                    }

                    /*
                     * Retry the transaction to see if the entity database is
                     * reopened.
                     */
                    logger.log(Level.INFO,
                               "Retrying transaction after " +
                               "DatabaseNotReadyException", dnre);
                    sleepMillis = RETRY_WAIT;
                    /* Loop and retry. */

                } finally {
                    TxnUtil.abort(txn);
                }
            }

            if (shuttingDown()) {
                throw new DBOperationFailedException("Service in shutdown");
            }
            throw new DBOperationFailedException(
                "Transaction retry limit exceeded", retryException);
        }

        /**
         * The following handlers allow for customized handling of
         * UnknownMasterException and ReplicaWriteException.
         */
        void handler(UnknownMasterException ume)
            throws AdminNotReadyException {
            /*
             * Attempted a modification while in the Replica state.
             */
            logger.log(Level.FINE,
                       "Write operation when not the master: {0}",
                       ume.getMessage());
            throw new AdminNotReadyException("Unable to complete " +
                                             "operation, admin is " +
                                             "not the master");
        }

        void handler(ReplicaWriteException rwe)
            throws AdminNotReadyException {
            /*
             * Attempted a modification while in the Replica state.
             */
            logger.log(Level.FINE,
                       "Write operation when not the master: {0}",
                       rwe.getMessage());
            throw new AdminNotReadyException("Unable to complete " +
                                             "operation, admin is " +
                                             "not the master");
        }

        /**
         * Must be implemented to perform operations using the given
         * Transaction.
         */
        protected abstract T doTransaction(Transaction txn);
    }

    /**
     * Signals that retry limit is exceeded.
     */
    public static class DBOperationFailedException extends
        IllegalStateException {

        private static final long serialVersionUID = 1L;

        public DBOperationFailedException(String msg) {
            super(msg);
        }

        public DBOperationFailedException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /* For use in error and usage messages. */
    public String getStorewideLogName() {
        return myParams.getStorageNodeParams().getHostname() + ":" +
            monitor.getStorewideLogName();
    }

    /**
     * Tells the Admin whether startup processing, done in another thread, has
     * finished. If any exceptions occur, saves the exception to transport
     * across thread boundaries.
     */
    static class StartupStatus {
        private enum Status { INITIALIZING, READY, ERROR }

        private Status status;
        private RuntimeException problem;

        StartupStatus() {
            status = Status.INITIALIZING;
        }

        void setReady(Admin admin) {
            synchronized (admin) {
                status = Status.READY;
                admin.notifyAll();
                admin.updateAdminStatus(ServiceStatus.RUNNING,
                                        null /* reason */);
            }
        }

        void setUnready(Admin admin) {
            synchronized (admin) {
                status = Status.INITIALIZING;
            }
        }

        boolean isReady(Admin admin) {
            synchronized (admin) {
                return status == Status.READY;
            }
        }

        void setError(Admin admin, RuntimeException e) {
            synchronized (admin) {
                status = Status.ERROR;
                problem = e;
                admin.notifyAll();
                admin.updateAdminStatus(ServiceStatus.ERROR_RESTARTING,
                                        (e != null) ? e.toString() : null);
            }
        }

        void waitForIsReady(Admin admin) {
            synchronized (admin) {
                while (status == Status.INITIALIZING) {
                    try {
                        admin.wait();
                    } catch (InterruptedException ie) {
                        throw new IllegalStateException
                            ("Interrupted while waiting for Admin " +
                             "initialization", ie);
                    }
                }
            }

            if (status == Status.READY) {
                return;
            }

            if (status == Status.ERROR) {
                throw problem;
            }
        }

        ServiceStatus getStatus(Admin admin) {
            synchronized (admin) {
                switch (status) {
                case INITIALIZING:
                    return ServiceStatus.STARTING;
                case READY:
                    return ServiceStatus.RUNNING;
                case ERROR:
                    return ServiceStatus.ERROR_RESTARTING;
                default:
                    throw new AssertionError();
                }
            }
        }
    }

    private KVStoreConfig getConfig() {
        final Topology topo = getCurrentTopology();
        final StorageNodeMap snMap = topo.getStorageNodeMap();
        final int nHelpers = Math.min(100, snMap.size());
        final String[] helperArray = new String[nHelpers];
        int i = 0;
        for (StorageNode sn : snMap.getAll()) {
            if (i >= nHelpers) {
                break;
            }
            helperArray[i] = sn.getHostname() + ':' + sn.getRegistryPort();
            i += 1;
        }
        final KVStoreConfig config = new KVStoreConfig(topo.getKVStoreName(),
                                                       helperArray);
        final Properties props = new Properties();
        props.setProperty(KVSecurityConstants.TRANSPORT_PROPERTY, "internal");
        config.setSecurityProperties(props);

        return config;
    }

    /**
     * Opens a new KVStore handle.  The caller is responsible for closing it.
     * <p>
     * In the future we may decide to maintain an open KVStore rather than
     * opening one whenever it is needed, if the overhead for opening is
     * unacceptable.  However, because the KVStore is associated with a user
     * login in a secure installation, we cannot blindly share KVStore handles.
     */
    public KVStore openKVStore() {
        final KVStoreConfig config = getConfig();
        final KVStoreUserPrincipal currentUser =
            KVStoreUserPrincipal.getCurrentUser();

        final LoginCredentials creds;
        if (currentUser == null) {
            creds = null;
        } else {
            LoginManager loginMgr = getLoginManager();
            creds = new ClientProxyCredentials(currentUser, loginMgr);
        }
        return KVStoreFactory.getStore(config, creds,
                                       null /* ReauthenticateHandler */);

    }

    private final Object internalKVStoreLock = new Object();

    /**
     * Opens a internal KVStore handle.
     */
    public KVStore getInternalKVStore() {
        final KVStore store = kvstore;
        if (store != null) {
            return store;
        }
        synchronized(internalKVStoreLock) {
            if (kvstore != null) {
                return kvstore;
            }
            final KVStoreConfig config = getConfig();
            final RequestDispatcherImpl requestDispatcher =
                RequestDispatcherImpl.createForAdmin(
                    config, getCurrentTopology(), getLoginManager(),
                    exceptionHandler, logger);
            /* Make sure dispatcher has the latest topo */
            requestDispatcher.
                getTopologyManager().update(getCurrentTopology());
            kvstore = KVStoreInternalFactory.getStore(
                config, requestDispatcher, getLoginManager(), logger);
            return kvstore;
        }
    }

    private void resetInternalKVStore() {
        synchronized(internalKVStoreLock) {
            if (kvstore != null) {
                kvstore.close();
            }
            kvstore = null;
        }
    }

    private TopologyDiff generateTopoDiff(String targetTopoName,
                                          String startTopoName) {
        checkIfReadonlyAdmin();
        Topology startTopo;
        if (startTopoName == null) {
            startTopo = getCurrentTopology();
        } else {
            final TopologyCandidate startCand = getCandidate(startTopoName);
            startTopo = startCand.getTopology();
        }
        final TopologyCandidate target = getCandidate(targetTopoName);
        final TopologyDiff diff = new TopologyDiff(startTopo, startTopoName,
                                                   target, parameters);
        return diff;
    }

    public String previewTopology(String targetTopoName, String startTopoName,
                                  boolean verbose, short jsonVersion) {
        if (jsonVersion == SerialVersion.ADMIN_CLI_JSON_V1_VERSION) {
            return generateTopoDiff(
                targetTopoName, startTopoName).display(verbose);
        }
        return generateTopoDiff(
            targetTopoName, startTopoName).displayJson(verbose).toString();
    }

    String changeRepFactor(final String candidateName,
                           final String snPoolName,
                           final DatacenterId dcId,
                           final int repFactor) {
        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool = freezeSNPool(snPoolName);

                try {
                    /* Create and save a new layout against the specified SNs */
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate, pool,
                                            getCurrentParameters(), myParams);

                    final TopologyCandidate newCandidate =
                        tb.changeRepfactor(repFactor, dcId);
                    stores.getTopologyStore().putCandidate(txn, newCandidate);
                    String message = "Changed replication factor in " +
                        newCandidate.getName();
                    return getSuccessMessageWithNotes(message, tb.getNotes());
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    /**
     * Change the type of a zone in a topology candidate.  Does nothing if the
     * zone already has the requested type.
     *
     * @param candidateName the name of the topology candidate to modify
     * @param dcId the ID of the zone to modify
     * @param type the desired type of the zone
     * @throws DatabaseException if there is a problem modifying the database
     */
    public String changeZoneType(final String candidateName,
                                 final DatacenterId dcId,
                                 final DatacenterType type) {
        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                final TopologyCandidate candidate = getCandidate(txn,
                                                                 candidateName);
                final Topology topo = candidate.getTopology();
                final Datacenter dc = topo.get(dcId);

                if (dc.getDatacenterType().equals(type)) {
                    return dcId + " is already of type " + type;
                }

                final StringBuilder sb = new StringBuilder();
                sb.append("Changed type of zone ").append(dcId)
                    .append(" to ").append(type)
                    .append(" in ").append(candidate.getName());

                /*
                 * Check to see if the zone type change requires modifying
                 * other contingent properties, and report any changes made
                 */
                boolean allowArbiters = dc.getAllowArbiters();
                boolean computedAllowArbiters =
                    Datacenter.computeAllowArbiters(
                        allowArbiters, type, dc.getRepFactor());
                if (allowArbiters != computedAllowArbiters) {
                    sb.append(computedAllowArbiters ?
                              "\nEnabled allow arbiters" :
                              "\nDisabled allow arbiters");
                    allowArbiters = computedAllowArbiters;
                }
                boolean masterAffinity = dc.getMasterAffinity();
                boolean computedMasterAffinity =
                    Datacenter.computeMasterAffinity(
                        masterAffinity, type, dc.getRepFactor());
                if (masterAffinity != computedMasterAffinity) {
                    sb.append(computedMasterAffinity ?
                              "\nEnabled master affinity" :
                              "\nDisabled master affinity");
                    masterAffinity = computedMasterAffinity;
                }

                topo.update(dc.getResourceId(),
                            Datacenter.newInstance(dc.getName(),
                                                   dc.getRepFactor(),
                                                   type,
                                                   allowArbiters,
                                                   masterAffinity));
                stores.getTopologyStore().putCandidate(txn, candidate);
                return sb.toString();
            }
        }.run();
    }


    public String changeZoneMasterAffinity(final String candidateName,
                                           final DatacenterId dcId,
                                           final boolean masterAffinity) {
        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                final TopologyCandidate candidate = getCandidate(txn,
                                                                 candidateName);
                final Topology topo = candidate.getTopology();
                final Datacenter dc = topo.get(dcId);

                if (dc.getMasterAffinity() == masterAffinity) {
                    return masterAffinity ?
                        "Zone " + dcId + " already has master affinity" :
                        "Zone " + dcId + " already has no master affinity";
                }

                Rules.validateMasterAffinity(
                    masterAffinity, dc.getDatacenterType(), dc.getRepFactor());

                topo.update(dc.getResourceId(),
                       Datacenter.newInstance(dc.getName(),
                                              dc.getRepFactor(),
                                              dc.getDatacenterType(),
                                              dc.getAllowArbiters(),
                                              masterAffinity));
                stores.getTopologyStore().putCandidate(txn, candidate);
                return "Changed zone master affinity for zone " + dcId +
                    " to " + masterAffinity + " in " + candidate.getName();
            }
        }.run();
    }

    /**
     * Alter the arbiter attribute of a zone in a topology candidate.
     * Does nothing if the zone already has the requested attribute value.
     *
     * @param candidateName the name of the topology candidate to modify
     * @param dcId the ID of the zone to modify
     * @param allowArbiters value of the attribute
     * @throws DatabaseException if there is a problem modifying the database
     */
    public String changeZoneArbiters(final String candidateName,
                                     final DatacenterId dcId,
                                     final boolean allowArbiters) {
        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                final TopologyCandidate candidate = getCandidate(txn,
                                                                 candidateName);
                final Topology topo = candidate.getTopology();
                final Datacenter dc = topo.get(dcId);

                if (dc.getAllowArbiters() == allowArbiters) {
                    return allowArbiters ?
                        "Zone " + dcId + " already allows arbiters" :
                        "Zone " + dcId + " already does not allow arbiters";
                }

                Rules.validateAllowArbiters(
                    allowArbiters, dc.getDatacenterType(), dc.getRepFactor());

                topo.update(dc.getResourceId(),
                            Datacenter.newInstance(dc.getName(),
                                                   dc.getRepFactor(),
                                                   dc.getDatacenterType(),
                                                   allowArbiters,
                                                   dc.getMasterAffinity()));
                stores.getTopologyStore().putCandidate(txn, candidate);
                return "Changed allow arbiters for zone " + dcId + " to " +
                        allowArbiters  + " in " + candidate.getName();
            }
        }.run();
    }

    String validateTopology(String candidateName, short jsonVersion) {
        Results results = null;
        String prefix = null;
        if (candidateName == null) {
            results = Rules.validate(getCurrentTopology(),
                                     getCurrentParameters(),
                                     true);
            prefix = "the current deployed topology";
        } else {
            final TopologyCandidate candidate = getCandidate(candidateName);
            results = Rules.validate(candidate.getTopology(),
                                     getCurrentParameters(),
                                     false);
            prefix = "topology candidate \"" + candidateName + "\"";
        }
        if (jsonVersion == SerialVersion.ADMIN_CLI_JSON_V1_VERSION) {
            return "Validation for " + prefix + ":\n" + results;
        }
        final ObjectNode top = JsonUtils.createObjectNode();
        top.put("candicateName", candidateName);
        final ArrayNode problemArray = top.putArray("problems");
        for (RulesProblem problem : results.getProblems()) {
            final ObjectNode on = JsonUtils.createObjectNode();
            on.put("resourceId", problem.getResourceId().toString());
            on.put("description", problem.toString());
            problemArray.add(on);
        }
        final ArrayNode warningArray = top.putArray("warnings");
        for (RulesProblem warning : results.getWarnings()) {
            final ObjectNode on = JsonUtils.createObjectNode();
            on.put("resourceId", warning.getResourceId().toString());
            on.put("description", warning.toString());
            warningArray.add(on);
        }
        final ArrayNode violationArray = top.putArray("violations");
        for (RulesProblem violation : results.getViolations()) {
            final ObjectNode on = JsonUtils.createObjectNode();
            on.put("resourceId", violation.getResourceId().toString());
            on.put("description", violation.toString());
            violationArray.add(on);
        }
        return top.toString();
    }

    String movePartition(String candidateName,
                         PartitionId partId,
                         RepGroupId rgId) {
        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool =
                    freezeSNPool(Parameters.DEFAULT_POOL_NAME);
                try {
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate, pool,
                                            getCurrentParameters(), myParams);
                    final TopologyCandidate newCandidate =
                        tb.movePartition(partId, rgId);
                    stores.getTopologyStore().putCandidate(txn, newCandidate);
                    return "Moved partition " + partId.getPartitionId() +
                           " to " + rgId;
                } catch (IllegalCommandException ice) {
                    throw new IllegalCommandException(ice.getMessage(),
                                                      ErrorMessage.NOSQL_5200,
                                                      new String[] {});
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    String moveRN(final String candidateName,
                  final RepNodeId rnId,
                  final StorageNodeId snId) {

        return new AdminTransaction<String>(RunTransaction.writeNoSync) {

            @Override
            protected String doTransaction(Transaction txn) {

                final TopologyCandidate candidate =
                    getCandidate(txn, candidateName);
                final StorageNodePool pool =
                    freezeSNPool(Parameters.DEFAULT_POOL_NAME);
                try {
                    /*
                     * Create and save a new layout against the specified SNs
                     */
                    final TopologyBuilder tb =
                        new TopologyBuilder(candidate, pool,
                                            getCurrentParameters(), myParams);

                    final RepNode oldRN = candidate.getTopology().get(rnId);
                    if (oldRN == null) {
                        throw new IllegalCommandException(
                            rnId + " doesn't exist, and can't be moved.",
                            ErrorMessage.NOSQL_5200,
                            new String[] {});
                    }

                    final TopologyCandidate newCandidate =
                        tb.relocateRN(rnId, snId);
                    final RepNode newRN = newCandidate.getTopology().get(rnId);
                    if (newRN == null) {
                        throw new IllegalStateException
                            (rnId +
                             " is missing from the new topology candidate: " +
                             TopologyPrinter.printTopology
                             (newCandidate.getTopology()));
                    }

                    if (!(newRN.getStorageNodeId().equals
                            (oldRN.getStorageNodeId()))) {

                        stores.getTopologyStore().putCandidate(txn,
                                                               newCandidate);
                        return "Moved " + rnId + " from " +
                            oldRN.getStorageNodeId() + " to " +
                            newRN.getStorageNodeId();
                    }

                    throw new IllegalCommandException(
                        "Couldn't find an eligible SN to house " + rnId,
                        ErrorMessage.NOSQL_5200,
                        new String[] {});
                } finally {
                    pool.thaw();
                }
            }
        }.run();
    }

    /**
     * This method adds message with the specific notes that may be there
     * during topology building.
     */
    private String getSuccessMessageWithNotes(String message,
                                              Set<String> notes) {

        StringBuilder retMsg = new StringBuilder(message);
        if (notes != null && !notes.isEmpty()) {
            notes.forEach(warning -> retMsg.append("\nNote: ").
                append(warning));
        }
        return retMsg.toString();
    }

    /*
     * These next few with @Override are for the MonitorKeeper interface.
     */
    @Override
    public Monitor getMonitor() {
        return monitor;
    }

    /**
     * Return the latency ceiling associated with the given RepNode.
     */
    @Override
    public int getLatencyCeiling(ResourceId rnid) {
        if (rnid instanceof RepNodeId) {
            checkIfReadonlyAdmin();
            final RepNodeParams rnp = parameters.get((RepNodeId) rnid);
            return rnp == null ? 0 : rnp.getLatencyCeiling();
        }

        return 0;
    }

    /**
     * Return the throughput floor associated with the given RepNode.
     */
    @Override
    public int getThroughputFloor(ResourceId rnid) {
        if (rnid instanceof RepNodeId) {
            checkIfReadonlyAdmin();
            final RepNodeParams rnp = parameters.get((RepNodeId) rnid);
            return rnp == null ? 0 : rnp.getThroughputFloor();
        }
        return 0;
    }

    private void updateAdminStatus(ServiceStatus newStatus, String reason) {
        if (owner == null) {
            return;
        }
        owner.updateAdminStatus(this, newStatus, reason);
    }

    /**
     * Gets the JE replication group admin for the specified Admin.
     */
    public ReplicationGroupAdmin getReplicationGroupAdmin(AdminId targetId) {

        /*
         * Use the local service parameters if targetId matches this service's
         * admin ID and there are no parameters because this admin was a
         * replica
         */
        final AdminParams targetAP;
        if (parameters != null) {
            targetAP = parameters.get(targetId);
        } else if (targetId.equals(adminId)) {
            targetAP =  myParams.getAdminParams();
        } else {
            targetAP = null;
        }
        if (targetAP == null) {
            return null;
        }

        String allNodes = targetAP.getNodeHostPort();
        final String helpers = targetAP.getHelperHosts();
        if (!"".equals(helpers) && (helpers != null)) {
            allNodes += ParameterUtils.HELPER_HOST_SEPARATOR + helpers;
        }
        final GlobalParams globalParams = (parameters != null) ?
            parameters.getGlobalParams() :
            myParams.getGlobalParams();
        final String storeName = globalParams.getKVStoreName();
        final String groupName = getAdminRepGroupName(storeName);

        return getReplicationGroupAdmin(groupName, allNodes);
    }

    /**
     * Gets the JE replication group admin for the specified RN.
     */
    public ReplicationGroupAdmin getReplicationGroupAdmin(RepNodeId targetId) {
        checkIfReadonlyAdmin();
        final RepNodeParams rnp = parameters.get(targetId);
        if (rnp == null) {
            return null;
        }
        String allNodes = rnp.getJENodeHostPort();
        final String helpers = rnp.getJEHelperHosts();
        if (!"".equals(helpers) && (helpers != null)) {
            allNodes += ParameterUtils.HELPER_HOST_SEPARATOR + helpers;
        }

        return getReplicationGroupAdmin(targetId.getGroupName(), allNodes);
    }

    /**
     * Gets the JE replication group for the group with the specified name by
     * contacting the specified helpers.
     */
    public ReplicationGroupAdmin
                          getReplicationGroupAdmin(String groupName,
                                                   String targetHelperHosts) {
        final Set<InetSocketAddress> helperSockets = new HashSet<>();
        final StringTokenizer tokenizer =
            new StringTokenizer(targetHelperHosts,
                                ParameterUtils.HELPER_HOST_SEPARATOR);
        while (tokenizer.hasMoreTokens()) {
            final String helper = tokenizer.nextToken();
            helperSockets.add(HostPortPair.getSocket(helper));
        }
        return new ReplicationGroupAdmin(groupName, helperSockets,
                                         repConfig.getRepNetConfig());
    }

    /**
     * Returns the JE replication group obtained from the group admin.
     *
     * @param rga the group admin
     * @return the replication group
     * @throws IllegalStateException if the group master is not found before
     * the timeout or if the call fails for another reason
     */
    public static ReplicationGroup getReplicationGroup(
        ReplicationGroupAdmin rga)
        throws IllegalStateException {

        /* TODO: Add KV parameters for the check or timeout values? */
        final long checkMillis = 1000;

        /*
         * Use a long enough timeout to account for the fact that JE HA master
         * election retries have a power-of-two backoff that can wait as long
         * as 32 seconds between attempts
         */
        final long timeoutMillis = 90000;
        final long stop = System.currentTimeMillis() + timeoutMillis;
        try {
            while (true) {
                try {
                    return rga.getGroup();
                } catch (UnknownMasterException e) {
                    final long now = System.currentTimeMillis();
                    if (now > stop) {
                        throw e;
                    }
                    Thread.sleep(checkMillis);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                "Problem getting replication group membership: " +
                e.getMessage(),
                e);
        }
    }

    /**
     * Removes the specified admin replica from its replication group.  It must
     * exist and not be the only admin the store.
     */
    public void removeAdminFromRepGroup(AdminId victim) {

        logger.info("Removing Admin replica " + victim +
                    " from the replication group.");

        final ReplicationGroupAdmin rga = getReplicationGroupAdmin(victim);
        if (rga == null) {
            throw new MemberNotFoundException
                ("The admin " + victim + " is not in the rep group.");
        }

        /* This call may also throw MemberNotFoundException. */
        final String victimNodeName = getAdminRepNodeName(victim);

        final int nAdmins = getAdminCount();
        if (nAdmins == 1) {
            throw new NonfatalAssertionException
                ("Attempting to remove the sole Admin instance" + victim);
        }

        rga.removeMember(victimNodeName);
    }

    /**
     * Causes the mastership of the admin db replication group to change to
     * another member.
     */
    public void transferMaster() {
        checkIfReadonlyAdmin();
        logger.info("Transferring Admin mastership");
        final Set<String> replicas = new HashSet<>();
        for (final AdminParams adminParams : parameters.getAdminParams()) {
            final AdminId aId = adminParams.getAdminId();
            if (adminId.equals(aId)) {
                continue;
            }
            if (!adminParams.getType().isPrimary()) {
                continue;
            }
            replicas.add(getAdminRepNodeName(aId));
        }

        try {
            environment.transferMaster(replicas, 60, TimeUnit.SECONDS,
               "Executing plan that requires this admin node to not be the " +
               "master.");
        } catch (Exception e) {
            throw new NonfatalAssertionException
                ("Master transfer failed", e);
        }
    }

    /**
     * Checks whether all of the nodes in the store are at or above the minor
     * version of {@code requiredVersion}.  Returns {@code true} if the
     * required version is supported by all nodes, and {@code false} if at
     * least one node does not support the required version.  Throws {@link
     * AdminFaultException} if there is a problem determining the versions of
     * the storage nodes or if there is a node does not support the
     * prerequisite version.  Once {@code true} is returned for a given
     * version, future calls to this method will always return {@code true}.
     *
     * <p>Version dependent features should keep calling this method until the
     * required feature is enabled.  Of course, features should be careful to
     * not constantly call these methods at a high rate.
     *
     * @param requiredVersion version to check against
     * @return {@code true} if all of the nodes in the store meet the required
     *         version, and {@code false} if a least one node does not
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    public boolean checkStoreVersion(KVVersion requiredVersion) {
        return VersionUtil.compareMinorVersion(getStoreVersion(),
                                               requiredVersion) >= 0;
    }

    /**
     * Checks whether all of the storage nodes in the store hosting an admin
     * instance are at or above the minor version of {@code requiredVersion}.
     *
     * @param requiredVersion version to check against
     * @return {@code true} if all of the nodes hosting an admin instance meet
     *         the required version, and {@code false} if a least one node does
     *         not
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    public boolean checkAdminGroupVersion(KVVersion requiredVersion) {
        return checkAdminGroupVersion(requiredVersion,
                                      Collections.<DatacenterId>emptySet(),
                                      null, null, false);
    }

    /**
     * Checks whether all of the storage nodes in the store hosting an admin
     * instance are at or above the minor version of {@code requiredVersion},
     * skipping nodes which are down due to failure of specified shard.
     *
     * @param requiredVersion version to check against
     * @param failedShard the RepGroupId of failed shard, or null for no failed
     * shard
     * @return {@code true} if all of the nodes hosting an admin instance meet
     *         the required version, and {@code false} if a least one node does
     *         not
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    public boolean checkAdminGroupVersion(KVVersion requiredVersion,
                                          RepGroupId failedShard) {
        return checkAdminGroupVersion(requiredVersion,
                                      Collections.<DatacenterId>emptySet(),
                                      failedShard, null, false);
    }

    /**
     * Checks whether all of the storage nodes in the store hosting an admin
     * instance are at or above the minor version of {@code requiredVersion},
     * skipping node which is hosting to be removed admin on failed SN.
     *
     * @param requiredVersion version to check against
     * @param victim the AdminId of to be removed admin
     * @param failedSN true if to be removed admin hosted on failed sn,
     * else false
     * @return {@code true} if all of the nodes hosting an admin instance meet
     *         the required version, and {@code false} if a least one node does
     *         not
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    public boolean checkAdminGroupVersion(KVVersion requiredVersion,
                                          AdminId victim,
                                          boolean failedSN) {
        return checkAdminGroupVersion(requiredVersion,
                                      Collections.<DatacenterId>emptySet(),
                                      null, victim, failedSN);
    }

    /**
     * Checks whether all of the storage nodes in the store hosting an admin
     * instance are at or above the minor version of {@code requiredVersion},
     * skipping nodes in the specified set of offline zones and nodes which
     * are down due to failure of specified shard and node hosting to be
     * removed admin on failed SN.
     *
     * @param requiredVersion version to check against
     * @param offlineZones the IDs of offline zones
     * @param failedShard the RepGroupId of failed shard, or null for no failed
     * shard
     * @param victim the AdminId of to be removed admin
     * @param failedSN true if to be removed admin is hosted on failed sn,
     * else false
     * @return {@code true} if all of the nodes hosting an admin instance meet
     *         the required version, and {@code false} if a least one node does
     *         not
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    private boolean checkAdminGroupVersion(KVVersion requiredVersion,
                                           Set<DatacenterId> offlineZones,
                                           RepGroupId failedShard,
                                           AdminId victim,
                                           boolean failedSN) {
        return VersionUtil.compareMinorVersion(
            getAdminGroupVersion(offlineZones, failedShard, victim, failedSN),
                                 requiredVersion) >= 0;
    }

    /**
     * Gets the highest minor version that all of the nodes in the store are
     * known to be at and support. Throws {@link AdminFaultException} if there
     * is a problem determining the versions of the storage nodes or if there
     * is a node does not support the prerequisite version. Note that nodes may
     * be at different patch levels and this method returns the lowest patch
     * level found.
     *
     * <p>When using this method to enable version sensitive features, callers
     * should compare the returned version to the desired version using {@link
     * oracle.kv.impl.util.VersionUtil#compareMinorVersion}.
     *
     * <p>Note that it is assumed that new nodes added to the store will be at
     * the latest version. This method will not detect new nodes being added
     * which have older software versions.
     *
     * @return the highest minor version that all of the nodes support
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    public KVVersion getStoreVersion() {

        if (VersionUtil.compareMinorVersion(storeVersion,
                                            KVVersion.CURRENT_VERSION) >= 0) {
            return storeVersion;
        }

        if (parameters == null) {
            return storeVersion;
        }

        /*
         * If not at the current minor version see if the global store
         * parameter has been updated.
         */
        final String sv = parameters.getGlobalParams().getStoreVersion();
        if (sv == null) {
            /* Not yet set */
            return storeVersion;
        }
        final KVVersion v = KVVersion.parseVersion(sv);
        synchronized (this) {
            if (v.compareTo(storeVersion) > 0) {
                storeVersion = v;
            }
            return storeVersion;
        }
    }

    /**
     * Gets the highest minor version that all storage nodes in the store
     * hosting an admin instance are known to be at and support.
     *
     * <p>When using this method to enable version sensitive features, callers
     * should compare the returned version to the desired version using {@link
     * VersionUtil#compareMinorVersion}.
     *
     * @return the highest minor version that the nodes hosting an admin
     *         instance support
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    KVVersion getAdminGroupVersion() {
        return getAdminGroupVersion(Collections.<DatacenterId>emptySet(),
                                    null, null, false);
    }

    /**
     * Gets the highest minor version that all storage nodes in the store
     * hosting an admin instance are known to be at and support, skipping nodes
     * in the specified set of offline zones and nodes which are down due to
     * failure of specified shard and node hosting to be removed admin on
     * failed SN.
     *
     * <p>When using this method to enable version sensitive features, callers
     * should compare the returned version to the desired version using {@link
     * VersionUtil#compareMinorVersion}.
     *
     * @param offlineZones the IDs of offline zones
     * @param failedShard the RepGroupId of failed shard, or null for no failed
     * shard
     * @param victim the AdminId of to be removed admin
     * @param failedSN true if to be removed admin hosted on failed sn,
     * else false
     * @return the highest minor version that all of the nodes hosting admin
     *         instance support
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    KVVersion getAdminGroupVersion(Set<DatacenterId> offlineZones,
                                   RepGroupId failedShard,
                                   AdminId victim,
                                   boolean failedSN) {
        if (VersionUtil.compareMinorVersion(adminVersion,
                                            KVVersion.CURRENT_VERSION) >= 0) {
            return adminVersion;
        }
        final List<StorageNodeId> snIds = new ArrayList<>();
        Parameters params = getCurrentParameters();
        Topology topo = getCurrentTopology();

        for (AdminId aId : params.getAdminIds()) {
            /*
             * If to be removed admin is hosted on a failed SN then skip
             * adding failed SN to the list of version check
             */
            if (victim != null &&
                (aId.getAdminInstanceId() == victim.getAdminInstanceId()) &&
                failedSN) {
                continue;
            }
            final StorageNodeId snId = params.get(aId).getStorageNodeId();
            if (offlineZones.contains(topo.getDatacenterId(snId))) {
                continue;
            }
            /*
             * In case of failed shard if capacity = 1 then skip
             * adding failed SN belonging to failed shard which is
             * hosting an admin. In case of capacity > 1, we assume
             * that SNs are up and running, only RNs have failed.
             * Hence we do not need to skip SNs in capacity > 1.
             */
            if (failedShard != null) {
                Set<RepNodeId> hostedRepNodes =
                    topo.getHostedRepNodeIds(snId);
                if (hostedRepNodes.size() == 1 &&
                    (hostedRepNodes.iterator().next().getGroupId() ==
                     failedShard.getGroupId())) {
                    continue;
                }
            }
            snIds.add(snId);
        }

        final KVVersion v = getSNsVersion(snIds);

        synchronized (this) {
            if (v.compareTo(adminVersion) > 0) {
                adminVersion = v;
            }
            return adminVersion;
        }
    }

    /**
     * Returns a map of the other Admins in the group with their software
     * version. If there is a problem getting the information, null is returned.
     *
     * @return map of the other Admins in the group or null
     */
    Map<AdminId, KVVersion> getOtherAdminVersions() {
        final Map<AdminId, KVVersion> map = new HashMap<>();
        try {
            final RegistryUtils registryUtils = createRegistryUtils();
            final Parameters params = getCurrentParameters();
            for (AdminId aId : params.getAdminIds()) {
                if (aId.equals(adminId)) {
                    continue;
                }
                final StorageNodeId snId = params.get(aId).getStorageNodeId();
                final KVVersion v =
                                getSNStatus(snId, registryUtils).getKVVersion();
                map.put(aId, v);
            }
        } catch (Exception e) {
            return null;
        }
        return map;
    }

    /**
     * Check on all the SNs which host admins to return the highest software
     * version of all the running admins.
     *
     * @return the highest software version of all the nodes hosting admin
     * @throws AdminFaultException if there was a problem checking versions
     */
    KVVersion getAdminHighestVersion() {
        final List<StorageNodeId> snIds = new ArrayList<>();
        Parameters params = getCurrentParameters();

        for (AdminId aId : params.getAdminIds()) {
            snIds.add(params.get(aId).getStorageNodeId());
        }

        KVVersion v = KVVersion.CURRENT_VERSION;

        final RegistryUtils registryUtils = createRegistryUtils();

        for (StorageNodeId snId : snIds) {
            StorageNodeStatus snStatus = getSNStatus(snId, registryUtils);
            final KVVersion snVersion = snStatus.getKVVersion();
            if (VersionUtil.compareMinorVersion(snVersion, v) > 0) {
                v = snVersion;
            }
        }

        return v;
    }

    /**
     * Gets the highest minor version that given list of the nodes in the store
     * are known to be at and support. Throws {@link AdminFaultException} if
     * there is a problem determining the versions of the storage nodes or if
     * there is a node does not support the prerequisite version. Note that
     * nodes may be at different patch levels and this method returns the
     * lowest patch level found.
     *
     * <p>Note that it is assumed that new nodes added to the store will be at
     * the latest version. This method will not detect new nodes being added
     * which have older software versions.
     *
     * @param  snIds given list of storage node Ids
     * @return the highest minor version that given nodes support
     * @throws AdminFaultException if there was a problem checking versions or
     *         a node does not support the prerequisite version
     */
    private KVVersion getSNsVersion(List<StorageNodeId> snIds) {

        final RegistryUtils registryUtils = createRegistryUtils();

        /* Start at our highest version, downgrade as needed. */
        KVVersion v = KVVersion.CURRENT_VERSION;

        for (StorageNodeId snId : snIds) {
            KVVersion snVersion = null;
            if (parameters != null) {
                ParameterMap snParams = parameters.get(snId).getMap();
                String version =
                    snParams.get(ParameterState.SN_SOFTWARE_VERSION).asString();
                if (version != null) {
                    snVersion = KVVersion.parseVersion(version);
                }
            }
            if (snVersion == null) {
                StorageNodeStatus snStatus = getSNStatus(snId, registryUtils);
                snVersion = snStatus.getKVVersion();
            }

            if (snVersion.compareTo(KVVersion.PREREQUISITE_VERSION) < 0) {
                final String prereq =
                    KVVersion.PREREQUISITE_VERSION.getNumericVersionString();
                throw new AdminFaultException(
                    new IllegalCommandException(
                        "Node " + snId + " is at software version " +
                        snVersion.getNumericVersionString() +
                        " which does not meet the current prerequisite." +
                        " It must be upgraded to version " + prereq +
                        " or greater."));
            }

            /* If we found someone of lesser minor version, downgrade */
            if (VersionUtil.compareMinorVersion(snVersion, v) < 0) {
                v = snVersion;
            }
        }

        return v;
    }

    /**
     * Return the node status of a given target SN.
     *
     * @param  snId the target SN's id.
     * @param  utils the registry utils to get the status of the target SN.
     * @return The status of the given SN.
     * @throws AdminFaultException if there was a problem getting the SN
     *         status.
     */
    StorageNodeStatus getSNStatus (StorageNodeId snId, RegistryUtils utils) {
        try {
            return utils.getStorageNodeAgent(snId).ping();
        } catch (RemoteException | NotBoundException e) {
            throw new AdminFaultException(e);
        }
    }

    /**
     * Checks whether the node hosting the current admin master instance is at
     * or above the minor version of {@code requiredVersion}. Throws
     * {@link AdminFaultException} if there is a problem determining the
     * version of the admin master or if it does not support the prerequisite
     * version.
     *
     * @param requiredVersion version to check against
     * @return {@code true} if the node in the store hosting admin master
     *         instance meet the required version, and {@code false} otherwise
     * @throws AdminFaultException if there was a problem checking version, or
     *         the admin is in detached mode, or the admin master does not
     *         support the prerequisite version
     */
    boolean checkAdminMasterVersion(KVVersion requiredVersion) {
        final KVVersion masterVersion;
        final AdminId currentMaster = masterId;

        if (currentMaster == null) {
            throw new AdminFaultException(
                new NonfatalAssertionException(
                    "Admin is in DETACHED mode. The master is unknown."));
        }

        if (currentMaster.equals(adminId)) {
            masterVersion = KVVersion.CURRENT_VERSION;
        } else {
            final Parameters p = getCurrentParameters();
            final AdminParams ap = p.get(currentMaster);
            masterVersion =
                getSNsVersion(Arrays.asList(ap.getStorageNodeId()));
        }

        return VersionUtil.compareMinorVersion(masterVersion,
                                               requiredVersion) >= 0;
    }

    /**
     * Returns the table metadata for read.
     */
    public TableMetadata getTableMetadata() {
        TableMetadata md = getMetadata(TableMetadata.class,
                                       MetadataType.TABLE);
        /* If the table MD has not been created return an empty instance */
        return (md != null) ? md : new TableMetadata(false);
    }

    /**
     * Returns the Metadata objects of the specified type.
     */
    @Nullable
    public <T extends Metadata<? extends MetadataInfo>> T
        getMetadata(final Class<T> returnType,
                    final MetadataType metadataType) {
        logger.log(Level.FINE, "Getting {0} metadata", metadataType);

        /*
         * getMetadata with RunTransaction.sync would fail with
         * DBOperationFailedException when admin lose quorum.
         * changed it to RunTransaction.readOnly because
         * stores.getMetadata() is a read operation. This allows
         * admin to be able to read if it lose quorum or
         * it's a read only admin.
         */
        return new AdminTransaction<T>(RunTransaction.readOnly) {

            @Override
            protected T doTransaction(Transaction txn) {
                /*
                 * In TableDatabaseStore.getTableMetadata(), if txn is
                 * null, it uses LockMode.READ_COMMITTED, otherwise,
                 * it uses LockMode.RMW.
                 * txn is created in runTransaction.run(), in this txn,
                 * only read operation is done. so it does not need
                 * LockMode.RMW in TableDatabaseStore.getTableMetadata().
                 * that's why we passed null to this function.
                 * If different read/write operations share the same txn,
                 * we should pass non-null txn to
                 * TableDatabaseStore.getTableMetadata(). That's the case
                 * of
                 * public <T extends Metadata<? extends MetadataInfo>> T
                 *     getMetadata(final Class<T> returnType,
                 *     final MetadataType metadataType,
                 *     Transaction txn)
                 */
                return stores.getMetadata(returnType, metadataType, null);
            }
        }.run();
    }

    /**
     * Returns the Metadata objects of the specified type in an existing
     * transaction.
     */
    public <T extends Metadata<? extends MetadataInfo>> T
        getMetadata(final Class<T> returnType,
                    final MetadataType metadataType,
                    Transaction txn) {
        logger.log(Level.FINE, "Getting {0} metadata", metadataType);

        return stores.getMetadata(returnType, metadataType, txn);
    }

    /**
     * Saves the specified Metadata object in the admin's store.
     */
    public void saveMetadata(final Metadata<?> metadata, final Plan plan) {
        logger.log(Level.FINE, "Storing {0} ", metadata);

        new AdminTransaction<Void>(RunTransaction.sync) {

            @Override
            protected Void doTransaction(Transaction txn) {

                if (plan != null) {
                    /*
                     * Inform the plan that the metadata is being updated. Save
                     * the plan if needed. Be sure to take the plan mutex before
                     * any JE locks are acquired in this transaction, per the
                     * synchronization hierarchy.
                     */
                    synchronized (plan) {
                        if (plan.updatingMetadata(metadata)) {
                            stores.putPlan(txn, plan);
                        }
                    }
                }

                saveMetadata(metadata, txn);
                return null;
            }
        }.run();
    }

    /**
     * Saves the specified Metadata object in the admin's store in an existing
     * transaction.
     *
     * The metadata is pruned before being written.
     */
    public void saveMetadata(final Metadata<?> metadata, Transaction txn) {
        saveMetadata(metadata, txn, false);
    }

    /**
     * Saves the specified Metadata object in the admin's store in an existing
     * transaction. If noOverwrite is true and the metadata object is already
     * in the store the new value will not be written and true is returned.
     *
     * The metadata is pruned before being written (successful or not).
     *
     * @return true if noOverwrite is true and the metadata object already
     *              exist
     */
    public boolean saveMetadata(final Metadata<?> metadata,
                                Transaction txn,
                                boolean noOverwrite) {
        metadata.pruneChanges(Integer.MAX_VALUE, MAX_MD_CHANGE_HISTORY);
        logger.log(Level.FINE, "Storing {0} ", metadata);
        return stores.putMetadata(metadata, txn, noOverwrite);
    }

    /**
     * Creates a transaction context for the instance of UpdateMetadata.
     * Returns the updated Metadata object.
     */
    public <T extends Metadata<?>> T updateMetadata(
        final UpdateMetadata<?> updateMetadata) {

        logger.log(Level.FINE, "Update Metadata {0} ", updateMetadata);

        return new AdminTransaction<T>(RunTransaction.sync) {

            @SuppressWarnings("unchecked")
            @Override
            protected T doTransaction(Transaction txn) {
                return (T)updateMetadata.doUpdateMetadata(txn);
            }
        }.run();
    }

    /**
     * Returns the LoginManager for the admin.
     */
    public LoginManager getLoginManager() {
        if (owner == null) {
            return null;
        }
        return owner.getLoginManager();
    }

    public AdminService getOwner() {
        return owner;
    }

    boolean isReady() {
        return startupStatus.isReady(this);
    }

    void installSecurityUpdater() {
        if (owner != null) {
            final AdminSecurity aSecurity = owner.getAdminSecurity();
            final LoginService loginService = owner.getLoginService();
            final LoginUpdater loginUpdater = new LoginUpdater();
            final SecurityMDUpdater secMDUpdater = new SecurityMDUpdater();
            final IDCSOAuthAuthenticator idcsAuthenticator =
                aSecurity.getIDCSOAuthAuthenticator();

            loginUpdater.addServiceParamsUpdaters(aSecurity);
            loginUpdater.addGlobalParamsUpdaters(aSecurity);
            secMDUpdater.addRoleChangeUpdaters(aSecurity);
            secMDUpdater.addUserChangeUpdaters(aSecurity);

            if (loginService != null) {
                loginUpdater.addServiceParamsUpdaters(loginService);
                loginUpdater.addGlobalParamsUpdaters(loginService);
                addSecurityMDListener(loginService);
            }

            if (idcsAuthenticator != null) {
                loginUpdater.addGlobalParamsUpdaters(idcsAuthenticator);
            }

            addParameterListener(loginUpdater.new ServiceParamsListener());
            addGlobalParameterListener(
                loginUpdater.new GlobalParamsListener());
            addSecurityMDListener(secMDUpdater.new RoleChangeListener());
            addSecurityMDListener(secMDUpdater.new UserChangeListener());
        }
    }

    public RoleResolver getRoleResolver() {
        if (owner == null) {
            return null;
        }
        return owner.getRoleResolver();
    }

    /* -- repairAdminQuorum -- */

    /**
     * Repairs the admin quorum by updating the JE HA rep group membership to
     * match the currently available admins, setting an electable group size
     * override on each admin to obtain quorum, and converting requested
     * secondary admins to primary admins.  If there are no existing primary
     * admins, establishes one secondary as the initial master by resetting the
     * JE replication group, which updates the group membership and allows the
     * other secondary admins to join.
     *
     * <p>First checks that the admins specified by the zoneIds and adminIds
     * parameters match the currently available admin nodes.  The nodes
     * specified must be currently available, and must include all available
     * primary nodes.  If nodes are specified by zone, then the zone must be
     * present in the topology and all nodes in the specified zone must be
     * available.
     *
     * <p>Then attempts to modify admin parameters and adjust the JE rep group
     * membership.  The command fails if any of those operations fail.
     *
     * <p>Returns null if the repair should be retried after the current admin
     * is restarted.
     *
     * <p>Since this method changes the types of admins without changing the
     * associated zone types, and also changes the types of offline admins
     * which cannot be notified of the change, the caller should call 'plan
     * failover' and 'plan repair-topology' commands to complete the repairs.
     *
     * @param zoneIds include admins in zones with these IDs
     * @param adminIds include admins with these IDs
     * @return the current set of admins or null
     * @throws IllegalCommandException if a requested admin is not found, if a
     * primary admin is found that was not requested, or if a requested zone is
     * not found
     * @throws IllegalStateException if there is a problem repairing the admin
     * quorum
     */
    public Set<AdminId> repairAdminQuorum(Set<DatacenterId> zoneIds,
                                          Set<AdminId> adminIds) {
        logger.info("Repair admin quorum: zones: " + zoneIds +
                    ", admins: " + adminIds);
        final Topology topo = getCurrentTopology();
        final RegistryUtils regUtils = createRegistryUtils();
        final Parameters params = getCurrentParameters();
        final Map<AdminId, AdminParams> allAdmins =
            findAllAdmins(regUtils, params);
        final SortedSet<AdminId> requestedAdmins =
            collectRequestedAdmins(zoneIds, adminIds, topo, params, allAdmins);
        logger.info("Repair admin quorum: requested admins: " +
                    requestedAdmins);

        int existingPrimaries = 0;
        for (final AdminId aId : requestedAdmins) {
            if (allAdmins.get(aId).getType().isPrimary()) {
                existingPrimaries++;
            }
        }

        /*
         * If there are no existing primary nodes, then establish the initial
         * master by using the first node to reset the JE replication group.
         * This operation resets the group membership, so there is no need to
         * delete members explicitly or set the electable group size override.
         */
        if (existingPrimaries == 0) {
            logger.info("Repair admin quorum: no existing primaries");
            boolean nextIsFirst = true;
            for (final AdminId aId : requestedAdmins) {
                final boolean first = nextIsFirst;
                nextIsFirst = false;
                final AdminParams adminParams = allAdmins.get(aId);
                if (repairAdminParams(regUtils, adminParams, 0, first)) {

                    /* Restarting the current admin */
                    return null;
                }
            }
            return requestedAdmins;
        }

        /*
         * Update the electable group size override on existing primary nodes
         * to establish quorum.  Although we could use reset rep group to do
         * this, it is probably safer to do it by modifying the electable group
         * size override, since that allows the nodes to perform an election.
         */
        for (final AdminId aId : requestedAdmins) {
            final AdminParams adminParams = allAdmins.get(aId);
            if (adminParams.getType().isPrimary()) {
                final boolean restart = repairAdminParams(
                    regUtils, adminParams, existingPrimaries, false);
                assert !restart : "Primary nodes should not need restarting";
            }
        }

        /*
         * Update the JE HA group membership information, if needed, to
         * remove nodes that are not in the requested set.
         */
        final ReplicationGroupAdmin rga = getReplicationGroupAdmin(adminId);
        final ReplicationGroup rg = getReplicationGroup(rga);
        final Set<InetSocketAddress> adminSockets = new HashSet<>();
        for (final AdminParams adminParams : allAdmins.values()) {
            adminSockets.add(
                HostPortPair.getSocket(adminParams.getNodeHostPort()));
        }
        for (final ReplicationNode jeRN : rg.getElectableNodes()) {
            if (!adminSockets.contains(jeRN.getSocketAddress())) {
                final String name = jeRN.getName();
                logger.info("Repair admin quorum: delete member: " + name);
                try {
                    rga.deleteMember(name);
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "Problem updating admin group membership for node " +
                        name + ": " + e.getMessage(),
                        e);
                }
            }
        }

        /* Clear special parameters on the primary nodes */
        for (final AdminId aid : requestedAdmins) {
            final AdminParams adminParams = allAdmins.get(aid);
            if (adminParams.getType().isPrimary()) {
                boolean restart =
                    repairAdminParams(regUtils, adminParams, 0, false);
                assert !restart : "Primary nodes should not need restarting";
            }
        }

        /*
         * Convert any secondary nodes to primary nodes after quorum has been
         * established so that they can join the existing group.
         */
        if (existingPrimaries < requestedAdmins.size()) {
            for (final AdminId aid : requestedAdmins) {
                final AdminParams adminParams = allAdmins.get(aid);
                if (!adminParams.getType().isPrimary()) {
                    if (repairAdminParams(regUtils, adminParams, 0, false)) {

                        /* Restarting the current admin */
                        return null;
                    }
                }
            }
        }
        return requestedAdmins;
    }

    /** Collect admin parameters from all available admins. */
    private Map<AdminId, AdminParams> findAllAdmins(RegistryUtils regUtils,
                                                    Parameters params) {

        /*
         * TODO: Maybe scan other SNs, perhaps with a standard utility, to see
         * if they have admins not included in the parameters?
         */
        final Map<AdminId, AdminParams> allAdmins = new HashMap<>();
        final List<AdminId> adminsToCheck =
            new ArrayList<>(params.getAdminIds());
        for (final AdminId aId : adminsToCheck) {
            final StorageNodeId snId = params.get(aId).getStorageNodeId();
            try {
                final CommandServiceAPI cs = regUtils.getAdmin(snId);
                final LoadParameters adminServiceParams = cs.getParams();
                allAdmins.put(aId,
                              new AdminParams(
                                  adminServiceParams.getMapByType(
                                      ParameterState.ADMIN_TYPE)));
            } catch (RemoteException | NotBoundException e) {
                logger.info("Admin " + aId + " is unreachable");
            }
        }
        return allAdmins;
    }

    /**
     * Returns all of the admins in the specified zone IDs and admin IDs,
     * checking that all of the admins specified are available and that they
     * include all available primary admins.  Returns a sorted set so callers
     * that iterate over the set will behave the same way if the same repair is
     * performed more than once.
     *
     * @return the requested admins
     * @throws IllegalCommandException if zones are not found or requested
     * admins do not match available ones
     */
    private SortedSet<AdminId> collectRequestedAdmins(
        Set<DatacenterId> zoneIds,
        Set<AdminId> adminIds,
        Topology topo,
        Parameters params,
        Map<AdminId, AdminParams> allAdmins) {

        final StringBuilder errorMessage = new StringBuilder();

        final Set<DatacenterId> missingZones = new HashSet<>();
        for (final DatacenterId dcId : zoneIds) {
            if (topo.get(dcId) == null) {
                missingZones.add(dcId);
            }
        }
        if (!missingZones.isEmpty()) {
            errorMessage.append("\n  Zones not found: ").append(missingZones);
        }

        final SortedSet<AdminId> requestedAdmins = new TreeSet<>(adminIds);
        for (final AdminId aId : params.getAdminIds()) {
            final StorageNodeId snId = params.get(aId).getStorageNodeId();
            final Datacenter dc = topo.getDatacenter(snId);
            if (zoneIds.contains(dc.getResourceId())) {
                requestedAdmins.add(aId);
            }
        }

        final Set<AdminId> missingPrimaries = new HashSet<>();
        for (final AdminParams adminParams : allAdmins.values()) {
            if (adminParams.getType().isPrimary()) {
                final AdminId aId = adminParams.getAdminId();
                if (!requestedAdmins.contains(aId)) {
                    missingPrimaries.add(aId);
                }
            }
        }
        if (!missingPrimaries.isEmpty()) {
            errorMessage.append("\n  Available primary admins not specified: ")
                .append(missingPrimaries);
        }

        final Set<AdminId> offlineAdmins = new HashSet<>(requestedAdmins);
        offlineAdmins.removeAll(allAdmins.keySet());
        if (!offlineAdmins.isEmpty()) {
            errorMessage.append("\n  Requested admins not found: ")
                .append(offlineAdmins);
        }

        if (errorMessage.length() > 0) {
            throw new IllegalCommandException(
                "Problems repairing admin quorum" + errorMessage);
        }

        return requestedAdmins;
    }

    /**
     * Update the node parameters as needed to make it a primary node, and have
     * the requested electable group size override and reset rep group
     * settings.  Returns true if the node requires a restart and it is the
     * currently running admin.
     *
     * @param regUtils registry utilities
     * @param adminParams the current admin parameters
     * @param groupSizeOverride the requested electable group size override
     * @param resetRepGroup the requested value for reset rep group
     * @return whether the current node needs to be restarted
     * @throws IllegalStateException if the update fails
     */
    private boolean repairAdminParams(RegistryUtils regUtils,
                                      AdminParams adminParams,
                                      int groupSizeOverride,
                                      boolean resetRepGroup) {

        final AdminId aId = adminParams.getAdminId();
        logger.info("Repair admin params: " + aId +
                    ", groupSizeOverride: " + groupSizeOverride +
                    ", resetRepGroup: " + resetRepGroup);

        final boolean currentIsPrimary = adminParams.getType().isPrimary();
        final int currentGroupSizeOverride =
            adminParams.getElectableGroupSizeOverride();
        final boolean currentResetRepGroup = adminParams.getResetRepGroup();

        assert !resetRepGroup || !currentIsPrimary
            : "Only reset replication group for secondary node";

        /* Check if node is OK as is */
        if (currentIsPrimary &&
            (currentGroupSizeOverride == groupSizeOverride) &&
            (currentResetRepGroup == resetRepGroup)) {
            logger.info("Repair admin params: OK: " + aId);
            return false;
        }

        final AdminParams newAdminParams =
            new AdminParams(adminParams.getMap());
        newAdminParams.setType(AdminType.PRIMARY);
        newAdminParams.setElectableGroupSizeOverride(groupSizeOverride);
        newAdminParams.setResetRepGroup(resetRepGroup);
        final StorageNodeId snId = adminParams.getStorageNodeId();
        try {
            final StorageNodeAgentAPI sna = regUtils.getStorageNodeAgent(snId);
            sna.newAdminParameters(newAdminParams.getMap());
            if (currentIsPrimary) {
                logger.info("Repair admin params: no restart: " + aId);
                regUtils.getAdmin(snId).newParameters();
                return false;
            }
            if (!adminId.equals(aId)) {
                logger.info("Repair admin params: restart: " + aId);
                sna.stopAdmin(false /* force */, "Repair admin parameters");
                sna.startAdmin();
                final StorageNode sn = regUtils.getTopology().get(snId);
                ServiceUtils.waitForAdmin(sn.getHostname(),
                                          sn.getRegistryPort(),
                                          regUtils.getLoginManager(),
                                          90, /* timeoutSec */
                                          ServiceStatus.RUNNING,
                                          logger);
                return false;
            }

            /*
             * Stop the current admin, and return true, to request that the
             * caller retry the repair after the admin restarts
             */
            logger.info("Repair admin params: restart current: " + aId);
            stopAdminService(true /* force */, "Repair parameters");
            return true;
        } catch (Exception e) {
            throw new IllegalStateException(
                "Problem attempting to update the admin quorum for admin: " +
                aId + ", SN ID: " + snId + ", host and port: " +
                adminParams.getNodeHostPort() + ": " + e.getMessage(),
                e);
        }
    }

   /* ---- DDL statement execution support ------ */

    /**
     * Asynchronously execute the specified DDL statement and return
     * information about the resulting plan.
     * @param namespace optional namespace to use for tables
     * @param serialVersion serialization version
     * @throws IllegalCommandException if the statement is invalid.
     */
    public ExecutionInfo executeStatement(String ddlStatement,
            String namespace,
            boolean validateNamespace,
            TableLimits limits,
            LogContext lc,
        short serialVersion) {
        /*
         * Parse the statement and fire off a plan to execute it. There are
         * three possible outcomes where the handler will not have issued a
         * plan:
         *
         * 1 - the statement is a describe statement, with a string return. In
         * that case, the handler will return success & a result string
         *
         * 2 - the statement had an error, and the handler will return an error
         * string. ResultString and planId will be null
         *
         * 3 - the handler will have issued a plan, and planId will be non-null.
         * ErrorString and ResultString will be null.
         */
        assert TestHookExecute.doHookIfSet(EXECUTE_HOOK, this);

        /*
         * The access checker instance in AdminService will be passed to
         * DdlHandler so that privilege check can be done for ddl operations.
         */
        final AccessChecker accessChecker =
                owner == null ?
                null :
                owner.getAdminSecurity().getAccessChecker();
        if (isLoggableWithCtx(logger, Level.FINE, lc)) {
            fineWithCtx(logger, "Issuing DDL statement: " + ddlStatement, lc);
        }

        DdlHandler handler =
           new DdlHandler(ddlStatement, this, namespace, validateNamespace,
               limits, accessChecker);

        int planId = handler.getPlanId();

        /*
         * If the handler isn't successful, it meant that the statement
         * didn't parse correctly, or was semantically incorrect.
         */
        if (!handler.getSuccess()) {
            /*
             * There should have been no plan executed, and there should be
             * an error string.
             */
            assert (planId == 0);
            assert (handler.getErrorMessage() != null);

            if (handler.canRetry()) {
                /* This is a transient problem, retries may succeed */
                throw new OperationFaultException
                    ("Error from " + ddlStatement + ": "
                     + handler.getErrorMessage());
            }

            /* This is a permanent problem, retries will not help */

            if (handler.getException() instanceof QueryException) {
                String msg = "User error in query: " +
                    handler.getException().toString();

                fineWithCtx(logger, msg, lc);
                throw ((QueryException)handler.getException()).
                    getWrappedIllegalArgument();
            }

            if (handler.getException() instanceof QueryStateException) {
                warningWithCtx(logger, handler.getException().toString(), lc);
                ((QueryStateException)handler.getException()).
                    throwClientException();
            }

            /*
             * Not all errors throw exceptions. Wrap these in a generic
             * QueryException. Location information is not available.
             */
            if (handler.getException() == null) {
                String msg = "User error in query: " +
                    handler.getErrorMessage();
                fineWithCtx(logger, msg, lc);
                throw new QueryException(msg).getWrappedIllegalArgument();
            }

            /* Unknown exception from the query */
            throw new IllegalCommandException
                ("Error from " + ddlStatement + ": " +
                 handler.getErrorMessage());
        }

        /* A successful outcome */
        if (handler.hasPlan()) {
            /* a plan was issued */
            return getExecutionStatus(planId, serialVersion);
        }

        /*
         * A statement that didn't require an administrative plan was issued,
         * i.e. an "if exists" or "if not exists", or a show or describe
         * statement.
         */
        DdlResultsReport report = new DdlResultsReport(handler, serialVersion);
        return new ExecutionInfoImpl
                (0,      // planId,
                 true,   // isTerminated
                 report.getStatus(),
                 report.getStatusAsJson(),
                 true,   // isSuccess
                 false,  // isCancelled
                 null,   // errorMessage
                 false,  // needsCancel
                 report.getResult());  // result
    }

    /**
     * This is not a DDL statement but is a DDL-like plan to change table
     * limits. If anything fails, cancel the plan.
     */
    ExecutionInfo setTableLimits(final String namespace,
                                 final String tableName,
                                 final TableLimits limits,
                                 short serialVersion) {
        int planId = getPlanner().createTableLimitPlan("SetTableLimits",
                                                       namespace,
                                                       tableName,
                                                       limits);
        try {
            approvePlan(planId);
            executePlanOrFindMatch(planId);
        } catch (Exception e) {
            cancelPlan(planId);
            throw new OperationFaultException
                ("Error trying to set table limits on table " + tableName);
        }
        return getExecutionStatus(planId, serialVersion);
    }

    /**
     * Check on the specified plan and return status. Differs from
     * {@link #getPlanStatus} in that getPlanStatus returns
     * information as a string, for display.
     *
     * TODO: perhaps deprecate getPlanStatus, which is used by the Admin CLI,
     * in the future, in favor of using getExecutionStatus. We could then have
     * the CLI and the DDL API share a common way of displaying plan status.
     *
     * @throws IllegalArgumentException if the plan Id is null. Otherwise, any
     * errors are handled and expressed by the ExecutionInfo, which will
     * have isSuccess == false.
     */
    public ExecutionInfo getExecutionStatus(int planId, short serialVersion) {
        assert TestHookExecute.doHookIfSet(EXECUTE_HOOK, this);
        /* Find the plan instance. */
        final Plan p = getPlanById(planId);
        if (p == null) {
            throw new IllegalCommandException
                ("Attempt to get status for plan " + planId +
                 " but it doesn't exist");
        }

        /*
         * The planRun has information about the last execution of this plan.
         * Hang onto this planRun in case another run starts.  This value may
         * be null if the plan is just getting started. [#27012]
         */
        PlanRun planRun = p.getExecutionState().getLatestPlanRun();
        DdlResultsReport ddlReport = new DdlResultsReport(p, serialVersion);

        /*
         * TODO: the planRun's failure description may need to be translated
         * into something more user friendly.
         */
        return new ExecutionInfoImpl(
            planId,
            (planRun != null) && planRun.isTerminated(),
            ddlReport.getStatus(),
            ddlReport.getStatusAsJson(),
            (planRun != null) && planRun.isSuccess(),
            (planRun != null) && planRun.isCancelled(),
            (planRun != null) ? planRun.getFailureDescription(false) : null,
            (planRun != null) && planRun.getState().equals(Plan.State.ERROR),
            ddlReport.getResult()); // result
    }

    /**
     * Return the state of the latest plan run.
     */
    public Plan.State getCurrentPlanState(int planId) {
        final Plan p = getPlanById(planId);
        if (p == null) {
            throw new IllegalCommandException
                ("Attempt to get status for plan " + planId +
                 " but it doesn't exist");
        }

        /*
         * The planRun has information about the last execution of this plan.
         * Hang onto this planRun in case another run starts.
         */
        return p.getExecutionState().getLatestPlanRun().getState();
    }

    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        assert !Thread.holdsLock(this);
        synchronized (threadLock) {
            final AdminParams oldParams = new AdminParams(oldMap);
            final AdminParams newParams = new AdminParams(newMap);

            /* Update electable group size override */
            final int newElectableGroupSizeOverride =
                newParams.getElectableGroupSizeOverride();
            if (oldParams.getElectableGroupSizeOverride() !=
                newElectableGroupSizeOverride) {
                repConfig.setElectableGroupSizeOverride(
                    newElectableGroupSizeOverride);
                environment.setRepMutableConfig(repConfig);
            }
            long newPollTime =
                newParams.getKVMetadataAdminCheckInterval().toMillis();
            if (oldParams.isMetadataAdminThreadEnabled() !=
                newParams.isMetadataAdminThreadEnabled()) {
                if (!newParams.isMetadataAdminThreadEnabled()) {
                    shutdownKVMetadataAdminThread();
                } else {
                    shutdownKVMetadataAdminThread();
                    startMetadataAdminThread(
                        newPollTime,
                        newParams.getKVMetadataAdminMaxPlanWait().toMillis());
                }
            } else if (newParams.isMetadataAdminThreadEnabled()) {
                /* Thread was and still is enabled check */
                long oldPollTime =
                    newParams.getKVMetadataAdminCheckInterval().toMillis();
                if (oldPollTime != newPollTime) {
                    shutdownKVMetadataAdminThread();
                    startMetadataAdminThread(
                        newPollTime,
                        newParams.getKVMetadataAdminMaxPlanWait().toMillis());
                }
            } else if (!newParams.getKVMetadataAdminMaxPlanWait().equals(
                       oldParams.getKVMetadataAdminMaxPlanWait()) &&
                       metadataAdminChecker != null) {
                metadataAdminChecker.setMaxPlanWait(
                    (int) newParams.getKVMetadataAdminMaxPlanWait().toMillis());
            }

            newPollTime =  newParams.getVersionCheckInterval().toMillis();
            if (oldParams.isVersionThreadEnabled() !=
                newParams.isVersionThreadEnabled()) {
                if (!newParams.isVersionThreadEnabled()) {
                    shutdownVersionUpdaterThread(false);
                } else {
                    shutdownVersionUpdaterThread(false);
                    startVersionUpdaterThread(newPollTime);
                }
            } else if (newParams.isVersionThreadEnabled()) {
                /* Thread was and still is enabled check */
                long oldPollTime = newParams.getVersionCheckInterval().toMillis();
                if (oldPollTime != newPollTime) {
                    shutdownVersionUpdaterThread(false);
                    startVersionUpdaterThread(newPollTime);
                }
            }

            if (oldParams.getMaxEvents() != newParams.getMaxEvents()) {
               stores.getEventStore().setMaxEvents(newParams.getMaxEvents());
            }

            /*
             * Check whether the new parameters specify a new node type that is not
             * reflected by the environment.  This could happen if, because of a
             * bug, we failed to restart the environment after updating the
             * parameter.
             */
            final NodeType newNodeType = getNodeType(newParams.getType());
            final NodeType envNodeType = environment.getRepConfig().getNodeType();
            if (!envNodeType.equals(newNodeType)) {
                throw new IllegalStateException(
                    "Environment for " + adminId + " has wrong node type:" +
                    " expected " + newNodeType +
                    ", found " + envNodeType);
            }
        }
    }

    /**
     * Starts a thread which monitors Admin upgrade. This should be called if
     * the Admin is being upgraded and the master must wait until all Admins
     * have been upgraded before the store schema can be updated.
     */
    void monitorUpgrade() {
        /**
         * If the threadLock is not held, then we are not managing the shutting
         * down of the admin and thus there is already another thread starting
         * the monitoring thread which calls us. No need to start another
         * thread simply return here. This could happen during a recursive call
         * that the upgradeMonitor calls initStore() which calls this method
         * again.
         */
        if (!Thread.holdsLock(threadLock)) {
            return;
        }

        if ((upgradeMonitor != null) && upgradeMonitor.isAlive()) {
            return;
        }

        /*
         * The monitor thread will check every 10 seconds to see if the stores
         * can be upgraded.
         */
        upgradeMonitor = new StoppableThread("MonitorUpgradeThread") {
            @Override
            public void run() {
                logger.log(Level.FINE, "{0} started", this);
                try {
                    Thread.sleep(10000);
                    while (true) {
                        /*
                         * Quit the thread if it is shutdown, the admin is
                         * closing or no longer the master, or the stores are
                         * being updated.
                         */
                        if (isShutdown() ||
                            isClosing() ||
                            !environment.getState().isMaster() ||
                            !stores.isReadOnly()) {
                            logger.log(Level.FINE, "{0} exiting", this);
                            return;
                        }
                        logger.log(Level.FINE,
                                  "{0} checking for upgrade",this);
                        initStores(true /* isMaster */);
                        Thread.sleep(10000);
                    }
                } catch (InterruptedException ex) {
                    logger.log(Level.WARNING,
                               "{0} interrupted, exited", this);
                }
            }
            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
        upgradeMonitor.start();
    }

    /*
     * For testing, wait for create/upgrade system tables completed.
     */
    public void joinSysTableMonitorThread() throws InterruptedException {

        /* Set to local t variable, to avoid sysTableMonitor is set to null
         * in another thread after checking it is not null.
         */
        Thread t = sysTableMonitor;
        if (t != null) {
            t.join();
        }
    }

    /**
     * Returns true if writes to system tables are permitted. If the call is
     * being made on behalf of a user table operation (userTable is true)
     * writes will be enabled if possible.
     *
     * @return true if writes to system tables are permitted
     */
    public boolean systemTablesEnabled(boolean userTable) {
        if (systemTablesEnabled) {
            return true;
        }
        if (!userTable) {
            return systemTablesEnabled;
        }

        /* A user table is present. Make sure the store is ready. */
        final Topology topo = getCurrentTopology();
        if ((topo == null) || !isStoreDeployed()) {
            return systemTablesEnabled;
        }

        /* Store is ready, enable writes.*/
        synchronized (this) {
            if (!systemTablesEnabled) {
                logger.info("Enabling system table writes, topo " +
                            topo.getSequenceNumber());
                /*
                 * If a store handle was created earlier it may not have the
                 * latest topology. Clearing it will force an update on the
                 * next access.
                 */
                resetInternalKVStore();

                /*
                 * Set the flag after resetting the store handle so that
                 * someone doesn't use the old handle for sys table ops.
                 */
                systemTablesEnabled = true;

                /*
                 * Start the system table monitor thread to update any
                 * system tables that need it.
                 */
                startSysTableMonitor();
            }
        }
        return systemTablesEnabled;
    }

    /*
     * Starts a system table monitor thread if one is not already running. If
     * one is running, it is notified in case it was in a wait.
     */
    void startSysTableMonitor() {
        assert Thread.holdsLock(this);
        if ((sysTableMonitor != null) && sysTableMonitor.isAlive()) {
            /* Thread may be sleeping */
            sysTableMonitor.notifyWaiters();
            return;
        }
        sysTableMonitor = new SysTableMonitor(this);
        sysTableMonitor.start();
    }

    /**
     * Shuts down the system table monitor thread.
     */
    private synchronized  void shutdownSysTableMonitor() {
        if (sysTableMonitor != null) {
            sysTableMonitor.shutdown();
            sysTableMonitor = null;
        }
    }

    /**
     * Persists the fact that a topology deployment has been successful.
     */
    public void storeDeploymentSucceeded() {
        if (memo.deploymentSucceeded()) {
            new AdminTransaction<Void>(RunTransaction.sync) {
                @Override
                protected Void doTransaction(Transaction txn) {
                    stores.putMemo(txn, memo);
                    return null;
                }
            }.run();
        }
    }

    /**
     * Returns true if a store has been deployed.
     */
    boolean isStoreDeployed() {
        return (memo == null) ? false : !memo.needsDeployment();
    }

    /**
     * Checks if the store is ready.
     *
     * If forTables is false returns true if the store is deployed and ready
     * for Key/Value operations. If forTables is true, returns true if the
     * store is ready and all system tables have been created and initialized.
     *
     * If forTables is true this method may return false during an upgrade if
     * there are new system tables and need to be created.
     *
     * If forTables is true writes to system tables will be enabled. Therefore,
     * it should not be set unless the caller is certain that the store will
     * only be used for table operations.
     */
    boolean isStoreReady(boolean forTables) {
        if (!forTables) {
            return isStoreDeployed();
        }
        if (!systemTablesReady) {
            systemTablesEnabled(true);
        }
        return systemTablesReady;
    }

    /*
     * Sets system tables ready flag.
     */
    void systemTablesReady() {
        systemTablesReady = true;
    }

    private String
        doFailoverProcessing(String candidateName,
                             StorageNodePool snPool,
                             Set<DatacenterId> newPrimaryZones,
                             Set<DatacenterId> offlineZones,
                             Transaction txn)  {
        Topology current = getCurrentTopology();
        TopologyCandidate candidate =
            new TopologyCandidate(candidateName, current);
        final Topology topo = candidate.getTopology();
        for (final DatacenterId dcId : topo.getDatacenterMap().getAllIds()) {
            Datacenter dc = topo.get(dcId);

            /* Get zone type for new primary zones and offline zones */
            DatacenterType newType;
            if (newPrimaryZones.contains(dcId)) {
                if (dc.getDatacenterType().equals(DatacenterType.PRIMARY)) {
                    continue;
                }
                newType = DatacenterType.PRIMARY;
            } else if (offlineZones.contains(dcId)) {
                if (dc.getDatacenterType().equals(DatacenterType.SECONDARY)) {
                    continue;
                }
                newType = DatacenterType.SECONDARY;
            } else {
                continue;
            }
            final boolean allowArbiters = Datacenter.computeAllowArbiters(
                dc.getAllowArbiters(), newType, dc.getRepFactor());
            final boolean masterAffinity = Datacenter.computeMasterAffinity(
                dc.getMasterAffinity(), newType, dc.getRepFactor());
            topo.update(dc.getResourceId(),
                        Datacenter.newInstance(dc.getName(),
                                               dc.getRepFactor(),
                                               newType,
                                               allowArbiters,
                                               masterAffinity));
        }

        TopologyBuilder tb =
            new TopologyBuilder(candidate,
                                snPool,
                                getCurrentParameters(),
                                getParams());
        candidate = tb.fixANProblems(candidate, offlineZones);

        final TopologyStore topoStore = stores.getTopologyStore();
        topoStore.putCandidate(txn,
                               new TopologyCandidate(candidateName,
                                                     candidate.getTopology()));

        return candidateName;
    }

    /**
     * Verify data for this admin.
     *
     * @param options properties for the verification
     *
     */
    public VerificationInfo startAndPollVerify(VerificationOptions options,
                                               int planId) {

        return verificationManager.startAndPollVerification(options,
                                                            planId);
    }

    public boolean interruptVerify() {
        return verificationManager.interruptVerification();
    }

    public MRTManager getMRTServiceManager() {
        return mrtServiceManager;
    }

    private void updateAndNotify(RepNodeParams rnp) {
        parameters.update(rnp);
        parameterChangeTracker.notifyListeners(rnp.getStorageNodeId());
    }

    private void updateAndNotify(StorageNodeParams snp) {
        parameters.update(snp);
        parameterChangeTracker.notifyListeners(snp.getStorageNodeId());
    }

    private void updateAndNotify(GlobalParams gp) {
        parameters.update(gp);
        parameterChangeTracker.notifyListeners(null);
    }

    private void updateAndNotify(ArbNodeParams anp) {
        parameters.update(anp);
        parameterChangeTracker.notifyListeners(anp.getStorageNodeId());
    }

    private void startMetadataAdminThread(long pollTimeMS, long maxPlanWait) {
        assert Thread.holdsLock(threadLock);
        assert metadataAdminChecker == null;
        metadataAdminChecker =
                new ParamConsistencyChecker(this,
                                            pollTimeMS,
                                            (int) maxPlanWait,
                                            logger);
    }

    private void shutdownKVMetadataAdminThread() {
        assert !Thread.holdsLock(this);
        assert Thread.holdsLock(threadLock);
        if (metadataAdminChecker != null) {
            metadataAdminChecker.shutdown();
            metadataAdminChecker = null;
        }
    }

    private void startVersionUpdaterThread(long pollTimeMS) {
        assert Thread.holdsLock(threadLock);
        assert versionUpdater == null;
        versionUpdater = new SoftwareVersionUpdater(this, pollTimeMS, logger);
    }

    private void shutdownVersionUpdaterThread(boolean force) {
        assert !Thread.holdsLock(this);
        assert Thread.holdsLock(threadLock);
        if (versionUpdater != null) {
            versionUpdater.shutdown(force);
            versionUpdater = null;
        }
    }

    class AdminStatsListener
        implements AdminStatsTracker.Listener {
        @Override
        public void receiveStats(StatsPacket packet) {
            if (owner == null) {
                return;
            }
            owner.receiveStats(packet);
        }
    }

    class KVMetadataAdminThreadParameterListenter
        implements ParameterListener {
        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            if (metadataAdminChecker != null) {
                metadataAdminChecker.newParameters(oldMap, newMap);
            }
            if (versionUpdater != null) {
                versionUpdater.newParameters(oldMap, newMap);
            }
        }
    }

    /**
     * Simple class used to register listeners for parameter changes
     * that effect an SN's configuration.
     */
    class SNParameterChangeTracker {

        private final Set<SNParameterChangeListener> listeners;

        public SNParameterChangeTracker() {
            listeners =
                Collections.synchronizedSet(
                    new HashSet<SNParameterChangeListener>());
        }

        public void addListener(SNParameterChangeListener listener) {
            listeners.add(listener);
        }

        public void removeListener(SNParameterChangeListener listener) {
            listeners.remove(listener);
        }

        public void notifyListeners(StorageNodeId snId) {
            for (SNParameterChangeListener listener : listeners) {
                listener.changeParameters(snId);
            }
        }
    }

    private interface SNParameterChangeListener {

        /**
         * Notify listener of new parameters.
         * @param snId Storage node of resource
         */
        public void changeParameters(StorageNodeId snId);
    }

    class KVAdminParameterListener implements SNParameterChangeListener {
        @Override
        public void changeParameters(StorageNodeId snId) {
            if (metadataAdminChecker != null) {
                metadataAdminChecker.changeParameters(snId);
            }
        }

    }

    /*
     * Convenience method that encapsulates the creation of a new RegistryUtils
     * instance that is performed throughout the Admin class. This method can
     * be overridden by subclasses of Admin so the Admin coverage tests that
     * need to employ a mocked or simulated instance of RegistryUtils can be
     * returned to the Admin methods that invoke this method.
     */
    RegistryUtils createRegistryUtils() {
        return new RegistryUtils(
            getCurrentTopology(), getLoginManager(), logger);
    }

    /* Convenience methods and classes used only by the Admin tests. */
    AdminId getAdminId() {
        return adminId;
    }

    AdminId getMasterId() {
        return masterId;
    }

    void setMasterId(final AdminId newAdminId) {
        masterId = newAdminId;

    }

    void setParameters(final Parameters newParameters) {
        parameters = newParameters;
    }

    /**
     * Saves the specified topology to the topology history system table.
     */
    public void writeTopologyToHistory(Topology topo) {
        if (systemTablesEnabled(false)) {
            if (topo == null) {
                return;
            }
            (new TopologyHistoryWriteSysTableUtil(this, logger))
                .writeWithRetry(topo, MAX_STORE_WRITE_RETRIES);
        } else {
            logger.log(
                Level.FINEST,
                "skip writing topology history "
                + "since system table not enabled");
        }
    }

    /**
     * Waits until either the specified timeout has passed or the admin is
     * shutting down. Returns {@code true} if the admin is shutting down;
     * otherwise timed out.
     */
    public boolean awaitUntilClosing(long timeoutMillis)
        throws InterruptedException
    {
        final long ts = System.currentTimeMillis();
        final long te = ts + timeoutMillis;
        final long deadline = te < 0 ? Long.MAX_VALUE : te;
        while (true) {
            final long t = System.currentTimeMillis();
            if (t >= deadline) {
                return false;
            }
            final long delay = deadline - t;
            synchronized(closingLock) {
                if (closing) {
                    return true;
                }
                closingLock.wait(delay);
            }
        }
    }

    /**
     * Gathers information about TLS credentials information on all SNAs in the
     * store and returns it as a string in JSON format.
     *
     * @see CommandServiceAPI#getTlsCredentialsInfo
     */
    public String getTlsCredentialsInfo() {
        if ((myParams == null) ||
            (myParams.getSecurityParams() == null) ||
            !myParams.getSecurityParams().isSecure()) {
            throw new IllegalCommandException(
                "Cannot get TLS credentials for a non-secure store");
        }

        /* Collect information from SNs */
        final RegistryUtils registryUtils = createRegistryUtils();
        final Topology currentTopology = getCurrentTopology();
        final ObjectNode snsMap = JsonUtils.createObjectNode();
        for (final StorageNodeId snid : currentTopology.getStorageNodeIds()) {
            ObjectNode sn;
            try {
                final StorageNodeAgentAPI snapi =
                    registryUtils.getStorageNodeAgent(snid);
                sn = JsonUtils.parseJsonObject(
                    snapi.getTlsCredentialsInfo());
            } catch (RemoteException|NotBoundException e) {
                sn = JsonUtils.createObjectNode();
                sn.put("exception", e.toString());
            }
            snsMap.put(snid.getFullName(), sn);
        }

        return computeTlsCredentialsSummary(snsMap).toString();
    }

    /**
     * Computes summary information about TLS credentials given the specified
     * SN information and returns a JSON node including the SNs and the
     * summary info.
     */
    static ObjectNode computeTlsCredentialsSummary(ObjectNode snsMap) {
        final Map<String, String> keystoreHashes = new HashMap<>();
        final Map<String, String> truststoreHashes = new HashMap<>();
        boolean exceptions = false;
        for (final Entry<String, JsonNode> entry : snsMap.entrySet()) {
            final String sn = entry.getKey();
            final ObjectNode snInfo = (ObjectNode) entry.getValue();
            final JsonNode exception = snInfo.get("exception");
            if (exception != null) {
                exceptions = true;
                continue;
            }
            final ObjectNode installed =
                snInfo.getObject("installedCredentials");
            final String keystoreHash =
                getAsText(installed.getObject("keystore"), "hash");
            keystoreHashes.put(sn, keystoreHash);
            final String truststoreHash =
                getAsText(installed.getObject("truststore"), "hash");
            truststoreHashes.put(sn, truststoreHash);
        }
        String installedCredentialsStatus =
            !exceptions ? "consistent" : "maybe-consistent";
        if (!sameValue(keystoreHashes) || !sameValue(truststoreHashes)) {
            installedCredentialsStatus = "mixed";
        }

        boolean pendingUpdates = false;
        for (final Entry<String, JsonNode> entry : snsMap.entrySet()) {
            final String sn = entry.getKey();
            final ObjectNode snInfo = (ObjectNode) entry.getValue();
            final JsonNode exception = snInfo.get("exception");
            if (exception != null) {
                continue;
            }
            final ObjectNode updates = snInfo.getObject("pendingUpdates");
            final String keystoreHash = keystoreHashes.get(sn);
            final ObjectNode keystoreUpdate = updates.getObject("keystore");
            if (keystoreUpdate != null) {
                final String updateHash = getAsText(keystoreUpdate, "hash");
                if (!keystoreHash.equals(updateHash)) {
                    keystoreHashes.put(sn, updateHash);
                    pendingUpdates = true;
                }
            }
            final String truststoreHash = truststoreHashes.get(sn);
            final ObjectNode truststoreUpdate =
                updates.getObject("truststore");
            if (truststoreUpdate != null) {
                final String updateHash = getAsText(truststoreUpdate, "hash");
                if (!truststoreHash.equals(updateHash)) {
                    truststoreHashes.put(sn, updateHash);
                }
                pendingUpdates = true;
            }
        }

        String pendingUpdatesStatus;
        if (!pendingUpdates) {
            pendingUpdatesStatus = "none";
        } else {
            pendingUpdatesStatus =
                !exceptions ? "consistent" : "maybe-consistent";
            if (!sameValue(keystoreHashes) || !sameValue(truststoreHashes)) {
                pendingUpdatesStatus = "mixed";
            }
        }

        final ObjectNode result = JsonUtils.createObjectNode();
        result.put("installedCredentialsStatus", installedCredentialsStatus);
        result.put("pendingUpdatesStatus", pendingUpdatesStatus);
        result.put("sns", snsMap);
        return result;
    }

    /** Check if all map values are the same */
    private static <V> boolean sameValue(Map<?, V> map) {
        boolean foundValue = false;
        V firstValue = null;
        for (final V value : map.values()) {
            if (!foundValue) {
                firstValue = value;
                foundValue = true;
            } else if (!Objects.equals(firstValue, value)) {
                return false;
            }
        }
        return true;
    }
}
