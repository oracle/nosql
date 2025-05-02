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

import static com.sleepycat.je.rep.NoConsistencyRequiredPolicy.NO_CONSISTENCY;
import static com.sleepycat.je.rep.QuorumPolicy.SIMPLE_MAJORITY;
import static com.sleepycat.je.rep.impl.RepParams.REPLAY_MAX_OPEN_DB_HANDLES;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.NoSuchFileException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.InternalFaultException;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.DatabaseUtils;
import oracle.kv.impl.util.DatabaseUtils.VerificationInfo;
import oracle.kv.impl.util.DatabaseUtils.VerificationManager;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.KVThreadFactory;
import oracle.kv.impl.util.StorageTypeDetector;
import oracle.kv.impl.util.StorageTypeDetector.StorageType;
import oracle.kv.impl.util.server.JENotifyHooks.LogRewriteListener;
import oracle.kv.impl.util.server.JENotifyHooks.RecoveryListener;
import oracle.kv.impl.util.server.JENotifyHooks.RedirectHandler;
import oracle.kv.impl.util.server.JENotifyHooks.SyncupListener;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.PingCollector;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.rep.InsufficientLogException;
import com.sleepycat.je.rep.NetworkRestore;
import com.sleepycat.je.rep.NetworkRestoreConfig;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.RepInternal;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.RestartRequiredException;
import com.sleepycat.je.rep.RollbackException;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackup;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackup.InsufficientVLSNRangeException;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackup.LoadThresholdExceededException;
import com.sleepycat.je.rep.impl.networkRestore.NetworkBackupStats;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.utilint.JVMSystemUtils;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * RepEnvManager is responsible for managing the handle to the replicated
 * environment and the database handles associated with the environment.
 * <p>
 * This class is effectively a component of RepNode and is used exclusively
 * by it.
 */
public class RepEnvHandleManager implements ParameterListener {

    /**
     * A test hook for calls to the recovery listener's progress method,
     * passing the RecoveryProgress phase.
     */
    public static volatile TestHook<RecoveryProgress> recoveryProgressTestHook;

    /**
     * A test hook called while holding the semaphore used to control opening
     * the JE HA environment, passing the service parameters.
     */
    public static volatile TestHook<RepNodeService.Params> openEnvTestHook;

    /* Hook to inject failures during network restore task */
    public static volatile TestHook<Integer> FAULT_HOOK;

    /* JE network backup test hook */
    public static volatile
        com.sleepycat.je.utilint.TestHook<File> NETWORK_BACKUP_HOOK;

    private final RepNode repNode;
    private final RepNodeService.Params repServiceParams;
    private final RepNodeService repNodeService;

    /* The parameters needed to configure the replicated environment. */
    private final File envDir;
    /* The type of storage used by the environment files. */
    private final StorageType envStorageType;
    private final File snapshotDir;
    private final EnvironmentConfig envConfig;
    private final ReplicationConfig renvConfig;
    private final VersionManager versionManager;

    /**
     * Listener factory used to create new listener instances each time a
     * replicated environment is reopened.
     */
    private final StateChangeListenerFactory listenerFactory;

    /*
     * Semaphore to ensure that only one thread updates the environment handle.
     */
    private final Semaphore renewRepEnvSemaphore = new Semaphore(1);
    /* Lock to synchronize access to the environment handle. */
    private final ReentrantReadWriteLock envLock =
        new ReentrantReadWriteLock();

    /* The handle being managed. */
    private ReplicatedEnvironment repEnv;

    private final Logger logger;

    /**
     * The object currently performing a network restore, or null if no network
     * restore is in progress.  Used to get network restore statistics.
     */
    private volatile NetworkRestore networkRestore = null;

    /**
     * Initiated from RepNodeAdminAPI to force this node network restore,
     * which is run asynchronously. Keep this in order to let caller pull
     * backup statistics from a running network restore process. Synchronize
     * when setting this field.
     */
    private volatile AsyncNetworkRestore asyncNetworkRestore = null;

    private final VerificationManager verificationManager;

    public RepEnvHandleManager(RepNode repNode,
                               StateChangeListenerFactory listenerFactory,
                               RepNodeService.Params params,
                               RepNodeService repNodeService) {

        assert listenerFactory != null;

        this.repNode = repNode;
        this.repNodeService = repNodeService;
        this.repServiceParams = params;
        this.logger = LoggerUtils.getLogger(this.getClass(), params);

        LoggerUtils.logSoftwareVersions(logger);

        final File storageDir = getStorageDir();
        final StorageNodeParams snParams = params.getStorageNodeParams();
        final String kvStoreName = params.getGlobalParams().getKVStoreName();
        envDir = FileNames.getEnvDir(snParams.getRootDirPath(),
                                     kvStoreName,
                                     storageDir,
                                     snParams.getStorageNodeId(),
                                     repNode.getRepNodeId());
        if (FileNames.makeDir(envDir)) {
            logger.info("Created new environment dir: " + envDir);
        }
        envStorageType = getStorageType(snParams, envDir);
        logger.log(Level.INFO, "Storage type of: {0} is {1}",
                   new Object[] { envDir, envStorageType.name() });

        snapshotDir = FileNames.getSnapshotDir(snParams.getRootDirPath(),
                                               kvStoreName,
                                               storageDir,
                                               snParams.getStorageNodeId(),
                                               repNode.getRepNodeId());
        final ParameterUtils pu =
            new ParameterUtils(repNode.getRepNodeParams().getMap());
        envConfig = createEnvConfig(pu);
        renvConfig = createRepEnvConfig(pu);
        this.listenerFactory = listenerFactory;
        versionManager = new VersionManager(logger, repNode);
        verificationManager =
            new VerificationManager(repNode.getRepNodeId(), logger);
    }

    /**
     * Create and initialize the replicated environment config used to
     * initialize the JE handle.
     */
    private ReplicationConfig createRepEnvConfig(ParameterUtils pu) {
        final ReplicationConfig config = pu.getRNRepEnvConfig();
        /* in some unit tests the rn service may not be available */
        if (repNodeService != null) {
            /* set stream authenticator generator */
            config.setAuthenticator(
                repNodeService.getRepNodeSecurity().getStreamAuthHandler());
        }

        logger.info("Non-default JE properties for environment: " +
                    pu.createProperties(false, false, 0L));

        config.setGroupName(repNode.getRepNodeId().getGroupName());
        config.setNodeName(repNode.getRepNodeId().getFullName());

        final RepNodeParams rnParams = repNode.getRepNodeParams();
        config.setNodeType(rnParams.getNodeType());
        config.setElectableGroupSizeOverride(
            rnParams.getElectableGroupSizeOverride());

        if (TestStatus.isActive()) {
            config.setConfigParam(RepParams.SO_REUSEADDR.getName(),
                                      "true");
            config.setConfigParam(RepParams.SO_BIND_WAIT_MS.getName(),
                                      "120000");
        }

        /* Configure the JE HA communication mechanism */
        if (repServiceParams.getSecurityParams() != null) {
            final Properties haProps =
                repServiceParams.getSecurityParams().getJEHAProperties();
            logger.info("DataChannelFactory: " +
                        haProps.getProperty(
                            ReplicationNetworkConfig.CHANNEL_TYPE));
            config.setRepNetConfig(
                ReplicationNetworkConfig.create(haProps));
        }

        config.setDisableAuthoritativeMaster(rnParams.getDisableAuthMaster());
        return config;
    }

    /**
     * Create and initialize the environment config used to create the JE
     * environment handle.
     */
    private EnvironmentConfig createEnvConfig(final ParameterUtils pu) {
        final EnvironmentConfig config = pu.getEnvConfig();
        final RepNodeParams rnParams = repServiceParams.getRepNodeParams();
        final StorageNodeParams snParams =
            repServiceParams.getStorageNodeParams();
        /*
         * Use EVICT_LN as the default cache mode to ensure that LNs in
         * partition dbs are explicitly evicted during any syncup operations
         * which replay these LNs.
         */
        config.setCacheMode(CacheMode.EVICT_LN);

        checkMemoryAllocation(repServiceParams);

        /*
         * Use the service name as the JE log file prefix so that multiple
         * services can store their log files in the same directory
         */
        config.setConfigParam(EnvironmentConfig.FILE_LOGGING_PREFIX,
                              repNode.getRepNodeId().getFullName());

        /*
         * Logging je.info, je.stat, je.config file in rnlogdir
         * if specified else in kvroot log directory.
         */
        String dirName = rnParams.getLogDirectoryPath();
        if (dirName == null) {
            dirName = snParams.getRootDirPath() + "/" +
                repServiceParams.getGlobalParams().getKVStoreName() + "/log";
        }
        config.setConfigParam(EnvironmentConfig.FILE_LOGGING_DIRECTORY,
                                 dirName);

        final String memFmt =
            "JVM Runtime maxMemory:%,d kB JE cache size:%,d kB";
        logger.info(String.format(memFmt,
                                  JVMSystemUtils.getRuntimeMaxMemory() / 1024,
                                  config.getCacheSize() / 1024));

        logger.info("Environment variable MALLOC_ARENA_MAX: " +
                    System.getenv("MALLOC_ARENA_MAX"));

        /* Modify the checkpoint interval based upon the storage directory. */
        return setEnvCheckpointInterval(config);
    }

    /**
     * Check whether the size of storage directory or root directory is
     * specified and log the message.
     *
     * @return the validated storage directory
     */
    private File getStorageDir() {
        final RepNodeParams rnParams = repServiceParams.getRepNodeParams();
        final StorageNodeParams snParams =
            repServiceParams.getStorageNodeParams();
        final File storageDir = rnParams.getStorageDirectoryFile();

        if (storageDir != null) {
            long storageDirSize = rnParams.getStorageDirectorySize();
            if (storageDirSize <= 0) {
                logger.warning("The storage directory " + storageDir +
                               " has no specified size");
            }
            try {
                long totalSpace = FileUtils.getDirectorySize(storageDir);
                if (storageDirSize > totalSpace) {
                    logger.warning(String.format(
                                "The storage directory %s is set to " +
                                "a size large than " +
                                "the total storage space (%,d)",
                                storageDir, totalSpace));
                }
            } catch (IllegalArgumentException e) {
                logger.log(Level.SEVERE,
                        "Exception verifying directory size for {0}: {1}",
                        new Object[] { storageDir, e });
            }
        } else {
            long rootDirDize = snParams.getRootDirSize();
            if (rootDirDize <= 0) {
                logger.warning("The root directory " +
                               snParams.getRootDirPath() +
                               " has no specified size");
            }
        }
        return storageDir;
    }

    /**
     * Check if the SN memory allocation is correct, or has a potential
     * problem, and make memory allocation logging that contains statistics.
     */
    private void checkMemoryAllocation(Params params) {

        final StorageNodeParams snp = params.getStorageNodeParams();
        final long snMemorySizeMB = snp.getMemoryMB();
        if (snMemorySizeMB == 0) {
            logger.info("Can't get SN memory size. No memory allocation log.");
            return;
        }

        final int rnHeapPercent = snp.getRNHeapPercent();
        final int jvmOverheadPercent = snp.getJVMOverheadPercent();
        final int snCapacity = snp.getCapacity();
        final RepNodeParams rnp = params.getRepNodeParams();
        final long rnMaxHeapMB = rnp.getMaxHeapBytes() / (1024 * 1024);

        checkMemoryAllocation(rnHeapPercent, jvmOverheadPercent,
                              snMemorySizeMB, snCapacity, rnMaxHeapMB,
                              logger);
    }

    /**
     * Checks parameters related to memory allocation. If the total memory
     * percentage for Java is no greater than 100%, logs memory parameters at
     * INFO and returns true. Otherwise, logs at WARNING and returns false.
     */
    public static boolean checkMemoryAllocation(int rnHeapPercent,
                                                int jvmOverheadPercent,
                                                long snMemorySizeMB,
                                                int snCapacity,
                                                long rnMaxHeapMB,
                                                Logger logger) {
        final int javaAllMemoryPercent = rnHeapPercent +
            ((rnHeapPercent * jvmOverheadPercent) / 100);
        final long allRNsMaxHeapMB =
            rnMaxHeapMB * ((snCapacity != 0) ? snCapacity : 1);

        final String memoryStats =
            String.format(" rnHeapPercent:%d" +
                          " jvmOverheadPercent:%d" +
                          " javaAllMemoryPercent:%d" +
                          " snMemorySizeMB:%d" +
                          " snCapacity:%d" +
                          " rnMaxHeapMB:%d" +
                          " allRNsMaxHeapMB:%d",
                          rnHeapPercent,
                          jvmOverheadPercent,
                          javaAllMemoryPercent,
                          snMemorySizeMB,
                          snCapacity,
                          rnMaxHeapMB,
                          allRNsMaxHeapMB);

        if (javaAllMemoryPercent <= 100) {
            logger.info("SN memory allocation parameters:" + memoryStats);
            return true;
        }
        logger.warning("Problem with SN memory allocation parameters: " +
                       "Java all memory percentage exceeds 100%:" +
                       memoryStats);
        return false;
    }

    /**
     * Sets the checkpoint interval, in the supplied config, based upon the
     * type of storage used by the env files. Tests show the following worst
     * case recovery times for the current checkpoint interval values:
     *
     *   Storage Type         Checkpoint Interval   Max Recovery Time
     *   ------------         -------------------   -----------------
     *   NVME                 8G                    5 min
     *   NVME                 4G                    2.5 min
     *   SSD                  2G                    2.5 min
     *   Default (hard disk)  500M                  15 min
     *
     * See: https://sleepycat-tools.us.oracle.com/trac/wiki/JEKV/RecoveryTimes
     *
     * @param config the config to be modified
     *
     * @return the modified config
     */
    private EnvironmentConfig
        setEnvCheckpointInterval(EnvironmentConfig config) {
        if (config.isConfigParamSet(
            EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL)) {
            /* Don't overwrite a param specified by the user. */
            return config;
        }
        final long newIntervalBytes;
        switch (envStorageType) {
            case NVME:
                newIntervalBytes = 8000000000L;
                break;
            case SSD:
                newIntervalBytes = 4000000000L;
                break;
            default:
                newIntervalBytes = 500000000L;
                break;
        }
        return config.setConfigParam(
            EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,
            Long.toString(newIntervalBytes));
    }

    /*
     * Returns the type of the storage directory. If the parameter
     * SN_STORAGE_TYPE is defined, its value is returned. Otherwise, an
     * attempt is made to detect the type of the environment's home
     * directory. If the detection fails, StorageType.UNKNOWN is returned.
     */
    private StorageType getStorageType(StorageNodeParams snParams,
                                       final File dir) {
        final String typeName = snParams.getStorageType();
        try {
            final StorageType type = StorageType.parseType(typeName);
            if (!type.equals(StorageTypeDetector.StorageType.UNKNOWN)) {
                return type;
            }
        } catch (IllegalArgumentException iae) {
            /* Either no parameter or unknown type */
            logger.log(Level.WARNING,
                       "Unsupported storage type: {0}, attempting auto detect",
                       typeName);
        }

        try {
            return StorageTypeDetector.detectType(dir);
        } catch (NoSuchFileException nsfe) {
            /* Should not happen */
            logger.log(Level.WARNING,
                       "Storage type detection failed: {0}", nsfe.getMessage());
            return StorageType.UNKNOWN;
        }
    }

    /**
     * Returns the storage type used to make various configuration decisions.
     */
    public StorageType envStorageType() {
        return envStorageType;
    }

    /**
     * Used to keep track of the number of partitions in this environment and
     * update the replica db handle cache size. The cache must be large enough
     * so that it can maintain one handle per partition. The cache exists to
     * avoid repeated closing and opening database handles, which are expensive
     * operations, during the replay of the replication stream.
     *
     * @param rnPartitions the number of partitions associated with this RN
     */
    public void updateRNPartitions(int rnPartitions) {
        final int configHandles = Integer.parseInt(renvConfig.
               getConfigParam(REPLAY_MAX_OPEN_DB_HANDLES.getName()));

        final int maxOpenHandles =
            Math.max(configHandles, (rnPartitions + 1 /* member db */));

        renvConfig.setConfigParam(REPLAY_MAX_OPEN_DB_HANDLES.getName(),
                                  Integer.toString(maxOpenHandles));

        /*
         * Check the cached setting of REPLAY_MAX_OPEN_DB_HANDLES in
         * renvConfig against the actual config used by the open environment
         * and correct it if necessary.
         */
        final ReplicatedEnvironment configEnv = getEnv(1);

        if ((configEnv == null) || !configEnv.isValid()) {
            /*
             * Environment is not available, renvConfig will take effect when
             * the environment is next opened.
             */
            return;
        }

        try {
            final int actualHandles =
                Integer.parseInt(configEnv.getMutableConfig().
                                 getConfigParam(REPLAY_MAX_OPEN_DB_HANDLES.
                                                getName()));
            if (actualHandles != maxOpenHandles) {
                /* It's different reset it. */
                configEnv.setRepMutableConfig(renvConfig);
                logger.info("Hosted partitions: " + rnPartitions +
                            ". Dynamically changed replay handles from: " +
                            actualHandles + " to: " + maxOpenHandles);
            }
        } catch (IllegalStateException e) {
            /* Ignore it, environment was subsequently closed. */
            return;
        } catch (EnvironmentFailureException ife) {
            /* Ignore it, environment was subsequently invalidated. */
            return;
        }
    }

    /**
     * Return the replicated environment handle waiting if necessary for
     * one to be established in the face of
     * {@link RestartRequiredException}s.
     *
     * @param timeoutMs the time to wait
     *
     * @return the replicated environment handle, or null if no handle was
     * established in the timeout window.
     */
     ReplicatedEnvironment getEnv(long timeoutMs) {
         if (timeoutMs == 0) {
             /* Avoids throw of interrupts in context that don't allow it. */
             return repEnv;
         }

        /* Wait for an environment to be established. */
        boolean lockAcquired;
        try {
            lockAcquired =
                envLock.readLock().tryLock(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unexpected interrupt", e);
        }

        if (!lockAcquired) {
            return null;
        }

        try {
            return repEnv;
        } finally {
            envLock.readLock().unlock();
        }
    }

    /**
     * Determines whether a state change event warrants env handle maint
     * actions.
     *
     * If the state has transitioned to DETACHED as a result of an exception in
     * a daemon thread, then the handle is re-established asynchronously.
     *
     * Note that the transition of a node to the DETACHED state due to some
     * EnvironmentFailureException could be detected concurrently in threads
     * processing requests as well. This mechanism serves as a backup when an
     * environment is quiescent from an application's viewpoint but some
     * administrative daemon (e.g. replica replay, cleaning, etc.) causes the
     * environment to be invalidated.
     *
     * @param env the non-null environment associated with the event
     *
     * @param stateChangeEvent the event
     */
    public void noteStateChange(ReplicatedEnvironment env,
                                StateChangeEvent stateChangeEvent) {
        if (!stateChangeEvent.getState().isDetached()) {
            return;
        }

        final EnvironmentFailureException efe = env.getInvalidatingException();

        if (efe == null) {
            logger.info(
                "Node in detached state. Handle is " +
                (env.isClosed() ? "closed." : "open."));
            return;
        }

        if (efe.isCorrupted()) {
            /*
             * Environment is corrupted, exit the process without restart.
             */
            logger.info("Exiting process. " +
                        " Node in detached state, environment corrupted." +
                        " Exception class: " + efe.getClass().getName() +
                        " Exception message: " + efe.getMessage());
            repNodeService.getFaultHandler().
                queueShutdown(efe, ProcessExitCode.getNoRestart(efe));
            return;
        }

        try {
            /* Provoke the exception that resulted in it becoming invalid. */
            throw efe;
        } catch (RollbackException rbe) {
            logger.info("Node in detached state. " +
                         "Handled being re-established.");
            /* asyncRenewRepEnv() will reopen the env. */
            asyncRenewRepEnv(env, rbe);
        } catch (InsufficientLogException ile) {
            asyncRenewRepEnv(env, ile);
        } catch (DatabaseException dbe) {
            /*
             * Something unanticipated with the environment, exit the process.
             * The SNA will restart this process.
             */
            logger.info("Exiting process. " +
                        " Node in detached state, environment invalid." +
                        " Exception class: " + dbe.getClass().getName() +
                        " Exception message: " + dbe.getMessage());
            repNodeService.getFaultHandler().
                 queueShutdown(dbe, ProcessExitCode.getRestart(dbe));
        }
    }

    /**
     * Recreates the replicated environment handle used to service all
     * requests directed at this node, performing the operation asynchronously
     * in a separate thread unless it is already underway.
     *
     * @param prevRepEnv the previous environment handle that is being
     * re-established. It's null if the handle is being created for the first
     * time; in this case the restartException argument must be null as well.
     *
     * @param restartException the exception that provoked the
     * re-establishment of the environment handle. It's null if prevRepEnv is
     * null. If non-null, it must be a "renewable" exception, that is one for
     * which isRenewable() returns true.
     */
    void asyncRenewRepEnv(ReplicatedEnvironment prevRepEnv,
                          DatabaseException restartException) {
        new AsyncRenewRepEnv(prevRepEnv, restartException).start();
    }

    /**
     * Recreates the replicated environment handle used to service all
     * requests directed at this node.
     * <p>
     * Multiple threads may simultaneously discover that the environment
     * has been invalidated and that new JE handles need to be established.
     * This method synchronizes the restart of the environment by ensuring
     * that at most one thread re-initiates the establishment of the
     * handles and any associated re-initializations, while the other
     * threads wait so they can use the newly established handle.
     *
     * @param prevRepEnv the previous environment handle that is being
     * re-established. It's null if the handle is being created for the first
     * time; in this case the restartException argument must be null as well.
     *
     * @param restartException the exception that provoked the
     * re-establishment of the environment handle. It's null if prevRepEnv is
     * null. If non-null, it must be a "renewable" exception, that is one for
     * which isRenewable() returns true.
     *
     * @return true if the environment handle was recreated by the call,
     * false if a creation was already in progress.
     */
    boolean renewRepEnv(ReplicatedEnvironment prevRepEnv,
                        DatabaseException restartException) {

        assert(((prevRepEnv == null) && (restartException == null)) ||
               ((prevRepEnv != null) && (restartException != null)));

        /*
         * The exception requiring a restart must satisfy isRenewable()
         * All others should result in a process exit.
         */
        assert((restartException == null) || isRenewable(restartException));

        if (!renewRepEnvSemaphore.tryAcquire()) {
            return false;
        }

        /* This thread is the one that will reopen the env handle. */
        try {
            envLock.writeLock().lockInterruptibly();
        } catch (InterruptedException ie) {
            renewRepEnvSemaphore.release();
            return false;
        }

        try {
            if ((repEnv != null) && (repEnv != prevRepEnv)) {
                /* It's already been renewed by some other thread. */
                return true;
            }

            /* Clean up previous environment. */
            if (prevRepEnv != null) {
                cleanupPrevEnv(prevRepEnv, restartException);
            }

            maybeResetRepGroup();
            repEnv = openEnv();
            setEnvForVerifyManager(repEnv);
            if (prevRepEnv == null) {
                /*
                 * Initial startup. Perform this check before doing anything
                 * else in case local databases need to be updated.
                 */
                versionManager.checkCompatibility(repEnv);
            }
            repEnv.setStateChangeListener(listenerFactory.create(repEnv));
            repNode.updateDbHandles(repEnv);
            /* Re-establish database handles. */
            logger.info("Replicated environment handle " +
                        ((prevRepEnv == null) ? "" : "re-") + "established." +
                        " Cache size: " + repEnv.getConfig().getCacheSize() +
                        ", State: " + repEnv.getState());
            return true;
        } finally {

            /*
             * Free the readers, so they can proceed with the new
             * environment handle.
             */
            envLock.writeLock().unlock();
            renewRepEnvSemaphore.release();
        }
    }

    /**
     * Predicate to determine whether the environment handle should be renewed
     * without restarting the process as a result of the exception.
     *
     * Only the InsufficientLogException and RollbackException subclasses of
     * EFE result in the handle actually being renewed in the process. All
     * other EFEs result in a process exit.
     */
    private boolean isRenewable(DatabaseException exception) {
        return exception instanceof InsufficientLogException ||
               exception instanceof RollbackException;
    }

    /**
     * Implement cleanup appropriate for the exception to ensure that the
     * environment is closed cleanly and can be reopened if necessary.
     *
     * @param prevRepEnv the env that needs to be cleaned up
     *
     * @param restartException the exception that provoked the close of the
     * environment
     */
    private void cleanupPrevEnv(ReplicatedEnvironment prevRepEnv,
                                DatabaseException restartException) {

        if (restartException instanceof InsufficientLogException) {
            /*
             * Restore the log files, so that the environment can be reopened.
             */
            networkRestore((InsufficientLogException) restartException);
        } else {
            logger.log(Level.INFO,
                       "Closing environment handle in response to exception",
                       restartException);
        }

        repNode.closeDbHandles(false);

        setEnvForVerifyManager(null);

        try {
            prevRepEnv.close();
        } catch (DatabaseException e) {
            /* Ignore the exception, but log it. */
            logger.log(Level.INFO, "Exception closing environment", e);
        }
    }

    /**
     * Close the environment.
     */
    void closeEnv() {
        /* Wait for readers to exit. */
        envLock.writeLock().lock();
        try {
            if (repEnv != null) {
                try {
                    repEnv.close();
                } catch (IllegalStateException ise) {
                    /*
                     * Log complaints about unclosed handles, etc. during
                     * the environment close and keep going.
                     */
                    logger.info("IllegalStateException during env close. " +
                                ise.getMessage());
                } catch (EnvironmentFailureException efe) {
                    /*
                     * Open database handles during a environment close on a
                     * replica can sometimes result in this exception. Ignore
                     * it and proceed as for ISE. Remove when JE SR22023 is
                     * addressed.
                     */
                    logger.info("Environment failure during close. " +
                                efe.getMessage());
                } finally {
                    /*
                     * Remove the pointer to the old environment so it can be
                     * GC'ed. This only really matters during testing in thread
                     * mode where the environments seem to stick around
                     * otherwise, and can cause OutOfMemoryErrors.
                     */
                    repEnv = null;
                }
            }
        } finally {
            envLock.writeLock().unlock();
        }
    }

    ReplicationNetworkConfig getRepNetConfig() {
        return renvConfig.getRepNetConfig();
    }

    long getReplicaAckTimeout(TimeUnit unit) {
        return renvConfig.getReplicaAckTimeout(unit);
    }

    /** Reset the JE replication group, if requested. */
    private void maybeResetRepGroup() {
        final RepNodeParams rnParams = repNode.getRepNodeParams();
        if (!rnParams.getResetRepGroup()) {
            return;
        }

        logger.info("Resetting replication group");
        final EnvironmentConfig resetEnvConfig = envConfig.clone();
        final ReplicationConfig resetRepConfig = renvConfig.clone();

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

        boolean networkRestoreDone = false;

        assert TestHookExecute.doHookIfSet(openEnvTestHook, repServiceParams);

        /*
         * Plumb JE environment logging and progress listening output to
         * KVStore monitoring.
         */
        envConfig.setLoggingHandler
            (new RepEnvRedirectHandler(repServiceParams));
        envConfig.setExtinctionFilter(new RepExtinctionFilter(repNode));
        envConfig.setRecoveryProgressListener
            (new RepEnvRecoveryListener(repServiceParams));
        renvConfig.setSyncupProgressListener
            (new RepEnvSyncupListener(repServiceParams));
        renvConfig.setLogFileRewriteListener
            (new RepEnvLogRewriteListener(snapshotDir, repServiceParams));

        while (true) {
            try {
                final ReplicatedEnvironment renv =
                        new ReplicatedEnvironment(envDir,
                                                  renvConfig,
                                                  envConfig,
                                                  NO_CONSISTENCY,
                                                  SIMPLE_MAJORITY);
                logger.info(String.format(
                                "Opened JE environment: " +
                                JEVersion.CURRENT_VERSION.getVersionString() +
                                " JVM max heap: %,d JE properties: %s",
                                JVMSystemUtils.getRuntimeMaxMemory(),
                                renv.getConfig()));
                return renv;
            } catch (UnknownMasterException unknownMaster) {

                /*
                 * Assuming that timeouts are correctly configured there
                 * isn't much point in retrying, just rethrow the
                 * exception.
                 */
                throw unknownMaster;
            } catch (InsufficientLogException ile) {
                if (networkRestoreDone) {

                    /*
                     * Should have made progress after the earlier network
                     * restore, propagate the exception to the caller so it
                     * can be logged and propagated back to the client.
                     */
                    throw ile;
                }
                networkRestore(ile);
                continue;
            } catch (RollbackException rbe) {
                Long time = rbe.getEarliestTransactionCommitTime();
                logger.info("Rollback exception retrying: " +
                            rbe.getMessage() +
                            ((time ==  null) ?
                             "" :
                             " Rolling back to: " + new Date(time)));
                continue;
            }
        }
    }

    /**
     * Configures and initiates a network restore operation in response to
     * an ILE
     */
    private void networkRestore(InsufficientLogException ile) {
        final NetworkRestoreConfig config = new NetworkRestoreConfig();

        // TODO: sort the ile.getLogProviders() list based upon
        // datacenter proximity and pass it in as the arg below.
        config.setLogProviders(null);
        final boolean nrConfigRetainLogFiles =
            repNode.getRepNodeParams().getNRConfigRetainLogFiles();
        config.setRetainLogFiles(nrConfigRetainLogFiles);
        /* Capture JE logging during network restore. */
        config.setLoggingHandler(new RepEnvRedirectHandler(repServiceParams));

        networkRestore = new NetworkRestore();
        try {
            networkRestore.execute(ile, config);
        } catch (EnvironmentFailureException efe) {
            /*
             * This may happen, for example, if no feeders were available to
             * supply the log files for the NR. Let the SN restart this RN, so
             * it can retry with a clean state.
             */
            logger.warning("Process exit queued." + efe.getMessage());
            repNodeService.getFaultHandler().
                queueShutdown(efe, ProcessExitCode.getRestart(efe));
            throw efe; /* Propagate the exception. */
        } finally {
            networkRestore = null;
        }
    }

    /**
     * Returns network backup statistics for a network restore that is
     * currently underway, or null if there is no network restore in progress
     * or the statistics are otherwise not available.
     */
    NetworkBackupStats getNetworkRestoreStats() {
        final NetworkRestore networkRestoreSnapshot = networkRestore;
        if (networkRestoreSnapshot != null) {
            return networkRestoreSnapshot.getNetworkBackupStats();
        }
        return null;
    }

    public boolean startAsyncNetworkRestore(RepNodeId sourceNode,
                                            boolean retainOriginalLogFile,
                                            long minVLSN) {

        final String msg;
        final boolean result;
        synchronized (this) {
            if (asyncNetworkRestore != null && asyncNetworkRestore.isAlive()) {
                msg = "Couldn't start async network restore, " +
                    "a restore task is already running.";
                result = false;
            } else {
                msg = "Start async network restore";
                asyncNetworkRestore  = new AsyncNetworkRestore(
                    sourceNode, retainOriginalLogFile, minVLSN);

                final KVThreadFactory factory =
                    new KVThreadFactory("NetworkRestoreThreadFactory", logger);
                final Thread t = factory.newThread(asyncNetworkRestore);
                asyncNetworkRestore.setExecutingThread(t);
                t.start();
                result = true;
            }
        }

        /* Log without holding the lock */
        logger.log(Level.INFO, msg);
        return result;
    }

    public synchronized NetworkRestoreStatus getAsyncNetworkRestoreStatus() {
        if (asyncNetworkRestore != null) {
            final boolean isCompleted = !asyncNetworkRestore.isAlive();
            final NetworkRestoreStatus status = asyncNetworkRestore.getStatus();
            if (isCompleted) {
                /* clean up */
                asyncNetworkRestore = null;
            }
            return status;
        }
        return null;
    }

    public void stopAsyncNetworkRestore(boolean force) {

        /*
         * Stop asyncNetworkRestore if it is alive, and clear it if it is
         * non-null. Synchronize when accessing asyncNetworkRestore but don't
         * hold the lock while stopping, to avoid blocking other requests.
         */
        final AsyncNetworkRestore restoreToStop;
        synchronized (this) {
            if (asyncNetworkRestore == null) {
                return;
            }
            if (!asyncNetworkRestore.isAlive()) {
                asyncNetworkRestore = null;
                return;
            }
            restoreToStop = asyncNetworkRestore;
        }
        restoreToStop.stop(force);

        /*
         * Only clear asyncNetworkRestore if it is the one we stopped, in case
         * there was a race with another start or stop
         */
        synchronized (this) {
            if (asyncNetworkRestore == restoreToStop) {
                asyncNetworkRestore = null;
            }
        }
    }

    /**
     * A custom Handler for JE log handling.  This class provides a unique
     * class scope for the purpose of logger creation.
     */
    private static class RepEnvRedirectHandler extends RedirectHandler {
        RepEnvRedirectHandler(Params repServiceParams) {
            super(LoggerUtils.getLogger(RepEnvRedirectHandler.class,
                                        repServiceParams));
        }
    }

    /**
     * A custom Handler for JE recovery recovery notification.  This class
     * provides a unique class scope for the purpose of logger creation.
     */
    private static class RepEnvRecoveryListener extends RecoveryListener {
        RepEnvRecoveryListener(Params repServiceParams) {
            super(LoggerUtils.getLogger(RepEnvRecoveryListener.class,
                                        repServiceParams));
        }
        /** Add a test hook */
        @Override
        public boolean progress(RecoveryProgress phase, long n, long total) {
            assert TestHookExecute.doHookIfSet(recoveryProgressTestHook,
                                               phase);
            return super.progress(phase, n, total);
        }
    }

    /**
     * A custom Handler for JE syncup progress notification.  This class
     * provides a unique class scope for the purpose of logger creation.
     */
    private static class RepEnvSyncupListener extends SyncupListener {
        RepEnvSyncupListener(Params repServiceParams) {
            super(LoggerUtils.getLogger(RepEnvSyncupListener.class,
                                        repServiceParams));
        }
    }

    /**
     * A custom Handler for JE log rewrite notification.  This class provides
     * a unique class scope for the purpose of logger creation.
     */
    private static class RepEnvLogRewriteListener extends LogRewriteListener {
        RepEnvLogRewriteListener(File snapshotDir, Params repServiceParams) {
            super(snapshotDir,
                  LoggerUtils.getLogger(RepEnvLogRewriteListener.class,
                                        repServiceParams));
        }
    }

    /**
     * Implementation of ParameterListener.
     */
    @Override
    public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
        final ReplicatedEnvironment env = getEnv(1);

        if ((env == null) || !env.isValid()) {
            /*
             * The parameters will be set on the env when the handle is created.
             */
            return;
        }

        boolean mutConfigUpdated = false;
        final EnvironmentMutableConfig mutableConfig = env.getMutableConfig();
        long oldSize = mutableConfig.getCacheSize();
        int oldPercent = mutableConfig.getCachePercent();
        long newSize = newMap.getOrZeroLong(ParameterState.JE_CACHE_SIZE);
        int newPercent = newMap.get(ParameterState.RN_CACHE_PERCENT).asInt();
        if (oldSize != newSize || oldPercent != newPercent) {
            mutableConfig.setCacheSize(newSize);
            mutableConfig.setCachePercent(newPercent);
            mutConfigUpdated = true;
        }

        String erasurePeriodStr = mutableConfig
            .getConfigParam(EnvironmentParams.ERASE_PERIOD.getName());
        long oldErasurePeriod = PropUtil.parseLongDuration(erasurePeriodStr);
        boolean isErasureEnabled = Boolean.parseBoolean(mutableConfig
            .getConfigParam(EnvironmentParams.ENV_RUN_ERASER.getName()));

        boolean runErasure =
            newMap.getOrDefault(ParameterState.JE_ENABLE_ERASURE).asBoolean();
        DurationParameter dp = (DurationParameter) newMap
            .getOrDefault(ParameterState.JE_ERASURE_PERIOD);
        long newErasurePeriod = dp.toMillis();

        if (oldErasurePeriod != newErasurePeriod ||
            isErasureEnabled != runErasure) {
            mutableConfig
                .setConfigParam(EnvironmentParams.ENV_RUN_ERASER.getName(),
                    Boolean.toString(runErasure));
            mutableConfig
                .setConfigParam(EnvironmentParams.ERASE_PERIOD.getName(),
                    dp.asString());
            mutConfigUpdated = true;
        }

        if (mutConfigUpdated) {
            env.setMutableConfig(mutableConfig);
        }

        final ReplicationMutableConfig repMutableConfig =
            env.getRepMutableConfig();
        final int oldElectableGroupSizeOverride =
            repMutableConfig.getElectableGroupSizeOverride();
        final RepNodeParams newParams = new RepNodeParams(newMap);
        final int newElectableGroupSizeOverride =
            newParams.getElectableGroupSizeOverride();

        final int oldNodePriority = repMutableConfig.getNodePriority();
        final int newNodePriority = newParams.getJENodePriority();
        if (oldElectableGroupSizeOverride != newElectableGroupSizeOverride ||
            oldNodePriority != newNodePriority) {
            repMutableConfig.setElectableGroupSizeOverride(
                newElectableGroupSizeOverride);
            repMutableConfig.setNodePriority(newNodePriority);
            env.setRepMutableConfig(repMutableConfig);
        }

        /*
         * Check whether the new parameters specify a new node type that is not
         * reflected by the environment.  This could happen if, because of a
         * bug, we failed to restart the environment after updating the
         * parameter.
         */
        final NodeType envNodeType = env.getRepConfig().getNodeType();
        final NodeType newNodeType = newParams.getNodeType();
        if (!envNodeType.equals(newNodeType)) {
            throw new IllegalStateException(
                "Environment for " + repNode.getRepNodeId() +
                " has wrong node type: expected " + newNodeType +
                ", found " + envNodeType);
        }
        ParameterUtils.setJEMutableParams(oldMap, newMap, env, logger);
    }

    /**
     * The factory interface used to create a new SCL each time an environment
     * is opened.
     */
    public interface StateChangeListenerFactory {
        public StateChangeListener create(ReplicatedEnvironment repEnv);
    }

    /**
     * Thread to perform an asynchronous renewal of the replicated environment
     * handle. This short-lived thread is used when an established thread of
     * control cannot be stalled for a potentially long-running operation like
     * the establishment of the environment handle.
     */
    private class AsyncRenewRepEnv extends StoppableThread {
        /* The environment provoking the exception. */
        ReplicatedEnvironment environment;

        /* Rollback exception in the replica thread. */
        DatabaseException exception;

        @Override
        public void run() {
            try {
                renewRepEnv(environment, exception);
            } finally {
                /* Remove references to avoid OOME. [#25649] */
                environment = null;
                exception = null;
            }
        }

        /* (non-Javadoc)
         * @see com.sleepycat.je.utilint.StoppableThread#getLogger()
         */
        @Override
        protected Logger getLogger() {
            return logger;
        }

        AsyncRenewRepEnv(ReplicatedEnvironment environment,
                         DatabaseException exception) {
            super("AsyncRenewRepEnvThread");
            if (!isRenewable(exception)) {
                throw new IllegalArgumentException("Unexpected exception: " +
                                                    exception);
            }
            this.environment = environment;
            this.exception = exception;
        }
    }

    /**
     * Task to perform a network restore from specified source node
     * asynchronously. A network restore process may be a long-running task,
     * which is initialized from RepNodeAdmin caller. This task also maintain
     * the errors during network restore in order to let caller poll and check
     * the status.
     */
    private class AsyncNetworkRestore implements Runnable {

        /*
         * Ignore the load threshold since this restore is intended to be
         * perform from specified server no matter how many feeders it has.
         */
        private static final int LOAD_THRESHOLD = Integer.MAX_VALUE;

        /* The size of the network restore client socket's receive buffer */
        private static final int RECEIVE_BUFFER_SIZE = 0x200000; /* 2 MB */

        /* Number of network backup execution retry */
        private static final int EXECUTION_RETRY = 5;

        private static final long RETRY_INTERVAL = 3000;

        private static final int THREAD_SOFT_SHUTDOWN_MS = 5000;

        private final RepNodeId sourceNode;

        /* Retain original log file */
        private final boolean retainOriginalLogFile;

        /* Minimal VLSN the restore must cover */
        private final long minVLSN;

        private volatile NetworkBackup backup;

        /*
         * The thread executing the source runnable. Need to keep this because
         * we may have to wait for the thread to exit on shutdown.
         */
        private volatile Thread executingThread = null;

        /*
         * Any errors happened during network restore, the only clue to
         * indicate if the process succeed or not.
         */
        private volatile NetworkRestoreException exception = null;

        /*
         * Retry number of backup execution.
         */
        private volatile int retryNum = 1;
        private volatile boolean stopped;

        AsyncNetworkRestore(RepNodeId sourceNode,
                            boolean retainOriginalLogFile,
                            long minVLSN) {
            this.sourceNode = sourceNode;
            this.retainOriginalLogFile = retainOriginalLogFile;
            this.minVLSN = minVLSN;
        }

        NetworkBackupStats getNetworkBackupStats() {
            if (backup != null) {
                return backup.getStats();
            }
            return null;
        }

        void setExecutingThread(Thread thread) {
            executingThread = thread;
        }

        boolean isAlive() {
            return (executingThread != null);
        }

        NetworkRestoreStatus getStatus() {
            return new NetworkRestoreStatus(!isAlive(), retryNum, exception,
                                            getNetworkBackupStats());
        }

        /**
         * Stop the thread running this task. If force, don't wait the thread
         * is completed.
         */
        synchronized void stop(boolean force) {
            stopped = true;

            if (force) {
                return;
            }

            final Thread thread = executingThread;

            /* Wait if there is a thread AND is is running */
            if ((thread != null) && thread.isAlive()) {
                assert Thread.currentThread() != thread;

                try {
                    logger.log(Level.FINE, "Waiting for {0} to exit", this);
                    thread.join(THREAD_SOFT_SHUTDOWN_MS);

                    if (isAlive()) {
                        logger.log(Level.FINE, "Stop of {0} timed out", this);
                    }
                } catch (InterruptedException ie) {
                    throw new IllegalStateException(ie);
                }
            }
        }

        private InetSocketAddress getSourceNodeSocketAddress() {
            final Topology topo = repNode.getTopology();

            if (topo == null) {
                throw new IllegalStateException(
                    "Source node not yet initialized");
            }
            PingCollector collector = new PingCollector(topo, logger);
            final RepGroupId rgId =
                new RepGroupId(repNode.getRepNodeId().getGroupId());
            final RepNodeStatus rnStatus =
                collector.getRepNodeStatus(rgId).get(sourceNode);

            if (rnStatus == null) {
                throw new IllegalStateException(
                    "Source node not yet initialized");
            }
            final String haHostPort = rnStatus.getHAHostPort();

            /* getHAHostPort() returns null for a R1 node */
            if (haHostPort == null) {
                throw new IllegalStateException("Source node " + sourceNode +
                                                " is running an incompatible " +
                                                "software version");
            }

            return HostPortPair.getSocket(haHostPort);
        }

        private void renewDatabaseHandles() {
            /* Re-establish database handles. */
            repEnv.setStateChangeListener(listenerFactory.create(repEnv));
            repNode.updateDbHandles(repEnv);
            logger.info("Replicated environment handle " +
                        "re-established." +
                        " Cache size: " + repEnv.getConfig().getCacheSize() +
                        ", State: " + repEnv.getState());
        }

        private void executeNetworkBackup(File envHome,
                                          NameIdPair server,
                                          RepImpl repImpl)
           throws Exception {

            while (!stopped) {
                Exception backupException = null;
                try {
                    backup = new NetworkBackup(
                        getSourceNodeSocketAddress(),
                        RECEIVE_BUFFER_SIZE,
                        envHome,
                        server,
                        retainOriginalLogFile,
                        LOAD_THRESHOLD,
                        minVLSN,
                        repImpl,
                        repImpl.getFileManager(),
                        repImpl.getLogManager(),
                        repImpl.getChannelFactory(),
                        new Properties());
                    backup.setInterruptHook(NETWORK_BACKUP_HOOK);
                    backup.execute();
                    assert TestHookExecute.doHookIfSet(FAULT_HOOK, 2);
                    return;
                } catch (InsufficientVLSNRangeException ivre) {
                    logger.log(Level.INFO, "Source node " + sourceNode +
                               " of network backup doesn't cover minimum vlsn");

                     /* Not retrying, source node doesn't cover minimum vlsn */
                     throw ivre;
                }  catch (DatabaseException de) {
                    logger.log(Level.INFO, "Source node " + sourceNode +
                               " of network backup is likely not" +
                               " functioning, " + retryNum +
                               " retry of network backup failed.");

                    backupException = de;
                } catch (IOException| ServiceConnectFailedException e) {
                    logger.log(Level.INFO, "Backup from node " + sourceNode +
                               " encounter network connection error, " +
                               retryNum + " retry of network backup failed.");
                    backupException = e;
                } catch (LoadThresholdExceededException e) {
                    /*
                     * Should not happen, currently use the maximum integer
                     * as load threshold. Reserve this in case need control
                     * in future.
                     */
                    logger.log(Level.INFO, "Source node " + sourceNode +
                               " of network backup is busy at this moment, " +
                               retryNum + " retry of network backup failed.");
                    backupException = e;
                } catch (Exception e) {
                    logger.log(Level.INFO, "Network backup from " + sourceNode +
                        " encounter unknown exception " +
                        retryNum + " retry of network backup failed.");
                    backupException = e;
                }

                if (retryNum >= EXECUTION_RETRY) {
                    throw backupException;
                }
                retryNum++;
                try {
                    Thread.sleep(RETRY_INTERVAL);
                } catch (InterruptedException ignored) {
                    throw new IllegalStateException("unexpected interrupt");
                }
            }
        }

        @Override
        public void run() {
            executingThread = Thread.currentThread();

            ReplicatedEnvironment prevEnv = getEnv(1);
            if (prevEnv == null || !prevEnv.isValid()) {
                this.exception = new NetworkRestoreException(
                    "Environment is invalid");
                return;
            }
            final RepImpl repImpl = RepInternal.getRepImpl(prevEnv);
            final NameIdPair clientId = repImpl.getRepNode().getNameIdPair();
            final File envHome = prevEnv.getHome();

            if (!renewRepEnvSemaphore.tryAcquire()) {
                this.exception = new NetworkRestoreException(
                    "Unable to obtain sempahore of updating env handle");
                return;
            }

            /* This thread is the one that will reopen the env handle. */
            try {
                envLock.writeLock().lockInterruptibly();
            } catch (InterruptedException ie) {
                renewRepEnvSemaphore.release();
                this.exception = new NetworkRestoreException(
                    "Unable to obtain lock of env handle");
                return;
            }

            try {
                /*
                 * Network backup requires caller to ensure execution
                 * succeed before accessing the environment. Close all
                 * database handles and environment as well as set current
                 * environment reference as null, so request handler won't
                 * execute request against current environment.
                 */
                cleanupPrevEnv(prevEnv, null);
                repEnv = null;

                /* Backup execution with configured retry */
                assert TestHookExecute.doHookIfSet(FAULT_HOOK, 1);
                executeNetworkBackup(envHome, clientId, repImpl);
                logger.log(Level.INFO, "Restore from {0} succeed, {1}",
                           new Object[] {sourceNode, backup.getStats()});
            } catch (Exception e) {
                logger.log(Level.INFO, "Restore from {0} failed, {1}",
                           new Object[] {sourceNode, e.getMessage()});
                this.exception = new NetworkRestoreException(e);
            } finally {

                try {

                    /*
                     * No matter this restore task succeed or not, clean up
                     * previous and reopen the environment as well as
                     * re-establish database handles.
                     */
                    cleanupPrevEnv(prevEnv, null);
                    repEnv = openEnv();
                    renewDatabaseHandles();
                } finally {

                    /*
                     * Free the readers, so they can proceed with the new
                     * environment handle.
                     */
                    envLock.writeLock().unlock();
                    renewRepEnvSemaphore.release();
                    executingThread = null;
                    prevEnv = null;
                }
            }
        }
    }

    public static class NetworkRestoreException extends InternalFaultException {
        private static final long serialVersionUID = 1L;

        private NetworkRestoreException(Throwable cause) {
            super(cause);
        }

        private NetworkRestoreException(String errorMsg) {
            super(new IllegalStateException(errorMsg));
        }
    }

    public void setEnvForVerifyManager(ReplicatedEnvironment env) {
        verificationManager.setEnv(env);
    }

    /**
     * Verify data for this RepNode.
     */
    public VerificationInfo startAndPollVerify(VerificationOptions options,
                                               int planId) {

        return verificationManager.startAndPollVerification(options,
                                                            planId);
    }

    public boolean interruptVerify() {
        return verificationManager.interruptVerification();
    }
}
