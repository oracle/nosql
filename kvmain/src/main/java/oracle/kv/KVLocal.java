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

package oracle.kv;

import static oracle.kv.impl.async.UnixDomainNetworkAddress.UNIX_DOMAIN_SOCKETS_JAVA_VERSION;
import static oracle.kv.impl.param.ParameterState.SN_SERVICE_STOP_WAIT;
import static oracle.kv.impl.util.VersionUtil.getJavaMajorVersion;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.UnixDomainNetworkAddress;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.sna.ProcessMonitor;
import oracle.kv.impl.sna.ProcessServiceManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.DatabaseUtils.VerificationOptions;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.kvlite.KVLite;
import oracle.kv.util.kvlite.KVLite.KVLiteParser;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/**
 * KVLocal is a handle for managing embedded Oracle NoSQL Database.
 *
 * <p>
 * Terminology:
 * <ul>
 * <li>Handle - an instance of an interface used to access the store</li>
 * <li>Embedded Oracle NoSQL database - a single-node store that is not
 * replicated</li>
 * <li>Store - Oracle NoSQL Database physical files, including configuration
 * files and data files</li>
 * <li>Root directory - a physical directory that contains a store<br>
 * The root directory must either not exist or contain a store.</li>
 * </ul>
 * <br>
 * You configure, start, and stop embedded Oracle NoSQL database using
 * KVLocal public APIs. For example:
 *
 * <pre>
 * // Create a KVLocalConfig object with root directory specified as "rootDir"
 * KVLocalConfig config = new KVLocalConfig.InetBuilder(rootDir)
 *                                         .build();
 * // Create an embedded NoSQL database and start an instance of it
 * KVLocal local = KVLocal.start(config);
 * // Get a store handle to the running embedded NoSQL database
 * KVStore storeHandle = local.getStore();
 * // Free up resources associated with the store handle
 * local.closeStore();
 * // Stop the running embedded NoSQL database instance
 * local.stop();
 *</pre>
 *
 * @since 22.1
 */
public class KVLocal {

    /* Only one embedded NoSQL database instance can be started in a JVM */
    private static final AtomicBoolean instanceStarted = new AtomicBoolean();
    private static final Logger staticLogger =
        ClientLoggerUtils.getLogger(KVLocal.class, "kvlocal");
    private static final String LOCK_FILE = "kvlocal-lock";

    /**
     * The name of a system property that specifies a space-separated list of
     * command line flags for the Java process used to run KVLite.
     */
    private static final String KVLITE_FLAGS_NAME =
        "oracle.kv.kvlocal.kvlite.flags";
    private static final String KVLITE_FLAGS =
        System.getProperty(KVLITE_FLAGS_NAME);

    /**
     * The name of a system property that specifies the number of milliseconds
     * to wait for KVLite to start.
     */
    private static final String KVLITE_START_TIMEOUT_NAME =
        "oracle.kv.kvlocal.kvlite.start.timeout";
    private static final long KVLITE_START_TIMEOUT =
        Long.getLong(KVLITE_START_TIMEOUT_NAME, 10 * 60 * 1000);

    private final KVLocalConfig localConfig;
    private final KVLiteMonitor kvlite;
    private volatile KVStore storeHandle;

    /**
     * The channel for the file that was locked to get exclusive access for
     * starting the KVLite in the configured root directory, or null if KVLite
     * has not been started. Locks are released on JVM exit, so the lock
     * provides a dependable way to know that this process is the only KVLocal
     * starting KVLite in the specified directory. Synchronize when setting
     * this field outside of the constructor and when closing the channel to
     * release the lock.
     */
    private volatile FileChannel lock;

    /**
     * Creates a KVLocal object.
     */
    private KVLocal(KVLocalConfig localConfig,
                    KVLiteMonitor kvlite,
                    KVStore storeHandle,
                    FileChannel lock) {
        this.localConfig = localConfig;
        this.kvlite = kvlite;
        this.storeHandle = storeHandle;
        this.lock = lock;
    }

    /**
     * Starts an embedded NoSQL database instance.
     *
     * <p>
     * If the root directory does not exist, a new store will be created
     * using parameters declared by KVLocalConfig. If a store already exists
     * in the root directory, that store is opened.
     *
     * <p>
     * The KVLocal instance obtained by this method must be explicitly
     * stopped using {@link KVLocal#stop}.
     *
     * @param config the KVLocal configuration parameters
     *
     * @return an instance of KVLocal
     *
     * @throws IllegalStateException if an embedded Oracle NoSQL Database
     * instance is already running, the root directory is not accessible, or
     * the root directory exists but does not contain a store
     * @throws IllegalArgumentException if existing store's parameters do not
     * match the parameters declared by KVLocalConfig
     * @throws KVLocalException if an error occurs when starting the embedded
     * NoSQL database instance
     */
    public static KVLocal start(KVLocalConfig config) {
        return startInternal(config, true, null);
    }

    /**
     * Starts an embedded NoSQL database instance from the root directory.
     *
     * <p>
     * The KVLocal instance obtained by this method must be explicitly
     * stopped using {@link KVLocal#stop}.
     *
     * @param rootDir the root directory of the existing store
     *
     * @return an instance of KVLocal
     *
     * @throws IllegalStateException if an embedded Oracle NoSQL Database
     * instance is already running, the root directory is not accessible,
     * the root directory's parent directory does not exist, or the root
     * directory exists but does not contain proper files
     * @throws KVLocalException if an error occurs when starting the embedded
     * NoSQL database instance
     */
    public static KVLocal startExistingStore(String rootDir) {

        KVLocalConfig config = getKVLocalConfigFromRootDir(rootDir);
        return startInternal(config, false, null);
    }

    /**
     * Stops the running embedded NoSQL database instance.
     *
     * @throws IllegalStateException if embedded NoSQL database was not started
     * as an embedded instance
     * @throws KVLocalException if an error occurs when stopping the embedded
     * NoSQL database instance
     */
    public synchronized void stop() {
        staticLogger.fine(() -> "KVLocal stop: " + localConfig);
        if (kvlite == null) {
            throw new IllegalStateException(
                "The embedded NoSQL database instance was not started" +
                " as an embedded instance");
        }
        try {
            stopKVLite(localConfig, kvlite, getServiceWaitMillis());
        } finally {
            if (lock != null) {
                try {
                    lock.close();
                } catch (IOException e) {
                }
                lock = null;
            }
            instanceStarted.set(false);
        }
    }

    /**
     * Returns the value of the SN_SERVICE_STOP_WAIT parameter, if available,
     * or else the default value.
     */
    private long getServiceWaitMillis() {
        try {
            final File configFile =
                getConfigFile(localConfig.getRootDirectory());
            final BootstrapParams bp =
                ConfigUtils.getBootstrapParams(configFile);
            final File snConfigPath = ConfigUtils.getSNConfigPath(bp);
            final StorageNodeParams snParams =
                ConfigUtils.getStorageNodeParams(snConfigPath);
            return snParams.getServiceWaitMillis();
        } catch (IllegalStateException e) {
            /* Just return the default value */
            final DurationParameter waitParam = (DurationParameter)
                ParameterState.lookup(SN_SERVICE_STOP_WAIT)
                .getDefaultParameter();
            return waitParam.toMillis();
        }
    }

    /** Returns the config.xml file for the specified root directory */
    private static final File getConfigFile(String rootDir) {
        return new File(rootDir + File.separator + "config.xml");
    }

    /** Stops KVLite using the specified service wait time. */
    private static void stopKVLite(KVLocalConfig localConfig,
                                   KVLiteMonitor kvlite,
                                   long serviceWaitMillis) {
        /* Try doing an orderly shutdown first */
        final StorageNodeAgentImpl snai = new StorageNodeAgentImpl();
        snai.parseArgs(
            new String[] { "-root", localConfig.getRootDirectory() });
        final StorageNodeAgent sna = snai.getStorageNodeAgent();
        try {
            sna.stopRunningAgent();
            /* Wait 2x to account for the two services: RN and admin */
            if (kvlite.waitProcess(2 * serviceWaitMillis)) {
                return;
            }
        } catch (IllegalStateException|InterruptedException e) {
            /* Couldn't contact the SNA this way */
            staticLogger.log(Level.FINE, "Couldn't contact SNA", e);
        }

        /*
         * If the orderly shutdown didn't seem to work, then kill the process
         */
        killKVLite(kvlite);
    }

    /** Kill the KVLite process. */
    private static void killKVLite(KVLiteMonitor kvlite) {
        try {
            kvlite.stopProcess(false /* isMonitor */);
        } catch (InterruptedException e) {
            throw new KVLocalException(
                "Exception in stop: " + e.getMessage(), e);
        }
    }

    /**
     * Whether the embedded NoSQL database instance is running.
     *
     * @return whether the embedded NoSQL database instance is running
     */
    public boolean isRunning() {
        return (kvlite != null) && kvlite.isRunning();
    }

    /**
     * Returns an instance of KVLocal based on the root directory, which must
     * contain an existing store.
     *
     * @param rootDir the root directory of the store
     * @return an instance of KVLocal
     * @throws IllegalStateException if the store directory is not found,
     * or the store directory is not accessible, or the root directory does
     * not contain an existing store.
     */
    public static KVLocal getKVLocal(String rootDir) {
        KVLocalConfig config = getKVLocalConfigFromRootDir(rootDir);
        return new KVLocal(config, null /* kvlite */, null /* storeHandle */,
                           null /* lock */);
    }

    /**
     * Gets a store handle to a running embedded NoSQL database.
     *
     * <p>
     * A new store handle is created as needed on the first call to this
     * method. All subsequent calls return the existing store handle. If the
     * existing store handle is cleaned up by invocation of {@link
     * KVLocal#closeStore}, the next call to this method will create a new
     * store handle again.
     *
     * <p>
     * The application must invoke {@link KVLocal#closeStore} when it
     * is done accessing the store to free up resources associated with the
     * store handle. Don't invoke {@link KVStore#close}, because it does not
     * free up all the resources associated with the store handle and makes
     * the store handle non-functional.
     *
     * @see KVLocal#closeStore
     *
     * @return an instance of KVStore
     *
     * @throws FaultException if the store is not started, or an error occurs
     * when getting the store
     */
    public synchronized KVStore getStore() {
        if (storeHandle == null) {
            storeHandle = getStore(localConfig);
        }
        return storeHandle;
    }

    /**
     * Close the store handle and release resources.
     */
    public synchronized void closeStore() {
        if (storeHandle != null) {
            storeHandle.close();
            storeHandle = null;
        }
    }

    /**
     * Restores the store from a snapshot.
     *
     * <p>
     * This method replaces the data in the root directory with the data
     * specified in the snapshot, then starts an embedded NoSQL database
     * instance. The KVLocal instance obtained by this method must be
     * explicitly stopped using {@link KVLocal#stop}.
     *
     * @param rootDir the root directory of the store
     * @param name the name of the snapshot, including date and time that was
     * generated by createSnapshot
     *
     * @return an instance of KVLocal
     *
     * @throws IllegalStateException if an embedded NoSQL database is running,
     * or the specified snapshot is not found, or the root directory is not
     * accessible, or the root directory does not contain an existing store
     * @throws KVLocalException if an error occurs when restoring from the
     * snapshot
     */
    public static KVLocal restoreFromSnapshot(String rootDir, String name) {
        KVLocalConfig config = getKVLocalConfigFromRootDir(rootDir);
        return startInternal(config, false, name);
    }

    /**
     * Creates a new snapshot using the specified name as suffix.
     *
     * <p>
     * This method backups the storage node data files, configuration files,
     * and adds other required files required for restore activities. The
     * snapshot data is stored in a directory inside of the root directory.
     * To preserve storage, invoke {@link KVLocal#removeSnapshot} to remove
     * obsolete snapshots.
     *
     * @param name the suffix to use for the snapshot name
     *
     * @return the generated snapshot name. The generated snapshot name has
     * date-time prefix. The date-time prefix consists of a 6-digit, year,
     * month, day value in YYMMDD format, and a 6-digit hour, minute, seconds
     * timestamp as HHMMSS. The date and time values are separated from each
     * other with a dash (-), and include a dash (-) suffix before the input
     * snapshot name.
     *
     * @throws KVLocalException if an error occurs when creating snapshot
     */
    public String createSnapshot(String name) {
        try {
            final CommandServiceAPI cs = getAdmin(localConfig);
            final Snapshot snapshot = new Snapshot(cs, false, null);
            final String snapshotName = snapshot.createSnapshot(name);
            if (!snapshot.succeeded()) {
                throw new KVLocalException(
                    "Problem creating snapshot: " + snapshot.getFailures());
            }
            return snapshotName;
        } catch (KVLocalException e) {
            throw e;
        } catch (RemoteException|RuntimeException e) {
            throw new KVLocalException
                ("Problem creating snapshot: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the names of all snapshots.
     *
     * @return an array of names of snapshots
     *
     * @throws KVLocalException if an error occurs when listing snapshots
     */
    public String[] listSnapshots() {
        try {
            final CommandServiceAPI cs = getAdmin(localConfig);
            final Snapshot snapshot = new Snapshot(cs, false, null);
            return snapshot.listSnapshots();
        } catch (RemoteException e) {
            throw new KVLocalException
                ("Problem listing snapshots: " + e.getMessage(), e);
        }
    }

    /**
     * Removes the named snapshot. The method will return successfully
     * if the named snapshot is not found.
     *
     * @param name the full name of the snapshot, including date and time that
     * was generated by createSnapshot
     *
     * @throws KVLocalException if an error occurs when removing snapshot
     */
    public void removeSnapshot(String name) {
        try {
            final CommandServiceAPI cs = getAdmin(localConfig);
            final Snapshot snapshot = new Snapshot(cs, false, null);
            snapshot.removeSnapshot(name);
            if (!snapshot.succeeded()) {
                throw new KVLocalException(
                    "Problem removing snapshot: " + snapshot.getFailures());
            }
        } catch (KVLocalException e) {
            throw e;
        } catch (RemoteException|RuntimeException e) {
            throw new KVLocalException
                ("Problem removing snapshot: " + e.getMessage(), e);
        }
    }

    /**
     * Verifies the store configuration by iterating over components and
     * checking their state against what the Admin database contains.
     *
     * <p>
     * This method checks if an embedded NoSQL database instance
     * is running properly and healthy. If there is any configuration error,
     * violations or warnings will be generated in the output. Violations are
     * issues that can cause problems and should be investigated.
     *
     * @param verbose whether the output contains verbose output. If false,
     * the output contains violations and warnings only.
     *
     * @return the verify configuration result in JSON format
     *
     * @throws KVLocalException if an error occurs when verifying
     * configuration
     *
     */
    public String verifyConfiguration(boolean verbose) {
        VerifyResults result;
        try {
            final CommandServiceAPI cs = getAdmin(localConfig);
            result = cs.verifyConfiguration(false, true, true);
        } catch (RemoteException|RuntimeException e) {
            throw new KVLocalException
                ("Problem verifying configuration: " + e.getMessage(), e);
        }

        if (verbose) {
            return result.display();
        }

        /* The non-verbose output contains violations and warnings only. If no
         * violation and warning are found, empty JSON will be returned.
         */
        if (result.okay()) {
            return "{}";
        }
        ObjectNode objNode = JsonUtils.parseJsonObject(result.display());
        Set<String> fnames = objNode.fieldNames();
        String[] fieldNames = fnames.toArray(new String[0]);
        for (String fname : fieldNames) {
            if (!fname.equals("violations") && !fname.equals("warnings")) {
                objNode.remove(fname);
            }
        }
        return JsonUtils.toJsonString(objNode, true);
    }

    /**
     * Verifies store data integrity.
     *
     * <p>
     * This method is relatively time consuming. It verifies the Log record
     * integrity on disk and B-tree integrity in memory.
     *
     * <p>
     * If any service instance (such as Admin or an RN) has persistent B-tree
     * or log corruptions, the service shuts down, and the JE (Berkeley)
     * environment is invalidated. JE then creates a file called
     * 7fffffff.jdb, placing it wherever other .jdb files exist in your
     * environment. Manual administration intervention is required to
     * recover from persistent data corruption. <br>
     *  If any service instance has transient corruption, the service
     *  automatically exits. Transient corruption can be caused by a memory
     *  corruption. Restarting embedded NoSQL database instance is required to
     *  recover from transient corruption.
     *
     * @return the verify data result in JSON format. If no corruption is
     * found, the result shows "No Btree Corruptions" and
     * "No Log File Corruptions".
     *
     * @throws KVLocalException if an error occurs when verifying data
     */
    public String verifyData() {
        final VerificationOptions options = new VerificationOptions(
            VerificationOptions.DEFAULT_VERIFY_BTREE,
            VerificationOptions.DEFAULT_VERIFY_LOG,
            VerificationOptions.DEFAULT_VERIFY_INDEX,
            VerificationOptions.DEFAULT_VERIFY_DATARECORD,
            VerificationOptions.DEFAULT_BTREE_DELAY,
            VerificationOptions.DEFAULT_LOG_DELAY,
            VerificationOptions.DEFAULT_EXPIRED_TIME,
            VerificationOptions.DEFAULT_SHOW_MISSING_FILES);

        Plan plan;
        try {
            final RepNodeId rid = new RepNodeId(1,1);
            final CommandServiceAPI cs = getAdmin(localConfig);
            final int planId = cs.createVerifyServicePlan(null, rid, options);

            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);

            plan = cs.getPlanById(planId);
        } catch (RemoteException|RuntimeException e) {
            throw new KVLocalException
                ("Problem verifying data: " + e.getMessage(), e);
        }
        return plan.getCommandResult().getReturnValue();
    }

    /**
     * Returns the configuration parameters.
     *
     * @return the configuration parameters
     */
    public KVLocalConfig getKVLocalConfig() {
        return localConfig;
    }

    /**
     * Returns the store name.
     *
     * @return the store name
     */
    public String getStoreName() {
        return localConfig.getStoreName();
    }

    /**
     * Returns the host name.
     *
     * @return the host name
     */
    public String getHostName() {
        return localConfig.getHostName();
    }

    /**
     * Returns the directory where NoSQL Database data is placed.
     *
     * @return the directory where NoSQL Database data is placed
     */
    public String getRootDirectory() {
        return localConfig.getRootDirectory();
    }

    /**
     * Returns the port number.
     *
     * @return the port number
     */
    public int getPort() {
        return localConfig.getPort();
    }

    /**
     * Returns the memory size in MB. The memory size is the total RAM on the
     * machine that can be used for the store.
     *
     * @return the memory size in MB
     */
    public int getMemoryMB() {
        return localConfig.getMemoryMB();
    }

    /**
     * Returns the storage directory size in GB.
     *
     * @return the storage directory size in GB
     */
    public int getStorageGB() {
        return localConfig.getStorageGB();
    }

    /**
     * Returns whether security is enabled.
     *
     * @return whether security is enabled
     */
    public boolean isSecure() {
        return localConfig.isSecure();
    }

    /** Gets a store appropriate for the configuration. */
    private static KVStore getStore(KVLocalConfig localConfig) {
        final KVStoreConfig config = new KVStoreConfig(
            localConfig.getStoreName(),
            localConfig.getHostName() + ":" + localConfig.getPort());
        if (localConfig.isSecure()) {
            final String securityPath =
                getSecurityPath(localConfig.getRootDirectory());
            config.setSecurityProperties(
                KVStoreLogin.createSecurityProperties(securityPath));
        }
        if (localConfig.isUnixDomain()) {
            config.setUseRmi(false);
        }
        return KVStoreFactory.getStore(config);
    }

    /**
     * Starts an embedded NoSQL database instance internally.
     *
     * @param config the KVLocal configuration parameters
     * @param verifyConfig whether to verify that config matches the
     * configuration in the root directory
     * @param snapshotName snapshot name
     *
     * @return an instance of KVLocal
     *
     * @throws IllegalStateException if an embedded Oracle NoSQL Database
     * instance is already running, or if a snapshot name is specified and the
     * snapshot is not found
     * @throws IllegalArgumentException if existing store's parameters do not
     * match the parameters declared by KVLocalConfig
     * @throws KVLocalException if an error occurs when starting the
     * embedded NoSQL database instance
     */
    private static KVLocal startInternal(KVLocalConfig config,
                                         boolean verifyConfig,
                                         String snapshotName) {
        staticLogger.fine(() -> "KVLocal start:" +
                          " config=" + config +
                          " verifyConfig=" + verifyConfig +
                          " snapshotName=" + snapshotName);
        if (config == null) {
            throw new IllegalArgumentException
                ("The KVLocalConfig cannot be null");
        }

        if (config.isUnixDomain() &&
            (getJavaMajorVersion() < UNIX_DOMAIN_SOCKETS_JAVA_VERSION)) {
            throw new IllegalStateException(
                "Starting KVLocal using Unix domain sockets requires"+
                " Java " + UNIX_DOMAIN_SOCKETS_JAVA_VERSION + " or later," +
                " but running Java version " +
                System.getProperty("java.version"));
        }

        if (!instanceStarted.compareAndSet(false, true)) {
            throw new IllegalStateException("Only one instance of embedded" +
                " NoSQL database can be running");
        }
        FileChannel lock = null;
        KVLiteMonitor kvlite = null;
        boolean complete = false;
        try {
            lock = getLock(config);
            final String rootDir = config.getRootDirectory();
            if (snapshotName != null) {
                final File snapshotDir = FileNames.getSnapshotNamedDir(
                    rootDir, snapshotName);
                if (!snapshotDir.exists()) {
                    throw new IllegalStateException(
                        "Snapshot directory not found: " + snapshotDir);
                }
            }

            /*
             * Throw exception if existing store's parameters do not match the
             * parameters declared by KVLocalConfig
             */
            if (verifyConfig) {
                validateParameters(config);
            }

            if (NetworkAddress.isUnixDomainHostname(config.getHostName())) {
                prepareSocketDirectory(config.getHostName());
            }

            /*
             * Root directory contains an existing store if there is a config
             * file
             */
            final File configFile = getConfigFile(rootDir);
            final boolean existingStore = configFile.exists();
            if (staticLogger.isLoggable(Level.FINEST)) {
                staticLogger.finest("KVLocal start: " +
                                    (existingStore ? "Found" : "No") +
                                    " existing store in: " + rootDir);
            }

            final List<String> command = new ArrayList<>();
            Collections.addAll(
                command,
                /* Java executable */
                System.getProperty("java.home") + File.separator + "bin" +
                File.separator + "java",
                /* Class path */
                "-cp", System.getProperty("java.class.path"),
                /* Heap size */
                "-Xmx" + config.getMemoryMB() + "m");
            /* Don't use RMI with Unix domain sockets */
            if (config.isUnixDomain()) {
                command.add("-D" + KVStoreConfig.USE_RMI + "=false");
                command.add("-D" + RegistryUtils.SERVER_USE_RMI + "=false");
                /*
                 * Use the extra args system property to pass this properties
                 * down to spawned processes
                 */
                command.add(
                    "-D" + ProcessServiceManager.JVM_EXTRA_ARGS_FLAG + "=" +
                    "-D" + KVStoreConfig.USE_RMI + "=false" +
                    ";-D" + RegistryUtils.SERVER_USE_RMI + "=false");
            }
            if (KVLITE_FLAGS != null) {
                Collections.addAll(command, KVLITE_FLAGS.split(" "));
            }
            /* TODO: GC args, logging, other RN command line flags */
            Collections.addAll(
                command,
                /* Class */
                KVLite.class.getName(),
                /* KVLite parameters */
                KVLiteParser.ROOT_FLAG, rootDir,
                KVLiteParser.STORE_FLAG, config.getStoreName(),
                KVLiteParser.PORT_FLAG, String.valueOf(config.getPort()),
                KVLiteParser.HOST_FLAG, config.getHostName(),
                KVLiteParser.PARTITION_FLAG, "1",
                KVLiteParser.NOTHREADS_FLAG,
                KVLiteParser.SECURE_CONFIG_FLAG,
                (config.isSecure() ?
                 KVLite.SECURITY_ENABLE :
                 KVLite.SECURITY_DISABLE),
                KVLiteParser.MEMORY_MB_FLAG,
                String.valueOf(config.getMemoryMB()),
                KVLiteParser.PRINT_STARTUP_OK_FLAG);
            if (snapshotName != null) {
                command.add(KVLiteParser.RESTORE_FROM_SNAPSHOT);
                command.add(snapshotName);
            }
            if (config.getStorageGB() > 0) {
                command.add(KVLiteParser.MOUNT_SIZE_FLAG);
                command.add(String.valueOf(config.getStorageGB()));
            }
            /*
             * TODO: Add additional command line flags, including GC parameters
             * and the value of the javaMisc and related parameters.
             */
            staticLogger.finest(() -> "KVLocal start: KVLite command line: " +
                                command);
            kvlite = new KVLiteMonitor(command);
            kvlite.startProcess();
            final KVStore store =
                waitForService(config, kvlite, existingStore);
            complete = true;
            staticLogger.fine("KVLocal start: Succeeded");
            return new KVLocal(config, kvlite, store, lock);
        } catch (IOException e) {
            throw new KVLocalException(
                "Problem starting store: " + e.getMessage(), e);
        } finally {
            if (!complete) {
                staticLogger.fine("KVLocal start: Failed");
                if (kvlite != null) {
                    killKVLite(kvlite);
                }
                if (lock != null) {
                    try {
                        lock.close();
                        staticLogger.finest("KVLocal start: Released lock");
                    } catch (IOException e) {
                        staticLogger.fine("KVLocal start:" +
                                          " Releasing lock failed: " + e);
                    }
                }

                /*
                 * Wait to clear instanceStarted until the kvlite for the old
                 * instance has been stopped.
                 */
                instanceStarted.set(false);
            }
        }
    }

    /**
     * Make sure that the directory that will be used to store Unix domain
     * socket files exists.
     */
    private static void prepareSocketDirectory(String unixDomainHost)
        throws IOException
    {
        final UnixDomainNetworkAddress address = (UnixDomainNetworkAddress)
            NetworkAddress.createNetworkAddress(unixDomainHost, 0);
        final Path path = Paths.get(address.getPathname());
        final Path socketDir = path.getParent();
        /*
         * A null directory means the root directory, but this directory should
         * be a subdirectory of the kvroot directory, so it can't be the root
         */
        if (socketDir == null) {
            throw new IllegalStateException(
                "Socket directory can't be the file system root directory");
        }
        /* Make sure the directory exists */
        if (!Files.exists(socketDir)) {
            try {
                Files.createDirectory(socketDir);

                /*
                 * Since the directory was newly created, no need to delete
                 * existing files
                 */
                staticLogger.finest(
                    () -> "KVLocal start:" +
                    " Created Unix domain socket directory: " + socketDir);
                return;
            } catch (FileAlreadyExistsException e) {

                /*
                 * We have the exclusive lock at this point, so we don't expect
                 * another process to be creating the sockets directory
                 * concurrently. But there isn't really anything wrong with
                 * something else creating the directory beyond it being
                 * unexpected, so ignore this exception.
                 */
            } catch (IOException e) {
                throw new IOException(
                    "Problem creating Unix domain socket directory: " +
                    socketDir,
                    e);
            }
        }

        /*
         * Delete any files in existing directory, ignoring any unexpected
         * subdirectories
         */
        Files.walkFileTree(
            socketDir, EnumSet.noneOf(FileVisitOption.class), 1,
            new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file,
                                                 BasicFileAttributes attrs)
                    throws IOException
                {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        staticLogger.finest(
            () -> "KVLocal start: Cleaned Unix domain socket directory: " +
            socketDir);
    }

    /**
     * Returns an instance of KVLocalConfig for the store in the specified root
     * directory.
     *
     * @throws IllegalStateException if either the configuration file or store
     * directory under the root directory are not found
     */
    private static KVLocalConfig getKVLocalConfigFromRootDir(String rootDir) {
        final File configFile = getConfigFile(rootDir);
        if (!configFile.exists()){
            throw new IllegalStateException(
                "Configuration file of the existing store was not found: " +
                configFile);
        }
        final BootstrapParams bp = ConfigUtils.getBootstrapParams(configFile);

        final String storeName = bp.getStoreName();
        if (storeName == null) {
            throw new IllegalStateException(
                "Store is not fully initialized in root directory: " +
                rootDir);
        }

        final File storeDirectory = new File(rootDir, storeName);
        if (!storeDirectory.exists()) {
            throw new IllegalStateException("Store directory was not found: " +
                                            storeDirectory);
        }

        final String hostName = bp.getHostname();
        final KVLocalConfig.Builder configBuilder;
        if (NetworkAddress.isUnixDomainHostname(hostName)) {
            configBuilder = new KVLocalConfig.UnixDomainBuilder(rootDir);
        } else {
            final int port = bp.getRegistryPort();
            final boolean isSecure = bp.getSecurityDir() != null;
            configBuilder = new KVLocalConfig.InetBuilder(rootDir)
                .setHostName(hostName)
                .setPort(port)
                .isSecure(isSecure);
        }

        final int memoryMB = bp.getMemoryMB();
        final int storageGB = getStorageSizeFromBP(bp);
        return configBuilder.setStoreName(storeName)
            .setMemoryMB(memoryMB)
            .setStorageGB(storageGB)
            .build();
    }

    private static int getStorageSizeFromBP(BootstrapParams bp) {
        String directoryPath = null;
        List<String> paths = bp.getStorageDirPaths();
        if (paths != null && paths.size() > 0) {
            directoryPath = paths.get(0);
        }

        if (directoryPath == null || bp.getStorageDirMap() == null  ||
            bp.getStorageDirMap().get(directoryPath) == null) {
            return KVLocalConfig.DEFAULT_STORAGE_SIZE_GB;
        }
        Parameter p = bp.getStorageDirMap().get(directoryPath);

        long size = SizeParameter.getSize(p);
        return (int)(size/(1024 * 1024 * 1024));
    }

    /**
     * Wait for service to start, and return the KVStore instance used to
     * determine that it was ready.
     *
     * @throws KVLocalException if an error occurs waiting for service
     * @throws IllegalStateException if the root directory does not contain a
     * valid store configuration
     */
    private static KVStore waitForService(KVLocalConfig config,
                                          KVLiteMonitor kvlite,
                                          boolean existingStore) {
        final int checkPeriodMs = 250;

        /*
         * Wait for the service to start successfully. Otherwise we might think
         * we started the store but are actually talking to a store started
         * separately.
         */
        final AtomicBoolean startupOK = new AtomicBoolean();
        final AtomicReference<String> saveStartupFailure =
            new AtomicReference<>();

        final long waitUntil =
            System.currentTimeMillis() + KVLITE_START_TIMEOUT;

        /*
         * TODO: Is there a better way to wait for successful startup so that
         * we wait if startup is making progress but stop quickly if it fails?
         * [KVSTORE-1473]
         */
        PollCondition.await(checkPeriodMs, KVLITE_START_TIMEOUT,
                            () -> {
                                final String startup = kvlite.isStartupOK();
                                if (startup == null) {
                                    startupOK.set(true);
                                    return true;
                                }
                                saveStartupFailure.set(startup);
                                /* Give up if kvlite has failed */
                                if (kvlite.restartsDisabled) {
                                    return true;
                                }
                                return false;
                            });

        final String startupFailure = saveStartupFailure.get() != null ?
            ":\n" + saveStartupFailure.get().trim() :
            "";

        /*
         * Wait for the security configuration to be created, which we need to
         * create a KVStore handle
         */
        if (config.isSecure()) {
            final String securityPath =
                getSecurityPath(config.getRootDirectory());
            final long timeout = waitUntil - System.currentTimeMillis();
            if ((timeout <= 0) ||
                !waitForFile(securityPath, kvlite, checkPeriodMs, timeout)) {
                if (existingStore) {
                    /* Configuration is invalid */
                    throw new IllegalStateException(
                        "Security configuration was not found: " +
                        securityPath + startupFailure);
                }
                /* Creating a new store failed */
                throw new KVLocalException(
                    "Service was not started" + startupFailure);
            }
        }

        if (!startupOK.get()) {
            throw new KVLocalException("Service was not started" +
                                       startupFailure);
        }

        /* Wait for the store to be ready */
        final AtomicReference<KVStore> store = new AtomicReference<>();
        boolean ready = false;
        try {
            final AtomicBoolean storeReady = new AtomicBoolean();
            long timeout = waitUntil - System.currentTimeMillis();
            if (timeout > 0) {
                PollCondition.await(
                    checkPeriodMs, timeout,
                    () -> {
                        /* Try getting the store, retry on FaultException */
                        try {
                            store.set(getStore(config));
                        } catch (FaultException e) {
                            saveStartupFailure.set(e.toString());
                            return false;
                        }
                        if (isStoreReady(store.get())) {
                            storeReady.set(true);
                            return true;
                        }
                        /* Give up if kvlite has failed */
                        if (kvlite.restartsDisabled) {
                            return true;
                        }
                        return false;
                    });
            }
            if (!storeReady.get()) {
                final String timeoutFailure =
                    (saveStartupFailure.get() != null) ?
                    ":\n" + saveStartupFailure.get() :
                    "";
                throw new KVLocalException(
                    "Timed out waiting for store to start" + timeoutFailure);
            }

            /*
             * Wait for the admin to be ready. Get the admin after getting the
             * KVStore because getting the KVStore is needed to set up the
             * registry client socket factory correctly. The admin access path
             * doesn't seem to set the RMI policy in the non-secure case,
             * meaning that accessing the admin of a non-secure KVLocal after
             * using a secure one would fail. Getting the KVStore first sets
             * things right; we might want to modify the admin access path at
             * some point to be more robust.
             */
            final AtomicBoolean adminReady = new AtomicBoolean();
            timeout = waitUntil - System.currentTimeMillis();
            if (timeout > 0) {
                PollCondition.await(
                    checkPeriodMs, timeout,
                    () -> {
                        try {
                            getAdmin(config);
                            adminReady.set(true);
                            return true;
                        } catch (KVLocalException e) {
                            saveStartupFailure.set(e.getMessage());
                        }
                        /* Give up if kvlite has failed */
                        if (kvlite.restartsDisabled) {
                            return true;
                        }
                        return false;
                    });
            }

            if (!adminReady.get()) {
                final String timeoutFailure =
                    (saveStartupFailure.get() != null) ?
                    ":\n" + saveStartupFailure.get() :
                    "";
                throw new KVLocalException(
                    "Timed out waiting for admin to start" + timeoutFailure);
            }

            ready = true;
            return store.get();
        } finally {
            /* Close the store if not returning it */
            if (!ready && (store.get() != null)) {
                store.get().close();
            }
        }
    }

    /**
     * Returns whether the store is ready. The store is ready if getting the
     * partition ID does not throw FaultException.
     */
    static boolean isStoreReady(KVStore store) {
        try {
            ((KVStoreImpl) store).getDispatcher().getPartitionId(new byte[0]);
            return true;
        } catch (FaultException e) {
            return false;
        }
    }

    /** Wait for a file to exist, returning whether it was found. */
    private static boolean waitForFile(String path,
                                       KVLiteMonitor kvlite,
                                       int checkPeriodMs,
                                       long timeoutMs) {
        final File file = new File(path);
        final AtomicBoolean found = new AtomicBoolean();
        PollCondition.await(
            checkPeriodMs, timeoutMs,
            () -> {
                if (file.exists()) {
                    found.set(true);
                    return true;
                }
                /* Give up if restarts are disabled */
                if (kvlite.restartsDisabled) {
                    return true;
                }
                return false;
            });
        return found.get();
    }

    /**
     * Validates bootstrap parameters.
     *
     * If there is an existing store in the root directory, the KVLocal opens
     * the existing store. The existing store's parameters might not match the
     * parameters declared by KVLocalConfig. IllegalArgumentException will be
     * thrown if any mismatched parameter is found.
     *
     * @throws IllegalArgumentException if existing store's parameters do not
     * match the parameters declared by KVLocalConfig
     */
    private static void validateParameters(KVLocalConfig config) {
        final File configFile = getConfigFile(config.getRootDirectory());

        /* No existing store */
        if (!configFile.exists()) {
            staticLogger.finest(
                () -> "KVLocal start: Config file not found: " + configFile);
            return;
        }

        final BootstrapParams bp = ConfigUtils.getBootstrapParams(configFile);

        if (!bp.getRootdir().equals(config.getRootDirectory())) {
            throw new IllegalArgumentException(
                "KVLocal's root directory '" + config.getRootDirectory() +
                "' does not match the existing store's root directory '" +
                bp.getRootdir() + "'");
        }
        if (!bp.getHostname().equals(config.getHostName())) {
            throw new IllegalArgumentException
                ("KVLocal's hostname " + config.getHostName() +
                " does not match the existing store's hostname " +
                bp.getHostname());
        }
        final String storeName = bp.getStoreName();
        if (storeName == null) {
            throw new IllegalArgumentException(
                "Existing store was not created successfully:" +
                " no store name was found");
        }
        if (!storeName.equals(config.getStoreName())) {
            throw new IllegalArgumentException
                ("KVLocal's storename '" + config.getStoreName() +
                "' does not match the existing store's storename '" +
                storeName + "'");
        }
        if (bp.getRegistryPort() != config.getPort()) {
            throw new IllegalArgumentException
                ("KVLocal's port " + config.getPort() +
                " does not match the existing store's port " +
                bp.getRegistryPort());
        }
        final String securityDir = bp.getSecurityDir();
        if (config.isSecure() && (securityDir == null)) {
            throw new IllegalArgumentException
                ("KVLocal's security enabled setting does not match the" +
                 " existing store, which has security disabled");
        }
        if (!config.isSecure() && (securityDir != null)) {
            throw new IllegalArgumentException
                ("KVLocal's security disabled setting does not match the" +
                 " existing store, which has security enabled");
        }
        /*
         * TODO: Consider allowing the caller to change this setting for an
         * existing store. To do that, we would probably need to store the
         * requested value separately from the bootstrap parameters.
         */
        if (bp.getMemoryMB() != config.getMemoryMB()) {
            throw new IllegalArgumentException(
                "KVLocal's memoryMB (" + config.getMemoryMB() +
                ") does not match the existing store's memoryMB (" +
                bp.getMemoryMB() + ")");
        }
        final int storageGB = getStorageSizeFromBP(bp);
        if (storageGB != config.getStorageGB()) {
            throw new IllegalArgumentException(
                "KVLocal's storageGB (" + config.getStorageGB() +
                ") does not match the existing store's storageGB (" +
                storageGB + ")");
        }
        staticLogger.finest(() -> "KVLocal start: Verified configuration: " +
                            config);
    }

    /**
     * Get security path.
     */
    static String getSecurityPath(String rootDir) {
        return new File(rootDir + File.separator +
            FileNames.SECURITY_CONFIG_DIR + File.separator +
            FileNames.USER_SECURITY_FILE).getPath();
    }

    /**
     * Get the Admin login manager.
     */
    static LoginManager getAdminLoginMgr(KVLocalConfig config) {
        final KVStoreLogin storeLogin = new KVStoreLogin(null, null);
        if (config.isSecure()) {
            storeLogin.updateLoginInfo("admin",
                getSecurityPath(config.getRootDirectory()));
        }
        final LoginCredentials creds = storeLogin.getLoginCredentials();

        /*
         * Skip renewing login tokens automatically since we will only use this
         * login manager temporarily and don't want to worry about stopping the
         * auto-renewal threads
         */
        return KVStoreLogin.getAdminLoginMgr(
            new String[] { config.getHostName() + ":" + config.getPort() },
            creds, false /* autoRenew */, staticLogger);
    }

    /**
     * Get the Admin command service.
     *
     * @throws KVLocalException if an error occurs getting Admin
     * command service
     */
    private static CommandServiceAPI getAdmin(KVLocalConfig config) {
        final LoginManager loginMgr = getAdminLoginMgr(config);
        try {
            return RegistryUtils.getAdmin(
                null /* storeName */, config.getHostName(), config.getPort(),
                loginMgr,
                (config.isUnixDomain() ?
                 Protocols.ASYNC_ONLY :
                 Protocols.getDefault()),
                staticLogger);
        } catch (RemoteException|NotBoundException e) {
            throw new KVLocalException("Exception in getting admin command" +
                " service, maybe the store is not" +
                " running: "+  e.getMessage(), e);
        }
    }

    /**
     * A process monitor for KVLite that tracks the number of restarts and
     * whether restarts have been disabled.
     */
    static class KVLiteMonitor extends ProcessMonitor {
        final AtomicLong restarts = new AtomicLong();
        volatile boolean restartsDisabled = false;
        KVLiteMonitor(List<String> command) {
            super(command, null /* env */,
                  Integer.MAX_VALUE /* restartCount */,
                  "kvlocal" /* serviceName */, staticLogger);
            /* Make sure the kvlite process shuts down on exit */
            Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    { setDaemon(true); }
                    @Override
                    public void run() {
                        destroyProcess();
                    }
                });

            /*
             * Set the startup buffer to a non-null value before starting. The
             * IOThread will set it to another non-null value during startup.
             * If the field gets set to null, then that means the startup
             * succeeded.
             */
            startupBuffer = new StringBuilder();
        }
        /**
         * Returns null if the service startup succeeded, an empty string if
         * the startup attempt is still underway, or else a string that
         * (hopefully) provides a hint about what went wrong.
         */
        String isStartupOK() {
            final StringBuilder sb = startupBuffer;
            return (sb == null) ? null : sb.toString();
        }
        @Override
        protected void onRestart() {
            /*
             * TODO: Maybe provide a way to report this information, perhaps
             * via a callback
             */
            restarts.incrementAndGet();
        }
        @Override
        protected void onExit() {
            restartsDisabled = true;
        }
        /*
         * For both of these threads, specify better thread names that don't
         * refer to "SNA", and make the thread daemon threads so they don't
         * prevent the JVM from exiting.
         */
        @Override
        protected IOThread createIOThread(String name) {
            final IOThread thread = super.createIOThread("kvlocal.io");
            thread.setDaemon(true);
            return thread;
        }
        @Override
        protected MonitorThread createMonitorThread(String name) {
            final MonitorThread thread =
                super.createMonitorThread("kvlocal.monitor");
            thread.setDaemon(true);
            return thread;
        }
    }

    /**
     * Obtains a file lock on LOCK_FILE in the root directory, creating the
     * root directory and the file as needed. Returns the open FileChannel
     * associated with the lock file if successful, which should be closed to
     * release the lock. Throws IOException if the lock cannot be obtained.
     */
    static FileChannel getLock(KVLocalConfig config) throws IOException {
        final Path root = Paths.get(config.getRootDirectory());
        if (!Files.exists(root)) {
            try {
                Files.createDirectory(root);
            } catch (FileAlreadyExistsException e) {
            } catch (IOException e) {
                throw new IOException(
                    "Problem creating root directory: " + root, e);
            }
        }
        final Path lockPath = root.resolve(LOCK_FILE);
        try {
            final FileChannel channel =
                FileChannel.open(lockPath,
                                 StandardOpenOption.CREATE,
                                 StandardOpenOption.WRITE);
            if (channel.tryLock() == null) {
                throw new IOException(
                    "Unable to obtain lock for root directory " + root +
                    " because it is held by another process");
            }
            staticLogger.finest(() -> "KVLocal start: Obtained lock: " +
                                lockPath);
            return channel;
        } catch (IOException e) {
            throw new IOException(
                "Problem obtaining lock for root directory " + root + ": " + e,
                e);
        }
    }
}
