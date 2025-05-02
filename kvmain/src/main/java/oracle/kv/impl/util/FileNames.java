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

package oracle.kv.impl.util;

import java.io.File;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * Responsible for the computation of all file pathnames related to the
 * KVStore.
 * <p>
 * The general structure of a kvstore directory is illustrated by the nested
 * list below. When explicit mount points are being used for RepNodes the
 * rgX-rnY directories may not be present. In that case they will exist in
 * other locations in the file system. Their parameters in snX/config.xml will
 * have a reference to that location.
 * <p>
 * kvStoreName
 * <ul>
 * <li>
 * security.policy</li>
 * <li>
 * config.xml</li>
 * <li>
 * adminboot.0.log</li>
 * <li>
 * snaboot.0.log</li>
 * <li>security<br>
 * Has several different different kinds of files relating to security.
 * <ul>
 * <li>security.xml<br>
 * The configuration file for security of the SN.  It includes specific
 * reference to the files listed below, which are named by convention here.
 * <li>store.keys<br>
 * The SSL keystore file for the SN
 * <li>store.trust<br>
 * The SSL truststore file for the SN
 * <li>store.pwd<br>
 * Typically only present for a CE installation, this is the open-source
 * password store file that contains the keystore password.
 * <li>store.wallet<br>
 * Only present for an EE installation.  This is the Oracle Wallet password
 * store directory that contains the keystore password.
 * <ul>
 * <li>updates<br>
 * Contains updates to the store.keys and store.trust files waiting to be
 * installed.
 * </ul>
 * </ul>
 * <li>
 * log<br>
 * Has five different kinds of files: ".log" files contain basic lifecycle
 * information and error messages for a service, ".log" files for gc activity
 * associated with a service, . ".perf" files contain an iostat type listing of
 * latency and throughput information. ".csv" are optional files that contain
 * performance information suitable for examining in a spreadsheet. ".stat" are
 * option files that contain JE environment stats.
 * <ul>
 * <li>admin-1_0.log</li>
 * <li>admin-1_1.log</li>
 * <li>storage_node-1_0.log</li>
 * <li>storage_node-1_1.log</li>
 * <li>rg1-rn1_0.log</li>
 * <li>rg1-rn1_1.log</li>
 * <li>rg1-rn1_summary.csv</li>
 * <li>rg1-rn1_0.perf</li>
 * <li>rg1-rn1_0.stat</li>
 * <li>rg1-rn1.gc.0</li>
 * <li>rg1-rn1.gc.1</li>
 * </ul>
 * </li>
 *
 * <li>
 * sn1
 * <ul>
 * <li>config.xml</li>
 * <li>
 * admin1
 * <ul>
 * <li>env</li>
 * <ul>
 * <li>00000000.jdb</li>
 * <li>00000001.jdb</li>
 * <li>...</li>
 * </ul>
 * <li>snapshots</li>
 * <li>recovery</li>
 * <li>webapp</li>
 * </ul>
 * </li>
 * <li>
 * rg1-rn1
 * <ul>
 * <li>env</li>
 * <ul>
 * <li>00000000.jdb</li>
 * <li>00000001.jdb</li>
 * <li>...</li>
 * </ul>
 * <li>snapshots</li>
 * <li>recovery</li>
 * </ul>
 * </li>
 * <li>
 * rg2-rn1
 * <ul>
 * <li>env</li>
 * <ul>
 * <li>00000000.jdb</li>
 * <li>00000003.jdb</li>
 * <li>...</li>
 * </ul>
 * <li>snapshots</li>
 * <li>recovery</li>
 * </ul>
 * </li>
 * <li>
 * collector
 * <ul>
 * <li>ping_0.json</li>
 * <li>ping_1.json</li>
 * <li>plan_0.json</li>
 * <li>rnEnv.json</li>
 * <li>rnException_0.json</li>
 * <li>rnOp_0.json</li>
 * <li>rnTable_0.json</li>
 * <li>rnJVM_0.json</li>
 * <li>...</li>
 * </ul> </li> </ul> </li>
 * <li>
 * sn2
 * <ul>
 * <li>config.xml</li>
 * <li>
 * rg1-rn2
 * <ul>
 * <li>env</li>
 * <ul>
 * <li>00000005.jdb</li>
 * <li>00000006.jdb</li>
 * <li>...</li>
 * </ul>
 * <li>snapshots</li>
 * <li>recovery</li>
 * </ul>
 * </li>
 * <li>
 * rg2-rn2
 * <ul>
 * <li>env</li>
 * <ul>
 * <li>00000000.jdb</li>
 * <li>00000009.jdb</li>
 * <li>...</li>
 * </ul>
 * <li>snapshots</li>
 * <li>recovery</li>
 * </ul>
 * </li>
 * <li>
 * collector
 * <ul>
 * <li>ping_0.json</li>
 * <li>ping_1.json</li>
 * <li>plan_0.json</li>
 * <li>rnEnv.json</li>
 * <li>rnException_0.json</li>
 * <li>rnOp_0.json</li>
 * <li>rnTable_0.json</li>
 * <li>rnJVM_0.json</li>
 * <li>...</li>
 * </ul> </li> </ul> </li> </ul>
 *
 */
public class FileNames {

    /* The SNA config file used to store SNA persistent state. */
    public static final String SNA_CONFIG_FILE = "config.xml";
    public static final String SECURITY_CONFIG_DIR = "security";
    public static final String SECURITY_CONFIG_FILE = "security.xml";

    /*
     * TODO: The Java Security Manager is being deprecated for removal in Java
     * 17 as part of JEP 411. We removed code that specified a Security Manager
     * and provided a security policy file in release 21.3. Remove creation of
     * the file itself once previous releases that used it are no longer
     * supported.
     */
    public static final String JAVA_SECURITY_POLICY_FILE = "security.policy";
    public static final String BOOTSTRAP_SNA_LOG = "snaboot";
    public static final String BOOTSTRAP_ADMIN_LOG = "adminboot";
    public static final String ENV_DIR = "env";
    public static final String SNAPSHOT_DIR = "snapshots";
    public static final String RECOVERY_DIR = "recovery";
    public static final String WEBAPP_DIR = "webapp";
    private static final String LOGGING_DIR = "log";
    public static final String SECURITY_PROPERTY_DIR = "security-property";
    public static final String SECURITY_PROPERTY_FILE_SUFFIX =
        "-security.properties";

    /* collector file names */
    public static final String COLLECTOR_DIR = "collector";

    public static final String COLLECTOR_PING_FILE_NAME = "ping";
    public static final String COLLECTOR_PLAN_FILE_NAME = "plan";
    public static final String COLLECTOR_RNOP_FILE_NAME = "rnOp";
    public static final String COLLECTOR_RNTABLE_FILE_NAME = "rnTable";
    public static final String COLLECTOR_RNJVM_FILE_NAME = "rnJVM";
    public static final String COLLECTOR_RNENDPOINTGROUP_FILE_NAME =
        "rnEndpointGroup";
    public static final String COLLECTOR_RNEXCEPTION_FILE_NAME = "rnException";
    public static final String COLLECTOR_RNENV_FILE_NAME = "rnEnv";
    public static final String COLLECTOR_RNEVENT_FILE_NAME = "rnEvent";
    public static final String COLLECTOR_LOGGINGSTATS_FILE_NAME =
        "loggingStats";

    public static final String COLLECTOR_FILE_SUFFIX = "json";

    /* security file names */
    public static final String WALLET_DIR = "store.wallet";
    public static final String PASSWD_FILE = "store.passwd";
    public static final String KEYSTORE_FILE = "store.keys";
    public static final String TRUSTSTORE_FILE = "store.trust";
    public static final String CLIENT_SECURITY_FILE = "client.security";
    public static final String CLIENT_TRUSTSTORE_FILE = "client.trust";
    public static final String USER_SECURITY_FILE = "user.security";
    public static final String USER_WALLET_DIR = "user.wallet";
    public static final String USER_PASSWD_FILE = "user.passwd";
    public static final String KERBEROS_KEYTAB_FILE = "store.keytab";
    public static final String SECURITY_UPDATES_DIR = "updates";
    public static final String RETRIEVE_SECURITY_UPDATES_FILE = "retrieve";

    /* Suffixes for performance stat .csv files. */
    public static final String DETAIL_CSV = "_detail.csv";
    public static final String SUMMARY_CSV = "_summary.csv";

    /* Regular log files */
    public static final String LOG_FILE_SUFFIX = "log";
    /* Performance data files. */
    public static final String PERF_FILE_SUFFIX = "perf";
    /* Environment stats, slow-thread dumps. */
    public static final String STAT_FILE_SUFFIX = "stat";
    /** Suffix for gzip compressed files */
    public static final String GZIP_FILE_SUFFIX = "gz";
    /** Files used to detect service crashes */
    public static final String CRASH_DETECT_FILE_SUFFIX = "up";

    private final File kvDir;
    /**
     * The constructor
     *
     * @param topology the topology used as the basis for file pathnames
     * @param rootDir the root directory for all KVStores on this machine.
     * There may be multiple kvstores in this directory.
     */
    public FileNames(Topology topology, File rootDir) {
        super();
        this.kvDir = new File(rootDir, topology.getKVStoreName());
    }

    /**
     * Returns the the root directory for all KVStore related files stored
     * on this machine.
     */
    public File getKvDir() {
        return kvDir;
    }

    public static File getKvDir(String rootDirPath, String kvStoreName) {
        return new File(rootDirPath, kvStoreName);
    }

    /**
     * Returns the security policy file associated with the KVStore
     */
    public static File getSecurityPolicyFile(File kvDir) {
        return new File(kvDir, JAVA_SECURITY_POLICY_FILE);
    }

    /**
     * Returns the directory for the security property files associated with
     * the KVStore.
     */
    public static File getSecurityPropertyDir(File snRoot) {
        return new File(snRoot, SECURITY_PROPERTY_DIR);
    }

    /**
     * Create the directory for the security property files associated with the
     * KVstore.
     */
    public static void makeSecurityPropertyDir(File snRoot) {
        final File securityPropertyDir = getSecurityPropertyDir(snRoot);
        makeDir(securityPropertyDir);
    }

    /**
     * Returns the logs directory associated with the KVstore
     */
    public static File getLoggingDir(File rootDir, String kvStoreName) {
        return new File(new File(rootDir, kvStoreName), LOGGING_DIR);
    }

    /**
     * Create the logging directory for the kvstore.
     */
    public static void makeLoggingDir(File rootDir, String kvStoreName) {

        File loggingDir = getLoggingDir(rootDir, kvStoreName);
        makeDir(loggingDir);
    }

    /**
     * Returns the logs directory associated with the Rep Node.
     */
    public static File getRNLoggingDir(File rootDir) {
        /*
         * TODO : Check on code points of usage..
         */
        return rootDir;
    }

    /**
     * Returns the gc logs directory associated with the Rep Node.
     */
    public static File getRNGCLoggingDir(File rootDir, String resourceName) {
        return new File(rootDir, resourceName);
    }

    /**
     * Create the logging directory for specific Rep Node.
     */
    public static void makeRNLoggingDir(File rootDir) {
        makeDir(rootDir);
    }

    /**
     * Returns the file used for Storage Node configuration information. The
     * file identifies the the services running on this storage node as well as
     * their configuration parameters.
     * <p>
     * Under normal circumstances, the SNA is the sole updater of this config
     * file, however the admin database is the database of record.
     *
     * @param storageNodeId identifies the SN associated with the config file
     *
     * @return the SNA config file
     */
    public static File getSNAConfigFile(String rootDirPath,
                                        String kvstoreName,
                                        StorageNodeId storageNodeId) {

        return new File
            (getStorageNodeDir(rootDirPath, kvstoreName, storageNodeId),
             SNA_CONFIG_FILE);
    }

    /**
     * Returns the directory used to hold the files associated with the
     * Resource.
     *
     * @param rootDirPath the kvstore directory name
     * @param kvstoreName the name of the store
     * @param serviceDir the service directory if specified by parameters
     * @param storageNodeId the SN on which the RN resides
     * @param resourceId identifies the resource (admin or RN)
     *
     * @return the service directory
     */
    public static File getServiceDir(String rootDirPath,
                                     String kvstoreName,
                                     File serviceDir,
                                     StorageNodeId storageNodeId,
                                     ResourceId resourceId) {
        if (serviceDir != null) {
            return new File(serviceDir, resourceId.getFullName());
        }
        File kvDir1 = new File(rootDirPath, kvstoreName);
        return new File(getStorageNodeDir(kvDir1, storageNodeId),
                        resourceId.getFullName());
    }

    /**
     * Returns the admin directory used to hold the files associated with the
     * Resource.
     *
     * @param rootDirPath the kvstore directory name
     * @param resourceId identifies the resource (admin)
     *
     * @return the service directory
     */
    public static File getAdminServiceDir(String rootDirPath,
                                          ResourceId resourceId) {
        return new File(rootDirPath, resourceId.getFullName());
    }

    /**
     * Returns the directory used to hold the environment associated with the
     * Resource.
     *
     * @param rootDirPath the kvstore directory name
     * @param kvstoreName the name of the store
     * @param storageNodeId the SN on which the RN resides
     * @param resourceId identifies the resource (admin or RN)
     *
     * @return the environment directory
     */
    public static File getEnvDir(String rootDirPath,
                                 String kvstoreName,
                                 File serviceDir,
                                 StorageNodeId storageNodeId,
                                 ResourceId resourceId) {
        return new File(getServiceDir(rootDirPath, kvstoreName, serviceDir,
                                      storageNodeId, resourceId), ENV_DIR);
    }

    /**
     * Returns the admin directory used to hold the files associated
     * with the Resource.
     *
     * @param rootDirPath the kvstore directory name
     * @param resourceId identifies the resource (admin)
     *
     * @return the service directory
     */
    public static File getAdminEnvDir(String rootDirPath,
                                      ResourceId resourceId) {
        return new File(getAdminServiceDir(rootDirPath, resourceId), ENV_DIR);
    }

    /**
     * Returns the directory used to hold the snapshots associated with the
     * Resource.
     */
    public static File getSnapshotDir(String rootDirPath,
                                      String kvstoreName,
                                      File serviceDir,
                                      StorageNodeId storageNodeId,
                                      ResourceId resourceId) {
        return new File(getServiceDir(rootDirPath, kvstoreName, serviceDir,
                                      storageNodeId, resourceId),
                        SNAPSHOT_DIR);
    }

    /**
     * Returns the admin directory used to hold the snapshots associated
     * with the Resource.
     */
    public static File getAdminSnapshotDir(String rootDirPath,
                                           ResourceId resourceId) {
        return new File(getAdminServiceDir(rootDirPath, resourceId),
                        SNAPSHOT_DIR);
    }

    /**
     * Return specific snapshot directory under snapshot base directory.
     * @param parent parent directory of snapshot source.
     * @param name full name of snapshot
     */
    public static File getSnapshotNamedDir(File parent, String name) {
        final File snapshotBase = getSnapshotBaseDir(parent);
        return new File(snapshotBase, name);
    }

    /**
     * Return base snapshot directory.
     * @param parent directory of snapshot source.
     */
    public static File getSnapshotBaseDir(File parent) {
        return new File(parent, SNAPSHOT_DIR);
    }

    /**
     * Return specific snapshot directory under snapshot base directory.
     * @param parentPath path string of parent directory of snapshot source.
     * @param name full name of snapshot
     */
    public static File getSnapshotNamedDir(String parentPath, String name) {
        final File snapshotBase = getSnapshotBaseDir(new File(parentPath));
        return new File(snapshotBase, name);
    }

    /**
     * Returns the directory used to hold recovery state associated with the
     * Resource.
     */
    public static File getRecoveryDir(String rootDirPath,
                                      String kvstoreName,
                                      File serviceDir,
                                      StorageNodeId storageNodeId,
                                      ResourceId resourceId) {
        return new File(getServiceDir(rootDirPath, kvstoreName, serviceDir,
                                      storageNodeId, resourceId),
                        RECOVERY_DIR);
    }

    /**
     * Returns the directory used to hold Admin console web application data
     * associated with the Resource.
     */
    public static File getAdminWebAppDir(String rootDirPath,
                                         String kvstoreName,
                                         File serviceDir,
                                         StorageNodeId storageNodeId,
                                         ResourceId resourceId) {
        return new File(getServiceDir(rootDirPath, kvstoreName, serviceDir,
                                      storageNodeId, resourceId),
                        WEBAPP_DIR);
    }

    /**
     * A common routine to make a directory, including missing parents.
     *
     * @param dir the File representing the directory to create
     * @return true if the directory is created, false if not (already exists)
     *
     * If the directory does not exist and cannot be created an exception is
     * thrown.
     */
    public static boolean makeDir(File dir) {
        boolean created = false;
        if (!dir.exists()) {
            created = dir.mkdirs();
            if (!created) {
                throw new IllegalStateException
                    ("Directory: " + dir + " creation failed.");
            }
        }
        return created;
    }

    /**
     * A common routine to remove an existing directory.
     *
     * @param dir the File representing the directory to remove
     * @return true if the directory is removed, false if not (not exists)
     *
     * If the directory exists and cannot be removed an exception is thrown.
     */
    public static boolean removeDir(File dir) {
        boolean removed = false;
        if (dir != null && dir.exists() && dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                if (file.isFile()) {
                    file.delete();
                }
                else if (file.isDirectory()) {
                    removeDir(file);
                }
            }
            removed = dir.delete();
            if (!removed) {
                throw new IllegalStateException
                    ("Directory: " + dir + " deletion failed.");
            }
        }
        return removed;
    }

    /**
     * Returns the directory associated with the storage node.
     *
     * @param storageNodeId identifies the storage node
     * @return the directory associated with the storage node
     */
    public File getStorageNodeDir(StorageNodeId storageNodeId) {
        return new File(kvDir, storageNodeId.getFullName());
    }

    public static File getStorageNodeDir(File kvDir1,
                                         StorageNodeId storageNodeId) {
        return new File(kvDir1, storageNodeId.getFullName());
    }

    public static File getStorageNodeDir(String rootDirPath,
                                         String kvstoreName,
                                         StorageNodeId storageNodeId) {
        File kvDir = new File(rootDirPath, kvstoreName);
        return getStorageNodeDir(kvDir, storageNodeId);
    }

    public static File getCollectorDir(String rootDirPath,
                                       String kvstoreName,
                                       StorageNodeId storageNodeId) {
        return new File
            (getStorageNodeDir(rootDirPath, kvstoreName, storageNodeId),
             COLLECTOR_DIR);
    }
}
