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

package oracle.kv.util.kvlite;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.AdminLoginManager;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginHandle;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.ManagedService;
import oracle.kv.impl.sna.ProcessMonitor;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.SecurityConfigCreator;
import oracle.kv.impl.util.SecurityConfigCreator.GenericIOHelper;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.utilint.JVMSystemUtils;

/**
 * A simple standalone KVStore instance.
 * <pre>
 * Usage:
 * ...KVLite -root &lt;kvroot&gt; &#92;
 *  [-store &lt;storename&gt; -host &lt;hostname&gt;
 *  -port &lt;port1:port2:...&gt;] &#92;
 *  [-noadmin] &#92;
 *  [-nothreads] [-shutdown] [-partitions n]
 *  [-harange start1,end1:start2,end2:...] &#92;
 *  [-servicerange start1,end1:start2,end2:...]
 *  [-secure-config &lt;enable|disable&gt;] -jmx &#92;
 *  [-restore-from-snapshot &lt;name of snapshot&gt;] &#92;
 *  [-admin-web-port &lt;admin web service port&gt;]
 *  [-numsns n] [-capacity n] [-repfactor n]
 *
 *   -nothreads -- means no threads and a separate process will be used for the
 *      RN. For multi shard store, it always uses separate processes.
 *   -shutdown -- will attempt to shutdown a running KVLite service, cleanly.
 *      e.g. KVLite -root &lt;kvroot&gt; -shutdown
 *   -noadmin -- do not start up bootstrap admin service. For multi shard store,
 *      it always starts 1 admin in the first SN.
 *   -secure-config &lt;enable|disable&gt; -- this option specifies whether to create
 *      a secure KVLite service, which is the default. When security is
 *      enabled, "-noadmin" option will be ignored, meaning that a bootstrap
 *      admin and regular will be created.
 *   -restore-from-snapshot &lt;name of snapshot&gt; -- When restart KVLite from
 *      existing root directory. If previous KVLite had been backup, snapshot
 *      directories will be created with snapshot name. Restore KVLite data
 *      and configurations using the specified snapshot. If no snapshot found,
 *      restart with snapshot will fail.
 *   -admin-web-port &lt;admin web service port&gt; -- Specify the port used to
 *      start up admin web service to service Http/Https request for admin
 *      command.
 *   -numsns -- number of storage nodes, default is 1.
 *   -capacity -- capacity for each storage node, default is 1.
 *   -repfactor -- replication factor for each shard, default is 1.
 * </pre>
 *
 * -partitions, -jmx -harange, -sns, -capacity, -repfactor and -servicerange are
 * "hidden" arguments in that they are not part of the usage message at this time.
 *
 * Kvroot is created if it does not exist.  If not provided, hostname defaults
 * to "localhost."  The -store, -host, -port, and -admin arguments only apply
 * when initially creating the store.  They are ignored for existing stores.
 *
 * This class will create and/or start a single RepNode in a single RepGroup by
 * default.
 * If the adminHttpPort is used a bootstrap admin instance will be created and
 * deployed, accessible via that port; otherwise there is no admin.
 *
 * The services are created as threads inside a single process unless the
 * -nothreads option is passed, which results in independent processes.
 *
 * This program depends only on kvstore.jar and je.jar unless the
 * adminHttpPort is passed.
 *
 * If number of storage nodes specified by -numsns is larger than 1, it will
 * deploy a multi shard store.
 * 1) to start/restart a multi shard store:
 * kvlite -port 5001:5021:5041 -numsns 3 -capacity 3 -repfactor 3
 * will start a kvstore on the local machine with 1 admin in the first SN.
 * If -port is not specified, it will use 5000:5020:5040 by default.
 * -servicerange/-harange usage:
 * -servicerange/-harange 5003,5007:5023,5027:5043,5047
 * the default -harange is port[i]+2, port[i]+9, for example,
 * if port is 5000:5020:5040, harange is 5002,5009:5022,5029:5042,5049
 * the default servicerange is null.
 * -mount/-storagedir/-storagedirsize/-restore-from-snapshot does not apply when
 *  it's a multi shard kvstore.
 *  2) to shutdown:
 *  kvlite -shutdown
 */
public class KVLite {

    public final static int DEFAULT_NUM_PARTITIONS = 10;
    private static final String DEFAULT_ROOT = "./kvroot";
    private static final String DEFAULT_STORE = "kvstore";
    private static final int DEFAULT_PORT = 5000;
    private static final String DEFAULT_STORAGE_DIR_SIZE = "10 GB";
    public static final String SECURITY_ENABLE = "enable";
    public static final String SECURITY_DISABLE = "disable";
    private static final int DEFAULT_ADMIN_WEB_PORT = -1;
    private static final String CONFIG_FILE_NAME = "config";
    private static final String CONFIG_FILE_FORMAT = ".xml";
    public static final String DEFAULT_SPLIT_STR=":";
    private static final Logger staticLogger =
        LoggerUtils.getLogger(KVLite.class, "main");
    private LoginHandle loginHandle;
    private LoginManager loginManager;

    /* External commands, for "java -jar" usage. */
    public static final String COMMAND_NAME = "kvlite";
    public static final String COMMAND_DESC =
        "start KVLite; note all args (-host, -port, etc) have defaults";
    public static final String COMMAND_ARGS =
        mkArgLine(CommandParser.getRootUsage(), DEFAULT_ROOT) + "\n\t" +
        mkArgLine(CommandParser.getStoreUsage(), DEFAULT_STORE) + "\n\t" +
        mkArgLine(CommandParser.getHostUsage(), "local host name") + "\n\t" +
        mkArgLine(CommandParser.getPortUsage(),
                  String.valueOf(DEFAULT_PORT)) + "\n\t" +
        mkArgLine(KVLiteParser.MOUNT_SIZE_FLAG + " <GB>", "10") + "\n\t" +
        mkArgLine(CommandParser.getNoAdminUsage(), "false") + "\n\t" +
        mkArgLine(KVLiteParser.SECURE_CONFIG_FLAG + " <" + SECURITY_ENABLE +
                  "|" + SECURITY_DISABLE + ">", SECURITY_ENABLE) + "\n\t" +
        mkArgLine(KVLiteParser.RESTORE_FROM_SNAPSHOT + " <name of snapshot>",
                  "no restore") + "\n\t" +
        mkArgLine(KVLiteParser.ADMIN_WEB_PORT_FLAG +
                  " <admin web service port>",
                  String.valueOf(DEFAULT_ADMIN_WEB_PORT));

    /*
     * Hidden: -shutdown, -partitions, -nothreads, -harange, -servicerange
     * -jmx, -storagedir, -storagedirsize, -numsns, -capacity, -repfactor,
     * -memorymb, -printstartupok
     */

    private String[] haPortRange;
    private String[] servicePortRange = null;
    private String host;
    private String kvroot;
    private String kvstore;
    private String mountPoint;
    private String mountPointSize = DEFAULT_STORAGE_DIR_SIZE;
    private int[] port;
    private boolean runBootAdmin;
    private int numPartitions;
    private boolean useThreads;
    private boolean verbose;
    private boolean enableJmx;
    private ParameterMap policyMap;
    private boolean isSecure;
    private String restoreSnapshotName;
    private int adminWebPort;
    private int memoryMB; /*optional for single RN kvstore*/
    private int repfactor = 1;
    private int numStorageNodes = 1;
    private int capacity = 1;
    private boolean singleRN = true;
    private boolean printStartupOK;
    private StorageNodeAgentImpl[] snas;
    private StorageNodeAgentAPI[] snaAPIs;
    private BootstrapParams[] bps;
    private boolean tableOnly;

    /**
     * Makes an arg usage line for an optional arg with a default value.  Adds
     * padding so default values line up neatly.  Looks like this:
     *      [argUsage]        # defaults to "defaultValue"
     */
    private static String mkArgLine(String argUsage, String defaultValue) {
       final StringBuilder builder = new StringBuilder();
       builder.append(CommandParser.optional(argUsage));
       while (builder.length() < 30) {
           builder.append(' ');
       }
       builder.append("# defaults to: ");
       builder.append(defaultValue);
       return builder.toString();
    }

    public KVLite(String kvroot,
                  String kvstore,
                  int registryPort,
                  boolean runBootAdmin,
                  String hostname,
                  String haPortRange,
                  String servicePortRange,
                  int numPartitions,
                  String mountPoint,
                  boolean useThreads,
                  boolean isSecure,
                  String restoreSnapshotName) {
        this(kvroot, kvstore, registryPort, runBootAdmin, hostname,
             haPortRange, servicePortRange, numPartitions, mountPoint,
             useThreads, isSecure, restoreSnapshotName, -1);
    }

    public KVLite(String kvroot,
                  String kvstore,
                  int registryPort,
                  boolean runBootAdmin,
                  String hostname,
                  String haPortRange,
                  String servicePortRange,
                  int numPartitions,
                  String mountPoint,
                  boolean useThreads,
                  boolean isSecure,
                  String restoreSnapshotName,
                  int adminWebPort) {
        this.kvroot = kvroot;
        this.kvstore = kvstore;
        this.port = new int[numStorageNodes];
        this.port[0] = registryPort;
        this.runBootAdmin = runBootAdmin;
        this.host = hostname;
        if (haPortRange != null) {
            this.haPortRange = new String[numStorageNodes];
            this.haPortRange[0] = haPortRange;
        }
        if (servicePortRange != null) {
            this.servicePortRange = new String[numStorageNodes];
            this.servicePortRange[0] = servicePortRange;
        }
        this.useThreads = useThreads;
        this.mountPoint = mountPoint;
        this.adminWebPort = adminWebPort;
        policyMap = null;
        verbose = true;
        enableJmx = false;
        this.numPartitions = numPartitions;
        this.isSecure = isSecure;
        this.restoreSnapshotName = restoreSnapshotName;
    }

    public KVLite(String kvroot,
            String kvstore,
            String registryPort,
            boolean runBootAdmin,
            String hostname,
            String haPortRange,
            String servicePortRange,
            int numPartitions,
            String mountPoint,
            boolean useThreads,
            boolean isSecure,
            String restoreSnapshotName,
            int adminWebPort,
            int numStorageNodes,
            int repfactor,
            int capacity
            ) {
        this.kvroot = kvroot;
        this.kvstore = kvstore;
        if (registryPort != null) {
            String[] portstr = registryPort.split(DEFAULT_SPLIT_STR);
            this.port = new int[portstr.length];
            for (int i = 0; i < portstr.length; i++) {
                if (portstr[i] != null)
                    this.port[i] = Integer.parseInt(portstr[i]);
            }
        } else {
            this.port = new int[numStorageNodes];
            for (int i = 0; i < numStorageNodes; i++) {
                this.port[i] = DEFAULT_PORT + i*20;
            }
        }
        this.runBootAdmin = runBootAdmin;
        this.host = hostname;
        if (haPortRange != null)
            this.haPortRange = haPortRange.split(DEFAULT_SPLIT_STR);
        if (servicePortRange != null)
            this.servicePortRange = servicePortRange.split(DEFAULT_SPLIT_STR);
        this.useThreads = useThreads;
        this.mountPoint = mountPoint;
        this.adminWebPort = adminWebPort;
        policyMap = null;
        verbose = true;
        enableJmx = false;
        this.numPartitions = numPartitions;
        this.isSecure = isSecure;
        this.restoreSnapshotName = restoreSnapshotName;
        this.repfactor = repfactor;
        this.numPartitions = numPartitions;
        this.numStorageNodes = numStorageNodes;
        this.capacity = capacity;
    }

    private KVLite() {
        this(null, null, 0, true, "localhost", null, null,
             DEFAULT_NUM_PARTITIONS, null, true, true, null,
             DEFAULT_ADMIN_WEB_PORT);
    }

    public ParameterMap getPolicyMap() {
        return policyMap;
    }

    public void setPolicyMap(ParameterMap map) {
        policyMap = map;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean getVerbose() {
        return verbose;
    }

    public void setTableOnly(boolean tableOnly) {
        this.tableOnly = tableOnly;
    }

    public boolean getTableOnly() {
        return tableOnly;
    }

    public void setEnableJmx(boolean enableJmx) {
        this.enableJmx = enableJmx;
    }

    public boolean getEnableJmx() {
        return enableJmx;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getAdminWebPort() {
        return adminWebPort;
    }

    public void setAdminWebPort(int adminWebPort) {
        this.adminWebPort = adminWebPort;
    }

    public File getMountPoint() {
        if (mountPoint != null) {
            return new File(mountPoint);
        }
        return null;
    }

    /*
     * Sets the storagedirsize in GB. This method must be called before start()
     * or it will have no effect. It will not affect existing stores.
     */
    public void setStorageSizeGB(int gb) {
        mountPointSize = (Integer.toString(gb) + " GB");
    }

    public int getMemoryMB() {
        return memoryMB;
    }

    public void setMemoryMB(int memoryMB) {
        this.memoryMB = memoryMB;
    }

    public int getNumStorageNodes() {
        return numStorageNodes;
    }

    public void setNumStorageNodes(int numStorageNodes) {
        this.numStorageNodes = numStorageNodes;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getRepfactor() {
        return repfactor;
    }

    public void setRepfactor(int repfactor) {
        this.repfactor = repfactor;
    }

    public StorageNodeId getStorageNodeId() {
        return getStorageNodeId(1);
    }

    public StorageNodeId getStorageNodeId(int i) {
        return new StorageNodeId(i);
    }

    private String getDefaultMountPoint() {
        /* For kvlite, use storage node directory as storage directory */
        return FileNames.getStorageNodeDir(
                kvroot,
                kvstore,
                getStorageNodeId(1)).getAbsolutePath();
    }

    static AdminLoginManager waitForSecurityStartUp(String host, int port) {
        /*
         * Don't enable auto-renew since we're not keeping this manager and
         * don't want to worry about stopping any threads
         */
        final AdminLoginManager alm =
            new AdminLoginManager(null, false /* autoRenew */, staticLogger);
        final int MAX_RETRY = 10;
        Exception lastException = null;
        for (int i=0; i < MAX_RETRY; i++) {
            try {
                if (!alm.bootstrap(host, port, null)) {
                    Thread.sleep(1000);
                } else {
                    return alm;
                }
            } catch (Exception e) {
                lastException = e;
            }
        }
        throw new RuntimeException(
            "Wait for admin login service fail", lastException);
    }

    private void showVersion() {
        System.out.println(KVVersion.CURRENT_VERSION);
        System.exit(0);
    }

    public class KVLiteParser extends CommandParser {

        public static final String NOTHREADS_FLAG = "-nothreads";
        private static final String VERSION_FLAG = "-version";
        public static final String PARTITION_FLAG = "-partitions";
        private static final String HARANGE_FLAG = "-harange";
        private static final String SERVICERANGE_FLAG = "-servicerange";
        private static final String MOUNT_FLAG = "-storagedir";
        private static final String OLD_MOUNT_FLAG = "-mount";
        public static final String MOUNT_SIZE_FLAG = "-storagedirsizegb";
        private static final String JMX_FLAG = "-jmx";
        public static final String SECURE_CONFIG_FLAG = "-secure-config";
        public static final String RESTORE_FROM_SNAPSHOT =
            "-restore-from-snapshot";
        private static final String ADMIN_WEB_PORT_FLAG = "-admin-web-port";
        private static final String STORAGENUM_FLAG="-numsns";
        private static final String REPFACTOR_FLAG="-repfactor";
        private static final String CAPACITY_FLAG="-capacity";
        public static final String MEMORY_MB_FLAG="-memorymb";
        public static final String PRINT_STARTUP_OK_FLAG="-printstartupok";

        private boolean shutdown;

        public KVLiteParser(String[] args) {
            super(args);
            shutdown = false;
        }

        public boolean getShutdown() {
            return shutdown;
        }

        @Override
        protected void verifyArgs() {
            if (getRootDir() == null) {
                missingArg(ROOT_FLAG);
            }
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(StorageNodeAgent.SHUTDOWN_FLAG)) {
                shutdown = true;
                return true;
            }
            if (arg.equals(NOTHREADS_FLAG)) {
                useThreads = false;
                return true;
            }
            if (arg.equals(JMX_FLAG)) {
                enableJmx = true;
                return true;
            }
            if (arg.equals(VERSION_FLAG)) {
                showVersion();
                return true;
            }
            if (arg.equals(HARANGE_FLAG)) {
                String haPortRangeStr = nextArg(arg);
                if (haPortRangeStr != null)
                    haPortRange = haPortRangeStr.split(DEFAULT_SPLIT_STR);
                return true;
            }
            if (arg.equals(SERVICERANGE_FLAG)) {
                String servicePortRangeStr = nextArg(arg);
                if (servicePortRangeStr != null)
                    servicePortRange =
                        servicePortRangeStr.split(DEFAULT_SPLIT_STR);
                return true;
            }
            if (arg.equals(ADMIN_WEB_PORT_FLAG)) {
                adminWebPort = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(PARTITION_FLAG)) {
                numPartitions = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(MOUNT_FLAG)) {
                mountPoint = nextArg(arg);
                return true;
            }
            /* [#21880] -mount is deprecated, replaced by -storagedir */
            if (arg.equals(OLD_MOUNT_FLAG)) {
                mountPoint = nextArg(arg);
                return true;
            }
            if (arg.equals(MOUNT_SIZE_FLAG)) {
                setStorageSizeGB(Integer.parseInt(nextArg(arg)));
                return true;
            }
            if (arg.equals(SECURE_CONFIG_FLAG)) {
                final String security = nextArg(arg);
                if (security.equals(SECURITY_ENABLE)) {
                    isSecure = true;
                    runBootAdmin = true;
                } else if (security.equals(SECURITY_DISABLE)) {
                    isSecure = false;
                } else {
                    usage("Unexpected value for " + SECURE_CONFIG_FLAG +
                        ": " + security);
                }
                return true;
            }
            if (arg.equals(RESTORE_FROM_SNAPSHOT)) {
                restoreSnapshotName = nextArg(arg);
                return true;
            }
            if (arg.equals(STORAGENUM_FLAG)) {
                numStorageNodes = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(REPFACTOR_FLAG)) {
                repfactor = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(CAPACITY_FLAG)) {
                capacity = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(MEMORY_MB_FLAG)) {
                memoryMB = Integer.parseInt(nextArg(arg));
                return true;
            }
            if (arg.equals(PRINT_STARTUP_OK_FLAG)) {
                printStartupOK = true;
                return true;
            }
            return false;
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME + "\n\t" +
                               COMMAND_ARGS);
            System.exit(1);
        }
    }

    private boolean parseArgs(String[] args) {
        String localHostname = "localhost";
        try {
            localHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            /* Use "localhost" default. */
        }
        KVLiteParser kp = new KVLiteParser(args);
        kp.setDefaults(DEFAULT_ROOT, DEFAULT_STORE, localHostname,
                       DEFAULT_PORT);
        kp.parseArgs();

        /*
         * Note we do not call setVerbose(kp.getVerbose()) because the verbose
         * option in this class is meant to be always on, or at least on by
         * default.  The verbose option in CommandParser, OTOH, is off by
         * default.
         */

        /* TODO: consider passing kp to KVLite for direct use */
        kvroot = kp.getRootDir();
        kvstore = kp.getStoreName();
        String portStr = kp.getPortStr();
        if (portStr != null) {
            String[] ports = portStr.split(DEFAULT_SPLIT_STR);
            port = new int[numStorageNodes];
            if (ports.length >= numStorageNodes) {
                for (int i = 0; i < numStorageNodes; i++) {
                    port[i] = Integer.parseInt(ports[i]);
                }
            } else {
                for (int i = 0; i < ports.length; i++) {
                    port[i] = Integer.parseInt(ports[i]);
                }
                for (int i = ports.length; i < numStorageNodes; i++) {
                    port[i] = port[i - 1] + 20;
                }
            }
        }
        runBootAdmin = kp.isRunBootAdmin();
        host = kp.getHostname();
        return kp.getShutdown();
    }

    /**
     * Tell a running KVLite instance to shut down.
     */
    public void shutdownStore(boolean force) {

        String singleRNfileName = CONFIG_FILE_NAME + CONFIG_FILE_FORMAT;
        String multiRNfileName = CONFIG_FILE_NAME + "0" + CONFIG_FILE_FORMAT;
        File singleRNfile =
            new File(kvroot + File.separator + singleRNfileName);
        File multiRNfile =
            new File(kvroot + File.separator + multiRNfileName);
        if (!singleRNfile.exists() && multiRNfile.exists()) {
            singleRN = false;
        } else if (singleRNfile.exists() && !multiRNfile.exists()) {
            singleRN = true;
        } else {
            sysErrPrintln("Error configuration file for" +
                                " root: " + kvroot);
        }

        for(int i = 0; true; i++) {
            String fileName = singleRN ? singleRNfileName :
                              CONFIG_FILE_NAME + i + CONFIG_FILE_FORMAT;
            File configfile = new File(kvroot + File.separator + fileName);
            if (!configfile.exists()) {
                if (i == 0) {
                    sysErrPrintln("Cannot find configuration file for" +
                                       " root: " + kvroot);
                }
                break;
            }
            try {
                BootstrapParams bp = ConfigUtils.getBootstrapParams(configfile);
                LoginManager login = null;
                if (bp.getSecurityDir() != null) {
                    File securityDir =
                        new File(bp.getRootdir(), bp.getSecurityDir());
                    File securityConfig = new File(securityDir,
                        StorageNodeAgent.DEFAULT_SECURITY_FILE);
                    if (securityConfig.exists()) {
                        SecurityParams sp =
                            ConfigUtils.getSecurityParams(securityConfig);
                        sp.initRMISocketPolicies(getLogger());
                        BootstrapParams.initRegistryCSF(sp);
                    }
                    login = new InternalLoginManager(null, getLogger());
                }
                String name =
                    RegistryUtils.bindingName
                    (bp.getStoreName(), getStorageNodeId(i+1).getFullName(),
                     RegistryUtils.InterfaceType.MAIN);
                StorageNodeAgentAPI snai =
                    RegistryUtils.getStorageNodeAgent
                    (bp.getHostname(), bp.getRegistryPort(), name, login,
                     getLogger());
                sysErrPrintln("Shutting down store " + bp.getStoreName() +
                              " in kvroot: " + kvroot);
                snai.shutdown(true /* stopService */, force,
                              "SNA shutdown requested");
            } catch (Exception e) {
                sysErrPrintln("Exception in shutdown, maybe the service is not " +
                              "running: " + e.getMessage());
            }
            if (singleRN) {
                break;
            }
        }
    }

    /**
     * Create a series of unregistered SNs, to mimic the initial deployment
     * of software on a storage node. These SNs do not yet have a storage node
     * id.
     */
    private void initStorageNodes() throws IOException {
        /*
         * Allow this to be called more than once.
         */
        if (snas != null) {
            return;
        }

        if (repfactor <= 0 || capacity <= 0 || numStorageNodes <= 0) {
            throw new IllegalArgumentException("ERROR: repfactor, capacity "
                    + "and numStorageNodes should not be 0");
        }
        if (repfactor > numStorageNodes) {
            throw new IllegalArgumentException("ERROR: repfactor should not "
                    + "be larger than numStorageNodes");
        }
        if(port == null || port.length != numStorageNodes) {
            throw new IllegalArgumentException("ERROR: port number is not "
                    + "equal to numStorageNodes " + port.length + " vs "
                    + numStorageNodes);
        }
        if (haPortRange != null && haPortRange.length != numStorageNodes) {
            throw new IllegalArgumentException("ERROR: haPortRange number is "
                    + "not equal to numStorageNodes");
        }
        if (servicePortRange != null &&
            servicePortRange.length != numStorageNodes) {
            throw new IllegalArgumentException("ERROR: servicePortRange "
                    + "number is not equal to numStorageNodes");
        }
        singleRN = (capacity == 1 && numStorageNodes == 1);

        snas = new StorageNodeAgentImpl[numStorageNodes];
        bps = new BootstrapParams[numStorageNodes];
        snaAPIs = new StorageNodeAgentAPI[numStorageNodes];

        if (!singleRN) {
            useThreads = false;
            mountPoint = null;
            restoreSnapshotName = null;
        } else {
            if (mountPoint == null) {
                mountPoint = getDefaultMountPoint();
            }
        }

        for (int i = 0; i < numStorageNodes; i++) {
            boolean createAdmin =
                (isSecure || runBootAdmin || !singleRN) && i == 0;

            String configFileName = "config" + i + ".xml";
            if (singleRN) {
                 configFileName = "config.xml";
            }
            generateBootstrapDir(configFileName, createAdmin, i);

            snas[i] = startSNA(kvroot, configFileName, createAdmin,
                               restoreSnapshotName, i);
        }
    }

    private void generateBootstrapDir(String configFileName,
                                      boolean runBootstrapAdmin,
                                      int i) {

        File rootDir = new File(kvroot);
        rootDir.mkdir();
        File configfile = new File(kvroot + File.separator + configFileName);
        File secfile = new File
        (kvroot + File.separator + FileNames.JAVA_SECURITY_POLICY_FILE);
        if (configfile.exists()) {
            bps[i] = ConfigUtils.getBootstrapParams(configfile);
            return;
        }

        if (kvstore == null || port[i] == 0) {
            sysErrPrintln("Store does not exist and there are " +
                          "insufficient arguments to create it.");
            new KVLiteParser(new String[0]).usage(null);
        }

        ArrayList<String> mountPoints = null;
        ArrayList<String> sizes = null;
        if (mountPoint != null) {
            mountPoints = new ArrayList<String>(capacity);
            sizes = new ArrayList<String>(capacity);
            mountPoints.add(mountPoint);
            sizes.add(mountPointSize);
        }

        String pathToFile = configfile.toString();
        String haRange = (haPortRange == null) ?
                         (port[i] + 2) + "," + (port[i] + 9) : haPortRange[i];
        String serviceRange = (servicePortRange == null) ?
                              null : servicePortRange[i];

        /*
         * We don't need JE HA when using Unix domain sockets because they
         * force a single host configuration with RF=1. Just use localhost so
         * that the JE HA configuration, which won't be used, doesn't fail.
         */
        final String haHost =
            NetworkAddress.isUnixDomainHostname(host) ? "localhost" : host;
        bps[i] = new BootstrapParams(
            kvroot, host, haHost, haRange,
            serviceRange, null, port[i],
            /*start 1 admin only, on the first SN by default*/
            (runBootAdmin && i == 0) ? adminWebPort : DEFAULT_ADMIN_WEB_PORT,
            capacity, null /*storageType*/,
            isSecure? (i == 0 ? generateSecurityDir() : "security") : null,
            runBootstrapAdmin,
            (enableJmx ? "oracle.kv.impl.mgmt.jmx.JmxAgent" : null));
        if (singleRN && memoryMB != 0) {
            bps[i].setMemoryMB(memoryMB);
        } else if (!singleRN){
            if (memoryMB == 0) {
                memoryMB = 8092;
            }
            memoryMB = bumpMemoryMB(memoryMB, capacity);
            bps[i].setMemoryMB(memoryMB);
        }
        bps[i].setStorgeDirs(mountPoints, sizes);

        ConfigUtils.createBootstrapConfig(bps[i], pathToFile);

        if (!secfile.exists()) {
            ConfigUtils.createSecurityPolicyFile(secfile);
        }
    }

    private int bumpMemoryMB(int memoryMBValue, int capacityValue) {
        if (!JVMSystemUtils.ZING_JVM) {
            return memoryMBValue;
        }

        /*
         * Use (capacity + 2) to account for SN and one Admin.
         */
        final int minHeapMem =
            (JVMSystemUtils.MIN_HEAP_MB * (capacityValue + 2));

        /* Assume the default SN_RN_HEAP_PERCENT is used. */
        final int heapPct =
            Integer.parseInt(ParameterState.SN_RN_HEAP_PERCENT_DEFAULT);

        /*
         * Heap memory is determined from memoryMB by KVS as follows:
         *   heapMemory = (memoryMB * heapPct) / 100
         * Invert that calculation here to get minMemoryMB from minHeapMem.
         * Add (heapPct - 1) to account for integer truncation.
         */
        final int minMemoryMB = ((minHeapMem * 100) + heapPct - 1) / heapPct;

        return Math.max(memoryMBValue, minMemoryMB);
    }

    /**
     * Start an instance of SNA assuming the bootstrap directory and file have
     * been created, and specifying whether to disable services.
     */
    private StorageNodeAgentImpl startSNA(String bootstrapDir,
                                         String bootstrapFile,
                                         boolean createAdmin,
                                         String restoreSnapshot,
                                         int i)
                                         throws IOException {
        final List<String> snaArgs = new ArrayList<String>();
        snaArgs.add(CommandParser.ROOT_FLAG);
        snaArgs.add(bootstrapDir);
        snaArgs.add(StorageNodeAgent.CONFIG_FLAG);
        snaArgs.add(bootstrapFile);
        if (useThreads) {
            snaArgs.add(StorageNodeAgent.THREADS_FLAG);
        }
        if (restoreSnapshot != null) {
            snaArgs.add(StorageNodeAgent.RESTORE_FROM_SNAPSHOT);
            snaArgs.add(restoreSnapshot);
            snaArgs.add(StorageNodeAgent.UPDATE_CONFIG_FLAG);
            snaArgs.add("true");
        }

        StorageNodeAgentImpl sna = new StorageNodeAgentImpl(createAdmin);
        try {
            sna.parseArgs(snaArgs.toArray(new String[snaArgs.size()]));
            sna.start();
            if (!useThreads) {
                sna.addShutdownHook();
            }
        } catch (IOException e) {
            sna = null;
            throw e;
        }

        if (isSecure && createAdmin) {
            if (!sna.isRegistered()) {
                waitForSecurityStartUp(host, port[0]);
            }
            loginManager = new InternalLoginManager(null, getLogger());
            loginHandle = loginManager.getHandle(
               new HostPort(host, port[0]), ResourceType.STORAGE_NODE);
        }

        snaAPIs[i] = StorageNodeAgentAPI.wrap(sna, loginHandle);
        return sna;
    }


    /**
     * Start without waiting for services.
     */
    public void start() {
        start(false);
    }

    private String getPortStr() {
        String portstr = "";
        if (port != null) {
            for (int i = 0; i < port.length; i++) {
                portstr += Integer.toString(port[i]);
                if (i != port.length -1) {
                    portstr +=DEFAULT_SPLIT_STR;
                }
            }
        }
        return portstr;
    }

    /**
     * Start the store, optionally waiting for the services to be in status
     * RUNNING.
     */
    public void start(boolean waitForServices) {
        try {
            startThrowsException(waitForServices);
        } catch (Exception e){
            String trace = LoggerUtils.getStackTrace(e);
            sysErrPrintln("KVLite: exception in start: " + trace);
        }
    }

    public void startThrowsException(boolean waitForServices)
        throws IOException, NotBoundException {

        if (printStartupOK) {
            ManagedService.setThreadModePrintStartupOK(true);
            ProcessMonitor.setPrintStartupOK(true);
        }
        initStorageNodes();
        int registered = 0;
        for (int i = 0; i < snas.length; i ++) {
            if (snas[i].isRegistered()) {
                registered ++;
            }
        }
        if (registered == snas.length) {
            if (verbose) {
                /*
                 * Modify state based on parameters read from existing store
                 */
                BootstrapParams bp = bps[0];
                String portstr = Integer.toString(bp.getRegistryPort());
                System.out.println
                    ("Opened existing store with config:\n" +
                     CommandParser.ROOT_FLAG + " " + bp.getRootdir() + " " +
                     CommandParser.STORE_FLAG + " " +
                     bp.getStoreName() + " " +
                     CommandParser.HOST_FLAG + " " +
                     bp.getHostname() + " " +
                     CommandParser.PORT_FLAG + " " + portstr + " " +
                     (bp.isHostingAdmin() ?
                      "" :
                      CommandParser.NO_ADMIN_FLAG + " ") +
                     KVLiteParser.SECURE_CONFIG_FLAG + " " +
                     ((bp.getSecurityDir() != null) ?
                      SECURITY_ENABLE :
                      SECURITY_DISABLE) + " " +
                     StorageNodeAgent.RESTORE_FROM_SNAPSHOT + " " +
                     restoreSnapshotName + " " +
                     KVLiteParser.ADMIN_WEB_PORT_FLAG + " " +
                     bp.getAdminWebServicePort());
            }
        } else if (registered == 0) {
            if (numPartitions == 0) {
                numPartitions = DEFAULT_NUM_PARTITIONS;
            }
            if (isSecure || runBootAdmin || !singleRN) {
                new KVLiteAdmin(kvstore, snas, policyMap,
                                numPartitions, repfactor,
                                tableOnly).run(getLogger());
            } else {
                new KVLiteRepNode(kvstore, snaAPIs[0],
                                  bps[0], numPartitions).run();
            }
            if (verbose) {
                System.out.println
                    ("Created new kvlite store with args:\n" +
                     CommandParser.ROOT_FLAG + " " + kvroot + " " +
                     CommandParser.STORE_FLAG + " " + kvstore + " " +
                     CommandParser.HOST_FLAG + " " + host + " " +
                     CommandParser.PORT_FLAG + " " + getPortStr() + " " +
                     (runBootAdmin ? "" :
                                     CommandParser.NO_ADMIN_FLAG + " ") +
                     KVLiteParser.ADMIN_WEB_PORT_FLAG + " " +
                     adminWebPort + " " +
                     KVLiteParser.SECURE_CONFIG_FLAG + " " +
                     (isSecure ? SECURITY_ENABLE : SECURITY_DISABLE) + " " +
                     ((restoreSnapshotName == null) ?
                         "" :
                         (StorageNodeAgent.RESTORE_FROM_SNAPSHOT + " " +
                         restoreSnapshotName)));
            }
            if (waitForServices) {
                if (verbose) {
                    System.out.println("Waiting for services to start");
                }
                CommandServiceAPI admin = null;
                if (isSecure || runBootAdmin || !singleRN) {
                    if (verbose) {
                        System.out.println("Waiting for admin at " +
                                           host + ":" + port[0]);
                    }
                        admin = ServiceUtils.waitForAdmin(
                            host, port[0], loginManager, 10,
                            ServiceStatus.RUNNING, getLogger());
                }
                ServiceStatus[] target = {ServiceStatus.RUNNING};
                if (verbose) {
                    System.out.println("Waiting for RepNode for store " +
                        kvstore + " at " + host + ":" + port[0]);
                }
                if (admin != null) {
                    Topology topo = admin.getTopology();
                    List<RepNode> repNodes = topo.getSortedRepNodes();
                    ServiceStatus[] targets = {ServiceStatus.RUNNING};
                    for (RepNode rn : repNodes) {
                        RepNodeId rnId = rn.getResourceId();
                        ServiceUtils.waitForRepNodeAdmin(topo, rnId,
                            snas[0].getStorageNodeAgent().getLoginManager(),
                                5, targets, getLogger());
                    }
                } else {
                    ServiceUtils.waitForRepNodeAdmin
                        (kvstore, host, port[0], new RepNodeId(1,1),
                             getStorageNodeId(1), loginManager, 10, target,
                             getLogger());
                }
            }
        } else {
            throw new IllegalStateException("KVLite: not all SNA registered");
        }
    }

    public void stop(boolean force) {
        try {
            stopThrowsException(force);
        } catch (Exception e) {
            sysErrPrintln("Exception in stop: " + e.getMessage());
        }
    }

    public void stopThrowsException(boolean force) throws RemoteException {
        if (verbose) {
            System.out.println("Stopping KVLite store " + kvstore);
        }
        if (snas == null) {
            return;
        }
        for (int i = 0; i <numStorageNodes; i++) {
            if (snaAPIs != null && snaAPIs[i] != null) {
                snaAPIs[i].shutdown(true /* stopServices */, force,
                                    "SNA shutdown requested");
            }
        }
    }

    public StorageNodeAgentImpl[] getSNAs() {
        return snas;
    }

    public StorageNodeAgentAPI[] getSNAPIs() {
        return snaAPIs;
    }

    public StorageNodeAgentImpl getSNA() {
        return (snas == null)? null: snas[0];
    }

    public StorageNodeAgentAPI getSNAPI() {
        return snaAPIs[0];
    }

    private String generateSecurityDir() {
        final ParsedConfig config = new ParsedConfig();
        /* Default to length 12 key store password */
        config.setKeystorePassword(
            SecurityUtils.generateKeyStorePassword(12));
        config.setPrintCreatedFiles(false);
        GenericIOHelper ioHelper = EmbeddedMode.isEmbedded()?
            new GenericIOHelper(EmbeddedMode.NULL_PRINTSTREAM):
            new GenericIOHelper(System.out);
        final SecurityConfigCreator scCreator =
            new SecurityConfigCreator(kvroot,
                                      config,
                                      ioHelper);
        try {
            scCreator.createConfig();
        } catch (Exception e) {
            throw new RuntimeException(
                "Caught exception when creating " +
                "security configurations", e);
        }
        return config.getSecurityDir();
    }

    /**
     * If the KVLite is in embedded mode, the error output will be redirected
     * to the log file. Otherwise, the error output will be printed on the
     * console.
     */
    private void sysErrPrintln(String msg) {
        if (!EmbeddedMode.isEmbedded()) {
            System.err.println(msg);
        } else {
            getLogger().info(msg);
        }
    }

    /**
     * Returns the SNA logger, or the bootstrap SNA logger if there is no SNA
     * yet.
     */
    private Logger getLogger() {
        final StorageNodeAgentImpl sna = getSNA();
        return (sna != null) ?
            sna.getLogger() :
            LoggerUtils.getBootstrapLogger(
                kvroot, FileNames.BOOTSTRAP_SNA_LOG, "KVLite");
    }

    public static void main(String[] args) {
        KVLite store = new KVLite();
        boolean shutdown = store.parseArgs(args);
        if (shutdown) {
            store.shutdownStore(false);
        } else {
            store.start();
        }
    }
}
