/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import static oracle.kv.impl.security.PasswordManager.FILE_STORE_MANAGER_CLASS;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.KVStoreFactory;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.as.AggregationService;
import oracle.kv.impl.mgmt.MgmtUtil;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.login.InternalLoginManager;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.TopoTopoResolver;
import oracle.kv.impl.security.login.TopologyResolver;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.DelayedALM;
import oracle.kv.impl.util.DelayedRLM;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.StorageNodeUtils.KerberosOpts;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.apache.hadoop.conf.Configuration;

/**
 * Start, stop, reconfigure nodes.
 */
public class CreateStore {

    private static final Logger logger =
        Logger.getLogger("oracle.kv.CreateStore");

    private static final int portsPerFinder = 20;
    private static final int haRange = 8;
    private static final int defaultPartitions = 300;
    private static final int defaultReplicationFactor = 3;
    private static final int defaultStorageNodes = 9;
    public static final String defaultUser = "admin";
    private static final String defaultUserPass = "NoSql@@123";
    public static final String defaultPassStore = "test.passwd";
    private static final String defaultLoginFile = "test.security";
    private LoginManager adminLoginMgr;

    /**
     * The min amount of memory with which to configure a capacity=1 SN in a
     * unit test. At this size we can accommodate a 3X3 config in 2.25G.
     *
     * Larger values risk making the machine thrash to the point where the
     * tests can produce spurious errors or cause vm thrashing on machines
     * with 3-4GB of memory.
     *
     * Smaller values risk cache eviction and JE level slowdowns for large
     * data sets.
     */
    public static final int MB_PER_SN = 256;
    public static final String MB_PER_SN_STRING = String.valueOf(MB_PER_SN);

    public static final String STORAGE_NODE_POOL_NAME = "CreateStorePool";

    /**
     * The default properties that should be specified when creating JE
     * environments.
     */
    public static final String JE_DEFAULT_PROPERTIES =
        canonicalJeParams(System.getProperty("test.je.props", ""));

    private String storeName;
    private String rootDir;
    private String hostname;
    private List<ZoneInfo> zones;
    private int numPartitions;
    private int numStorageNodes;
    private int numStorageNodesInPool;
    private int capacity = 1;
    private int[] capacities;
    private int totalPrimaryRepFactor;
    private int totalRepFactor;
    private int numShards;
    private int startPort;
    private final int memoryMB;
    private StorageNodeAgent[] snas;
    private PortFinder[] portFinders;
    private CommandServiceAPI cs;
    private int csIndex = -1;
    private RepNodeAdminAPI[] rns;
    private RemoteTestAPI[] rnTest;
    private ArbNodeAdminAPI[]ans;
    private boolean useThreads;
    private boolean verbose;
    private String mgmtImpl;
    private ParameterMap policyMap;
    private boolean secure;
    private String userExternalAuth;
    private Set<Parameter> extraParams;
    private final List<SecureUser> secureUsers =
        new ArrayList<SecureUser>();
    private boolean mgmtPortsShared;
    private String restoreSnapshotName;
    private String updateConfig;
    private boolean addSNToInitiallySingleSNStore = false;

    /** Whether SNAs should each have their own root directories */
    private boolean separateRoots;

    /* Kerberos configuration options for each sn */
    private KerberosOpts[] krbOpts;

    /* Security Configuration option */
    private SecureOpts secOpts;

    private File defaultUserLogin;

    private Properties defaultUserLoginProps;

    /* Hang on to per-SN information for unit test usage. */
    private final Map<StorageNodeId,PortFinder> portFinderMap =
        new HashMap<StorageNodeId,PortFinder>();

    private final Map<StorageNodeId,StorageNodeAgent> snaMap =
        new HashMap<StorageNodeId,StorageNodeAgent>();

    private final Map<StorageNodeId,List<RepNodeId>> snToRNs =
        new HashMap<StorageNodeId,List<RepNodeId>>();

    private final Map<StorageNodeId,List<ArbNodeId>> snToANs =
            new HashMap<StorageNodeId,List<ArbNodeId>>();

    private final Set<StorageNodeId> snsWithAdmins =
        new HashSet<StorageNodeId>();

    /*
     * AdminDeployed and adminLocations provide a mechanism for placing admins
     * on particular SNs, for specific test cases. The SNs specified by the ids
     * in adminLocations should host Admins. Defaults to SNs 1, 2, 3.
     */
    private final AtomicBoolean adminDeployed = new AtomicBoolean(false);
    private Set<Integer> adminLocations = new HashSet<>();

    private AggregationService as = null;

    public static class SecureUser {
        private final String username;
        private final String password;
        private final boolean isAdmin;

        public SecureUser(String username, String password, boolean admin) {
            this.username = username;
            this.password = password;
            this.isAdmin = admin;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public boolean getIsAdmin() {
            return isAdmin;
        }
    }

    /** Information about a zone. */
    public static class ZoneInfo {
        public final int repFactor;
        public final DatacenterType zoneType;
        public boolean allowArbiters;
        public boolean masterAffinity;
        public ZoneInfo(int repFactor) {
            this(repFactor, DatacenterType.PRIMARY);
        }
        public ZoneInfo(int repFactor, DatacenterType zoneType) {
            if (repFactor < 0) {
                throw new IllegalArgumentException(
                    "The repFactor must be greater than 0");
            }
            this.repFactor = repFactor;
            if (zoneType == null) {
                throw new IllegalArgumentException(
                    "The zone type must not be null");
            }
            this.zoneType = zoneType;
            allowArbiters = false;
        }
        public static List<ZoneInfo> primaries(int... replicationFactors) {
            final List<ZoneInfo> result =
                new ArrayList<ZoneInfo>(replicationFactors.length);
            for (int replicationFactor : replicationFactors) {
                result.add(new ZoneInfo(replicationFactor));
            }
            return result;
        }

        public ZoneInfo setAllowArbiters(boolean allowArbiters) {
            this.allowArbiters = allowArbiters;
            return this;
        }

        public ZoneInfo setMasterAffinity(boolean masterAffinity) {
            this.masterAffinity = masterAffinity;
            return this;
        }
    }

    /**
     * Defaults for the version that uses main()
     */
    public CreateStore() {
        startPort = 0;
        numStorageNodes = defaultStorageNodes;
        numStorageNodesInPool = 0;
        numPartitions = defaultPartitions;
        zones = ZoneInfo.primaries(defaultReplicationFactor);
        hostname = "localhost";
        useThreads = false;
        verbose = false;
        secure = TestBase.SECURITY_ENABLE;
        memoryMB = 1024;
        restoreSnapshotName = null;
        updateConfig = "true";

        /* Default SNs which host Admins */
        adminLocations.add(1);
        adminLocations.add(2);
        adminLocations.add(3);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       int replicationFactor,
                       int numPartitions,
                       int capacity)
        throws Exception {

        this(storeName, startPort, numStorageNodes,
             ZoneInfo.primaries(replicationFactor), numPartitions, capacity);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       List<ZoneInfo> zones,
                       int numPartitions,
                       int capacity)
        throws Exception {

        this(storeName, startPort, numStorageNodes,
             zones, numPartitions, capacity, capacity * MB_PER_SN);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       int replicationFactor,
                       int numPartitions,
                       int capacity,
                       int memoryMB)
        throws Exception {

        this(storeName, startPort, numStorageNodes,
             ZoneInfo.primaries(replicationFactor), numPartitions, capacity,
             memoryMB);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       List<ZoneInfo> zones,
                       int numPartitions,
                       int capacity,
                       int memoryMB)
        throws Exception {

        /* Don't use threads; don't configure mgmt */
        this(storeName, startPort, numStorageNodes,
             zones, numPartitions, capacity, memoryMB, false, null,
             false, TestBase.SECURITY_ENABLE, null /* userExternalAuth */);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       int replicationFactor,
                       int numPartitions,
                       int capacity,
                       int memoryMB,
                       boolean useThreads,
                       String mgmtImpl)
        throws Exception {

        this(storeName, startPort, numStorageNodes,
             ZoneInfo.primaries(replicationFactor), numPartitions, capacity,
             memoryMB, useThreads, mgmtImpl);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       List<ZoneInfo> zones,
                       int numPartitions,
                       int capacity,
                       int memoryMB,
                       boolean useThreads,
                       String mgmtImpl)
        throws Exception {

        this(storeName, startPort, numStorageNodes, zones,
             numPartitions, capacity, memoryMB, useThreads, mgmtImpl,
             false, TestBase.SECURITY_ENABLE,
             null /* userExternalAuth */);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       int replicationFactor,
                       int numPartitions,
                       int capacity,
                       int memoryMB,
                       boolean useThreads,
                       String mgmtImpl,
                       boolean mgmtPortsShared,
                       boolean secure)
        throws Exception {

        this(storeName, startPort, numStorageNodes,
             ZoneInfo.primaries(replicationFactor), numPartitions, capacity,
             memoryMB, useThreads, mgmtImpl, mgmtPortsShared, secure,
             null /* userExternalAuth */);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       int replicationFactor,
                       int numPartitions,
                       int[] capacities,
                       int memoryMB,
                       boolean useThreads,
                       String mgmtImpl,
                       boolean mgmtPortsShared,
                       boolean secure)
        throws Exception {

        this(storeName, startPort, numStorageNodes,
             ZoneInfo.primaries(replicationFactor), numPartitions, -1,
             memoryMB, useThreads, mgmtImpl, mgmtPortsShared, secure,
             null /* userExternalAuth */);
        this.capacities = capacities;
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       int replicationFactor,
                       int numPartitions,
                       int capacity,
                       int memoryMB,
                       boolean useThreads,
                       String mgmtImpl,
                       boolean mgmtPortsShared,
                       boolean secure,
                       String userExternalAuth)
        throws Exception {

        this(storeName, startPort, numStorageNodes,
             ZoneInfo.primaries(replicationFactor), numPartitions, capacity,
             memoryMB, useThreads, mgmtImpl, mgmtPortsShared, secure,
             userExternalAuth);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       List<ZoneInfo> zones,
                       int numPartitions,
                       int capacity,
                       int memoryMB,
                       boolean useThreads,
                       String mgmtImpl,
                       boolean mgmtPortsShared,
                       boolean secure)
        throws Exception {

        this(storeName, startPort, numStorageNodes,
             zones, numPartitions, capacity,
             memoryMB, useThreads, mgmtImpl, mgmtPortsShared, secure,
             null);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       List<ZoneInfo> zones,
                       int numPartitions,
                       int capacity,
                       int memoryMB,
                       boolean useThreads,
                       String mgmtImpl,
                       boolean mgmtPortsShared,
                       boolean secure,
                       String userExternalAuth)
        throws Exception {

        this(storeName, startPort, numStorageNodes,
             zones, numPartitions, capacity, memoryMB, useThreads,
             mgmtImpl, mgmtPortsShared, secure, userExternalAuth,
             null /* exraParams */);
    }

    public CreateStore(String storeName,
                       int startPort,
                       int numStorageNodes,
                       List<ZoneInfo> zones,
                       int numPartitions,
                       int capacity,
                       int memoryMB,
                       boolean useThreads,
                       String mgmtImpl,
                       boolean mgmtPortsShared,
                       boolean secure,
                       String userExternalAuth,
                       Set<Parameter> extraParams)
        throws Exception {

        this.useThreads = useThreads;
        this.storeName = storeName;
        if ((zones == null) || zones.isEmpty()) {
            throw new IllegalStateException("Zones must not be empty");
        }
        this.zones = zones;
        this.numPartitions = numPartitions;
        this.numStorageNodes = numStorageNodes;
        this.numStorageNodesInPool = numStorageNodes;
        this.startPort = startPort - 1;
        this.rootDir = TestUtils.getTestDir().toString();
        this.hostname = "localhost";
        this.capacity = capacity;
        this.memoryMB = memoryMB;
        this.mgmtImpl = mgmtImpl;
        this.mgmtPortsShared = mgmtPortsShared;
        this.secure = secure;
        this.userExternalAuth = userExternalAuth;
        this.extraParams = extraParams;
        this.krbOpts = null;
        cs = null;
        verbose = false;
        policyMap = null;
        restoreSnapshotName = null;
        updateConfig = "true";

        /* Default SNs which host Admins */
        adminLocations.add(1);
        adminLocations.add(2);
        adminLocations.add(3);
    }

    public void setPolicyMap(ParameterMap map) {
        policyMap = map;
    }

    public ParameterMap getPolicyMap() {
        return policyMap;
    }

    public void setPoolSize(int size) {
        numStorageNodesInPool = size;
    }

    public void setAllowArbiters(boolean allowArbiters) {
        for (ZoneInfo zone : zones) {
            zone.setAllowArbiters(allowArbiters);
        }

    }

    public void setMasterAffinities(boolean masterAffinity) {
        for (ZoneInfo zone : zones) {
            zone.setMasterAffinity(masterAffinity);
        }
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public void setKrbOpts(KerberosOpts[] krbOpts) {
        this.krbOpts = krbOpts;
    }

    public void setSecureOpts(SecureOpts secOpts) {
        this.secOpts = secOpts;
    }

    /**
     * Specify whether SNAs should have separate root directories. Make sure to
     * call this method before initializing SNAs and don't change the value
     * after that.
     */
    public void setSeparateRoots(boolean value) {
        separateRoots = value;
    }

    /**
     * Returns the root directory for the SNA with the specified (0-based)
     * index.
     */
    public String getRootDir(int snaIdx) {
        return separateRoots ?
            rootDir + File.separator + "root" + snaIdx :
            rootDir;
    }

    public void setUseThreads(boolean value) {
        useThreads = value;
    }

    public void addUser(String username, String pw, boolean admin) {
        secureUsers.add(new SecureUser(username, pw, admin));
    }

    public void addUser(SecureUser user) {
        if (!secure) {
            throw new IllegalArgumentException("store is not secure");
        }
        secureUsers.add(user);
    }

    public void setAddSNToInitiallySingleSNStore() {
        addSNToInitiallySingleSNStore = true;
    }

    public void setExtraParams(Set<Parameter> extraParams) {
        this.extraParams = extraParams;
    }

    public File getTrustStore() {
        if (!secure) {
            return null;
        }

        String secDir = "security";

        /*
         * A store having customized Kerberos configuration, which have
         * multiple security directories, use the first one as suffix of
         * security dir to find trust store. Also check for the special case
         * when addSN method is used to add SN to initially single SN store in
         * which case security directory would not have the suffix '0'.
         */
        if (((SecurityUtils.hasKerberos(userExternalAuth) &&
              numStorageNodes > 1) ||
             (krbOpts != null && krbOpts.length > 1)) &&
            !addSNToInitiallySingleSNStore) {
            secDir = secDir + "0";
        }
        return new File(new File(getRootDir(0), secDir),
                        SSLTestUtils.SSL_CTS_NAME);
    }

    public File getKeyStore() {
        if (!secure) {
            return null;
        }

        String secDir = "security";

        /*
         * A store having customized Kerberos configuration, which have
         * multiple secuirty directories, use the first one as suffix of
         * security dir to find trust store.
         */
        if ((SecurityUtils.hasKerberos(userExternalAuth) &&
             numStorageNodes > 1) ||
            (krbOpts != null && krbOpts.length > 1)) {
            secDir = secDir + "0";
        }
        return new File(new File(getRootDir(0), secDir), "store.keys");
    }

    /**
     * Optionally specify which SNs, in addition to sn1, should host
     * Admins. Note that since SN1 holds the original bootstrap Admin, it will
     * always host an Admin, regardless of the adminLocations mechanism.
     *
     * If not specified, the default is that SN1, SN2 and SN3 will each host an
     * SN. Must be specified before CreateStore.start() is called.  Once set,
     * the locations will be used for all subsequent use of the test store.
     *
     * SN1 must always be specified.
     *
     * For example, sn1, sn2, sn3 will host Admins.
     *   CreateStore testStoreCreator = new CreateStore(....)
     *   testStoreCreator.start(...)
     *
     * or in this next case, sn1, sn3, sn5 will host an Admin. But if there are
     * fewer SNs than Admin locations, i.e if there are only 4 SNs, then only
     * sn1 and sn3 will host an Admin.
     *
     *   CreateStore testStoreCreator = new CreateStore( ... );
     *   testStore.setAdminLocations(1, 3, 5);
     *   testStore.start();
     *
     * If setAdminLocations is called multiple times, the last call supercedes
     * all earlier ones. In other words, the admin sets don't accumulate.
     */
    public void setAdminLocations(int... hostingSNs) {

        if (adminDeployed.get()) {
            throw new RuntimeException(
                    "Can't call CreateStore.setAdminLocations() after calling "
                            + "CreateStore.start()");
        }

        Set<Integer> targetSNs = new HashSet<Integer>();
        for (int i : hostingSNs) {
            targetSNs.add(i);
        }

        if (!targetSNs.contains(1)) {
            throw new IllegalArgumentException
                ("sn1 must be specified, as it always hosts an Admin.");
        }

        adminLocations = targetSNs;
    }

    /**
     * Returns the SN IDs of the SNs hosting admins.
     */
    public Set<Integer> getAdminLocations() {
        return adminLocations;
    }

    /**
     * Compute the admin locations needed to create RF admins in each current
     * zone.
     *
     * @return the indices of SNs with admins
     */
    public int[] computeAdminsForZones() {
        initNumShards();

        /* Allocate RF admins per zone */
        final int[] adminSNs = new int[totalRepFactor];
        int adminSNsOffset = 0;
        int zoneId = 0;
        int zoneRemainingRepFactor = 0;
        int zoneRemainingCapacity = 0;
        for (int i = 1; i <= numStorageNodesInPool; i++) {
            if (zoneRemainingCapacity == 0) {
                if (++zoneId > zones.size()) {
                    break;
                }
                zoneRemainingRepFactor = zones.get(zoneId-1).repFactor;
                zoneRemainingCapacity = numShards * zoneRemainingRepFactor;
            }
            if (zoneRemainingRepFactor > 0) {
                adminSNs[adminSNsOffset++] = i;
                zoneRemainingRepFactor--;
            }

            if (zoneRemainingCapacity > 0)  {
                if (capacities == null) {
                    zoneRemainingCapacity -= capacity;
                } else {
                    zoneRemainingCapacity -= capacities[i-1];
                }
            }
        }
        return adminSNs;
    }

    /**
     * Create a series of unregistered SNs, to mimic the initial deployment
     * of software on a storage node. These SNs do not yet have a storage node
     * id.
     */
    public void initStorageNodes()
        throws Exception {

        /*
         * Allow this to be called more than once.
         */
        if (snas != null) {
            return;
        }

        initNumShards();

        snas = new StorageNodeAgent[numStorageNodes];
        portFinders = new PortFinder[numStorageNodes];
        verbose("Creating " + numStorageNodes +
                " storage nodes in base rootDir: " + rootDir);

        if (secOpts == null) {
            secOpts = new SecureOpts().setSecure(secure).
                    setUserExternalAuth(userExternalAuth);
        } else {
            secOpts.setUserExternalAuth(userExternalAuth);
        }

        int mgmtTrapPort = 0;
        for (int i = 0; i < numStorageNodes; i++) {
            mgmtTrapPort = initStorageNode(i, false, mgmtTrapPort);
        }
        verbose("Done creating storage nodes");
    }
    /**
     * Create an unregistered SN, to mimic the initial deployment of software
     * on a storage node.
     *
     * @param i index of the SN.
     *
     * @param addSN whether use case is adding an SN after initial store
     * deployment.
     *
     * @param mgmtTrapPort SNMP trap port.
     *
     * @return trap port set while creating first SN.
     */
    private int initStorageNode(int i, boolean addSN, int mgmtTrapPort)
        throws Exception {

        final String snaRootDir = getRootDir(i);
        if (separateRoots) {
            new File(snaRootDir).mkdir();
        }
        PortFinder pf = new PortFinder(startPort, haRange, hostname);
        KerberosOpts krbOpt = null;

        if (krbOpts != null && krbOpts.length > i && krbOpts[i] != null) {
            krbOpt = krbOpts[i];
        } else if (SecurityUtils.hasKerberos(userExternalAuth)) {
            krbOpt = new KerberosOpts();

            /* Simply use host name with snId for instance name */
            krbOpt.setInstanceName(pf.getHostname() + i);
        }

        if (krbOpt != null) {
            /*
             * Default location of Kerberos configuration file is
             * under root directory
             */
            krbOpt.setDefaultKrbConf(snaRootDir);

            /*
             * Kerberos requires different security configuration for
             * each sn, so use different security directory name
             */
            if (numStorageNodes > 1) {
                secOpts.setSecurityDir(FileNames.SECURITY_CONFIG_DIR + i);
            }
        }

        /*
         * If sharing a trap port, then use the
         * port from the first storage node.
         */
        if (mgmtImpl != null & mgmtPortsShared && i == 0) {
            mgmtTrapPort = pf.getMgmtTrapPort();
        }

        snas[i] = StorageNodeUtils.createUnregisteredSNA
            (snaRootDir,
             pf,
             addSN ? 0 : ((capacities == null) ? capacity : capacities[i]),
             "config" + i + ".xml",
             useThreads,
             i == 0, /* Create Admin for first one */
             mgmtImpl,
             0, /* mgmtPollPort */
             mgmtTrapPort,
             memoryMB,
             null, /* storageDirs */
             secOpts,
             krbOpt,
             extraParams);

        verbose("Created Storage Node " + i + " on host:port " +
                hostname + ":" + pf.getRegistryPort());
        startPort += portsPerFinder;
        portFinders[i] = pf;
        return mgmtTrapPort;
    }

    /**
     * Initialize the numShards, numStorageNodesInPool, and totalRepFactor
     * fields.
     */
    private void initNumShards() {
        if (totalPrimaryRepFactor != 0) {
            return;
        }

        if (numStorageNodesInPool == 0) {
            numStorageNodesInPool = numStorageNodes;
        }
        for (final ZoneInfo zone : zones) {
            if (zone.zoneType == DatacenterType.PRIMARY) { // RESOLVE why count secondaries
               totalPrimaryRepFactor += zone.repFactor;
            }
            totalRepFactor += zone.repFactor;
        }
        int totalPoolCapacity = 0;
        if (capacities == null) {
            totalPoolCapacity = numStorageNodesInPool * capacity;
        } else {
            for (int nodeCapacity : capacities) {
                totalPoolCapacity += nodeCapacity;
            }
        }
        if (totalPoolCapacity < totalRepFactor) {
            throw new IllegalStateException(
                "SN pool capacity is too low: need " + totalRepFactor +
                ", found " + totalPoolCapacity);
        }
        // RESOLVE the number of shards takes into account secondaries?
        numShards = totalPoolCapacity / totalRepFactor;
    }

    private void initSNMaps(StorageNodeId snId,
                            PortFinder pf,
                            StorageNodeAgent sna) {
        snaMap.put(snId, sna);
        portFinderMap.put(snId, pf);
        snToRNs.put(snId, new ArrayList<RepNodeId>());
        snToANs.put(snId, new ArrayList<ArbNodeId>());
    }

    private void verbose(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String newRootDir) {
        rootDir = newRootDir;
    }

    public String getHostname() {
        return hostname;
    }

    public String getStoreName() {
        return storeName;
    }

    public int getRegistryPort(StorageNodeId snId) {
        PortFinder pf = portFinderMap.get(snId);
        if (pf == null) {
            throw new IllegalStateException("No SNA for id " + snId);
        }
        return pf.getRegistryPort();
    }

    public LoginManager getSNALoginManager(int id) {
        if (!secure) {
            return null;
        }
        return getStorageNodeAgent(id).getLoginManager();
    }

    public LoginManager getAdminLoginManager() {
        if (!secure) {
            return null;
        }
        return adminLoginMgr;
    }

    public void setRestoreSnapshot(String snapshotName) {
        restoreSnapshotName = snapshotName;
    }

    public void setUpdateConfig(String updateConfig) {
        this.updateConfig = updateConfig;
    }

    public void start() throws Exception {
        start(true);
    }

    public void start(boolean isCreateDefaultUser)
        throws Exception {

        initStorageNodes();
        LoginManager loginMgr = snas[0].getLoginManager();
        if (secure) {
            verbose("Get anonymous admin login");
            loginMgr = new DelayedALM(
                new String[] { snas[0].getHostname() + ":" +
                               snas[0].getRegistryPort() },
                null);
            adminLoginMgr = loginMgr;
            TestUtils.waitForSecurityStartUp(
                snas[0].getHostname(), snas[0].getRegistryPort());
        }

        verbose("Creating plans for store " + storeName);
        cs = ServiceUtils.waitForAdmin(snas[0].getHostname(),
                                       snas[0].getRegistryPort(),
                                       loginMgr,
                                       10, ServiceStatus.RUNNING, logger);
        csIndex = 0;

        /*
         * If we are configured for security, upgrade to a real login
         */
        cs.configure(storeName);

        /*
         * Deploy Datacenters.
         */
        int zoneId = 1;
        for (final ZoneInfo zone : zones) {
            int planId = cs.createDeployDatacenterPlan(
                "DCPlan" + zoneId, "Zone" + zoneId, zone.repFactor,
                zone.zoneType, zone.allowArbiters, false);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
            zoneId++;
        }

        /*
         * Set policy map if non-null or if default JE properties were
         * specified
         */
        final ParameterMap mergedPolicyMap =
            mergeParameterMapDefaults(policyMap);
        if (mergedPolicyMap != null) {
            cs.setPolicies(mergedPolicyMap);
        }

        /*
         * Deploy first SN.
         */
        int planId = cs.createDeploySNPlan
            ("Deploy SN", new DatacenterId(1), snas[0].getHostname(),
             snas[0].getRegistryPort(), "comment");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
        initSNMaps(snas[0].getStorageNodeId(), portFinders[0], snas[0]);

        /*
         * Deploy admin
         */
        StorageNodeId adminSNId = snas[0].getStorageNodeId();
        planId = cs.createDeployAdminPlan
            ("Deploy admin",
             adminSNId);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
        snsWithAdmins.add(snas[0].getStorageNodeId());

        /*
         * Need a storage pool for the store deployment.
         */
        cs.addStorageNodePool(STORAGE_NODE_POOL_NAME);
        cs.addStorageNodeToPool(STORAGE_NODE_POOL_NAME,
                                snas[0].getStorageNodeId());

        /*
         * Deploy the rest of the Storage Nodes
         */
        verbose("Deploying the storage nodes and admin replicas");
        adminDeployed.set(true);
        zoneId = 1;
        int zoneRemainingCapacity = zones.get(zoneId-1).repFactor * numShards;
        for (int i = 1; i < snas.length; i++) {
            if (capacities == null) {
                zoneRemainingCapacity -= capacity;
            } else {
                zoneRemainingCapacity -= capacities[i-1];
            }
            if ((zoneRemainingCapacity <= 0) && (zoneId < zones.size())) {
                zoneId++;
                zoneRemainingCapacity =
                    zones.get(zoneId-1).repFactor * numShards;
            }

            planId = cs.createDeploySNPlan
                ("Deploy SN", new DatacenterId(zoneId), snas[i].getHostname(),
                 snas[i].getRegistryPort(), "comment");
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
            if (i < numStorageNodesInPool) {
                cs.addStorageNodeToPool(STORAGE_NODE_POOL_NAME,
                                        snas[i].getStorageNodeId());
            } else if ((zoneId < zones.size()) ||
                       (zoneRemainingCapacity > 0)) {
                throw new IllegalStateException(
                    "Pool is too small to provide sufficient SN capacity for" +
                    " all zones.  Remaining zones: " +
                    (zones.size() - zoneId) +
                    ", remaining rep factor for current zone: " +
                    zoneRemainingCapacity);
            }

            initSNMaps(snas[i].getStorageNodeId(), portFinders[i], snas[i]);

            /*
             * Create Admins on the SNs specified by adminLocation.
             */
            adminSNId = snas[i].getStorageNodeId();
            if (adminLocations.contains(adminSNId.getStorageNodeId())) {
                verbose("Deploying Admin Replica");
                adminSNId = snas[i].getStorageNodeId();
                planId = cs.createDeployAdminPlan(
                    "Deploy Admin",
                    adminSNId,
                    null /* Default admin type to zone type */);
                cs.approvePlan(planId);
                cs.executePlan(planId, false);
                cs.awaitPlan(planId, 0, null);
                cs.assertSuccess(planId);
                snsWithAdmins.add(snas[i].getStorageNodeId());
            }
        }

        /*
         * The store
         */
        verbose("Deploying the store");
        cs.createTopology("_CreateStoreTopo",
                          STORAGE_NODE_POOL_NAME,
                          numPartitions,
                          false);
        planId = cs.createDeployTopologyPlan("Deploy CreateStore",
                                             "_CreateStoreTopo", null);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verbose("Done with plans, waiting for RepNodes");

        /*
         * Wait for the RepNodes to come up.  Store the RepNodeAdminAPI
         * interfaces for later retrieval.
         */
        Topology topo = cs.getTopology();
        List<RepNode> repNodes = topo.getSortedRepNodes();
        ServiceStatus[] target = {ServiceStatus.RUNNING};
        int i = 0;
        rns = new RepNodeAdminAPI[repNodes.size()];
        rnTest = new RemoteTestAPI[repNodes.size()];
        for (RepNode rn : repNodes) {
            RegistryUtils ru = new RegistryUtils(topo, null, logger);
            RepNodeId rnId = rn.getResourceId();
            rns[i] =
                ServiceUtils.waitForRepNodeAdmin(topo, rnId,
                                                 snas[0].getLoginManager(),
                                                 5, target, logger);
            snToRNs.get(rn.getStorageNodeId()).add(rnId);
            rnTest[i++] = ru.getRepNodeTest(rnId);
        }

        Set<ArbNodeId> anids = topo.getArbNodeIds();
        i = 0;
        ans = new ArbNodeAdminAPI[anids.size()];
        for (ArbNodeId anId : anids) {
            ArbNode an = topo.get(anId);
            StorageNode sn = topo.get(an.getStorageNodeId());
            ans[i++] =
                ServiceUtils.waitForArbNodeAdmin(topo.getKVStoreName(),
                                                 sn.getHostname(),
                                                 sn.getRegistryPort(),
                                                 anId,
                                                 sn.getStorageNodeId(),
                                                 snas[0].getLoginManager(),
                                                 5,
                                                 target,
                                                 logger);

            snToANs.get(an.getStorageNodeId()).add(anId);
        }



        /*
         * User security
         */
        if (secure && secureUsers.size() > 0) {
            verbose("Create users");

            for (SecureUser user : secureUsers) {
                planId = createUser(
                    user.getUsername(), user.getPassword(), user.getIsAdmin());
            }

            verbose("Done creating users");
        }

        if (secure && isCreateDefaultUser) {
            createDefaultUser();
        }

        verbose("Store deployment complete");
    }

    /**
     * Add an SN having no RN to the store, return its registry port number.
     * In case of adding SNs to an initially single SN store, first call
     * {@link CreateStore#setAddSNToInitiallySingleSNStore} to set the flag.
     */
    public int addSN(int zoneId, boolean containsAdmin)
        throws Exception {

        if (zoneId > zones.size()) {
            throw new IllegalArgumentException("Provided zone ID " + zoneId +
                " is greater than number of existing zones " + zones.size());
        }
        int i = numStorageNodes;
        numStorageNodesInPool = ++numStorageNodes;
        StorageNodeAgent[] newSnas = Arrays.copyOf(snas, i+1);
        snas = newSnas;
        PortFinder[] newPortFinders = Arrays.copyOf(portFinders, i+1);
        portFinders = newPortFinders;
        verbose("Creating a storage nodes in root " + rootDir);

        initStorageNode(i, true, 0);

        int planId = cs.createDeploySNPlan
            ("Deploy SN", new DatacenterId(zoneId), snas[i].getHostname(),
             snas[i].getRegistryPort(), "comment");
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        initSNMaps(snas[i].getStorageNodeId(), portFinders[i], snas[i]);
        if (containsAdmin) {
            verbose("Deploying Admin Replica");
            StorageNodeId adminSNId = snas[i].getStorageNodeId();
            planId = cs.createDeployAdminPlan(
                "Deploy Admin",
                adminSNId,
                null /* Default admin type to zone type */);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
            cs.assertSuccess(planId);
            snsWithAdmins.add(snas[i].getStorageNodeId());
        }
        return portFinders[i].getRegistryPort();
    }

    /**
     * Starts an aggregation service in the current VM. Returns the instance.
     * If wait is true, this method will not return until the AS has started
     * polling the store.
     */
    public AggregationService startAggregationService(int throughputHistorySec,
                                                    int throughputPollPeriodSec,
                                                    boolean wait)
            throws Exception {
        if (as != null) {
            throw new IllegalStateException("AggregationService has already" +
                                            " been started");
        }

        final String[] hostPorts = new String[1];
        hostPorts[0] = hostname + ":" + getRegistryPort();

        as = AggregationService.
                createAggregationService(storeName, hostPorts,
                                         throughputHistorySec,
                                         throughputPollPeriodSec,
                                         1000 /*tableSizePollPeriodSec*/,
                                         1000 /*peakThroughputCollectionPeriodSec*/,
                                         1 /*peakThroughputTTLDay*/,
                                         2 /*maxThreads*/);

        if (wait) {
            /* Wait for the AS to start polling */
            assertTrue("Awaiting Aggregation Service startup",
                       new PollCondition(500, 5000) {
                           @Override
                           protected boolean condition() {
                               return as.getPollCount() > 0L;
                           }
                       }.await());
        }
        return as;
    }

    /**
     * Stops the aggregation service is one is running.
     */
    public void stopAggregationService() {
        if (as != null) {
            as.stop();
            as = null;
        }
    }

    /**
     * Add '-security <default user login path>' if security is enabled.
     */
    public String[] maybeAddSecurityFlag(String... args) {
        if (!secure) {
            return args;
        }
        final String[] result = Arrays.copyOf(args, args.length + 2);
        result[result.length - 2] = "-security";
        result[result.length - 1] = defaultUserLogin.getPath();
        return result;
    }

    /**
     * Add '-security <default user login path>' if security is enabled.
     */
    public List<String> maybeAddSecurityFlag(List<String> cmd) {
        if (secure) {
            cmd.add("-security");
            cmd.add(defaultUserLogin.getPath());
        }
        return cmd;
    }


    /*
     * Statistics gathering unit test code need to use internal login store
     * handle, just like the product code does. Otherwise the check of external
     * user accessing internal system table will fail the unit test.
     */
    public KVStore getInternalStoreHandle(KVStore store)
        throws KVStoreException {
        if (!secure) {
            return store;
        }
        /*
         * The new built up internal store handle share the same topology with
         * the passed in store handle.
         */
        TopologyManager topoMgr =
            ((KVStoreImpl) store).getDispatcher().getTopologyManager();
        LoginManager internalLoginMgr = new InternalLoginManager(
            new TopoTopoResolver(
                new TopoTopoResolver.TopoMgrTopoHandle(topoMgr),
                new TopologyResolver.SNInfo(getHostname(),
                                            getRegistryPort(),
                                            getStorageNodeIds()[0]),
                                            logger),
            logger);
        /* Fake client Id */
        ClientId clientId = new ClientId(System.currentTimeMillis());
        /*
         * To build an internal store handle, the security property need to set
         * as no user name property, no password property, for internal login.
         */
        final KVStoreConfig config =
            new KVStoreConfig(getStoreName(),
                              getHostname() + ":" + getRegistryPort());
        Properties props = new Properties();
        props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                  KVSecurityConstants.SSL_TRANSPORT_NAME);
        props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                  getTrustStore().getPath());
        config.setSecurityProperties(props);
        RequestDispatcher dispatcher =
            RequestDispatcherImpl.createForClient(
                config, clientId, internalLoginMgr,
                ((KVStoreImpl) store).getExceptionHandler(),
                logger);

        KVStore internalStore = new KVStoreImpl(
            logger, dispatcher, config, internalLoginMgr, null);

        return KVStoreImpl.makeInternalHandle(internalStore);
    }

    /*
     * Create default user and its login file.
     */
    private void createDefaultUser() throws Exception {

        createUser(defaultUser, defaultUserPass, true);

        grantRoles(defaultUser, RoleInstance.READWRITE_NAME);

        PasswordManager pwdMgr;
        try {
            pwdMgr = PasswordManager.load(FILE_STORE_MANAGER_CLASS);
        } catch (Exception e) {
            throw new RuntimeException(
                "Fail to load class: " + FILE_STORE_MANAGER_CLASS, e);
        }
        final File secDir = new File(getRootDir(0), secOpts.getSecurityDir());
        final File passStore = new File(secDir, defaultPassStore);
        defaultUserLogin = new File(secDir, defaultLoginFile);
        if (!passStore.exists()) {
            final PasswordStore pwdStore = pwdMgr.getStoreHandle(passStore);
            try {
                pwdStore.create(null);
                pwdStore.setSecret(defaultUser, defaultUserPass.toCharArray());
                pwdStore.save();
                pwdStore.discard();
            } catch (IOException e) {
                throw new RuntimeException(
                   "Fail to create password store: " + defaultUserPass, e);
            }

        }
        defaultUserLoginProps = new Properties();
        defaultUserLoginProps.put(
            KVSecurityConstants.AUTH_USERNAME_PROPERTY, defaultUser);
        defaultUserLoginProps.setProperty(
            KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
            passStore.getPath());
        addTransportProps(defaultUserLoginProps);

        if (!defaultUserLogin.exists()) {
            try {
                ConfigUtils.storeProperties(
                    defaultUserLoginProps, null, defaultUserLogin);
            } catch (IOException e) {
                throw new RuntimeException(
                    "Fail to create login file: " + defaultUserLogin.getPath(),
                        e);
            }
        }
        /*
         * Hide the output in outStream.
         */
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream output = new PrintStream(outStream);
        CommandShell shell = new CommandShell(System.in, output);
        shell.connectAdmin(getHostname(), getRegistryPort(),
                           defaultUser, defaultUserLogin.getPath());
        cs = shell.getAdmin();
    }

    public File getDefaultUserLoginFile() {
        return defaultUserLogin;
    }

    public LoginManager getRepNodeLoginManager() {
        if (!secure) {
            return null;
        }
        LoginCredentials creds =
            KVStoreLogin.makeLoginCredentials(defaultUserLoginProps);
        return KVStoreLogin.getRepNodeLoginMgr(getHostname(),
                                               getRegistryPort(),
                                               creds, storeName);
    }

    public String getDefaultUserLoginPath() {
        if (!secure) {
            return null;
        }
        return defaultUserLogin.getPath();
    }

    public LoginManager getSNALoginManager(StorageNodeId id) {
        if (!secure) {
            return null;
        }
        return getStorageNodeAgent(id).getLoginManager();
    }

    public KVStoreConfig createKVConfig() {
        return createKVConfig(secure);
    }

    public KVStoreConfig createKVConfig(boolean requestSecure) {
        final KVStoreConfig config =
            new KVStoreConfig(storeName, hostname + ":" + getRegistryPort());
        if (requestSecure) {
            config.setSecurityProperties(defaultUserLoginProps);
        }
        return config;
    }

    public void setSecInHadoopConfiguration(Configuration conf) {
        if (!secure) {
            return;
        }
        conf.set(KVSecurityConstants.SECURITY_FILE_PROPERTY,
                 defaultUserLogin.getPath());
        conf.set(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                 getTrustStore().getPath());
        conf.set(KVSecurityConstants.AUTH_USERNAME_PROPERTY, defaultUser);
        conf.set(KVSecurityConstants.AUTH_PWDFILE_PROPERTY,
                 defaultUserLoginProps.getProperty(
                     KVSecurityConstants.AUTH_PWDFILE_PROPERTY));
    }

    public void setSecInHadoopProperties(Properties props) {
        if (!secure) {
            return;
        }
        props.setProperty(KVSecurityConstants.SECURITY_FILE_PROPERTY,
                          defaultUserLogin.getPath());
        props.setProperty(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                          getTrustStore().getPath());
        props.setProperty(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                          defaultUserLoginProps.getProperty(
                              KVSecurityConstants.AUTH_PWDFILE_PROPERTY));
    }
    public String getDefaultUserName() {
        if (!secure) {
            return null;
        }
        return defaultUser;
    }

    public CommandServiceAPI getAdmin() {
        return cs;
    }

    public int getAdminIndex() {
        return csIndex;
    }

    public RepNodeAdminAPI getRepNodeAdmin(int index) {
        return rns[index];
    }

    public ArbNodeAdminAPI getArbNodeAdmin(int index) {
        return ans[index];
    }

    public RemoteTestAPI[] getRepNodeTestAPI() {
        return rnTest;
    }

    /**
     * Get an a RepNode admin service handle using a specific login.
     */
    public RepNodeAdminAPI getRepNodeAdmin(RepNodeId rnId,
                                           String username,
                                           char[] password)
        throws Exception {

        final Topology topo = cs.getTopology();
        final StorageNodeId snId = topo.get(rnId).getStorageNodeId();
        final StorageNode sn = topo.get(snId);

        LoginManager loginMgr =
            new DelayedRLM(
                new String[] { sn.getHostname() + ":" + sn.getRegistryPort() },
                new PasswordCredentials(username, password),
                storeName);

        return ServiceUtils.waitForRepNodeAdmin(
            storeName, sn.getHostname(), sn.getRegistryPort(), rnId, snId,
            loginMgr, 5, new ServiceStatus[] { ServiceStatus.RUNNING },
            logger);
    }

    public RepNodeAdminAPI getRepNodeAdmin(RepNodeId rnId)
        throws Exception {

        return getRepNodeAdmin(rnId, 5 /* timeoutSec */);
    }

    public RepNodeAdminAPI getRepNodeAdmin(RepNodeId rnId, int timeoutSec)
        throws Exception {

        final Topology topo = cs.getTopology();

        return ServiceUtils.waitForRepNodeAdmin(
            topo, rnId, snas[0].getLoginManager(), timeoutSec,
            new ServiceStatus[] { ServiceStatus.RUNNING }, logger);
    }

    public ArbNodeAdminAPI getArbNodeAdmin(ArbNodeId anId)
        throws Exception {

        return getArbNodeAdmin(anId, 5 /* timeoutSec */);
    }

    public ArbNodeAdminAPI getArbNodeAdmin(ArbNodeId anId, int timeoutSec)
        throws Exception {

        final Topology topo = cs.getTopology();

        return ServiceUtils.waitForArbNodeAdmin(
            topo, anId, snas[0].getLoginManager(), timeoutSec,
            new ServiceStatus[] { ServiceStatus.RUNNING }, logger);
    }

    /**
     * Get an a command service handle using a specific login.
     */
    public CommandServiceAPI getAdmin(String username, char[] password)
        throws Exception {

        LoginManager newLoginMgr =
            new DelayedALM(
                new String[] { snas[0].getHostname() + ":" +
                               snas[0].getRegistryPort() },
                new PasswordCredentials(username, password));

        return ServiceUtils.waitForAdmin(snas[0].getHostname(),
                                         snas[0].getRegistryPort(),
                                         newLoginMgr,
                                         10, ServiceStatus.RUNNING, logger);
    }

    public CommandServiceAPI getAdmin(int index)
        throws Exception {

        return getAdmin(index, 10);
    }

    public CommandServiceAPI getAdmin(int index, int timeoutSec)
        throws Exception {

        StorageNodeAgent sna = snas[index];
        return ServiceUtils.waitForAdmin(sna.getHostname(),
                                         sna.getRegistryPort(),
                                         sna.getLoginManager(),
                                         timeoutSec, ServiceStatus.RUNNING,
                                         logger);
    }

    public CommandServiceAPI getAdminMaster() {

        /**
         * Retry if a failover is happening.
         */
        for (int i = 0; i < 10; i++ ) {
            for (int j = 0; j < snas.length; j++) {
                try {
                    CommandServiceAPI newcs = getAdmin(j);
                    AdminStatus adminStatus = newcs.getAdminStatus();
                    if (adminStatus.getIsAuthoritativeMaster()) {
                        cs = newcs;
                        csIndex = j;
                        return newcs;
                    }
                } catch (Exception ignored) {
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        return null;
    }

    public void shutdown() {
        shutdown(true);
    }

    public void shutdown(boolean force) {
        stopAggregationService();

        if (snas != null) {
            for (int i = 0; i < snas.length; i++) {
                if (snas[i] != null) {
                    snas[i].shutdown(true, force);
                    snas[i] = null;
                }
            }
        }
        snas = null;
    }

    public void restart()
        throws Exception {

        verbose("Restarting store");
        shutdown(true);
        snas = new StorageNodeAgent[numStorageNodes];
        for (int i = 0; i < numStorageNodes; i++) {
            final String snaRootDir = getRootDir(i);
            if (restoreSnapshotName != null) {
                snas[i] = StorageNodeUtils.startSNA(snaRootDir,
                                                    "config" + i + ".xml",
                                                    useThreads, false,
                                                    false,
                                                    restoreSnapshotName,
                                                    updateConfig);
            } else {
                snas[i] = StorageNodeUtils.startSNA(snaRootDir,
                                                    "config" + i + ".xml",
                                                    useThreads, false);
            }
        }

        Thread.sleep(5000);

        /*
         * Now reset some additional state in this class.
         */
        cs = getAdminMaster();

        /*
         * Wait for the RepNodes to come up.  Store the RepNodeAdminAPI
         * interfaces for later retrieval.
         */
        Topology topo = cs.getTopology();
        List<RepNode> repNodes = topo.getSortedRepNodes();
        ServiceStatus[] target = {ServiceStatus.RUNNING};
        int i = 0;
        for (RepNode rn : repNodes) {
            RepNodeId rnId = rn.getResourceId();
            rns[i++] = ServiceUtils.waitForRepNodeAdmin(topo, rnId,
                snas[0].getLoginManager(), 20, target, logger);
            snToRNs.get(rn.getStorageNodeId()).add(rnId);
        }

        Set<ArbNodeId> anids = topo.getArbNodeIds();
        i = 0;
        ans = new ArbNodeAdminAPI[anids.size()];
        for (ArbNodeId anId : anids) {
            ArbNode an = topo.get(anId);
            StorageNode sn = topo.get(an.getStorageNodeId());
            ans[i++] =
                ServiceUtils.waitForArbNodeAdmin(topo.getKVStoreName(),
                                                 sn.getHostname(),
                                                 sn.getRegistryPort(),
                                                 anId,
                                                 sn.getStorageNodeId(),
                                                 snas[0].getLoginManager(),
                                                 20,
                                                 target,
                                                 logger);

            snToANs.get(an.getStorageNodeId()).add(anId);
        }
        verbose("Store restart complete");
    }

    public void shutdownSNA(int snaIdx, boolean force) {
        snas[snaIdx].shutdown(true, force);
    }

    public void startSNA(int snaIdx)
        throws Exception {

        startSNA(snaIdx, false /* disableServices */);
    }

    public void startSNA(int snaIdx, boolean disableServices)
        throws Exception {

        startSNA(snaIdx, disableServices, !disableServices /* awaitServices */);
    }

    public void startSNA(int snaIdx,
                         boolean disableServices,
                         boolean awaitServices)
        throws Exception {

        final String snaRootDir = getRootDir(snaIdx);
        if (restoreSnapshotName != null) {
            snas[snaIdx] = StorageNodeUtils.startSNA(
                snaRootDir, "config" + snaIdx + ".xml", useThreads,
                false /* createAdmin */, disableServices,
                restoreSnapshotName, updateConfig);
        } else {
            snas[snaIdx] = StorageNodeUtils.startSNA(
                snaRootDir, "config" + snaIdx + ".xml", useThreads,
                false /* createAdmin */, disableServices);
        }
        Thread.sleep(5000);

        StorageNodeId snaId = snas[snaIdx].getStorageNodeId();
        snaMap.put(snaId, snas[snaIdx]);
        LoginManager loginMgr = snas[snaIdx].getLoginManager();

        getAdminMaster();

        /*
         * Now reset some additional state in this class.
         */
        if (snaIdx == 0) {
            snsWithAdmins.add(snaId);
        }

        /*
         * Wait for the RepNodes to come up.
         */
        if (awaitServices) {
            Topology topo = cs.getTopology();
            List<RepNode> repNodes = topo.getSortedRepNodes();
            ServiceStatus[] target = {ServiceStatus.RUNNING};
            snToRNs.put(snaId, new ArrayList<RepNodeId>());
            int i = 0;
            for (RepNode rn : repNodes) {
                if (rn.getStorageNodeId().equals(snaId)) {
                    RepNodeId rnId = rn.getResourceId();
                    rns[i] = ServiceUtils.waitForRepNodeAdmin(
                        topo, rnId, loginMgr, 30, target, logger);
                    snToRNs.get(snaId).add(rnId);
                }
                i++;
            }
        }
    }

    public PortFinder[] getPortFinders() {
        return portFinders;
    }

    public int getRegistryPort() {
        return getRegistryPort(csIndex);
    }

    /**
     * @param index identifies an SN
     */
    public int getRegistryPort(int index) {
        return portFinders[index].getRegistryPort();
    }

    public PortFinder getPortFinder(StorageNodeId snId) {
        return portFinderMap.get(snId);
    }

    public StorageNodeAgent getStorageNodeAgent(int index) {
        return snas[index];
    }

    public StorageNodeId[] getStorageNodeIds() {
        StorageNodeId[] snids = new StorageNodeId[snas.length];
        for (int i = 0; i < snas.length; i++) {
            snids[i] = snas[i].getStorageNodeId();
        }
        return snids;
    }

    /*
     * The following methods are used to query the topology of the store for
     * testing reasons.
     */

    /**
     * Return true if this SN hosts an admin.
     */
    public boolean hasAdmin(StorageNodeId snId) {
        return snsWithAdmins.contains(snId);
    }

    /**
     * Returns the admin ID for specified SN, or null if the specified SN does
     * not have an admin.
     */
    public AdminId getAdminId(int index) {
        if (!snsWithAdmins.contains(snas[index].getStorageNodeId())) {
            return null;
        }
        int adminId = 0;
        for (int i = 0; i <= index; i++) {
            final StorageNodeId snId = snas[i].getStorageNodeId();
            if (snsWithAdmins.contains(snId)) {
                adminId++;
            }
        }
        return new AdminId(adminId);
    }

    /**
     * Returns the storage Id of the SN that hosts the admin of adminId, null
     * if no match.
     */
    public StorageNodeId getStorageNodeId(AdminId adminId) {
        int count = 0;
        for (StorageNodeAgent sna : snas) {
            StorageNodeId snId = sna.getStorageNodeId();
            if (!snsWithAdmins.contains(snId)) {
                continue;
            }
            count ++;
            if ((new AdminId(count)).equals(adminId)) {
                return snId;
            }
        }
        return null;
    }

    /**
     * Support for security: grant roles for the given user.
     */
    public void grantRoles(String user, String... roles)
        throws Exception {

        final Set<String> roleSet = new HashSet<String>();
        Collections.addAll(roleSet, roles);
        final int planId =
            cs.createGrantPlan("Grant roles", user, roleSet);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    /**
     * Support for security: remove the specified roles from the given user.
     */
    public void revokeRoles(String user, String... roles)
        throws Exception {

        final Set<String> roleSet = new HashSet<String>();
        Collections.addAll(roleSet, roles);
        final int planId =
            cs.createRevokePlan("Revoke roles", user, roleSet);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }


    /**
     * Return the number of RNs that this SN hosts.
     */
    class CreateStoreParser extends CommandParser {

        private static final String THREADS_FLAG = "-threads";
        private static final String PARTITION_FLAG = "-partitions";
        private static final String RF_FLAG = "-rf";
        private static final String NUM_SN_FLAG = "-num_sns";
        private static final String CAPACITY_FLAG = "-capacity";
        private static final String SECURE_FLAG = "-secure";
        private static final String USERDEF_FLAG = "-userdef";
        private static final String EXTAUTH_FLAG = "-extauth";

        /**
         * If specified, the next argument should be the name of a file
         * containing a serialized SecureOpts instance for use in configuring
         * security for the store.
         */
        private static final String SECOPTS_FLAG = "-secopts";

        private boolean shutdown;
        MgmtUtil.ConfigParserHelper mgmtParser;

        public CreateStoreParser(String[] args) {
            super(args);
            shutdown = false;
            mgmtParser = new MgmtUtil.ConfigParserHelper(this);
        }

        public boolean getShutdown() {
            return shutdown;
        }

        @Override
        public void verifyArgs() {
            if (getRootDir() == null) {
                missingArg(ROOT_FLAG);
            }
            if (!shutdown) {
                if (getStoreName() == null) {
                    missingArg(STORE_FLAG);
                }
                if (getRegistryPort() == 0) {
                    missingArg(PORT_FLAG);
                }
            }
        }

        @Override
        public boolean checkArg(String arg) {
            if (arg.equals(StorageNodeAgent.SHUTDOWN_FLAG)) {
                shutdown = true;
                return true;
            }
            if (arg.equals(THREADS_FLAG)) {
                useThreads = true;
                return true;
            }
            if (arg.equals(NUM_SN_FLAG)) {
                numStorageNodes = nextIntArg(arg);
                return true;
            }
            if (arg.equals(RF_FLAG)) {
                zones = ZoneInfo.primaries(nextIntArg(arg));
                return true;
            }
            if (arg.equals(PARTITION_FLAG)) {
                numPartitions = nextIntArg(arg);
                return true;
            }
            if (arg.equals(CAPACITY_FLAG)) {
                capacity = nextIntArg(arg);
                return true;
            }
            if (arg.equals(SECURE_FLAG)) {
                secure = true;
                return true;
            }
            if (arg.equals(USERDEF_FLAG)) {
                return addUser(nextArg(arg));
            }
            if (arg.equals(EXTAUTH_FLAG)) {
                userExternalAuth = nextArg(arg);
                return true;
            }
            if (arg.equals(SECOPTS_FLAG)) {
                try (ObjectInputStream in =
                     new ObjectInputStream(
                         new FileInputStream(nextArg(arg)))) {
                    secOpts = (SecureOpts) in.readObject();
                } catch (IOException|ClassNotFoundException e) {
                    throw new IllegalStateException(
                        "Problem reading SecureOpts object: " + e, e);
                }
                return true;
            }
            if (mgmtParser.checkArg(arg)) {
                mgmtImpl = mgmtParser.getSelectedImplClass();
                return true;
            }
            return false;
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println("Usage: CreateStore " +
                               getRootUsage() + "\n\t" +
                               getStoreUsage() + "\n\t " +
                               getPortUsage() + "\n\t" +
                               optional(getHostUsage()) + "\n\t" +
                               optional(NUM_SN_FLAG + " <numStorageNodes>") +
                               "\n\t" +
                               optional(PARTITION_FLAG + " <numPartitions>") +
                               "\n\t" +
                               optional(RF_FLAG + " <replicationFactor>") +
                               "\n\t" +
                               optional(CAPACITY_FLAG + " <RNs per SN>") +
                               "\n\t" +
                               optional(SECURE_FLAG) +
                               "\n\t" +
                               optional(USERDEF_FLAG +
                                        " <username:<password>:<isAdmin>") +
                               "\n\t" +
                               optional(EXTAUTH_FLAG + " <kerberos>") +
                              "\n\t" +
                               optional(THREADS_FLAG) + "\n\t" +
                               optional(StorageNodeAgent.SHUTDOWN_FLAG) +
                               "\n\t" +
                               MgmtUtil.getMgmtUsage() +
                               "\n\t" +
                               getUsage());
            System.exit(1);
        }
    }

    /*
     * Return the bootstrap config file for the number SN
     */
    public File getBootstrapConfigFile(int snNum) {
        return new File(getRootDir(snNum) + File.separator + "config" +
                        snNum + ".xml");
    }

    public void shutdownStore(boolean force) {

        verbose("Shutting down store: " + storeName  +
                " in base rootDir: " + rootDir);
        for (int i = 0; i < numStorageNodes; i++) {
            final String snaRootDir = getRootDir(i);
            File configFile =
                new File(snaRootDir + File.separator + "config" + i + ".xml");
            verbose("Shutting down storage node " + i + " config file " +
                    configFile);
            if (!configFile.exists()) {
                System.err.println("Cannot find configuration file: " +
                                   configFile);
                return;
            }
            try {
                BootstrapParams bp = ConfigUtils.getBootstrapParams(configFile);
                String name =
                    RegistryUtils.bindingName
                    (bp.getStoreName(),
                     new StorageNodeId(bp.getStorageNodeId()).getFullName(),
                     RegistryUtils.InterfaceType.MAIN);
                verbose("Attempting to contact Storage Node : " +
                        bp.getHostname() + ":" + bp.getRegistryPort());
                StorageNodeAgentAPI snai =
                    RegistryUtils.getStorageNodeAgent
                    (bp.getHostname(), bp.getRegistryPort(), name,
                     snas[i].getLoginManager(), logger);
                System.err.println("Shutting down SNA " + i +
                                   ", config file: " + configFile);
                snai.shutdown(true, force);
            } catch (Exception e) {
                System.err.println("Exception in shutdown: " + e);
            }
        }
    }

    private boolean parseArgs(String[] args) {
        CreateStoreParser csp = new CreateStoreParser(args);
        csp.parseArgs();
        verbose = csp.getVerbose();
        rootDir = csp.getRootDir();
        storeName = csp.getStoreName();
        startPort = csp.getRegistryPort() - 1;
        if (csp.getHostname() != null) {
            hostname = csp.getHostname();
        }
        return csp.getShutdown();
    }

    public static void main(String[] args) throws Exception {

        CreateStore store = new CreateStore();
        boolean shutdown = store.parseArgs(args);
        if (shutdown) {
            store.shutdownStore(false);
        } else {
            store.initStorageNodes();
            store.start();
            System.out.println("CLI Admin on port " +
                               store.getRegistryPort());
        }
    }

    /**
     * Return the list of RNs that are hosted on this SN.
     */
    public List<RepNodeId> getRNs(StorageNodeId snId) {
        return snToRNs.get(snId);
    }

    /**
     * Return the number of RNs that are hosted on this SN.
     */
    public int numRNs(StorageNodeId snId) {
        return snToRNs.get(snId).size();
    }

    /**
     * Return the number of ANs that are hosted on this SN.
     */
    public int numANs(StorageNodeId snId) {
        return snToANs.get(snId).size();
    }


    public StorageNodeAgent getStorageNodeAgent(StorageNodeId snId) {
        return snaMap.get(snId);
    }

    public void addTransportProps(Properties props) {
        if (secOpts!= null && secOpts.noSSL()) {
            props.put(KVSecurityConstants.TRANSPORT_PROPERTY, "clear");
        } else {
            props.put(KVSecurityConstants.TRANSPORT_PROPERTY,
                      KVSecurityConstants.SSL_TRANSPORT_NAME);
            props.put(KVSecurityConstants.SSL_TRUSTSTORE_TYPE_PROPERTY,
                      "PKCS12");
            props.put(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY,
                      getTrustStore().getPath());
        }
    }

    public KVStore getSecureStore(LoginCredentials creds) {
        final KVStoreConfig kvConfig =
            new KVStoreConfig(storeName, hostname + ":" + getRegistryPort());
        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        return KVStoreFactory.getStore(kvConfig, creds, null);
    }

    public KVStore getSecureStore(LoginCredentials creds, int
        sgAttrCacheTimeoutMs) {
        final KVStoreConfig kvConfig =
            new KVStoreConfig(storeName, hostname + ":" + getRegistryPort());
        Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);
        kvConfig.setSGAttrsCacheTimeout(sgAttrCacheTimeoutMs);

        return KVStoreFactory.getStore(kvConfig, creds, null);
    }

    /**
     * Accepts a user definition in the form username:password:isAdmin in
     * support of main().
     * For example:  "joe:mysecret:true".
     */
    private boolean addUser(String userDef) {
        final String[] userParts = userDef.split(":");
        if (userParts.length != 3) {
            System.err.println("User definition must be user:pw:isAdmin");
            return false;
        }
        final boolean isAdmin = Boolean.parseBoolean(userParts[2]);
        addUser(userParts[0], userParts[1], isAdmin);
        return true;
    }

    private int createUser(String userName,
                           String password,
                           boolean isAdmin) throws RemoteException {
        int planId = cs.createCreateUserPlan("_CreateUser_" + userName,
                                             userName,
                                             true,
                                             isAdmin,
                                             password.toCharArray());
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
        return planId;
    }

    public static String getDefaultUser() {
        return defaultUser;
    }

    public static String getDefaultUserPass() {
        return defaultUserPass;
    }

    /**
     * Returns the parameter map that should be used to set store policy or
     * service parameters given the desired test-specific policy map passed as
     * an argument. This method merges in any additional default parameters.
     * Returns null if no parameters are needed. If map is null, then no
     * test-specific parameters will be used, but the return value may still
     * return a map that represents default parameters. Does not modify the
     * supplied map.
     */
    public static ParameterMap mergeParameterMapDefaults(ParameterMap map) {
        ParameterMap mergedMap = null;

        /* Set JE_MISC defaults */
        if ((JE_DEFAULT_PROPERTIES != null) &&
            !JE_DEFAULT_PROPERTIES.equals("")) {
            if (map == null) {
                if (mergedMap == null) {
                    mergedMap = new ParameterMap();
                }
                mergedMap.setParameter(ParameterState.JE_MISC,
                                       JE_DEFAULT_PROPERTIES);
            } else {
                if (mergedMap == null) {
                    mergedMap = map.copy();
                }
                String jeMiscDefault = JE_DEFAULT_PROPERTIES;
                if (!jeMiscDefault.endsWith(";")) {
                    jeMiscDefault += ";";
                }
                final String jeMisc =
                    map.getOrDefault(ParameterState.JE_MISC).asString();
                mergedMap.setParameter(ParameterState.JE_MISC,
                                       mergeJeParams(jeMiscDefault, jeMisc));
            }
        }

        return (mergedMap != null) ? mergedMap : map;
    }

    /**
     * Applies the consumer to each JE parameter key and value specified in the
     * string.
     */
    public static void forEachJeParams(String params,
                                       BiConsumer<String, String> func) {
        for (String item : params.split(";")) {
            item = item.trim();
            if ("".equals(item)) {
                continue;
            }
            final String[] split = item.split("[ =]", 2);
            func.accept(split[0], split[1]);
        }
    }

    /** Returns a String representing JE parameters in a canonical form. */
    public static String canonicalJeParams(String params) {
        final StringBuilder sb = new StringBuilder();
        try (final Formatter fmt = new Formatter(sb)) {
            forEachJeParams(params, (k, v) -> fmt.format("%s %s;", k, v));
            return sb.toString();
        }
    }

    /**
     * Merges two sets of JE parameters, represented as string values, and
     * returns the result with any duplicate settings removed.
     */
    public static String mergeJeParams(String initial, String update) {
        final StringBuilder sb = new StringBuilder();
        try (final Formatter fmt = new Formatter(sb)) {
            final SortedMap<String, String> initialMap = new TreeMap<>();
            forEachJeParams(initial, initialMap::put);
            final SortedMap<String, String> updateMap = new TreeMap<>();
            forEachJeParams(update, updateMap::put);
            initialMap.forEach((k, v) -> {
                    if (!updateMap.containsKey(k)) {
                        fmt.format("%s %s;", k, v);
                    }
                });
            updateMap.forEach((k, v) -> fmt.format("%s %s;", k, v));
            return sb.toString();
        }
    }
}
