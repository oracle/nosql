/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy.util;

import static oracle.kv.impl.security.PasswordManager.FILE_STORE_MANAGER_CLASS;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.impl.admin.AdminStatus;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Start, stop, reconfigure nodes.
 */
public class CreateStore {

    private static final Logger logger =
        Logger.getLogger("oracle.kv.CreateStore");

    private static final int portsPerFinder = 20;
    private static final int haRange = 8;
    
    //In some cases it took more than 20 seconds.
    private static final int deployWaitSeconds =
    	Integer.getInteger("test.deploywaitseconds", 20);

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
    private int totalPrimaryRepFactor;
    private int totalRepFactor;
    private int numShards;
    private int startPort;
    private final int memoryMB;
    private StorageNodeAgent[] snas;
    private StorageNodeAgent[] expansionSnas;
    private PortFinder[] portFinders;
    private CommandServiceAPI cs;
    private int csIndex = -1;
    private RepNodeAdminAPI[] rns;
    private boolean useThreads;
    private boolean verbose;
    private String mgmtImpl;
    private ParameterMap policyMap;
    private Set<Parameter> extraParams;

    /* Hang on to per-SN information for unit test usage. */
    private final Map<StorageNodeId,PortFinder> portFinderMap =
        new HashMap<StorageNodeId,PortFinder>();

    private final Map<StorageNodeId,StorageNodeAgent> snaMap =
        new HashMap<StorageNodeId,StorageNodeAgent>();

    private final Map<StorageNodeId,List<RepNodeId>> snToRNs =
        new HashMap<StorageNodeId,List<RepNodeId>>();

    private final Set<StorageNodeId> snsWithAdmins =
        new HashSet<StorageNodeId>();

    /*
     * AdminDeployed and adminLocations provide a mechanism for placing admins
     * on particular SNs, for specific test cases. The SNs specified by the ids
     * in adminLocations should host Admins. Defaults to SNs 1, 2, 3.
     */
    private final AtomicBoolean adminDeployed = new AtomicBoolean(false);
    private Set<Integer> adminLocations = new HashSet<>();

    /** Information about a zone. */
    public static class ZoneInfo {
        public final int repFactor;
        public final DatacenterType zoneType;
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
        }
        public static List<ZoneInfo> primaries(int... replicationFactors) {
            final List<ZoneInfo> result =
                new ArrayList<ZoneInfo>(replicationFactors.length);
            for (int replicationFactor : replicationFactors) {
                result.add(new ZoneInfo(replicationFactor));
            }
            return result;
        }

        public ZoneInfo setMasterAffinity(boolean masterAffinity) {
            this.masterAffinity = masterAffinity;
            return this;
        }
    }

    public CreateStore(String rootDir,
                       String storeName,
                       int startPort,
                       int numStorageNodes,
                       int replicationFactor,
                       int numPartitions,
                       int capacity,
                       int memoryMB,
                       boolean useThreads,
                       String mgmtImpl)
        throws Exception {


        this.useThreads = useThreads;
        this.storeName = storeName;
        this.zones = ZoneInfo.primaries(replicationFactor);
        this.numPartitions = numPartitions;
        this.numStorageNodes = numStorageNodes;
        this.numStorageNodesInPool = numStorageNodes;
        this.startPort = startPort - 1;
        this.rootDir = rootDir;
        this.hostname = "localhost";
        this.capacity = capacity;
        this.memoryMB = memoryMB;
        this.mgmtImpl = null;
        this.extraParams = extraParams;
        cs = null;
        verbose = false;
        policyMap = null;

        /* Default SNs which host Admins */
        adminLocations.add(1);
        adminLocations.add(2);
        adminLocations.add(3);
    }

    public void setExpansionSnas(StorageNodeAgent[] expSnas) {
        expansionSnas = expSnas;
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

    public void setMasterAffinities(boolean masterAffinity) {
        for (ZoneInfo zone : zones) {
            zone.setMasterAffinity(masterAffinity);
        }
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
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
                zoneRemainingCapacity -= capacity;
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
                " storage nodes in root " + rootDir);

        for (int i = 0; i < numStorageNodes; i++) {
            PortFinder pf = new PortFinder(startPort, haRange, hostname);

            snas[i] = CreateStoreUtils.createUnregisteredSNA
                (rootDir,
                 pf,
                 capacity,
                 "config" + i + ".xml",
                 useThreads,
                 i == 0, /* Create Admin for first one */
                 memoryMB,
                 extraParams);

            verbose("Created Storage Node " + i + " on host:port " +
                    hostname + ":" + pf.getRegistryPort());
            startPort += portsPerFinder;
            portFinders[i] = pf;
        }
        verbose("Done creating storage nodes");
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
        int totalPoolCapacity = numStorageNodesInPool * capacity;
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

    public void start() throws Exception {

        initStorageNodes();

        verbose("Creating plans for store " + storeName);
        cs = CreateStoreUtils.waitForAdmin(snas[0].getHostname(),
                                       snas[0].getRegistryPort(),
                                       10, logger);
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
                zone.zoneType, false /* no arbiters */, false);
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
            zoneRemainingCapacity -= capacity;
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
        cs.awaitPlan(planId, deployWaitSeconds, TimeUnit.SECONDS);
        cs.assertSuccess(planId);

        verbose("Done with plans, waiting for RepNodes");

        /*
         * Wait for the RepNodes to come up.  Store the RepNodeAdminAPI
         * interfaces for later retrieval.
         */
        Topology topo = cs.getTopology();
        List<RepNode> repNodes = topo.getSortedRepNodes();

        int i = 0;
        rns = new RepNodeAdminAPI[repNodes.size()];
        for (RepNode rn : repNodes) {
            RegistryUtils ru = new RegistryUtils(topo, null, logger);
            RepNodeId rnId = rn.getResourceId();
            StorageNode sn = topo.get(rn.getStorageNodeId());
            rns[i] =
                CreateStoreUtils.waitForRepNodeAdmin(topo.getKVStoreName(),
                                                     sn.getHostname(),
                                                     sn.getRegistryPort(),
                                                     rnId,
                                                     null,
                                                     10, ServiceStatus.RUNNING,
                                                     logger);
            snToRNs.get(rn.getStorageNodeId()).add(rnId);
        }

        /* wait for system tables to be ready */
        new PollCondition(500, 40000) {
            @Override
            protected boolean condition() {
                try {
                    return cs.isStoreReady(true /* tableOnly */);
                } catch (RemoteException e) {
                    return false;
                }
            }
        }.await();

        verbose("Store deployment complete");
    }

    public KVStoreConfig createKVConfig() {
        return new KVStoreConfig(storeName, hostname + ":" + getRegistryPort());
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

    public CommandServiceAPI getAdmin(int index)
        throws Exception {

        return getAdmin(index, 10);
    }

    public CommandServiceAPI getAdmin(int index, int timeoutSec)
        throws Exception {

        StorageNodeAgent sna = snas[index];
        return CreateStoreUtils.waitForAdmin(sna.getHostname(),
                                             sna.getRegistryPort(),
                                             timeoutSec,
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
        shutdownSnas(snas, force);
        shutdownSnas(expansionSnas, force);
        snas = null;
        expansionSnas = null;
    }

    private void shutdownSnas(StorageNodeAgent[] snAgents, boolean force) {
        if (snAgents != null) {
            for (int i = 0; i < snAgents.length; i++) {
                if (snAgents[i] != null) {
                    snAgents[i].shutdown(true, force);
                    snAgents[i] = null;
                }
            }
        }
    }

    public void shutdownSNA(int snaIdx, boolean force) {
        snas[snaIdx].shutdown(true, force);
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

    /*
     * Return the bootstrap config file for the number SN
     */
    public File getBootstrapConfigFile(int snNum) {
        return new File(rootDir + File.separator + "config" + snNum + ".xml");
    }

    public void shutdownStore(boolean force) {

        verbose("Shutting down store: " + storeName  +
                " in rootDir: " + rootDir);
        for (int i = 0; i < numStorageNodes; i++) {
            File configFile =
                new File(rootDir + File.separator + "config" + i + ".xml");
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

    public StorageNodeAgent getStorageNodeAgent(StorageNodeId snId) {
        return snaMap.get(snId);
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
