/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util;

import static oracle.kv.impl.async.StandardDialogTypeFamily.STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import oracle.kv.KVStoreConfig;
import oracle.kv.KVVersion;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.TestProcessFaultHandler;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.OperationsStatsTracker;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.rep.RepNodeService.Params;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgentInterfaceResponder;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManager.SNInfo;
import oracle.kv.impl.sna.masterBalance.MasterBalanceManagerInterface;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.topo.util.TopoUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * Creates KV test configurations with different numbers of data centers,
 * storage nodes, replication factors, partitions, etc. Returns the XXXParams,
 * RepNodes, and RepHandlers.
 *
 * Meant to be used on the "remote" components of kvstore (SNA, RepNode).
 *
 * NOTE: this class in intended to be used for single-process testing of the
 * components created.  As a result the RepNodes created will have their JE
 * cache sizes set based on JVM heap/RepFactor.  If this is not sufficiently
 * accurate it'd be possible to just count RepNodes in the Topology.
 */
public class KVRepTestConfig {

    private final static LoginManager NULL_LOGIN_MGR = null;
    private static final Logger logger =
        LoggerUtils.getLogger(KVRepTestConfig.class, "test");

    /* Convenient topo constants. */
    public final static RepNodeId RG1_RN1_ID = new RepNodeId(1,1);
    public final static RepNodeId RG1_RN2_ID = new RepNodeId(1,2);
    public final static RepNodeId RG1_RN3_ID = new RepNodeId(1,3);

    private final String kvstoreName;

    /* The directory used for the test. */
    private final File testDir = TestUtils.getTestDir();

    private final Topology topo;
    private final GlobalParams globalParams;
    private final int numRNs;

    /* The map of rns generated for the test. */
    private final Map<RepNodeId, oracle.kv.impl.rep.RepNode> rns;

    /* Map of StorageNodeParams generated for the test. */
    private final Map<StorageNodeId, StorageNodeParams> snpMap;

    /* The mock SNAs used for the unit tests. */
    private final Map<StorageNodeId, SNA> snas;

    /* Map of RepNodeParams generated for the test. */
    private final Map<RepNodeId, RepNodeParams> rnpMap;

    /* Map of SecurityParams generated for the test. */
    private final Map<ResourceId, SecurityParams> spMap;

    /* The list of request handlers generated for the test. */
    private final LinkedHashMap<RepNodeId, RequestHandlerImpl> rhs;

    /* The set of RNService objects created for the test. */
    private final HashMap<RepNodeId, RepNodeService> rnServices =
        new HashMap<RepNodeId, RepNodeService>();

    /** Registries that we created. */
    private final List<Registry> registries = new LinkedList<Registry>();

    /** Handles for async service registries that we created. */
    private final List<ListenHandle> serviceRegistries =
        new LinkedList<ListenHandle>();

    private final UncaughtExceptionHandler exceptionHandler;

    /**
     * Make a test configuration that supports the following topology and also
     * lets you specify specific StatsParams.
     */
    public KVRepTestConfig(TestBase test,
                           int nDC,
                           int nSN,
                           int repFactor,
                           int nPartitions)
        throws IOException {

        this(test, nDC, nSN, repFactor, nPartitions, 0);
    }

    /**
     * Make a test configuration that supports the following topology,
     * including the number of secondary zones, and also lets you specify
     * specific StatsParams.
     */
    public KVRepTestConfig(TestBase test,
                           int nDC,
                           int nSN,
                           int repFactor,
                           int nPartitions,
                           int nSecondaryZones)
        throws IOException {

        this(test, nDC, nSN, repFactor, nPartitions, nSecondaryZones, 0);
    }

    /**
     * Make a test configuration that supports the following topology,
     * including the number of secondary zones and shards, and also lets you
     * specify specific StatsParams.
     */
    public KVRepTestConfig(TestBase test,
                           int nDC,
                           int nSN,
                           int repFactor,
                           int nPartitions,
                           int nSecondaryZones,
                           int nShards)
        throws IOException {

        kvstoreName = test.getKVStoreName();
        exceptionHandler = (test instanceof UncaughtExceptionHandler) ?
            (UncaughtExceptionHandler) test :
            (th, e) -> logger.log(Level.SEVERE,
                                  "Thread exception handler invoked: " +
                                  th.getName(),
                                  e);

        /*
         * Create a port locator for use in setting up registry ports in the
         * SNA, and later in this method for setting up HA ports in the
         * RepNodeParams.
         */
        FreePortLocator portLocator = TopoUtils.makeFreePortLocator();
        topo = TopoUtils.create(kvstoreName, nDC, nSN, repFactor, nPartitions,
                                nSecondaryZones, nShards, portLocator);
        globalParams = new GlobalParams(kvstoreName);

        /*
         * RNs require the store version parameter to be set to enable new
         * functions.
         */
        globalParams.setStoreVersion(
                        KVVersion.CURRENT_VERSION.getNumericVersionString());
        numRNs = topo.getRepGroupMap().size() * repFactor * nDC;

        spMap = new HashMap<ResourceId, SecurityParams>();

        /* Storage node params used in this test.*/
        snpMap = new HashMap<StorageNodeId, StorageNodeParams>();
        for (StorageNode sn : topo.getStorageNodeMap().getAll()) {
            StorageNodeParams snp = createSNParams(sn.getHostname(),
                                                   sn.getRegistryPort(),
                                                   sn.getStorageNodeId());
            snpMap.put(sn.getStorageNodeId(), snp);

            SecurityParams sp = SecurityParams.makeDefault();
            sp.initRMISocketPolicies(logger);
            spMap.put(sn.getStorageNodeId(), sp);
        }

        snas = new HashMap<StorageNodeId, SNA>();

        /* RepNodes and RequestHandlers used by the test. */
        rns = new LinkedHashMap<RepNodeId,
                                oracle.kv.impl.rep.RepNode>();
        rhs = new LinkedHashMap<RepNodeId, RequestHandlerImpl>();

        rnpMap = new HashMap<RepNodeId, RepNodeParams>();
        for (RepGroup rg : topo.getRepGroupMap().getAll()) {
            int groupNum = rg.getResourceId().getGroupId();
            for (int nodeNum=1; nodeNum <= repFactor * nDC; nodeNum++) {

                RepNodeId id = new RepNodeId(groupNum, nodeNum);
                final RequestHandlerImpl rh = createRH(id, portLocator.next());
                rhs.put(id, rh);
                rns.put(id, rh.getRepNode());
            }
        }
    }

    /**
     * Start up just the RNs in the KVS
     */
    public void startupRNs() {
        for (oracle.kv.impl.rep.RepNode rn : rns.values()) {
            rn.startup();
            rn.updateMetadata(getTopology());
        }
    }

    public void stopRNs() {
        for (oracle.kv.impl.rep.RepNode rn : rns.values()) {
            rn.stop(false);
        }

        /*
         * RepNodeService is responsible for closing file handlers in the
         * kvstore, but we do so explicitly when unit testing at a level below
         * the RepNodeService.
         */
        LoggerUtils.closeHandlers(kvstoreName);
    }

    /**
     * Simulates startup of SNs, which in this test environment simply involves
     * establishing the registry. Users of this method must also invoke stopSNs
     * to ensure that the registry port is freed up after it has been used.
     */
    public void startupSNs() {
        for (StorageNode sn : topo.getStorageNodeMap().getAll()) {
            final int port = sn.getRegistryPort();
            try {
                registries.add(TestUtils.createRegistry(port));
            } catch (RemoteException e) {
                throw new RuntimeException("Unexpected exception: " + e, e);
            }
            if (AsyncRegistryUtils.serverUseAsync) {
                try {
                    serviceRegistries.add(
                        TestUtils.createServiceRegistry(port));
                } catch (IOException e) {
                    throw new RuntimeException("Unexpected exception: " + e, e);
                }
            }
        }
    }

    public void stopSNs() {
        for (Registry registry : registries) {
            TestUtils.destroyRegistry(registry);
        }
        registries.clear();
        for (ListenHandle serviceRegistry : serviceRegistries) {
            serviceRegistry.shutdown(true);
        }
        serviceRegistries.clear();
    }

    /*
     * Tests which need them can start/stop MBMs. They create a mock SNA in the
     * registry that satisfies MBM requests.
     */
    public void startupMBMs() throws RemoteException {
        for (StorageNode sn : topo.getStorageNodeMap().getAll()) {
            final SNA sna = new SNA(kvstoreName, sn);
            final ListenHandle listenHandle =
                RegistryUtils.rebind(
                    sn.getHostname(),
                    sn.getRegistryPort(),
                    kvstoreName,
                    sn.getStorageNodeId(),
                    RegistryUtils.InterfaceType.MAIN,
                    sna,
                    DEFAULT_CSF,
                    DEFAULT_SSF,
                    STORAGE_NODE_AGENT_INTERFACE_TYPE_FAMILY,
                    () -> new StorageNodeAgentInterfaceResponder(
                        sna, DEFAULT_THREAD_POOL, logger), logger);
            snas.put(sn.getResourceId(),
                     new SNA(kvstoreName, sn, listenHandle));
        }
    }

    public void stopMBMs() throws RemoteException {
        for (StorageNode sn : topo.getStorageNodeMap().getAll()) {
            final SNA sna = snas.get(sn.getResourceId());
            RegistryUtils.unbind(sn.getHostname(),
                                 sn.getRegistryPort(),
                                 sn.getStorageNodeId().getFullName(),
                                 sna,
                                 sna.listenHandle,
                                 logger);
            sna.shutdown();
        }
    }

    public void restartRH(RepNodeId rnId)
        throws RemoteException {

        RequestHandlerImpl rh = getRH(rnId);
        RepNode rn = rh.getRepNode();
        RepNodeId id = rn.getRepNodeId();

        /* Use forced shutdown to test recovery */
        rn.stop(true);
        rh.stop();

        /* Now create a new RH and start it up */
        rh = createRH(id, true, rnpMap.get(id).getHAPort());
        rn = rh.getRepNode();
        rhs.put(id, rh);
        rns.put(id, rn);
        rn.startup();
        rh.startup();
    }

    public void startupRHs() throws RemoteException {
        /*
         * First start the simulated SNs, since they are always assumed to
         * exist and they provide the registry that's subsequently used by the
         * request handlers.
         */
        startupSNs();
        startupRNs();
        for (RequestHandlerImpl rh : rhs.values()) {
            rh.startup();
        }
    }

    public void stopRHs() {
        stopRNs();
        for (RequestHandlerImpl rh : rhs.values()) {
            rh.stop();
        }
        /*
         * Stop the SNs last since they control the registry.
         */
        stopSNs();

        /*
         * RepNodeService is responsible for closing file handlers in the
         * kvstore, but we do so explicitly when unit testing at a level below
         * the RepNodeService.
         */
        LoggerUtils.closeHandlers(kvstoreName);
    }

    public Collection<RepNodeService> getRepNodeServices() {
        return rnServices.values();
    }

    public RepNodeService getRepNodeService(RepNodeId rnId) {
        return rnServices.get(rnId);
    }

    /**
     * Starts all the simulated SNs, as well as the RN service instances
     * associated with the KVS. The SNs are needed since they supply the
     * registry needed by the RN services.
     * <p>
     * All RN services are in the RUNNING state upon return from this method.
     */
    public void startRepNodeServices() {
        startRepNodeServices(Collections.<RepNodeId> emptySet());
    }

    public void startRepNodeSubset(RepNodeId... subsetRNs) {
        for (final RepNodeId rnId : Arrays.asList(subsetRNs)) {
            final RepNodeService rnService = createRNService(rnId);
            rnService.start(getTopology());
        }
    }

    /**
     * Start up rn services, but don't pass the topology to the designated RNs.
     * This entrypoint can be used by tests where an RN needs to get its
     * topology via another RN or client.
     */
    public void startRepNodeServices(Set<RepNodeId> noTopologyRNs) {
        startupSNs();

        final RepNode rn1SortedRNs[] =
            getRNs().toArray(new RepNode[getRNs().size()]);
        /*
         * Get rn1s to the start of the list, so that they can start the RG and
         * other RNs can join.
         */
        Arrays.sort(rn1SortedRNs, new Comparator<RepNode> () {
            @Override
            public int compare(RepNode o1, RepNode o2) {
                return (o1.getRepNodeId().getNodeNum() <
                       o2.getRepNodeId().getNodeNum()) ? -1 : 0;
            }
        });

        for (final RepNode rn : rn1SortedRNs) {

            final RepNodeId rnId = rn.getRepNodeId();

            final RepNodeService rnService = createRNService(rnId);
            if (noTopologyRNs.contains(rnId)) {


                /*
                 * Pass the topology explicitly, so that the service does not
                 * have to wait for the configure call from the SNA
                 */
                rnService.start(null);
            } else {
                rnService.start(getTopology());
            }
        }
    }

    /**
     * Stops all the RN services and SNs that were started by
     * startRepNodeServices.
     */
    public void stopRepNodeServices() {
        for (final RepNodeService service : rnServices.values()) {
            service.stop(false, "test");
        }

        stopSNs();
    }

    /**
     * Stops the specified RN services. The services are removed from the
     * configuration. Throws IllegalArgumentException if any service is not
     * in the configuration.
     */
    public void stopRepNodeServicesSubset(boolean force,
                                          RepNodeId... subsetRNs) {
        for (final RepNodeId rnId : subsetRNs) {
            final RepNodeService service = rnServices.remove(rnId);
            if (service == null) {
                throw new IllegalArgumentException("RN not found: " + rnId);
            }
            service.stop(force, "test");
        }
    }

    /**
     * Create a KVStoreConfig suitable for accessing this KVS
     */
    public KVStoreConfig getKVSConfig() {
        StorageNode sn = topo.getStorageNodeMap().getAll().iterator().next();
        KVStoreConfig config =
                new KVStoreConfig(kvstoreName,
                                  sn.getHostname() + ":" +
                                  sn.getRegistryPort());
        /*
         * Make the socket timeouts large enough, so they don't produce
         * spurious failures.
         */
        config.setSocketOpenTimeout(10000, TimeUnit.MILLISECONDS);
        config.setSocketReadTimeout(60000, TimeUnit.MILLISECONDS);
        return config;
    }

    public Topology getTopology() {
        return topo;
    }

    public String getTestPath() {
        return testDir.toString();
    }

    public String getStorePath() {
        return testDir.toString() + File.separator + kvstoreName;
    }

    /**
     * Returns the list of request handlers generated for the test
     */
    public List<RequestHandlerImpl> getRHs() {
        return new LinkedList<RequestHandlerImpl>(rhs.values());
    }

    /**
     * Returns the list of RNs generated for the test
     */
    public ArrayList<oracle.kv.impl.rep.RepNode> getRNs() {
        return new ArrayList<oracle.kv.impl.rep.RepNode>(rns.values());
    }

    public oracle.kv.impl.rep.RepNode getRN(RepNodeId rnId) {
        return rns.get(rnId);
    }

    public RequestHandlerImpl getRH(RepNodeId rnId) {
        return rhs.get(rnId);
    }

    /**
     * Returns the current master of the provided rep group. Returns
     * {@code null} if no master found within the provided timeout.
     */
    public oracle.kv.impl.rep.RepNode getMaster(RepGroupId rgId,
                                                long timeoutMillis)
    {
        return getMaster(rgId, timeoutMillis, Collections.emptySet());
    }

    /**
     * Returns the current master of the provided rep group. Returns
     * {@code null} if no master found within the provided timeout.
     */
    public oracle.kv.impl.rep.RepNode getMaster(RepGroupId rgId,
                                                long timeoutMillis,
                                                Set<RepNodeId> excluded)
    {
        final List<oracle.kv.impl.rep.RepNode> targetRNs = getRNs().stream()
            .filter((rn) -> rn.getRepNodeId().getGroupId() == rgId.getGroupId())
            .filter((rn) -> !excluded.contains(rn.getRepNodeId()))
            .collect(Collectors.toList());
        final AtomicReference<oracle.kv.impl.rep.RepNode> master =
            new AtomicReference<>(null);
        final PollConditionFunc masterFound = () -> {
            return targetRNs.stream().filter((rn) -> {
                final ReplicatedEnvironment env = rn.getEnv(0);
                if (env == null) {
                    return false;
                }
                final boolean isMaster = State.MASTER.equals(env.getState());
                if (isMaster) {
                    master.set(rn);
                }
                return isMaster;
            }).count() != 0;
        };
        PollCondition.await(100, timeoutMillis, masterFound);
        return master.get();
    }

    /**
     * Returns the KV security params generated for initialization of the RN
     * or SN
     */
    public SecurityParams getSecurityParams(ResourceId rid) {
        return spMap.get(rid);
    }

    /**
     * Returns the KV per-node params generated for initialization of the RN
     */
    public RepNodeParams getRepNodeParams(RepNodeId rnId) {
        return rns.get(rnId).getRepNodeParams();
    }

    /**
     * Returns a newly created, and initialized, RepNode component based upon
     * its entry in the Topology.
     */
    private RequestHandlerImpl createRH(RepNodeId rnId,
                                        boolean dirExists,
                                        int haPort) {

        oracle.kv.impl.topo.RepNode rn = topo.get(rnId);
        StorageNodeId snId = rn.getStorageNodeId();
        final StorageNode sn = topo.get(snId);

        RepNodeParams rnParams = createRepNodeParams(sn, rn, haPort);
        StorageNodeParams snp = snpMap.get(snId);
        SecurityParams sp = SecurityParams.makeDefault();
        Params params = new Params(sp, globalParams, snp, rnParams);

        sp.initRMISocketPolicies(logger);
        spMap.put(rnId, sp);

        /* Make the directory for the RN */
        File envDir =  FileNames.getEnvDir(snp.getRootDirPath(),
                                           globalParams.getKVStoreName(),
                                           rnParams.getStorageDirectoryFile(),
                                           snp.getStorageNodeId(),
                                           rnId);
        boolean made = FileNames.makeDir(envDir);

        RequestDispatcher requestDispatcher =
            RequestDispatcherImpl.createForRN(globalParams,
                                              params.getRepNodeParams(), null,
                                              exceptionHandler, logger);

        RepNode repNode = new RepNode(params, requestDispatcher, null);

        TestFaultHandler faultHandler =
            new TestFaultHandler(logger, null);
        RequestHandlerImpl rh =
            new RequestHandlerImpl(requestDispatcher, faultHandler, null);
        repNode.initialize(rh.getListenerFactory());
        rh.initialize(params, repNode, new OperationsStatsTracker());

        if (!dirExists) {
            assertTrue("failed to makedirs:" + envDir, made);
        }
        return rh;
    }

    private RequestHandlerImpl createRH(RepNodeId rnId, int haPort) {
        return createRH(rnId, false, haPort);
    }

    /**
     * Create a RN service instance associated with the rnId
     */
    public RepNodeService createRNService(RepNodeId rnId) {

        final Topology topology = getTopology();
        StorageNodeId snId = topology.get(rnId).getStorageNodeId();
        GlobalParams gp = getGlobalParams();
        gp.setStoreVersion(KVVersion.CURRENT_VERSION.getVersionString());
        SecurityParams sp = getSecurityParams(rnId);
        StorageNodeParams snp = getStorageNodeParams(snId);
        RepNodeParams rnp = getRepNodeParams(rnId);

        LoadParameters lp = new LoadParameters();
        lp.addMap(snp.getMap());
        lp.addMap(gp.getMap());
        lp.addMap(rnp.getMap());
        RepNodeService rnService = new RepNodeService(true, exceptionHandler);
        rnService.initialize(sp, rnp, lp);
        rnServices.put(rnId, rnService);
        rhs.put(rnId, rnService.getReqHandler());
        rns.put(rnId, rnService.getReqHandler().getRepNode());
        return rnService;
    }

    private StorageNodeParams createSNParams(String hostname,
                                             int registryPort,
                                             StorageNodeId snid)
        throws IOException {

        StorageNodeParams snp =
            new StorageNodeParams(snid, hostname, registryPort, null);

        ParameterMap map = snp.getMap();
        map.setParameter(ParameterState.SN_ROOT_DIR_PATH,
                         testDir.getCanonicalPath());
        map.setParameter(ParameterState.COMMON_PORTRANGE, "5000,5010");
        return snp;
    }

    /**
     * Create RepNodeParams properties based on Topology. Method is public
     * so that returned params can be customized by test.
     */
    public RepNodeParams createRepNodeParams
        (final StorageNode sn,
         final oracle.kv.impl.topo.RepNode rn,
         final int haPort) {

        RepNodeParams rnp =
            new RepNodeParams(sn.getStorageNodeId(), rn.getResourceId(),
                              false, TopoUtils.TOPOHOST, haPort,
                              TopoUtils.TOPOHOST, haPort,
                              null /* mountPoint */, NodeType.ELECTABLE);

        /*
         * Adjust cache size to share the available memory.  All RepNodes will
         * run on the same JVM ("this" one).  Technically even this split could
         * overload memory but for test cases that's unlikely.
         */
        rnp.setRNCachePercent(rnp.getRNCachePercent() / numRNs);

        rnpMap.put(rn.getResourceId(), rnp);

        setJEParams(rnp.getMap(), rn);
        return rnp;
    }

    /*
     * Create JE properties based upon topology. Note that the first node in
     * the group: rgX-rn1 is always chosen as the Master in these tests.
     */
    private void setJEParams (ParameterMap map,
                              oracle.kv.impl.topo.RepNode rn) {

        RepNodeId rnId = rn.getResourceId();
        final StorageNode sn = topo.get(rn.getStorageNodeId());
        int haPort = rnpMap.get(rnId).getHAPort();
        final String hostPort = sn.getHostname() + ":" + haPort;

        map.setParameter(ParameterState.JE_HOST_PORT, hostPort);

        /* Set the helper host, to always be node 1 in the group for tests. */
        RepNodeId nodeOne = new RepNodeId(rnId.getGroupId(), 1);
        RepNodeParams nodeOneRNP = rnpMap.get(nodeOne);
        final StorageNode hsn = topo.get(nodeOneRNP.getStorageNodeId());
        map.setParameter(ParameterState.JE_HELPER_HOSTS,
                         hsn.getHostname() + ":" + nodeOneRNP.getHAPort());
    }

    /**
     * Returns the RN service params associated with the repNodeId
     */
    public Params getParams(RepNodeId repNodeId) {

        final StorageNodeParams storageNodeParams =
            snpMap.get(topo.get(repNodeId).getStorageNodeId());

        final RepNodeParams repNodeParams =
            rns.get(repNodeId).getRepNodeParams();

        final SecurityParams securityParams = spMap.get(repNodeId);

        return new Params(securityParams,
                          globalParams,
                          storageNodeParams,
                          repNodeParams);
    }

    public GlobalParams getGlobalParams() {
        return globalParams;
    }

    public StorageNodeParams getStorageNodeParams(StorageNodeId snId) {
        return snpMap.get(snId);
    }

    /**
     * Sometimes a test just needs a StorageNodeParams, and it doesn't really
     * matter which sn it belongs to.
     */
    public StorageNodeParams getAnyStorageNodeParams() {
        return snpMap.values().iterator().next();
    }

    class TestFaultHandler extends TestProcessFaultHandler {

        public TestFaultHandler(Logger logger,
                                ProcessExitCode defaultExitCode) {
            super(logger, defaultExitCode);
        }

        @Override
        public void queueShutdownInternal(Throwable fault,
                                          ProcessExitCode exitCode) {
            fault.printStackTrace();
            throw new UnsupportedOperationException("Method not implemented: " +
                                                    "queueShutdown",
                                                    fault);
        }
    }

    /* A mock SNA that responds to MBM requests. */
    static private final class SNA extends StorageNodeAgentNOP {

        private final MasterBalanceManagerInterface mbm;
        final ListenHandle listenHandle;

        SNA(String kvstoreName, StorageNode sn) {
            this(kvstoreName, sn, null);
        }

        SNA(String kvstoreName, StorageNode sn, ListenHandle listenHandle) {
            this.listenHandle = listenHandle;
            SNInfo snInfo = new SNInfo(kvstoreName,
                                       sn.getResourceId(),
                                       sn.getHostname(),
                                       sn.getRegistryPort());
            mbm = MasterBalanceManager.create
               (false, snInfo, LoggerUtils.getLogger(KVRepTestConfig.class,
                                                     "mbm"),
                NULL_LOGIN_MGR);
        }

        public void shutdown() {
            mbm.shutdown();
        }

        @Override
        public void noteState(StateInfo stateInfo, AuthContext authContext,
                              short serialVersion)
            throws RemoteException {

            mbm.noteState(stateInfo, authContext, serialVersion);
        }


        @Override
        public MDInfo getMDInfo(AuthContext authContext, short serialVersion)
            throws RemoteException {

            return mbm.getMDInfo(authContext, serialVersion);
        }

        @Override
        public boolean cancelMasterLease(StorageNode lesseeSN,
                                         oracle.kv.impl.topo.RepNode rn,
                                         AuthContext authContext,
                                         short serialVersion)
            throws RemoteException {

            return mbm.cancelMasterLease(lesseeSN, rn, authContext,
                                         serialVersion);
        }

        @Override
        public boolean getMasterLease(MasterLeaseInfo masterLease,
                                      AuthContext authContext,
                                      short serialVersion)
            throws RemoteException {

            return mbm.getMasterLease(masterLease, authContext, serialVersion);
        }
    }

}
