/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.sna;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.rmi.NotBoundException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.StorageNodeUtils;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.utilint.JVMSystemUtils;

import org.junit.runners.Parameterized.Parameters;

/**
 * Base class for Storage Node Agent tests.
 *
 * TODO: now that StorageNodeUtils exists, change callers of static methods
 * that are implemented in StorageNodeUtils to call directly.
 */
public class StorageNodeTestBase extends TestBase {

    protected static final LoginManager NULL_LOGIN_MGR = null;

    protected final boolean useThreads;
    protected final boolean isWindows;
    protected final PortFinder portFinder;
    protected StorageNodeAgent sna;
    protected boolean useMountPoints;

    /**
     * These are up for grabs.
     */
    protected static final String CONFIG_FILE_NAME =
                                StorageNodeAgent.DEFAULT_CONFIG_FILE;
    protected static final String BAD_DIR = "/nodirectory";
    protected static final String BAD_FILE = "notafile";
    protected static final String testhost = "localhost";
    protected static final String configName = "config.xml";
    protected static final int startPort = 6000;
    protected static final int haRange = 5;
    protected static final int numStorageNodes = 1;
    protected static final int numPartitions = 10;
    protected static final int maxRepNodes = 10;
    protected static final int replicationFactor = 3;
    protected static final int defaultTimeout = 40;
    protected static final String mountDir = "mount";

    protected StorageNodeTestBase(boolean useThreads) {
        this.useThreads = useThreads;
        String os = System.getProperty("os.name");
        isWindows = os.contains("Windows");
        portFinder = new PortFinder(startPort, haRange, testhost);
    }

    /**
     * Provides the default parameters for use by subclass that are tests
     * parameterized to run in thread and process modes.
     */
    @Parameters(name="Use_Thread={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(new Object[][] {{true}, {false}});
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        killManagedProcesses();
        TestUtils.clearTestDirectory();

        /*
         * In case socket factory use didn't fully take, prevent collisions
         * between previously created CSFs and those to be created in the
         * future.
         */
        TestUtils.newCsfGeneration();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        killManagedProcesses();
        LoggerUtils.closeAllHandlers();
        if (!useThreads) {
            /* Make sure there aren't any leftovers */
            int nm = numManagedProcesses();
            assertEquals(0, nm);
        }
    }

    protected LoginManager getLoginMgr() {
        return sna.getLoginManager();
    }

    protected void killProcess(Integer pid) {
        ManagedService.killProcess(pid);
    }

    protected void killManagedProcesses() {
        if (sna != null) {
            try {
                sna.shutdown(true, true);
            } catch (Exception ignored) {
            }
            String bootstrapName = sna.makeBootstrapAdminName();
            ManagedService.killManagedProcesses(null, bootstrapName, logger);
        }
        ManagedService.killManagedProcesses(kvstoreName, null, logger);
    }

    protected void killRepNode(RepNodeId rnid) {
        List<Integer> list = ManagedService.findManagedProcesses
            (kvstoreName, rnid.getFullName(), logger);
        assert(list.size() == 1) : list.toString();
        killProcess(list.get(0));
    }

    protected int numManagedProcesses() {
        return (ManagedService.findManagedProcesses(kvstoreName, null,
                                                    logger).size());
    }

    static public void assertPing(StorageNodeAgentAPI snai,
                                  ServiceStatus value)
        throws Exception {

        ServiceStatus pingResult = snai.ping().getServiceStatus();
        assertEquals(value, pingResult);
    }

    static public void assertPing(RepNodeAdminAPI rnai,
                                  ServiceStatus value)
        throws Exception {

        ServiceStatus pingResult = rnai.ping().getServiceStatus();
        assertEquals(value, pingResult);
    }

    static public void assertPing(CommandServiceAPI admin, ServiceStatus value)
        throws Exception {

        ServiceStatus pingResult = admin.ping();
        assertEquals(value, pingResult);
    }

    static public void assertShutdown(StorageNodeAgentAPI snai)
        throws Exception {

        try {
            ServiceStatus pingResult = snai.ping().getServiceStatus();
            fail("SNA is not shutdown, ping returned " + pingResult);
        } catch (Exception expected) {
            /* success */
        }
    }

    static public void assertShutdown(RepNodeAdminAPI rnai) {
        try {
            ServiceStatus pingResult = rnai.ping().getServiceStatus();
            fail("RepNode is not shutdown, ping returned " + pingResult);
        } catch (Exception expected) {
            /* success */
        }
    }

    /**
     * Wait for an RN to shutdown, throwing an exception if the shutdown takes
     * too long.
     */
    public static void awaitShutdown(RepNodeAdminAPI rnai) {
        if (!PollCondition.await(1000, 30000,
                                 () -> {
                                     try {
                                         rnai.ping();
                                         return false;
                                     } catch (Exception e) {
                                         return true;
                                     }
                                 })) {
            assertShutdown(rnai);
        }
    }

    protected void assertStopped(RepNodeId rnid,
                                 Topology topo,
                                 StorageNodeId snid,
                                 boolean value)
        throws Exception {

        RepNodeParams rnp =
            ConfigUtils.getRepNodeParams
            (FileNames.getSNAConfigFile(TestUtils.getTestDir().toString(),
                                        topo.getKVStoreName(),
                                        snid), rnid, null);
        assert(rnp.isDisabled() == value);
    }

    protected StorageNodeAgentAPI startRepNodes(Topology topo,
                                                RepNodeId[] rnids,
                                                int numRepNodes)
        throws Exception {

        assert(numRepNodes > 0);
        RepNodeId repNodeIds[] = new RepNodeId[maxRepNodes];
        RepNodeParams[] rnps = new RepNodeParams[maxRepNodes];
        ParameterMap[] ps = new ParameterMap[maxRepNodes];
        initRepNodes(topo, repNodeIds, rnps, ps);
        for (int i = 0; i < numRepNodes; i++) {
            rnids[i] = repNodeIds[i];
        }

        StorageNodeId snid = topo.get(repNodeIds[0]).getStorageNodeId();
        StorageNodeAgentAPI snai = createRegisteredStore(snid, false);

        for (int i = 0; i < numRepNodes; i++) {
            snai.createRepNode(ps[i], createMetadataSet(topo));
        }

        for (int i = 0; i < numRepNodes; i++) {
            waitForRNAdmin(rnids[i]);
        }
        return snai;
    }

    protected File makeMountPoints(int n) {
        String name = "mp";
        File mp = new File(TestUtils.getTestDir(), mountDir);
        mp.mkdir();
        for (int i = 0; i < n; ++i) {
            name = name + i;
            new File(mp, name).mkdir();
        }
        return mp;
    }

    protected void initRepNodes(Topology topo, RepNodeId[] repNodeIds,
                                RepNodeParams[] rnps, ParameterMap ps[]) {
        int i = 0;
        int haPort = portFinder.getHaFirstPort();
        File mountRoot =
            makeMountPoints(replicationFactor * topo.getRepGroupMap().size());
        String[] mounts = mountRoot.list();
        for (RepGroup rg : topo.getRepGroupMap().getAll()) {
            int groupNum = rg.getResourceId().getGroupId();
            for (int nodeNum = 1; nodeNum <= replicationFactor; nodeNum++) {
                RepNodeId id = new RepNodeId(groupNum, nodeNum);
                repNodeIds[i] = id;
                StorageNodeId snid = topo.get(id).getStorageNodeId();
                rnps[i] = createRepNodeParams(snid, id, haPort++,
                                              mountRoot, mounts[i]);
                ps[i] = rnps[i++].getMap();
            }
        }
    }

    /*
     * Keep the RN resource usage low for tests
     */
    private void setRepNodeResources(RepNodeParams rnp) {
        if (!JVMSystemUtils.ZING_JVM) {
            rnp.setJECacheSize(33 * 1024 * 1024);
        }
        int rnHeapPercent =
            Integer.parseInt(ParameterState.SN_RN_HEAP_PERCENT_DEFAULT);
        int rnHeapMB = (rnHeapPercent * MB_PER_SN) / 100;
        rnp.setJavaMiscParams(
            "-Xmx" + ParameterUtils.applyMinHeapMB(rnHeapMB) + "M");
    }

    private RepNodeParams createRepNodeParams(StorageNodeId snid,
                                              RepNodeId rnid,
                                              int haPort,
                                              File mountRoot,
                                              String mountPoint) {

        RepNodeParams rnp =
            new RepNodeParams(snid, rnid, false /* disabled */, testhost,
                              haPort, testhost, haPort, /* helperHost */
                              null /* mountPoint */, NodeType.ELECTABLE);
        setRepNodeResources(rnp);
        if (useMountPoints) {
            rnp.setStorageDirectory
                (new File(mountRoot, mountPoint).getAbsolutePath(), 0L);
        }
        return rnp;
    }

    /**
     * Create and start a store that's already been registered.
     */
    protected StorageNodeAgentAPI startRegisteredStore(StorageNodeId snid)
        throws Exception {

        String testDir = generateBootstrapDir(portFinder, 1, configName);
        sna = startSNA(testDir, configName, useThreads, true);

        StorageNodeAgentAPI snai = null;
        int port = portFinder.getRegistryPort();
        try {
            snai = getHandle(
                kvstoreName, testhost, port, snid, NULL_LOGIN_MGR);
        } catch (NotBoundException nbe) {
            /**
             * Some tests revert the store to bootstrap state, try that.
             */
            snai = RegistryUtils.getStorageNodeAgent(testhost,
                                                     port,
                                                     sna.getServiceName(),
                                                     NULL_LOGIN_MGR,
                                                     logger);
        }
        return snai;
    }

    /**
     * Register an SNA to a store using default state.
     */
    public static StorageNodeAgentAPI
        registerStore(StorageNodeAgentAPI snai,
                      LoginManager loginMgr,
                      int port,
                      String kvstoreName,
                      StorageNodeId snid,
                      boolean hostingAdmin)
        throws Exception {

        return StorageNodeUtils.registerStore(snai, loginMgr, port, testhost,
                                              kvstoreName, snid, hostingAdmin);
    }

    /**
     * Start and register an SNA to a store using default state.
     */
    protected StorageNodeAgentAPI createRegisteredStore(StorageNodeId snid,
                                                        boolean hostingAdmin)
        throws Exception {

        return createRegisteredStore(snid, hostingAdmin, null);
    }

    protected StorageNodeAgentAPI
        createRegisteredStore(StorageNodeId snid,
                              boolean hostingAdmin,
                              SecureOpts secureOpts)
        throws Exception {

        sna = createRegisteredStore(kvstoreName, portFinder, configName,
                                    snid, hostingAdmin, useThreads, true,
                                    secureOpts);
        return getHandle
            (kvstoreName, sna.getHostname(), sna.getRegistryPort(), snid,
             NULL_LOGIN_MGR);
    }

    /**
     * Public static method to start and register an SNA.  TODO: combine this
     * with protected internal method above.
     */
    public static StorageNodeAgent createRegisteredStore(String kvstoreName,
                                                         PortFinder portFinder,
                                                         String configFileName,
                                                         StorageNodeId snid,
                                                         boolean hostingAdmin,
                                                         boolean useThreads,
                                                         boolean createAdmin,
                                                         SecureOpts secOpts)
        throws Exception {

        return StorageNodeUtils.createRegisteredStore
            (testhost, kvstoreName, portFinder, configFileName,
             snid, hostingAdmin, useThreads, createAdmin, secOpts);
    }

    /**
     * Get a handle for a registered SNA instance.
     */
    protected static StorageNodeAgentAPI getHandle(String kvstoreName,
                                                   String hostname,
                                                   int port,
                                                   StorageNodeId snid,
                                                   LoginManager loginMgr)
        throws Exception {

        return StorageNodeUtils.getHandle(kvstoreName, hostname, port, snid,
                                          loginMgr);
    }

    /**
     * Get a bootstrap handle for SNA.
     */
    public static StorageNodeAgentAPI getBootstrapHandle(String hostname,
                                                         int port,
                                                         LoginManager loginMgr)
        throws Exception {

        return StorageNodeUtils.getBootstrapHandle(hostname, port, loginMgr);
    }

    protected StorageNodeAgentAPI createUnregisteredSNA()
        throws Exception {

        return createUnregisteredSNA(null);
    }

    protected StorageNodeAgentAPI createUnregisteredSNA(SecureOpts secureOpts)
        throws Exception {

        sna = StorageNodeUtils.createUnregisteredSNA
            (TestUtils.getTestDir().toString(),
             portFinder, 1, configName, useThreads, true, null, 0, 0, 1024,
             null, secureOpts);
        StorageNodeAgentAPI snai = StorageNodeUtils.getBootstrapHandle
            (testhost, portFinder.getRegistryPort(),
             sna.getLoginManager());
        assertPing(snai, ServiceStatus.WAITING_FOR_DEPLOY);
        return snai;
    }

    /**
     * Public method to create an unregistered SNA.
     */
    public static StorageNodeAgent
        createUnregisteredSNA(String rootDir,
                              PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              int memoryMB,
                              List<String> storageDirs)
        throws Exception {

        return StorageNodeUtils.createUnregisteredSNA
            (rootDir, portFinder, capacity,
             configFileName, useThreads, createAdmin, null, 0, 0, memoryMB,
             storageDirs, null);
    }

    public static StorageNodeAgent
        createUnregisteredSNA(String rootDir,
                              PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              int memoryMB)
        throws Exception {

        return createUnregisteredSNA(rootDir,
                                     portFinder,
                                     capacity,
                                     configFileName,
                                     useThreads,
                                     createAdmin,
                                     memoryMB,
                                     null);
    }

    /**
     * Public method to create an unregistered SNA.
     */
    public static StorageNodeAgent
        createUnregisteredSNA(PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              int memoryMB,
                              List<String> storageDirs)
        throws Exception {

        return createUnregisteredSNA(TestUtils.getTestDir().toString(),
                                     portFinder,
                                     capacity,
                                     configFileName,
                                     useThreads,
                                     createAdmin,
                                     memoryMB,
                                     storageDirs);
    }

    public static StorageNodeAgent
        createUnregisteredSNA(PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              int memoryMB)
        throws Exception {

        return createUnregisteredSNA(portFinder,
                                     capacity,
                                     configFileName,
                                     useThreads,
                                     createAdmin,
                                     memoryMB,
                                     null);
    }

    public static StorageNodeAgent
        createUnregisteredSNA(PortFinder portFinder,
                              int capacity,
                              String configFileName,
                              boolean useThreads,
                              boolean createAdmin,
                              boolean secure)
        throws Exception {

        return StorageNodeUtils.createUnregisteredSNA
            (TestUtils.getTestDir().toString(), portFinder, capacity,
             configFileName, useThreads, createAdmin, null, 0, 0, 1024, null,
             new SecureOpts().setSecure(secure));
    }

    public static StorageNodeAgent
    createUnregisteredSNA(PortFinder portFinder,
                          int capacity,
                          String configFileName,
                          boolean useThreads,
                          boolean createAdmin,
                          boolean secure,
                          int memoryMB)
    throws Exception {

        return StorageNodeUtils.createUnregisteredSNA
            (TestUtils.getTestDir().toString(), portFinder, capacity,
             configFileName, useThreads, createAdmin, null, 0, 0, memoryMB, null,
             new SecureOpts().setSecure(secure));
    }

    public static StorageNodeAgentAPI
        createNoBootstrapSNA(PortFinder portFinder,
                             int capacity,
                             String configFileName)
        throws Exception {

        return StorageNodeUtils.createNoBootstrapSNA
            (portFinder, capacity, configFileName);
    }

    public static void cleanBootstrapDir()
        throws Exception {

        String testDir = TestUtils.getTestDir().toString();
        File configfile = new File(testDir + File.separator + configName);
        File secfile = new File
            (testDir + File.separator + FileNames.JAVA_SECURITY_POLICY_FILE);
        if (configfile.exists()) {
            configfile.delete();
        }
        if (secfile.exists()) {
            secfile.delete();
        }
    }

    public static void cleanStoreDir(String kvstoreName)
        throws Exception {

        StorageNodeUtils.cleanStoreDir
            (TestUtils.getTestDir().toString(), kvstoreName);
    }

    public static String generateBootstrapDir(PortFinder pf,
                                              int capacity,
                                              String configFileName)
        throws Exception {

        return StorageNodeUtils.generateBootstrapDir
            (pf, capacity, configFileName);
    }

    public static String generateBootstrapDir(PortFinder pf,
                                              int capacity,
                                              String configFileName,
                                              boolean runBootAdmin)
        throws Exception {

        return StorageNodeUtils.generateBootstrapDir
            (pf, capacity, configFileName, runBootAdmin);
    }

    public static String generateBootstrapDir(String testDir,
                                              PortFinder pf,
                                              int capacity,
                                              int memoryMB,
                                              String configFileName)
        throws Exception {

        return StorageNodeUtils.generateBootstrapDir
            (testDir, pf, capacity, memoryMB, null, 0, 0, configFileName);
    }

    /**
     * Start an instance of SNA assuming the bootstrap directory and file have
     * been created.
     */
    public static StorageNodeAgent startSNA(String bootstrapDir,
                                            String bootstrapFile,
                                            boolean useThreads,
                                            boolean createAdmin)
        throws Exception {

        return StorageNodeUtils.startSNA(bootstrapDir, bootstrapFile,
                                         useThreads, createAdmin);
    }

    /**
     * Wrapper for waitForRNAdmin that adds timeout and required status.
     */
    protected RepNodeAdminAPI waitForRNAdmin(RepNodeId rnid)
        throws Exception {

        RepNodeAdminAPI rnai = waitForRNAdmin
            (rnid, sna.getStorageNodeId(), sna.getStoreName(),
             sna.getHostname(), sna.getRegistryPort(), 40);
        assert(rnai != null);
        assertPing(rnai, ServiceStatus.RUNNING);
        return rnai;
    }

    /**
     * Similar to above but this may fail and return null.
     */
    protected RepNodeAdminAPI waitForRNAdmin(RepNodeId rnid,
                                             int timeoutSecs) {
        try {
            return waitForRNAdmin
                (rnid, sna.getStorageNodeId(), sna.getStoreName(),
                 sna.getHostname(), sna.getRegistryPort(), timeoutSecs);
        } catch (Exception ignored) {
        }
        return null;
    }

    public static RepNodeAdminAPI waitForRNAdmin(RepNodeId rnid,
                                                 StorageNodeId snid,
                                                 String kvstoreName,
                                                 String hostname,
                                                 int port,
                                                 int timeout)
        throws Exception {

        return StorageNodeUtils.waitForRNAdmin(rnid, snid, kvstoreName,
                                               hostname, port, timeout);
    }

    /**
     * Wrapper for waitForRNAdmin that adds timeout and required status.  This
     * version assumes that it will succeed.
     */
    public static CommandServiceAPI waitForAdmin(String hostname, int port)
        throws Exception {

        return StorageNodeUtils.waitForAdmin(hostname, port);
    }

    /**
     * Wrapper for waitForRNAdmin that adds timeout and required status.  This
     * version assumes that it will succeed.
     */
    public static CommandServiceAPI waitForAdmin(String hostname, int port,
                                                 LoginManager loginMgr)
        throws Exception {

        return StorageNodeUtils.waitForAdmin(hostname, port, loginMgr);
    }

    /**
     * Similar to above but this one may fail and return null.
     */
    public static CommandServiceAPI waitForAdmin(String hostname,
                                                 int port,
                                                 int timeout) {
        return StorageNodeUtils.waitForAdmin(hostname, port, timeout);
    }

    protected AdminParams createAdminParams(AdminId aid,
                                            StorageNodeId snid,
                                            String hostname) {

        AdminParams ap = new AdminParams(aid, snid, null, "");
        // TODO: Guy thinks the literal integers below are dummy values.
        // Should verify that. The history is that this method received
        // an admin http port and used it for the helper ports as well
        // as the erstwhile http port parameter of AdminParams.
        ap.setJEInfo(hostname, 5001, hostname, 5001);
        return ap;
    }

    /**
     * Only use one RepGroup.
     */
    protected Topology createTopology(String hostname, int numRepNodes) {
        return createTopology(hostname, 1, numRepNodes);
    }

    /**
     * Create a topology with numRepGroups RepGroups and numRepNodes RepNodes
     * in each group.
     */
    protected Topology createTopology(String hostname, int numRepGroups,
                                      int numRepNodes) {

        Topology topo = new Topology(kvstoreName);
        Datacenter dc = Datacenter.newInstance(kvstoreName, numRepNodes,
                                               DatacenterType.PRIMARY, false,
                                               false);
        topo.add(dc);

        /**
         * Now a StorageNode.
         */
        int port = portFinder.getRegistryPort();
        StorageNode sn =
            new StorageNode(dc, hostname, port);
        topo.add(sn);

        /**
         * Now RepGroup, RepNode.  NOTE: need to add RG to Topo before adding
         * RNs to RG.
         */
        for (int i = 0; i < numRepGroups; i++) {
            RepGroup rg = new RepGroup();
            topo.add(rg);
            for (int j = 0; j < numRepNodes; j++) {
                RepNode rn = new RepNode(sn.getResourceId());
                rg.add(rn);
            }
            /**
             * Add some partitions
             */
            for (int k = 0; k < numPartitions; k++) {
                topo.add(new Partition(rg));
            }
        }
        return topo;
    }

    protected static void delay(int seconds)
        throws Exception {
        Thread.sleep(seconds*1000);
    }

    protected Set<Metadata<? extends MetadataInfo>>
            createMetadataSet(Metadata<?>... mds) {

        return new HashSet<Metadata<? extends MetadataInfo>>(
                Arrays.asList(mds));
    }
}
