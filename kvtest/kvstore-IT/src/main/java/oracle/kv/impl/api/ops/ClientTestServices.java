/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.PasswordCredentials;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminService;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.AdminTestConfig;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.SizeParameter;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.sna.ServiceManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.StorageNodeUtils.SecureOpts;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.PingCollector;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * KVStore services for use in black-box testing of the client API.
 */
class ClientTestServices {

    public static final String STOP_ALL_MASTERS = "-stopAllMasters";
    public static final String STOP_ALL_REPLICAS = "-stopAllReplicas";
    public static final String STORAGE_SIZE = "-storageSize";
    public static final String SECURITY_FLAG = "-security";

    private static final Logger logger =
        LoggerUtils.getLogger(ClientTestServices.class, "test");
    private static final int REP_FACTOR = 2; // Must be <= nSNs (currently 2)
    private static final int N_PARTITIONS = 100;
    private static final int startPort1 = 5000;
    private static final int startPort2 = 6000;
    private static final int haRange = 5;
    private static final String USER = "testuser";
    private static final String PASS = "NoSql00__testpass";
    private final String kvstoreName;
    private final PortFinder portFinder1;
    private final PortFinder portFinder2;
    private final AdminTestConfig atc;
    private final File testDir;
    private Admin admin;
    private AdminService adminService;
    private StorageNodeAgentImpl sna1;
    private StorageNodeAgentImpl sna2;
    private final String testhost;
    private LoginManager loginMgr = null;

    /**
     * Creates a services object that can be used to configure the KVStore.
     * start() must be called before opening the store, and stop() must be
     * called after closing the store.
     */
    ClientTestServices(String kvstoreName)
        throws Exception {

        this.kvstoreName = kvstoreName;
        TestStatus.setActive(true);
        portFinder1 = new PortFinder(startPort1, haRange);
        portFinder2 = new PortFinder(startPort2, haRange);
        atc = new AdminTestConfig(kvstoreName, portFinder1);
        testDir = atc.getTestDir();
        testhost = atc.getTestHost();
    }

    /**
     * Prints configuration information to output:
     * Storename, hostname, registry ports
     */
    public void printConfig(PrintStream out) {
        out.println(kvstoreName);
        out.println(testhost);
        out.println(portFinder1.getRegistryPort());
        out.println(portFinder2.getRegistryPort());
    }


    /**
     * Starts the services.
     *
     * @param secure whether to create a secured store
     */
    void start(boolean secure, long storageSize)
        throws Exception {

        /* Create and start a pair of SNAs. */
        String bootstrapFileName1 = "config1.xml";
        String bootstrapFileName2 = "config2.xml";
        String bootstrapFilePath1 =
            testDir + File.separator + bootstrapFileName1;
        String bootstrapFilePath2 =
            testDir + File.separator + bootstrapFileName2;
        String secfilePath =
            testDir + File.separator + FileNames.JAVA_SECURITY_POLICY_FILE;
        final File secDir = new File(testDir, FileNames.SECURITY_CONFIG_DIR);

        BootstrapParams bp1 = new BootstrapParams(
            testDir.toString(), testhost, testhost, portFinder1.getHaRange(),
            null /*servicePortRange*/, null /*storeName*/,
            portFinder1.getRegistryPort(), -1,
            1 /*capacity*/, null /*storageType*/,
            secure ? FileNames.SECURITY_CONFIG_DIR : null,
            true, null);

        BootstrapParams bp2 = new BootstrapParams(
            testDir.toString(), testhost, testhost, portFinder2.getHaRange(),
            null /*servicePortRange*/, null /*storeName*/,
            portFinder2.getRegistryPort(), -1,
            1 /*capacity*/, null /*storageType*/,
            secure ? FileNames.SECURITY_CONFIG_DIR : null,
            true, null);

        bp1.setMemoryMB(MB_PER_SN);
        bp2.setMemoryMB(MB_PER_SN);

        if (storageSize != 0) {
            /* Capacity is one, so use the root dir as the storage dir. */
            List<String> storageDirs =
                Collections.singletonList(testDir.toString());
            List<String> storageDirSizes =
                Collections.singletonList(String.valueOf(storageSize));

            bp1.setStorgeDirs(storageDirs, storageDirSizes);
            bp2.setStorgeDirs(storageDirs, storageDirSizes);
        }

        if (secure) {
            if (secDir.exists()) {
                fail("Detected existing security dir: " + secDir);
            }
            final SecureOpts secOpts = new SecureOpts().setSecure(true);
            if (TestUtils.isSSLDisabled()) {
                secOpts.setNoSSL(true);
            }
            TestUtils.generateSecurityDir(secDir, secOpts, null);
        }

        try {
            ConfigUtils.createBootstrapConfig(bp1, bootstrapFilePath1);
            ConfigUtils.createBootstrapConfig(bp2, bootstrapFilePath2);
        } catch (Exception e) {
            throw new Exception("Couldn't create bootstrap file", e);
        }
        TestUtils.generateSecurityPolicyFile
            (secfilePath, TestUtils.SEC_POLICY_STRING);

        /**
         * Use threads for the store to allow the process creator to just kill
         * this process for easy cleanup.
         */
        String[] snaArgs = new String[] {
            CommandParser.ROOT_FLAG, testDir.toString(),
            StorageNodeAgent.CONFIG_FLAG, bootstrapFileName1,
            StorageNodeAgent.THREADS_FLAG
        };

        /*
         * start() and parseArgs are not remote method on
         * StorageNodeAgentInterface, so must declare StorageNodeAgentImpl,
         * rather than StorageNodeAgentInterface.
         */
        sna1 = new StorageNodeAgentImpl(true);
        sna1.parseArgs(snaArgs);
        sna1.start();

        sna2 = new StorageNodeAgentImpl(true);
        snaArgs[3] = bootstrapFileName2;
        sna2.parseArgs(snaArgs);
        sna2.start();
        assertEquals
            // CRC: Worth replacing null with AuthContext.NULL_CTX, to make it
            // clearer what these particular nulls are?
            (sna1.ping(null, SerialVersion.CURRENT).getServiceStatus(),
             ServiceStatus.WAITING_FOR_DEPLOY);
        assertEquals
            (sna2.ping(null, SerialVersion.CURRENT).getServiceStatus(),
             ServiceStatus.WAITING_FOR_DEPLOY);

        /*
         * Start AdminService and get Admin, so that Admin can communicate with
         * SNs in cases with secured transport.
         */
        AdminServiceParams adminServiceParams = atc.getParams();
        if (secure) {
            adminServiceParams.setSecurityParams(
                SecurityUtils.loadSecurityParams(secDir));
        }
        AdminParams ap = adminServiceParams.getAdminParams();
        SecurityParams sp = adminServiceParams.getSecurityParams();
        StorageNodeParams snp = adminServiceParams.getStorageNodeParams();
        GlobalParams gp = adminServiceParams.getGlobalParams();

        LoadParameters lp = new LoadParameters();
        lp.addMap(snp.getMap());
        lp.addMap(gp.getMap());
        lp.addMap(ap.getMap());

        /*
         * Stop the bootstrap admin before starting the registered one so we
         * can export the new admin using the same dialog type. But only do
         * this in the non-secure case. In the secure case, we don't have
         * credentials set up, so the stop call would fail. And it seems that
         * there is no conflict between the bootstrap admin and the real admin
         * in the bootstrap case, so the stop call is not needed.
         */
        if (!secure) {
            final ServiceManager adminServiceManager =
                sna1.getStorageNodeAgent().getAdminServiceManager();
            adminServiceManager.dontRestart();
            final CommandServiceAPI adminAPI = ServiceUtils.waitForAdmin(
                bp1.getHostname(), bp1.getRegistryPort(), loginMgr,
                5 /* seconds */, ServiceStatus.RUNNING, logger);
            adminAPI.stop(false /* force */, "replace bootstrap");
            adminServiceManager.waitFor(10000);
        }

        adminService = new AdminService(true);
        adminService.initialize(sp, ap, lp);
        adminService.start();

        admin = adminService.getAdmin();
        Topology t = admin.getCurrentTopology();
        assertEquals(kvstoreName, t.getKVStoreName());
        Parameters p = admin.getCurrentParameters();
        assertEquals(kvstoreName, p.getGlobalParams().getKVStoreName());

        /*
         * By default, create a .csv file and generate performance stats
         * frequently, to exercise monitoring while running API operations.
         */
        ParameterMap map = admin.copyPolicy();
        map.setParameter(ParameterState.SP_COLLECT_ENV_STATS,
                         Boolean.toString(true));
        map.setParameter(ParameterState.GP_COLLECTOR_INTERVAL, "3 SECONDS");
        map.setParameter(ParameterState.MP_CREATE_CSV, Boolean.toString(true));

        /*
         * Reduce some default times to make these operations work faster on
         * small stores.
         */
        map.setParameter(ParameterState.AP_CHECK_ADD_INDEX, "1 s");
        map.setParameter(ParameterState.AP_BROADCAST_METADATA_RETRY_DELAY,
                         "1 s");

        /*
         * Set JE parameters to control JE data verifier.
         */
        String configProps =
            "je.env.runVerifier=" +
            System.getProperty("test.je.env.runVerifier", "true") +
            ";je.env.verifySchedule=" +
            System.getProperty("test.je.env.verifierSchedule", "* * * * *") + ";";
        map.setParameter(ParameterState.JE_MISC, configProps);

        admin.setPolicy(map);

        /* Create and run a DeployDatacenterPlan */
        int planId = admin.getPlanner().
            createDeployDatacenterPlan("deploy data center", "Miami",
                                       REP_FACTOR,
                                       DatacenterType.PRIMARY, false, false);
        runPlan(admin, 1, planId);

        /* Check the resulting topology changes. */
        t = admin.getCurrentTopology();

        DatacenterMap dcmap = t.getDatacenterMap();
        assertEquals(dcmap.size(), 1);

        Datacenter dc = dcmap.getAll().iterator().next();
        assertEquals(dc.getName(), "Miami");

        /*
         * Create and run DeployStorageNode plans for SNA1 and 2.  First,
         * deploy SNA1.
         */
        StorageNodeParams snParams =
            new StorageNodeParams(testhost, portFinder1.getRegistryPort(),
                                  "Nodus Operandi-SNA1");
        planId =
            admin.getPlanner().
            createDeploySNPlan("deploy storage node 1", dc.getResourceId(),
                               snParams);
        runPlan(admin, 2, planId);
        assertEquals
            (sna1.ping(null, SerialVersion.CURRENT).getServiceStatus(),
             ServiceStatus.RUNNING);

        /*
         * Before deploying SNA2, we need an Admin
         */

        planId =
            admin.getPlanner().
            createDeployAdminPlan("deploy admin", new StorageNodeId(1));
        runPlan(admin, 3, planId);

        /* Deploy SNA2. */
        snParams =
            new StorageNodeParams(testhost, portFinder2.getRegistryPort(),
                                  "Nodus Operandi-SNA2");

        planId =
            admin.getPlanner().
            createDeploySNPlan("deploy storage node 2", dc.getResourceId(), snParams);
        runPlan(admin, 4, planId);
        assertEquals
            (sna2.ping(null, SerialVersion.CURRENT).getServiceStatus(),
             ServiceStatus.RUNNING);

        /* Check the resulting topology changes. */
        t = admin.getCurrentTopology();

        StorageNodeMap smap = t.getStorageNodeMap();
        assertEquals(smap.size(), 2);
        final String poolName = "IM1Test SNPool";
        admin.addStorageNodePool(poolName);
        Iterator<StorageNode> iter = smap.getAll().iterator();
        StorageNode sn = iter.next();
        assertEquals(sn.getDatacenterId(), dc.getResourceId());
        assertEquals(sn.getStorageNodeId().getStorageNodeId(), 1);
        admin.addStorageNodeToPool(poolName, sn.getStorageNodeId());
        sn = iter.next();
        assertEquals(sn.getDatacenterId(), dc.getResourceId());
        assertEquals(sn.getStorageNodeId().getStorageNodeId(), 2);
        admin.addStorageNodeToPool(poolName, sn.getStorageNodeId());

        /* Create and execute a DeployKVStorePlan. */
        admin.createTopoCandidate("candidate1", poolName,
                                  N_PARTITIONS, false,
                                  SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
        planId = admin.getPlanner().createDeployTopoPlan
            ("deploy kv store", "candidate1", null);
        runPlan(admin, 5, planId);

        Thread.sleep(4000);

        t = admin.getCurrentTopology();
        PingCollector collector = new PingCollector(t, logger);
        for (Entry<ResourceId, ServiceStatus> entry :
                 collector.getTopologyStatus().entrySet()) {
            ServiceStatus status = entry.getValue();
            assertEquals("running", status.name().toLowerCase());
        }

        /* Add new users. */
        if (secure) {
            createPasswordStore();
            planId = admin.getPlanner().
                createCreateUserPlan("add new user",
                                     USER,
                                     true /* isEnabled */,
                                     true /* isAdmin */,
                                     PASS.toCharArray(),
                                     null /* pwdLifetime*/);
            runPlan(admin, 6, planId);

            /*
             * Grant readwrite role to user so that client op tests go
             * smoothly
             */
            planId = admin.getPlanner().createGrantPlan(
                "grant roles to user", USER,
                Collections.singleton("readwrite"));
            runPlan(admin, 7, planId);

            loginMgr =
                KVStoreLogin.getRepNodeLoginMgr(testhost,
                                                bp1.getRegistryPort(),
                                                new PasswordCredentials(
                                                    USER, PASS.toCharArray()),
                                                kvstoreName);
        }
        /* Start a mock stream manager for multi-region table mode. */
        boolean triedOnce = false;
        while (true) {
            try {
                KVStore kvstore = admin.getInternalKVStore();
                TestBase.mrTableSetUp(kvstore);
                break;
            } catch (IllegalStateException e) {
                /*
                 * Setting up the table metadata system table can cause the
                 * internal kvstore to be closed, resulting in an
                 * IllegalStateException if it is accessed again. Retry if this
                 * is the first time so that we get a new KVStore instance.
                 */
                if (triedOnce) {
                    throw e;
                }
                triedOnce = true;
            }
        }
    }

    /**
     * Creates a password store for auto login of client tests. The filestore
     * is used currently.
     */
    private void createPasswordStore()
        throws Exception {
        final File fileLoc = new File(testDir, "pwdfile");
        fileLoc.delete();
        final PasswordManager pwdMgr = PasswordManager.load(
            "oracle.kv.impl.security.filestore.FileStoreManager");
        assertNotNull(pwdMgr);

        PasswordStore pwdfile = pwdMgr.getStoreHandle(fileLoc);
        assertTrue(!pwdfile.exists());
        pwdfile.create(null /* auto login */);
        assertTrue(pwdfile.exists());
        pwdfile.setSecret(USER, PASS.toCharArray());
        pwdfile.save();
    }

    /**
     * Execute command line commands after starting.
     */
    private void runCommands(String[] args)
        throws Exception {

        for (int i = 0; i < args.length; i += 1) {
            final String arg = args[i];
            if (arg.equals(STOP_ALL_MASTERS)) {
                stopAllMasters();
                continue;
            }
            if (arg.equals(STOP_ALL_REPLICAS)) {
                stopAllReplicas();
                continue;
            }
            if (arg.equals(SECURITY_FLAG)) {
                /* Handled in main. */
                continue;
            }
            if (arg.equals(STORAGE_SIZE)) {
                /* Handled in main. */
                i += 1;
                continue;
            }
            throw new IllegalArgumentException("Unknown arg: " + arg);
        }
    }

    /**
     * Used to put all rep groups into a state where only replicas are running
     * and there is not a quorum of nodes for holding an election.  In a two
     * node group, this is done simply by killing the master.
     */
    private void stopAllMasters()
        throws Exception {

        stopRepNodes(State.MASTER);
    }

    /**
     * Used to put all rep groups into a state where only the master is running
     * and there is not a quorum for ack'ing a commit.
     */
    private void stopAllReplicas()
        throws Exception {

        stopRepNodes(State.REPLICA);
    }

    private void stopRepNodes(State repState)
        throws Exception {

        final Topology topology = admin.getCurrentTopology();
        final RegistryUtils regUtils =
            new RegistryUtils(topology, loginMgr, logger);

        for (final RepGroup rg : topology.getRepGroupMap().getAll()) {
            for (final RepNode rn : rg.getRepNodes()) {

                final RepNodeAdminAPI rna =
                    regUtils.getRepNodeAdmin(rn.getResourceId());

                final RepNodeStatus status = rna.ping();

                if (repState.equals(status.getReplicationState())) {
                    /* use the SNA to stop the RN to avoid restart */
                    StorageNodeAgentAPI snai =
                        regUtils.getStorageNodeAgent(rn.getStorageNodeId());
                    snai.stopRepNode(rn.getResourceId(), true, false);
                }
            }
        }
    }

    /**
     * Stops the services.
     */
    void stop()
        throws Exception {

        if (admin != null) {
            /* Create and execute a StopRepNodesPlan */
            Topology t = admin.getCurrentTopology();
            int planId =
                admin.getPlanner().createStopAllRepNodesPlan("StopAllRN");
            runPlan(admin, 7, planId);

            PingCollector collector = new PingCollector(t, logger);
            for (Entry<ResourceId, ServiceStatus> entry :
                collector.getTopologyStatus().entrySet()) {
                if (entry.getKey().getType() ==
                    ResourceId.ResourceType.REP_NODE) {
                    ServiceStatus status = entry.getValue();
                    assertEquals("unreachable",
                                  status.name().toLowerCase());
                }
            }
        }

        /*
         * Only rep nodes are every really shutdown. Shut down the admin and
         * SNAs just to exercise the code.
         */
        adminService.stop(true, "test");
        if (sna1 != null) {
            sna1.shutdown(true /* stopServices */, false /* force */,
                          null /* reason */, null /* authCtx */,
                          SerialVersion.CURRENT);
        }
        if (sna2 != null) {
            sna2.shutdown(true /* stopServices */, false /* force */,
                          null /* reason */, null /* authCtx */,
                          SerialVersion.CURRENT);
        }

        LoggerUtils.closeAllHandlers();
        TestBase.mrTableTearDown();
    }

    /** Execute a plan, verify its end state, verify its id */
    private void runPlan(Admin admin1,
                         int expectedNumUserPlans,
                         int planId) {

        final Map<Integer, Plan> plans =
                admin1.getPlanRange(1, 20, null /*owner*/);

        /* Remove system plans */
        final Iterator<Plan> itr = plans.values().iterator();
        while (itr.hasNext()) {
            if (itr.next().isSystemPlan()) {
                itr.remove();
            }
        }
        assertEquals(plans.size(), expectedNumUserPlans);

        Plan plan = admin1.getPlanById(planId);
        assertEquals(plan.getId(), planId);

        admin1.approvePlan(plan.getId());
        admin1.savePlan(plan, Admin.CAUSE_APPROVE);
        admin1.executePlan(planId, false);
        admin1.awaitPlan(planId, 0, null);
        admin1.assertSuccess(planId);
    }

    /**
     * A main exists so that this class can be executed in a separate process
     * to allow manipulation of classpath.  This makes it possible to run the
     * client tests against kvclient.jar but use kvstore.jar for this class.
     *
     * The first argument should be the name of KVStore.
     *
     * The following command args may be passed (use constants at top of file):
     *
     *  -stopAllMasters
     *    See #stopAllMasters
     *
     *  -stopAllReplicas
     *    See #stopAllReplicas
     *
     * And an optionally flag:
     *
     *  -security
     *    To build and start a secured store.
     */
    public static void main(String[] args)
        throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException(
                "KVStore name argument is required");
        }

        final String kvstoreName = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        TestUtils.clearTestDirectory();

        boolean secure = false;
        long storageSize = 0;

        for (int i = 0; i < args.length; i += 1) {
            final String arg = args[i];
            if (arg.equals(SECURITY_FLAG)) {
                secure = true;
                continue;
            }
            if (arg.equals(STORAGE_SIZE)) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException(
                        "Need two args after " + STORAGE_SIZE);
                }
                i += 1;
                storageSize = new SizeParameter("", args[i]).asLong();
                continue;
            }
        }
        ClientTestServices cts = new ClientTestServices(kvstoreName);
        cts.start(secure, storageSize);
        cts.runCommands(args);
        cts.printConfig(System.err);

        /**
         * Don't exit on purpose.  The caller will kill the process.
         */
        while (true) {
            try {
                Thread.sleep(60000);
            } catch (InterruptedException ignored) {
                return;
            }
        }
    }
}
