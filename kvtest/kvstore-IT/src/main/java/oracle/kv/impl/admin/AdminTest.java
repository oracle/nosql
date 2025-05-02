/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static oracle.kv.util.CreateStore.MB_PER_SN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.LogRecord;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.RMIFailed;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.DatacenterParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.plan.DeploymentInfo;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.admin.topo.Validations;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.measurement.LatencyResult;
import oracle.kv.impl.measurement.Measurement;
import oracle.kv.impl.measurement.PerfStatType;
import oracle.kv.impl.monitor.Monitor;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.monitor.views.ServiceStatusTracker;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.monitor.StatsPacket;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupMap;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.StorageNodeMap;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.PingCollector;

import com.sleepycat.je.rep.NodeType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;



/**
 * Tests the Admin, interfaces and persistability.
 */
@RunWith(FilterableParameterized.class)
public class AdminTest extends AdminTestBase {

    private final String COMMENT1 = "comment 1";
    private final String COMMENT2 = "comment 2";
    private final String COMMENT3 = "comment 3";
    private final String COMMENT4 = "comment 4";

    private int nPlansCreatedSuccessfully = 0;
    private int nAdmins = 0;
    protected int nStorageNodes = 0;
    protected final PlanStateVerifier planStateVerifier = new PlanStateVerifier();

    /* The max number of topo changes maintained by the Admin. */
    private final String maxTopoChanges;

    /* Constructed in makeStatsPacket, used in checkEvents */
    private static String singleIntPerfEventSubStr;
    private static String singleCumPerfEventSubStr;
    private static String multiIntPerfEventSubStr;
    private static String multiCumPerfEventSubStr;

    private final static int startPort4 = 13290;
    private final PortFinder portFinder4 = new PortFinder(startPort4, haRange);
    private StorageNodeAgent extraSn;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
        ExpectedStatuses.reset();
        /* Configure max topo changes. */
        atc.getParams().getAdminParams().getMap().
            setParameter(ParameterState.AP_MAX_TOPO_CHANGES, maxTopoChanges);
        /*
         * Use a shorter EventRecorder polling interval so we don't need to
         * wait for long to see events
         */
        atc.getParams().getAdminParams().getMap()
            .setParameter(ParameterState.ADMIN_EVENT_RECORDER_POLLING_INTERVAL,
                          "250 ms");
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();

        if (extraSn != null) {
            extraSn.shutdown(true, true);
        }
    }

    /**
     * @return Max topology change for parameterized tests.
     */
    @Parameterized.Parameters(name="Max_topo_changes={0}")
    public static List<Object[]> genParams() {
        if (PARAMS_OVERRIDE != null) {
            return PARAMS_OVERRIDE;
        }
        return Arrays.asList(
            new Object[][] {{ParameterState.AP_MAX_TOPO_CHANGES_DEFAULT},
            /* 10 guarantees pruning, since it's > num partitions. */
                            {"10"}});
    }

    /**
     * Set the max topo changes for this run of the test suite.
     */
    public AdminTest(String topoChanges) {
       maxTopoChanges = topoChanges;
    }

    /**
     * Basic simple admin exercise, with event checking.
     */
    @Test
    public void testBasic()
        throws Exception {

        runBasic(false);
    }

    /**
     * The same as testBasic, but with the event expiry period set low, so
     * there should be no events in the database at the end.
     */
    @Test
    public void testEventTruncation()
        throws Exception {

        runBasic(true);
    }

    /**
     * Tests maximum number of events that may be stored in the
     * EventStore. Uses the default and non-default number of
     * events.
     */
    @Test
    public void testMaxEvents()
        throws Exception {
        /* Use default or 1000 events */
        int maxEvents =
            (Integer.parseInt(maxTopoChanges) < 1000) ? 0 : 1000;
        runMaxEvents(maxEvents);
    }

    /**
     * Exercise the basic interfaces.
     * @throws InterruptedException
     * @throws RemoteException
     */
    private void runBasic(boolean truncateEvents)
        throws Exception {

        AdminServiceParams adminServiceParams = atc.getParams();

        Admin admin = new Admin(adminServiceParams);

        try {
            setSmallHeap(admin);

            Topology t = admin.getCurrentTopology();
            assertEquals(kvstoreName, t.getKVStoreName());
            Parameters p = admin.getCurrentParameters();
            assertEquals(kvstoreName, p.getGlobalParams().getKVStoreName());

            /*
             * Put a recordable LogRecord into the stream during the lifetime
             * of the first Admin instance.
             */
            admin.getLogger().severe("severe message 1");

            /* Give the admin a chance to start before killing it */
            Thread.sleep(1000);
            admin.shutdown(false, "test");

            /* Open it again. */
            admin = new Admin(adminServiceParams);
            setSmallHeap(admin);

            t = admin.getCurrentTopology();
            assertEquals(kvstoreName, t.getKVStoreName());
            p = admin.getCurrentParameters();
            assertEquals(kvstoreName, p.getGlobalParams().getKVStoreName());
            admin.getMonitor().trackPlanStateChange(planStateVerifier);

            nStorageNodes = 0;

            if (truncateEvents == true) {

                /*
                 * During this test, we are going to set the expiry age to
                 * zero, and verify that the critical events repository is
                 * empty at the end.
                 */
                admin.getParams().getAdminParams().setEventExpiryAge("0 DAYS");

                /*
                 * We need to prune at every opportunity.
                 */
                admin.setEventStoreAgingFrequency(1);
            }

            /*
             * Put a second recordable message into the stream.
             */
            admin.getLogger().severe("severe message 2");

            /* Deploy a Datacenter. */
            Datacenter dc = deployDatacenter(admin, 2);

            /* Deploy a StorageNode */
            StorageNode sn = firstDeployStorageNode
                                 (admin, dc,
                                  portFinder1.getRegistryPort(),
                                  COMMENT1);

            /* Deploy an AdminService */
            deployAdminService(admin, sn, true);

            /* Deploy another StorageNode */
            sn = firstDeployStorageNode
                     (admin, dc, portFinder2.getRegistryPort(), COMMENT2);

            /* Deploy another AdminService */
            deployAdminService(admin, sn, true);

            /* Deploy some RepNodes */
            deployKVStore(admin, 2, 100);

            /*
             * Put a third recordable message into the stream.
             */
            admin.getLogger().severe("severe message 3");

            /*
             * Put a recordable PerfEvent into the stream. We want to set the
             * latency ceiling so that this PerfEvent is marked as
             * "needsAlert", so it will be recorded by the EventRecorder.
             */
            RepNodeId rnid = new RepNodeId(1,1);
            setLatencyCeiling(admin, rnid);

            long currentMillis = new Date().getTime();
            StatsPacket packet =
                makeStatsPacket(currentMillis - 20, currentMillis);
            admin.getMonitor().publish
                                   (rnid, Arrays.asList((Measurement)packet));

            /* Update admin params before verify */
            updateAdminParams(admin, adminServiceParams.getAdminParams(), sna1);

            /* Try out the VerifyConfiguration capability. */
            VerifyConfiguration verifier =
                new VerifyConfiguration(admin, true, true, false,
                                        LoggerUtils.getLogger(this.getClass(),
                                                              "AdminTest"));
            /*
             * Beware left-to-right order of evaluation of args to assertTrue.
             */
            verifyTopology(verifier);

            if (truncateEvents == false) {
                /* Verify recording of CriticalEvents. */
                checkEvents(admin);
            } else {
                /* Verify that the CriticalEvents repository was truncated. */
                checkNoEvents(admin);
            }

            /* Quick check of collecting the version info */
            assertEquals(admin.getStoreVersion().
                                    compareTo(KVVersion.CURRENT_VERSION), 0);
            verifyTopologyPruning(admin);
        } finally {
            admin.shutdown(true, "test");
        }
    }

    /**
     * Verifies that the topology maintained by the Admin is pruned as
     * specified in its configuration.
     */
    private void verifyTopologyPruning(Admin admin) {
        final int max =
            admin.getParams().getAdminParams().getMaxTopoChanges();
        final int topoChanges = admin.getCurrentTopology().
            getChangeTracker().getChanges().size();

        assertTrue(topoChanges <= max);
    }

    /**
     * Update the admin DB and SNA configuration file to match changes made to
     * the admin service parameters.
     */
    private void updateAdminParams(Admin admin,
                                   AdminParams adminParams,
                                   StorageNodeAgent sna)
        throws Exception {

        admin.updateParams(adminParams);
        final StorageNodeAgentAPI snaApi =
            RegistryUtils.getStorageNodeAgent(sna.getStoreName(),
                                              sna.getHostname(),
                                              sna.getRegistryPort(),
                                              sna.getStorageNodeId(),
                                              null,
                                              logger);
            snaApi.newAdminParameters(adminParams.getMap());
    }

    /**
     * Test that plans are idempotent
     */
    @Test
    public void testRepeats()
        throws Exception {

        AdminServiceParams adminServiceParams = atc.getParams();

        Admin admin = new Admin(adminServiceParams);
        setSmallHeap(admin);

        Topology t = admin.getCurrentTopology();
        assertEquals(kvstoreName, t.getKVStoreName());
        Parameters p = admin.getCurrentParameters();
        assertEquals(kvstoreName, p.getGlobalParams().getKVStoreName());

        /* Do a forced shutdown to ensure that restart works */
        admin.shutdown(true, "test");

        /* Open it again. */
        admin = new Admin(adminServiceParams);
        setSmallHeap(admin);

        t = admin.getCurrentTopology();
        assertEquals(kvstoreName, t.getKVStoreName());
        admin.getMonitor().trackPlanStateChange(planStateVerifier);
        p = admin.getCurrentParameters();
        assertEquals(kvstoreName, p.getGlobalParams().getKVStoreName());

        nStorageNodes = 0;
        /* Deploy a Datacenter, and repeat that plan */
        Datacenter dc = deployDatacenter(admin, 3);
        dc = deployDatacenter(admin, 3);

        try {
            deployDatacenter(admin, 5);
            fail ("Should fail because the repfactor was changed");
        } catch (IllegalCommandException expected) {
        }

        /* Deploy a StorageNode. Repeat the plan */
        int sn1RegPort = portFinder1.getRegistryPort();
        StorageNode sn = firstDeployStorageNode(admin, dc, sn1RegPort,
                                                COMMENT1);
        repeatDeployStorageNode(admin, dc, sn1RegPort, COMMENT1);

        try {
            StorageNodeParams snParams =
                new StorageNodeParams("localhost", sn1RegPort, null);
            snParams.setCapacity(7);
            deployStorageNode(admin, dc, snParams);
            fail("should fail because the params were changed.");
        } catch (IllegalCommandException expected) {
        }

        /* Deploy an AdminService */
        deployAdminService(admin, sn, true);
        deployAdminService(admin, sn, false);

        /* Deploy more storage nodes */
        int sn2RegPort = portFinder2.getRegistryPort();
        sn = firstDeployStorageNode(admin, dc, sn2RegPort, COMMENT2);
        sn = firstDeployStorageNode(admin, dc, portFinder3.getRegistryPort(),
                                    COMMENT3);
        repeatDeployStorageNode(admin, dc, sn2RegPort, COMMENT2);

        /* Deploy another AdminService */
        deployAdminService(admin, sn, true);

        /*
         * Deploy some RepNodes, and repeat it
         */
        deployKVStore(admin, 3, 100);
        deployKVStore(admin, 3, 100);

        try {
            deployKVStore(admin, 3, 50);
            fail ("Should fail because the number of partitions was changed");
        } catch (IllegalCommandException expected) {
        }

        startExtraStorageNode();
        try {
            firstDeployStorageNode(admin, dc, portFinder4.getRegistryPort(),
                                   COMMENT4);
            deployKVStore(admin, 3, 100);
            fail ("Should fail because the number of SNs was changed");
        } catch (IllegalCommandException expected) {
            removeStorageNode(admin, extraSn);
        }

        /* Repeat the deploy of a storage node after repNodes exist.*/
        repeatDeployStorageNode(admin, dc, sn2RegPort, COMMENT2);

        /* Run the stop plan twice.*/
        Set<RepNodeId> stopTargets = pickTargets(admin, 2);

        /* All RNs should be running at this point. */
        checkServiceStatus(admin, ServiceStatus.RUNNING, stopTargets,
                           ServiceStatus.RUNNING);

        /* Update admin params before verify */
        updateAdminParams(admin, adminServiceParams.getAdminParams(), sna1);

        /* Try out the VerifyConfiguration capability. */
        VerifyConfiguration verifier =
            new VerifyConfiguration(admin, true, true, false,
                                    LoggerUtils.getLogger(this.getClass(),
                                    "AdminTest"));

        verifyTopology(verifier);

        stopRepNodes(admin, stopTargets, true);
        stopRepNodes(admin, stopTargets, true);

        /* restart the same nodes twice */
        startRepNodes(admin, stopTargets);
        startRepNodes(admin, stopTargets);
        verifyTopology(verifier);

        /* Deploying the datacenter again should have no effect. */
        deployDatacenter(admin, 3);

        verifyTopology(verifier);
        verifyTopologyPruning(admin);
        admin.shutdown(true, "test");
    }

    /*
     * Uses the specified verifier to verify the topology, filtering
     * InsufficientAdmins and MissingRootDirectorySize violations.
     */
    private void verifyTopology(VerifyConfiguration verifier) {
        verifier.verifyTopology();
        final VerifyResults results = verifier.getResults();
        final Iterator<Problem> itr = results.getViolations().iterator();
        while (itr.hasNext()) {
            if (itr.next() instanceof InsufficientAdmins) {
                itr.remove();
            }
        }

        final Iterator<Problem> warnItr = results.getWarnings().iterator();
        while (warnItr.hasNext()) {
            if (warnItr.next() instanceof MissingRootDirectorySize) {
                warnItr.remove();
            }
        }
        assertTrue(results.display(), results.okay());
    }

    /* For debugging */
    @SuppressWarnings("unused")
    private void showRNP(Admin admin) {
        Parameters currentParams = admin.getCurrentParameters();
        for (RepNodeParams rnp : currentParams.getRepNodeParams()) {
            System.err.println("-----------");
            System.err.println(rnp.getMap());
        }
    }

    /**
     * Compose a totally fake topology and run the verify command. We expect
     * errors for all of it. TODO, it is now very hard to verify a topology
     * without a valid admin, because VerifyConfiguration expects to make
     * actual remote calls to services. Find a new way to rig this.
     */
    public void xtestVerifyBadTopology() {

        AdminServiceParams adminServiceParams = atc.getParams();
        Admin admin = new Admin(adminServiceParams);

        /* Make an artificial topology */
        Topology topo = new Topology("BadTopology");
        Datacenter dc1 = topo.add(
            Datacenter.newInstance("EC-datacenter", 2,
                                   DatacenterType.PRIMARY, false, false));
        Datacenter dc2 = topo.add(
            Datacenter.newInstance("JP-datacenter", 2,
                                   DatacenterType.SECONDARY, false, false));
        StorageNode sn1 = topo.add(new StorageNode(dc1,"sn1-hostname", 5000));
        StorageNode sn2 = topo.add(new StorageNode(dc2,"sn2-hostname", 5001));

        RepGroup rg1 = topo.add(new RepGroup());
        RepNode rn1 = rg1.add(new RepNode(sn1.getResourceId()));
        RepNode rn2 = rg1.add(new RepNode(sn2.getResourceId()));

        RepGroup rg2 = topo.add(new RepGroup());
        RepNode rn3 = rg2.add(new RepNode(sn1.getResourceId()));
        RepNode rn4 = rg2.add(new RepNode(sn2.getResourceId()));



        /* Also make an artificial set of parameters. */
        //        Parameters params = new Parameters("BadTopology");
        DatacenterParams dcp1 =
            (new DatacenterParams(dc1.getResourceId(), "Lexington"));
        DatacenterParams dcp2 =
            (new DatacenterParams(dc2.getResourceId(), "Tokyo"));

        admin.saveTopoAndParams(topo, DeploymentInfo.makeStartupDeploymentInfo(),
                                dcp1, null);
        admin.saveTopoAndParams(topo, DeploymentInfo.makeStartupDeploymentInfo(),
                                dcp2, null);
        admin.updateParams((new StorageNodeParams(sn1.getResourceId(),
                                                 "sn1-hostname", 5000, null)),
                            null);

        admin.updateParams((new StorageNodeParams(sn2.getResourceId(),
                                                 "sn2-hostname", 5000, null)),
                            null);

        admin.updateParams(new RepNodeParams(sn1.getResourceId(),
                                             rn1.getResourceId(),
                                             false, null, 0, null, null,
                                             NodeType.ELECTABLE));
        admin.updateParams(new RepNodeParams(sn2.getResourceId(),
                                             rn2.getResourceId(),
                                             false, null, 0, null, null,
                                             NodeType.SECONDARY));
        admin.updateParams(new RepNodeParams(sn1.getResourceId(),
                                             rn3.getResourceId(),
                                             false, null, 0, null, null,
                                             NodeType.ELECTABLE));
        admin.updateParams(new RepNodeParams(sn2.getResourceId(),
                                             rn4.getResourceId(),
                                             false, null, 0, null, null,
                                             NodeType.SECONDARY));


        VerifyConfiguration verify =
            new VerifyConfiguration(admin, true, true, false,
                                    LoggerUtils.getLogger(this.getClass(),
                                    "AdminTest"));
        verify.verifyTopology(topo, null, admin.getCurrentParameters(),
                              false /* topo not deployed */);
        VerifyResults results = verify.getResults();
        List <Problem> violations = results.getViolations();
        for (Problem p : violations) {
            assertTrue((p instanceof RMIFailed) ||
                       (p instanceof Validations.RulesProblem));
        }
        assertEquals(violations.toString(), 12, violations.size());
    }

    /**
     * Create a set of numChosen RepNodes as targets for testing stops and
     * starts..
     */
    private Set<RepNodeId> pickTargets(Admin admin, int numTargets) {

        Set<RepNodeId> targetSet = new HashSet<RepNodeId>();
        int numChosen = 0;
        for (RepNodeId id : admin.getCurrentTopology().getRepNodeIds()) {
            targetSet.add(id);
            if (++numChosen == numTargets) {
                break;
            }
        }

        return targetSet;
    }

    /**
     * Create and run a DeployDatacenterPlan
     */
    Datacenter deployDatacenter(Admin admin, int repFactor) {

        int ddcid = admin.getPlanner().
            createDeployDatacenterPlan("deploy data center", "Miami", repFactor,
                                       DatacenterType.PRIMARY, false, false);

        runAndVerifyPlan(admin, ddcid);

        /* Check the resulting topology changes. */
        Topology t = admin.getCurrentTopology();

        DatacenterMap dcmap = t.getDatacenterMap();
        assertEquals(dcmap.size(), 1);

        Datacenter dc = dcmap.getAll().iterator().next();

        assertEquals(dc.getName(), "Miami");

        /* Check the resulting parameters changes. */

        Parameters p = admin.getCurrentParameters();
        assertNotNull(p.get(dc.getResourceId()));

        return dc;
    }

    protected StorageNode firstDeployStorageNode(Admin admin,
                                                 Datacenter dc,
                                                 int registryPort,
                                                 String comment) {

        nStorageNodes++;
        StorageNodeParams snParams =
            new StorageNodeParams("localhost", registryPort, comment);
        return deployStorageNode(admin, dc, snParams);
    }

    protected StorageNode repeatDeployStorageNode(Admin admin,
                                                  Datacenter dc,
                                                  int registryPort,
                                                  String comment) {

        StorageNodeParams snParams =
            new StorageNodeParams("localhost", registryPort, comment);
        return deployStorageNode(admin, dc, snParams);
    }

    private void removeStorageNode(Admin admin,
                                   StorageNodeAgent sna) {
        sna.shutdown(true, true);
        final StorageNodeId snId = sna.getStorageNodeId();

        int rsnId = admin.getPlanner().createRemoveSNPlan(
            "remove storage node", snId);
        runAndVerifyPlan(admin, rsnId);

        nStorageNodes--;

        /* Check the resulting topology changes. */
        Topology t = admin.getCurrentTopology();

        StorageNodeMap smap = t.getStorageNodeMap();
        assertEquals(smap.size(), nStorageNodes);

        Collection<StorageNode> allSNs = smap.getAll();
        for (StorageNode sn : allSNs) {
            assertNotEquals(sn.getResourceId(), snId);
        }

        /* Check the resulting parameters changes. */
        Parameters p = admin.getCurrentParameters();
        assertNull(p.get(snId));

        /* Check that the default sn pool was shrank */
        StorageNodePool pool = admin.getCurrentParameters().getStorageNodePool
            (Parameters.DEFAULT_POOL_NAME);
        assertEquals(pool.size(), nStorageNodes);
    }

    /* Create and run a DeployStorageNode plan. */
    protected StorageNode deployStorageNode(Admin admin,
                                            Datacenter dc,
                                            StorageNodeParams snParams) {

        int dsnid = admin.getPlanner().
            createDeploySNPlan("deploy storage node",
                               dc.getResourceId(), snParams);
        runAndVerifyPlan(admin, dsnid);

        /* Check the resulting topology changes. */
        Topology t = admin.getCurrentTopology();

        StorageNodeMap smap = t.getStorageNodeMap();
        assertEquals(smap.size(), nStorageNodes);

        Collection<StorageNode> allSNs = smap.getAll();

        StorageNode sn = null;
        for (StorageNode s : allSNs) {
            if (s.getRegistryPort() == snParams.getRegistryPort()) {
                sn = s;
                break;
            }
        }
        assertNotNull(sn);

        assertEquals(sn.getDatacenterId(), dc.getResourceId());

        /* Check the resulting parameters changes. */

        Parameters p = admin.getCurrentParameters();
        assertNotNull(p.get(sn.getResourceId()));

        /* Check that the default sn pool was augmented */

        StorageNodePool pool = admin.getCurrentParameters().getStorageNodePool
            (Parameters.DEFAULT_POOL_NAME);

        assertEquals(pool.size(), nStorageNodes);

        return sn;
    }

    private void startExtraStorageNode() throws Exception {
        if (extraSn == null) {
            extraSn = StorageNodeTestBase.createUnregisteredSNA
            (portFinder4, 1, "config4.xml", false, false, secured, MB_PER_SN);
        }
    }

    /* Create and run a DeployAdmin plan. */
    void deployAdminService(Admin admin,
                            StorageNode sn,
                            boolean notRepeat) {

        /* There should be nAdmins Admins in Parameters. */
        Parameters p = admin.getCurrentParameters();
        assertEquals(p.getAdminCount(), nAdmins);
        if (notRepeat) {
            nAdmins++;
        }

        int daid = admin.getPlanner().
            createDeployAdminPlan("deploy admin", sn.getStorageNodeId());

        runAndVerifyPlan(admin, daid);

        /* This Admin should be in Parameters now. */
        p = admin.getCurrentParameters();
        assertEquals(p.getAdminCount(), nAdmins);
        verifyAdminHelpers(p);
    }

    /* Create and run a DeployKVStore plan. */
    void deployKVStore(Admin admin, int repFactor, int numPartitions) {

        Parameters p = admin.getCurrentParameters();

        StorageNodePool pool = p.getStorageNodePool
            (Parameters.DEFAULT_POOL_NAME);

        /*
         * This method is called twice in some tests. So check for topo
         * existing before making the assert
         */
        if (admin.getCurrentTopology() == null) {
            assertFalse("Store should not be ready", admin.isStoreDeployed());
        }
        admin.createTopoCandidate("first", Parameters.DEFAULT_POOL_NAME,
                                  numPartitions, false,
                                  SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
        int dkvsid = admin.getPlanner().
            createDeployTopoPlan("firstPlan", "first", null);
        runAndVerifyPlan(admin, dkvsid);
        assertTrue("Store should be ready", admin.isStoreDeployed());

        /* Check the resulting topology changes. */
        Topology t = admin.getCurrentTopology();
        String printResult = TopologyPrinter.printTopology(t, null, true);
        logger.info(" deployed> " + printResult);
        RepGroupMap rmap = t.getRepGroupMap();
        assertEquals(rmap.size(), 1);

        RepGroup rg = rmap.getAll().iterator().next();

        Collection<RepNode> allRNs = rg.getRepNodes();

        /* Get a fresh copy of Parameters that reflects the plan results. */
        p = admin.getCurrentParameters();

        /* Verify that there is one RepNode per StorageNode. */
        List<StorageNodeId> dSNs = new ArrayList<StorageNodeId>();
        for (RepNode rn : allRNs) {
            StorageNodeId snid = rn.getStorageNodeId();

            assertTrue(pool.contains(snid));
            assertFalse(dSNs.contains(snid));
            dSNs.add(snid);

            /* Check the corresponding Parameters changes */
            assertNotNull(p.get(rn.getResourceId()));
        }
        assertEquals(dSNs.size(), repFactor);
    }

    /**
     * Stop the specified RepNodes, and verify the stoppage by pinging service
     * status.
     */
    void stopRepNodes(Admin admin, Set<RepNodeId> rns, boolean force) {
        int planId = admin.getPlanner().createStopServicesPlan
            ("stop some nodes", rns);
        runAndVerifyPlan(admin, planId, force);
        checkServiceStatus(admin, ServiceStatus.RUNNING, rns,
                           ServiceStatus.UNREACHABLE);
    }

    /**
     * Start the specified RepNodes, and verify that they are up by pinging
     * service status.
     */
    void startRepNodes(Admin admin, Set<RepNodeId> rns) {
        int planId = admin.getPlanner().createStartServicesPlan
            ("start some nodes", rns);
        runAndVerifyPlan(admin, planId);
        checkServiceStatus(admin, ServiceStatus.RUNNING, rns,
                           ServiceStatus.RUNNING);
    }

    /**
     * Set latency ceiling very low for repnode 1,1.
     */
    void setLatencyCeiling(Admin admin, RepNodeId rnid) {
        Parameters p = admin.getCurrentParameters();
        RepNodeParams rnp = p.get(rnid);
        rnp.setLatencyCeiling(1);
        int planId = admin.getPlanner().
            createChangeParamsPlan("change latency",
                                   new RepNodeId(1,1), rnp.getMap());
        runAndVerifyPlan(admin, planId);
    }

    private static Map<Integer, Plan> getAllUserPlans(Admin admin) {
        final TreeMap<Integer, Plan> collector = new TreeMap<>();
        int currentId = 1;
        while (true) {
            collector.putAll(admin.getPlanRange(currentId, 100, null /*owner*/));
            int nextId = collector.lastKey().intValue() + 1;
            if (currentId == nextId) {
                break;
            }
            currentId = nextId;
        }
        /* Remove any system plans */
        final Iterator<Plan> itr = collector.values().iterator();
        while (itr.hasNext()) {
            Plan p = itr.next();

            if (p.isSystemPlan()) {
                itr.remove();
            }
        }

        return collector;
    }

    private void runAndVerifyPlan(Admin admin, int planId) {
        runAndVerifyPlan(admin, planId, false);
    }

    private void runAndVerifyPlan(Admin admin, int planId, boolean force) {
        planStateVerifier.clear();

        final Map<Integer, Plan> plans = getAllUserPlans(admin);
        assertEquals(plans.size(), ++nPlansCreatedSuccessfully);

        Plan plan = admin.getPlanById(planId);
        assertEquals(plan.getId(), planId);

        admin.approvePlan(plan.getId());
        admin.savePlan(plan, Admin.CAUSE_APPROVE);
        admin.executePlan(planId, force);
        admin.awaitPlan(planId, 0, null);

        assertEquals(Plan.State.SUCCEEDED, plan.getState());

        /*
         * Check that there were plan state changes issued for this plan
         * execution. All successfully executed plans should have gone through
         * PENDING, APPROVED, RUNNING, SUCCEEDED
         */
        planStateVerifier.checkChanges(planId,
                                       Plan.State.APPROVED,
                                       Plan.State.RUNNING,
                                       Plan.State.SUCCEEDED);
    }

    private void checkServiceStatus(Admin admin,
                                    ServiceStatus defaultStatus,
                                    Set<RepNodeId> targets,
                                    ServiceStatus targetStatus) {
        PingCollector collector =
            new PingCollector(admin.getCurrentTopology(), logger);

        Map<ResourceId, ServiceStatus> pingMap = collector.getTopologyStatus();

        for (Entry<ResourceId, ServiceStatus> entry : pingMap.entrySet()) {
            ServiceStatus expected = defaultStatus;
            if (targets.contains(entry.getKey())) {
                expected = targetStatus;
            }

            assertEquals(entry.getKey() + " not in expected state of " +
                         expected, expected, entry.getValue());
        }
    }

    private static class PlanStateVerifier
        implements ViewListener<PlanStateChange> {

        private final List<PlanStateChange> seen = new ArrayList<>();

        void clear() {
            seen.clear();
        }

        /**
         * @param newData newly computed results, which haven't been previously
         * announced.
         */
        @Override
        synchronized public void newInfo(ResourceId resourceId,
                                         PlanStateChange newData) {
            seen.add(newData);
        }

        /**
         * Verify that we have only seen the plan state changes we expect.
         */
        synchronized void checkChanges(int planId,
                                       Plan.State... expectedPlanStates) {
            /* Extract the changes for the specified plan. */
            final List<PlanStateChange> seenForPlan = new ArrayList<>();
            for (PlanStateChange psc : seen) {
                if (psc.getPlanId() == planId) {
                    seenForPlan.add(psc);
                }
            }

            assertEquals(expectedPlanStates.length, seenForPlan.size());
            for (int i = 0; i < expectedPlanStates.length; i++) {
                PlanStateChange oneChange = seenForPlan.get(i);
                assertEquals(planId, oneChange.getPlanId());
                assertEquals(expectedPlanStates[i], oneChange.getStatus());
            }
        }
    }

    /*
     * Set the heap small for each process, since this test makes quite a few.
     */
    private void setSmallHeap(Admin admin) {
        ParameterMap map = admin.copyPolicy();
        int rnHeapPercent =
            Integer.parseInt(ParameterState.SN_RN_HEAP_PERCENT_DEFAULT);
        int rnHeapMB = (rnHeapPercent * MB_PER_SN) / 100;
        map.setParameter(ParameterState.JVM_RN_OVERRIDE, "-Xmx" + rnHeapMB + "M");
        admin.setPolicy(map);
    }

    private static class StatInfo {
        public ResourceId rid;
        public ServiceStatus status;
        public boolean seen;
        public StatInfo(ResourceId rid, ServiceStatus status) {
            this.rid = rid;
            this.status = status;
            this.seen = false;
        }
        @Override
        public String toString() {
            return String.format("StatInfo[rid=%s status=%s seen=%s]",
                                 rid,
                                 status,
                                 seen);
        }
    }

    /**
     * Codify the manipulation of the expected statuses.
     */
    private static class ExpectedStatuses {
        static List<StatInfo> statuses;

        static {
            statuses= new ArrayList<StatInfo>();

            for (final ServiceStatus status : new ServiceStatus[] {
                    ServiceStatus.STARTING, ServiceStatus.RUNNING }) {
                for (final ResourceId id : new ResourceId[] {
                        new StorageNodeId(1), new StorageNodeId(2),
                        new RepNodeId(1, 1), new RepNodeId(1, 2) }) {
                    statuses.add(new StatInfo(id, status));
                }
            }
        }

        /**
         * Record that a particular status was seen.  If the given status
         * wasn't in the expected list, return false; otherwise set the
         * associated "seen" field and return true.
         */
        static boolean setSeen(ServiceChange sc) {
            boolean found = false;
            for (StatInfo si : statuses) {
                if (si.rid.equals(sc.getTarget()) &&
                    si.status == sc.getStatus()) {
                    assertEquals("target:" + sc.getTarget() + " status:" +
                    sc.getStatus(), false, si.seen);
                    si.seen = true;
                    found = true;
                    break;
                }
            }

            return found;
        }

        static List<StatInfo> getExpectedEvents() {
            return new ArrayList<StatInfo>(statuses);
        }

        static List<StatInfo> getSeenEvents() {
            final List<StatInfo> result = new ArrayList<>();
            for (final StatInfo si : statuses) {
                if (si.seen) {
                    result.add(si);
                }
            }
            return result;
        }

        /**
         * Return true if all entrys have the "seen" field set.
         */
        static boolean isAllSeen() {
            for (StatInfo si : statuses) {
                if (si.seen == false) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Generated an appropriate "expected item not seen" message.
         */
        static String getFirstUnseenMessage() {
            for (StatInfo si : statuses) {
                if (si.seen == false) {
                    return "Missing expected status event for id:" + si.rid +
                        " status:" + si.status;
                }
            }
            return "";
        }

        /**
         * Set all the "seen" fields to false.
         */
        static void reset() {
            for (StatInfo si : statuses) {
                si.seen = false;
            }
        }
    }

    /**
     * Do a spot check on the EventRecorder.
     */
    private void checkEvents(Admin admin) throws InterruptedException {
        List<String> expectedLogMessages = new ArrayList<String>();
        expectedLogMessages.add("severe message 1");
        expectedLogMessages.add("severe message 2");
        expectedLogMessages.add("severe message 3");

        /*
         * At this point, we have to wait until all the expected ServiceChange
         * messages have been delivered to the Admin.  This call achieves that
         * by subscribing to the ServiceStatusTracker and waiting until all the
         * expected events have gone by.
         */
        verifyStatusesReceived(admin);
        ExpectedStatuses.reset();

        /*
         * Now, ensure that the EventRecorder thread has completed its task.
         */
        admin.syncEventRecorder();

        /*
         * Get all the recorded events in the database, starting at the epoch.
         */
        List<CriticalEvent> events =
            admin.getEvents(0L, 0L, CriticalEvent.EventType.ALL);

        /*
         * Check the reported events against the expected ones.
         */
        int expectedLogIndex = 0;
        int expectedPerfIndex = 0;
        for (CriticalEvent ev : events) {
//            System.out.println("CRITICAL EVENT " + ev.getEventType() + " " + ev.getLogEvent().getMessage());
//            System.out.println("LOOKING FOR " + expectedLogMessages.get(expectedLogIndex));
            switch (ev.getEventType()) {
            case STAT:
                ServiceChange sc = ev.getStatusEvent();

                boolean found = ExpectedStatuses.setSeen(sc);
                if (! found) {

                    /*
                     * Sometimes we see the waiting status, but we can't always
                     * expect it.
                     */
                    assertEquals(ServiceStatus.WAITING_FOR_DEPLOY,
                                 sc.getStatus());
                }
                break;

            case LOG:
                LogRecord lr = ev.getLogEvent();
                assertTrue
                    ("Unexpected log message: " + lr.getMessage(),
                     lr.getMessage().endsWith
                     (expectedLogMessages.get(expectedLogIndex++)));
                break;

            case PERF:
                PerfEvent pe = ev.getPerfEvent();
                String ps = pe.toString();
                assertTrue("Unexpected single interval " +
                           "sub-string of perf event: " + ps,
                           ps.contains(singleIntPerfEventSubStr));
                assertTrue("Unexpected single interval cummulative " +
                           "sub-string of perf event: " + ps,
                           ps.contains(singleCumPerfEventSubStr));
                assertTrue("Unexpected multi interval non-cummulative " +
                           "sub-string of perf event: " + ps,
                           ps.contains(multiIntPerfEventSubStr));
                assertTrue("Unexpected multi interval cummulative " +
                           "sub-string of perf event: " + ps,
                           ps.endsWith(multiCumPerfEventSubStr));

                expectedPerfIndex++;
                break;

            default:
                /* We don't expect other types of events in this test. */
                assertTrue("unexpected event type " + ev.getEventType(),
                           false);
            }
        }

        assertTrue(ExpectedStatuses.getFirstUnseenMessage(),
                   ExpectedStatuses.isAllSeen());

        assertEquals(expectedLogMessages.size(), expectedLogIndex);

        assertEquals(1, expectedPerfIndex);
    }

    private void checkNoEvents(Admin admin) {

        admin.syncEventRecorder();
        List<CriticalEvent> events =
            admin.getEvents(0L, 0L, CriticalEvent.EventType.ALL);

        /*
         * The number will sometimes be one, because the event that triggered
         * the last pruning will not itself be pruned.  It is an artificial
         * situation for the ager to be pruning the very same event that
         * triggered it, so I am not worried about this discrepancy.  It is
         * sufficient that the number of events is small, showing that the
         * pruner is doing its job.
         */
        assertTrue("More than one event where we expected only 0 or 1",
                   events.size() <= 1);
    }

    /**
     * Go directly to the ServiceChangeTracker to wait until all the expected
     * service change events have been received.
     */
    private void verifyStatusesReceived(Admin admin)
        throws InterruptedException {

        long since = 0;
        Monitor m = admin.getMonitor();
        ServiceStatusTracker statusTracker = m.getServiceChangeTracker();

        /* This loop implements the Tracker polling protocol. */
        while (true) {
            Tracker.RetrievedEvents<ServiceChange> eventsContainer;

            final long timeout = 40000; /* wait at most 40 seconds. */
            final long maxTime = System.currentTimeMillis() + timeout;
            while (true) {
                eventsContainer = statusTracker.retrieveNewEvents(since);

                if (eventsContainer.size() != 0) {
                    break;
                }

                if (System.currentTimeMillis() >= maxTime) {
                    fail("Timeout waiting for ServiceChange events" +
                         "\nExpected: " + ExpectedStatuses.getExpectedEvents() +
                         "\nSeen: " + ExpectedStatuses.getSeenEvents());
                }
                Thread.sleep(1000);
            }

            since = eventsContainer.getLastSyntheticTimestamp();

            /*
             * For each service status received, mark it as "seen in the
             * expectedStatuses list.
             */
            for (ServiceChange sc : eventsContainer.getEvents()) {
                ExpectedStatuses.setSeen(sc);
            }

            /* Now check to see if we have seen all the expected statuses. */
            if (ExpectedStatuses.isAllSeen()) {
                /* We're done! */
                return;
            }
        }
    }

    private static StatsPacket makeStatsPacket(long start, long end) {

        /* For constructing the latency info components contained in the
         * expected sub-string.
         */
        final long singleIntTotalOps=400_000_000_000L;
        final long singleIntTotalReq=500_000_000_000L;
        final int singleIntMin=1;
        final int singleIntMax=100;
        final int singleIntAvg=10;
        final int singleInt95Th=95;
        final int singleInt99Th=99;
        final int singleIntOverflow=136;

        final long singleCumTotalOps=1_000_000_000_000L;
        final long singleCumTotalReq=2_000_000_000_000L;
        final int singleCumMin=2;
        final int singleCumMax=200;
        final int singleCumAvg=20;
        final int singleCum95Th=195;
        final int singleCum99Th=199;
        final int singleCumOverflow=236;

        final long multiIntTotalOps=600_000_000_000L;
        final long multiIntTotalReq=700_000_000_000L;
        final int multiIntMin=1;
        final int multiIntMax=200;
        final int multiIntAvg=10;
        final int multiInt95Th=195;
        final int multiInt99Th=199;
        final int multiIntOverflow=336;

        final long multiCumTotalOps = 3_000_000_000_000L;
        final long multiCumTotalReq = 4_000_000_000_000L;
        final int multiCumMin = 2;
        final int multiCumMax = 200;
        final int multiCumAvg = 50;
        final int multiCum95Th = 195;
        final int multiCum99Th = 199;
        final int multiCumOverflow = 436;

        StatsPacket packet = new StatsPacket(start, end, "rg1-rn1", "rg1");

        /* Add single interval non-commulative latency info to the packet. */
        packet.add(new LatencyInfo
                   (PerfStatType.USER_SINGLE_OP_INT, start, end,
                    new LatencyResult(
                        singleIntTotalReq, singleIntTotalOps,
                        MILLISECONDS.toNanos(singleIntMin),
                        MILLISECONDS.toNanos(singleIntMax),
                        MILLISECONDS.toNanos(singleIntAvg),
                        MILLISECONDS.toNanos(singleInt95Th),
                        MILLISECONDS.toNanos(singleInt99Th),
                        singleIntOverflow)));

        /* Add single interval cummulative latency info to the packet. */
        packet.add(new LatencyInfo
                   (PerfStatType.USER_SINGLE_OP_CUM, start, end,
                    new LatencyResult(
                        singleCumTotalReq, singleCumTotalOps,
                        MILLISECONDS.toNanos(singleCumMin),
                        MILLISECONDS.toNanos(singleCumMax),
                        MILLISECONDS.toNanos(singleCumAvg),
                        MILLISECONDS.toNanos(singleCum95Th),
                        MILLISECONDS.toNanos(singleCum99Th),
                        singleCumOverflow)));

        /* Add multi interval non-cummulative latency info to the packet. */
        packet.add(new LatencyInfo
                   (PerfStatType.USER_MULTI_OP_INT, start, end,
                    new LatencyResult(
                        multiIntTotalReq,
                        multiIntTotalOps,
                        MILLISECONDS.toNanos(multiIntMin),
                        MILLISECONDS.toNanos(multiIntMax),
                        MILLISECONDS.toNanos(multiIntAvg),
                        MILLISECONDS.toNanos(multiInt95Th),
                        MILLISECONDS.toNanos(multiInt99Th),
                        multiIntOverflow)));

        /* Add multi interval cummulative latency info to the packet. */
        packet.add(new LatencyInfo
                   (PerfStatType.USER_MULTI_OP_CUM, start, end,
                    new LatencyResult(
                        multiCumTotalReq,
                        multiCumTotalOps,
                        MILLISECONDS.toNanos(multiCumMin),
                        MILLISECONDS.toNanos(multiCumMax),
                        MILLISECONDS.toNanos(multiCumAvg),
                        MILLISECONDS.toNanos(multiCum95Th),
                        MILLISECONDS.toNanos(multiCum99Th),
                        multiCumOverflow)));

        /* Construct the sub-strings expected in the perf event that will
         * result from the packet returned by this method.
         */
        singleIntPerfEventSubStr = "totalOps=" + singleIntTotalOps +
                                   " totalReq=" + singleIntTotalReq +
                                   " min=" + singleIntMin +
                                   " max=" + singleIntMax +
                                   " avg=" + singleIntAvg +
                                   " 95th=" + singleInt95Th +
                                   " 99th=" + singleInt99Th +
                                   " overflow=" + singleIntOverflow +
                                   " ALERT";

        singleCumPerfEventSubStr = "totalOps=" + singleCumTotalOps +
                                   " totalReq=" + singleCumTotalReq +
                                   " min=" + singleCumMin +
                                   " max=" + singleCumMax +
                                   " avg=" + singleCumAvg +
                                   " 95th=" + singleCum95Th +
                                   " 99th=" + singleCum99Th +
                                   " overflow=" + singleCumOverflow;

        multiIntPerfEventSubStr = "totalOps=" + multiIntTotalOps +
                                  " totalReq=" + multiIntTotalReq +
                                  " min=" + multiIntMin +
                                  " max=" + multiIntMax +
                                  " avg=" + multiIntAvg +
                                  " 95th=" + multiInt95Th +
                                  " 99th=" + multiInt99Th +
                                  " overflow=" + multiIntOverflow +
                                  " ALERT";

        multiCumPerfEventSubStr = "totalOps=" + multiCumTotalOps +
                                  " totalReq=" + multiCumTotalReq +
                                  " min=" + multiCumMin +
                                  " max=" + multiCumMax +
                                  " avg=" + multiCumAvg +
                                  " 95th=" + multiCum95Th +
                                  " 99th=" + multiCum99Th +
                                  " overflow=" + multiCumOverflow;
        return packet;
    }

    private void verifyAdminHelpers(Parameters p) {
        Collection <AdminParams> aParams = p.getAdminParams();
        for (AdminParams ap : aParams) {
            String myNodeHostPort = ap.getNodeHostPort();
            for (AdminParams ap1 : aParams) {
                if (ap.equals(ap1)) {
                    continue;
                }
                assertTrue
                    ("Missing from " + ap1.getAdminId() + ": " + myNodeHostPort,
                     ap1.getHelperHosts().contains(myNodeHostPort));
            }
        }
    }

    /*
     * Inserts more than the maximum number of events and checks that
     * pruning works.
     */
    private void runMaxEvents(int maxEvents)
        throws Exception {

        /* Currently hardcoded in Admin.java */
        final long EVENT_AGING_FREQ = 100;

        /* Number of additional events */
        final long MORE_EVENTS = 1000;

        AdminServiceParams adminServiceParams = atc.getParams();

        Admin admin = new Admin(adminServiceParams);

        try {

            Topology t = admin.getCurrentTopology();
            Parameters p = admin.getCurrentParameters();

            t = admin.getCurrentTopology();
            assertEquals(kvstoreName, t.getKVStoreName());
            p = admin.getCurrentParameters();
            assertEquals(kvstoreName, p.getGlobalParams().getKVStoreName());
            admin.getMonitor().trackPlanStateChange(planStateVerifier);

            nStorageNodes = 0;

            /* Deploy a Datacenter. */
            Datacenter dc = deployDatacenter(admin, 2);

            /* Deploy a StorageNode */
            StorageNode sn =
                firstDeployStorageNode(admin, dc,
                                       portFinder1.getRegistryPort(),
                                       COMMENT1);

            /* Deploy an AdminService */
            deployAdminService(admin, sn, true);

            /* Deploy another StorageNode */
            sn =
                firstDeployStorageNode(admin, dc,
                                       portFinder2.getRegistryPort(), COMMENT2);

            /* Deploy another AdminService */
            deployAdminService(admin, sn, true);

            /* Deploy some RepNodes */
            deployKVStore(admin, 2, 100);

            if (maxEvents > 0) {
                ParameterMap map = new ParameterMap();
                map.setType(ParameterState.ADMIN_TYPE);
                map.setParameter(ParameterState.AP_EVENT_MAX, Integer.toString(maxEvents)) ;
                int pid = admin.getPlanner().createChangeAllAdminsPlan(
                    "changeAdminParams", null, map);
                runAndVerifyPlan(admin, pid);

                /* Needed due to test environment */
                admin.newParameters();
            }
            /*
             * recordable messages into the stream plus the number
             * of events to insure that a pruning is performed.
             */
            long eventsToInsert =
                admin.getMaxEvents() + MORE_EVENTS +EVENT_AGING_FREQ;
            for (int i=0; i < eventsToInsert; i++) {
                admin.getLogger().severe("severe message " + i);
            }

            admin.syncEventRecorder();

            /*
             * Get all the recorded events in the database, starting at the epoch.
             */
            List<CriticalEvent> events =
                admin.getEvents(0L, 0L, CriticalEvent.EventType.ALL);

            assertTrue(events.size() <
                       (admin.getMaxEvents() + EVENT_AGING_FREQ));
        } finally {
            admin.shutdown(true, "test");
        }
    }
}
