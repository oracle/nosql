/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.AdminTestConfig;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.VerifyConfiguration;
import oracle.kv.impl.admin.VerifyResults;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterMap;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupMap;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Test;

/**
 * Exercise error conditions during sn and store deployment.
 */
public class ProblematicDeployTest extends TestBase {

    private List<StorageNodeAgent> snaList;

    @Override
    public void setUp() throws Exception {

        super.setUp();

        snaList = new ArrayList<StorageNodeAgent>();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();

        for (StorageNodeAgent sna : snaList) {
            sna.shutdown(true, true);
        }
    }

    /**
     * Deploy a SN that has no bootstrap SNA running.The plan should fail.
     * Retry several times, then bring up the bootstrap SNA, and check for
     * a successful deployment.
     * @throws Exception
     */
    @Test
    public void testRetryBadSNDeploy()
        throws Exception {

        PortFinder portFinder1 = new PortFinder(13230, 5);
        PortFinder portFinder2 = new PortFinder(13250, 5);
        AdminTestConfig atc = new AdminTestConfig(kvstoreName, portFinder1);
        startupBootstrapSNA(portFinder1, "config0.xml");

        Admin admin = new Admin(atc.getParams());

        Datacenter dc = deployDatacenter(admin, 1);
        dc = deployDatacenter(admin, 1);

        /* Deploy a legitimate StorageNode.*/
        StorageNode sn = deployStorageNode
             (admin, dc, portFinder1.getRegistryPort(), "");

        /* Deploy an AdminService */
        deployAdminService(admin, sn);

        /* Deploy an invalid SN. */
        StorageNodeParams snParams =
            new StorageNodeParams("localhost",
                                  portFinder2.getRegistryPort(),
                                  "");

        int badid = admin.getPlanner().createDeploySNPlan
            ("deploy badSN", dc.getResourceId(), snParams);
        AbstractPlan badSNPlan = (AbstractPlan) admin.getPlanById(badid);
        admin.approvePlan(badid);
        admin.savePlan(badSNPlan, Admin.CAUSE_APPROVE);

        /* We should see an RMI connect exception. */
        admin.executePlan(badid, false);
        Plan.State status = admin.awaitPlan(badid, 0, null);
        assertEquals(Plan.State.ERROR, status);
        ExceptionTransfer latest = badSNPlan.getExceptionTransfer();
        assertTrue("Expected ConnectException: " + latest.getFailure(),
                   latest.getFailure() instanceof ConnectException);
        assertTrue("Exception message should mention storage node agent: " +
                   latest.getFailure().getMessage(),
                   latest.getFailure().getMessage().contains(
                       "storage node agent"));
        logger.info("Exception when SN service is not found:\n" +
                    latest.getFailure());

        checkNumTopoSNs(admin, 1);

        /* Should happen again when we retry. */
        admin.executePlan(badid, false);
        status = admin.awaitPlan(badid, 0, null);
        assertEquals(Plan.State.ERROR, status);

        latest = badSNPlan.getExceptionTransfer();
        assertTrue("Expected ConnectException, found: " + latest.getFailure(),
                   latest.getFailure() instanceof ConnectException);

        checkNumTopoSNs(admin, 1);

        /*
         * Try to create a store plan on this single, invalid SN, should fail
         * because pool has bad SNs in it.
         */
        try {
            admin.createTopoCandidate("Store Candidate",
                                      Parameters.DEFAULT_POOL_NAME,
                                      100, false,
                                      SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
        } catch (IllegalCommandException expected) {
        }
        checkNumTopoSNs(admin, 1);

        /* Now try deploying the SN, should work. */
        startupBootstrapSNA(portFinder2, "config1.xml");
        admin.executePlan(badid, false);
        admin.awaitPlan(badid, 0, null);
        admin.assertSuccess(badid);

        /* Update admin params before verify */
        final AdminParams adminParams = atc.getParams().getAdminParams();
        admin.updateParams(adminParams);
        final StorageNodeAgent sna1 = snaList.get(0);
        final StorageNodeAgentAPI snaApi =
            RegistryUtils.getStorageNodeAgent(sna1.getStoreName(),
                                              sna1.getHostname(),
                                              sna1.getRegistryPort(),
                                              sna1.getStorageNodeId(),
                                              null,
                                              logger);
        snaApi.newAdminParameters(adminParams.getMap());

        /* Check that there are two SNs, and that all services are alive. */
        checkNumTopoSNs(admin, 2);
        VerifyConfiguration verifier =
            new VerifyConfiguration(admin, true, true, false, logger);
        /* Beware left-to-right order of evaluation of args to assertTrue. */
        verifier.verifyTopology();
        VerifyResults results = verifier.getResults();
        assertEquals(results.display(), 0, results.numViolations());
        admin.shutdown(true, "test");
    }

    private void checkNumTopoSNs(Admin admin, int expectedNumSNs) {
        Topology topo = admin.getCurrentTopology();
        assertEquals(expectedNumSNs, topo.getStorageNodeMap().size());
    }

    /**
     * Test deploying two stores that both attempt to enlist the same SN
     * [#23672]
     */
    @Test
    public void testDeploySimultaneously()
        throws Exception {

        /* Deploy first store with initial SN */
        final PortFinder portFinder1sn2;
        {
            PortFinder portFinder1sn1 = new PortFinder(13230, 5);
            AdminTestConfig atc1 =
                new AdminTestConfig(kvstoreName, portFinder1sn1);
            startupBootstrapSNA(portFinder1sn1, "config1sn1.xml");
            Admin admin1 = new Admin(atc1.getParams());
            Datacenter dc1 = deployDatacenter(admin1, 1);
            StorageNode sn1 = deployStorageNode(
                admin1, dc1, portFinder1sn1.getRegistryPort(), "");
            deployAdminService(admin1, sn1);

            /* Deploy second SN to first store */
            portFinder1sn2 = new PortFinder(13250, 5);
            startupBootstrapSNA(portFinder1sn2, "config1sn2.xml");
            deployStorageNode(
                admin1, dc1, portFinder1sn2.getRegistryPort(), "");
        }

        /* Deploy second store with initial SN */
        PortFinder portFinder2sn1 = new PortFinder(13270, 5);
        String kvstoreName2 = kvstoreName + "-2";
        AdminTestConfig atc2 =
            new AdminTestConfig(kvstoreName2, portFinder2sn1);
        startupBootstrapSNA(portFinder2sn1, "config2sn1.xml");
        Admin admin2 = new Admin(atc2.getParams());
        Datacenter dc2 = deployDatacenter(admin2, 1);
        StorageNode store2sn1 = deployStorageNode(
            admin2, dc2, portFinder2sn1.getRegistryPort(), "");
        deployAdminService(admin2, store2sn1);

        /*
         * Attempt to deploy a second SN on the second store using the same
         * host/port as for the second SN on the first store.
         */
        StorageNodeParams snParams = new StorageNodeParams(
            "localhost", portFinder1sn2.getRegistryPort(), "");
        int planId = admin2.getPlanner().createDeploySNPlan(
            "deploy bad SN", dc2.getResourceId(), snParams);
        Plan plan = admin2.getPlanById(planId);
        try {
            runBadPlan(admin2, plan);
            fail("Expected failure");
        } catch (OperationFaultException e) {
            assertTrue(
                "Exception message should mention bootstrap storage node:" +
                e.getMessage(),
                e.getMessage().toLowerCase().contains(
                    "bootstrap storage node"));
            logger.info("Exception when bootstrap SN service is not found:\n" +
                        e);
        }
    }

    /**
     * Deploy a SN that has a bad HAPortRange, and try to deploy a store.
     * Then redeploy it, and redo the store.
     * TODO: this test is not complete - it does not yet do the "fix and
     * redeploy portion."  As-is it appears to interfere with the success of
     * other test cases in this class so it is X'd out until it can be
     * completed.  It makes more sense to move this entire case to a higher
     * level of test, e.g. the AdminUtils and CommandService level, where it
     * can more closely emulate mistakes made by actual users.
     *
     * @throws Exception
     */
    public void XtestBadHAPortRange()
        throws Exception {

        PortFinder portFinder1 = new PortFinder(13230, 5);
        @SuppressWarnings("unused")
        PortFinder portFinder2 = new PortFinder(13250, 5);
        PortFinder badPortFinder = new BadHARangePortFinder(13250);
        AdminTestConfig atc = new AdminTestConfig(kvstoreName, portFinder1);
        startupBootstrapSNA(portFinder1, "config0.xml");
        startupBootstrapSNA(badPortFinder, "config1.xml");

        Admin admin = new Admin(atc.getParams());
        Datacenter dc = deployDatacenter(admin, 2);
        dc = deployDatacenter(admin, 2);

        /* Deploy a legitimate StorageNode.*/
        StorageNode sn = deployStorageNode
            (admin, dc, portFinder1.getRegistryPort(), "");

        /* Deploy an AdminService */
        deployAdminService(admin, sn);

        /* Deploy a SN with a bad port range.*/
        sn = deployStorageNode
            (admin, dc, badPortFinder.getRegistryPort(), "");

        /*
         * Try to create a store plan on this single, invalid SN, should fail
         * because ha port range is bad.
         */
        admin.createTopoCandidate("Bad", Parameters.DEFAULT_POOL_NAME, 100,
                                  false,
                                  SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
        int planId = admin.getPlanner().createDeployTopoPlan("should fail store",
                                                             "Bad", null);
        Plan plan = admin.getPlanById(planId);
        try {
            runBadPlan(admin, plan);
            fail("Should fail, HA port range is bad");
        } catch (OperationFaultException expected) {
            /* keep going */
        }

        VerifyConfiguration verifier =
            new VerifyConfiguration(admin, true, true, false, logger);
        /* Beware left-to-right order of evaluation of args to assertFalse. */
        boolean verified = verifier.verifyTopology();
        assertFalse(verifier.getResults().display(), verified);

        /*
         * TODO: write test code to fix the problem:
         * 1.  change SN params to have a valid HA range
         * 2.  retry the plan and/or restart the SNA to get the RN
         * running.
         */
        admin.shutdown(true, "test");
    }

    private void startupBootstrapSNA(PortFinder portFinder, String configFile)
        throws Exception {
        StorageNodeAgent sna = StorageNodeTestBase.createUnregisteredSNA
            (portFinder, 1, configFile, false, true, 1024);
        snaList.add(sna);
    }

    /**
     * Create and run a DeployDatacenterPlan
     */
    Datacenter deployDatacenter(Admin admin, int repFactor) {

        int ddcid = admin.getPlanner().
            createDeployDatacenterPlan("deploy data center", "Miami", repFactor,
                                       DatacenterType.PRIMARY, false, false);
        runPlan(admin, ddcid);
        Topology t = admin.getCurrentTopology();
        DatacenterMap dcmap = t.getDatacenterMap();
        assertEquals(dcmap.size(), 1);
        Datacenter dc = dcmap.getAll().iterator().next();
        return dc;
    }

    /* Create and run a DeployStorageNode plan. */
    private StorageNode deployStorageNode(Admin admin,
                                          Datacenter dc,
                                          int registryPort,
                                          String comment) {

        StorageNodeParams snParams =
            new StorageNodeParams("localhost", registryPort, comment);

        int dsnid =
            admin.getPlanner().
            createDeploySNPlan("deploy storage node", dc.getResourceId(),
                               snParams);
        runPlan(admin, dsnid);

        Topology t = admin.getCurrentTopology();
        StorageNode sn = null;
        for (StorageNode s : t.getStorageNodeMap().getAll()) {
            if (s.getRegistryPort() == registryPort) {
                sn = s;
                break;
            }
        }
        assertNotNull(sn);

        return sn;
    }

    /* Create and run a DeployAdmin plan. */
    void deployAdminService(Admin admin,
                            StorageNode sn) {

        int daid = admin.getPlanner().
            createDeployAdminPlan("deploy admin",
                                  sn.getStorageNodeId());

        runPlan(admin, daid);
    }

    /* Create and run a DeployKVStore plan. */
    void deployKVStore(Admin admin, int repFactor, int numPartitions) {

        Parameters p = admin.getCurrentParameters();

        StorageNodePool pool = p.getStorageNodePool
            (Parameters.DEFAULT_POOL_NAME);
        admin.createTopoCandidate("Okay", Parameters.DEFAULT_POOL_NAME,
                                  numPartitions, false,
                                  SerialVersion.ADMIN_CLI_JSON_V1_VERSION);
        int dkvsid = admin.getPlanner().createDeployTopoPlan
            ("deploy kv store", "Okay", null);
        runPlan(admin, dkvsid);

        /* Check the resulting topology changes. */
        Topology t = admin.getCurrentTopology();

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

    private void runPlan(Admin admin, int planId) {
        Plan plan = admin.getPlanById(planId);
        runPlan(admin, plan);
    }

    private void runPlan(Admin admin, Plan plan) {
        admin.approvePlan(plan.getId());
        admin.savePlan(plan, Admin.CAUSE_APPROVE);
        admin.executePlan(plan.getId(), false);
        admin.awaitPlan(plan.getId(), 0, null);
        assertEquals(Plan.State.SUCCEEDED, plan.getState());
    }

    /** Expect failure */
    private void runBadPlan(Admin admin, Plan plan) {
        admin.approvePlan(plan.getId());
        admin.savePlan(plan, Admin.CAUSE_APPROVE);
        admin.executePlan(plan.getId(), false);
        admin.awaitPlan(plan.getId(), 0, null);
        admin.assertSuccess(plan.getId());
    }

    /**
     * A PortFinder which is hard coded to return an invalid HA Port Range.
     */
    private class BadHARangePortFinder extends PortFinder {
        int nextPortToUse = 1;

        public BadHARangePortFinder(int startingPort) {
            super(startingPort, 7 /*haRangeSize*/, "localhost");
        }
        @Override
            public String getHaRange() {
            return "1,7";
        }

        @Override
            public int getHaRangeSize() {
            return 7;
        }

        @Override
        public int getHaFirstPort() {
            return 1;
        }

        @Override
        public int getHaNextPort() {
            return nextPortToUse++;
        }
    }
}
