/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.KVVersion;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.RMIFailed;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.DeployAdmin;
import oracle.kv.impl.arb.ArbNodeStatus;
import oracle.kv.impl.arb.admin.ArbNodeAdminAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.rep.RepNodeStatus;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeStatus;
import oracle.kv.impl.sna.StorageNodeTestBase;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Migrate all the services from one store to another. Also experiment with
 * using the remove-storagenode plan to clean up after migration.
 */
public class MigrateSNTest extends TestBase {

    private static final int START_PORT = 13000;
    private static final int NUM_PORTS = 5;

    private List<TestSN> testSNs;
    private final LoginManager loginMgr = null;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        testSNs = new ArrayList<TestSN>();
    }

    @Override
    public void tearDown()
        throws Exception {
        DeployAdmin.FAULT_HOOK = null;
        super.tearDown();
        for (TestSN testSN: testSNs) {
            testSN.sna.shutdown(true, true);
        }
        LoggerUtils.closeAllHandlers();
    }

    /* Struct to hang onto to each bootstrap SNA */
    private class TestSN {
        final StorageNodeAgent sna;
        final PortFinder portFinder;
        final int registryPort;
        private boolean hostsAdmin;

        /* RepNode currently running on this SN */
        private RepNodeId rnId;
        /* ArbNode currently running on this SN */
        private ArbNodeId anId;

        TestSN(int whichSNA, int startPort, int rangeSize)
            throws Exception {

            portFinder = new PortFinder(startPort, rangeSize);
            sna = StorageNodeTestBase.createUnregisteredSNA
                (portFinder,
                 1,
                 "sna" + whichSNA + "config.xml",
                 true, // useThreads
                 true, // createAdmin
                 1024); // Set memoryMB
            registryPort = startPort + 1;
            hostsAdmin = false;
        }

        StorageNodeId getSNId() {
            return sna.getStorageNodeId();
        }

        void setUsesAdmin() {
            hostsAdmin = true;
        }

        boolean usesAdmin() {
            return hostsAdmin;
        }

        void swap(TestSN other) {
            rnId = other.rnId;
            other.rnId = null;
            anId = other.anId;
            other.anId = null;
            hostsAdmin = other.hostsAdmin;
            other.hostsAdmin = false;
        }

        void setRepNodeId(RepNodeId rnId2) {
            rnId = rnId2;
        }

        public RepNodeId getRepNodeId() {
            return rnId;
        }

        void setArbNodeId(ArbNodeId anId2) {
            anId = anId2;
        }

        public ArbNodeId getArbNodeId() {
            return anId;
        }
    }

    private void startSNs(int numSNs) throws Exception {

        for (int i = 0; i < numSNs; i++) {
            testSNs.add(new TestSN(i, START_PORT + (i * 50), NUM_PORTS));
        }
    }

    /**
     * Deploys the datacenter with the given repFactor and deploys all
     * storageNodes in the datacenter. Creates a StorageNodePool with
     * the required number of storageNodes and deploys the Topology.
     *
     * @param repFactor The datacenter repFactor.
     * @param totalSNs The total SNs deployed in the datacenter.
     * @return CommandServiceAPI
     * @throws Exception
     */
    private CommandServiceAPI startStore(int repFactor,
                                         int totalSNs)
        throws Exception {

        int numSNsToUse = repFactor;

        /* If the primary RF = 2 will need one more SN to host arbiters.
         */
        if (repFactor == 2) {
            numSNsToUse = numSNsToUse + 1;
        }

        final String poolName = "TestPool";

        /* This is the bootstrap admin on the first SN. */
        CommandServiceAPI cs =
            ServiceUtils.waitForAdmin(testSNs.get(0).sna.getHostname(),
                                      testSNs.get(0).sna.getRegistryPort(),
                                      loginMgr, 5, ServiceStatus.RUNNING,
                                      logger);

        cs.configure(kvstoreName);

        /* Update policy if needed */
        final ParameterMap policyMap = mergeParameterMapDefaults(null);
        if (policyMap != null) {
            cs.setPolicies(policyMap);
        }

        /* Deploy Datacenter. */
        int planId = cs.createDeployDatacenterPlan("DCPlan", "Miami",
                                                   repFactor,
                                                   DatacenterType.PRIMARY,
                                                   true,
                                                   false);
        runPlan(cs, planId);

        /* Deploy all the SNs */
        DatacenterId dcId = new DatacenterId(1);
        for (int i = 0; i < totalSNs; i++) {
            TestSN tSN = testSNs.get(i);
            planId = cs.createDeploySNPlan("Deploy SN", dcId,
                                           tSN.sna.getHostname(),
                                           tSN.sna.getRegistryPort(),
                                           "comment");
            runPlan(cs, planId);

            /*
             * Deploy the Admins, interleaving it with the SN deploy so as to
             * satisfy the rule that the first Admin must be deployed on
             * the first deployed SN.
             *
             * # of Admins must be >= RF
             */
            if (i < repFactor) {
                tSN.setUsesAdmin();
                planId = cs.createDeployAdminPlan
                    ("Deploy admin", tSN.getSNId());
                runPlan(cs, planId);
            }
        }

        /*
         * Need a storage pool for the store deployment.Put just enough
         * SNs into the pool, leaving others available for migration.
         */
        cs.addStorageNodePool(poolName);
        for (int i = 0; i < numSNsToUse; i++) {
            cs.addStorageNodeToPool(poolName, testSNs.get(i).getSNId());
        }

        /*
         * Make RepNodes and ArbNodes
         */
        cs.createTopology("firstTopo", poolName, 100, false);
        planId = cs.createDeployTopologyPlan("RunStore", "firstTopo", null);
        runPlan(cs, planId);

        /*
         * Wait for the RepNodes and ArbNodes to come up.
         */
        Topology topo = cs.getTopology();
        List<RepNode> repNodes = topo.getSortedRepNodes();

        /* This test assumes that each SN used at start has 1 RN. */
        assertEquals(repFactor, repNodes.size());

        ServiceStatus[] target = {ServiceStatus.RUNNING};
        for (int i = 0; i < repFactor; i++) {
            RepNodeId rnId = repNodes.get(i).getResourceId();
            ServiceUtils.waitForRepNodeAdmin(topo, rnId, loginMgr, 5, target,
                                             logger);
            int snIndex =
                repNodes.get(i).getStorageNodeId().getStorageNodeId() - 1;
            testSNs.get(snIndex).setRepNodeId(rnId);
        }

        for (ArbNodeId anId : topo.getArbNodeIds()) {
            ArbNode an = topo.get(anId);
            StorageNode sn = topo.get(an.getStorageNodeId());
            ServiceUtils.waitForArbNodeAdmin(topo.getKVStoreName(),
                                             sn.getHostname(),
                                             sn.getRegistryPort(),
                                             anId,
                                             sn.getStorageNodeId(),
                                             loginMgr,
                                             5,
                                             target,
                                             logger);
            int snIndex =
                topo.get(anId).getStorageNodeId().getStorageNodeId() - 1;
            testSNs.get(snIndex).setArbNodeId(anId);
        }

        return cs;
    }

    private void runPlan(CommandServiceAPI cs, int planId)
        throws RemoteException {
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

    }

    @Test
    public void testMigrate()
        throws Exception {

        /*
         * Start up 9 SNs. We need to have a quorum for Admins and RNs when
         * one is down, so initially
         * sn1 - RN & Admin
         * sn2 - RN & Admin
         * sn3 - RN & Admin
         * sn4 - RN only.
         */
        int totalSNs = 12;
        startSNs(totalSNs);
        final CommandServiceAPI cs = startStore(4 /* RF */,
                                                totalSNs);
        final List<Problem> violations =
            cs.verifyConfiguration(true, true, false).getViolations();
        assertEquals(violations.toString(), 0, violations.size());

        /*
         * Check that each SN has the appropriate RN and possibly an Admin on
         * it.
         */
        TestSN tsn1 = testSNs.get(0);
        TestSN tsn2 = testSNs.get(1);
        TestSN tsn3 = testSNs.get(2);
        TestSN tsn4 = testSNs.get(3);
        TestSN tsn5 = testSNs.get(4);
        TestSN tsn6 = testSNs.get(5);
        TestSN tsn7 = testSNs.get(6);
        TestSN tsn8 = testSNs.get(7);
        TestSN tsn9 = testSNs.get(8);
        TestSN tsn10 = testSNs.get(9);

        checkServices(tsn1);
        checkServices(tsn2);
        checkServices(tsn3);
        checkServices(tsn4);

        StorageNodeId sn1 = tsn1.getSNId();
        StorageNodeId sn2 = tsn2.getSNId();
        StorageNodeId sn4 = tsn4.getSNId();
        StorageNodeId sn5 = tsn5.getSNId();
        StorageNodeId sn6 = tsn6.getSNId();
        StorageNodeId sn7 = tsn7.getSNId();
        StorageNodeId sn8 = tsn8.getSNId();
        StorageNodeId sn9 = tsn9.getSNId();
        StorageNodeId sn10 = tsn10.getSNId();
        StorageNodeId badSNId = new StorageNodeId(100);

        /*
         * We'll be migrating SN2 to SN5, but will first try a variety of
         * error cases.
         */

        /*
         * try sn1 -> sn2, should fail, in a migration, the destination should
         * not have running services.
         */
        expectCreateFailure(cs, sn1, sn2);

        /* try sn5 -> sn6, should fail, neither node has services. */
        expectCreateFailure(cs, sn5, sn6);

        /* try sn1 -> nonexistent SN, should fail, new node doesn't exist. */
        expectCreateFailure(cs, sn1, badSNId);

        /* try nonexistent SN -> sn1, should fail, old node doesn't exist. */
        expectCreateFailure(cs, badSNId, sn1);

        /* try sn1 -> sn5, should fail, old node must be dead. */
        expectRunFailure(cs, sn1, sn5);

        /*
         * Experiment with removeSN. Try several times to use removeSNPlan in
         * invalid ways. Then successfully remove sn10 and try to migrate
         * sn1->sn10.  Should fail; sn10 existed at one point but has since
         * been removed by the time we try the migration.
         */

        try {
            cs.createRemoveSNPlan("remove nonexistent id ", badSNId);
            fail("createRemovePlan of " + badSNId +
                 " should fail because " + badSNId + " does not exist.");
        } catch (AdminFaultException expected) {
        }

        /* Try removal after shutting down the node, should be successful.*/
        tsn10.sna.shutdown(true, true);
        int removePlanId = cs.createRemoveSNPlan("remove " + sn10, sn10);
        removeSN(cs, sn10, removePlanId, true);

        /* repeat the removal. */
        try {
            removePlanId = cs.createRemoveSNPlan("remove " + sn10, sn10);
            fail("createRemovePlan of " + badSNId +
                 " should fail because " + badSNId +
                 " has already been removed");
        } catch (AdminFaultException expected) {
        }

        /*
         * Shut down sn2 in preparation for migration. Try sn2 -> sn5, and
         * make sn5 look like it's an old version. Should fail, destination
         * SN must be >= the admin's current version.
         */
        tsn2.sna.shutdown(true, true);
        StorageNodeStatus.setTestKVVersion(new KVVersion(12, 0, 2, 0, 0, null));
        expectRunFailure(cs, sn2, sn5);
        StorageNodeStatus.setTestKVVersion(null);

        /* Try sn2 -> sn5, Should fail, new node must be alive. */
        tsn5.sna.shutdown(true, true);
        expectRunFailure(cs, sn2, sn5);

        /* Okay, sn2 is shut down now, migrate it to sn6. */
        tsn6.setUsesAdmin();
        int planId = cs.createMigrateSNPlan("Migrate sn2 to sn6", sn2, sn6);
        runPlan(cs, planId);

        /*
         * Verify the configuration. We expect to see it complain that sn2,sn5
         * are down, since we explicitly shut them down. There should be
         * services on sn1,3,4,6.
         */
        verify(cs, sn2, sn5);
        checkServices(tsn1);
        tsn6.swap(tsn2);
        checkServices(tsn6); // sn6 houses rep node 2
        checkServices(tsn3);
        checkServices(tsn4);

        /*
         * Now there are RNs on sn1,3,4,6. Try migrating sn6 -> sn7.
         * Should fail, sn6 is alive.
         */
        planId = runWithFailure("Migrate sn6 to sn7 with Retry", cs, sn6, sn7);

        /*
         * Now shutdown sn6, and retry the plan (using the same plan instance)
         * sn2, sn5, sn6 should be down.
         */
        tsn7.setUsesAdmin();
        tsn6.sna.shutdown(true, true);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
        verify(cs, sn2, sn5, sn6);
        checkServices(tsn1);
        checkServices(tsn3);
        checkServices(tsn4);
        tsn7.swap(tsn6);
        checkServices(tsn7);

        /*
         * Recreate and rerun the plan, no topology changes should happen.
         */
        planId = cs.createMigrateSNPlan("Repeat sn6 to sn7", sn6, sn7);
        runPlan(cs, planId);
        verify(cs, sn2, sn5, sn6);
        checkServices(tsn1);
        checkServices(tsn7); // sn7 houses rep node 2
        checkServices(tsn3);
        checkServices(tsn4);

        /*
         * At this point, the rep nodes are at topology sequence N. Make a few
         * intervening topology changes that would not get naturally propagated
         * in this unit test's lifetime to the RNs, such as the creation of a
         * datacenter or SN, which bumps the topology sequence to N+1 and
         * N+2. Then do a migration, which uses topo sequence N+3.
         *
         * For this to work properly, the migration must query each RN for its
         * current topo sequence, and send updates N+1 and N+2 and N+3.It takes
         * two topo changes to provoke the bug found in [#20751]
         */
        /* Do two gratuitous topology changes */
        DatacenterId dcId = new DatacenterId(1);
        planId = cs.createDeploySNPlan("Deploy Extra SN",
                                       dcId,
                                       testSNs.get(10).sna.getHostname(),
                                       testSNs.get(10).sna.getRegistryPort(),
                                       "comment");
        runPlan(cs, planId);
        planId = cs.createDeploySNPlan("Deploy Second Extra SN",
                                       dcId,
                                       testSNs.get(11).sna.getHostname(),
                                       testSNs.get(11).sna.getRegistryPort(),
                                       "comment");
        runPlan(cs, planId);

        planId = cs.createMigrateSNPlan("Migrate sn7 to sn8", sn7, sn8);
        tsn7.sna.shutdown(true, true);
        tsn8.setUsesAdmin();
        runPlan(cs, planId);
        verify(cs, sn2, sn5, sn6, sn7);
        checkServices(tsn1);
        tsn8.swap(tsn7);
        checkServices(tsn8); // sn8 houses rep node 2
        checkServices(tsn3);
        checkServices(tsn4);

        /* Now migrate sn4, which has an Admin, to sn9 */
        planId = cs.createMigrateSNPlan("Migrate sn4 to sn9", sn4, sn9);
        tsn4.sna.shutdown(true, true);
        runPlan(cs, planId);
        verify(cs, sn2, sn4, sn5, sn6, sn7);
        checkServices(tsn1);
        checkServices(tsn8);
        checkServices(tsn3);
        tsn9.swap(tsn4);
        checkServices(tsn9);

        /* remove all the empty SNs, and re-verify. */
        cleanupDeadSNs(cs, sn2, sn4, sn5, sn6, sn7);
        verify(cs);

        /* Make sure the Admin's helper hosts were updated properly. */
        verifyAdminHelpers(cs);
    }

    /**
     * Inject failures into the migrate-sn plan around the deployment of the
     * admin and check that the repair plan can fix the problems. Although
     * focused on the admin, the test case ends up exercising RN related fixes
     * too.
     */
    @Test
    public void testRepairAdmin()
        throws Exception {

        /*
         * sn1 - RN & Admin
         * sn2 - RN & Admin
         * sn3 - RN & Admin
         *
         */
        int totalSNs = 6;
        startSNs(totalSNs);
        final CommandServiceAPI cs = startStore(4 /* RF */,
                                                totalSNs);
        final List<Problem> violations =
            cs.verifyConfiguration(true, true, false).getViolations();
        assertEquals(violations.toString(), 0, violations.size());

        /*
         * Check that each SN has the appropriate RN and possibly an Admin on
         * it.
         */
        TestSN tsn1 = testSNs.get(0);
        TestSN tsn2 = testSNs.get(1);
        TestSN tsn3 = testSNs.get(2);
        TestSN tsn4 = testSNs.get(3);
        TestSN tsn5 = testSNs.get(4);
        TestSN tsn6 = testSNs.get(5);

        checkServices(tsn1);
        checkServices(tsn2);
        checkServices(tsn3);
        checkServices(tsn4);

        StorageNodeId sn2 = tsn2.getSNId();
        StorageNodeId sn3 = tsn3.getSNId();
        StorageNodeId sn5 = tsn5.getSNId();
        StorageNodeId sn6 = tsn6.getSNId();

        /*
         * Migration and repair round 1: Shut down sn2 in preparation for
         * migration. Try sn2 -> sn6. Invoke test hook at location 1.
         */
        tsn2.sna.shutdown(true, true);
        tsn6.setUsesAdmin();
        DeployAdmin.FAULT_HOOK = new DeployUtils.TaskHook(1);
        int planId = cs.createMigrateSNPlan("Migrate sn2 to sn6", sn2, sn6);
        try {
            runPlan(cs, planId);
            fail("Should fail");
        } catch (AdminFaultException expected) {
            cs.cancelPlan(planId);
        }
        DeployAdmin.FAULT_HOOK = null;

        /* Should be violations  */
        Problem unexpected =
            verifyWithExpectedViolations(cs, sn2,
                new AdminId(2), new RepNodeId(1, 1), /* helper hosts */
                new RepNodeId(1, 2), /* helper hosts */
                new RepNodeId(1, 3), /* helper hosts */
                new RepNodeId(1, 4), /* helper hosts */
                sn6);
        assertTrue("Unexpected problem: " + unexpected, unexpected == null);

        /* Repair */
        planId = cs.createRepairPlan("Repair");
        runPlan(cs, planId);

        /* Verify again, should only have complaints about SN2 */
        int retries = 10;
        while (retries > 0) {
            /* May take a little while for the migrated services to come up. */
            unexpected = verifyWithExpectedViolations(cs, sn2);
            if (unexpected == null) {
                break;
            }
            System.err.println("Saw " + unexpected);
            retries--;
            Thread.sleep(500);
        }
        assertTrue("Unexpected problem: " + unexpected, unexpected == null);

        /*
         * Migration and repair round 2: Shut down sn3 in preparation for
         * migration to sn5. Invoke test hook at location 2.
         */
        tsn3.sna.shutdown(true, true);
        tsn5.setUsesAdmin();
        DeployAdmin.FAULT_HOOK = new DeployUtils.TaskHook(2);
        planId = cs.createMigrateSNPlan("Migrate sn3 to sn5", sn3, sn5);
        try {
            runPlan(cs, planId);
            fail("Should fail");
        } catch (AdminFaultException expected) {
            cs.cancelPlan(planId);
        }
        DeployAdmin.FAULT_HOOK = null;

        /* There should be some violations */
        unexpected = verifyWithExpectedViolations(cs,
            sn2, sn3, new AdminId(3),
            new RepNodeId(1, 1), /* helper hosts */
            new RepNodeId(1, 2), /* helper hosts */
            new RepNodeId(1, 3), /* helper hosts */
            new RepNodeId(1, 4)); /* helper hosts */
        assertTrue("Unexpected problem: " + unexpected, unexpected == null);

        /* Repair */
        planId = cs.createRepairPlan("Repair");
        runPlan(cs, planId);

        /* Check results, should only have complaints about sn2 and sn3 */
        retries = 10;
        while (retries > 0) {
            /* May take a little while for the migrated services to come up. */
            unexpected = verifyWithExpectedViolations(cs, sn2, sn3);
            if (unexpected == null) {
                break;
            }
            System.err.println("Saw repair2: " + unexpected);
            retries--;
            Thread.sleep(500);
        }
        assertTrue("Unexpected problem: " + unexpected, unexpected == null);
        tsn6.swap(tsn2);
        tsn5.swap(tsn3);
        checkServices(tsn1);
        checkServices(tsn4);
        checkServices(tsn5); // sn5 houses admin3
        checkServices(tsn6); // sn6 houses admin2
    }

    /**
     * Tests to insure arbiter services are migrated from one SN to another
     * SN when migrateSN plan is run.
     * @throws Exception
     */
    @Test
    public void testANMigrate()
        throws Exception {
        /*
         * Start up 6 SNs.
         * sn1 - RN & Admin
         * sn2 - RN & Admin
         * sn3 - AN
         */
        int totalSNs = 6;
        startSNs(totalSNs);
        final CommandServiceAPI cs = startStore(2 /* RF */,
                                                totalSNs);
        final List<Problem> violations =
            cs.verifyConfiguration(true, true, false).getViolations();
        assertEquals(violations.toString(), 0, violations.size());

        /*
         * Check that each SN has the appropriate RN/AN and possibly an Admin on
         * it.
         */
        TestSN tsn1 = testSNs.get(0);
        TestSN tsn2 = testSNs.get(1);
        TestSN tsn3 = testSNs.get(2);
        TestSN tsn4 = testSNs.get(3);
        TestSN tsn5 = testSNs.get(4);
        TestSN tsn6 = testSNs.get(5);

        checkServices(tsn1);
        checkServices(tsn2);
        checkANServices(tsn3);

        StorageNodeId sn3 = tsn3.getSNId();
        StorageNodeId sn4 = tsn4.getSNId();
        StorageNodeId sn5 = tsn5.getSNId();
        StorageNodeId sn6 = tsn6.getSNId();
        StorageNodeId badSNId = new StorageNodeId(100);

        /*
         * We'll be migrating SN3 to SN5, but will first try a variety of
         * error cases.
         */

        /* try sn3 -> nonexistent SN, should fail, new node doesn't exist. */
        expectCreateFailure(cs, sn3, badSNId);

        /* try sn3 -> sn4, should fail, old node must be dead. */
        expectRunFailure(cs, sn3, sn4);

        /*
         * Shut down sn3 in preparation for migration. Try sn3 -> sn4, and
         * make sn4 look like it's an old version. Should fail, destination
         * SN must be >= the admin's current version.
         */
        tsn3.sna.shutdown(true, true);
        StorageNodeStatus.setTestKVVersion(new KVVersion(12, 0, 2, 0, 0, null));
        expectRunFailure(cs, sn3, sn4);
        StorageNodeStatus.setTestKVVersion(null);

        /* Try sn3 -> sn4, Should fail, new node must be alive. */
        tsn4.sna.shutdown(true, true);
        expectRunFailure(cs, sn3, sn4);

        /* Okay, sn3 is shut down now, migrate it to sn5. */
        int planId = cs.createMigrateSNPlan("Migrate sn3 to sn5", sn3, sn5);
        runPlan(cs, planId);

        /*
         * Verify the configuration. We expect to see it complain that sn3,sn4
         * are down, since we explicitly shut them down. There should be
         * services on sn1,2,5.
         */
        verify(cs, sn3, sn4);
        checkServices(tsn1);
        checkServices(tsn2);
        tsn5.swap(tsn3);
        checkANServices(tsn5);

        /*
         * Now there are RNs on sn1,2 and AN in sn5. Try migrating sn5 -> sn6.
         * Should fail, sn5 is alive.
         */
        planId = runWithFailure("Migrate sn5 to sn6 with Retry", cs, sn5, sn6);

        /*
         * Now shutdown sn5, and retry the plan (using the same plan instance)
         * sn3, sn4, sn5 should be down.
         */
        tsn5.sna.shutdown(true, true);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        verify(cs, sn3, sn4, sn5);
        checkServices(tsn1);
        checkServices(tsn2);
        tsn6.swap(tsn5);
        checkANServices(tsn6);

        /*
         * Recreate and rerun the plan, no topology changes should happen.
         */
        planId = cs.createMigrateSNPlan("Repeat sn5 to sn6", sn5, sn6);
        runPlan(cs, planId);
        verify(cs, sn3, sn4, sn5);
        checkServices(tsn1);
        checkServices(tsn2);
        checkANServices(tsn6);

        /* remove all the empty SNs, and re-verify. */
        cleanupDeadSNs(cs, sn3, sn4, sn5);
        verify(cs);
    }

    /**
     * check that all SNs are up, except for the list of expected dead SNs.
     */
    private void verify(CommandServiceAPI cs,
                        StorageNodeId ... expectedDeadSNs)
        throws RemoteException {

        VerifyResults results = cs.verifyConfiguration(true, true, false);
        assertEquals(results.display(),
                     expectedDeadSNs.length,
                     results.numViolations());

        List<Problem> violations = results.getViolations();
        for (int i = 0; i < expectedDeadSNs.length; i++) {
            assertEquals(expectedDeadSNs[i], violations.get(i).getResourceId());
            assertTrue(violations.get(i) instanceof RMIFailed);
        }
    }

    /**
     * Check that all verification exceptions are only for the expected
     * set of problematic resources.
     * @return any unexpected problems.
     */
    private Problem verifyWithExpectedViolations
        (CommandServiceAPI cs, ResourceId ... expectedProblems)
        throws RemoteException {

        VerifyResults results = cs.verifyConfiguration(true, true, false);
        Set<ResourceId> expected =
            new HashSet<ResourceId>(Arrays.asList(expectedProblems));

        for (Problem p : results.getViolations()) {
            if (!expected.contains(p.getResourceId())) {
                return p;
            }
        }
        return null;
    }

    /**
     * remove the list of empty, dead SNs.
     */
    private void cleanupDeadSNs(CommandServiceAPI cs,
                                StorageNodeId ... expectedDeadSNs)
        throws RemoteException {

        for (StorageNodeId snId : expectedDeadSNs) {
            int removePlanId = cs.createRemoveSNPlan("remove " + snId, snId);
            removeSN(cs,snId, removePlanId, true);
        }
    }

    /** Check that each SN has a RepNode on it, and an Admin if configured. */
    private void checkServices(TestSN testSN)
        throws RemoteException, NotBoundException {

        int snRegistryPort = testSN.registryPort;

        try {
            CommandServiceAPI cs =
                RegistryUtils.getAdmin("localhost", snRegistryPort, loginMgr,
                                       logger);
            if (!testSN.usesAdmin()) {
                fail("Should not be able to contact an Admin on " +
                     testSN.getSNId());
            }
            ServiceStatus adminStatus = cs.ping();
            assertEquals(ServiceStatus.RUNNING, adminStatus);

        } catch (RemoteException e) {
            attemptConnectToAdmin(testSN, e);
        } catch (NotBoundException e) {
            attemptConnectToAdmin(testSN, e);
        }

        RepNodeAdminAPI rnAPI = RegistryUtils.getRepNodeAdmin(
            kvstoreName, "localhost", snRegistryPort, testSN.getRepNodeId(),
            loginMgr, logger);

        RepNodeStatus rnStatus = rnAPI.ping();
        assertEquals(testSN.getRepNodeId() + " " + rnStatus,
                     ServiceStatus.RUNNING, rnStatus.getServiceStatus());
    }

    /** Check that SN has a ArbNode on it. */
    private void checkANServices(TestSN testSN)
        throws RemoteException, NotBoundException {

        int snRegistryPort = testSN.registryPort;

        ArbNodeAdminAPI anAPI = RegistryUtils.getArbNodeAdmin(
            kvstoreName, "localhost", snRegistryPort, testSN.getArbNodeId(),
            loginMgr, logger);

        ArbNodeStatus anStatus = anAPI.ping();
        assertEquals(testSN.getArbNodeId() + " " + anStatus,
                     ServiceStatus.RUNNING, anStatus.getServiceStatus());
    }

    private void attemptConnectToAdmin(TestSN testSN, Exception e) {
        /*
         * If !usesAdmin(), we expect a RMIException -- there should not be
         * an Admin on this node.
         */
        if (testSN.usesAdmin()) {
            fail("Problem accessing Admin on " + testSN.getSNId() +
                 " " + e.toString());
        }
    }

    private void expectCreateFailure(CommandServiceAPI cs,
                                     StorageNodeId oldNode,
                                     StorageNodeId newNode)
        throws RemoteException {
        try {
            cs.createMigrateSNPlan("ExpectCreateFail", oldNode, newNode);
            fail("expect failure");
        } catch (AdminFaultException expected) {
            assertEquals("oracle.kv.impl.admin.IllegalCommandException",
                         expected.getFaultClassName());
        }
    }

    private int runWithFailure(String planName,
                               CommandServiceAPI cs,
                               StorageNodeId oldNode,
                               StorageNodeId newNode)
        throws RemoteException {

        int planId = 0;
        try {
            planId = cs.createMigrateSNPlan(planName, oldNode, newNode);
            runPlan(cs, planId);
            fail("expect failure");
        } catch (AdminFaultException expected) {
            String exName = expected.getFaultClassName();
            assertTrue(exName.equals
                       ("oracle.kv.impl.fault.OperationFaultException") ||
                       exName.equals
                       ("oracle.kv.impl.admin.IllegalCommandException"));
        }

        return planId;
    }

    private void expectRunFailure(CommandServiceAPI cs,
                                  StorageNodeId oldNode,
                                  StorageNodeId newNode)
        throws RemoteException {

        int planId = 0;
        try {
            planId = runWithFailure("ExpectRunFailure", cs, oldNode, newNode);
        } finally {
            cs.cancelPlan(planId);
        }
    }

    /**
     * Run the removePlanId, and check that it's been successful.
     */
    private void removeSN(CommandServiceAPI cs,
                          StorageNodeId deleteTarget,
                          int removePlanId,
                          boolean checkForExistence)
        throws RemoteException {

        Topology originalTopo = cs.getTopology();

        /* The SN should exist. */
        if (checkForExistence) {
            StorageNode sn = originalTopo.getStorageNodeMap().get(deleteTarget);
            assertTrue(sn != null);
        }

        /* Remove the SN. */
        runPlan(cs, removePlanId);

        /*
         * Check that the SN does not exist in the topology, params, or
         * storage node pools.
         */
        Topology resultingTopo = cs.getTopology();
        StorageNode sn = resultingTopo.getStorageNodeMap().get(deleteTarget);
        assertTrue(sn == null);

        List<String> poolNames = cs.getStorageNodePoolNames();
        for (String pn : poolNames) {
            for (StorageNodeId snid : cs.getStorageNodePoolIds(pn)) {
                assertTrue(deleteTarget + " exists in storage node pool " + pn,
                           !snid.equals(deleteTarget));
            }
        }

        Parameters params = cs.getParameters();
        assertTrue(deleteTarget + " should not exist in params, but does",
                   params.get(deleteTarget) == null);
    }

    private void verifyAdminHelpers(CommandServiceAPI cs)
        throws RemoteException {

        Collection <AdminParams> aParams = cs.getParameters().getAdminParams();
        for (AdminParams ap : aParams) {
            String myNodeHostPort = ap.getNodeHostPort();
            for (AdminParams ap1 : aParams) {
                if (ap.equals(ap1)) {
                    continue;
                }
                assertTrue
                    ("Missing from " + ap1.getAdminId() + ": " +
                     myNodeHostPort,
                     ap1.getHelperHosts().contains(myNodeHostPort));
            }
        }
    }
}
