/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.ServiceStopped;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.task.RelocateAN;
import oracle.kv.impl.admin.topo.Validations.ANNotAllowedOnSN;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.ArbNode;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Test problems with AN relocation.
 */
public class DeployRelocateANTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    private final String poolName = Parameters.DEFAULT_POOL_NAME;

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        RelocateAN.FAULT_HOOK = null;
        super.tearDown();
        if (su != null) {
            su.close();
        }

        if (sysAdminInfo != null) {
            sysAdminInfo.getSNSet().shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Test handling of cases where a RF=2 shard has just 1 RN and
     * 1 AN, and cannot relocate the arbiter.
     */
    @Test
    public void testRNShutdownANRelocateFailure()
        throws Exception {
        /*
         * Create a topology with 1 Zone.
         * ZN1: 2 SNs (Capacity 1), 1 SN (Capacity 0).
         * Zone RF=2. Primary Zone and allows Arbiters.
         * Initial topology created with primary RF=2. AN in SN3.
         * Change SN3 parameter to not allow arbiters.
         * Deploy 0 capacity SN (SN4). Rebalance the topology.
         * Candidate topology has AN relocating from SN3 to SN4.
         * Shutdown SN2 and deploy the candidate topology.
         * Deployment is unsuccessful because the shard should
         * have atleast 2 electable nodes to execute writes.
         */

        CommandServiceAPI[] csArray = new CommandServiceAPI[9];
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 6 /* SNs */,
                                                 "DataCenterA",
                                                 2 /* repFactor*/,
                                                 false /* useThreads */,
                                                 MB_PER_SN_STRING /* memory */,
                                                 true);

        /* Deploy two more SNs on DataCenterA */
        csArray[0] = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        CommandServiceAPI cs = csArray[0];
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 2);

        cs.verifyConfiguration(false, true, false);
        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, poolName, 100, false);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.checkTopo(cs.getTopology(),
                              1 /* numDCs */,
                              3 /*numSNs*/,
                              1 /*numRGs*/,
                              2 /*numRNs*/,
                              1 /*numArbs*/,
                              100 /*numParts*/);

        /*
         * Change SN3 parameter SN_HOST_ARBITERS to false so that the store
         * is out of compliance.
         */
        snSet.changeParams(cs, ParameterState.SN_ALLOW_ARBITERS, "false", 2);
        /*
         * Deploy SN4 with capacity 0.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 3);

        String fix = "fix";
        cs.copyCurrentTopology(fix);
        /*
         * Rebalance the topology. Arbiter rg1-an1 relocates from
         * SN3 to SN4.
         */
        cs.rebalanceTopology(fix, poolName, null);
        DeployUtils.printCandidate(cs, fix, logger);
        DeployUtils.printPreview(fix, false /* noChange*/, cs, logger);
        /*
         * Load some data.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /*
         * SN2 has 1 RN for the shard. Shut it down, so that the shard
         * only have 1 live RN (on SN1) and 1 arbiter (on SN3). This makes
         * the test sure to exercise the 2-failure case when moving the
         * arbiter away.
         */
        snSet.shutdown(1, logger);

        /*
         * Load some data with SN2 shutdown.
         */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1);
        expectedNumRecords = 500;
        allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
        planNum = cs.createDeployTopologyPlan(fix, fix, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        try {
            cs.assertSuccess(planNum);
            fail("Should have failed");
        } catch (Exception expected) {
        }

        DeployUtils.printCurrentTopo(cs, "After Failure", logger);
        DeployUtils.printPreview(first, true /* noChange*/, cs, logger);

        /*
         * Restart SN2, and then check that all services are up and available.
         */
        Topology topo = cs.getTopology();
        Set<RepNodeId> rnsOnSN = new HashSet<RepNodeId>();
        StorageNodeId targetSN = snSet.getId(1);
        for (RepNodeId rnId : topo.getRepNodeIds()) {
            if (topo.get(rnId).getStorageNodeId().equals(targetSN)) {
                rnsOnSN.add(rnId);
            }
        }
        snSet.restart(1, rnsOnSN, logger);

        Set<Problem> empty = Collections.emptySet();
        Set<Class<?>> permissible = new HashSet<Class<?>>();
        permissible.add(ANNotAllowedOnSN.class);
        permissible.add(InsufficientAdmins.class);
        permissible.add(MissingRootDirectorySize.class);
        DeployUtils.checkDeployment(cs, 2, empty, permissible, logger);
    }

    /**
     * Tests that the plan re-exec takes care of all errors.
     * The test injects fake errors at key points while relocating
     * AN and insures that reexecuting the plan without setting any
     * hooks is successful.
     */
    @Test
    public void testRelocateFailureReExec() throws Exception {
        relocateWithFailures(true /* killCleanup */,
                             true /* reexec */,
                             false /* runRepair*/);
    }

    /**
     * Tests that the repair plan cleans up all errors.
     * The test injects fake errors at key points while relocating
     * AN and insures that running a repair plan takes care of all
     * the errors.
     */
    @Test
    public void testRelocateFailureRepair() throws Exception {
        relocateWithFailures(true /* killCleanup */,
                             false /* reexec */,
                             true /* runRepair*/);
    }

    /**
     * Tests that the task cleanup is successful, and leaves no errors.
     * The test injects fake errors at key points while relocating
     * AN and insures that the task cleanup job is successful and takes
     * care of all the errors.
     */
    @Test
    public void testRelocateFailureCleanup() throws Exception {
        relocateWithFailures(false /* killCleanup */,
                             false /* reexec */,
                             false /* runRepair*/);
    }

    /**
     * Setup a 2 shard system, with multiple RNs on a single SN, and then
     * move a AN around, invoking failures at key places. Fix the problems by
     * A. Letting the cleanup code run.
     * B. reexecing the topology plan.
     * C. Running the repair plan.
     */

    private void relocateWithFailures(boolean killCleanup,
                                      boolean reexec,
                                      boolean runRepair)
        throws Exception {

        final int NUM_PARTITIONS = 10;

        /* Bootstrap the Admin, the first DC, and 6 SNs */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 6 /* SNs */,
                                                 "DataCenterA",
                                                 2 /* repFactor*/,
                                                 true /* useThreads */,
                                                 MB_PER_SN_STRING /* memory */,
                                                 true);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3, 4, 5);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 2, 3, 4, 5);

        /* make an initial topology */
        final String first = "firstCandidate";
        cs.createTopology(first, poolName, NUM_PARTITIONS, false);

        Topology candidateTopo = cs.getTopologyCandidate(first).getTopology();
        DeployUtils.checkTopo(candidateTopo,
                              1 /* numDCs */,
                              6 /*numSNs*/,
                              2 /*numRGs*/,
                              4 /*numRNs*/,
                              2 /*numArbs*/,
                              NUM_PARTITIONS /*numParts*/);
        int planNum = cs.createDeployTopologyPlan(first, first, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        ArbNodeId rg1_an1 = new ArbNodeId(1, 1);
        RepNodeId rg1_rn1 = new RepNodeId(1, 1);
        RepNodeId rg1_rn2 = new RepNodeId(1, 2);
        ArbNodeId rg2_an1 = new ArbNodeId(2, 1);
        RepNodeId rg2_rn1 = new RepNodeId(2, 1);
        RepNodeId rg2_rn2 = new RepNodeId(2, 2);

        /*
         * Do a series of runs where a failure is injected in the middle of
         * the relocate. If killCleanup is specified, a second failure is
         * injected before task cleanup, so the plan finishes with the store in
         * an inconsistent state.
         *
         * Note that the test needs a different set of AN movements depending
         * on whether the topology is supposed to be fixed by reexec, task
         * cleanup, or the repair plan. If the re-exec is used, then the
         * move is supposed to make the change successful. If cleanup or repair
         * is used, the movement may be reverted, depending on when the failure
         * was injected. For example, suppose we are testing the relocation of
         * ANA from SNx to SNy. If the test does reexec, no matter where the
         * fault is injected, the AN will successfully move. If the test uses
         * task cleanup or repair plan to fix inconsistencies, the AN will
         * sometimes remain on SNx, and sometimes move to SNy, depending on
         * where the fault was injected.
         */

        /* Move rg1-an1 from SN3 to SN5, kill right after the first AN stop */
        failAndRerun(cs, "fail2", rg1_an1, 2 /*hook*/,
                     killCleanup, reexec, runRepair, rg1_an1);

        /*
         * Move rg2-an1 from SN4 to SN3, kill right after the topo and params
         * are changed
         */
        failAndRerun(cs, "fail3", rg2_an1, 3 /*hook*/,
                     killCleanup, reexec, runRepair, rg2_an1, rg2_rn1,
                     rg2_rn2);

        if (reexec) {
            /*
             * Move rg1-an1 from SN5 to SN4, kill right after the JE HA group is
             * updated
             */
            failAndRerun(cs, "fail4", rg1_an1, 4 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_an1, rg1_rn1,
                         rg1_rn2);
            /*
             * Move rg2-an1 from SN3 to SN5, kill after param, topo, JE updated,
             * but not yet started on new SN
             */
            failAndRerun(cs, "fail5", rg2_an1, 5 /*hook*/,
                         killCleanup, reexec, runRepair, rg2_an1);

            /*
             * Move rg1-an1 from SN4 to SN3, kill just before SN is removed
             */
            failAndRerun(cs, "fail6", rg1_an1, 6 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_an1);
        } else {
            /*
             * Move rg1-an1 from SN3 to SN5, kill right after the JE HA group is
             * updated
             */
            failAndRerun(cs, "fail4", rg1_an1, 4 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_an1, rg1_rn1,
                         rg1_rn2);
            /*
             * Move rg2-an1 from SN4 to SN6, kill after param, topo, JE updated,
             * but not yet started on new SN
             */
            failAndRerun(cs, "fail5", rg2_an1, 5 /*hook*/,
                         killCleanup, reexec, runRepair, rg2_an1);

            /*
             * Move rg1-an1 from SN5 to SN4, kill just before SN is removed
             */
            failAndRerun(cs, "fail6", rg1_an1, 6 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_an1);
        }
    }

    /**
     * Move a AN and provoke one or more failures in the middle of the task.
     * Clear the test hook and re-execute the test and it should work.
     *
     * @param target AN to move
     * @param hookVal which test hook value from RelocateAN to die at. This
     *  test is very susceptible to any hook changes.
     * @param killCleanup if true, we'll kill the RelocateAN task before
     *  cleanup
     * @param ridsWithProblems the set of RNs/ANs that should fail the verify
     */
    private void failAndRerun(CommandServiceAPI cs,
                              String topoName,
                              ArbNodeId target,
                              int hookVal,
                              boolean killCleanup,
                              boolean reexec,
                              boolean runRepair,
                              ResourceId... ridsWithProblems)
        throws RemoteException {

        ArbNode arbNode = cs.getTopology().get(target);
        int sourceSNIndex = arbNode.getStorageNodeId().getStorageNodeId() - 1;
        /* Make a topology that moves target AN */
        moveAN(topoName, target);

        /* Run it with a failure */
        if (killCleanup) {
            RelocateAN.FAULT_HOOK = new TaskHook(hookVal, 7);
        } else {
            RelocateAN.FAULT_HOOK = new TaskHook(hookVal);
        }

        int planNum = cs.createDeployTopologyPlan(topoName, topoName, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        try {
            cs.assertSuccess(planNum);
            fail("Should have failed");
        } catch (Exception expected) {
            /* nothing to do */
        }

        logger.info("====== After intentional failure =====");

        VerifyResults vResult = cs.verifyConfiguration(false, true, false);
        Set<ResourceId> sawProblems = new HashSet<ResourceId>();
        List<Problem> violations = vResult.getViolations();
        for (Problem p : violations) {
            if ((p.getResourceId() instanceof RepNodeId) ||
                (p.getResourceId() instanceof ArbNodeId)) {
                sawProblems.add(p.getResourceId());
            }
        }

        for (Problem p : vResult.getWarnings()) {
            if (p instanceof ServiceStopped) {
                sawProblems.add(p.getResourceId());
            }
        }

        Set<ResourceId> expected = new HashSet<ResourceId>();
        for (ResourceId rid: ridsWithProblems) {
            expected.add(rid);
        }

        if (killCleanup) {
            /*
             * The cleanup itself was killed, so we do expect that there will
             * be violations from the verify.
             */

            assertTrue("expected=" + expected + " saw=" + sawProblems,
                       expected.containsAll(sawProblems));
            assertTrue("expected=" + expected + " saw=" + sawProblems,
                       sawProblems.containsAll(expected));
        } else {
            /*
             * The cleanup should have fixed all violations but the admin
             * and/or arbiter hosted in SN that does not allow arbiters.
             */
            List<Class<?>> violationClass = new ArrayList<Class<?>>();
            if (violations.size() == 2) {
                violationClass.add(violations.get(0).getClass());
                violationClass.add(violations.get(1).getClass());
                assertTrue(violationClass.contains(InsufficientAdmins.class));
                assertTrue(violationClass.contains(ANNotAllowedOnSN.class));
            } else {
                assertTrue(violations.size() == 1);
                violationClass.add(violations.get(0).getClass());
                assertTrue(violationClass.contains(InsufficientAdmins.class));
            }
        }

        /* Repair this situation by reexecing */
        if (reexec) {
            /* Execute this plan again! */
            RelocateAN.FAULT_HOOK = null;
            cs.executePlan(planNum, false);
            cs.awaitPlan(planNum, 0, null);
            cs.assertSuccess(planNum);
        } else if (runRepair) {
            planNum = cs.createRepairPlan("RepairPlan");
            cs.approvePlan(planNum);
            cs.executePlan(planNum, false);
            cs.awaitPlan(planNum, 0, null);
            cs.assertSuccess(planNum);
        }

        /* Check deployment */
        Set<Problem> empty = Collections.emptySet();
        Set<Class<?>> permissible = new HashSet<Class<?>>();
        permissible.add(ANNotAllowedOnSN.class);
        permissible.add(MultipleRNsInRoot.class);
        permissible.add(InsufficientAdmins.class);
        permissible.add(MissingRootDirectorySize.class);
        DeployUtils.checkDeployment(cs, 3, empty, permissible, logger);

        /* Revert the source SN parameters to allow hosting arbiters */
        revertSNParams(sourceSNIndex);
    }

    /**
     * Change the parameter of the SN to allow hosting arbiters.
     *
     * @param sourceSNIndex
     * @throws RemoteException
     */
    private void revertSNParams(int sourceSNIndex)
        throws RemoteException {

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.changeParams(cs,
                           ParameterState.SN_ALLOW_ARBITERS,
                           "true",
                           sourceSNIndex);
    }

    /**
     * Change the parameter of the arbiter hosting SN to not allow arbiters.
     * Use rebalance to alter the topology so that the arbiter to moved to
     * another SN which allows arbiters.
     *
     * @param topoName
     * @param target
     * @throws RemoteException
     */
    private void moveAN(String topoName, ArbNodeId target)
        throws RemoteException {

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        ArbNode arbNode = cs.getTopology().get(target);
        int sourceSNIndex = arbNode.getStorageNodeId().getStorageNodeId() - 1;
        snSet.changeParams(cs,
                           ParameterState.SN_ALLOW_ARBITERS,
                           "false",
                           sourceSNIndex);
        cs.copyCurrentTopology(topoName);
        cs.rebalanceTopology(topoName, poolName, null);
    }

    /**
     * A test hook that throws a RuntimeException when the hook value matches
     * the target value.
     */
    private class TaskHook implements TestHook<Integer> {
        private final Set<Integer> targets;

        TaskHook(int... targetVals) {
            this.targets = new HashSet<Integer>();
            for (int i : targetVals) {
                targets.add(i);
            }
        }

        @Override
        public void doHook(Integer value) {
            if (targets.contains(value)) {
                if (value == 6) {
                    /*
                     * Wait for the AN to appear on its SN.
                     */
                    try {
                        Thread.sleep(1000 * 10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                throw new RuntimeException("Injecting fault at point " +
                                           value);
            }
        }
    }

}
