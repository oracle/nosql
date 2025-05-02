/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.ServiceStopped;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.plan.task.RelocateRN;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MissingStorageDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Test problems with RN relocation, which is done for rebalancing, and for
 * migrate-rn.
 */
public class DeployRelocateRNTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        RelocateRN.FAULT_HOOK = null;
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
     * When moving a repnode onto a SN with storage directories, make sure that
     * they are assigned to use storage dirs.
     */
    @Test
    public void testMoveRepNodeStorageDir()
        throws Exception {

        CommandServiceAPI[] csArray = new CommandServiceAPI[9];
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 4 /* max of 4 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        /* Deploy two more SNs on DataCenterA */
        csArray[0] = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        CommandServiceAPI cs = csArray[0];
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        cs.verifyConfiguration(false, true, false);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 30, false);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              1 /*numRGs*/, 3 /*numRNs*/, 30 /*numParts*/);

        /* Add a fourth SN that has a storage directory*/
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3);
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
                File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir1_A", null, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 3);


        /* Move rg1-rn1 to sn4 */
        RepNodeId target = new RepNodeId(1, 1);
        String move = "move";
        cs.copyCurrentTopology(move);
        cs.moveRN(move, target, new StorageNodeId(4));
        DeployUtils.printCandidate(cs, move, logger);
        DeployUtils.printPreview(move, false /* noChange*/, cs, logger);

        planNum = cs.createDeployTopologyPlan(move, move, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /* Check that the mount point was used. */
        Parameters params = cs.getParameters();
        RepNodeParams rnp = params.get(target);
        final File file = rnp.getStorageDirectoryFile();
        assertTrue(file != null);
        File envHome = new File(file, target.toString());
        assertTrue(envHome.toString(), envHome.exists());

        Set<Problem> noProblems = Collections.emptySet();
        Set<Class<?>> permissible = new HashSet<Class<?>>();
        permissible.add(UnderCapacity.class);
        permissible.add(InsufficientAdmins.class);
        permissible.add(MissingStorageDirectorySize.class);
        permissible.add(MissingRootDirectorySize.class);
        DeployUtils.checkDeployment(cs, 3, noProblems, permissible, logger);
    }

    /**
     * Test handling of cases where a shard does not have quorum, and cannot
     * update its group db.
     */
    @Test
    public void test2PtFailureCleanup()
        throws Exception {

        CommandServiceAPI[] csArray = new CommandServiceAPI[9];
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 4 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        /* Deploy two more SNs on DataCenterA */
        csArray[0] = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        CommandServiceAPI cs = csArray[0];
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);

        /* deploy an Admin on each SN */
        for (int i = 1; i <3; i++) {
            csArray[i] = AdminUtils.deployAdmin(csArray[0], snSet, i);
        }

        cs.verifyConfiguration(false, true, false);
        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/, 100 /*numParts*/);

        /* Change the capacity so the store is out of compliance */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0);
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3);

        String fix = "fix";
        cs.copyCurrentTopology(fix);
        cs.rebalanceTopology(fix, Parameters.DEFAULT_POOL_NAME, null);
        DeployUtils.printCandidate(cs, fix, logger);
        DeployUtils.printPreview(fix, false /* noChange*/, cs, logger);

        /*
         * SN2 has a RN from each shard. Shut it down, so that both shards
         * only have 2 live RNs (those on sn1 and sn3). This makes the test
         * sure to exercise the 2-failure case when moving one of the RNs
         * away.
         */
        snSet.shutdown(1, logger);
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
        permissible.add(OverCapacity.class);
        permissible.add(UnderCapacity.class);
        permissible.add(MultipleRNsInRoot.class);
        permissible.add(InsufficientAdmins.class);
        permissible.add(MissingStorageDirectorySize.class);
        permissible.add(MissingRootDirectorySize.class);
        DeployUtils.checkDeployment(cs, 3, empty, permissible, logger);
    }

    /**
     * Tests that the plan re-exec takes care of all errors
     */
    @Test
    public void testRelocateFailureReExec() throws Exception {
        relocateWithFailures(true /* killCleanup */,
                             true /* reexec */,
                             false /* runRepair*/);
    }

    /**
     * Tests that the repair plan cleans up all errors.
     */
    @Test
    public void testRelocateFailureRepair() throws Exception {
        relocateWithFailures(true /* killCleanup */,
                             false /* reexec */,
                             true /* runRepair*/);
    }

    /**
     * Tests that the task cleanup is successful, and leaves no errors.
     */
    @Test
    public void testRelocateFailureCleanup() throws Exception {
        relocateWithFailures(false /* killCleanup */,
                             false /* reexec */,
                             false /* runRepair*/);
    }

    /**
     * Setup a 2 shard system, with multiple RNs on a single SN, and then
     * move a RN around, invoking failures at key places. Fix the problems by
     * A. Letting the cleanup code run.
     * B. reexecing the topology plan.
     * C. Running the repair plan.
     */

    private void relocateWithFailures(boolean killCleanup,
                                      boolean reexec,
                                      boolean runRepair)
        throws Exception {

        final int NUM_PARTITIONS = 10;

        /* Bootstrap the Admin, the first DC, and 5 SNs */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 5 /* max SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        /*
         * Enable statistics gathering to work around a problem involving
         * waiting for an up-to-date shard [#26468]
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_SG_ENABLED, "true");
        cs.setPolicies(mergeParameterMapDefaults(map));

        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3, 4);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);

        /* Put a mount point on the first and fourth SNs */
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDirA_sn1", null, 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDirB_sn1", null, 0);
        snSet.changeMountPoint(cs, true, testdir + "storageDirA_sn2", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDirB_sn2", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDirA_sn3", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDirB_sn3", null, 1);

        /* make an initial topology */
        final String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, NUM_PARTITIONS,
                          false);

        Topology candidateTopo = cs.getTopologyCandidate(first).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1 /*dc*/, 5 /*nSNs*/,
                              2, 6, NUM_PARTITIONS);
        int planNum = cs.createDeployTopologyPlan(first, first, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        RepNodeId rg1_rn1 = new RepNodeId(1, 1);
        RepNodeId rg1_rn2 = new RepNodeId(1, 2);
        RepNodeId rg1_rn3 = new RepNodeId(1, 3);

        /*
         * Do a series of runs where a failure is injected in the middle of
         * the relocate. If killCleanup is specified, a second failure is
         * injected before task cleanup, so the plan finishes with the store in
         * an inconsistent state.
         *
         * Note that the test needs a different set of RN movements depending
         * on whether the topology is supposed to be fixed by reexec, task
         * cleanup, or the repair plan. If the re-exec is used, then the
         * move is supposed to make the change successful. If cleanup or repair
         * is used, the movement may be reverted, depending on when the failure
         * was injected. For example, suppose we are testing the relocation of
         * RNA from SNx to SNy. If the test does reexec, no matter where the
         * fault is injected, the RN will successfully move. If the test uses
         * task cleanup or repair plan to fix inconsistencies, the RN will
         * sometimes remain on SNx, and sometimes move to SNy, depending on
         * where the fault was injected.
         */

        /* Move rn2 from SN2 to SN4, kill right after the first RN stop */
        failAndRerun(cs, "fail2", rg1_rn2, 4 /*sn*/, 2 /*hook*/,
                     killCleanup, reexec, runRepair, rg1_rn2);

        /*
         * Move rn1 from SN1 to SN5, kill right after the topo and params
         * are changed
         */
        failAndRerun(cs, "fail3", rg1_rn1, 5 /*sn*/, 3 /*hook*/,
                     killCleanup, reexec, runRepair, rg1_rn1, rg1_rn2,
                     rg1_rn3);

        if (reexec) {
            /*
             * Move rn3 from SN3 to SN1, kill right after the JE HA group is
             * updated
             */
            failAndRerun(cs, "fail4", rg1_rn3, 1 /*sn*/, 4 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_rn3, rg1_rn2,
                         rg1_rn1);
            /*
             * Move rn1 from SN5 to SN3, kill after param, topo, JE updated,
             * but not yet started on new SN
             */
            failAndRerun(cs, "fail5", rg1_rn1, 3 /*sn*/, 5 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_rn1);

            /*
             * Move rn2 from SN4 to SN5, kill just before SN is removed
             */
            failAndRerun(cs, "fail6", rg1_rn2, 5 /*sn*/, 6 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_rn2);
        } else {
            /*
             * Move rn3 from SN3 to SN4, kill right after the JE HA group is
             * updated
             */
            failAndRerun(cs, "fail4", rg1_rn3, 4 /*sn*/, 4 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_rn3, rg1_rn2,
                         rg1_rn1);
            /*
             * Move rn1 from SN1 to SN3, kill after param, topo, JE updated,
             * but not yet started on new SN
             */
            failAndRerun(cs, "fail5", rg1_rn1, 3 /*sn*/, 5 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_rn1);

            /*
             * Move rn1 from SN2 to SN5, kill just before SN is removed
             */
            failAndRerun(cs, "fail6", rg1_rn1, 5 /*sn*/, 6 /*hook*/,
                         killCleanup, reexec, runRepair, rg1_rn1);
        }
    }

    /**
     * Move a RN and provoke one or more failures in the middle of the task.
     * Clear the test hook and re-execute the test and it should work.
     *
     * @param target RN to move
     * @param hookVal which test hook value from RelocateRN to die at. This
     *  test is very supsceptible to any hook changes.
     * @param killCleanup if true, we'll kill the RelocateRN task before
     *  cleanup
     * @param rnsWithProblems the set of RNs that should fail the verify
     */
    private void failAndRerun(CommandServiceAPI cs,
                              String topoName,
                              RepNodeId target,
                              int destinationSN,
                              int hookVal,
                              boolean killCleanup,
                              boolean reexec,
                              boolean runRepair,
                              RepNodeId... rnsWithProblems)
        throws RemoteException {

        /* Make a topology that moves target  */
        cs.copyCurrentTopology(topoName);
        cs.moveRN(topoName, target, new StorageNodeId(destinationSN));

        /* Run it with a failure */
        if (killCleanup) {
            RelocateRN.FAULT_HOOK = new TaskHook(hookVal, 7);
        } else {
            RelocateRN.FAULT_HOOK = new TaskHook(hookVal);
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
        Set<RepNodeId> sawProblems = new HashSet<RepNodeId>();
        List<Problem> violations = vResult.getViolations();
        for (Problem p : violations) {
            if (p.getResourceId() instanceof RepNodeId) {
                sawProblems.add((RepNodeId)p.getResourceId());
            }
        }

        for (Problem p : vResult.getWarnings()) {
            if (p instanceof ServiceStopped) {
                sawProblems.add((RepNodeId)p.getResourceId());
            }
        }

        Set<RepNodeId> expected = new HashSet<RepNodeId>();
        for (RepNodeId rnId: rnsWithProblems) {
            expected.add(rnId);
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
            /* The cleanup should have fixed all violations but the admin */
            assertEquals("Violations exist: " + violations,
                         1, violations.size());
            assertTrue(violations.get(0).getClass().
                            equals(InsufficientAdmins.class));
        }

        /* Repair this situation by reexecing */
        if (reexec) {
            /* Execute this plan again! */
            RelocateRN.FAULT_HOOK = null;
            cs.executePlan(planNum, false);
            cs.awaitPlan(planNum, 0, null);
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
        permissible.add(UnderCapacity.class);
        permissible.add(MultipleRNsInRoot.class);
        permissible.add(InsufficientAdmins.class);
        permissible.add(MissingStorageDirectorySize.class);
        permissible.add(MissingRootDirectorySize.class);
        DeployUtils.checkDeployment(cs, 3, empty, permissible, logger);
    }

    /*
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
                     * Wait for the RN to appear on its SN.
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
