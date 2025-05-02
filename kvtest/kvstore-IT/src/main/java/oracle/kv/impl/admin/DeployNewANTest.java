/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;

import java.util.HashSet;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.ServiceStopped;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.task.DeployNewARB;
import oracle.kv.impl.admin.plan.task.DeployNewRN;
import oracle.kv.impl.admin.plan.task.RemoveAN;
import oracle.kv.impl.admin.topo.Validations.ExcessANs;
import oracle.kv.impl.admin.topo.Validations.HelperParameters;
import oracle.kv.impl.admin.topo.Validations.InsufficientANs;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.NoPartition;
import oracle.kv.impl.admin.topo.Validations.NoPrimaryDC;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.AdminType;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/*
 * Test problems with AN deployment.
 */
public class DeployNewANTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    private final String poolName = Parameters.DEFAULT_POOL_NAME;

    @Override
    public void setUp() throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        DeployNewRN.FAULT_HOOK = null;
        DeployNewARB.FAULT_HOOK = null;
        RemoveAN.FAULT_HOOK = null;

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
     * Tests that the repair plan cleans up all the errors.
     * The test injects a fake error right after the updated
     * topology and the new AN/RN params are persisted in the
     * Admin DB. Insure that the repair plan clears the new
     * RN/AN from the Admin DB (topology and parameters).
     */
    @Test
    public void testDepoloyNewANRepair() throws Exception {
        failAndRepair(true /* KillCleanup */);
    }

    /**
     * Tests that the task cleanup is successful, and leaves no errors.
     * The test injects a fake error right after the updated
     * topology and the new AN/RN params are persisted in the
     * Admin DB. Insure that the task cleanup clears the new
     * RN/AN from the Admin DB (topology and parameters).
     */
    @Test
    public void testDepoloyNewANCleanup() throws Exception {
        failAndRepair(false /* KillCleanup */);
    }

    @Test
    public void testRemoveANRepair() throws Exception {
        failAndRepair2(true /* KillCleanup */);
    }

    @Test
    public void testRemoveANCleanup() throws Exception {
        failAndRepair2(false /* KillCleanup */);
    }

    /**
     * Deploy a single shard with Primary RF=2. Deploy 2 RNs and
     * 1 arbiter. Invoke failures at key places while deploying
     * RNs and arbiters. Fix the problems by:
     * A. Running the Repair Plan
     * B. Letting the cleanup code run.
     */
     private void failAndRepair(boolean killCleanup) throws Exception {

        final int NUM_PARTITIONS = 10;

        /* Bootstrap the Admin, the first DC, and 3 SNs */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 3 /* SNs */,
                                                 "DataCenterA",
                                                 2 /* repFactor*/,
                                                 true /* useThreads */,
                                                 MB_PER_SN_STRING /* memory */,
                                                 true);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 2);

        TaskHook taskHook = null;
        taskHook = new TaskHook(killCleanup);
        DeployNewARB.FAULT_HOOK = taskHook;

        Set<Problem> startingProblems = new HashSet<Problem>();
        Set<Class<?>> permissible = new HashSet<Class<?>>();

        try {
            int attempt = 0;
            Plan.State result = null;
            while (result != Plan.State.SUCCEEDED) {
                attempt++;
                logger.info("Attempt " + attempt);
                String candidateName = "Candidate_" + attempt;
                cs.createTopology(candidateName,
                                  poolName,
                                  NUM_PARTITIONS,
                                  false);
                permissible.clear();
                int planNum = cs.createDeployTopologyPlan(candidateName,
                                                          candidateName, null);
                cs.approvePlan(planNum);
                cs.executePlan(planNum, false);
                result = cs.awaitPlan(planNum, 0, null);

                try {
                    cs.assertSuccess(planNum);
                } catch (Exception expected) {
                    /* nothing to do */
                }

                if (!result.equals(Plan.State.SUCCEEDED)) {
                    cs.cancelPlan(planNum);
                } else {
                    /*
                     * No issues except InsufficientAdmins should exist after
                     * the plan has succeeded.
                     */
                    permissible.add(InsufficientAdmins.class);
                    permissible.add(NoPartition.class);
                    permissible.add(MissingRootDirectorySize.class);
                    DeployUtils.checkDeployment(cs,
                                                2,
                                                startingProblems,
                                                permissible,
                                                logger);
                }

                /*
                 * Repair this situation by running a repair plan.
                 */
                if (killCleanup) {
                    planNum = cs.createRepairPlan("RepairPlan");
                    cs.approvePlan(planNum);
                    cs.executePlan(planNum, false);
                    cs.awaitPlan(planNum, 0, null);
                    cs.assertSuccess(planNum);
                } else {
                    permissible.add(HelperParameters.class);
                }

                /*
                 * Permissible errors after a Plan repair or Task cleanup
                 */
                permissible.add(InsufficientAdmins.class);
                permissible.add(InsufficientRNs.class);
                permissible.add(InsufficientANs.class);
                permissible.add(UnderCapacity.class);
                permissible.add(NoPrimaryDC.class);
                permissible.add(NoPartition.class);
                permissible.add(MissingRootDirectorySize.class);
                DeployUtils.checkDeployment(cs,
                                            2,
                                            startingProblems,
                                            permissible,
                                            logger);
            }
        } catch (Exception e) {
            logger.info("Test exited loop with " +
                         LoggerUtils.getStackTrace(e));
            throw e;
        } finally {
            DeployNewRN.FAULT_HOOK = null;
            logger.info("Injected faults at " + taskHook.showInjections());
        }
    }

    /**
     * Deploy a topo with Primary RF=2. Deploy 2 RNs and
     * 1 arbiter. Invoke failures at key places while deploying
     * RNs and arbiters for change RF=3. Fix the problems by:
     * A. Running the Repair Plan
     * B. Letting the cleanup code run.
     */
    private void failAndRepair2(boolean killCleanup) throws Exception {

        final int NUM_PARTITIONS = 10;
        final String dcName = "DataCenterA";

        /* Bootstrap the Admin, the first DC, and 3 SNs */
        sysAdminInfo =
            AdminUtils.bootstrapStore(kvstoreName,
                                      6 /* SNs */,
                                      dcName,
                                      2 /* repFactor*/,
                                      true /* useThreads */,
                                      MB_PER_SN_STRING /* memory */,
                                      true);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName), MB_PER_SN_STRING, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "0", 2);
        AdminUtils.deployAdmin(cs, snSet, 1, AdminType.PRIMARY);

        TaskHook taskHook = null;
        taskHook = new TaskHook(killCleanup);

        int attempt = 0;
        String candidateName = "Candidate_" + attempt;
        cs.createTopology(candidateName,
                          poolName,
                          NUM_PARTITIONS,
                          false);
        int planNum = cs.createDeployTopologyPlan(candidateName,
                                                  candidateName, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        /*
         * Assert that the deployed topology has the number of shards, RNs
         * and ANs that are expected.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */,
                              3 /*numSNs*/, 1 /*numRGs*/, 2 /*numRNs*/,
                              1/*nARBs*/, 10 /*numParts*/);

        Set<Problem> startingProblems = new HashSet<Problem>();
        Set<Class<?>> permissible = new HashSet<Class<?>>();
        permissible.add(InsufficientAdmins.class);
        permissible.add(MissingRootDirectorySize.class);
        DeployUtils.checkDeployment(cs, 2 /* rf */,
                                    startingProblems,
                                    permissible,
                                    logger);

        /*
         * Add  SNs
         */
        snSet.deploy(cs, sysAdminInfo.getDCId(dcName), MB_PER_SN_STRING,
                     3, 4, 5);
        AdminUtils.deployAdmin(cs, snSet, 2, AdminType.PRIMARY);


        RemoveAN.FAULT_HOOK = taskHook;

        try {
            Plan.State result = null;
            while (result != Plan.State.SUCCEEDED) {
                attempt++;
                logger.info("Attempt " + attempt);
                candidateName = "Candidate_" + attempt;

                cs.copyCurrentTopology(candidateName);
                cs.changeRepFactor(candidateName, poolName,
                        sysAdminInfo.getDCId(dcName), 3);

                cs.validateTopology(candidateName);

                permissible.clear();
                permissible.add(UnderCapacity.class);
                planNum = cs.createDeployTopologyPlan(candidateName,
                         candidateName, null);
                cs.approvePlan(planNum);
                cs.executePlan(planNum, false);
                result = cs.awaitPlan(planNum, 0, null);

                try {
                    cs.assertSuccess(planNum);
                } catch (Exception expected) {
                    /* nothing to do */
                }

                if (!result.equals(Plan.State.SUCCEEDED)) {
                    cs.cancelPlan(planNum);
                } else {
                    /*
                     * InsufficientAdmins should exist after
                     * the plan has succeeded. Also, the
                     * helper host parameters will be out of
                     * sync until the updateHelperHost task
                     * is executed. If the failure occurs after
                     * the AN is removed from the topo, the
                     * subsequent execution will not generate
                     * updateHelperHost tasks because the AN
                     * is already removed from the topo. Therefore
                     * no updatehelperhost task is generated on the
                     * next execution.
                     */
                    permissible.add(InsufficientAdmins.class);
                    permissible.add(HelperParameters.class);
                    permissible.add(MissingRootDirectorySize.class);
                    DeployUtils.checkDeployment(cs,
                                                2,
                                                startingProblems,
                                                permissible,
                                                logger);
                }

                /*
                 * Repair this situation by running a repair plan.
                 */
                if (killCleanup) {
                    planNum = cs.createRepairPlan("RepairPlan");
                    cs.approvePlan(planNum);
                    cs.executePlan(planNum, false);
                    cs.awaitPlan(planNum, 0, null);
                    cs.assertSuccess(planNum);
                    permissible.remove(HelperParameters.class);
                } else {
                    permissible.add(HelperParameters.class);
                }

                /*
                 * Permissible errors after a Plan repair or Task cleanup
                 */
                permissible.add(UnderCapacity.class);
                permissible.add(ExcessANs.class);
                permissible.add(ServiceStopped.class);
                permissible.add(MissingRootDirectorySize.class);
                DeployUtils.checkDeployment(cs,
                                            2,
                                            startingProblems,
                                            permissible,
                                            logger);
            }
        } catch (Exception e) {
            logger.info("Test exited loop with " +
                        LoggerUtils.getStackTrace(e));
            throw e;
        } finally {
            RemoveAN.FAULT_HOOK = null;
            logger.info("Injected faults at " + taskHook.showInjections());
        }
    }

    /*
     * A TestHook which throws RunTime Exceptions by:
     * 1) Injecting a fake error when task cleanup is killed and
     *    cleanup code is called.
     * 2) Injecting a fake error at count 1 in DeployNewRN and
     *    DeployNewArb code.
     */
     private class TaskHook implements TestHook<String> {
        private final Set<String> targets;
        private final boolean killCleanup;

        TaskHook(boolean killCleanup) {
            this.killCleanup = killCleanup;
            targets = new HashSet<String>();
        }

        @Override
        public void doHook(String value) {

            if (killCleanup && value.contains("cleanup")) {
                throw new RuntimeException("Injecting fault at " + value);
            }

            if (!killCleanup && value.contains("cleanup")) {
                return;
            }

            if (targets.add(value)){
                throw new RuntimeException("Injecting fault at " + value);
            }
        }

        public String showInjections() {
            StringBuilder sb = new StringBuilder();
            for (String hook: targets) {
                sb.append(hook).append("\n");
            }
            return sb.toString();
        }
    }
}
