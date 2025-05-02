/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.VerifyConfiguration.ParamMismatch;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.RMIFailed;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.task.DeployNewRN;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.InsufficientRNs;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.admin.topo.Validations.NoPartition;
import oracle.kv.impl.admin.topo.Validations.NoPrimaryDC;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Test problems with RN relocation, which is done for rebalancing, and for
 * migrate-rn.
 */
public class DeployNewRNTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

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
     * Setup a 2 shard system, with multiple RNs on a single SN, and then
     * move a RN around, invoking failures at key places.
     */
    @Test
    public void testVerifyRepair() throws Exception {

        final int NUM_PARTITIONS = 10;

        /* Bootstrap the Admin, the first DC, and 5 SNs */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 5 /* max SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3, 4);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);

        /*
         * Determine what are okay validation complaints when verify is issued
         * before and after the repair.
         */
        Set<Problem> startingProblems = new HashSet<Problem>();
        Set<Class<?>> preRepairPermissible = new HashSet<Class<?>>();
        preRepairPermissible.add(InsufficientAdmins.class);
        preRepairPermissible.add(InsufficientRNs.class);
        preRepairPermissible.add(NoPrimaryDC.class);
        preRepairPermissible.add(NoPartition.class);
        preRepairPermissible.add(UnderCapacity.class);
        preRepairPermissible.add(RMIFailed.class);
        preRepairPermissible.add(ParamMismatch.class);
        preRepairPermissible.add(MultipleRNsInRoot.class);

        Set<Class<?>> postRepairPermissible = new HashSet<Class<?>>();
        postRepairPermissible.add(NoPartition.class);
        postRepairPermissible.add(UnderCapacity.class);
        postRepairPermissible.add(InsufficientAdmins.class);
        postRepairPermissible.add(InsufficientRNs.class);
        postRepairPermissible.add(MultipleRNsInRoot.class);

        /* Deploy an initial topology */
        FaultInjector injector = new FaultInjector();
        DeployNewRN.FAULT_HOOK = injector;
        try {
            int attempt = 0;
            Plan.State result = null;
            while (result != Plan.State.SUCCEEDED) {
                attempt++;
                logger.info("Attempt " + attempt);
                String candidateName = "Candidate_" + attempt;
                cs.createTopology(candidateName,
                                  Parameters.DEFAULT_POOL_NAME,
                                  NUM_PARTITIONS,
                                  false);

                int planNum = cs.createDeployTopologyPlan(candidateName,
                                                          candidateName, null);
                cs.approvePlan(planNum);
                cs.executePlan(planNum, false);
                result = cs.awaitPlan(planNum, 0, null);

                Set<Class<?>> usePreRepair = null;
                Set<Class<?>> usePostRepair = null;
                if (!result.equals(Plan.State.SUCCEEDED)) {
                    /* Okay to have some issues after the repair is done. */
                    cs.cancelPlan(planNum);
                    usePreRepair = preRepairPermissible;
                    usePostRepair = postRepairPermissible;
                } else {
                    /*
                     * No issues should exist after the plan is done, both
                     * before and after repair.
                     */
                    usePreRepair = new HashSet<Class<?>>();
                    usePreRepair.add(InsufficientAdmins.class);
                    usePreRepair.add(MultipleRNsInRoot.class);
                    usePreRepair.add(NoPartition.class);
                    usePreRepair.add(UnderCapacity.class);
                    usePostRepair = usePreRepair;
                }
                usePreRepair.add(MissingRootDirectorySize.class);
                DeployUtils.checkDeployment(cs, 3, startingProblems,
                                            usePreRepair, logger);
                /* Try a fix */
                runRepairPlan(cs);
                VerifyResults postRepair =
                    cs.verifyConfiguration(false, true, false);
                usePostRepair.add(MissingRootDirectorySize.class);
                logger.info("Post repair: " + postRepair.display());
                DeployUtils.checkDeployment(cs, 3, startingProblems,
                                            usePostRepair, logger);
            }
        } catch (Exception e) {
            logger.info("Test exited loop with " +
                         LoggerUtils.getStackTrace(e));
            throw e;
        } finally {
            DeployNewRN.FAULT_HOOK = null;
            logger.info("Injected faults at " + injector.showInjections());
        }
    }

    private void runRepairPlan(CommandServiceAPI cs) throws RemoteException {
        int planNum = cs.createRepairPlan("RepairPlan");
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
    }

    /*
     * A test hook that throws a RuntimeException if th
     * counted down to 0. The idea is in plan execution 1, the fake error is
     * injected at count 1, plan execution 2 incurs an error at count 2, etc.
     */
    class FaultInjector implements TestHook<String> {
        private final ConcurrentMap<String, String> tagsSeen;

        FaultInjector() {
            tagsSeen = new ConcurrentHashMap<String, String>();
        }

        @Override
        public void doHook(String tag) {

            if (tagsSeen.putIfAbsent(tag, tag) == null) {
                throw new RuntimeException("Injecting fault at " + tag);
            }
        }

        public String showInjections() {
            StringBuilder sb = new StringBuilder();
            for (String hook: tagsSeen.values()) {
                sb.append(hook).append("\n");
            }
            return sb.toString();
        }
    }
}


