/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.PlanExecutor;
import oracle.kv.impl.admin.plan.StatusReport;
import oracle.kv.impl.admin.plan.task.RelocateRN;
import oracle.kv.impl.admin.topo.Validations;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

/**
 * Inject systematic failures, at every task boundary and within some tasks,
 * into running elasticity plans. The same plan is retried over and over until
 * the plan succeeds.
 *
 * Tests plan and task idempotency.
 */
public class DeployFailTest extends TestBase {

    private SysAdminInfo sysAdminInfo;
    private StoreUtils su;

    /*
     * For interactive debugging -- make the test run faster by disabling the
     * fault injection for some cases.
     */
    private final boolean ENABLE_FAULTS = true;

    @Override
    public void setUp() throws Exception {

        /* Allow write-no-sync durability for faster test runs. */
        TestStatus.setWriteNoSyncAllowed(true);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        RelocateRN.FAULT_HOOK = null;
        PlanExecutor.FAULT_HOOK = null;

        super.tearDown();
        if (su != null) {
            su.close();
        }

        if (sysAdminInfo != null) {
            sysAdminInfo.getSNSet().shutdown();
            sysAdminInfo = null;
        }
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Deploy a brand new store, then rebalance, then redistribute, while
     * injecting failures. The same plan is re-executed after each failure.
     * @throws Exception
     */
    @Test
    public void testRetryPlanAfterFailure() throws Exception {
        runRetryAfterFailure(false);
    }

    /**
     * Deploy a brand new store, then rebalance, then redistribute, while
     * injecting failures. A new plan is created and executed after each
     * failure.
     * @throws Exception
     */
    @Test(timeout=1200000)
    public void testNewPlanAfterFailure() throws Exception {
        runRetryAfterFailure(true);
    }

    private void runRetryAfterFailure(boolean newPlan) throws Exception {
        final int NUM_PARTITIONS = 30;

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
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

        /* Deploy two more SNs on DataCenterA */
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);

        /* make an initial topology */
        final String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, NUM_PARTITIONS,
                          false);

        Topology candidateTopo = cs.getTopologyCandidate(first).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1 /*dc*/, 3 /*nSNs*/,
                              2, 6, NUM_PARTITIONS);

        /*
         * Deploy initial topology. Make the following set of problems
         * permissible for the post-plan-error check, because they can
         * naturally occur when the plan ends due to an interrupt or failure.
         */
        Set<Class<?>> errorPermissible = new HashSet<Class<?>>();
        errorPermissible.add(Validations.InsufficientRNs.class);
        errorPermissible.add(Validations.UnderCapacity.class);
        errorPermissible.add(Validations.NonOptimalNumPartitions.class);
        errorPermissible.add(Validations.MultipleRNsInRoot.class);
        errorPermissible.add(Validations.NoPrimaryDC.class);
        errorPermissible.add(Validations.NoPartition.class);
        errorPermissible.add(VerifyConfiguration.ParamMismatch.class);

        /*
         * These errors are okay even when the plan runs to completion without
         * an injected failure.
         */
        Set<Class<?>> successPermissible = new HashSet<Class<?>>();
        successPermissible.add(Validations.UnderCapacity.class);
        successPermissible.add(Validations.MultipleRNsInRoot.class);
        successPermissible.add(Validations.InsufficientAdmins.class);
        successPermissible.add(MissingRootDirectorySize.class);

        /* The initial deployment must use the same plan */
        executeWithErrors("initialDeploy", cs, first, errorPermissible,
                          successPermissible, false, false);

        /*
         * Assert that the deployed topology has the number of shards and RNs
         * that we expect.
         */
        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 3 /*numSNs*/,
                              2 /*numRGs*/, 6 /*numRNs*/,
                              NUM_PARTITIONS);
        
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    5000,
                                    logger,
                                    1, InsufficientAdmins.class, 
                                    3, MultipleRNsInRoot.class,
                                    3, MissingRootDirectorySize.class);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Change the capacity so the store is out of compliance */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 1, 2);

        /*
         * Add more SNs,and try a new rebalance.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3, 4);
        String secondAttempt = "rebalanceAttempt";
        DeployUtils.printCurrentTopo(cs, secondAttempt, logger);
        cs.copyCurrentTopology(secondAttempt);
        cs.rebalanceTopology(secondAttempt, Parameters.DEFAULT_POOL_NAME, null);

        /*
         * Try a rebalance
         */
        errorPermissible.clear();
        errorPermissible.add(Validations.InsufficientRNs.class);
        errorPermissible.add(Validations.UnderCapacity.class);
        errorPermissible.add(Validations.NonOptimalNumPartitions.class);
        errorPermissible.add(Validations.MultipleRNsInRoot.class);
        errorPermissible.add(Validations.NoPrimaryDC.class);
        errorPermissible.add(Validations.NoPartition.class);
        errorPermissible.add(VerifyConfiguration.ParamMismatch.class);

        executeWithErrors(secondAttempt, cs, secondAttempt, errorPermissible,
                          successPermissible, false, newPlan);

        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 5 /*numSNs*/,
                              2 /*numRGs*/,  6 /*numRNs*/,
                              NUM_PARTITIONS);
        Set<Problem> empty = Collections.emptySet();
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, empty,
                                    successPermissible, logger);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /*
         * Check for data
         * display status
         */

        /*
         * Change the capacity so more shards could be created, though only
         * through both migration and RN relocation.Make a candidate.
         */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 3, 4);
        String redistrib = "redistribute";
        DeployUtils.printCurrentTopo(cs, redistrib, logger);
        cs.copyCurrentTopology(redistrib);
        cs.redistributeTopology(redistrib, Parameters.DEFAULT_POOL_NAME);

        errorPermissible.clear();
        errorPermissible.add(Validations.InsufficientRNs.class);
        errorPermissible.add(Validations.UnderCapacity.class);
        errorPermissible.add(Validations.NonOptimalNumPartitions.class);
        errorPermissible.add(Validations.MultipleRNsInRoot.class);
        errorPermissible.add(Validations.NoPrimaryDC.class);
        errorPermissible.add(Validations.NoPartition.class);
        errorPermissible.add(VerifyConfiguration.ParamMismatch.class);

        Set<Problem> preExisting =
            executeWithErrors(redistrib, cs, redistrib, errorPermissible,
                              successPermissible, false, newPlan);

        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 5 /*numSNs*/,
                              3 /*numRGs*/,  9 /*numRNs*/,
                              NUM_PARTITIONS);
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, preExisting,
                                   successPermissible, logger);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    /**
     * Deploy a brand new store, then rebalance, then redistribute, while
     * injecting failures in the tasks. The same plan is re-executed after each
     * failure.
     * @throws Exception
     */
    @Test
    public void testRetryAfterTaskFailure() throws Exception {
        runRetryAfterTaskFailure(false);
    }

    /**
     * Deploy a brand new store, then rebalance, then redistribute, while
     * injecting failures in the tasks. A new plan is created and executed after
     * each failure.
     * @throws Exception
     */
    @Test
    public void testNewPlanAfterTaskFailure() throws Exception {
        runRetryAfterTaskFailure(true);
    }

    private void runRetryAfterTaskFailure(boolean newPlan) throws Exception {
        final int NUM_PARTITIONS = 30;

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
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

        /* Deploy two more SNs on DataCenterA */
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);

        /* make an initial topology */
        final String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, NUM_PARTITIONS,
                          false);

        int planNum = cs.createDeployTopologyPlan(first, first, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /* Load some data. Ensure that each partition has at least 2 records */
        su = new StoreUtils(kvstoreName,
                            AdminUtils.HOSTNAME,
                            snSet.getRegistryPort(0),
                            StoreUtils.RecordType.INT,
                            1 /* seed */);
        int expectedNumRecords = 500;
        Collection<PartitionId> allPIds =
            cs.getTopology().getPartitionMap().getAllIds();
        List<Key> keys = su.load(expectedNumRecords, allPIds, 1);
        DeployUtils.checkContents(su, keys, expectedNumRecords);

        /* Change the capacity so the store is out of compliance */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 1, 2);

        /*
         * Add more SNs,and try a new rebalance.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3, 4);
        String secondAttempt = "rebalanceAttempt";
        DeployUtils.printCurrentTopo(cs, secondAttempt, logger);
        cs.copyCurrentTopology(secondAttempt);
        cs.rebalanceTopology(secondAttempt, Parameters.DEFAULT_POOL_NAME, null);

        /*
         * Try a rebalance
         */
        final Set<Class<?>> errorPermissible = new HashSet<Class<?>>();
        errorPermissible.add(Validations.InsufficientRNs.class);
        errorPermissible.add(Validations.UnderCapacity.class);
        errorPermissible.add(Validations.NonOptimalNumPartitions.class);
        errorPermissible.add(Validations.MultipleRNsInRoot.class);
        errorPermissible.add(VerifyConfiguration.ParamMismatch.class);
        errorPermissible.add(VerifyConfiguration.StatusNotRight.class);

        Set<Class<?>> successPermissible = new HashSet<Class<?>>();
        successPermissible.add(Validations.InsufficientAdmins.class);
        successPermissible.add(Validations.UnderCapacity.class);
        successPermissible.add(Validations.MultipleRNsInRoot.class);
        successPermissible.add(MissingRootDirectorySize.class);

        executeWithErrors(secondAttempt, cs, secondAttempt, errorPermissible,
                          successPermissible, true, newPlan);

        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 5 /*numSNs*/,
                              2 /*numRGs*/,  6 /*numRNs*/,
                              NUM_PARTITIONS);

        DeployUtils.checkDeployment(cs, 3 /* repFactor */,
                                    Collections.<Problem>emptySet(),
                                    successPermissible, logger);
        DeployUtils.checkContents(su, keys, expectedNumRecords);
    }

    /**
     * Test the particular case where a topology plan moves two RNs from the
     * same shard. In SR [#22722], a mid-task failure from moving the first
     * RN made resulted in a situation where the shard only has 2 RNs up.
     * Subsequent executions of the plan decide that the topology move has
     * succeeded because all metadata is consistent, but the follow on task
     * then disables a second RN from that shard, creating a Catch-22 situation
     * of shard unavailability.
     * @throws Exception
     */
    @Test
    public void testMultipleMovesInShard() throws Exception {

        final int NUM_PARTITIONS = 10;
        final String MOVE = "move";

        /* Bootstrap the Admin, the first DC, and 5 SNs */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 5 /* max SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3, 4);

        /* make an initial topology */
        final String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, NUM_PARTITIONS,
                          false);

        Topology candidateTopo = cs.getTopologyCandidate(first).getTopology();
        DeployUtils.checkTopo(candidateTopo, 1 /*dc*/, 5 /*nSNs*/,
                              1, 3, NUM_PARTITIONS);
        int planNum = cs.createDeployTopologyPlan(first, first, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /*
         * Make a topology that moves rg1-rn1 to sn4, and rg1-rn2 to sn5,
         * which will result in a plan with relocation of two RNs from the
         * same shard. These tasks will execute serially, so in theory, there
         * is quorum available at all times.
         */

        cs.copyCurrentTopology(MOVE);
        RepNodeId rn1 = new RepNodeId(1, 1);
        RepNodeId rn2 = new RepNodeId(1, 2);
        /* Move rg1-rn1 to sn4 */
        cs.moveRN(MOVE, rn1, new StorageNodeId(4));
        cs.moveRN(MOVE, rn2, new StorageNodeId(5));
        DeployUtils.printCandidate(cs, MOVE, logger);
        DeployUtils.printPreview(MOVE, false /* noChange*/, cs, logger);

        final Set<Class<?>> errorPermissible = new HashSet<Class<?>>();
        errorPermissible.add(Validations.InsufficientRNs.class);
        errorPermissible.add(Validations.UnderCapacity.class);
        errorPermissible.add(Validations.NonOptimalNumPartitions.class);
        errorPermissible.add(Validations.MultipleRNsInRoot.class);
        errorPermissible.add(VerifyConfiguration.ParamMismatch.class);
        errorPermissible.add(VerifyConfiguration.StatusNotRight.class);

        final Set<Class<?>> successPermissible = new HashSet<Class<?>>();
        successPermissible.add(Validations.UnderCapacity.class);
        successPermissible.add(Validations.MultipleRNsInRoot.class);

        final Set<Problem> preExisting =
            executeWithErrors(MOVE, cs, MOVE, errorPermissible,
                              successPermissible, true, false);

        DeployUtils.checkTopo(cs.getTopology(), 1 /* numDCs */, 5 /*numSNs*/,
                              1 /*numRGs*/,  3 /*numRNs*/,
                              NUM_PARTITIONS);
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, preExisting,
                                    successPermissible, logger);
    }

    /**
     * Repeat the execution of a plan, running a fault injector that
     * systematically inserts an error at every increasing distances. After
     * enough retries, the last plan execution runs with no injected failures,
     * and should succeed.
     */
    private Set<Problem> executeWithErrors(String planName,
                                           CommandServiceAPI cs,
                                           String candidateName,
                                           Set<Class<?>> errorPermissible,
                                           Set<Class<?>> successPermissible,
                                           boolean taskErrors,
                                           boolean newPlan)
        throws RemoteException {

        /* Keep track of problems that exist before the test. */
        VerifyResults results = cs.verifyConfiguration(false, true, false);
        Set<Problem> preExisting = new HashSet<Problem>();
        preExisting.addAll(results.getViolations());
        preExisting.addAll(results.getWarnings());

        /* Display some information on what to expect. */
        DeployUtils.printCandidate(cs, candidateName, logger);
        String previewInfo = cs.preview(candidateName, null, true);
        logger.log(Level.INFO, "preview of {0}: {1}",
                   new Object[] {candidateName, previewInfo});

        int planNum = 0;
        boolean success = false;
        int faultCount = 0;
        while (!success) {

            /*
             * In each iteration of the loop, inject an exception at a new spot
             */
            if (ENABLE_FAULTS) {
                if (taskErrors) {
                    RelocateRN.FAULT_HOOK = new DeployUtils.TaskHook(++faultCount);
                } else {
                    PlanExecutor.FAULT_HOOK =
                            new DeployUtils.FaultCounter(++faultCount);
                }
            }

            /* Create a new plan if needed (initially or if newPlan is set) */
            if ((planNum == 0) || newPlan) {
                planNum = cs.createDeployTopologyPlan(planName + faultCount,
                                                      candidateName, null);
                cs.approvePlan(planNum);
            }

            try {
                cs.executePlan(planNum, false);
                cs.awaitPlan(planNum, 0, null);
                try {
                    cs.assertSuccess(planNum);
                    success = true;
                } catch (Exception e) {
                    logger.info("Fault count=" + faultCount + ": " + e);
                }

                /* Validate the topology */
                DeployUtils.printCurrentTopo(cs, "Current: " + faultCount,
                                             logger);
                DeployUtils.checkDeployment
                    (cs, 3, preExisting,
                     (success ? successPermissible : errorPermissible), logger);

                /*
                 * Intentionally always generate the report, whether or
                 * not it's displayed, to exercise the generation.
                 */
                String statusReport = cs.getPlanStatus
                    (planNum,
                     (StatusReport.VERBOSE_BIT |
                      StatusReport.SHOW_FINISHED_BIT),
                     false /* json */);

                logger.fine(statusReport);
            } finally {
                /* Be sure to clear the fault injector! */
                PlanExecutor.FAULT_HOOK = null;
                RelocateRN.FAULT_HOOK = null;
            }
        }
        return preExisting;
    }
}
