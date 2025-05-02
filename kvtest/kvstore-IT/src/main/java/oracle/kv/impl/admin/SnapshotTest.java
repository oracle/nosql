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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.Snapshot.SnapshotOperation;
import oracle.kv.impl.admin.Snapshot.SnapResultSummary;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.plan.PlanExecutor;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.server.LoggerUtils;

import org.junit.Test;

public class SnapshotTest extends TestBase {

    private SysAdminInfo sysAdminInfo;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        PlanExecutor.FAULT_HOOK = null;
        super.tearDown();
        if (sysAdminInfo != null) {
            sysAdminInfo.getSNSet().shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    /**
     * During the change topology plan is executing, start a snapshot command.
     * Add test for SR [#21864].
     */
    @Test
    public void testConcurrentPlanThenSnapshot()
        throws Exception {

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
        /* Deploy three more SNs on DataCenterA */
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1);
        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.checkTopo(cs.getTopology(),
                              1 /* numDCs */,
                              4 /*numSNs*/,
                              2 /*numRGs*/,
                              6 /*numRNs*/,
                              100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    5000,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    2, MultipleRNsInRoot.class,
                                    4, MissingRootDirectorySize.class);
        /*
         * Change the capacity to provoke a rebalance. Try to execute a snapshot
         * command when the rebalance plan is running.
         */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0, 1);
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     4, 5);
        String rebalance = "rebalanceAttempt";
        DeployUtils.printCurrentTopo(cs, rebalance, logger);
        cs.copyCurrentTopology(rebalance);
        cs.rebalanceTopology(rebalance, Parameters.DEFAULT_POOL_NAME, null);
        planNum = cs.createDeployTopologyPlan(rebalance, rebalance, null);
        DeployUtils.printPreview(rebalance, false, cs, logger);
        cs.approvePlan(planNum);
        /* Run the plan, but pause it in the middle */
        CountDownLatch gotToStall = new CountDownLatch(1);
        CountDownLatch waitForRelease = new CountDownLatch(1);
        StallHook staller = new StallHook(3, gotToStall, waitForRelease);
        PlanExecutor.FAULT_HOOK = staller;
        cs.executePlan(planNum, false);
        gotToStall.await();
        /*
         * Try to execute a snapshot command during the rebalance plan.
         */
        PlanExecutor.FAULT_HOOK = null;
        String sname = "snapName";
        try {
            cs.executeSnapshotOperation(SnapshotOperation.CREATE, sname, null);
            fail("Should have refused to run snapshot command");
        } catch (AdminFaultException expected) {
            isIllegalCommandException(expected);
        }
        waitForRelease.countDown();
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.checkTopo(cs.getTopology(),
                              1 /* numDCs */,
                              6 /*numSNs*/,
                              2 /*numRGs*/,
                              6 /*numRNs*/,
                              100 /*numParts*/);
        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    5000,
                                    logger,
                                    1, InsufficientAdmins.class,
                                    6, MissingRootDirectorySize.class);
        DeployUtils.printCurrentTopo(cs, "after rebalance", logger);
    }

    @Test
    public void testConcurrentCreateSnapshot()
        throws Exception {
        final String sname1 = "concurrentSnapName1";
        final String sname2 = "concurrentSnapName2";

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 3    /* max of 3 SNs */,
                                                 "DataCenterA",
                                                 3    /* repFactor*/,
                                                 true /* useThreads*/,
                                                 MB_PER_SN_STRING);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_SG_ENABLED, "true");
        cs.setPolicies(mergeParameterMapDefaults(map));
        /* Deploy two more SNs on DataCenterA */
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs,
                     sysAdminInfo.getDCId("DataCenterA"),
                     MB_PER_SN_STRING,
                     1, 2);
        snSet.changeParams(cs,
                           ParameterState.COMMON_CAPACITY,
                           "1",
                           0, 1, 2);
        /* make an initial topology */
        String topoName = "concurrentCreateSnapshotTopo";
        String planName = "concurrentCreateSnapshotPlan";
        cs.createTopology(topoName, Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan(planName, topoName, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.checkTopo(cs.getTopology(),
                              1   /* numDCs */,
                              3   /* numSNs */,
                              1   /* numRGs */,
                              3   /* numRNs */,
                              100 /* numParts */);

        /* execute the 1st concurrent create snapshot command,
         * but pause it in the middle
         */
        CountDownLatch gotToStall = new CountDownLatch(1);
        CountDownLatch waitForRelease = new CountDownLatch(1);
        StallHook staller = new StallHook(1, gotToStall, waitForRelease);
        SnapshotOperationProxy.faultHook = staller;
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    SnapResultSummary snapResSumm =
                        cs.executeSnapshotOperation(SnapshotOperation.CREATE,
                                                    sname1,
                                                    null);
                    assertTrue("Concurrent Snapshot1 creation failed",
                               snapResSumm.getSucceeded());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        gotToStall.await(30, TimeUnit.SECONDS);

        /* execute the 2nd concurrent create snapshot command */
        SnapshotOperationProxy.faultHook = null;
        SnapResultSummary snapResSumm =
            cs.executeSnapshotOperation(SnapshotOperation.CREATE,
                                        sname2,
                                        null);
        assertTrue("Concurrent Snapshot2 creation failed",
                   snapResSumm.getSucceeded());

        waitForRelease.countDown();

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
    }

    private void isIllegalCommandException(AdminFaultException e) {

        String faultClassName = e.getFaultClassName();
        assertEquals(IllegalCommandException.class.getName(), faultClassName);
    }

    /**
     * When we get to the failure count point, release the countdown latch
     * so some other action (like admin shutdown) can be coordinated, and then
     * incur an exception.
     */
    class StallHook implements TestHook<Integer> {
        private final CountDownLatch gotToStall;
        private final CountDownLatch waitForRelease;
        private final AtomicInteger counter;

        public StallHook(int i,
                         CountDownLatch gotToStall,
                         CountDownLatch waitForRelease) {
            this.gotToStall = gotToStall;
            counter = new AtomicInteger(i);
            this.waitForRelease = waitForRelease;
        }

        @Override
        public void doHook(Integer unused) {
            if (counter.decrementAndGet() == 0) {
                logger.info("---------- UNSTALLING ----- ");
                gotToStall.countDown();
                if (waitForRelease != null) {
                    try {
                        waitForRelease.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        throw new RuntimeException (e);
                    }
                }
            }
        }
    }
}
