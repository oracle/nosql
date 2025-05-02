/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.admin.param.StorageNodeParams.calcEndpointGroupNumThreads;
import static oracle.kv.util.CreateStore.MB_PER_SN;
import static oracle.kv.util.CreateStore.MB_PER_SN_STRING;
import static oracle.kv.util.CreateStore.mergeParameterMapDefaults;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminUtils.SNSet;
import oracle.kv.impl.admin.AdminUtils.SysAdminInfo;
import oracle.kv.impl.admin.VerifyConfiguration.AvailableStorageLow;
import oracle.kv.impl.admin.VerifyConfiguration.Problem;
import oracle.kv.impl.admin.VerifyConfiguration.RMIFailed;
import oracle.kv.impl.admin.VerifyConfiguration.ServiceStopped;
import oracle.kv.impl.admin.VerifyConfiguration.StorageDirectoryProblem;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.ExecutionState.ExceptionTransfer;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.Plan.State;
import oracle.kv.impl.admin.plan.PlanExecutor;
import oracle.kv.impl.admin.plan.task.AddPartitions;
import oracle.kv.impl.admin.topo.Validations;
import oracle.kv.impl.admin.topo.Validations.HelperParameters;
import oracle.kv.impl.admin.topo.Validations.InsufficientAdmins;
import oracle.kv.impl.admin.topo.Validations.MissingRootDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MissingStorageDirectorySize;
import oracle.kv.impl.admin.topo.Validations.MultipleRNsInRoot;
import oracle.kv.impl.admin.topo.Validations.OverCapacity;
import oracle.kv.impl.admin.topo.Validations.UnderCapacity;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.PingCollector;

import com.sleepycat.je.utilint.JVMSystemUtils;

import org.junit.Test;

/**
 * Test edge cases
 */
public class DeployProblemsTest extends TestBase {

    private static final LoginManager NULL_LOGIN_MGR = null;

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

        PlanExecutor.FAULT_HOOK = null;
        AddPartitions.beforeSaveTopoHook = null;
        Admin.beginShutdownHook = null;
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
     * Interrupt a plan while it is running on one admin, and then the plan can
     * retry it on another admin automatically.
     */
    @Test
    public void testAdminFailover()
        throws Exception {

        CommandServiceAPI[] cs = new CommandServiceAPI[9];
        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        /* Deploy two more SNs on DataCenterA */
        cs[0] = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs[0], sysAdminInfo.getDCId("DataCenterA"),
                     MB_PER_SN_STRING, 1, 2);

        /* deploy an Admin on each SN */
        for (int i = 1; i <3; i++) {
            cs[i] = AdminUtils.deployAdmin(cs[0], snSet, i);
        }

        CommandServiceAPI master = cs[0];
        master.verifyConfiguration(false, true, false);
        /* make an initial topology */
        String first = "firstCandidate";
        master.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = master.createDeployTopologyPlan("InitialDeploy", first,
                                                      null);
        master.approvePlan(planNum);

        CountDownLatch gotToStall = new CountDownLatch(1);
        StallHook staller = new StallHook(3, gotToStall, true);
        PlanExecutor.FAULT_HOOK = staller;

        master.executePlan(planNum, false);
        gotToStall.await();
        master.stop(true);
        Thread.sleep(10000);

        /*
         * This isn't working, how to restart? If it can be restarted, then
         * this test could be made more rigorous, by starting and stopping
         * Admins in a loop.
         * cs[0] = AdminUtils.restartAdmin(snSet, 0);
         */
        master = snSet.getAdminMaster();
        master.verifyConfiguration(false, true, false);

        /*
         * The plan does not restart on the new admin master automatically when
         * plan state is ERROR
         */
        if (master.awaitPlan(planNum, 0, null) == State.ERROR) {
            master.executePlan(planNum, false);
        }

        /* Wait plan executed successfully */
        master.awaitPlan(planNum, 0, null);
        master.assertSuccess(planNum);
        DeployUtils.checkTopo(master.getTopology(),
                              1 /* numDCs */,
                              3 /*numSNs*/,
                              1 /*numRGs*/,
                              3 /*numRNs*/,
                              100 /*numParts*/);

        /* Currently one problem because shutdown admin won't restart */
        DeployUtils.checkDeployment(master, 3 /* repFactor */, 5000, logger,
                                    1, VerifyConfiguration.RMIFailed.class,
                                    3, MissingRootDirectorySize.class);
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
        private final boolean throwError;

        public StallHook(int i, CountDownLatch gotToStall, boolean throwError) {
            this.gotToStall = gotToStall;
            counter = new AtomicInteger(i);
            this.throwError = throwError;
            waitForRelease = null;
        }

        public StallHook(int i, CountDownLatch gotToStall,
                         CountDownLatch waitForRelease) {
            this.gotToStall = gotToStall;
            counter = new AtomicInteger(i);
            this.throwError = false;
            this.waitForRelease = waitForRelease;
        }

        @Override
        public void doHook(Integer unused) {
            if (counter.decrementAndGet() == 0) {
                logger.info("---------- UNSTALLING ----- ");
                gotToStall.countDown();
                if (throwError) {
                    throw new RuntimeException("Fake error");
                }
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

    @Test
    public void testViolationsBeforeAndAfterTopologyDeployment()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        /* Deploy two more SNs on DataCenterA */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2",0, 1);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
        /*
         * Look for violations before topology deployment
         */
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, 5000, logger,
                4, UnderCapacity.class, 1, InsufficientAdmins.class,
                4, MissingRootDirectorySize.class);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, true);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        /*
         * Look for violations after topology deployment
         */
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, 5000, logger,
                2, MultipleRNsInRoot.class, 1, InsufficientAdmins.class,
                4, MissingRootDirectorySize.class);
    }

    @Test
    public void testMissingStorageDirAtTopologyDeployment()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 1 /* max of 1 SNs */,
                                                 "DataCenterA",
                                                 1 /* repFactor*/);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0);
        /* Set the storageDir for that SN*/
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        String storagedir = testdir + "storageDirA_sn1";
        snSet.changeMountPoint(cs, true, storagedir, "50_MB", 0);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 30, false);

        /* Rename the storagedir so that it is not available
         * topology-deploy time
         */
        String storagedirtmp = testdir + "storageDirA_sn1_tmp";
        File srcf = new File(storagedir);
        File destf = new File(storagedirtmp);
        srcf.renameTo(destf);

        /*
         * Reduce the retry duration from the default value of 10 minutes to 20
         * seconds: the retries will all fail, so no point waiting for long
         */
        ParameterMap policies = cs.getPolicyParameters();
        policies.setParameter(ParameterState.AP_NEW_RN_RETRY_TIME,
        		"20 SECONDS");
        cs.setPolicies(mergeParameterMapDefaults(policies));

        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, true);
        Plan.State state = cs.awaitPlan(planNum, 0, null);
        /* Plan must fail due to missing storagedir */
       	assertEquals(Plan.State.ERROR, state);
        ExceptionTransfer latest = cs.getPlanById(planNum)
        		.getLatestRunExceptionTransfer();
        String msg = latest.getDescription();
        assertTrue(msg, msg.contains("does not exist"));
        /* Put back the valid storagedir */
        destf.renameTo(srcf);

        /* execute the plan again. It must pass */
        cs.executePlan(planNum, true);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /*
         * Since storage space is 50 MB, so we are expected to get Low Disk
         * Space warnings. We will explicitly remove them for test purpose.
        */
        VerifyResults results = cs.verifyConfiguration(false, true, false);
        final Iterator<Problem> warnItr = results.getWarnings().iterator();
        while (warnItr.hasNext()) {
            if (warnItr.next() instanceof AvailableStorageLow) {
                warnItr.remove();
            }
        }

        assertTrue(results.display(), results.okay());
    }

    /**
     * Test that shutting down an SN during a store deployment does not result
     * in a long deadlock as described in [#25137].  The deadlock is still
     * present, but the time was shorted to 10 seconds when the Admin.shutdown
     * was modified to wait only 10 seconds.
     */
    @Test
    public void testShutdownDuringDeployment()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 1 /* max of 1 SNs */,
                                                 "DataCenterA",
                                                 1 /* repFactor*/);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     0);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 1, false);

        int planNum =
            cs.createDeployTopologyPlan("InitialDeploy", first, null);

        /*
         * Add a test hook in the AddPartitions task to wait to save the
         * topology until the shutdown has started.
         */
        final Semaphore taskEnterLatch = new Semaphore(0);
        final Semaphore taskContinueLatch = new Semaphore(0);
        AddPartitions.beforeSaveTopoHook = x -> {
            taskEnterLatch.release();
            try {
                taskContinueLatch.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException("Unexpected interrupt", e);
            }
        };

        cs.approvePlan(planNum);
        cs.executePlan(planNum, true);

        /*
         * Wait for the AddPartitions task to be ready to save the topology,
         * then add a test hook to Admin.shutdown to wait until the method has
         * been called.
         */
        taskEnterLatch.acquire();
        final Semaphore adminShutdownEnterLatch = new Semaphore(0);
        final Semaphore adminShutdownContinueLatch = new Semaphore(0);
        Admin.beginShutdownHook = x -> {
            adminShutdownEnterLatch.release();
            try {
                adminShutdownContinueLatch.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException("Unexpected interrupt", e);
            }
        };

        /* Start the shutdown in a separate thread */
        final Semaphore shutdownDoneLatch = new Semaphore(0);
        new Thread(() -> {
                try {
                    snSet.getSNA(0).shutdown(true, false);
                    shutdownDoneLatch.release();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
        }).start();

        /*
         * Wait for Admin.shutdown to be called, then let the AddPartitions
         * continue, which should cause it to block getting the Admin lock.
         */
        adminShutdownEnterLatch.acquire();
        taskContinueLatch.release();

        /*
         * Wait a little bit to block on the Admin lock, then let the shutdown
         * continue.
         */
        Thread.sleep(100);
        adminShutdownContinueLatch.release();

        assertTrue("Shutdown should only wait 10 seconds",
                   shutdownDoneLatch.tryAcquire(20, TimeUnit.SECONDS));
    }

    @Test
    public void testMigrateSNStoragedir()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 4 /* max of 4 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        CommandServiceAPI[] csArray = new CommandServiceAPI[9];
        csArray[0] = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        CommandServiceAPI cs = csArray[0];

        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        /* Set the storageDir for sn2 */
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        snSet.changeMountPoint(
            cs, true, testdir + "storageDir1_A", "1000000000", 0);
        snSet.changeMountPoint(
            cs, true, testdir + "storageDir1_B", "1000000000", 0);
        snSet.changeMountPoint(
            cs, true, testdir + "storageDir1_C", "1000000000", 0);
        snSet.changeMountPoint(
            cs, true, testdir + "storageDir2_A", "1000000000", 1);
        snSet.changeMountPoint(
            cs, true, testdir + "storageDir2_B", "1000000000", 1);
        snSet.changeMountPoint(
            cs, true, testdir + "storageDir2_C", "1000000000", 1);
        snSet.changeMountPoint(
            cs, true, testdir + "storageDir3_A", "1000000000", 2);
        snSet.changeMountPoint(
            cs, true, testdir + "storageDir3_B", "1000000000", 2);
        snSet.changeMountPoint(
            cs, true, testdir + "storageDir3_C", "1000000000", 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1, 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 30, false);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3);
        final String sd4A = testdir + "storageDir4_A";
        final String sd4B = testdir + "storageDir4_B";
        final String sd4C = testdir + "storageDir4_C";

        snSet.changeMountPoint(
            cs, true, sd4A, "900000000", 3);
        snSet.changeMountPoint(
            cs, true, sd4B, "1000000000", 3);
        snSet.changeMountPoint(
            cs, true, sd4C, "1000000000", 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 3);

        StorageNodeId sn2 = snSet.getId(1);
        StorageNodeId sn4 = snSet.getId(3);

        snSet.shutdown(1, logger);

        /* sn2 is shut down now, migrate it to sn4. */
        int migratePlanNum = cs.createMigrateSNPlan("Migrate sn2 to sn4",
                                                    sn2,
                                                    sn4);

        cs.approvePlan(migratePlanNum);
        cs.executePlan(migratePlanNum, false);

        Plan.State migratePlanState = cs.awaitPlan(migratePlanNum, 0, null);
        /* Plan must fail due to */
        assertEquals(Plan.State.ERROR, migratePlanState);
        ExceptionTransfer latest = cs.getPlanById(migratePlanNum)
            .getLatestRunExceptionTransfer();
        String msg = latest.getDescription();

        assertTrue(msg, msg.contains("Cannot find suitable storage dir"));

        /* Cancel the failed plan */
        cs.cancelPlan(migratePlanNum);

        snSet.changeMountPoint(
            cs, true, sd4A, "1000000000", 3, false);

        migratePlanNum = cs.createMigrateSNPlan("Migrate sn2 to sn4",
                                                sn2,
                                                sn4);

        cs.approvePlan(migratePlanNum);
        cs.executePlan(migratePlanNum, false);
        migratePlanState = cs.awaitPlan(migratePlanNum, 0, null);
        assertEquals(Plan.State.SUCCEEDED, migratePlanState);

        Parameters params = cs.getParameters();

        for (RepNodeParams rnp : params.getRepNodeParams()) {
            if (rnp.getStorageNodeId().equals(sn4)) {
                final String path = rnp.getStorageDirectoryPath();
                assertTrue(path.equals(sd4A) ||
                           path.equals(sd4B) ||
                           path.equals(sd4C));
                assertEquals(rnp.getStorageDirectorySize(), 1000000000);
            }
        }
    }

    @Test
    public void testMissingStoragedirMigrateSN()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 4 /* max of 4 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        CommandServiceAPI[] csArray = new CommandServiceAPI[9];
        csArray[0] = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        CommandServiceAPI cs = csArray[0];

        /*
         * Deploy two more SNs on DataCenterA and set the mount points:
         *
         * On sn1 - no mount points, so 3 RNs in root
         * On sn2 - 3 mount points, for each RN
         * On sn3 - no mount point, so 3 RNs in root
         */

        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        /* Set the storageDir for sn2 */
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_B", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_C", null, 1);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1, 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 30, false);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /*
         * Add a fourth SN that has a storage directory without storagedirs
         * migrate-sn to this (sn4) will inherit from the source sn (sn2),
         * sn4 will use its own mount points.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     3);
        snSet.changeMountPoint(cs, true, testdir + "storageDir4_A", null, 3);
        snSet.changeMountPoint(cs, true, testdir + "storageDir4_B", null, 3);
        snSet.changeMountPoint(cs, true, testdir + "storageDir4_C", null, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 3);

        /*
         * The only problems that we expect are
         *  1 - InsufficientAdmins - zn1 needs 2 more Admins for repFactor=3
         *  2 - MultipleRNsInRoot - storagedirs not specified for sn1 & sn3
         *  1 - UnderCapacity - sn4 has no RNs yet, so under capacity
         */
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, 5000, logger,
                                    1, InsufficientAdmins.class, /* violations */
                                    2, MultipleRNsInRoot.class, /* violations*/
                                    1, UnderCapacity.class, /* violations */
                                    2, MissingStorageDirectorySize.class,
                                    2, MissingRootDirectorySize.class);

        /*
         * Migrate sn2->sn4
         * shutdown sn2, the source sn, as required by the migrate-sn plan
         */
        StorageNodeId sn2 = snSet.getId(1);
        StorageNodeId sn4 = snSet.getId(3);

        snSet.shutdown(1, logger);

        /* Rename the storagedir4_B, mountpoint for sn4
         * so that it is not available at migrate-sn plan
         */
        String storagedirtmp = testdir + "storageDir4_B_tmp";
        String srcStoragedir = testdir + "storageDir4_B";
        File srcf = new File(srcStoragedir);
        File destf = new File(storagedirtmp);
        srcf.renameTo(destf);

        /* sn2 is shut down now, migrate it to sn4. */
        int migratePlanNum = cs.createMigrateSNPlan("Migrate sn2 to sn4",
                                                    sn2,
                                                    sn4);

        cs.approvePlan(migratePlanNum);
        cs.executePlan(migratePlanNum, false);

        Plan.State migratePlanState = cs.awaitPlan(migratePlanNum, 0, null);
        /* Plan must fail due to missing storagedir */
        assertEquals(Plan.State.ERROR, migratePlanState);
        ExceptionTransfer latest = cs.getPlanById(migratePlanNum)
            .getLatestRunExceptionTransfer();
        String msg = latest.getDescription();
        assertTrue(msg, msg.contains("does not exist"));


        /*
         * Find the number of RNs on SN4 that didn't get to start before
         * encountering the fatal "directory not present" error above.
         * It should be 1 RN with the missing directory + either 1 or 2 RNs
         * of the remaining RNs depending on timing and order in which the RNs
         * are started
         * */

        Topology topoTmp = cs.getTopology();
        Set<RepNodeId> unreachableRNIdsTmp = getUnreachableRepNodeIds(topoTmp);
        int violationsRMIFailed = unreachableRNIdsTmp.size()+1 /* ping() sn2 */;
        int violationServiceStopped = unreachableRNIdsTmp.size()-1;

        /*
         * The only problems that we expect are
         *  1 - InsufficientAdmins: zn1 needs 2 more Admins for repFactor=3
         *  2 - MultipleRNsInRoot: storagedirs not specified for sn1 & sn3
         *  1 - UnderCapacity: sn2.RNs were moved, sn2 is now so under capacity
         *  violationsRMIFailed+1: RMIFailed - sn2 shutdown, # of sn4.RNs down
         *  violationServiceStopped-1: Service Stopped for sn4.RNs that
         *                      doesn't have the bad storageDir
         */
        DeployUtils.checkProblemsWithWait(cs, 5000, logger,
                                          1, InsufficientAdmins.class,
                                          2, MultipleRNsInRoot.class,
                                          1, UnderCapacity.class,
                                          6, HelperParameters.class,
                                          2, MissingStorageDirectorySize.class,
                                          2, MissingRootDirectorySize.class,
                                          violationsRMIFailed, RMIFailed.class,
                                          violationServiceStopped, ServiceStopped.class,
                                          1, StorageDirectoryProblem.class
                                          );

        /* Cancel the failed migratePlanNum */
        cs.cancelPlan(migratePlanNum);

        /* Put back the valid storagedir */
        destf.renameTo(srcf);

        int repairPlanNum = cs.createRepairPlan("RepairPlan");
        cs.approvePlan(repairPlanNum);
        cs.executePlan(repairPlanNum, false);
        cs.awaitPlan(repairPlanNum, 0, null);
        cs.assertSuccess(repairPlanNum);

        /*
         * The only problems that we expect are
         * 1 - InsufficientAdmins - zn1 needs 2 more Admins for repFactor=3
         * 2 - MultipleRNsInRoot - storagedirs not specified for sn1 & sn3
         * 1 - UnderCapacity - sn2 is down, RNs unreachable, so under capacity
         * 2  - RMIFailed (Ping() sn2 fail and 1 out of 3 RNs on sn4
         * the other RNs gets started on repair-plan
         *
         */
        DeployUtils.checkProblemsWithWait(cs, 5000, logger,
                                          1, InsufficientAdmins.class,
                                          2, MultipleRNsInRoot.class,
                                          1, UnderCapacity.class,
                                          2, RMIFailed.class,
                                          2, MissingStorageDirectorySize.class,
                                          2, MissingRootDirectorySize.class
                                          );

        /* Find that one RN that didn't start on repair-plan and start it */
        Topology topo = cs.getTopology();
        Set<RepNodeId> unreachableRNIds = getUnreachableRepNodeIds(topo);
        /* There must only be one unreachable RN */
        assertEquals(1, unreachableRNIds.size());

        int startRnPlanNum = cs.createStartServicesPlan(
                                                        "Start Unreachable Nodes",
                                                        unreachableRNIds);

         cs.approvePlan(startRnPlanNum );
         cs.executePlan(startRnPlanNum, true);
         cs.awaitPlan(startRnPlanNum, 0, null);
         cs.assertSuccess(startRnPlanNum);

         /*
          * The only problems that we expect are
          * 1 - InsufficientAdmins - zn1 needs 2 more Admins for repFactor=3
          * 2 - MultipleRNsInRoot - storagedirs not specified for sn1 & sn3
          * 1 - UnderCapacity - sn2 is down, RNs unreachable, so under capacity
          * 1 - RMIFailed (Ping() sn2 fail,
          * All RNs on sn4 are RUNNING after StartServicesPlan
          * and repair-plan
          */
         DeployUtils.checkProblemsWithWait(cs, 5000, logger,
                                           1, InsufficientAdmins.class,
                                           2, MultipleRNsInRoot.class,
                                           1, UnderCapacity.class,
                                           1, RMIFailed.class,
                                           2, MissingStorageDirectorySize.class,
                                           2, MissingRootDirectorySize.class
                                           );

         /* Migration is successful. Remove sn2 */
         int removePlanNum = cs.createRemoveSNPlan("remove " + sn2, sn2);
         cs.approvePlan(removePlanNum);
        /* execute the plan again. It must pass */
         cs.executePlan(removePlanNum, true);
         cs.awaitPlan(removePlanNum, 0, null);
         cs.assertSuccess(removePlanNum);

         /*
          * The only problems that we expect are
          * 1 - InsufficientAdmins - zn1 needs 2 more Admins for repFactor=3
          * 2 - MultipleRNsInRoot - storagedirs not specified for sn1 & sn3
         */
         DeployUtils.checkProblemsWithWait(cs,5000 , logger,
                                           1, InsufficientAdmins.class,
                                           2, MultipleRNsInRoot.class,
                                           1, MissingStorageDirectorySize.class,
                                           2, MissingRootDirectorySize.class
                                           );

    }

    @Test
    public void testMissingStorageDirSNShutdownAndRestart()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 3 /* max of 3 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        CommandServiceAPI[] csArray = new CommandServiceAPI[9];
        csArray[0] = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        CommandServiceAPI cs = csArray[0];

        /*
         * Deploy 1 more SN on DataCenterA and set mount points:
         *
         * On sn1 - no mount points, so 3 RNs in root
         * On sn2 - 3 mount points, for each RN
         * On sn3 - no mount point, so 3 RNs in root
         */

        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        /* Set the storageDir for sn2 */
        String testdir = System.getProperty(TestUtils.DEST_DIR) +
            File.separator;
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_A", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_B", null, 1);
        snSet.changeMountPoint(cs, true, testdir + "storageDir2_C", null, 1);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "3", 0, 1, 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 30, false);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /*
         * The only problems that we expect are
         *  1 - InsufficientAdmins
         *  2 - MultipleRNsInRoot
         */
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, 5000, logger,
                                    1, InsufficientAdmins.class, /* violations */
                                    2, MultipleRNsInRoot.class, /* violations*/
                                    1, MissingStorageDirectorySize.class,
                                    2, MissingRootDirectorySize.class
                                    );

        /*
         * shutdown sn2
         */
        Topology topo = cs.getTopology();
        snSet.shutdown(1 /*sn2*/, logger);

        /* Rename the storagedir so that it is not available for sn2 restart */
        String storagedirtmp = testdir + "storageDirB_sn1_tmp";
        File srcf = new File(testdir + "storageDir2_B");
        File destf = new File(storagedirtmp);
        srcf.renameTo(destf);

        // restart sn2 that was shutdown.
        try {
            restartSN(snSet, 1 /*whichSN=sn2*/, topo);
            fail ("one of the RNs will be UNREACHABLE");
        } catch (Exception expected) {

        }

        /*
         * The only problems that we expect are
         *  1 - InsufficientAdmins
         *  2 - MultipleRNsInRoot
         *  1 - MountPointProblem
         *  1 - RMIFailed.class (1 RN with the missing storagedir is down)
         */
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, 5000, logger,
                                    1, InsufficientAdmins.class,/* violations */
                                    2, MultipleRNsInRoot.class, /* violations*/
                                    1, StorageDirectoryProblem.class,
                                    1, RMIFailed.class,
                                    1, MissingStorageDirectorySize.class,
                                    2, MissingRootDirectorySize.class
                                    );

        /* Put back the valid storagedir */
        destf.renameTo(srcf);

        Set<RepNodeId> unreachableRNIds = getUnreachableRepNodeIds(topo);
        /* There must only be one unreachable RN */
        assertEquals(1, unreachableRNIds.size());

        int startRnPlanNum = cs.createStartServicesPlan
            ("Start Unreachable Nodes",
             unreachableRNIds);
        cs.approvePlan(startRnPlanNum );
        cs.executePlan(startRnPlanNum, true);
        cs.awaitPlan(startRnPlanNum, 0, null);
        cs.assertSuccess(startRnPlanNum);

        /*
         * All services are up as before the missing storage dir error
         * The only problems that we expect are
         *  1 - InsufficientAdmins
         *  2 - MultipleRNsInRoot
         */
        DeployUtils.checkDeployment(cs, 3 /* repFactor */, 5000, logger,
                                    1, InsufficientAdmins.class, /* violations */
                                    2, MultipleRNsInRoot.class, /* violations*/
                                    1, MissingStorageDirectorySize.class,
                                    2, MissingRootDirectorySize.class
                                    );

    }

    /**
     * Check that concurrent plans properly coordinate, checking which
     * components will be modified by the plan.
     */
    @Test
    public void testConcurrentPlans()
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

        /* Deploy two more SNs on DataCenterA */
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2",0, 1);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        int otherPlan = cs.createDeployTopologyPlan("AnotherDeploy", first,
                                                    null);
        cs.approvePlan(planNum);

        /* Run the plan, but pause it in the middle */
        CountDownLatch gotToStall = new CountDownLatch(1);
        CountDownLatch waitForRelease = new CountDownLatch(1);
        StallHook staller = new StallHook(3, gotToStall, waitForRelease);
        PlanExecutor.FAULT_HOOK = staller;
        cs.executePlan(planNum, false);
        gotToStall.await();

        /*
         * Try starting a second topology plan while the first on is running,
         * it should fail.
         */
        cs.approvePlan(otherPlan);
        try {
            cs.executePlan(otherPlan, false);
            fail("Should have refused to run another topology plan");
        } catch (AdminFaultException expected) {
            isIllegalCommandException(expected);
        }

        waitForRelease.countDown();
        PlanExecutor.FAULT_HOOK = null;
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
                                    4, MissingRootDirectorySize.class,
                                    1, InsufficientAdmins.class,
                                    2, MultipleRNsInRoot.class);

        /*
         * Change the capacity to provoke a rebalance. Try to run a conflicting
         * migrateSN during the rebalance.
         */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0, 1);
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     4, 5);
        /* Make one more SN, for a migration. */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     6);

        String rebalance = "rebalanceAttempt";
        DeployUtils.printCurrentTopo(cs, rebalance, logger);
        cs.copyCurrentTopology(rebalance);
        cs.rebalanceTopology(rebalance, Parameters.DEFAULT_POOL_NAME, null);
        planNum = cs.createDeployTopologyPlan(rebalance, rebalance, null);
        DeployUtils.printPreview(rebalance, false, cs, logger);

        StorageNodeId snToMove = new StorageNodeId(4);
        int migratePlanNum = cs.createMigrateSNPlan("migrate",
                                                    snToMove,
                                                    new StorageNodeId(7));

        /* Run the plan, but pause it in the middle */
        gotToStall = new CountDownLatch(1);
        waitForRelease = new CountDownLatch(1);
        staller = new StallHook(3, gotToStall, waitForRelease);
        PlanExecutor.FAULT_HOOK = staller;
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        gotToStall.await();

        /*
         * Try running a migrateSN on a SN that is not involved while the
         * rebalance is active. TODO: contemplate outlawing a migrateSN on
         * a shard that is active in a relocation, because then there is no
         * quorum for the address update.
         */
        PlanExecutor.FAULT_HOOK = null;
        cs.approvePlan(migratePlanNum);

        /*
         * Tried to actually get the migrateSN to run, but there wasn't
         * enough quorum to let the two plans proceed. Add a third shard?
         */
        //snSet.getSNA(3).shutdown(true, true);
        try {
            cs.executePlan(migratePlanNum, false);
            fail("Should have refused to run migrate SN plan due to SN lock " +
                 "conflict.");
        } catch (AdminFaultException expected) {
            isIllegalCommandException(expected);
        }

        /* let the rebalance continue */
        waitForRelease.countDown();
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);

        /* shutdown SN used in migration */
        snSet.shutdown(snToMove.getStorageNodeId() - 1, logger);

        /* re-execute the migration */
        try {
            cs.executePlan(migratePlanNum, false);
            fail("Expected migration to fail because topo is different");
        } catch (AdminFaultException expected) {
            assertTrue(expected.getMessage().contains("topology version"));
        }

        cs.cancelPlan(migratePlanNum);

        /* Recreate and execute migrate SN plan based on new topology. */
        migratePlanNum = cs.createMigrateSNPlan("migrate",
                        snToMove,
                        new StorageNodeId(7));
        cs.approvePlan(migratePlanNum);
        cs.executePlan(migratePlanNum, false);
        cs.awaitPlan(migratePlanNum, 0, null);

        DeployUtils.checkTopo(cs.getTopology(),
                              1 /* numDCs */,
                              7 /*numSNs*/,
                              2 /*numRGs*/,
                              6 /*numRNs*/,
                              100 /*numParts*/);

        DeployUtils.checkDeployment(cs, 3 /* repFactor */, 5000, logger,
                                    1, InsufficientAdmins.class,
                                    1, UnderCapacity.class,
                                    1, VerifyConfiguration.RMIFailed.class,
                                    7, MissingRootDirectorySize.class);
        /* DeployUtils.printCurrentTopo(cs, "after rebalance", logger); */
    }

    /**
     * Test the parsing of a heap value from a JVM flags string.
     */
    @Test
    public void testHeapParsing() {
        assertEquals(333, RepNodeParams.parseJVMArgsForHeap
                     ("-Xmx", "-Xmx28 -XX:stuff -Xmx333"));

        assertEquals(28, RepNodeParams.parseJVMArgsForHeap
                     ("-Xmx", "-Xmx28 -XX:stuff"));

        assertEquals((28L * 1024 * 1024 * 1024),
                     RepNodeParams.parseJVMArgsForHeap
                     ("-Xmx", "-Dtest=bar -Xmx28g -XX:stuff"));
        assertEquals((28L * 1024 * 1024 * 1024),
                     RepNodeParams.parseJVMArgsForHeap
                     ("-Xmx", "-Dtest=bar -Xmx28G -XX:stuff"));

        assertEquals(3*1024, RepNodeParams.parseJVMArgsForHeap
                     ("-Xmx", "-Xmx2k -XX:stuff -Xmx3k"));
        assertEquals(3*1024, RepNodeParams.parseJVMArgsForHeap
                     ("-Xmx", "-Xmx2K -XX:stuff -Xmx3K"));

        assertEquals(2*1024, RepNodeParams.parseJVMArgsForHeap
                     ("-Xms", "-Xmx2K -Xms2K -XX:stuff -Xmx3K"));

        assertEquals(3*1024*1024L,
                     RepNodeParams.parseJVMArgsForHeap
                     ("-Xmx", "-Xmx2k -XX:stuff -Xmx3m -Xbax"));
        assertEquals(3*1024*1024L,
                     RepNodeParams.parseJVMArgsForHeap
                     ("-Xmx", "-Xmx2k -XX:stuff -Xmx3M -Xbax"));
    }

    /**
     * Test setting of RN heap and JE cache size
     */
    @Test
    public void testHeapCacheSize()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();

        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);
        snSet.changeParams(
            cs, ParameterState.COMMON_MEMORY_MB, "10000", 0, 1, 2);

        String first = "first";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);

        /* Default cache/heap percent are JVM vendor-specific. */
        final int heapPct =
            Integer.parseInt(ParameterState.SN_RN_HEAP_PERCENT_DEFAULT);
        final int cachePct =
            Integer.parseInt(ParameterState.RN_CACHE_PERCENT_DEFAULT);

        /*
         * Bump memoryMB for non-RN memory. Assume SN is hosting admin for now,
         * because this is assumed by calculateRNHeapAndCache.
         */
        final int nonRNHeapMB = StorageNodeParams.getNonRNHeapMB(0, true);

        /*
         * Check that the heap and cache value in the Admin and RN's copies of
         * the RN params is what we expect
         */
        checkHeapAndCacheValues(cs, 1, 1, 10000, heapPct, cachePct);

        /* Sanity check the actual cache sizing calculations */
        ParameterMap emptyPolicies = new ParameterMap();
        assertEquals(
            (heapPct * 10000) / 100,
            StorageNodeParams.calculateRNHeapAndCache(
                emptyPolicies, 1, 1, 10000 + nonRNHeapMB, heapPct, cachePct, 0).
                getHeapMB());
        assertEquals(
            cachePct,
            StorageNodeParams.calculateRNHeapAndCache(
                emptyPolicies, 1, 1, 0, heapPct, cachePct, 0).
                getCachePercent());
        assertEquals(
            0,
            StorageNodeParams.calculateRNHeapAndCache(
                emptyPolicies, 1, 1, 0, heapPct, cachePct, 0).
                getCacheBytes());

        /* Test more variations. */
        snSet.changeParams(
            cs, ParameterState.COMMON_MEMORY_MB, "18000", 0, 1, 2);
        checkHeapAndCacheValues(cs, 1, 1, 18000, heapPct, cachePct);

        snSet.changeParams(cs, ParameterState.SN_RN_HEAP_PERCENT, "60",
                           0, 1, 2);
        checkHeapAndCacheValues(cs, 1, 1, 18000, 60, cachePct);

        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);
        checkHeapAndCacheValues(cs, 2, 1, 18000, 60, cachePct);

        snSet.changeParams(cs, ParameterState.COMMON_MEMORY_MB, "0", 0, 1, 2);
        checkHeapAndCacheValues(cs, 2, 1, 0, 60, cachePct);
    }

    /**
     * Check that the Admin params have the expected JE cache settings for
     * all three RNs. Use verify to check the RN's notion of what their params
     * are. TODO: maybe also add command service entry point to query
     * the JE environment directly.
     * @throws RemoteException
     */
    private void checkHeapAndCacheValues(CommandServiceAPI cs,
                                         int capacity,
                                         int numRNsOnSN,
                                         int memoryMB,
                                         int rnHeapPercent,
                                         int rnCachePercent)
        throws RemoteException {

        Collection<RepNodeParams> rnpSet =
            cs.getParameters().getRepNodeParams();
        assertEquals("set=" + rnpSet, 3, rnpSet.size());

        RNHeapAndCacheSize heapAndCache =
            StorageNodeParams.calculateRNHeapAndCache(new ParameterMap(),
                                                      capacity,
                                                      numRNsOnSN,
                                                      memoryMB,
                                                      rnHeapPercent,
                                                      rnCachePercent,
                                                      0 /* numArbs */);

        for (RepNodeParams rnp : rnpSet) {
            logger.info("params=" + rnp.getJavaMiscParams());
            assertEquals("rn " + rnp.getRepNodeId() + " with jvmargs=" +
                         rnp.getJavaMiscParams() + " not as expected ",
                         heapAndCache.getCachePercent(),
                         rnp.getRNCachePercent());
            assertEquals("rn " + rnp.getRepNodeId() + " with jvmargs=" +
                         rnp.getJavaMiscParams() + " not as expected ",
                         heapAndCache.getCacheBytes(), rnp.getJECacheSize());
            assertEquals("Max heap not right on " + rnp.getRepNodeId() +
                         " with jvmargs=" + rnp.getJavaMiscParams() +
                         " not as expected ",  heapAndCache.getHeapMB(),
                         rnp.getMaxHeapMB());
            assertEquals("Min heap not right on " + rnp.getRepNodeId() +
                         " with jvmargs=" +  rnp.getJavaMiscParams() +
                         " not as expected ", heapAndCache.getHeapMB(),
                         rnp.getMinHeapMB());
        }

        VerifyResults results = cs.verifyConfiguration(false, true, false);
        /* The only problems that we expect are under capacity warnings */
        assertEquals(results.display(), 1, results.numViolations());
        assertTrue(results.getViolations().get(0).getClass().
                   equals(InsufficientAdmins.class));

        for (Problem p : results.getWarnings()) {
            if (!(p instanceof Validations.MissingRootDirectorySize)) {
                assertTrue("problem = " + p,
                           p instanceof Validations.UnderCapacity);
            }
        }
    }

    private void isIllegalCommandException(AdminFaultException e) {

        String faultClassName = e.getFaultClassName();
        assertTrue("fault was " + faultClassName,
                   faultClassName.contains("IllegalCommandException"));
    }

    /**
     * Test use of the force flag for deploy-topology
     */
    @Test
    public void testForce()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 9 /* max of 9 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();

        /*
         * Make an initial topology -- only one SN! No shards are
         * made
         */
        String first = "firstCandidate";
        try {
            cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
            fail("not possible to build an empty initial topo");
        } catch (AdminFaultException expected) {
            logger.info(expected.toString());
            isIllegalCommandException(expected);
        }

        /* Try to run change-repfactor, shouldn't be allowed */
        String changeRF1 = "ChangeRF1";
        cs.copyCurrentTopology(changeRF1);
        try {
            cs.changeRepFactor(changeRF1, Parameters.DEFAULT_POOL_NAME,
                               new DatacenterId(1), 1);
            fail("Can't run changeRepFactor first");
        } catch (AdminFaultException expected) {
            logger.info(expected.toString());
            isIllegalCommandException(expected);
        }

        /* add more SNs, make a new candidate */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2, 3);
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
        DeployUtils.printCandidate(cs, first, logger);
        logger.info(cs.validateTopology(first));

        /* Deploy */
        DeployUtils.printCurrentTopo(cs, "before deploy", logger);
        DeployUtils.printPreview(first, false, cs, logger);
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);

        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        DeployUtils.printCurrentTopo(cs, "after deploy", logger);

        /* try to change the repfactor to less than existing */
        String changeRF2 = "changeRF2";
        cs.copyCurrentTopology(changeRF2);
        try {
            cs.changeRepFactor(changeRF2, Parameters.DEFAULT_POOL_NAME,
                               new DatacenterId(1), 1);
           fail("can't reduce rf");
        } catch (AdminFaultException expected) {
            isIllegalCommandException(expected);
        }

        /*
         * Try to change the repfactor to 5. Since there are not enough SNs,
         * the topo will be in violation, and the execute will refuse to run it.
         */
        cs.changeRepFactor(changeRF2, Parameters.DEFAULT_POOL_NAME,
                           new DatacenterId(1), 5);
        DeployUtils.printCandidate(cs, changeRF2, logger);
        logger.info(cs.validateTopology(changeRF2));

        DeployUtils.printCurrentTopo(cs, "before deploy", logger);
        DeployUtils.printPreview(changeRF2, false, cs, logger);
        planNum = cs.createDeployTopologyPlan(changeRF2, changeRF2, null);
        cs.approvePlan(planNum);
        try {
           cs.executePlan(planNum, false);
           fail("Should not run");
        } catch (Exception expected) {
            logger.severe("got ==>" + expected);
        }

        cs.executePlan(planNum, true);
        cs.awaitPlan(planNum, 0, null);
        DeployUtils.printCurrentTopo(cs, "after deploy", logger);
        logger.info(cs.validateTopology(null));
    }

    /**
     * Change the capacity of a SN that is already hosting two RNs, and make
     * sure the JE cache size, RN heap size, parallel gc threads adjust
     * accordingly.
     *
     * The RN heap and JE cache sizes are a function of max(capacity, number of
     * RNS actually hosted on ths SN). In other words, if capacity is <
     * the number of RNs on the SN, we will use the actual number of RNs on
     * the SN as the determining factor, to make sure that there those RNs
     * will still be able to co-exist.
     */
    @Test
    public void testCapacityChange()
        throws Exception {

        /*
         * Make an initial 2 shard, 6 RN, 3 SN store, using processes, not
         * threads for SNs. Processes must be used in order for each SN to
         * have a heap size.
         */

        /*
         * With Zing, use a large amount to accommodate 1 GB min heap size.
         * Without Zing, use a small amount to run on smaller machines.
         */
        final int memMb = JVMSystemUtils.ZING_JVM ? 12800 : 2 * MB_PER_SN;
        final String memMbStr = String.valueOf(memMb);

        /*
         * Bootstrap the Admin, the first DC, and the first SN. Use processes
         * rather than threads to make sure that each RN truly tries out its
         * JVM params.
         */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 4 /* max of 4 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/,
                                                 false /* useThread */,
                                                 memMbStr);

        /*
         * Enable statistics gathering to work around a problem involving
         * waiting for an up-to-date shard [#26468]
         */
        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.RN_SG_ENABLED, "true");
        cs.setPolicies(mergeParameterMapDefaults(map));

        SNSet snSet = sysAdminInfo.getSNSet();

        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), memMbStr, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "2", 0, 1, 2);
        snSet.changeParams(cs, ParameterState.COMMON_NUMCPUS, "8", 0, 1, 2);

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
        DeployUtils.printPreview(first, false, cs, logger);
        DeployUtils.printCandidate(cs, first, logger);

        /*
         * The initial deploy will only work if the JE cache size for the
         * RN is the min of ((SN's max mb / capacity ), heap size)
         */
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.printCurrentTopo(cs, "initial topo", logger);

        VerifyResults results = cs.verifyConfiguration(false, true, false);
        assertEquals(results.getViolations().toString(), 1,
                     results.numViolations());
        assertTrue(results.getViolations().get(0).getClass().
                   equals(InsufficientAdmins.class));

        /* Default heap/cache percent are JVM vendor-specific. */
        final int heapPct =
            Integer.parseInt(ParameterState.SN_RN_HEAP_PERCENT_DEFAULT);
        final int cachePct =
            Integer.parseInt(ParameterState.RN_CACHE_PERCENT_DEFAULT);

        /*
         * See what the cache size is on the two RNs on sn1. The two ought to
         * be roughly the same, although not exactly because cache sizes are
         * determined dynamically in JE based on available memory.
         */
        List<RepNodeId> rnsOnSN =
            getRNsOnSN(1, cs.getParameters().getRepNodeParams());
        Topology topo = cs.getTopology();
        RegistryUtils registry =
            new RegistryUtils(topo, NULL_LOGIN_MGR, logger);
        long twoRNsCacheSize = getActualCache(registry, rnsOnSN.get(0));
        logger.info("Actual cache for " + rnsOnSN.get(0) + "=" +
                    twoRNsCacheSize);
        assertRoughlyEqual(
            twoRNsCacheSize, getActualCache(registry, rnsOnSN.get(1)), 0.01);
        checkCapacityParams(topo, 1, 2, memMb, heapPct, cachePct, 8,
                            cs.getParameters().getRepNodeParams());

        /*
         * Change the capacity for SN1 down to 1. Although capacity changes
         * should reset the heap and cache, in this case they should be the
         * same, because the number of RNs is > capacity.
         */
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 0);
        logger.info("capacity changed downward to 1");

        /* See if all the RNs stay up. */
        results = cs.verifyConfiguration(false, true, false);
        assertEquals(results.getViolations().toString(), 2,
                     results.numViolations());
        assertTrue(results.getViolations().get(0).getClass().
                   equals(OverCapacity.class));
        assertTrue(results.getViolations().get(1).getClass().
                   equals(InsufficientAdmins.class));

        Collection<RepNodeParams> rnpSet =
            cs.getParameters().getRepNodeParams();
        assertEquals("set=" + rnpSet, 6, rnpSet.size());

        checkCapacityParams(
            cs.getTopology(), 1, 1, memMb, heapPct, cachePct, 8,
            cs .getParameters().getRepNodeParams());

        assertRoughlyEqual(
            twoRNsCacheSize, getActualCache(registry, rnsOnSN.get(0)), 0.01);
        assertRoughlyEqual(
            twoRNsCacheSize, getActualCache(registry, rnsOnSN.get(1)), 0.01);

        /*
         * Change the NumCPUS for SN1 down to 4.
         */
        snSet.changeParams(cs, ParameterState.COMMON_NUMCPUS, "4", 0);
        checkCapacityParams(
            cs.getTopology(), 1, 1, memMb, heapPct, cachePct, 4,
            cs .getParameters().getRepNodeParams());

        /*
         * For SN2: clear memoryMB, and change the capacity. The heap and
         * cache settings should be cleared.
         */
        logger.info("Clearing out memory_mb setting for SN2");
        rnsOnSN = getRNsOnSN(2, cs.getParameters().getRepNodeParams());
        assertRoughlyEqual(
            twoRNsCacheSize, getActualCache(registry, rnsOnSN.get(0)), 0.01);
        assertRoughlyEqual(
            twoRNsCacheSize, getActualCache(registry, rnsOnSN.get(1)), 0.01);

        snSet.changeParams(cs, ParameterState.COMMON_MEMORY_MB, "0", 1);
        logger.info("Reducing capacity for SN2");
        snSet.changeParams(cs, ParameterState.COMMON_CAPACITY, "1", 1);

        /*
         * The cache should have been changed to a value that is based on the
         * the machine defaults. It would be better to specify exactly what
         * that value should be, but that's too hard to figure out in a unit
         * test that is run on many machines. It should not be 0, and should
         * not be what it was before.
         */
        long newCache = getActualCache(registry, rnsOnSN.get(0));
        assertTrue(newCache != 0);
        assertTrue(newCache != twoRNsCacheSize);
        newCache = getActualCache(registry, rnsOnSN.get(1));
        assertTrue(newCache != 0);
        assertTrue(newCache != twoRNsCacheSize);

        DeployUtils.checkDeployment(cs, 3 /* rf */,
                                    5000,
                                    logger,
                                    2, OverCapacity.class,
                                    3, MultipleRNsInRoot.class,
                                    3, MissingRootDirectorySize.class,
                                    1, InsufficientAdmins.class);
        checkCapacityParams(cs.getTopology(), 2, 1, 0, heapPct, cachePct, 8,
                            cs.getParameters().getRepNodeParams());

        /*
         * Rebalance the topology, and fix the over-capacity SN1. This
         * should result in the RN's memory getting adjusted up to the size
         * appropriate for a RN that is the sole occupant of a SN.
         */
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), memMbStr, 3);
        String oneMoreSN = "oneMoreSN";
        cs.copyCurrentTopology(oneMoreSN);
        cs.rebalanceTopology(oneMoreSN, Parameters.DEFAULT_POOL_NAME, null);
        planNum = cs.createDeployTopologyPlan(oneMoreSN, oneMoreSN, null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        cs.assertSuccess(planNum);
        DeployUtils.printCurrentTopo(cs, "rebalanced topo", logger);
        results = cs.verifyConfiguration(false, true, false);
        logger.info("After rebalance: " + results.display());

        rnsOnSN = getRNsOnSN(1, cs.getParameters().getRepNodeParams());
        registry = new RegistryUtils(cs.getTopology(), NULL_LOGIN_MGR, logger);
        long singleRNCacheSize = getActualCache(registry, rnsOnSN.get(0));
        logger.info("Cache for " + rnsOnSN.get(0) + " " + singleRNCacheSize);
        /*
         * Note that larger memory sizes seem to result in more variability in
         * the JVM runtime max memory value that JE uses to pick cache sizes,
         * so we need a reasonably large slop factor here.
         */
        assertRoughlyEqual(
            (2 * twoRNsCacheSize)>>3, singleRNCacheSize>>3, 0.1);

        rnsOnSN = getRNsOnSN(4, cs.getParameters().getRepNodeParams());
        singleRNCacheSize = getActualCache(registry, rnsOnSN.get(0));
        logger.info("Cache for " + rnsOnSN.get(0) + " " + singleRNCacheSize);
        assertRoughlyEqual(
            (2 * twoRNsCacheSize)>>3, singleRNCacheSize>>3, 0.1);
    }

    private static void assertRoughlyEqual(long val1,
                                           long val2,
                                           double allowedTolerance) {

        final double diff = Math.abs(val1 - val2);
        final double actualTolerance = diff / Math.max(val1, val2);

        if (actualTolerance <= allowedTolerance) {
            return;
        }

        fail("Values differ by " + actualTolerance + " but only " +
                allowedTolerance + " tolerance ratio is allowed." +
                " val1=" + val1 + " val2=" + val2);
    }

    /**
     * Return a list of RNs that live on the given SN
     */
    private List<RepNodeId> getRNsOnSN(int snNum,
                                       Collection<RepNodeParams> rnpSet) {
        List<RepNodeId> rnIds = new ArrayList<RepNodeId>();
        for (RepNodeParams rnp : rnpSet) {
            if (rnp.getStorageNodeId().getStorageNodeId() != snNum) {
                continue;
            }
            rnIds.add(rnp.getRepNodeId());
        }
        return rnIds;
    }

    private void checkCapacityParams(Topology topo,
                                     int snNum,
                                     int capacity,
                                     int snMemMB,
                                     int rnHeapPercent,
                                     int rnCachePercent,
                                     int numCPUs,
                                     Collection<RepNodeParams> rnpSet) {

        Set<RepNodeId> hostedRNs =
            topo.getHostedRepNodeIds(new StorageNodeId(snNum));
        RNHeapAndCacheSize heapAndCache =
            StorageNodeParams.calculateRNHeapAndCache(new ParameterMap(),
                                                      capacity,
                                                      hostedRNs.size(),
                                                      snMemMB,
                                                      rnHeapPercent,
                                                      rnCachePercent,
                                                      0 /* numArbs */);

        int gcThreads = StorageNodeParams.calcGCThreads
            (numCPUs, capacity, 4, /* gcThreadFloor */
             8 /* gcThreadThreshold */, 62 /* gcThreadPercent */);

        /* Check the rn params for SN1 */
        for (RepNodeParams rnp : rnpSet) {
            if (rnp.getStorageNodeId().getStorageNodeId() != snNum) {
                continue;
            }
            logger.info(rnp.getRepNodeId() + " params=" +
                        rnp.getJavaMiscParams());
            assertEquals("rn " + rnp.getRepNodeId() + " with jvmargs=" +
                         rnp.getJavaMiscParams() + " not as expected ",
                         heapAndCache.getCachePercent(),
                         rnp.getRNCachePercent());
            assertEquals("rn " + rnp.getRepNodeId() + " with jvmargs=" +
                         rnp.getJavaMiscParams() + " not as expected ",
                         heapAndCache.getCacheBytes(), rnp.getJECacheSize());
            assertEquals("rn " + rnp.getRepNodeId() + " with jvmargs=" +
                         rnp.getJavaMiscParams() + " not as expected ",
                         heapAndCache.getHeapMB(), rnp.getMaxHeapMB());
            assertEquals("rn " + rnp.getRepNodeId() + " with jvmargs=" +
                         rnp.getJavaMiscParams() + " not as expected ",
                         gcThreads, rnp.getParallelGCThreads());
            /**
            * As part of jvm tuning for out-of-cache (kvstore-888) we
            * decided to make the number of concurrent threads the same
            * as the number of parallel threads for g1gc.
            * This test is to check that its indeed the case, as well
            * as to cover the method getConcurrentGCThreads().
            */
            assertEquals("rn " + rnp.getRepNodeId() + " with jvmargs=" +
                         rnp.getJavaMiscParams() + " not as expected ",
                         gcThreads, rnp.getConcurrentGCThreads());
        }
    }

    /**
     * Ask a RN for its JE cache size, and return it in MB
     */
    private long getActualCache(RegistryUtils registry, RepNodeId rnId)
        throws RemoteException, NotBoundException {

        final RepNodeAdminAPI rnAdmin = registry.getRepNodeAdmin(rnId);
        return rnAdmin.getInfo().getEnvConfig().getCacheSize();
    }

    @Test
    public void testConcurrentDeploys()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 3 /* max of 3 SNs */,
                                                 "DataCenterA",
                                                 3 /* repFactor*/);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("DataCenterA"), MB_PER_SN_STRING,
                     1, 2);

        /* Make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME, 100, false);
        DeployUtils.printCandidate(cs, first, logger);
        logger.info(cs.validateTopology(first));
        DeployUtils.printPreview(first, false, cs, logger);

        /* Create two different deployment plans */
        int planA = cs.createDeployTopologyPlan("DeployA", first, null);
        int planB = cs.createDeployTopologyPlan("DeployB", first, null);

        cs.approvePlan(planA);
        cs.approvePlan(planB);
        cs.executePlan(planA, false);
        cs.awaitPlan(planA, 0, null);
        cs.assertSuccess(planA);
        try {
            cs.executePlan(planB, false);
            fail("Should fail, because planB is on an old topology ");
        } catch (AdminFaultException expected) {
            isIllegalCommandException(expected);
        }

        cs.cancelPlan(planB);
        int planC = cs.createDeployTopologyPlan("DeployC", first, null);
        cs.approvePlan(planC);
        cs.executePlan(planC, false);
        cs.awaitPlan(planC, 0, null);
        cs.assertSuccess(planC);

        DeployUtils.checkDeployment(cs, 3, 5000, logger,
                                    1, InsufficientAdmins.class,
                                    3, MissingRootDirectorySize.class);
    }

    @Test
    public void testGCThreadCalculation() {

        /*
         * args to calcGCThreads:
         *       numCPUs, capacity, gc floor, gcThreshold, gcThresholdPercent
         */

        /* if numCPUs is 0, we won't set the gc threads at all */
        assertEquals(0, StorageNodeParams.calcGCThreads(0, 1, 4, 8, 62));

        assertEquals(1, StorageNodeParams.calcGCThreads(1, 1, 4, 8, 62));
        assertEquals(2, StorageNodeParams.calcGCThreads(2, 1, 3, 8, 62));
        assertEquals(3, StorageNodeParams.calcGCThreads(4, 2, 3, 8, 62));
        assertEquals(4, StorageNodeParams.calcGCThreads(4, 1, 4, 8, 62));
        assertEquals(6, StorageNodeParams.calcGCThreads(6, 1, 6, 8, 62));
        assertEquals(6, StorageNodeParams.calcGCThreads(6, 3, 6, 8, 62));
        assertEquals(8, StorageNodeParams.calcGCThreads(8, 1, 4, 8, 62));
        assertEquals(8, StorageNodeParams.calcGCThreads(9, 1, 4, 8, 62));
        assertEquals(8, StorageNodeParams.calcGCThreads(10, 1, 4, 8, 62));
        assertEquals(8, StorageNodeParams.calcGCThreads(11, 1, 4, 8, 62));
        assertEquals(8, StorageNodeParams.calcGCThreads(12, 1, 4, 8, 62));
        assertEquals(8, StorageNodeParams.calcGCThreads(13, 1, 4, 8, 62));
        assertEquals(8, StorageNodeParams.calcGCThreads(14, 1, 4, 8, 62));
        assertEquals(9, StorageNodeParams.calcGCThreads(15, 1, 4, 8, 62));

        assertEquals(1, StorageNodeParams.calcGCThreads(1, 2, 4, 8, 62));
        assertEquals(2, StorageNodeParams.calcGCThreads(2, 2, 4, 8, 62));
        assertEquals(3, StorageNodeParams.calcGCThreads(3, 2, 4, 8, 62));
        assertEquals(4, StorageNodeParams.calcGCThreads(4, 2, 4, 8, 62));
        assertEquals(4, StorageNodeParams.calcGCThreads(5, 2, 4, 8, 62));
        assertEquals(4, StorageNodeParams.calcGCThreads(6, 2, 4, 8, 62));

        /* BDA-like conditions */
        assertEquals(5, StorageNodeParams.calcGCThreads(64, 12, 4, 8, 62));

        /*
         * The floor is > the threshold -- make sure that we use the
         * threshold.
         */
        assertEquals(8, StorageNodeParams.calcGCThreads(64, 12, 10, 8, 62));
        assertEquals(10, StorageNodeParams.calcGCThreads(64, 12, 10, 20, 62));
    }

    @Test
    public void testCalcEndpointGroupNumThreads() {
        try {
            StorageNodeParams.calcEndpointGroupNumThreads(0, 1, 1, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            StorageNodeParams.calcEndpointGroupNumThreads(1, -1, 1, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            StorageNodeParams.calcEndpointGroupNumThreads(1, 1, 0, 1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
        try {
            StorageNodeParams.calcEndpointGroupNumThreads(1, 1, 1, -1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        /* capacity=0 => return floor */
        assertEquals(2, calcEndpointGroupNumThreads(1, 0, 2, 3));
        assertEquals(9, calcEndpointGroupNumThreads(10, 0, 9, 8));

        /* vary capacity */
        assertEquals(32, calcEndpointGroupNumThreads(16, 1, 2, 200));
        assertEquals(16, calcEndpointGroupNumThreads(16, 2, 2, 200));
        assertEquals(5, calcEndpointGroupNumThreads(16, 6, 2, 200));
        assertEquals(4, calcEndpointGroupNumThreads(16, 8, 2, 200));
        assertEquals(2, calcEndpointGroupNumThreads(16, 13, 2, 200));
        assertEquals(2, calcEndpointGroupNumThreads(16, 16, 2, 200));
        assertEquals(2, calcEndpointGroupNumThreads(16, 17, 2, 200));

        /* vary capacity, different floor */
        assertEquals(4, calcEndpointGroupNumThreads(16, 8, 3, 200));
        assertEquals(3, calcEndpointGroupNumThreads(16, 13, 3, 200));
        assertEquals(3, calcEndpointGroupNumThreads(16, 16, 3, 200));
        assertEquals(3, calcEndpointGroupNumThreads(16, 17, 3, 200));

        /* vary capacity, different percent */
        assertEquals(8, calcEndpointGroupNumThreads(16, 8, 3, 400));
        assertEquals(4, calcEndpointGroupNumThreads(16, 13, 3, 400));
        assertEquals(4, calcEndpointGroupNumThreads(16, 16, 3, 400));
        assertEquals(3, calcEndpointGroupNumThreads(16, 17, 3, 400));

        /* vary numCPUs */
        assertEquals(2, calcEndpointGroupNumThreads(1, 6, 2, 200));
        assertEquals(2, calcEndpointGroupNumThreads(2, 6, 2, 200));
        assertEquals(2, calcEndpointGroupNumThreads(4, 6, 2, 200));
        assertEquals(2, calcEndpointGroupNumThreads(8, 6, 2, 200));
        assertEquals(5, calcEndpointGroupNumThreads(16, 6, 2, 200));
        assertEquals(8, calcEndpointGroupNumThreads(24, 6, 2, 200));
        assertEquals(10, calcEndpointGroupNumThreads(32, 6, 2, 200));
        assertEquals(21, calcEndpointGroupNumThreads(64, 6, 2, 200));

        /* vary percent */
        assertEquals(2, calcEndpointGroupNumThreads(32, 3, 2, 0));
        assertEquals(5, calcEndpointGroupNumThreads(32, 3, 2, 50));
        assertEquals(13, calcEndpointGroupNumThreads(32, 3, 2, 125));
        assertEquals(106, calcEndpointGroupNumThreads(16, 3, 2, 2000));
    }

    /**
     * R2 requires that there be only 1 datacenter, but R2.1 allows multiple
     * ones.
     */
    @Test
    public void testOneDCConstraint()
        throws Exception {

        /* Bootstrap the Admin, the first DC, and the first SN. */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 3 /* max of 3 SNs */,
                                                 "FirstDC",
                                                 3 /* repFactor*/);
        CommandServiceAPI cs = sysAdminInfo.getCommandService();

        /* Deploy DC 1 again */
        AdminUtils.deployDatacenter(cs, "FirstDC", 3, sysAdminInfo);

        /* Deploy DC 2 */
        AdminUtils.deployDatacenter(cs, "SecondDC", 3, sysAdminInfo);

        /* Deploy DC 1 yet again */
        AdminUtils.deployDatacenter(cs, "FirstDC", 3, sysAdminInfo);

        /* Deploy DC 1 with different rep factor */
        try {
            AdminUtils.deployDatacenter(cs, "FirstDC", 2, sysAdminInfo);
            fail("Should fail -- different rep factor");
        } catch (AdminFaultException e) {
            assertEquals(IllegalCommandException.class.getName(),
                         e.getFaultClassName());
        }
    }

    /**
     * Test that information about a RN failure propagates to the Admin and is
     * displayed in the show plan -id <id> command. The goal is to ensure that
     * the primary problem is displayed to the user.
     */
    @Test
    public void testRNFailurePropagation()
        throws Exception {

        /*
         * Bootstrap the Admin, the first DC, and the first SN. Use processes
         * rather than threads to make sure that each RN truly tries out its
         * JVM params.
         */
        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 2 /* max of 4 SNs */,
                                                 "DataCenterA",
                                                 1 /* repFactor*/,
                                                 //   false /* useThread */,
                                                 false /* useThread */,
                                                 MB_PER_SN_STRING);

        CommandServiceAPI cs = sysAdminInfo.getCommandService();
        ParameterMap map = new ParameterMap();

        /* Set up a bad JVM param. */
        String badParam = "-XXXUSER-MISTAKE-INVALID-PARAM";
        map.setParameter(ParameterState.JVM_MISC, badParam);

        /*
         * Make sure that we turn on logging for the unit test, to make it
         * easier to debug.
         */
        map.setParameter(ParameterState.JVM_LOGGING,
                         System.getProperty("java.util.logging.config.file"));

        /*
         * Retry will always fail, use a shorter time.
         */
        map.setParameter(ParameterState.AP_NEW_RN_RETRY_TIME, "5 s");

        cs.setPolicies(mergeParameterMapDefaults(map));

        /* make an initial topology */
        String first = "firstCandidate";
        cs.createTopology(first, Parameters.DEFAULT_POOL_NAME,
                          10 /* numPartitions*/, false);
        DeployUtils.printPreview(first, false, cs, logger);
        DeployUtils.printCandidate(cs, first, logger);

        /* This deployment should fail, because of the bad JVM param. */
        int planNum = cs.createDeployTopologyPlan("InitialDeploy", first,
                                                  null);
        cs.approvePlan(planNum);
        cs.executePlan(planNum, false);
        cs.awaitPlan(planNum, 0, null);
        String status = cs.getPlanStatus(planNum, 0, false /* json */);
        logger.info("=================\n" + status);

        /* Make sure the status contains a mention of the bad param setting.*/
        assertTrue(status.contains(badParam));
        DeployUtils.printCurrentTopo(cs, "initial topo", logger);

        VerifyResults results = cs.verifyConfiguration(false, true, false);
        assertEquals(results.getViolations().toString(), 2,
                     results.numViolations());

        /* Clear the bad JVM param, now set a bad cache size */
        map = new ParameterMap();
        map.setParameter(ParameterState.JVM_MISC, "");
        map.setParameter(ParameterState.JE_CACHE_SIZE, "100000000000");
        int fixupId = cs.createChangeParamsPlan
            ("FixRNParams", new RepNodeId(1, 1), map);
        cs.approvePlan(fixupId);
        cs.executePlan(fixupId, false);
        cs.awaitPlan(fixupId, 0, null);

        /*
         * The plan status should now mention the new problem, and should no
         * longer mention the old problem.
         */
        status = cs.getPlanStatus(fixupId, 0, false /* json */);
        logger.info("=================\n" + status);
        assertFalse(status.contains(badParam));
        assertTrue(status.contains("but the JVM is only configured for"));
        results = cs.verifyConfiguration(false, true, false);
        assertEquals(results.getViolations().toString(), 2,
                     results.numViolations());
    }

    /**
     * Attempt to migrate RNs and SNs to different datacenters.
     */
    @Test
    public void testMigrateRNsSNsDifferentDatacenters()
        throws Exception {

        sysAdminInfo = AdminUtils.bootstrapStore(kvstoreName,
                                                 8 /* max SNs */, "Primary",
                                                 3 /* RF */);

        final CommandServiceAPI cs = sysAdminInfo.getCommandService();

        AdminUtils.deployDatacenter(cs, "Secondary1", 1 /* RF */,
                                    DatacenterType.SECONDARY, sysAdminInfo);
        AdminUtils.deployDatacenter(cs, "Secondary2", 1 /* RF */,
                                    DatacenterType.SECONDARY, sysAdminInfo);

        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("Primary"), MB_PER_SN_STRING,
                     0, 1, 2, 3);
        snSet.deploy(cs, sysAdminInfo.getDCId("Secondary1"), MB_PER_SN_STRING,
                     4, 5);
        snSet.deploy(cs, sysAdminInfo.getDCId("Secondary2"), MB_PER_SN_STRING,
                     6, 7);

        AdminUtils.makeSNPool(cs, "pool", snSet.getIds(0, 1, 2, 4, 6));

        cs.createTopology("topo", "pool", 100, false);
        final int topoPlan = cs.createDeployTopologyPlan("topoPlan", "topo",
                                                         null);
        cs.approvePlan(topoPlan);
        cs.executePlan(topoPlan, false);
        cs.awaitPlan(topoPlan, 0, null);
        final Topology topo = cs.getTopology();
        DeployUtils.checkTopo(topo, 3 /*numDCs*/, 8 /*numSNs*/, 1 /*numRGs*/,
                              5 /*numRNs*/, 100 /*numParts*/);

        /* Attempt to migrate RN from primary SN 2 to secondary SN 5 */
        cs.copyCurrentTopology("migrateRNPlan");
        final RepNodeId rnId2 =
            topo.getHostedRepNodeIds(snSet.getId(2)).iterator().next();
        try {
            cs.moveRN("migrateRNPlan", rnId2, snSet.getId(5));
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            isIllegalCommandException(e);
            assertThat(e.getMessage(), containsString("different zone"));
            logger.info("Move RN to different DC: " + e.getMessage());
        }

        /* Attempt to migrate RN from secondary SN 4 to secondary SN 7 */
        final RepNodeId rnId4 =
            topo.getHostedRepNodeIds(snSet.getId(4)).iterator().next();
        try {
            cs.moveRN("migrateRNPlan", rnId4, snSet.getId(7));
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            isIllegalCommandException(e);
            assertThat(e.getMessage(), containsString("different zone"));
            logger.info("Move RN to different DC: " + e.getMessage());
        }

        /* Shutdown primary SN 2 and attempt to migrate to secondary SN 5 */
        snSet.shutdown(2, logger);
        try {
            cs.createMigrateSNPlan("migrateSNPlan", snSet.getId(2),
                                   snSet.getId(5));
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            isIllegalCommandException(e);
            assertThat(e.getMessage(), containsString("different zone"));
            logger.info("Move SN to different DC: " + e.getMessage());
        }

        /* Shutdown secondary SN 4 and attempt to migrate to secondary SN 7 */
        snSet.shutdown(4, logger);
        try {
            cs.createMigrateSNPlan("migrateSNPlan", snSet.getId(4),
                                   snSet.getId(7));
            fail("Expected AdminFaultException");
        } catch (AdminFaultException e) {
            isIllegalCommandException(e);
            assertThat(e.getMessage(), containsString("different zone"));
            logger.info("Move SN to different DC: " + e.getMessage());
        }
    }

    /**
     * Deploy a secondary datacenter first to make sure that the self-electing
     * RN in a shard is not a secondary node.
     */
    @Test
    public void testSecondaryDatacenterFirst()
        throws Exception {

        /* Create secondary data center first */
        sysAdminInfo = AdminUtils.bootstrapStore(
            kvstoreName, 2 /* max SNs */, "Secondary", 1 /* RF */,
            true /* useThreads */, MB_PER_SN_STRING /* mb */,
            DatacenterType.SECONDARY);

        /* Then create primary data center */
        final CommandServiceAPI cs = sysAdminInfo.getCommandService();
        AdminUtils.deployDatacenter(cs, "Primary", 1 /* RF */,
                                    DatacenterType.PRIMARY, sysAdminInfo);
        final SNSet snSet = sysAdminInfo.getSNSet();
        snSet.deploy(cs, sysAdminInfo.getDCId("Primary"), MB_PER_SN_STRING, 1);

        /* Create topology */
        AdminUtils.makeSNPool(cs, "pool", snSet.getIds(0, 1));
        cs.createTopology("topo", "pool", 100, false);
        final int topoPlan = cs.createDeployTopologyPlan("topoPlan", "topo",
                                                         null);
        cs.approvePlan(topoPlan);
        cs.executePlan(topoPlan, false);
        cs.awaitPlan(topoPlan, 30, TimeUnit.SECONDS);
        cs.assertSuccess(topoPlan);
        DeployUtils.printCurrentTopo(cs, "Current topo", logger);
        DeployUtils.checkTopo(cs.getTopology(),
                              2 /*numDCs*/,
                              2 /*numSNs*/,
                              1 /*numRGs*/,
                              2 /*numRNs*/,
                              100 /*numParts*/);
    }

    /** Restart the specified SN. */
    private void restartSN(SNSet snSet, int whichSN, Topology topo)
        throws Exception {

        snSet.restart(whichSN, topo.getHostedRepNodeIds(snSet.getId(whichSN)),
                      logger);
    }

    /**
     * Returns the set of RepNodeIds that are UNREACHABLES on this SN
     */
    private Set<RepNodeId> getUnreachableRepNodeIds(Topology topo)
    					throws Exception {
    	 Set<RepNodeId> unreachableRNs = new HashSet<RepNodeId>();

        PingCollector collector = new PingCollector(topo, logger);
        for (Entry<ResourceId, ServiceStatus> entry :
            collector.getTopologyStatus().entrySet()) {

            if (entry.getKey().getType() == ResourceId.ResourceType.REP_NODE) {

            	/*
                 * Gather the rep nodes that show an UNREACHABLE status */
                if (entry.getValue() == ServiceStatus.UNREACHABLE) {
                	unreachableRNs.add((RepNodeId)entry.getKey());
                }
            }
        }
        return unreachableRNs;
    }
}
