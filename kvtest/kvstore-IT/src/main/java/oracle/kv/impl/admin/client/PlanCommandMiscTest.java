/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static java.util.Arrays.asList;
import static oracle.kv.impl.admin.AdminUtils.awaitPlanSuccess;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.util.CreateStore;
import oracle.kv.util.CreateStore.ZoneInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Miscellaneous plan tests. */
public class PlanCommandMiscTest extends TestBase {

    private static final int startPort = 5000;

    private CreateStore createStore;
    private int numSNs = 3;
    private List<ZoneInfo> zones =
        asList(new ZoneInfo(1), new ZoneInfo(1), new ZoneInfo(1));
    private int[] adminSNs;
    private ByteArrayOutputStream shellOutput;
    private CommandShell shell;
    private CommandServiceAPI cs;

    /** Whether to use threads to create services */
    private boolean useThreads = true;

    @Before
    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
    }

    @After
    @Override
    public void tearDown() {
        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
        logger.info("Shell output for " + testName.getMethodName() + ": " +
                    shellOutput);
    }

    private void createStore() throws Exception {
        createStore = new CreateStore(
            kvstoreName,
            startPort,
            numSNs, /* Storage Nodes */
            zones,
            numSNs * 10, /* Partitions */
            1 /* capacity */,
            CreateStore.MB_PER_SN,
            useThreads,
            null /* mgmtImpl */);
    }

    private void init()
        throws Exception {

        if (createStore == null) {
            createStore();
        }

        /* Allocate RF admins per zone */
        adminSNs = createStore.computeAdminsForZones();
        createStore.setAdminLocations(adminSNs);

        createStore.start();
        shellOutput = new ByteArrayOutputStream();
        shell = new CommandShell(System.in, new PrintStream(shellOutput));
        shell.connectAdmin(
            "localhost", createStore.getRegistryPort(),
            createStore.getDefaultUserName(),
            createStore.getDefaultUserLoginPath());
        cs = shell.getAdmin();
    }

    /**
     * Test that removing the master admin does not run into a problem where
     * the old master attempts to join as a replica after it is removed.
     * [#24571]
     */
    @Test
    public void testRemoveMasterAdmin() throws Exception {

        /*
         * I would have preferred to control the timing to make sure the
         * replica joins after the removal occurs, but that requires JE hooks
         * that are not available.  This test was useful for reproducing the
         * failure on my local machine and confirming that the fix worked, so
         * leave it at that.
         *
         * In addition, the failure in this case only involves SEVERE logging
         * output.  Because the admin gets shutdown immediately afterwards, I
         * was not able to get the output at the new admin.
         */

        init();
        int planId =
            cs.createRemoveAdminPlan("remove-admins", null, new AdminId(1),
                                     false);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        assertTrue("New admin master",
                   new PollCondition(1000, 60000) {
                       @Override
                       protected boolean condition() {
                           try {
                               cs = createStore.getAdminMaster();
                               return createStore.getAdminIndex() != 0;
                           } catch (Exception e) {
                               return false;
                           }
                       }
                   }.await());
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }

    /**
     * Test that executing a plan to deploy a redistributed topology after
     * deploying a zone without supplying SNs will fail unless -force is
     * specified. [KVSTORE-542]
     */
    @Test
    public void testDeployZoneNoSNs() throws Exception {

        /*
         * Start with an RF=1 two shard store. Create 3 SNs but only use 2 for
         * now.
         */
        numSNs = 3;
        zones = asList(new ZoneInfo(1));
        createStore();
        createStore.setPoolSize(2);
        init();

        /* Create a second zone */
        final int zonePlan =
            cs.createDeployDatacenterPlan("createZonePlan", "zone2", 1,
                                          DatacenterType.SECONDARY,
                                          false, /* allowArbiters */
                                          false /* masterAffinity */);
        awaitPlanSuccess(cs, zonePlan);

        /* Create a new topology that will not populate the new zone */
        cs.copyCurrentTopology("topo2");
        cs.redistributeTopology("topo2", CreateStore.STORAGE_NODE_POOL_NAME);

        /*
         * Attempt to deploy the new topology and make sure that executing the
         * plan fails because of multiple existing violations
         */
        final int topoPlan2 =
            cs.createDeployTopologyPlan("deployTopoPlan", "topo2", null);
        cs.approvePlan(topoPlan2);
        AdminFaultException exception =
            checkException(() -> cs.executePlan(topoPlan2, false /* force */),
                           AdminFaultException.class,
                           "existing violations");
        logger.info("Got exception: " + exception);

        /* Make sure the plan works with force=true */
        awaitPlanSuccess(cs, topoPlan2, true /* force */);

        /* Add sn3 (in zone 1) to the pool */
        cs.addStorageNodeToPool(CreateStore.STORAGE_NODE_POOL_NAME,
                                createStore.getStorageNodeIds()[2]);

        /* Redistribute will add a new shard... */
        cs.copyCurrentTopology("topo3");
        cs.redistributeTopology("topo3", CreateStore.STORAGE_NODE_POOL_NAME);
        final int topoPlan3 =
            cs.createDeployTopologyPlan("deployTopoPlan3", "topo3", null);
        cs.approvePlan(topoPlan3);

        /* ... but that produces a new violation for zone 2 */
        exception =
            checkException(() -> cs.executePlan(topoPlan3, false /* force */),
                           AdminFaultException.class,
                           "a new violation");
        logger.info("Got exception: " + exception);
    }
}
