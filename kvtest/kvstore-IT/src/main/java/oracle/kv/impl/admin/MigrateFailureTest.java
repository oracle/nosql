/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.TimeUnit;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test migration in a 2x2 store.  This fails because SN migration requires
 * that the source SN is down and in a RF2 situation, there will be no
 * quorum. Some manual steps are required to make it work and this test checks
 * that this manual workaround works.
 *
 * 1.  try migrating while the original node is still running (fails)
 * 2.  stop the original and try again (fails because there is no quorum)
 * 3.  change params on the "new" master to get it to come up anyway.  This
 * "fails" because it's not running, but it'll get the new params.
 * 4.  start the new master -- it will come up
 * 5.  retry the original migrate plan -- this works.
 */
public class MigrateFailureTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;
    private static final int rf = 2;
    private static final int numSns = 4;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestStatus.setManyRNs(true);
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        if (createStore != null) {
            createStore.shutdown();
        }
        LoggerUtils.closeAllHandlers();
    }

    @Test
    public void testReadOnly()
        throws Exception {

        /**
         * 4 Storage Nodes (2 spares), RF of 2.Note that we need 3 Admins,so
         * that they can still form a quorum when one SN is down, so that
         * administrative commands can continue on.
         */
        createStore =
            new CreateStore(kvstoreName, startPort, numSns, rf, 10, 1);
        createStore.setPoolSize(rf);
        createStore.start();

        CommandServiceAPI cs = createStore.getAdmin();
        Topology t = cs.getTopology();
        logger.info(TopologyPrinter.printTopology(t));
        assertNotNull(t);

        /**
         * Find the two SNs that host a RN, and attempt to migrate one SN to
         * the free SN while the original SN is still up, which is verboten.
         * Expect a failure.
         */
        StorageNodeId snids[] = createStore.getStorageNodeIds();
        StorageNodeId snA = snids[0];
        StorageNodeId snB = snids[1];
        StorageNodeId freeSN = snids[3];

        assertEquals(1, createStore.numRNs(snA));
        assertEquals(1, createStore.numRNs(snB));
        assertEquals(0, createStore.numRNs(freeSN));
        assertEquals(false, createStore.hasAdmin(freeSN));

        int planId = 0;
        try {
            planId = cs.createMigrateSNPlan(null, snA, freeSN);
            cs.approvePlan(planId);
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 30, TimeUnit.SECONDS);
            cs.assertSuccess(planId);

            fail("Plan should have failed");
        } catch (Exception e) {
        }

        /**
         * Stop the original, get a new cs (it'll fail over), and try again.
         */
        createStore.getStorageNodeAgent(snA).shutdown(true, false);
        cs = createStore.getAdminMaster();
        assertNotNull(cs);
        cs.verifyConfiguration(false, true, false);
        try {
            cs.executePlan(planId, false);

            /* This plan will never succeed, because it can't get quorum for
             * the migration of the RN, so sleep for a bit, and then interrupt
             * it.
             */
            Thread.sleep(1000);
            cs.interruptPlan(planId);

            cs.awaitPlan(planId, 0, null);

            /* We expect this to throw the original exception */
            cs.assertSuccess(planId);
            fail("Retry should fail as well, for now");
        } catch (Exception e) {
            cs.cancelPlan(planId);
        }

        /**
         * At this point the other replica in the shard is going to be hung
         * (timing out) trying to come up.  It won't.  Worse, stopping it
         * gracefully doesn't work because the stop blocks on a lock in
         * RepEnvHandleManager.  So...  forcibly stop the process.
         *
         * TODO: should electableGroupSizeOverride state be changed back later?
         */
        RepNodeId rnId = createStore.getRNs(snB).get(0);
        createStore.getStorageNodeAgent(snB).stopRepNode(rnId, true);

        /**
         * Change params to tell the replica node that it has quorum on its
         * own. 
         */ 
        ParameterMap map = new ParameterMap();
        RepNodeParams rnParams = new RepNodeParams(map);
        rnParams.setElectableGroupSizeOverride(1);
        planId = cs.createChangeParamsPlan(null, rnId, map);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 30, TimeUnit.SECONDS);
        cs.assertSuccess(planId);

        createStore.getStorageNodeAgent(snB).startRepNode(rnId);

        /**
         * Now, re-create and retry the original migrate plan
         */
        planId = cs.createMigrateSNPlan(null, snA, freeSN);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);

        /**
         * Verify the new configuration
         */
        VerifyResults vr = cs.verifyConfiguration(false, true, false);
        List<VerifyConfiguration.Problem> problems = vr.getViolations();

        /**
         * This should really be 0 but the dead SNA shows up.
         */
        assertTrue(problems.toString(),
                   (problems.size() == 0 || problems.size() == 1));
    }
}
