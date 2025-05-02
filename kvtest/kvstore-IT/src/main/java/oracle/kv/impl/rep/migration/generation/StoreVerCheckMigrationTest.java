/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration.generation;

import static java.util.logging.Level.INFO;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test that migration should minimal store version
 */
public class StoreVerCheckMigrationTest extends TableTestBase {

    private static final boolean traceOnScreen = false;

    /* # of shards after expansion */
    private static final int NUM_PARTS = 4;

    @Override
    public void setUp() throws Exception {
        kvstoreName = "mystore";
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
    }

    @Override
    public void tearDown() throws Exception {
       createStore.shutdown();
    }

    /**
     * Test if the store is not upgraded, the migration should be rejected by
     * source.
     */
    @Test
    public void testCheckStoreVerInMigration() throws Exception {

//        final KVVersion oldVer = KVVersion.R2_1;

        /* Arrange to get the RepNodeService instance */
        final AtomicReference<RepNodeService> repNodeService =
            new AtomicReference<>();
        /* 1x1 store, only 1 rn */
        RepNodeService.startTestHook = repNodeService::set;
        trace("Test hook set");

        /* Create the store using threads so the hooks will work */
        createStore = new CreateStore(
            kvstoreName, startPort, 2 /* SNs */, 1 /* RF */,
            NUM_PARTS /* partitions */, 1 /* capacity */,
            CreateStore.MB_PER_SN /* memoryMB */, true /* useThreads */,
            null /* mgmtImpl */);

        createStore.setPoolSize(1); /* reserve one SN for later. */
        createStore.start();
        trace("Store created");

        assertTrue("Waiting for RepNodeService",
                   new PollCondition(500, 10000) {
                       @Override
                       protected boolean condition() {
                           return repNodeService.get() != null;
                       }
                   }.await());

        final RepNodeService rns = repNodeService.get();
        if (rns == null) {
            fail("Cannot obtain RNS handle");
            return;
        }

        final RepNode rn = rns.getRepNode();
        if (rn == null) {
            fail("Cannot obtain RepNode handle");
            return;
        }
        trace("Get RN handle: " + rn.getRepNodeId());

        // TODO - Thw following code does not work as expected. The store version
        // is set by the Admin in an async thread run shortly after the Admin
        // starts. Setting the store version at this point is unlikely to
        // affect the RN.
        // Also, if the store version is not set, the migration does not wait
        // but will fail (along with the plan).
        // Lastly, once set to version x the RN will reject a new
        // patameter with a version less than x unless restarted (which is a
        // separate issue).
        //
//        /*
//         * Since software version update runs at interval of 1 hours by
//         * default, we mock the updater thread here and set the store version
//         */
//        setStoreVersion(oldVer);
//        trace("Store version set to " + oldVer);

//        /* try expand store, it should fail */
//        final Plan.State st = expandStore();
//        assertEquals("Plan should stuck", Plan.State.RUNNING, st);
//
//        /* migration cannot complete */
//        final MigrationManager mm = rn.getMigrationManager();
//        IntStream.range(1, NUM_PARTS + 1)
//                 .forEach(i -> assertEquals(
//                     RepNodeAdmin.PartitionMigrationState.UNKNOWN,
//                     mm.getMigrationState(new PartitionId(i))
//                       .getPartitionMigrationState()));
//
//        final MigrationService ms = mm.getMigrationService();
//        assertEquals("Expect half partitions fail to move to new shard",
//                     NUM_PARTS / 2,
//                     ms.getRequestErrors());
    }

    @SuppressWarnings("unused")
    private void setStoreVersion(KVVersion storeVersion) throws Exception {
        final ParameterMap pm =
            new ParameterMap(ParameterState.GLOBAL_TYPE,
                             ParameterState.GLOBAL_TYPE);
        pm.setParameter(ParameterState.GP_STORE_VERSION,
                        storeVersion.getNumericVersionString());
        final CommandServiceAPI admin = createStore.getAdmin();
        final int planId =
            admin.createChangeGlobalComponentsParamsPlan(
                "UpdateGlobalVersionMetadata", pm);
        try {
            admin.approvePlan(planId);
            admin.executePlan(planId, false);
            admin.awaitPlan(planId, 10 * 1000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Encountered exception " +
                                      "running update store version plan.",
                       e);
            admin.cancelPlan(planId);
            throw e;
        }
    }

    @SuppressWarnings("unused")
    private Plan.State expandStore() throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        final String topo = "newTopo";
        cs.copyCurrentTopology(topo);
        cs.redistributeTopology(topo, "AllStorageNodes");
        trace("try to expand store to two shards");
        final int planId =
            cs.createDeployTopologyPlan("deploy-new-topo", topo, null);
        cs.approvePlan(planId);
        trace("Plan approved");
        cs.executePlan(planId, false);
        trace("Plan executed, will timeout in 30 seconds");
        return cs.awaitPlan(planId, 30, TimeUnit.SECONDS);
    }

    private void trace(String message) {
        trace(INFO, message);
    }

    private void trace(Level level, String message) {
        if (traceOnScreen) {
            if (logger.isLoggable(level)) {
                System.out.println(message);
            }
        }
        logger.log(level, message);
    }
}
