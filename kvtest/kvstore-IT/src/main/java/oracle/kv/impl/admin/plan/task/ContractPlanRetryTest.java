/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.plan.task;

import static oracle.kv.util.TestUtils.checkException;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.util.CreateStore;

import org.junit.Test;


/**
 * [KVSTORE-1844] NPE in RemoveRepNode.removeRepNodeJob
 */
public class ContractPlanRetryTest extends TestBase {

    private static final int startPort = 5000;
    private static final int haRange = 5;

    private CreateStore createStore = null;

    @Override
    public void tearDown() throws Exception {

        if (createStore != null) {
            createStore.shutdown(true);
        }
    }

    private void createStore(
        int nsns,
        int rf,
        int capacity,
        int partitions) throws Exception {

        final String storeName = "kvstore";
        final int port =
            (new PortFinder(startPort, haRange)).getRegistryPort();
        createStore =
            new CreateStore(
                storeName, port,
                nsns, /* nsns */
                rf, /* rf */
                partitions,
                capacity,
                2, /* mb */
                true, /* useThreads */
                null);
        createStore.start();
    }

    @Test
    public void testContractPlanRetry() throws Exception {

        logger.fine(() -> "Creating store");
        createStore(6 /*nsns*/, 3 /*rf*/, 1 /*capacity*/, 10 /*partitions*/);

        logger.fine(() -> "Contracting store");
        final CommandServiceAPI cs = createStore.getAdmin();
        final String poolname = CreateStore.STORAGE_NODE_POOL_NAME;

        for (int i = 0; i < 3; ++i) {
            cs.removeStorageNodeFromPool(poolname, new StorageNodeId(i + 4));
        }

        String contractTopoName = "contractTopo";
        cs.copyCurrentTopology(contractTopoName);
        cs.contractTopology(contractTopoName, poolname);

        RemoveShard.REMOVESHARD_FAULT_HOOK = (v) -> {
            throw new IllegalStateException();
        };

        int planId = cs.createDeployTopologyPlan(
            "deploy contraction", contractTopoName, null);
        runPlan(planId, true);

        RemoveShard.REMOVESHARD_FAULT_HOOK = null;

        runPlan(planId, false);

        logger.fine(() -> "Contracted store");
    }

    private void runPlan(int planId, boolean isException) throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);

        if (isException) {
            final String pattern =
                "Plan [0-9]+ \\[deploy contraction\\] ended with ERROR: " +
                "[0-9]+\\/RemoveShard rg2 failed";
            checkException(() -> cs.assertSuccess(planId),
                           AdminFaultException.class,
                           pattern);
        } else {
            cs.assertSuccess(planId);
        }
    }
}
