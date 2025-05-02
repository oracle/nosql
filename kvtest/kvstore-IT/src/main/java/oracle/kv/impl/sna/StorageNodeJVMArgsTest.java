/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.sna;

import static oracle.kv.util.TestUtils.isRunning;
import static oracle.kv.util.TestUtils.isShutdown;
import static org.junit.Assert.assertTrue;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.VersionUtil;
import oracle.kv.util.CreateStore;

import org.junit.Assume;
import org.junit.Test;

public class StorageNodeJVMArgsTest extends TestBase {
    @Test
    public void testMainStartWithJVMExcludeArgs()
        throws Exception {

        int majorJavaVersion = VersionUtil.getJavaMajorVersion();
            /* skip the test for Java version < 21 */
        Assume.assumeFalse("Skipping test for java version < 21",
                               majorJavaVersion < 21);

        /*
         * Only run this test in process mode, because we need validate JVM
         * args at process start.
         */
        CreateStore createStore = new CreateStore(kvstoreName,
                                                  5000,
                                                  3, /* n SNs */
                                                  3, /* rf */
                                                  10, /* n partitions */
                                                  2, /* capacity per SN */
                                                  2 * CreateStore.MB_PER_SN,
                                                  false,
                                                  null);
        createStore.start();
        CommandServiceAPI cs = createStore.getAdmin();
        DatacenterId dcId =
            cs.getTopology().getDatacenterId(createStore
                                                 .getStorageNodeAgent(0)
                                                 .getStorageNodeId());

        /* Add JVM exclude arguments through params */
        changeJVMParameterAllRns(cs, dcId, ParameterState.JVM_MISC,
                                 "-XX:G1RSetRegionEntries " +
                                 "-XX:G1RSetSparseRegionEntries");

        /* Restart a repNode with JVM exclude argument config */
        StorageNodeId stopSNId = createStore.getStorageNodeIds()[1];
        RepNodeId stopRN = createStore.getRNs(stopSNId).get(0);
        StorageNodeAgent stopSN = createStore.getStorageNodeAgent(1);
        RepNodeAdminAPI targetNodeRna =
            createStore.getRepNodeAdmin(stopRN);
        assertTrue(stopSN.stopRepNode(stopRN, true));
        assertTrue(isShutdown(targetNodeRna));

        assertTrue(stopSN.startRepNode(stopRN));
        isRunning(createStore, stopSNId, stopRN,
                                          logger);

        createStore.shutdown(true);
    }

    private void changeJVMParameterAllRns(CommandServiceAPI cs,
                                                DatacenterId dcId,
                                                String paramName, String params)
        throws Exception {

        /* Set up a change-parameters plan to override JVM parameters
         * and execute plan */
        ParameterMap map = new ParameterMap();
        map.setParameter(paramName, params);
        int planId = cs.createChangeAllParamsPlan(
            "setJVMOverride", dcId, map);
        cs.approvePlan(planId);
        cs.executePlan(planId, false);
        cs.awaitPlan(planId, 0, null);
        cs.assertSuccess(planId);
    }
}
