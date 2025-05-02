/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.param;

import static org.junit.Assert.assertEquals;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.plan.task.WriteNewParams;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test changing parameters using the CreateStore test infrastructure.
 */
public class ChangeParamsCreateStoreTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;

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
        WriteNewParams.testEnableCheckRemoved = false;
    }

    /**
     * Test setting the RN electableGroupSizeOverride parameter to 1 and then
     * to the default value of 0 and make sure it really gets changed back to
     * 0. [#27513]
     */
    @Test
    public void testSetElectableGroupSizeOverride()
        throws Exception {

        /*
         * For now, the correct behavior only happens if we explicitly enable
         * checking for removed parameters, since the fix for [#27513] was only
         * applied in the failover case. This hook setting can be removed when
         * we solve the problem more generally. [#27527]
         */
        WriteNewParams.testEnableCheckRemoved = true;

        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      1, /* Storage Nodes */
                                      1, /* Replication Factor */
                                      300, /* Partitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN, /* memoryMB*/
                                      /*
                                       * Use threads so that we can set
                                       * testEnableCheckRemoved
                                       */
                                      true, /* useThreads */
                                      null); /* mgmtImpl */
        createStore.start();

        final CommandServiceAPI cs = createStore.getAdmin();
        final RepNodeId rnId = new RepNodeId(1, 1);
        Parameters params = cs.getParameters();
        RepNodeParams rnParams = params.get(rnId);

        rnParams.setElectableGroupSizeOverride(1);
        int pid = cs.createChangeParamsPlan(
            "change electable group size to 1", rnId, rnParams.getMap());
        cs.approvePlan(pid);
        cs.executePlan(pid, false);
        cs.awaitPlan(pid, 0, null);
        cs.assertSuccess(pid);

        params = cs.getParameters();
        rnParams = params.get(rnId);
        assertEquals(1, rnParams.getElectableGroupSizeOverride());

        rnParams.setElectableGroupSizeOverride(0);
        pid = cs.createChangeParamsPlan(
            "change electable group size to 0", rnId, rnParams.getMap());
        cs.approvePlan(pid);
        cs.executePlan(pid, false);
        cs.awaitPlan(pid, 0, null);
        cs.assertSuccess(pid);

        params = cs.getParameters();
        rnParams = params.get(rnId);
        assertEquals(0, rnParams.getElectableGroupSizeOverride());
    }
}
