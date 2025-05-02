/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.client.admin;

import static org.junit.Assert.assertNotNull;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Tests for ClientAdminService.
 */
public class ClientAdminServiceTest extends TestBase {

    private static final int startPort = 5000;

    private CreateStore createStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (createStore != null) {
            createStore.shutdown(true);
            createStore = null;
        }
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Test that asking for the exception status of a newly created plan
     * doesn't run into a race condition where the plan history has not yet
     * been recorded. [#27012]
     */
    @Test
    public void testGetExecutionStatusRace()
        throws Exception {

        /* Create a small store */
        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      1, /* numSns */
                                      1, /* rf */
                                      2, /* numPartitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN,
                                      true /* useThreads */,
                                      null /* mgmtImp */);
        createStore.start();

        /*
         * Create a plan, but don't execute it, so that it will have a plan ID
         * but no plan history.
         */
        CommandServiceAPI cs = createStore.getAdmin();
        StorageNodeId snids[] = createStore.getStorageNodeIds();
        RepNodeId rnId = createStore.getRNs(snids[0]).get(0);
        ParameterMap map = new ParameterMap();
        RepNodeParams rnParams = new RepNodeParams(map);
        rnParams.setElectableGroupSizeOverride(1);
        int planId = cs.createChangeParamsPlan(null, rnId, map);

        /* Get client admin service */
        FindClientAdminService finder = new FindClientAdminService(
            cs.getTopology(), logger, createStore.getSNALoginManager(0),
            null /* clientId */,
            Protocols.get(createStore.createKVConfig()));
        ClientAdminServiceAPI service = finder.getDDLService();

        /* Attempt to get the execution status of the plan */
        assertNotNull(service.getExecutionStatus(planId));
    }
}
