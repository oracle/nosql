/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.TestBase;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test a couple of scenarios where Admin replicas may be down.
 * o start a store with replicated admin
 * o stop the admin replicas
 * o try some read-only and not-read-only operations
 * Read-only operations should succeed.
 *
 * TODO: do not-read-only operations.  They aren't done now because the retries
 * and timeouts make the test too long.
 */
public class AdminDownTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;
    private static final int rf = 3;
    private static final int numSns = 3;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        TestUtils.clearTestDirectory();
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
         * 3 Storage Nodes, RF of 3.
         */
        createStore = new CreateStore(kvstoreName,
                                      startPort, numSns, rf, 10, 1);
        createStore.start();

        killAdminReplicas();

        CommandServiceAPI cs = createStore.getAdmin();
        Topology t = cs.getTopology();
        assertNotNull(t);

        /**
         * This will retry and eventually timeout.
         */
        /* TODO: enable this with shorter timeouts.
          try {
          cs.setEventExpiryAge(1000L);
          fail("Operation should have timed out");
          } catch (Exception e) {
          }
        */
    }

    private void killAdminReplicas()
        throws Exception {

        StorageNodeId[] storeSnids = createStore.getStorageNodeIds();

        /* Count on the fact that the master is on the first SNA */
        CommandServiceAPI cs = createStore.getAdmin();
        AdminStatus adminStatus = cs.getAdminStatus();
        assertTrue(adminStatus.getIsAuthoritativeMaster());
        for (int i = 1; i < storeSnids.length; i++) {
            RegistryUtils ru = new RegistryUtils(
                cs.getTopology(),
                createStore.getSNALoginManager(storeSnids[i]), logger);
            StorageNodeAgentAPI snai =
                ru.getStorageNodeAgent(storeSnids[i]);
            assertNotNull(snai);
            assertTrue(snai.stopAdmin(false));
        }
    }
}
