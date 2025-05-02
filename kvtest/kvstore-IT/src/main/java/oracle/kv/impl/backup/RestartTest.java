/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.backup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.test.RemoteTestAPI;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupMap;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.ServiceUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test situations where nodes go away and possibly come back and how
 * the system deals with that.
 */
public class RestartTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;
    private static final int rf = 3;

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

    /**
     * Test a RepGroup restart scenario:
     * o stop all nodes in the RG
     * o start all but one
     * o remove the environment from the final one
     * o restart the final one.
     * This tests a situation that was detected and fixed in SR [#20711].
     */
    @Test
    public void testRestartRepGroup()
        throws Exception {

        /**
         * 3 Storage Nodes, RF of 3.
         */
        createStore = new CreateStore(kvstoreName,
                                      startPort, rf, rf, 10, 1);
        createStore.start();

        CommandServiceAPI cs = createStore.getAdmin();
        Topology t = cs.getTopology();
        Parameters params = cs.getParameters();

        /* Check the cache size setting of the one RepNode in this kvstore */
        RepGroupMap groupMap = t.getRepGroupMap();
        int rnCount = 0;
        RepNodeId repNodeIds[] = new RepNodeId[rf];
        RepNodeParams repNodeParams[] = new RepNodeParams[rf];
        StorageNodeAgentAPI snas[] = new StorageNodeAgentAPI[rf];
        StorageNodeId snids[] = new StorageNodeId[rf];
        for (RepGroup rg : groupMap.getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                RepNodeId rid = rn.getResourceId();
                repNodeIds[rnCount] = rid;
                repNodeParams[rnCount] = params.get(rid);
                snids[rnCount] = rn.getStorageNodeId();
                RegistryUtils ru = new RegistryUtils(
                    t, createStore.getSNALoginManager(rn.getStorageNodeId()),
                    logger);
                snas[rnCount++] =
                    ru.getStorageNodeAgent(rn.getStorageNodeId());
            }
        }
        assertEquals(rf, rnCount);

        /**
         * Stop the RepNodes.
         */
        for (int i = 0; i < rf; i++) {
            assertTrue(snas[i].stopRepNode(repNodeIds[i], false, false));
        }

        /**
         * Remove the environment from the 3rd
         */
        File envDir = FileNames.getEnvDir(TestUtils.getTestDir().toString(),
                                          t.getKVStoreName(),
                                          repNodeParams[rf-1].
                                                  getStorageDirectoryFile(),
                                          snids[rf-1],
                                          repNodeIds[rf-1]);
        removeFiles(envDir);

        /**
         * Before restarting them all, test a case where a not-yet fully
         * started repnode is stopped.  Do this by starting one, allowing it to
         * hang on an election, then stop it.
        assertTrue(snas[0].startRepNode(repNodeIds[0]));
        ServiceStatus[] target = {ServiceStatus.STARTING};
        ServiceUtils.waitForRepNodeAdmin(createStore.getStoreName(),
                                         createStore.getHostname(),
                                         createStore.getRegistryPort(snids[0]),
                                         repNodeIds[0],
                                         NULL_LOGIN_MGR,
                                         5,
                                         target);
        boolean foo = snas[0].stopRepNode(repNodeIds[0], false);
         */

        /**
         * Restart RepNodes.
         */
        for (int i = 0; i < rf; i++) {
            assertTrue(snas[i].startRepNode(repNodeIds[i]));
        }
        ServiceStatus[] target = {ServiceStatus.RUNNING};
        for (int i = 0; i < rf; i++) {
            ServiceUtils.waitForRepNodeAdmin(createStore.getStoreName(),
                                             createStore.getHostname(),
                                             createStore.getRegistryPort(snids[i]),
                                             repNodeIds[i],
                                             snids[i],
                                             createStore.
                                                 getSNALoginManager(snids[i]),
                                             20,
                                             target,
                                             logger);
        }
    }

    /**
     * Create a 2x3 store and stop a SN or two and perform snapshot operations.
     * Also test a situation from [#20765] where a RN is restarted.
     */
    @Test
    public void testBadStorageNodes()
        throws Exception {

        createStore = new CreateStore(kvstoreName,
                                      startPort, rf * 2, rf, 10, 1);
        createStore.start();

        CommandServiceAPI cs = createStore.getAdmin();
        Topology t = cs.getTopology();
        logger.info(TopologyPrinter.printTopology(t));

        /**
         * Do a snapshot -- will succeed.  This also causes the SNAs to cache
         * handles for their services.
         */
        Snapshot snap = new Snapshot(cs, false, null);
        snap.createSnapshot("snap");
        assertTrue(snap.succeeded());

        /**
         * Kill one RN in each RG, remember the ids.  They will be restarted by
         * the SNA
         */
        RepGroupMap groupMap = t.getRepGroupMap();
        StorageNodeId snids[] = new StorageNodeId[2];
        RepNodeId rnids[] = new RepNodeId[2];
        int i = 0;
        for (RepGroup rg : groupMap.getAll()) {
            for (RepNode rn : rg.getRepNodes()) {
                snids[i] = rn.getStorageNodeId();
                rnids[i] = rn.getResourceId();

                RegistryUtils ru = new RegistryUtils(
                    t, createStore.getSNALoginManager(snids[i]), logger);
                RemoteTestAPI rti = ru.getRepNodeTest(rnids[i++]);
                try {
                    rti.processExit(true /* restart */);
                    fail("This call fails because there's no RMI reply");
                } catch (Exception ignored) {
                }
                break;
            }
        }

        /**
         * Give the RNs a couple of seconds to come back.
         */
        Thread.sleep(2000);
        ServiceStatus[] target = {ServiceStatus.RUNNING};
        for (i = 0; i < 2; i++) {
            ServiceUtils.waitForRepNodeAdmin(createStore.getStoreName(),
                                             createStore.getHostname(),
                                             createStore.getRegistryPort(snids[i]),
                                             rnids[i],
                                             snids[i],
                                             createStore.
                                                 getSNALoginManager(snids[i]),
                                             20,
                                             target,
                                             logger);
        }

        /**
         * Try some snapshot operations with the restarted RNs [#20765]
         */
        snap = new Snapshot(cs, false, null);
        snap.createSnapshot("snapRestart");
        assertTrue(snap.succeeded());

        /**
         * This time shut down the SNAs themselves to test listing of snapshots
         */
        for (i = 0; i < snids.length; i++) {
            RegistryUtils ru = new RegistryUtils(
                t, createStore.getSNALoginManager(snids[i]), logger);
            StorageNodeAgentAPI snai =
                ru.getStorageNodeAgent(snids[i]);
            snai.shutdown(true, false);
        }

        /**
         * Refresh the cs and try some snapshot operations
         */
        cs = createStore.getAdminMaster();
        assertNotNull(cs);
        snap = new Snapshot(cs, false, null);
        snap.createSnapshot("snapQuorum");
        List<Snapshot.SnapResult> fail = snap.getFailures();

        /**
         * 2 or more failures -- 2 RNs and possibly some admins
         */
        int expectedFailures = 2; /* 2 RNs should be missing snapshots.*/
        for (i = 0; i < snids.length; i++) {
            if (createStore.hasAdmin(snids[i])) {
                expectedFailures++;
            }
        }
        assertEquals(fail.toString(), expectedFailures, fail.size());

        /**
         * Make sure that the basic list works but also make sure that listing
         * against specific storage nodes succeeds for nodes that are up and
         * fails against the 2 down nodes.
         */
        String[] snaps = snap.listSnapshots();
        assertTrue(snaps.length == 3);
        StorageNodeId[] storeSnids = createStore.getStorageNodeIds();
        for (int j = 0; j < storeSnids.length; j++) {
            try {
                snaps = snap.listSnapshots(storeSnids[j]);
                assertTrue(!storeSnids[j].equals(snids[0]) &&
                           !storeSnids[j].equals(snids[1]));
            } catch (Exception e) {
                assertTrue(storeSnids[j].equals(snids[0]) ||
                           storeSnids[j].equals(snids[1]));
            }
        }
    }

    private void removeFiles(File target) {
        if (target.isDirectory()) {
            for (File f : target.listFiles()) {
                assertTrue(f.delete());
            }
        }
        assertTrue(target.delete());
    }
}
