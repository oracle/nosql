/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.backup;

import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.Snapshot;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.sna.StorageNodeAgent;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
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
import oracle.kv.impl.util.StoreUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Test a specific scenario for [#20701]
 * 1.  creates a 1x3 store
 * 2.  add data
 * 3.  snapshot
 * 4.  stop all RNs and remove their environments
 * 5.  set up recovery for one of the RNs and start it
 * 6.  start the other two RNs in the group and get network restore from JE HA
 * 7.  validate state at the end.
 */
public class RestoreTest extends TestBase {

    private CreateStore createStore;
    private static final int startPort = 5000;
    private static final int rf = 3;

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
    public void testRestoreRepGroup()
        throws Exception {

        final String snapName = "restoreSnap";

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
                    t, createStore.getSNALoginManager(snids[rnCount]), logger);
                snas[rnCount++] =
                    ru.getStorageNodeAgent(rn.getStorageNodeId());
            }
        }
        assertEquals(rf, rnCount);

        /**
         * Load some data and create a snapshot
         */
        StoreUtils su = new StoreUtils(createStore.getStoreName(),
             createStore.getHostname(), createStore.getRegistryPort(snids[0]),
             createStore.getDefaultUserLoginPath(),
             StoreUtils.RecordType.UUID, 0, null, null);

        List<Key> initialKeys = su.load(20);
        su.keysExist(initialKeys);
        CommandServiceAPI admin = createStore.getAdminMaster();
        Snapshot snapshot = new Snapshot(admin, false, null);
        String snap = snapshot.createSnapshot(snapName);

        /**
         * Stop the RepNodes and remove their environments.
         */
        for (int i = 0; i < rf; i++) {
            assertTrue(snas[i].stopRepNode(repNodeIds[i], false, false));
            File envDir = FileNames.getEnvDir
                (TestUtils.getTestDir().toString(),
                 t.getKVStoreName(),
                 repNodeParams[i].getStorageDirectoryFile(),
                 snids[i],
                 repNodeIds[i]);
            removeFiles(envDir);
        }

        /**
         * Set up the first RN for recovery:
         * - create the recovery directory
         * - move the snapshot to the recovery directory
         */
        File recoveryDir = FileNames.getRecoveryDir
                (TestUtils.getTestDir().toString(),
                 t.getKVStoreName(),
                 repNodeParams[0].getStorageDirectoryFile(),
                 snids[0],
                 repNodeIds[0]);
        File snapshotDir = FileNames.getSnapshotDir
                (TestUtils.getTestDir().toString(),
                 t.getKVStoreName(),
                 repNodeParams[0].getStorageDirectoryFile(),
                 snids[0],
                 repNodeIds[0]);
        File snapDir = new File(snapshotDir, snap);
        recoveryDir.mkdirs();
        snapDir.renameTo(new File(recoveryDir, snap));

        /**
         * Restart first RepNode
         */
        assertTrue(snas[0].startRepNode(repNodeIds[0]));

        /**
         * Restart the others
         */
        for (int i = 1; i < rf; i++) {
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
                                             40,
                                             target,
                                             logger);
        }
        su.keysExist(initialKeys);
    }

    @Test
    public void testRestoreFromSnapshot() throws Exception {
        final String snapName = "restoreSnap";

        createStore = new CreateStore(kvstoreName,
                                      startPort, rf, rf, 10, 1);
        createStore.start();

        StoreUtils su = new StoreUtils(createStore.getStoreName(),
             createStore.getHostname(), createStore.getRegistryPort(),
             createStore.getDefaultUserLoginPath(),
             StoreUtils.RecordType.UUID, 0, null, null);

        List<Key> initialKeys = su.load(20);
        su.keysExist(initialKeys);
        CommandServiceAPI admin = createStore.getAdminMaster();
        Snapshot snapshot = new Snapshot(admin, false, null);
        String snap = snapshot.createSnapshot(snapName);
        StorageNodeId [] snaIds = createStore.getStorageNodeIds();
        StorageNodeAgent [] snas = new StorageNodeAgent[snaIds.length];
        for (StorageNodeId id : snaIds) {
            snas[id.getStorageNodeId() - 1] =
                createStore.getStorageNodeAgent(id);
        }
        File [] envDirs = new File[snas.length];
        for (int i = 0; i < rf; i++) {
            final File rnDir = new File(
                snas[i].getKvConfigFile().getParent(),
                "rg1-rn" + (i + 1));
             envDirs[snas[i].getStorageNodeId().getStorageNodeId() - 1] =
                 new File(rnDir, "env");
        }
        File [] snaConfigs = new File[snas.length];
        for (int i = 0; i < rf; i++) {
            snaConfigs[snas[i].getStorageNodeId().getStorageNodeId() - 1] =
                snas[i].getKvConfigFile();
        }
        createStore.shutdown();

        for (File env : envDirs) {
            removeFiles(env);
        }

        for (File config : snaConfigs) {
            config.delete();
        }

        createStore.setRestoreSnapshot(snap);

        createStore.restart();

        su.keysExist(initialKeys);

        /* Clean up the auto restore after use */
        createStore.setRestoreSnapshot(null);
    }

    /**
     * Test that starting SNA with an invalid snapshot does not hang
     * if -update-config is false and throws proper exception message
     * for both -update-config true and false
     * [KVSTORE-1786]: Start with unknown snapshot hangs silently
     */
    @Test
    public void testRestoreFromInvalidSnapshot() throws Exception {
        final String snapName = "NO_SUCH_SNAPSHOT";

        createStore = new CreateStore(kvstoreName,
                                      startPort, rf, rf, 10, 1);
        createStore.start();

        createStore.shutdown();

        String pattern =
            "Failed to find snapshot directory: .*\\/snapshots\\/" + snapName;

        createStore.setRestoreSnapshot(snapName);
        createStore.setUpdateConfig("true");
        checkException(() -> createStore.restart(),
                       IllegalStateException.class,
                       pattern);

        createStore.setRestoreSnapshot(snapName);
        createStore.setUpdateConfig("false");
        checkException(() -> createStore.restart(),
                       IllegalStateException.class,
                       pattern);

        createStore.setRestoreSnapshot(snapName);
        createStore.setUpdateConfig(null);
        checkException(() -> createStore.restart(),
                       IllegalStateException.class,
                       pattern);

        pattern = "Flag -update-config requires boolean value";
        createStore.setRestoreSnapshot(snapName);
        createStore.setUpdateConfig("UNKNOWN");
        checkException(() -> createStore.restart(),
                       IllegalArgumentException.class,
                       pattern);
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
