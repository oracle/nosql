/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.CreateStore;

import org.junit.Test;

public class RepNodeDiskLimitExceptionTest extends TestBase {

    private static final int numSNs = 3;
    private static final Random rand = new Random();
    private CreateStore createStore = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (createStore != null) {
            createStore.shutdown(false);
        }
    }

    @Test(timeout=480000)
    public void testRequests() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      numSNs, /* Storage Nodes */
                                      3, /* Replication Factor */
                                      3, /* Partitions */
                                      1  /* capacity */);
        createStore.start();
        final int diskCapacity = 1024 * 1024;
        changeRNDirSizeAndCheckInterval(
                new RepNodeId(1, 1), Integer.toString(diskCapacity));
        changeRNDirSizeAndCheckInterval(
                new RepNodeId(1, 2), Integer.toString(diskCapacity));
        changeRNDirSizeAndCheckInterval(
                new RepNodeId(1, 3), Integer.toString(diskCapacity));
        verifyAllRNRunning();

        /* Write until we reach disk limit */
        final int maxId = writeDataUntilException();

        /* Read some data and they should all succeed */
        verifyReadCanSucceed(maxId);

        /* Restart all the nodes */
        restartNode(new RepNodeId(1, 1));
        restartNode(new RepNodeId(1, 2));
        restartNode(new RepNodeId(1, 3));

        /* The restarted nodes should be running */
        verifyAllRNRunning();

        /* Read some data and they should all succeed */
        verifyReadCanSucceed(maxId);
    }

    private void changeRNDirSizeAndCheckInterval(RepNodeId rnId, String size)
        throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();
        ParameterMap map = cs.getParameters().get(rnId).getMap();
        map.setParameter(ParameterState.RN_MOUNT_POINT_SIZE, size);
        map.setParameter(ParameterState.JE_MISC,
                "je.cleaner.bytesInterval=1000;");
        int planId = cs.createChangeParamsPlan(
                "change dir size for " + rnId, rnId, map);
        runPlanAndVerify(
                planId, false /* not force */, true /* suceed */);
    }

    private void runPlanAndVerify(int planId,
                                  boolean force,
                                  boolean shouldSucceed) throws Exception {

        CommandServiceAPI cs = createStore.getAdmin();
        String name = cs.getPlanById(planId).getName();
        cs.approvePlan(planId);
        try {
            cs.executePlan(planId, force);
            cs.awaitPlan(planId, 0, null);
        } catch (Throwable t) {
            if (!shouldSucceed) {
                return;
            }
        }
        if (shouldSucceed) {
            cs.assertSuccess(planId);
        } else {
            fail(name + " should fail");
        }
    }

    private void verifyAllRNRunning() throws Exception {
        Set<RepNodeId> nodes = new HashSet<RepNodeId>();
        for (StorageNodeId snId : createStore.getStorageNodeIds()) {
            nodes.addAll(createStore.getRNs(snId));
        }
        RegistryUtils regUtils = new RegistryUtils(
                createStore.getAdmin().getTopology(),
                createStore.getAdminLoginManager(),
                logger);
        for (RepNodeId rnId : nodes) {
            try {
                assertEquals(
                        regUtils.getRepNodeAdmin(rnId).
                        ping().getServiceStatus(),
                        ServiceStatus.RUNNING);
            } catch (Exception e) {
                fail(String.format("%s not stopped.", rnId));
            }
        }
    }

    private int writeDataUntilException() {

        final String[] kvhosts = {
            String.format("%s:%s",
                createStore.getHostname(),
                createStore.getRegistryPort(new StorageNodeId(1))) };
        final KVStoreConfig kvconfig = new KVStoreConfig(kvstoreName, kvhosts);
        final KVStore kvstore = KVStoreFactory.getStore(kvconfig);
        final TableAPI tableAPI = kvstore.getTableAPI();
        final String tableName = "table";
        kvstore.executeSync(
                "CREATE TABLE IF NOT EXISTS " + tableName + " " +
                "(name STRING, " +
                " id INTEGER, " +
                " PRIMARY KEY (id))");
        Table table = tableAPI.getTable(tableName);
        while (table == null) {
            try {
                Thread.sleep(3 * 1000);
            } catch (InterruptedException e) {
                fail("Interrupted in waiting");
            }
            table = tableAPI.getTable(tableName);
        }
        int id = 0;
        while (true) {
            try {
                Row row = table.createRow();
                row.put("id", id);
                row.put("name", "name" + id);
                tableAPI.put(row, null, null);
                id ++;
            } catch (FaultException e) {
                break;
            }
        }
        return id;
    }

    private void verifyReadCanSucceed(int maxId) {
        final String[] kvhosts = {
            String.format("%s:%s",
                createStore.getHostname(),
                createStore.getRegistryPort(new StorageNodeId(1))) };
        final KVStoreConfig kvconfig = new KVStoreConfig(kvstoreName, kvhosts);
        final KVStore kvstore = KVStoreFactory.getStore(kvconfig);
        final TableAPI tableAPI = kvstore.getTableAPI();
        final String tableName = "table";
        final Table table = tableAPI.getTable(tableName);

        final int cntReads = 100;
        for (int i = 0; i < cntReads; ++i) {
            final int id = rand.nextInt(maxId);
            PrimaryKey key = table.createPrimaryKey();
            key.put("id", id);
            @SuppressWarnings("unused")
            Row row = tableAPI.get(
                    key, new ReadOptions(null, 0, null));
        }
    }

    private void restartNode(RepNodeId rnId) throws Exception {
        final Set<RepNodeId> nodes =
            new HashSet<RepNodeId>(Arrays.asList(rnId));
        int planId;
        planId = createStore.getAdmin().
            createStopServicesPlan("stop " + rnId, nodes);
        runPlanAndVerify(planId, true, true);
        planId = createStore.getAdmin().
            createStartServicesPlan("start " + rnId, nodes);
        runPlanAndVerify(planId, true, true);
    }

}

