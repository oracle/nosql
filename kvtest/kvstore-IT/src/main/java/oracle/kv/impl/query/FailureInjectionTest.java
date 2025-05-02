/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.query.runtime.server.TableScannerFactory;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.junit.Test;

/**
 * Query tests involves failure injection.
 */
public class FailureInjectionTest extends RepNodeTestBase {

    private KVRepTestConfig config;
    private KVStore kvs;

    private static final String USER = "User";
    private static final String USER_ID = "Id";
    private static final String USER_NAME = "Name";
    private static final TableImpl USER_TABLE =
        TableBuilder.createTableBuilder(USER)
        .addInteger(USER_ID)
        .addString(USER_NAME)
        .primaryKey(USER_ID)
        .shardKey(USER_ID)
        .buildTable();
    private static final String DELETE_SINGLE_USER_QUERY = String.format(
        "delete from %s where %s = 0 returning %s", USER, USER_ID, USER_NAME);

    @Override
    public void setUp() throws Exception {

        super.setUp();

        /*
         * This will create two RGs.
         * RG1 will start with partitions 1,3,5,7,9
         */
        config = new KVRepTestConfig(this,
                                     1, /* nDC */
                                     3, /* nSN */
                                     3, /* repFactor */
                                     10, /* nPartitions */
                                     0, /* nSecondaryZones */
                                     1 /* nShards */);

        /*
         * Individual tests need to start rep node services after setting
         * any test specific configuration parameters.
         */
    }

    @Override
    public void tearDown() throws Exception {

        config.stopRepNodeServices();
        config = null;
        if (kvs != null) {
            kvs.close();
        }
        super.tearDown();
    }

    /**
     * Prepares for a scan test. Create table and indices and put rows.
     */
    private void prepareTest() {
        config.startRepNodeServices();

        kvs = KVStoreFactory.getStore(config.getKVSConfig());

        createTable();
        populateRows(100);
    }

    private void createTable() {
        final RepNode master = config.getMaster(new RepGroupId(1), 100);
        final TableMetadata md = (TableMetadata) master
            .getMetadata(MetadataType.TABLE).getCopy();
        md.addTable(USER_TABLE.getInternalNamespace(),
                    USER_TABLE.getName(),
                    USER_TABLE.getParentName(),
                    USER_TABLE.getPrimaryKey(),
                    null, // primaryKeySizes
                    USER_TABLE.getShardKey(),
                    USER_TABLE.getFieldMap(),
                    null, // TTL
                    null, // limits
                    false, 0,
                    null, null);
        boolean success;
        success = master.updateMetadata(md);
        assertTrue(success);
    }

    private void populateRows(int numRows) {
        final TableAPI tableAPI = kvs.getTableAPI();
        final Table table = tableAPI.getTable(USER_TABLE.getFullName());
        for (int i = 0; i < numRows; ++i) {
            final Row row = table.createRow();
            row.put(USER_ID, i);
            row.put(USER_NAME, "name" + i);
            tableAPI.put(row, null, null);
        }
    }

    /**
     * Tests delete failed on the first attempt due to master transfer. The
     * delete will be forwarded to the new master. However, the modified state
     * during the first attempt should not affect the execution on the new
     * master.
     *
     * [KVSTORE-2351]
     */
    @Test
    public void testDeleteDuringMasterTransfer() throws Exception {
        prepareTest();
        logger.info("Prepared the test");
        final Semaphore mainSteps = new Semaphore(0);
        final Semaphore hookSteps =
            prepareDeleteDuringMasterTransferHook(mainSteps);

        final ExecutorService executor =
            Executors.newSingleThreadExecutor();
        final Future<RecordValue> f = executor.submit(() -> {
            StatementResult sr = kvs.executeSync(
                DELETE_SINGLE_USER_QUERY, null);
            for (RecordValue r : sr) {
                return r;
            }
            return null;
        });
        executor.shutdown();
        logger.info("Submitted query");
        /*
         * Release a permit so that the hook can proceed to release a permit for
         * us. This will make sure the hook is taking effect.
         */
        mainSteps.release();
        hookSteps.acquireUninterruptibly();
        /*
         * Hook is taking effect now. Let's do a master transfer.
         */
        logger.info("Transfer master");
        final RepNode master = config.getMaster(new RepGroupId(1), 100);
        master.getEnv(0)
            .transferMaster(config.getRNs().stream()
                .filter((rn) -> rn.getRepNodeId().getGroupId() == 1)
                .filter(
                    (rn) -> !rn.getRepNodeId().equals(master.getRepNodeId()))
                .map((rn) -> rn.getEnv(0).getNodeName())
                .collect(Collectors.toSet()), 1, TimeUnit.SECONDS);
        /*
         * Wait until master transfer is done (have to sleep a bit longer to
         * make sure the master transfer is really started) and then let hook to
         * proceed. Release enough permits for forwarding and retrying.
         */
        Thread.sleep(1000);
        config.getMaster(new RepGroupId(1), 1000);
        mainSteps.release(100);
        /*
         * Wait a bit for the query to finish.
         */
        executor.awaitTermination(10, TimeUnit.SECONDS);
        RecordValue rv = f.get();
        assertTrue(rv != null);
        logger.info(String.format("Obtained result: %s", rv));
        assertEquals(rv.get(USER_NAME).asString().get(), "name0");
    }

    private Semaphore
        prepareDeleteDuringMasterTransferHook(Semaphore mainSteps)
    {
        final Semaphore hookSteps = new Semaphore(0);
        final TestHook<TableScannerFactory.PrimaryTableScannerHookObject> hook =
            (obj) ->
            {
                final InternalOperation op = obj.op;
                final RepNodeId rnId = obj.handler.getRepNode().getRepNodeId();
                logger
                    .info(String.format("running %s on %s in hook", op, rnId));
                /*
                 * Await for the main thread to release the first permit. The
                 * main thread will then wait for us to release a permit.
                 */
                mainSteps.acquireUninterruptibly();
                /*
                 * Release a permit so that the main thread can start the master
                 * transfer.
                 */
                hookSteps.release();
                logger.info(String.format(
                    "waiting for master transfer while running %s on %s", op,
                    rnId));
                /*
                 * Wait again for the master transfer to happen.
                 */
                mainSteps.acquireUninterruptibly();
                logger
                    .info(String.format("continue running %s on %s", op, rnId));
            };
        TableScannerFactory.primaryTableScanHookObtainedNext = hook;
        tearDowns.add(
            () -> TableScannerFactory.primaryTableScanHookObtainedNext = null);
        return hookSteps;
    }
}
