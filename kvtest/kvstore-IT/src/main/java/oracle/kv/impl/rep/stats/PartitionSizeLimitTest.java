/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.stats;

import static oracle.kv.impl.api.table.TableLimits.NO_LIMIT;
import static oracle.kv.impl.param.ParameterState.RN_SG_ENABLED;
import static oracle.kv.impl.param.ParameterState.RN_SG_INCLUDE_STORAGE_SIZE;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import oracle.kv.ExecutionFuture;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.TablePartitionSizeLimitException;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.table.ResourceCollector;
import oracle.kv.impl.rep.table.ResourceCollector.TopCollector;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.SpeedyTTLTime;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.sklogger.ScheduleStart;

/**
 * Unit test for partition limits. This test is in the stats package because
 * it depends on the intermediate table size update mechanism.
 */
public class PartitionSizeLimitTest extends TestBase {

    /*
     * This test needs a large number of partitions since the per-partition
     * limit is a percent of the table size limit divided by the number of
     * partitions. The min table limit is 1G so dividing by 100 makes the
     * per-partition limit manageable.
     */
    private final static int N_PART = 100;

    private final static int MB = 1024 * 1024;

    private final static int TABLE_SIZE_LIMIT_GB = 2;

    private final static int SIZE_PERCENT = 2;

    /* The actual per-partition table limit */
    private final static int PARTITION_SIZE_LIMIT_MB =
                TopCollector.getPartitionSizeLimitMB(TABLE_SIZE_LIMIT_GB,
                                                     SIZE_PERCENT,
                                                     N_PART);

    private final static long SCAN_INTERVAL_MS = 60*1000;

    private final static int SEED = 1235;
    private final Random rnd = new Random(SEED);

    /*
     * Shard keys belonging to different partitions. Note that the keys
     * for the alternate partition were selected by testing. If the number
     * of partitions change (N_PART) the key mapping may not result in a
     * different partition and this will fail.
     */
    private final static int SHARD_KEY_A = 1235;
    private final static int SHARD_KEY_B = 2;
    private final static boolean SUB_A = true;
    private final static boolean SUB_B = false;

    private CreateStore createStore = null;
    private static final String namespace = "StatsNS";

    private final TableImpl userTable;
    private final TableImpl childTable;

    /* Create a 10k record */
    private final static int RECORD_SIZE = 1024*10;
    private final static byte[] DATA = new byte[RECORD_SIZE];

    /*
     * The intermediate size update has thresholds for updates. So we need
     * adjust record counts so that the thresholds are meet.
     */
    private final static int POS_FUDGE =
      (int)(IntermediateTableSizeUpdate.POSITIVE_THRESHOLD_BYTES / RECORD_SIZE);

    public PartitionSizeLimitTest() {
        userTable = TableBuilder.createTableBuilder(namespace, "User")
                                .addInteger("id")
                                .addInteger("subId")
                                .addBinary("data")
                                .primaryKey("id", "subId")
                                .shardKey("id")
                                .buildTable();
        childTable = TableBuilder.createTableBuilder("Child", "child table",
                                                     userTable)
                                 .addInteger("income")
                                 .addBinary("childdata")
                                 .primaryKey("income")
                                 .buildTable();
    }

    @Override
    public void tearDown() throws Exception {
        if (createStore != null) {
            createStore.shutdown(false);
        }
        super.tearDown();
        KeyStatsCollector.foundUserTablesTestHook = null;
    }

    /**
     * The basic test flow is that the test waits for a full partition scan
     * and then runs a test case. We know when this happens because the stats
     * scan uses ScheduleStart. Once a full scan is completed, the test will
     * perform some table operations and then reads the updated size stats.
     * It then will then wait for the next scan, re-read the size stats and
     * compare.
     */
    @Test
    public void testNonChildTablePartitionSizeLimit() throws Exception {
        /* Start a single node KVStore */
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      N_PART, /* Partitions */
                                      1, /* capacity */
                                      3 * CreateStore.MB_PER_SN,
                                      true /* useThreads */,
                                      null);
        setPolicies(createStore);
        createStore.start();
        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);
        final TableAPI api = store.getTableAPI();

        /* Prepare to wait for the stats thread to notice the user table */
        final CompletableFuture<Void> foundUserTables =
            new CompletableFuture<>();
        KeyStatsCollector.foundUserTablesTestHook =
            v -> foundUserTables.complete(null);

        final CommandServiceAPI cliAdmin = createStore.getAdmin();
        createTable(userTable, cliAdmin);

        foundUserTables.get(120, TimeUnit.SECONDS);

        /* Write records, all in the same partition (same shard key) */
        int nRecords = getNRecords(0L);
        runOperation(nRecords, (id) -> put(id, api, SHARD_KEY_A, SUB_A));

        /*
         * Wait until after the scan period to start tests. The partition size
         * limit mechanism relies on the intermediate size update. That in
         * turn needs to have stats records present for the table. Hence the
         * wait for a scan to happen after the table is created.
         */
        waitToNextScan();

        long size = getTableSize(api, userTable);
        long sizeMB = size / MB;
        assertTrue("Table is not big enough, needs to be over " +
                   PARTITION_SIZE_LIMIT_MB + " MB, was " + sizeMB,
                   sizeMB > PARTITION_SIZE_LIMIT_MB);

        /* Enable partition size check */
        setDefaultPercent(SIZE_PERCENT, cliAdmin);

        try {
            /* Writes to that partition should eventually fail */
            runOperation(6, 100, (id) -> put(id, api, SHARD_KEY_A, SUB_B));
            fail("Expected TablePartitionSizeLimitException");
        } catch (TablePartitionSizeLimitException expected) {}

        /*  Writes to a different partition should work. */
        runOperation(6, 100, (id) -> put(id, api, SHARD_KEY_B, SUB_A));
        /* Remove the records so that the size calc is for the one partition */
        runOperation(600, (id) -> delete(id, api, SHARD_KEY_B, SUB_A));

        waitMin();

        /*
         * Delete enough records to get below the partition limit. Deletes
         * should be allowed even though there is a write limit.
         */
        size = getTableSize(api, userTable);
        nRecords = getNRecords(size);
        runOperation(nRecords, (id) -> delete(id, api, SHARD_KEY_A, SUB_A));

        waitMin();
        size = getTableSize(api, userTable);
        sizeMB = size / MB;
        assertTrue("Table is not under the limit of " +
                   PARTITION_SIZE_LIMIT_MB + " MB, was " + sizeMB,
                   sizeMB < PARTITION_SIZE_LIMIT_MB);

        /* Writes should work again */
        runOperation(200, (id) -> put(id, api, SHARD_KEY_A, SUB_A));

        /* Write until over the limit */
        try {
            runOperation(15, 100, (id) -> put(id, api, SHARD_KEY_A, SUB_A));
            fail("Expected TablePartitionSizeLimitException");
        } catch (TablePartitionSizeLimitException expected) {}

        waitMin();

        /* More deletes to get under the limit again */
        size = getTableSize(api, userTable);
        nRecords = getNRecords(size);
        runOperation(nRecords, (id) -> delete(id, api, SHARD_KEY_A, SUB_A));

        waitMin();

        /* Now add a child table */
        createTable(childTable, cliAdmin);

        /* Should be able to write including via a child table */
        runOperation(200, (id) -> put(id, api, SHARD_KEY_A, SUB_A));
        runOperation(200, (id) -> putChild(id, api, SUB_A));

        /* Write via the child table until over the limit */
        try {
            runOperation(15, 1000, (id) -> putChild(id, api, SUB_A));
            fail("Expected TablePartitionSizeLimitException");
        } catch (TablePartitionSizeLimitException expected) {}

        /* Disable partition size check */
        setDefaultPercent(0, cliAdmin);

        /* Should be back in business */
        runOperation(6, 100, (id) -> put(id, api, SHARD_KEY_A, SUB_A));
    }

    /**
     * The basic test flow is that the test waits for a full partition scan
     * and then runs a test case. We know when this happens because the stats
     * scan uses ScheduleStart. Once a full scan is completed, the test will
     * perform some table operations and then reads the updated size stats.
     * It then will then wait for the next scan, re-read the size stats and
     * compare.
     */
    @Test
    public void testHierarchyTablePartitionSizeLimit() throws Exception {
        /* Start a single node KVStore */
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      N_PART, /* Partitions */
                                      1, /* capacity */
                                      3 * CreateStore.MB_PER_SN,
                                      true /* useThreads */,
                                      null);
        setPolicies(createStore);
        createStore.start();
        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);
        final TableAPI api = store.getTableAPI();

        /* Prepare to wait for the stats thread to notice the user table */
        final CompletableFuture<Void> foundUserTables =
            new CompletableFuture<>();
        KeyStatsCollector.foundUserTablesTestHook =
            v -> foundUserTables.complete(null);

        final CommandServiceAPI cliAdmin = createStore.getAdmin();
        createTable(userTable, cliAdmin);
        createTable(childTable, cliAdmin);

        foundUserTables.get(120, TimeUnit.SECONDS);

        /* Write records, all in the same partition (same shard key) */
        int nRecords = getNRecords(0L);
        runOperation(nRecords, (id) -> put(id, api, SHARD_KEY_A, SUB_A));

        /*
         * Wait until after the scan period to start tests. The partition size
         * limit mechanism relies on the intermediate size update. That in
         * turn needs to have stats records present for the table. Hence the
         * wait for a scan to happen after the table is created.
         */
        waitToNextScan();

        long size = getTableSize(api, userTable);
        long sizeMB = size / MB;
        assertTrue("Table is not big enough, needs to be over " +
                   PARTITION_SIZE_LIMIT_MB + " MB, was " + sizeMB,
                   sizeMB > PARTITION_SIZE_LIMIT_MB);

        /* Enable partition size check */
        setDefaultPercent(SIZE_PERCENT, cliAdmin);

        try {
            /* Writes to that partition should eventually fail */
            runOperation(6, 100, (id) -> put(id, api, SHARD_KEY_A, SUB_B));
            fail("Expected TablePartitionSizeLimitException");
        } catch (TablePartitionSizeLimitException expected) {}

        /*  Writes to a different partition should work. */
        runOperation(6, 100, (id) -> put(id, api, SHARD_KEY_B, SUB_A));
        /* Remove the records so that the size calc is for the one partition */
        runOperation(600, (id) -> delete(id, api, SHARD_KEY_B, SUB_A));

        waitMin();

        /*
         * Delete enough records to get below the partition limit. Deletes
         * should be allowed even though there is a write limit.
         */
        size = getTableSize(api, userTable);
        nRecords = getNRecords(size);
        runOperation(nRecords, (id) -> delete(id, api, SHARD_KEY_A, SUB_A));

        waitMin();
        size = getTableSize(api, userTable);
        sizeMB = size / MB;
        assertTrue("Table is not under the limit of " +
                   PARTITION_SIZE_LIMIT_MB + " MB, was " + sizeMB,
                   sizeMB < PARTITION_SIZE_LIMIT_MB);

        /* Writes should work again */
        runOperation(200, (id) -> put(id, api, SHARD_KEY_A, SUB_A));

        /* Write until over the limit */
        try {
            runOperation(15, 100, (id) -> put(id, api, SHARD_KEY_A, SUB_A));
            fail("Expected TablePartitionSizeLimitException");
        } catch (TablePartitionSizeLimitException expected) {}

        waitMin();

        /* More deletes to get under the limit again */
        size = getTableSize(api, userTable);
        nRecords = getNRecords(size);
        runOperation(nRecords, (id) -> delete(id, api, SHARD_KEY_A, SUB_A));

        waitMin();

        /* Should be able to write including via a child table */
        runOperation(200, (id) -> put(id, api, SHARD_KEY_A, SUB_A));
        runOperation(200, (id) -> putChild(id, api, SUB_A));

        /* Write via the child table until over the limit */
        try {
            runOperation(15, 1000, (id) -> putChild(id, api, SUB_A));
            fail("Expected TablePartitionSizeLimitException");
        } catch (TablePartitionSizeLimitException expected) {}

        /* Disable partition size check */
        setDefaultPercent(0, cliAdmin);

        /* Should be back in business */
        runOperation(6, 100, (id) -> put(id, api, SHARD_KEY_A, SUB_A));
    }

    private Void put(int id, TableAPI api,int shardKey, boolean makeAorB) {
        api.put(makeRow(id, api, shardKey, makeAorB), null, null);
        return null;
    }

    private Void putChild(int id, TableAPI api, boolean putAorB) {
        api.put(makeChildRow(id, api, putAorB), null, null);
        return null;
    }

    private Void delete(int id, TableAPI api, int shardKey, boolean makeAorB) {
        api.delete(makeKey(id, api, shardKey, makeAorB), null, null);
        return null;
    }

    private PrimaryKey makeKey(int id, TableAPI api, int shardKey, boolean makeAorB) {
        final PrimaryKey key =
                    api.getTable(userTable.getInternalNamespace(),
                                 userTable.getFullName()).createPrimaryKey();
        fillInKey(key, id, shardKey, makeAorB);
        return key;
    }

    private void fillInKey(Row row, int id, int shardKey, boolean makeAorB) {
        int subId = makeAorB ? id : Integer.MAX_VALUE - id;
        row.put("id", shardKey);
        row.put("subId", subId);
    }

    private Row makeRow(int id, TableAPI api, int shardKey, boolean makeAorB) {
        final Row row = api.getTable(userTable.getInternalNamespace(),
                                     userTable.getFullName()).createRow();
        fillInKey(row, id, shardKey, makeAorB);
        row.put("data", DATA);
        return row;
    }

    private Row makeChildRow(int id, TableAPI api, boolean makeAorB) {
        final Row row = api.getTable(childTable.getInternalNamespace(),
                                     childTable.getFullName()).createRow();
        fillInKey(row, id, SHARD_KEY_A, makeAorB);
        row.put("income", id);
        row.put("childdata", DATA);
        return row;
    }

    /*
     * Gets the number of record needed to go over (or under) the
     * partition size limit from the specified table size.
     */
    private int getNRecords(long size) {
        return (int)Math.abs((size - (PARTITION_SIZE_LIMIT_MB * MB)) /
                             RECORD_SIZE) +
                    POS_FUDGE;
    }

    /*
     * Runs the specified operation nOps times.
     */
    private void runOperation(int nOps, Function<Integer, Void> op)
            throws InterruptedException {
        runOperation(1, nOps, op);
    }

    /*
     * Runs the specified operation nOps times every 2 seconds for opSec.
     */
    private void runOperation(int opSec,
                              int nOps,
                              Function<Integer, Void> op)
            throws InterruptedException {

        /* Reset so that we generate the same key order */
        rnd.setSeed(SEED);

        int j = opSec;
        while (j > 0) {
            for (int i = 0; i < nOps; i++) {
                op.apply(rnd.nextInt());
            }
            if (j <= 0) {
                break;
            }
            j--;
            Thread.sleep(2*1000);
        }
    }

    private long nextScanMS(long now) {
        return ScheduleStart.calculateDelay(SCAN_INTERVAL_MS, now);
    }

    /*
     * Wait until past the next scan period.
     */
    private void waitToNextScan() throws InterruptedException {
        Thread.sleep(nextScanMS(System.currentTimeMillis()) + 5*1000);
    }

    private void waitMin() throws InterruptedException {
        Thread.sleep(KeyStatsCollector.MIN_SIZE_UPDATE_INTERVAL_MS);
    }

    /*
     * Scan the stats table to get the total size of the test table.
     */
    private long getTableSize(TableAPI tableAPI, TableImpl table) {
        final PrimaryKey pk =
                tableAPI.getTable(TableStatsPartitionDesc.TABLE_NAME).
                                                            createPrimaryKey();
        pk.put(TableStatsPartitionDesc.COL_NAME_TABLE_NAME,
               table.getFullNamespaceName());

        long size = 0L;
        final TableIterator<Row> iter = tableAPI.tableIterator(pk, null, null);
        while (iter.hasNext()) {
            Row row = iter.next();
            long pSize = row.get(TableStatsPartitionDesc.COL_NAME_TABLE_SIZE)
                                                                .asLong().get();
            size += pSize;
        }
        return size;
    }

    private void createTable(TableImpl table, CommandServiceAPI cliAdmin)
            throws RemoteException {
        /*
         * Note that we set size limit so that the intermediate table size
         * tracking happens.
         */
        int planId = cliAdmin.createAddTablePlan(
                         "AddTable",
                         table.getNamespace(),
                         table.getName(),
                         table.isTop() ? null : table.getParent().getFullName(),
                         table.getFieldMap(),
                         table.getPrimaryKey(),
                         table.getPrimaryKeySizes(),
                         table.getShardKey(),
                         null, // ttl
                         table.isTop() ?
                                    new TableLimits(NO_LIMIT, NO_LIMIT,
                                                    TABLE_SIZE_LIMIT_GB) :
                                    null,
                         table.isR2compatible(),
                         table.getSchemaId(),
                         table.isJsonCollection(),
                         table.getJsonCollectionMRCounters(),
                         null);

        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);
        cliAdmin.awaitPlan(planId, 0, null);
    }

    private void setDefaultPercent(int percent, CommandServiceAPI cliAdmin)
            throws RemoteException {
        final ParameterMap map = new ParameterMap();
        map.setType(ParameterState.REPNODE_TYPE);
        map.setParameter(ParameterState.RN_PARTITION_SIZE_PERCENT,
                        Integer.toString(percent));
        final int planId =
                cliAdmin.createChangeAllParamsPlan("set default percent to " +
                                                   percent, null, map);
        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);
        cliAdmin.awaitPlan(planId, 0, null);
    }

    private void setPolicies(CreateStore cstore) {
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.AP_CHECK_ADD_INDEX, "1 s");
        map.setParameter(RN_SG_ENABLED, "true");
        map.setParameter(RN_SG_INCLUDE_STORAGE_SIZE, "true");
        map.setParameter(ParameterState.RN_SG_INTERVAL, "60 s");
        map.setParameter(ParameterState.RN_SG_SIZE_UPDATE_INTERVAL, "10 s");
        map.setParameter(ParameterState.RN_SG_LEASE_DURATION, "60 s");
        map.setParameter(ParameterState.RN_SG_SLEEP_WAIT, "50 ms");
        cstore.setPolicyMap(map);
    }

    /**
     * Tests that the partition size limit check works with TTL.
     *
     * The approach of computing size from write delta is inaccurate with TTL
     * present (see ResourceCollector.sizeDeltaMap). However, it still works
     * eventually by reading from system table which is updated with full scan
     * (see KeyStatsCollector#scan).
     *
     * Test steps:
     *
     * - Create a table with size limit of 1G, and partition size of 1M with
     * partitionSizeLimitScaling.
     * - Insert records into a single shard with TTL of 1 hour until it exceeds
     * the partition size limit. Use SpeedyTTLTime to simulate TTL time passing
     * with faster clock frequency.
     * - Waits until all rows expire.
     * - Insert rows again should still succeed.
     *
     * [KVSTORE-2711]
     */
    @Test
    public void testPartitionSizeLimitCheckWithTTL() throws Exception {
        logger.fine("test started");
        KeyStatsCollector.testIgnoreMinimumDurations = true;
        ResourceCollector.partitionSizeLimitScaling = 1000;
        tearDowns.add(() -> {
            KeyStatsCollector.testIgnoreMinimumDurations = false;
            ResourceCollector.partitionSizeLimitScaling = 1;
        });
        final int port = 5000;
        final int partitionSizeLimit = 1 * 1024 * 1024;
        createStore = new CreateStore(kvstoreName, port, 1 /* Storage nodes */,
            1 /* rf */, 10 /* partitions */, 1 /* capacity */,
            CreateStore.MB_PER_SN, true /* useThreads */, null /* mgmtImpl */);
        /* Enable stats */
        setPartitionSizeLimitPolicies(createStore);
        createStore.start();
        logger.fine("store started");
        /* Prepare to wait for the stats thread to notice the user table */
        final CompletableFuture<Void> foundUserTables =
            new CompletableFuture<>();
        KeyStatsCollector.foundUserTablesTestHook =
            v -> foundUserTables.complete(null);
        /* Create table */
        final KVStore kvstore = KVStoreFactory.getStore(
            new KVStoreConfig(kvstoreName, String.format("localhost:%s", port)));
        createSkewTable(kvstore, partitionSizeLimit);
        logger.fine("table created");
        final TableAPI api = kvstore.getTableAPI();
        final Table table = api.getTable("users");
        /* Before insert, in JE TTL simulates one hour by one second. */
        final int fakeMillisPerHour = 1000;
        final SpeedyTTLTime speedyTime = new SpeedyTTLTime(fakeMillisPerHour);
        speedyTime.start();
        /* Wait for stats collector to notice user table. */
        foundUserTables.get(120, TimeUnit.SECONDS);
        insertUntilLimit(api, table, partitionSizeLimit,
            TimeToLive.ofHours(10));
        logger.fine("inserted rows until limit");
        printUserStatsLine(api, TableStatsPartitionDesc.TABLE_NAME);
        assertTrue("number of rows in the users table should not be zero",
                getNumUserRows(api) != 0);
        logger.fine("sleep until row expired and stats scan happend");
        Thread.sleep(25 * 1000);
        printUserStatsLine(api, TableStatsPartitionDesc.TABLE_NAME);
        assertTrue("number of rows in the users table should be zero",
                getNumUserRows(api) == 0);
        logger.fine("all rows expired");
        /* Insert more rows should succeed. */
        insertRows(api, table, 0, 1000, new String("name"),
            TimeToLive.ofHours(10));
        assertTrue("number of rows in the users table should not be zero",
                getNumUserRows(api) != 0);
    }

    private void setPartitionSizeLimitPolicies(CreateStore cstore) {
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.AP_CHECK_ADD_INDEX, "1 s");
        map.setParameter(RN_SG_ENABLED, "true");
        map.setParameter(RN_SG_INCLUDE_STORAGE_SIZE, "true");
        map.setParameter(ParameterState.RN_SG_INTERVAL, "10 s");
        map.setParameter(ParameterState.RN_SG_SIZE_UPDATE_INTERVAL, "2 s");
        map.setParameter(ParameterState.RN_SG_LEASE_DURATION, "5 s");
        map.setParameter(ParameterState.RN_SG_SLEEP_WAIT, "1 s");
        map.setParameter(ParameterState.RN_PARTITION_SIZE_PERCENT, "1");
        cstore.setPolicyMap(map);
    }

    private void createSkewTable(KVStore kvstore, int partitionSizeLimit)
        throws Exception {

        final String ddl = "create table users " +
            "(shardid integer, id integer, name String, " +
            " primary key(shard(shardid), id))";
        final int kb = 1024;
        /* Sets the throughput limit so that we hit the size limit first. */
        final TableLimits limits =
            new TableLimits(TableLimits.NO_LIMIT /* readLimit */,
                5 * partitionSizeLimit / kb /* writeLimit */,
                1 /* sizeLimit in gb */);
        final ExecutionFuture future = ((KVStoreImpl) kvstore)
            .execute(ddl.toCharArray(), null, limits);
        final StatementResult res = future.get();
        assertTrue(res.isSuccessful());
    }

    private void insertUntilLimit(TableAPI api,
                                  Table table,
                                  int partitionSizeLimit,
                                  TimeToLive ttl) {
        final int nameBytesLen = 1000;
        final byte[] nameBytes = new byte[nameBytesLen];
        Arrays.fill(nameBytes, (byte) 'a');
        final String name = new String(nameBytes);
        try {
            insertRows(api, table, 0, 5 * partitionSizeLimit / nameBytesLen,
                name, ttl);
            fail("Expected failure with partition size check");
        } catch (TablePartitionSizeLimitException e) {
            logger.fine("insert got exception: " +
                CommonLoggerUtils.getStackTrace(e));
        }
    }

    private void insertRows(TableAPI api,
                            Table table,
                            int startId,
                            int numRows,
                            String name,
                            TimeToLive ttl) {
        for (int id = startId; id < startId + numRows; ++id) {
            final Row row = table.createRow();
            row.put("shardid", 1);
            row.put("id", id);
            row.put("name", name);
            row.setTTL(ttl);
            api.put(row, null, null);
        }
    }

    private void printUserStatsLine(TableAPI api, String tableName) {
        final PrimaryKey pk = waitForTable(api, tableName).createPrimaryKey();
        final TableIterator<Row> iter = api.tableIterator(pk, null, null);
        try {
            while (iter.hasNext()) {
                final Row row = iter.next();
                if (row.get(TableStatsPartitionDesc.COL_NAME_TABLE_NAME)
                    .asString().get().equals("users"))
                {
                    logger.fine(row.toJsonString(false));
                    break;
                }
            }
        } finally {
            iter.close();
        }
    }

    private long getNumUserRows(TableAPI api) {
        final PrimaryKey pk = waitForTable(api, "users").createPrimaryKey();
        pk.put("shardid", 1);

        final TableIterator<Row> iter = api.tableIterator(pk, null, null);
        final Iterable<Row> it = () -> iter;
        try {
            return StreamSupport.stream(it.spliterator(), false).count();
        } finally {
            iter.close();
        }
    }
}
