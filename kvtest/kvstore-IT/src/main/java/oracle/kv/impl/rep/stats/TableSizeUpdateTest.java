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
import static oracle.kv.impl.rep.stats.IntermediateTableSizeUpdate.POSITIVE_THRESHOLD_BYTES;
import static oracle.kv.impl.rep.stats.IntermediateTableSizeUpdate.NEGATIVE_THRESHOLD_BYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import java.rmi.RemoteException;
import java.util.Random;
import java.util.function.Function;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.sklogger.ScheduleStart;

/**
 * Unit test for the intermediate table size update mechanism.
 *
 * Note that this is a long running test as it needs to wait for multiple
 * table stat scans, which run every 60 seconds.
 */
public class TableSizeUpdateTest extends TestBase {

    private final static int N_PART = 5;

    /**
     * The intermediate update waits for write deltas to go over + and -
     * thresholds before an update is done. We need to allow for the last set
     * of operations to be missed. Update is per-partition so there could be
     * up to N_PART missing sets. These are the fudge factors for the type
     * of operation.
     */
    private final static long INSERT_FUDGE = POSITIVE_THRESHOLD_BYTES * N_PART;
    private final static long DELETE_FUDGE = -NEGATIVE_THRESHOLD_BYTES * N_PART;

    private final static long SCAN_INTERVAL_MS = 60*1000;

    private final static int SEED = 1235;
    private final Random rnd = new Random(SEED);

    private final static boolean A_RECORDS = true;
    private final static boolean B_RECORDS = false;

    private CreateStore createStore = null;
    private static final String namespace = "StatsNS";

    private final TableImpl userTable;
    private final TableImpl childTable;

    /* Large block of data to force over update thresholds. */
    private final static byte[] DATA = new byte[5000];

    public TableSizeUpdateTest() {
        userTable = TableBuilder.createTableBuilder(namespace, "User")
                                .addInteger("id")
                                .addInteger("subId")
                                .addString("firstName")
                                .addString("lastName")
                                .addInteger("age")
                                .addBinary("data")
                                .primaryKey("id", "subId")
                                .shardKey("id")
                                .buildTable();
        childTable = TableBuilder.createTableBuilder("Child", "child table",
                                                     userTable)
                                 .addInteger("income")
                                 .addString("string")
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
    public void testUpdate() throws Exception {
        /* Start a single node KVStore */
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      N_PART, /* Partitions */
                                      1, /* capacity */
                                      3 * CreateStore.MB_PER_SN,
                                      false,
                                      null);
        setPolicies(createStore);
        createStore.start();
        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);
        final TableAPI api = store.getTableAPI();

        final CommandServiceAPI cliAdmin = createStore.getAdmin();
        createTable(userTable, cliAdmin);
        createTable(childTable, cliAdmin);

        /* Wait until after the scan period to start tests */
        waitToNextScan();
        long scannedSize = getTableSize(api, userTable);
        assertEquals("Initial table should be empty", 0L, scannedSize);

        long scannedChildSize = getTableSize(api, childTable);
        assertEquals("Initial child table should be empty",
                     0L, scannedChildSize);

        /*
         * A scan just ended. We have about 60 seconds modify the table and
         * then waiting for the intermediate counts to be updated (which
         * should be happening every 10 seconds).
         */

        /* -- put (no previous record) -- */

        runOperation(15, 1000, (id) -> put(id, api, A_RECORDS));
        runOperation(15, 1000, (id) -> putChild(id, api, A_RECORDS));
        waitForUpdates();
        long updatedSize = getTableSize(api, userTable);
        long updatedChildSize = getTableSize(api, childTable);

        /*
         * At this point updatedSize should be the table size values updated
         * from the from the last scan. Wait for the next scan, which will
         * refresh the values, and then compare.
         */
        waitToNextScan();
        scannedSize = getTableSize(api, userTable);
        checkSize(updatedSize, scannedSize, INSERT_FUDGE);
        scannedChildSize = getTableSize(api, childTable);
        checkSize(updatedChildSize, scannedChildSize, INSERT_FUDGE);

        /* -- put (previous record w/different size) -- */

        runOperation(15, 1000, (id) -> put(id, api, B_RECORDS));
        runOperation(15, 1000, (id) -> putChild(id, api, B_RECORDS));
        waitForUpdates();
        updatedSize = getTableSize(api, userTable);
        updatedChildSize = getTableSize(api, childTable);
        waitToNextScan();
        scannedSize = getTableSize(api, userTable);
        checkSize(updatedSize, scannedSize, INSERT_FUDGE);
        scannedChildSize = getTableSize(api, childTable);
        checkSize(updatedChildSize, scannedChildSize, INSERT_FUDGE);

        /* -- deleteIfVersion -- */

        /*
         * Delete (ifVersion) only 900 rows for the first 10 seconds. We don't
         * delete all of the rows so that we are not fooled by the zero floor
         * in the size update calculations.
         */
        runOperation(10, 900, (id) -> deleteIfVersion(id, api));
        waitForUpdates();
        updatedSize = getTableSize(api, userTable);
        waitToNextScan();
        scannedSize = getTableSize(api, userTable);
        checkSize(updatedSize, scannedSize, DELETE_FUDGE);

        /* -- delete -- */

        /* Delete everything */
        runOperation(40, 1100, (id) -> delete(id, api));
        waitForUpdates();
        updatedSize = getTableSize(api, userTable);
        //assertEquals("Updated table size should be zero", 0L, updatedSize);
        waitToNextScan();
        scannedSize = getTableSize(api, userTable);
        assertEquals("Scanned table size should be zero", 0L, scannedSize);
        checkSize(updatedSize, 0L, DELETE_FUDGE);

        /*  -- putIfAbsent -- */

        runOperation(30, 1000, (id) -> putIfAbsent(id, api, A_RECORDS));
        waitForUpdates();
        updatedSize = getTableSize(api, userTable);
        waitToNextScan();
        scannedSize = getTableSize(api, userTable);
        checkSize(updatedSize, scannedSize, INSERT_FUDGE);

        /* -- putIfVersion (previous record w/different size) -- */

        runOperation(30, 1000, (id) -> putIfVersion(id, api, B_RECORDS));
        waitForUpdates();
        updatedSize = getTableSize(api, userTable);
        waitToNextScan();
        scannedSize = getTableSize(api, userTable);
        checkSize(updatedSize, scannedSize, INSERT_FUDGE);

        /* Delete everything */
        runOperation(40, 1100, (id) -> delete(id, api));
        waitForUpdates();
        updatedSize = getTableSize(api, userTable);
        waitToNextScan();
        scannedSize = getTableSize(api, userTable);
        assertEquals("Scanned table size should be zero", 0L, scannedSize);
        checkSize(updatedSize, 0L, DELETE_FUDGE);

        /* -- miltiDelete -- */

        /* this inserts 100 records for each shard key */
        runOperation(30, 1000, (id) -> multiPut(id, api, 100));
        waitToNextScan();

        /* delete only partial */
        runOperation(20, 10, (id) -> multiDelete(id, api));
        waitForUpdates();
        updatedSize = getTableSize(api, userTable);
        waitToNextScan();
        scannedSize = getTableSize(api, userTable);
        checkSize(updatedSize, scannedSize, DELETE_FUDGE);
    }

    /**
     * Compares the updated size and the scanned size. Fails if the difference
     * of the sizes is over the fudge factor.
     */
    private void checkSize(long updatedSize, long scannedSize, long fudge) {
        final long diff = Math.abs(updatedSize - scannedSize);
        assertTrue("Updated (expected) and scanned sizes were not within the" +
                   " allowable range, updated size " + updatedSize +
                   " scanned size " + scannedSize + " +/- " + fudge,
                   diff < fudge);
    }

    private Void put(int id, TableAPI api, boolean putAorB) {
        api.put(makeRow(id, api, putAorB), null, null);
        return null;
    }

    private Void putChild(int id, TableAPI api, boolean putAorB) {
        api.put(makeChildRow(id, api, putAorB), null, null);
        return null;
    }

    private Void putIfAbsent(int id, TableAPI api, boolean putAorB) {
        api.putIfAbsent(makeRow(id, api, putAorB), null, null);
        return null;
    }

    private Void putIfVersion(int id, TableAPI api, boolean putAorB) {
        final Row oldRow = api.get(makeKey(id, api), null);
        if (oldRow != null) {
            api.putIfVersion(makeRow(id, api, putAorB),
                             oldRow.getVersion(), null, null);
        }
        return null;
    }

    private Void delete(int id, TableAPI api) {
        api.delete(makeKey(id, api), null, null);
        return null;
    }

    private Void deleteIfVersion(int id, TableAPI api) {
        PrimaryKey key = makeKey(id, api);
        Row row = api.get(key, null);
        if (row != null) {
            api.deleteIfVersion(key, row.getVersion(), null, null);
        }
        return null;
    }

    private Void multiDelete(int id, TableAPI api) {
        PrimaryKey key = makeKey(id, api);
        /* Will cause all of the records with the shard key to be deleted */
        key.putNull("subId");
        api.multiDelete(key, null, null);
        return null;
    }

    /*  Inserts nRows records with the same shard key  */
    private Void multiPut(int id, TableAPI api, int nRows) {
        final Row row = makeRow(id, api, true);
        for (int i = 0; i < nRows; i++) {
            row.put("subId", i);
            api.put(row, null, null);
        }
        return null;
    }

    private PrimaryKey makeKey(int id, TableAPI api) {
        final PrimaryKey key =
                    api.getTable(userTable.getInternalNamespace(),
                                 userTable.getFullName()).createPrimaryKey();
        key.put("id", id);
        key.put("subId", id);
        return key;
    }

    private Row makeRow(int id, TableAPI api, boolean makeAorB) {
        final Row row = api.getTable(userTable.getInternalNamespace(),
                                     userTable.getFullName()).createRow();
        /* Extra to make the records different size */
        int extra = (makeAorB) ? Integer.MAX_VALUE - id : id;
        row.put("id", id);
        row.put("subId", id);
        row.put("firstName", "joe" + extra);
        row.put("lastName", "cool" + extra);
        row.put("age", id);
        row.put("data", DATA);
        return row;
    }

    private Row makeChildRow(int id, TableAPI api, boolean makeAorB) {
        final Row row = api.getTable(childTable.getInternalNamespace(),
                                     childTable.getFullName()).createRow();
        /* Extra to make the records different size */
        int extra = (makeAorB) ? Integer.MAX_VALUE - id : id;
        row.put("id", id);
        row.put("subId", id);
        row.put("income", id);
        row.put("string", "string" + extra);
        row.put("childdata", DATA);
        return row;
    }

    /**
     * Runs the specified operation nOps times every 2 seconds for opSec and
     * then returns the table size after waiting long enough for the an update
     * to include all of the changes.
     */
    private void runOperation(int opSec,
                              int nOps,
                              Function<Integer, Void> op)
            throws InterruptedException {
        assert opSec < 50;

        /* Reset so that we generate the same key order */
        rnd.setSeed(SEED);

        long now = System.currentTimeMillis();

        /* Make sure we have enough time */
        long nextScanMS = nextScanMS(now);
        assertTrue("Not enough time to run operations: " + nextScanMS + "ms",
                   nextScanMS > opSec*1000);

        /* Run operation for the first n seconds. */
        long opTime = now + opSec*1000;

        /* Loop checking the updated stats for 50 seconds */
        long update = now + 50*1000;

        while (now < update) {
            if (opTime > now) {
                for (int i = 0; i < nOps; i++) {
                    op.apply(rnd.nextInt());
                }
            } else {
                break;
            }
            Thread.sleep(2*1000);
            now = System.currentTimeMillis();
        }
    }

    private long nextScanMS(long now) {
        return ScheduleStart.calculateDelay(SCAN_INTERVAL_MS, now);
    }

    /*
     * Wait until just before the next scan to give time for updates to finish.
     */
    private void waitForUpdates() throws InterruptedException {
        final long waitTime = nextScanMS(System.currentTimeMillis()) - 5*1000;
        if (waitTime > 0) {
            Thread.sleep(waitTime);
        }
    }
    /*
     * Wait until past the next scan period.
     */
    private void waitToNextScan() throws InterruptedException {
        Thread.sleep(nextScanMS(System.currentTimeMillis()) + 5*1000);
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
            size += row.get(TableStatsPartitionDesc.COL_NAME_TABLE_SIZE)
                                                                .asLong().get();
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
                                    new TableLimits(NO_LIMIT, NO_LIMIT, 10) :
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
}
