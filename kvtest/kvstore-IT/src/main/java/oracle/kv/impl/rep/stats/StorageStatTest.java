/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.stats;


import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_AVG_KEY_SIZE;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_COUNT;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_PARTITION_ID;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_TABLE_NAME;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_TABLE_SIZE;
import static oracle.kv.impl.systables.TableStatsPartitionDesc.COL_NAME_TABLE_SIZE_WITH_TOMBSTONES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.rep.table.TableManager;
import oracle.kv.impl.systables.PartitionStatsLeaseDesc;
import oracle.kv.impl.systables.SysTableRegistry;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.pubsub.PubSubTestBase;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;

import org.junit.Test;

/**
 * Unit test of the storage statistics with tombstone
 */
public class StorageStatTest extends RepNodeTestBase {

    private static final String USER_TABLE_NAME = "user";
    private static final int NUM_ROWS = 1024;
    private static final int PARTITION_ID = 1;
    private static final RepNodeId REP_NODE_ID = new RepNodeId(1, 1);
    private static final WriteOptions WRITE_OPT =
        new WriteOptions(Durability.COMMIT_NO_SYNC, 10000,
                         MILLISECONDS);
    private static final Set<Integer> REGION_IDS =
        new HashSet<>(Arrays.asList(1, 2, 3));

    private KVRepTestConfig config;
    private KVStore kvs;
    private TableAPI tapi;
    private boolean traceOnScreen = false;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (config != null) {
            config.stopRepNodeServices();
            config = null;
        }
        if (kvs != null) {
            kvs.close();
        }
        super.tearDown();
    }

    /**
     * Test the tombstone statistics in multi-region table
     * 1. Create a local multi-region table
     * 2. Insert N rows
     * 3. Check the system table to ensure storage size is correct
     * 4. Delete N rows;
     * 5. Check the system table to ensure storage size is 0, tombstone size
     * is not zero;
     */
    @Test
    public void testStorage() {
        prepareTestEnv();
        trace("Test environment created successfully," +
              "\ntopology:\n" + topo2String(config));


        /* load data */
        final Set<Integer> ids = insertRows();
        trace("Loaded # of rows: " + ids.size());

        /* do partition scan to collect stats */
        doPartitionScan();
        trace("Scan after insertion is done");

        /* verify storage stats, expect no tombstone */
        Row row = getTableStatRow();
        assertNotNull("Cannot find stat row for the table", row);
        trace("Row=" + row.toJsonString(true));
        long count = getCount(row);
        int avgKeySz = getAvgKeySize(row);
        /* all live data */
        assertEquals(NUM_ROWS, count);
        assertTrue(avgKeySz > 0);
        long size = getTableSize(row);
        long sizeTb = getTableWithTbSize(row);
        trace("#rows=" + count +
              ", storage size w/o tombstone=" + size +
              ", size w/ tombstone=" + sizeTb);
        /* no tombstone, thus they are equal */
        assertEquals("Expect equal size without tombstone", size, sizeTb);
        assertTrue(size > 0);

        trace("Storage stats verified");

        /* delete all rows */
        deleteRows(ids);
        trace("Deleted # of rows: " + ids.size() +
              ", doTombstone=" + WRITE_OPT.doTombstone());

        /* do partition scan to collect stats */
        doPartitionScan();
        trace("Scan after deletion is done");

        /* verify storage stats, see tombstone stats */
        row = getTableStatRow();
        assertNotNull("Cannot find stat row for the table", row);

        trace("Row=" + row.toJsonString(true));
        count = getCount(row);
        avgKeySz = getAvgKeySize(row);
        /* no live data, all tombstones */
        assertEquals(0, count);
        assertEquals(0, avgKeySz);

        size = getTableSize(row);
        sizeTb = getTableWithTbSize(row);
        trace("#rows=" + count +
              ", storage size w/o tombstone=" + size +
              ", size w/ tombstone=" + sizeTb);
        /* all rows deleted */
        assertEquals("All rows deleted, size should be 0", 0, size);
        /* has tombstone, each is at least 20 bytes for now */
        assertTrue("Expect tombstone size at least 20 bytes each," +
                   ", number of tombstones= " + NUM_ROWS +
                   ", actual size=" + sizeTb,
                   sizeTb > 20 * NUM_ROWS);
        trace("Storage stats verified");

        trace("Start upgrade test after rows are deleted");
        testUpgradeNullStat();
        trace("Upgrade test completed");

        kvs.close();
        kvs = null;
        trace("Test completed");
    }

    private Row getTableStatRow() {
        final Set<Row> stats = readSystemTable();
        for (Row row : stats) {
            if (getTableName(row).equals(USER_TABLE_NAME) &&
                getPartitionId(row) == PARTITION_ID) {
                return row;
            }
        }
        return null;
    }

    private Set<Integer> insertRows() {
        final Table tb = kvs.getTableAPI().getTable(USER_TABLE_NAME);
        assertNotNull(tb);
        final Set<Integer> ret = new HashSet<>();
        for (int i = 0; i < NUM_ROWS; i++) {
            final Row row = makeRandomRow(tb, i);
            tapi.put(row, null, WRITE_OPT);
            final Row r = readRow(i);
            assertNotNull(r);
            assertEquals(row.toJsonString(true), r.toJsonString(true));
            ret.add(i);
        }
        return ret;
    }

    private Row readRow(int id) {
        final Table tb = kvs.getTableAPI().getTable(USER_TABLE_NAME);
        assertNotNull(tb);
        final PrimaryKey key = tb.createPrimaryKey();
        key.put("id", id);
        return tapi.get(key, null);
    }

    private void deleteRows(Set<Integer> ids) {
        final Table table = kvs.getTableAPI().getTable(USER_TABLE_NAME);
        final PrimaryKey key = table.createPrimaryKey();
        /* create tombstone in deletion */
        WRITE_OPT.setDoTombstone(true);
        for (Integer id : ids) {
            key.put("id", id);
            final boolean succ = tapi.delete(key, null, WRITE_OPT);
            assertTrue(succ);
            final Row r = readRow(id);
            assertNull(r);
        }
    }

    private void doPartitionScan() {
        final PartitionScan pscan = createPartScan(tapi);
        try {
            pscan.runScan();
        } catch (Exception exp) {
            fail("Fail to scan the partition, error=" + exp +
                 "\n" + LoggerUtils.getStackTrace(exp));
        }
    }

    private void prepareTestEnv() {
        /* Start KVStore */
        try {
            config = new KVRepTestConfig(this,
                                         1, /* nDC */
                                         1, /* nSN */
                                         1, /* repFactor */
                                         1 /* nPartitions */);
            config.startRepNodeServices();
            trace("Store started");
        } catch (IOException e) {
            fail("Cannot create store config, error=" + e);
        }

        kvs = KVStoreFactory.getStore(config.getKVSConfig());
        tapi = kvs.getTableAPI();

        /* Create tables for PartitionScan */
        TableMetadata md = new TableMetadata(false);
        final RepNode rn1 = config.getRN(REP_NODE_ID);
        addSysTable(rn1, PartitionStatsLeaseDesc.class, md);
        addSysTable(rn1, TableStatsPartitionDesc.class, md);
        addUserTable(rn1, md);
        trace("System tables added=(" + PartitionStatsLeaseDesc.TABLE_NAME +
              ", " + TableStatsPartitionDesc.TABLE_NAME +
              "), user table added=" + USER_TABLE_NAME);
        getTableFromRN(rn1);
    }

    private String getTableName(Row row) {
        final String tb = row.getTable().getFullNamespaceName();
        if (!tb.equals(TableStatsPartitionDesc.TABLE_NAME)) {
            fail("Stat row from table=" + tb +
                 " not from table=" + TableStatsPartitionDesc.TABLE_NAME);
        }
        return row.get(COL_NAME_TABLE_NAME).asString().get();
    }

    private int getPartitionId(Row row) {
        final String tb = row.getTable().getFullNamespaceName();
        if (!tb.equals(TableStatsPartitionDesc.TABLE_NAME)) {
            fail("Stat row from table=" + tb +
                 " not from table=" + TableStatsPartitionDesc.TABLE_NAME);
        }
        return row.get(COL_NAME_PARTITION_ID).asInteger().get();
    }

    private long getCount(Row row) {
        final String tb = row.getTable().getFullNamespaceName();
        if (!tb.equals(TableStatsPartitionDesc.TABLE_NAME)) {
            fail("Stat row from table=" + tb +
                 " not from table=" + TableStatsPartitionDesc.TABLE_NAME);
        }
        return row.get(COL_NAME_COUNT).asLong().get();
    }

    private int getAvgKeySize(Row row) {
        final String tb = row.getTable().getFullNamespaceName();
        if (!tb.equals(TableStatsPartitionDesc.TABLE_NAME)) {
            fail("Stat row from table=" + tb +
                 " not from table=" + TableStatsPartitionDesc.TABLE_NAME);
        }
        return row.get(COL_NAME_AVG_KEY_SIZE).asInteger().get();
    }

    private long getTableSize(Row row) {
        final String tb = row.getTable().getFullNamespaceName();
        if (!tb.equals(TableStatsPartitionDesc.TABLE_NAME)) {
            fail("Stat row from table=" + tb +
                 " not from table=" + TableStatsPartitionDesc.TABLE_NAME);
        }
        return row.get(COL_NAME_TABLE_SIZE).asLong().get();
    }

    private long getTableWithTbSize(Row row) {
        final String tb = row.getTable().getFullNamespaceName();
        if (!tb.equals(TableStatsPartitionDesc.TABLE_NAME)) {
            fail("Stat row from table=" + tb +
                 " not from table=" + TableStatsPartitionDesc.TABLE_NAME);
        }
        return TableStatsPartitionDesc.getSizeValue(row);
    }

    private PartitionScan createPartScan(TableAPI tableAPI) {
        final PartitionLeaseManager.PartitionLeaseInfo leaseInfo =
            new PartitionLeaseManager.PartitionLeaseInfo(
                PARTITION_ID, "rg1-rn1", 1000, TimeToLive.ofDays(1));

        return new PartitionScan(tableAPI,
                                 new PartitionId(PARTITION_ID),
                                 config.getRN(REP_NODE_ID),
                                 new PartitionLeaseManager(tableAPI),
                                 leaseInfo,
                                 System.currentTimeMillis(),
                                 geRNLogger());
    }

    private Set<Row> readSystemTable() {
        final Set<Row> ret = new HashSet<>();
        final PrimaryKey pk = tapi.getTable(TableStatsPartitionDesc.TABLE_NAME)
                                  .createPrimaryKey();
        final TableIterator<Row> iter = tapi.tableIterator(pk, null, null);
        while (iter.hasNext()) {
            final Row row = iter.next();
            ret.add(row);
        }
        iter.close();
        return ret;
    }

    private void addSysTable(RepNode rn, Class<?> c, TableMetadata md) {

        final TableImpl table = SysTableRegistry.getDescriptor(c).buildTable();
        assert table != null;
        assert table.isSystemTable();

        md.addTable(table.getInternalNamespace(),
                    table.getName(),
                    null,
                    table.getPrimaryKey(),
                    null,
                    table.getShardKey(),
                    table.getFieldMap(),
                    null,
                    null, /*beforeImageTTL*/
                    null,
                    false, 0, null, null/* owner */,
                    true /* sysTable */,
                    null /* identity col */,
                    null /* regions */,
                    false, /* json collection */
                    null   /* mr counters for json coll */);

        rn.updateMetadata(clone(md));
    }

    private void addUserTable(RepNode rn, TableMetadata md) {
        final TableImpl table = buildUserTable();
        md.addTable(null,
                    table.getName(),
                    null,
                    table.getPrimaryKey(),
                    null,
                    table.getShardKey(),
                    table.getFieldMap(),
                    null,
                    null, /*beforeImageTTL*/
                    null,
                    false, 0, null, null/* owner */,
                    false /* not sysTable */,
                    null /* identity col */,
                    null /* regions */,
                    false, /* json collection */
                    null   /* mr counters for json coll */);

        rn.updateMetadata(clone(md));
    }

    private static Metadata<?> clone(Metadata<?> md) {
        return SerializationUtil.getObject(SerializationUtil.getBytes(md),
                                           md.getClass());
    }

    private Logger geRNLogger() {
        final RepNode rn1 = config.getRN(REP_NODE_ID);
        return LoggerUtils.getLogger(rn1.getClass(),
                                     rn1.getRepNodeId().toString(),
                                     rn1.getRepNodeId(),
                                     rn1.getGlobalParams(),
                                     rn1.getStorageNodeParams());
    }

    /* dump topology from a RN */
    private static String topo2String(KVRepTestConfig config) {
        final RepNode rn = config.getRN(REP_NODE_ID);
        return PubSubTestBase.dumpTopo(rn.getTopology());
    }

    private Row makeRandomRow(Table table, int which) {
        final Row row = table.createRow();
        row.put("id", which);
        row.put("desc", "Desc-" + UUID.randomUUID());
        return row;
    }

    private void trace(String message) {
        if (traceOnScreen) {
            System.err.println(message);
        }

        if (logger != null) {
            message = "[TEST] " + message;
            logger.info(message);
        }
    }

    private void getTableFromRN(RepNode masterRN) {
        final TableManager tm = masterRN.getTableManager();
        final TableMetadata tmd = tm.getTableMetadata();
        final Map<String, Table> tbs = tmd.getTables();
        for (Table tb : tbs.values()) {
            trace("TB=" + tb.getFullNamespaceName() +
                  ", id=" + ((TableImpl) tb).getId());
        }
        final TableImpl tb =
            (TableImpl) kvs.getTableAPI().getTable(USER_TABLE_NAME);
        assertNotNull(tb);
    }

    private TableImpl buildUserTable() {
        final TableBuilder builder =
            TableBuilder.createTableBuilder(USER_TABLE_NAME);
        builder.addInteger("id")
               .addString("desc")
               .primaryKey("id")
               .shardKey("id");
        REGION_IDS.forEach(builder::addRegion);
        return builder.buildTable();
    }

    /**
     * Test that in upgrade, the table size with tombstone column may
     * return null
     */
    private void testUpgradeNullStat() {
        final Row row = getTableStatRow();
        assertNotNull(row);
        /* live data size */
        final long size = getTableSize(row);
        /* all data deleted, live data should be 0 */
        assertEquals(0, size);
        /* size with tombstone should be greater than 0 */
        long sizeWithTb = TableStatsPartitionDesc.getSizeValue(row);
        assertTrue("table size=" + sizeWithTb +
                   " with tb should be greater than 0",
                   sizeWithTb > 0);

        /* set hook to simulate an upgrade, null will return from the stat */
        setUpgradeTestHook();
        /* re-read the stat */
        sizeWithTb = TableStatsPartitionDesc.getSizeValue(row);
        /* it should return the live table size */
        assertEquals(size, sizeWithTb);
        TableStatsPartitionDesc.upgradeTestHook = null;
    }

    private void setUpgradeTestHook() {
        TableStatsPartitionDesc.upgradeTestHook = row -> {
            final long sz =
                row.get(COL_NAME_TABLE_SIZE_WITH_TOMBSTONES).asLong().get();
            trace("Size=" + sz + ", make it null");
            /* simulate an upgrade, null may return from the col */
            row.put(COL_NAME_TABLE_SIZE_WITH_TOMBSTONES,
                    NullValueImpl.getInstance());
        };
    }
}
