/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.impl.api.AggregateThroughputTracker;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.rep.stats.PartitionLeaseManager.PartitionLeaseInfo;
import oracle.kv.impl.systables.PartitionStatsLeaseDesc;
import oracle.kv.impl.systables.SysTableRegistry;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.Database;

import org.junit.Test;

/**
 * Unit test for PartitionScan.
 */
public class PartitionScanTest extends RepNodeTestBase {
    private final RepNodeId rg1rn1Id = new RepNodeId(1, 1);
    private KVRepTestConfig config;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        /* Start KVStore */
        config = new KVRepTestConfig(this,
                                     1, /* nDC */
                                     1, /* nSN */
                                     3, /* repFactor */
                                     1 /* nPartitions */);
    }

    @Override
    public void tearDown() throws Exception {
        config.stopRepNodeServices();
        config = null;

        super.tearDown();
    }

    @Test
    public void testBasic() throws Exception {
        config.startRepNodeServices();

        final RepNode rn1 = config.getRN(rg1rn1Id);
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        Logger rnLogger = LoggerUtils.getLogger(rn1.getClass(),
                                                rn1.getRepNodeId().toString(),
                                                rn1.getRepNodeId(),
                                                rn1.getGlobalParams(),
                                                rn1.getStorageNodeParams());
        PartitionScan scan = new PartitionScan(apiImpl, new PartitionId(1),
                                               rn1, null, null,
                                               System.currentTimeMillis(),
                                               rnLogger);

        /* Check the stats table existing or not */
        assertFalse(scan.checkStatsTable(((TableMetadata)rn1.
                                          getMetadata(MetadataType.TABLE))));

        Database db = scan.getDatabase();
        /* Check whether partition database exists or not */
        assertTrue(db.getDatabaseName().indexOf("p1") != -1);

        /* No data existing in KVStore and only one iteration scan is enough */
        assertFalse(scan.scanDatabase(rn1.getEnv(5000), db));

        /* Create tables for PartitionScan */
        TableMetadata md = new TableMetadata(false);
        addSysTable(rn1, PartitionStatsLeaseDesc.class, md);
        addSysTable(rn1, TableStatsPartitionDesc.class, md);

        /* Create a populate a user table so there is more to scan */
        final TableImpl userTable = TableBuilder.createTableBuilder("User")
            .addInteger("key")
            .addString("value")
            .primaryKey("key")
            .buildTable();
        for (int i = 0; i < 1000; i++) {
            final Row row = userTable.createRow();
            row.put("key", i);
            final char[] chars = new char[1024];
            Arrays.fill(chars, 0, 1022, 'x');
            chars[1023] = (char) ('0' + (i%10));
            row.put("value", new String(chars));
            apiImpl.put(row, null, null);
        }

        assertTrue(scan.checkStatsTable(((TableMetadata)rn1.
                                         getMetadata(MetadataType.TABLE))));

        PartitionLeaseManager plTable = new PartitionLeaseManager(apiImpl);

        PartitionLeaseInfo leaseInfo =
                new PartitionLeaseInfo(1, "rg1-rn1", 1000, TimeToLive.ofDays(1));

        scan = new PartitionScan(apiImpl, new PartitionId(1),
                                 rn1, plTable, leaseInfo,
                                 System.currentTimeMillis(),
                                 rnLogger);

        final AggregateThroughputTracker resourceTracker =
            (AggregateThroughputTracker) rn1.getAggregateThroughputTracker();
        final long kbReadBeforeScan = resourceTracker.getReadKB();

        scan.runScan();

        /*
         * The tracked throughput should not include the 1000 KB value size,
         * just the key bytes for each of the 1000 items, no more than 100
         * bytes.
         */
        final long kbReadByScan =
            resourceTracker.getReadKB() - kbReadBeforeScan;
        assertTrue("KB read should be less than 100, found " + kbReadByScan,
                    kbReadByScan < 100);

        /* Check results */
        PrimaryKey pk = apiImpl.getTable(PartitionStatsLeaseDesc.TABLE_NAME).
                createPrimaryKey();

        TableIterator<Row> iter = apiImpl.tableIterator(pk, null, null);
        int lineNum =  0;
        while (iter.hasNext()) {
            iter.next();
            lineNum++;
        }
        iter.close();
        assertEquals(lineNum, 1);

        pk = apiImpl.getTable(TableStatsPartitionDesc.TABLE_NAME).
                createPrimaryKey();

        iter = apiImpl.tableIterator(pk, null, null);
        lineNum =  0;
        while (iter.hasNext()) {
            iter.next();
            lineNum++;
        }
        iter.close();
        assertEquals(lineNum, 3);

        kvs.close();
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
                    false,  0, null, null/* owner */,
                    true /* sysTable */,
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
}
