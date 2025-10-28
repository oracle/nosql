/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.stats;

import static oracle.kv.impl.api.table.TableTestBase.makeIndexList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.impl.api.AggregateThroughputTracker;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.rep.RepNodeTestBase;
import oracle.kv.impl.rep.stats.IndexLeaseManager.IndexLeaseInfo;
import oracle.kv.impl.systables.IndexStatsLeaseDesc;
import oracle.kv.impl.systables.SysTableRegistry;
import oracle.kv.impl.systables.TableStatsIndexDesc;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TimeToLive;

import org.junit.Test;

/**
 * Unit test for TableIndexScan.
 */
public class TableIndexScanTest extends RepNodeTestBase {
    private static final String namespace = "IndexScanNS";

    private static final TableImpl userTable =
        TableBuilder.createTableBuilder(namespace, "User")
        .addInteger("id")
        .addString("firstName")
        .addString("lastName")
        .addInteger("age")
        .primaryKey("id")
        .shardKey("id")
        .buildTable();

    private final RepNodeId rg1rn1Id = new RepNodeId(1, 1);
    private KVRepTestConfig config;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        /* Start KVStore */
        config = new KVRepTestConfig(this,
                                     1, /* nDC */
                                     1, /* nSN */
                                     1, /* repFactor */
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
        TableIndexScan scan = new TableIndexScan(apiImpl,
                                                 userTable, "FirstName",
                                                 rn1,
                                                 null, null,
                                                 System.currentTimeMillis(),
                                                 rnLogger);

        /* Check the stats table existing or not */
        assertFalse(scan.checkStatsTable(((TableMetadata)rn1.
                                         getMetadata(MetadataType.TABLE))));

        /* Create tables for TableIndexScan */
        TableMetadata md = new TableMetadata(false);
        addSysTable(rn1, IndexStatsLeaseDesc.class, md);
        addSysTable(rn1, TableStatsIndexDesc.class, md);
        addTable(rn1, userTable, md);

        /* Populate data into userTable */
        addRows(userTable.getInternalNamespace(), userTable.getFullName(),
                1000, apiImpl);

        md.addIndex(namespace, "FirstName", userTable.getFullName(),
                    makeIndexList("firstName"), null, true, false,
                    null);
        rn1.updateMetadata(clone(md));

        md.updateIndexStatus(namespace, "FirstName", userTable.getFullName(),
                             IndexImpl.IndexStatus.READY);
        rn1.updateMetadata(clone(md));

        Thread.sleep(500);

        assertTrue(scan.checkStatsTable(((TableMetadata)rn1.
                                         getMetadata(MetadataType.TABLE))));

        IndexLeaseManager ilTable = new IndexLeaseManager(apiImpl);

        IndexLeaseInfo leaseInfo = new IndexLeaseInfo(userTable,
                                                      "FirstName", 1,
                                                      "rg1-rn1", 1000,
                                                      TimeToLive.ofDays(1));

        final AggregateThroughputTracker resourceTracker =
            (AggregateThroughputTracker) rn1.getAggregateThroughputTracker();
        final long kbReadBeforeScan = resourceTracker.getReadKB();

        scan = new TableIndexScan(apiImpl, userTable, "FirstName",
                                  rn1,
                                  ilTable, leaseInfo,
                                  System.currentTimeMillis(),
                                  rnLogger);
        scan.runScan();

        /*
         * Scan should count entry sizes without rounding, less than 100 bytes
         * per entry
         */
        final long kbReadByScan =
            resourceTracker.getReadKB() - kbReadBeforeScan;
        assertTrue("KB read by scan should be less than 100, found " +
                   kbReadByScan,
                   kbReadByScan < 100);

        /* Check results */
        PrimaryKey pk = apiImpl.getTable(IndexStatsLeaseDesc.TABLE_NAME).
                createPrimaryKey();

        TableIterator<Row> iter = apiImpl.tableIterator(pk, null, null);
        int lineNum =  0;
        while (iter.hasNext()) {
            iter.next();
            lineNum++;
        }
        iter.close();
        assertEquals(lineNum, 1);

        pk = apiImpl.getTable(TableStatsIndexDesc.TABLE_NAME).createPrimaryKey();

        iter = apiImpl.tableIterator(pk, null, null);
        lineNum =  0;
        while (iter.hasNext()) {
            iter.next();
            lineNum++;
        }
        iter.close();
        assertEquals(lineNum, 1);

        kvs.close();
    }

    private void addTable(RepNode rn, Table table, TableMetadata md) {
        md.addTable(table.getNamespace(),
                    table.getName(),
                    null,
                    table.getPrimaryKey(),
                    null,
                    table.getShardKey(),
                    ((TableImpl)table).getFieldMap(),
                    null,
                    null, /*beforeImageTTL*/
                    null,
                    false,  0, null, null/* owner */);

        rn.updateMetadata(clone(md));
    }

    private void addSysTable(RepNode rn, Class<?> c, TableMetadata md) {
        final TableImpl table = SysTableRegistry.getDescriptor(c).buildTable();
        assert table != null;
        assert table.isSystemTable();

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

    static void addRows(String ns,
                        String tableName,
                        int numRows,
                        TableAPI apiImpl) {
        Row row = apiImpl.getTable(ns, tableName).createRow();
        for (int i = 0; i < numRows; i++) {
            row.put("id", i);
            row.put("firstName", "joe" + i);
            row.put("lastName", "cool" + i);
            row.put("age", 10 + i);
            apiImpl.put(row, null, null);
        }
    }
}
