/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.TestBase;
import oracle.kv.Version;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.stats.IndexLeaseManager.IndexLeaseInfo;
import oracle.kv.impl.systables.IndexStatsLeaseDesc;
import oracle.kv.impl.systables.SysTableRegistry;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Unit test for IndexLeaseTable
 */
public class IndexLeaseManagerTest extends TestBase {
    private CreateStore createStore = null;
    private static final String namespace = "ILMNS";

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (createStore != null) {
            createStore.shutdown(false);
        }
        super.tearDown();
    }

    @Test
    public void testBasic() throws Exception {

        final String tableName = "myTable";
        final String tableNamespaceName = (namespace != null ?
                                           (namespace + ":" + tableName) :
                                           tableName);

        /* Start 1*1 KVStore */
        createStore = new CreateStore("kvtest-" + this.getClass().getName() +
                                      "-testBasic",
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      3, /* Partitions */
                                      1); /* capacity */

        createStore.start();

        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        /* Internal store handle to access the system table */
        store = createStore.getInternalStoreHandle(store);
        TableAPI tableAPI = store.getTableAPI();
        waitForTable(tableAPI, IndexStatsLeaseDesc.TABLE_NAME);

        final Table table = SysTableRegistry.getDescriptor(IndexStatsLeaseDesc.class).buildTable();
        assert table != null;

        IndexLeaseManager ilTable = new IndexLeaseManager(tableAPI);

        /* Check the columns name */
        assertEquals(ilTable.getLeaseTableName(), IndexStatsLeaseDesc.TABLE_NAME);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("tableName", tableNamespaceName);
        pk.put("indexName", "myIndex");
        pk.put("shardId", 1);
        /* Dummy table to package namespace and table name. */
        TableImpl myTable  =
            TableBuilder.createTableBuilder(namespace, "myTable")
            .addString("foo")
            .primaryKey("foo")
            .buildTable();

        IndexLeaseInfo leaseInfo = new IndexLeaseInfo(myTable,
                                                      "myIndex",
                                                      1,
                                                      "rg1-rn1",
                                                      1000,
                                                      TimeToLive.ofDays(1));
        /* Check LeaseInfo */
        assertEquals(ilTable.createPrimaryKey(leaseInfo), pk);
        assertEquals(leaseInfo.toString(), "index " + tableNamespaceName +
                     ".myIndex in shard-1 by rg1-rn1");

        /* Check the values in row */
        Row row = ilTable.createRow(leaseInfo, false);
        assertEquals(row.get("tableName").asString().get(), tableNamespaceName);
        assertEquals(row.get("indexName").asString().get(), "myIndex");
        assertEquals(row.get("shardId").asInteger().get(), 1);
        assertEquals(row.get("leasingRN").asString().get(), "rg1-rn1");

        /* Check leaseExpiry and lastUpdated */
        String lastUpdatedStr = row.get("lastUpdated").asString().get();
        assertTrue(lastUpdatedStr.isEmpty());

        row = ilTable.createRow(leaseInfo, true);
        long leaseExpiry =
            getTimeFromDate(row.get("leaseExpiry").asString().get());
        long lastUpdated =
                getTimeFromDate(row.get("lastUpdated").asString().get());
        assertEquals(leaseExpiry, lastUpdated);
    }

    @Test
    public void testLeaseManager() throws Exception {
        /* Start 1*1 KVStore */
        createStore = new CreateStore("kvtest-" + this.getClass().getName() +
                                      "-testLeaseManager",
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      3, /* Partitions */
                                      1); /* capacity */

        createStore.start();

        final CommandServiceAPI cliAdmin = createStore.getAdmin();
        TableImpl table = getTestTableBuilder().buildTable();

        /* Create myTable */
        int planId = cliAdmin.createAddTablePlan(
                         "AddTable",
                         table.getInternalNamespace(),
                         "myTable",
                         null,
                         table.getFieldMap(),
                         table.getPrimaryKey(),
                         table.getPrimaryKeySizes(),
                         table.getShardKey(),
                         null, // ttl
                         null, // limits
                         table.isR2compatible(),
                         table.getSchemaId(),
                         table.isJsonCollection(),
                         table.getJsonCollectionMRCounters(),
                         null);

        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);
        cliAdmin.awaitPlan(planId, 0, null);

        /* Add index*/
        planId = cliAdmin.createAddIndexPlan("AddIndex",
                                             table.getInternalNamespace(),
                                             "myIndex",
                                             "myTable",
                                             new String[] {"myIndex"},
                                             null, true, null);
        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);
        cliAdmin.awaitPlan(planId, 0, null);

        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        /* Internal store handle to access the system table */
        store = createStore.getInternalStoreHandle(store);
        TableAPI tableAPI = store.getTableAPI();
        waitForTable(tableAPI, IndexStatsLeaseDesc.TABLE_NAME);
        IndexLeaseInfo leaseInfo = new IndexLeaseInfo(table,
                                                      "myIndex",
                                                      1,
                                                      "rg1-rn1",
                                                      5000,
                                                      TimeToLive.ofDays(1));
        IndexLeaseManager manager = new IndexLeaseManager(tableAPI);

        /* Check returns of methods of StatsLeaseManager */
        assertNull(manager.getStoredLease(leaseInfo));
        assertNotNull(manager.createLease(leaseInfo));
        StatsLeaseManager<IndexLeaseInfo>.Lease lease =
                manager.getStoredLease(leaseInfo);
        assertNotNull(lease);

        /* Waiting 4600ms to let the time in the needed extend period */
        Version version = manager.extendLeaseIfNeeded(leaseInfo,
                lease.getLatestVersion());
        assertEquals(version, lease.getLatestVersion());
        Thread.sleep(4600);
        version = manager.extendLeaseIfNeeded(leaseInfo,
                lease.getLatestVersion());
        assertNotNull(version);

        assertNotNull(manager.terminateLease(leaseInfo, version));

        lease = manager.getStoredLease(leaseInfo);
        assertNotNull(lease);
        assertEquals(lease.getExpiryDate(), lease.getLastUpdated());
        version = manager.renewLease(leaseInfo, lease.getLatestVersion());
        assertNotNull(version);
        version = manager.terminateLease(leaseInfo, version);
        assertNotNull(version);
    }

    static TableBuilder getTestTableBuilder() {
        final TableBuilder builder = TableBuilder.createTableBuilder(namespace,
                                                                     "myTable");

        /* Types of all fields within this table */
        builder.addString("myIndex");
        builder.primaryKey("myIndex");
        builder.shardKey("myIndex");

        return builder;
    }

    private long getTimeFromDate(String date) throws ParseException {
        long time = StatsLeaseManager.DATE_FORMAT.parse(date).getTime();
        return time;
    }
}
