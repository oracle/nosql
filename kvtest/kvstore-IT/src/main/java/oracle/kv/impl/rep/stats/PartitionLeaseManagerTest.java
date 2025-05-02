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
import oracle.kv.impl.rep.stats.PartitionLeaseManager.PartitionLeaseInfo;
import oracle.kv.impl.systables.PartitionStatsLeaseDesc;
import oracle.kv.impl.systables.SysTableRegistry;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Unit test for PartitionLeaseTable.
 */
public class PartitionLeaseManagerTest extends TestBase {    
    private CreateStore createStore = null;

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
        /* Start 1*1 KVStore */
        createStore = new CreateStore("kvtest-" + this.getClass().getName() +
                                      "-testBasic",
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      3, /* Partitions */
                                      1); /* capacity */
        createStore.start();

        final Table table = SysTableRegistry.
                getDescriptor(PartitionStatsLeaseDesc.class).buildTable();
        assert table != null;

        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);
        TableAPI tableAPI = store.getTableAPI();
        waitForTable(tableAPI, PartitionStatsLeaseDesc.TABLE_NAME);

        PartitionLeaseManager plTable = new PartitionLeaseManager(tableAPI);

        /* Check the columns name */
        assertEquals(plTable.getLeaseTableName(),
                     PartitionStatsLeaseDesc.TABLE_NAME);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("partitionId", 1);

        PartitionLeaseInfo leaseInfo =
                new PartitionLeaseInfo(1, "rg1-rn1", 1000, TimeToLive.ofDays(1));
        /* Check LeaseInfo */
        assertEquals(plTable.createPrimaryKey(leaseInfo), pk);
        assertEquals(leaseInfo.toString(), "partition-1 by rg1-rn1");

        /* Check the values in row */
        Row row = plTable.createRow(leaseInfo, false);
        assertEquals(row.get("partitionId").asInteger().get(), 1);
        assertEquals(row.get("leasingRN").asString().get(), "rg1-rn1");

        /* Check leaseExpiry and lastUpdated */
        long leaseExpiry =
                getTimeFromDate(row.get("leaseExpiry").asString().get());
        String lastUpdatedStr = row.get("lastUpdated").asString().get();
        assertTrue(lastUpdatedStr.isEmpty());

        row = plTable.createRow(leaseInfo, true);
        leaseExpiry = getTimeFromDate(row.get("leaseExpiry").asString().get());
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

        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);
        TableAPI tableAPI = store.getTableAPI();
        waitForTable(tableAPI, PartitionStatsLeaseDesc.TABLE_NAME);
        PartitionLeaseInfo leaseInfo =
                new PartitionLeaseInfo(1, "rg1-rn1", 1000, TimeToLive.ofDays(1));
        PartitionLeaseManager manager = new PartitionLeaseManager(tableAPI);

        /* Check returns of methods of StatsLeaseManager */
        assertNull(manager.getStoredLease(leaseInfo));
        assertNotNull(manager.createLease(leaseInfo));
        StatsLeaseManager<PartitionLeaseInfo>.Lease lease =
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

    private long getTimeFromDate(String date) throws ParseException {
        long time = StatsLeaseManager.DATE_FORMAT.parse(date).getTime();
        return time;
    }
}
