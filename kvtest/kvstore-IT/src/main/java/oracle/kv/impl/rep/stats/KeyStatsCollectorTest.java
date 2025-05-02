/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.stats;

import static oracle.kv.impl.param.ParameterState.RN_SG_ENABLED;
import static oracle.kv.impl.param.ParameterState.RN_SG_INCLUDE_STORAGE_SIZE;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.StoreIteratorException;
import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.rep.table.TableManager;
import oracle.kv.impl.systables.IndexStatsLeaseDesc;
import oracle.kv.impl.systables.PartitionStatsLeaseDesc;
import oracle.kv.impl.systables.SysTableDescriptor;
import oracle.kv.impl.systables.SysTableRegistry;
import oracle.kv.impl.systables.TableStatsIndexDesc;
import oracle.kv.impl.systables.TableStatsPartitionDesc;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.CreateStore;

import org.junit.Test;

/**
 * Unit test for KeyStatsCollector
 */
public class KeyStatsCollectorTest extends TestBase {

    /*
     * In the tests below, the expected number of table stats partition rows is
     * the number of tables + 1 x number of partitions. This includes system
     * tables. The extra table is from the dummy $KV$ table that represents
     * key/value entries.
     */
    private final static int N_PART = 9;

    private final static int N_SYS_TABLES = SysTableRegistry.descriptors.length;

    private final static int N_SYS_TABLE_INDEXES = getNSysTableIndexes();

    private static int getNSysTableIndexes() {
        int nIndexes = 0;
        for (SysTableDescriptor desc : SysTableRegistry.descriptors) {
            nIndexes += desc.getIndexDescriptors().length;
        }
        return nIndexes;
    }

    private static final String namespace = "StatsNS";

    private static final TableImpl userTable =
        TableBuilder.createTableBuilder(namespace, "User")
        .addInteger("id")
        .addString("firstName")
        .addString("lastName")
        .addInteger("age")
        .primaryKey("id")
        .shardKey("id")
        .buildTable();

    private CreateStore createStore = null;
    private TableAPI api;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        suppressSystemError();
        /* Allow key stats to run more frequently */
        KeyStatsCollector.testIgnoreMinimumDurations = true;
    }

    @Override
    public void tearDown() throws Exception {
        if (createStore != null) {
            createStore.shutdown(false);
        }

        resetSystemError();
        KeyStatsCollector.testIgnoreMinimumDurations = false;
        super.tearDown();
    }

    @Test
    /**
     * Test whether the stats gathering is enabled by default
     * @throws Exception
     */
    public void testDefault() throws Exception {
        final int N_SHARDS = 3;
        /* Start 3*3 KVStore */
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      3, /* Storage nodes */
                                      3, /* Replication factor */
                                      N_PART, /* Partitions */
                                      3, /* capacity */
                                      3 * CreateStore.MB_PER_SN,
                                      true, /* useThreads */
                                      null);
        /* Use default for enabling stats, but reduce time values */
        setPolicies(createStore, null);
        createStore.start();
        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);
        api = store.getTableAPI();

        /*
         * Although key and size stats are enabled by default, there won't be
         * any stats until a user table is created.
         */
        checkNoResults();

        final CommandServiceAPI cliAdmin = createStore.getAdmin();
        createTable(userTable, cliAdmin);
        checkResults(N_PART,                          /* partLeases */
                     N_SYS_TABLE_INDEXES * N_SHARDS,  /* indexLeases */
                     (N_SYS_TABLES + 2) * N_PART,     /* partitions */
                     /* Sizes should only need to appear for user tables */
                     v -> v >= N_PART,                /* partsWithSizes */
                     N_SYS_TABLE_INDEXES * N_SHARDS); /* indexes */
    }

    @Test
    public void testBasic() throws Exception {
        final int N_SHARDS = 3;
        /* Start 3*3 KVStore */
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      3, /* Storage nodes */
                                      3, /* Replication factor */
                                      N_PART, /* Partitions */
                                      3, /* capacity */
                                      3 * CreateStore.MB_PER_SN,
                                      true, /* useThreads */
                                      null);
        setPolicies(createStore, false); /* disable stats */
        createStore.start();
        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);
        api = store.getTableAPI();

        final CommandServiceAPI cliAdmin = createStore.getAdmin();

        /* Because stats are disabled, no statistics are gathered. */
        checkNoResults();

        /*
         * Enable statistics, but still no stats because there are no user
         * tables
         */
        changeParameter(RN_SG_ENABLED, "true", cliAdmin);
        checkNoResults();

        /* Create a user table */
        createTable(userTable, cliAdmin);
        checkResults(N_PART,                          /* partLeases */
                     N_SYS_TABLE_INDEXES * N_SHARDS,  /* indexLeases */
                     (N_SYS_TABLES + 2) * N_PART,     /* partitions */
                     v -> v == 0,                     /* partsWithSizes */
                     N_SYS_TABLE_INDEXES * N_SHARDS); /* indexes */

        /* Enable table sizes statistics */
        changeParameter(RN_SG_INCLUDE_STORAGE_SIZE, "true", cliAdmin);
        checkResults(N_PART,                          /* partLeases */
                     N_SYS_TABLE_INDEXES * N_SHARDS,  /* indexLeases */
                     (N_SYS_TABLES + 2) * N_PART,     /* partitions */
                    /*
                     * Sizes should be enabled now, but only need to appear
                     * for user tables
                     */
                     v -> v >= N_PART,                /* partsWithSizes */
                     N_SYS_TABLE_INDEXES * N_SHARDS); /* indexes */

        /* Add an index */
        createIndex("User", "MyIndex", new String[] {"firstName"}, cliAdmin);

        /* Populate data into user table */
        addRows(userTable.getInternalNamespace(), userTable.getFullName(),
            15000, api);
        checkResults(N_PART,                                /* partLeases */
                     (N_SYS_TABLE_INDEXES + 1) * N_SHARDS,  /* indexLeases */
                     (N_SYS_TABLES + 2) * N_PART,           /* partitions */
                     (N_SYS_TABLE_INDEXES + 1) * N_SHARDS); /* indexes */

        /* Remove the user table */
        removeTable("User", cliAdmin);

        /*
         * Although there are now no user tables, the existing stats collection
         * will continue to run, so there will still be stats.
         */
        checkResults(N_PART,                          /* partLeases */
                     N_SYS_TABLE_INDEXES * N_SHARDS,  /* indexLeases */
                     (N_SYS_TABLES + 1) * N_PART,     /* partitions */
                     N_SYS_TABLE_INDEXES * N_SHARDS); /* indexes */
    }

    /**
     * This case is to test scanning a dropped secondary database.
     * Before scanning index secondary database, the database is re-open. And
     * there is a case that the secondary database is already closed, and in the
     * next step will be removed when the database is re-open. The steps is as
     * follows:
     * 1. Scanning thread goes to the hook before reopen secondary database.
     * 2. The test thread executes the plan to remove the index via removing the
     * the table.
     * 3. Secondary databases updating thread goes to the hook before removing
     * the secondary databases after closing the database..
     * 4. Scanning thread reopen the secondary database.
     * 5. Scanning thread goes to the hook after reopen secondary database.
     * 6. Secondary databases updating thread tries to remove the database
     * 7. Secondary databases updating thread goes to the hook after remove the
     * database.
     */
    @Test
    public void testScanningDropSecondaryDatabase() throws Exception {
        final int N_SHARDS = 1;
        /* Start 1*1 KVStore */
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      1, /* Storage nodes */
                                      1, /* Replication factor */
                                      N_PART, /* Partitions */
                                      1, /* capacity */
                                      3 * CreateStore.MB_PER_SN,
                                      true, /* useThreads */
                                      null);
        setPolicies(createStore, true); /* enable stats */
        createStore.start();

        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);
        api = store.getTableAPI();

        final CommandServiceAPI cliAdmin = createStore.getAdmin();

        /* Create a table with an index */
        createTable(userTable, cliAdmin);
        createIndex("User", "MyIndex", new String[] {"firstName"}, cliAdmin);

        /* Populate data into user table */
        addRows(userTable.getInternalNamespace(),
                userTable.getFullName(), 15000, api);

        checkResults(N_PART,                                /* partLeases */
                     (N_SYS_TABLE_INDEXES + 1) * N_SHARDS,  /* indexLeases */
                     (N_SYS_TABLES + 2) * N_PART,           /* partitions */
                     (N_SYS_TABLE_INDEXES + 1) * N_SHARDS); /* indexes */

        final CountDownLatch beforeOpenLatch = new CountDownLatch(1);
        final CountDownLatch beforeRemoveLatch = new CountDownLatch(1);
        final CountDownLatch afterOpenLatch = new CountDownLatch(1);
        final CountDownLatch afterRemoveLatch = new CountDownLatch(1);
        TableIndexScan.BEFORE_OPEN_HOOK = new TestHook<Integer>() {
            @Override
            public void doHook(Integer i) {
                try {
                    beforeOpenLatch.countDown();

                    /*
                     * Waiting secondary database updating thread goes to the
                     * hook before removing database
                     */
                    beforeRemoveLatch.await();
                } catch (InterruptedException e) {

                }
            }
        };

        TableIndexScan.AFTER_OPEN_HOOK = new TestHook<Integer>() {
            @Override
            public void doHook(Integer i) {
                try {
                   afterOpenLatch.countDown();

                   /* Waiting the database is removed */
                   afterRemoveLatch.await();
                } catch (InterruptedException e) {

                }
            }
        };

        TableManager.BEFORE_REMOVE_HOOK = new TestHook<Integer>() {
            @Override
            public void doHook(Integer i) {
                try {
                    beforeRemoveLatch.countDown();

                    /* Waiting the database is reopen */
                    afterOpenLatch.await();
                } catch (InterruptedException e) {

                }
            }
        };

        TableManager.AFTER_REMOVE_HOOK = new TestHook<Integer>() {
            @Override
            public void doHook(Integer i) {
                afterRemoveLatch.countDown();
            }
        };

        /* Waiting scanning thread goes to the hook before re-open database */
        beforeOpenLatch.await();
        /* Remove a table */
        removeTable("User", cliAdmin);

        checkResults(N_PART,                          /* partLeases */
                     N_SYS_TABLE_INDEXES * N_SHARDS,  /* indexLeases */
                     (N_SYS_TABLES + 1) * N_PART,     /* partitions */
                     N_SYS_TABLE_INDEXES * N_SHARDS); /* indexes */
    }

    @Test
    public void testRemoveStatsRecordsByShard() throws Exception {

        /* Start 1*1 KVStore */
        createStore = new CreateStore(kvstoreName,
                                      5000,
                                      9, /* Storage nodes */
                                      3, /* Replication factor */
                                      N_PART, /* Partitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN,
                                      true, /* useThreads */
                                      null);
        setPolicies(createStore, true); /* enable stats */
        createStore.start();

        KVStore store = KVStoreFactory.getStore(createStore.createKVConfig());
        store = createStore.getInternalStoreHandle(store);
        api = store.getTableAPI();

        final CommandServiceAPI cliAdmin = createStore.getAdmin();

        /* Create a table with an index */
        createTable(userTable, cliAdmin);
        createIndex("User", "MyIndex", new String[] {"firstName"}, cliAdmin);

        /* Populate data into user table */
        addRows(userTable.getInternalNamespace(), userTable.getFullName(),
            15000, api);

        final int N_SHARDS_3 = 3;
        checkResults(N_PART,                                  /* partLeases */
                     (N_SYS_TABLE_INDEXES + 1) * N_SHARDS_3,  /* indexLeases */
                     (N_SYS_TABLES + 2) * N_PART,             /* partitions */
                     (N_SYS_TABLE_INDEXES + 1) * N_SHARDS_3); /* indexes */

        /* contract topology */
        contractTopology(cliAdmin);

        final int N_SHARDS_2 = 2;
        assertTrue(
            "Awaiting results",
            new AwaitResults(
                N_PART,                                 /* partLeases */
                (N_SYS_TABLE_INDEXES + 1) * N_SHARDS_2, /* indexLeases */
                (N_SYS_TABLES + 2) * N_PART,            /* partitions */
                t -> true,                              /* partsWithSizes */
                (N_SYS_TABLE_INDEXES + 1) * N_SHARDS_2) /* indexes */
            {
                @Override
                protected boolean condition() {
                    try {
                        return super.condition();
                    } catch (StoreIteratorException sti) {
                        /*
                         * An iteration may fail with a StoreIteratorException
                         * due to the local topology not being updated after
                         * the contraction. If that happens the iteration will
                         * timeout looking for the removed shard. Simply retry
                         * until the topology is updated.
                         */
                        return false;
                    }
                }
            }.await());

        /* Remove a table */
        removeTable("User", cliAdmin);
        checkResults(N_PART,                            /* partLeases */
                     N_SYS_TABLE_INDEXES * N_SHARDS_2,  /* indexLeases */
                     (N_SYS_TABLES + 1) * N_PART,       /* partitions */
                     N_SYS_TABLE_INDEXES * N_SHARDS_2); /* indexes */
    }

    /* Contract topology from 3*1 to 2*1 */
    private void contractTopology(CommandServiceAPI cliAdmin)
            throws RemoteException {
        cliAdmin.cloneStorageNodePool("clonedPool", "CreateStorePool");
        cliAdmin.removeStorageNodeFromPool("clonedPool", new StorageNodeId(7));
        cliAdmin.removeStorageNodeFromPool("clonedPool", new StorageNodeId(8));
        cliAdmin.removeStorageNodeFromPool("clonedPool", new StorageNodeId(9));
        cliAdmin.copyCurrentTopology("contract");
        cliAdmin.contractTopology("contract", "clonedPool");

        int rsnId = cliAdmin.createDeployTopologyPlan("contract topology",
                                                      "contract", null);
        cliAdmin.approvePlan(rsnId);
        cliAdmin.executePlan(rsnId, false);
        cliAdmin.awaitPlan(rsnId, 0, null);
    }

    /**
     * Returns the number of rows in a table.
     */
    private int getTableLine(String tableName) {
        return getTableLine(tableName, row -> true);
    }

    /**
     * Returns the number of rows in a table that satisfy a predicate.
     */
    private int getTableLine(String tableName,
                             Predicate<Row> predicate) {
        /* no namespace -- these are system tables */
        final PrimaryKey pk = waitForTable(api, tableName).createPrimaryKey();

        final TableIterator<Row> iter = api.tableIterator(pk, null, null);
        try {
            int lineNum = 0;
            while (iter.hasNext()) {
                if (predicate.test(iter.next())) {
                    lineNum++;
                }
            }
            return lineNum;
        } finally {
            iter.close();
        }
    }

    /**
     * Returns whether a row obtained from the TableStatsPartition contains a
     * non-zero table size value, and also performs some consistency checks.
     */
    private static boolean getHasTableSize(Row row) {
        final int avgKeySize =
            row.get(TableStatsPartitionDesc.COL_NAME_AVG_KEY_SIZE)
            .asInteger().get();
        final long tableSize =
            row.get(TableStatsPartitionDesc.COL_NAME_TABLE_SIZE)
            .asLong().get();

        /* Table size should be zero since there were no entries */
        if (avgKeySize == 0) {
            assertTrue("Zero average key size, but table size is " + tableSize,
                       tableSize == 0);
        }

        /* Not enabled */
        if (tableSize == 0) {
            return false;
        }

        /* Table size should be bigger than average key size */
        assertTrue("Table size " + tableSize +
                   " is smaller than average key size " + avgKeySize,
                   tableSize >= avgKeySize);
        return true;
    }

    private void changeParameter(String paramName, String paramValue,
                                 CommandServiceAPI cliAdmin)
                                         throws RemoteException {
        ParameterMap map = new ParameterMap();
        map.setType(ParameterState.REPNODE_TYPE);
        map.setParameter(paramName, paramValue);

        int p = cliAdmin.createChangeAllParamsPlan("change stats interval",
                                                   null, map);
        cliAdmin.approvePlan(p);
        cliAdmin.executePlan(p, false);
        cliAdmin.awaitPlan(p, 0, null);
    }

    private void createIndex(String tableName, String indexName,
                             String[] fields, CommandServiceAPI cliAdmin)
                                     throws RemoteException {
        int planId = cliAdmin.createAddIndexPlan("AddIndex", namespace,
                                                 indexName, tableName, fields,
                                                 null, true, null);
        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);
        cliAdmin.awaitPlan(planId, 0, null);
    }

    private void createTable(TableImpl table, CommandServiceAPI cliAdmin)
            throws RemoteException {
        int planId = cliAdmin.createAddTablePlan(
                         "AddTable",
                         table.getInternalNamespace(),
                         table.getFullName(),
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
    }

    private void removeTable(String tableName, CommandServiceAPI cliAdmin)
            throws RemoteException {
        int planId = cliAdmin.createRemoveTablePlan("RemoveTable",
                                                    namespace,
                                                    tableName);

        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);
        cliAdmin.awaitPlan(planId, 0, null);
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

    private static void setPolicies(CreateStore cstore, Boolean enabled) {
        ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.AP_CHECK_ADD_INDEX, "1 s");
        if (enabled != null) {
            map.setParameter(RN_SG_ENABLED, (enabled ? "true" : "false"));
            map.setParameter(RN_SG_INCLUDE_STORAGE_SIZE,
                             (enabled ? "true" : "false"));
        }
        /*
         * Related to statistics gathering, to reduce duration when
         * scanning.
         */
        map.setParameter(ParameterState.RN_SG_INTERVAL, "1 s");
        map.setParameter(ParameterState.RN_SG_LEASE_DURATION, "1 s");
        map.setParameter(ParameterState.RN_SG_SLEEP_WAIT, "5 ms");
        cstore.setPolicyMap(map);
    }

    private void checkResults(int partitionLeases,
                              int indexLeases,
                              int partitions,
                              int indexes) {
        checkResults(partitionLeases, indexLeases, partitions,
                     v -> true /* partitionsWithSizes */, indexes);
    }

    /**
     * Check for the expected items in the various statistics tables.
     *
     * @param partitionLeases the number of entries in the PartitionStatsLease
     * table, one per partition
     * @param indexLeases the number of entries in the IndexStatsLease table,
     * one per index
     * @param partitions the number of entries in the TableStatsPartition
     * table, one for each partition for each table, including the dummy $KV$
     * table that represents key/value entries
     * @param partsWithSizes a predicate to call with the number of entries in
     * the TableStatsPartition table that have non-zero table sizes
     * @param indexes the number of entries in the TableStatsIndex table, one
     * for each shard for each table for each index
     */
    private void checkResults(int partitionLeases,
                              int indexLeases,
                              int partitions,
                              Predicate<Integer> partsWithSizes,
                              int indexes) {
        new AwaitResults(partitionLeases, indexLeases, partitions,
                         partsWithSizes, indexes).check();
    }

    private void checkNoResults() throws Exception {
        /*
         * Since we want to confirm there is no change from the original
         * values, need to wait the entire time and then check
         */
        Thread.sleep(100000);
        new AwaitResults(0, 0, 0, v -> true, 0).check(true);
    }

    private class AwaitResults extends PollCondition {
        final int partitionLeases;
        final int indexLeases;
        final int partitions;
        final Predicate<Integer> partsWithSizes;
        final int indexes;

        AwaitResults(int partitionLeases,
                     int indexLeases,
                     int partitions,
                     Predicate<Integer> partsWithSizes,
                     int indexes) {
            super(500, 100000);
            this.partitionLeases = partitionLeases;
            this.indexLeases = indexLeases;
            this.partitions = partitions;
            this.partsWithSizes = partsWithSizes;
            this.indexes = indexes;

        }

        void check() {
            if (!await()) {
                check(true);
            }
        }

        @Override
        protected boolean condition() {
            return check(false);
        }

        private boolean check(boolean throwOnFailure) {
            final int pl = getTableLine(PartitionStatsLeaseDesc.TABLE_NAME);
            final int il = getTableLine(IndexStatsLeaseDesc.TABLE_NAME);
            final int p = getTableLine(TableStatsPartitionDesc.TABLE_NAME);
            final int pws = getTableLine(TableStatsPartitionDesc.TABLE_NAME,
                                         r -> getHasTableSize(r));
            final int i = getTableLine(TableStatsIndexDesc.TABLE_NAME);
            final StringBuilder sb = new StringBuilder();
            boolean result = true;
            if (partitionLeases != pl) {
                result = false;
                sb.append("\npartitionLeases: expected " + partitionLeases +
                          " found " + pl);
            }
            if (indexLeases != il) {
                result = false;
                sb.append("\nindexLeases: expected " + indexLeases +
                          " found " + il);
            }
            if (partitions != p) {
                result = false;
                sb.append("\npartitions: expected " + partitions +
                          " found " + p);
            }
            if (!partsWithSizes.test(pws)) {
                result = false;
                sb.append("\npartsWithSizes: returned false for " + pws);
            }
            if (indexes != i) {
                result = false;
                sb.append("\nindexes: expected " + indexes + " found " + i);
            }
            if (!result && throwOnFailure) {
                fail("Check results failed:" + sb);
            }
            return result;
        }
    }
}
