/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep.migration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.FileHandler;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStoreFactory;
import oracle.kv.Version;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableTestBase;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.WriteOptions;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.contextlogger.LogFormatter;

import org.junit.Test;

/**
 * This is a integration unit test that verifies the consistency of row
 * creation times before and after a partition migration. It uses createStore
 * to set up the test environment.
 * <p>
 * The test performs the following steps:
 * 1. Inserts and updates rows in a table prior to migration.
 * 2. Initiates a partition migration via a topology change.
 * 3. Performs concurrent updates to the rows during the partition migration.
 * 4. Validates that the row creation times remain unchanged after migration.
 * <p>
 * This test ensures that partition migration preserves row creation time
 * metadata even under concurrent write workloads during Partition Migration.
 */
public class RowCreationTimeTest extends TableTestBase {

    private static final boolean trace_on_screen = false;
    private static final int NUM_INIT_ROWS = 100 * 1024;
    private static final WriteOptions WRITE_OPTIONS =
        new WriteOptions(Durability.COMMIT_NO_SYNC, 10000, MILLISECONDS);
    private static final ReadOptions READ_OPTIONS =
        new ReadOptions(Consistency.NONE_REQUIRED, 10000, MILLISECONDS);
    private Thread writerThread;

    private ArrayList<Long> beforeMigrationRowsCreationTime;
    private ArrayList<Long> afterMigrationRowsCreationTime;

    @Override
    public void setUp() throws Exception {
        beforeMigrationRowsCreationTime = new ArrayList<>();
        afterMigrationRowsCreationTime = new ArrayList<>();
        kvstoreName = "mystore";
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        createStore = new CreateStore(kvstoreName,
                                      startPort,
                                      2, /* n SNs */
                                      1, /* rf */
                                      10, /* n partitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN,
                                      true,
                                      null);
        createStore.setPoolSize(1); /* reserve one SN for later. */
        createStore.start();
        store = KVStoreFactory.getStore(createKVConfig(createStore));
        addLoggerFileHandler();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testRowCreationTimeIntegrationTest() throws Exception {
        final CommandServiceAPI cs = createStore.getAdmin();
        /*
         * Initial topology :
         * rg1:[[rg1-rn1] sn=sn1]
         * Partitions on rg1:[1,2,3,4,5,6,7,8,9,10]
         */
        trace(TopologyPrinter.printTopology(cs.getTopology()));
        String tableName = "mytable";
        /* Create table and insert initial rows */
        final TableAPI tableAPI = store.getTableAPI();
        executeDdl("CREATE TABLE " + tableName + " " +
                   "(id INTEGER, firstName STRING, lastName STRING," +
                   "age INTEGER, PRIMARY KEY (id))");
        final Table tbl = tableAPI.getTable(tableName);
        insertRowsIntoTable(tbl);
        trace(NUM_INIT_ROWS + " rows loaded into table " + tbl.getFullName());
        /* Perform pre-migration updates on rows to make creation time
         * different from modification time
         */
        updateWorkLoad(tbl);

        trace("Table data before migration : ");
        traceRowsFromTable(tbl);

        final String topo = "newTopo";
        /* New topology and redistribute to initiate partition migration */
        cs.copyCurrentTopology(topo);
        cs.redistributeTopology(topo, "AllStorageNodes");

        trace("Expand store to two shards");
        final int planId = cs.createDeployTopologyPlan("deploy-new-topo",
                                                       topo,
                                                       null);
        cs.approvePlan(planId);

        /* ====== Start background writer thread ====== */
        setUpConcurrentWriteThread(tableAPI, tableName);
        /* Start concurrent writes during migration */
        writerThread.start();

        try {
            cs.executePlan(planId, false);
            cs.awaitPlan(planId, 0, null);
        } finally {
            writerThread.interrupt();
            writerThread.join();
        }
        /* ====== End background writer thread ====== */

        trace("Expanded store to two shards");
        /*
         * Final topology :
         * rg1:[[rg1-rn1] sn=sn1]
         * rg2:[[rg2-rn1] sn=sn2]
         * Partitions on rg1:[6,7,8,9,10]
         * Partitions on rg2:[1,2,3,4,5]
         */
        trace(TopologyPrinter.printTopology(cs.getTopology()));

        /* Fetch data post-migration */
        getRowsFromTableAfterMigration(tbl);

        trace("Table data after migration : ");
        traceRowsFromTable(tbl);

        assertFalse(beforeMigrationRowsCreationTime.isEmpty());

        trace("Checking creation time of rows before and after migration");
        /* Assert that creation time list before and after are of equal size */
        assertEquals("The before and after creation time list do not have the" +
                     " same size",
                     beforeMigrationRowsCreationTime.size(),
                     afterMigrationRowsCreationTime.size()
        );

        /* Assert that both lists contain the same values at each position */
        for (int i = 0; i < beforeMigrationRowsCreationTime.size(); i++) {
            assertEquals("Values at row " + i + 1 + " do not match",
                         beforeMigrationRowsCreationTime.get(i),
                         afterMigrationRowsCreationTime.get(i));
        }
        trace("Creation time of rows before and after migration are same");
    }

    /**
     * Create a random row with given ID
     */
    private RowImpl makeRandomRow(TableImpl table, int which) {
        final RowImpl row = table.createRow();
        row.put("id", which);
        row.put("firstName",
                "FirstName-" + ThreadLocalRandom.current().nextInt(1, 1000));
        row.put("lastName",
                "lastName-" + ThreadLocalRandom.current().nextInt(1, 1000));
        row.put("age", ThreadLocalRandom.current().nextInt(20, 80));
        return row;
    }


    /**
     * Insert initial rows and capture their creation time before PM.
     */
    private void insertRowsIntoTable(Table tbl) {
        final TableAPI tableAPI = store.getTableAPI();
        Table table = tableAPI.getTable(tbl.getName());
        PrimaryKey pk = table.createPrimaryKey();
        for (int i = 0; i < NUM_INIT_ROWS; i++) {
            RowImpl row = makeRandomRow((TableImpl) tbl, i);
            tableAPI.putIfAbsent(row, null, WRITE_OPTIONS);
            pk.put("id", i);
            row = (RowImpl) tableAPI.get(pk, READ_OPTIONS);
            beforeMigrationRowsCreationTime.add(row.getCreationTime());
        }
    }

    /**
     * Perform update workload before migration
     */
    private void updateWorkLoad(Table tbl) {
        final TableAPI tableAPI = store.getTableAPI();
        for (int i = 0; i < NUM_INIT_ROWS; i++) {
            final RowImpl row = makeRandomRow((TableImpl) tbl, i);
            final Version ver = tableAPI.putIfPresent(row, null, WRITE_OPTIONS);
            assertNotNull("Fail to update row=" + row.toJsonString(false), ver);
        }
    }

    /**
     * Set up background writer thread for concurrent updates
     */
    private void setUpConcurrentWriteThread(TableAPI tableAPI,
                                            String tableName) {
        final Table tblForThread = tableAPI.getTable(tableName);
        writerThread = new Thread(() -> {
            try {
                int counter = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    int id = counter % NUM_INIT_ROWS;
                    RowImpl row = makeRandomRow((TableImpl) tblForThread, id);
                    final Version ver = tableAPI.putIfPresent(row,
                                                              null,
                                                              WRITE_OPTIONS);
                    assertNotNull("Fail to update row=" +
                                  row.toJsonString(false), ver);
                    /*
                     * It provides a reliable point for the thread to detect
                     * interrupt() signals and exit gracefully, preventing
                     * it from running indefinitely during long-running
                     * operations.
                     */
                    Thread.sleep(1);
                    counter++;
                }
            } catch (InterruptedException e) {
                /* Allow thread to exit gracefully */
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                fail("Exception in writer thread: " + e.getMessage());
            }
        });
    }

    /**
     * Fetch rows after migration and capture their creation time
     */
    private void getRowsFromTableAfterMigration(Table tbl) {
        final TableAPI tableAPI = store.getTableAPI();
        for (int i = 0; i < NUM_INIT_ROWS; i++) {
            Table table = tableAPI.getTable(tbl.getName());
            PrimaryKey pk = table.createPrimaryKey();
            pk.put("id", i);
            RowImpl row = (RowImpl) tableAPI.get(pk, READ_OPTIONS);
            afterMigrationRowsCreationTime.add(row.getCreationTime());
            assertTrue(row.getCreationTime() != row.getLastModificationTime());
        }
    }

    private void trace(String msg) {
        logger.info(msg);

        if (trace_on_screen) {
            System.out.println(msg);
        }
    }

    private void traceRowsFromTable(Table tbl) {
        final TableAPI tableAPI = store.getTableAPI();
        KVStoreImpl storeImpl = (KVStoreImpl) store;
        int rowsToPrint = 100;
        for (int i = 0; i < rowsToPrint; i++) {
            Table table = tableAPI.getTable(tbl.getName());
            PrimaryKey pk = table.createPrimaryKey();
            pk.put("id", i);
            RowImpl row = (RowImpl) tableAPI.get(pk, READ_OPTIONS);
            trace("row : " + (i + 1) + "   :   " + row.toString());
            trace("Creation time: " + row.getCreationTime());
            trace("Modification time: " + row.getLastModificationTime());
            trace("PartitionId : " +
                  storeImpl.getPartitionId(row.getPrimaryKey(false)));
        }
    }

    private void addLoggerFileHandler() throws IOException {
        final String fileName = "testlog";
        final String path = TestUtils.getTestDir().getAbsolutePath();
        final File loggerFile = new File(new File(path), fileName);
        final FileHandler handler =
            new FileHandler(loggerFile.getAbsolutePath(), false);
        handler.setFormatter(new LogFormatter(null));
        tearDowns.add(() -> logger.removeHandler(handler));
        logger.addHandler(handler);
        logger.info("Add test log file handler: path=" + path +
                    ", log file name=" + fileName +
                    ", file exits?=" + loggerFile.exists());
    }
}

