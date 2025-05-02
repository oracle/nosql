/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.rep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.fault.RNUnavailableException;
import oracle.kv.impl.rep.PartitionMigrationTest.BaseHook;
import oracle.kv.impl.rep.PartitionMigrationTest.ReadHook;
import oracle.kv.impl.rep.admin.RepNodeAdmin.PartitionMigrationState;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import com.sleepycat.je.Database;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryIntegrityException;
import com.sleepycat.je.Transaction;

import org.junit.Test;

/**
 * Tests for the RepNode behaviors under SecondaryIntegrityException
 */
public class SecondaryIntegrityExceptionTest extends RepNodeTestBase {

    private KVRepTestConfig config;
    private TableMetadata md;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        md = new TableMetadata(false);
    }

    @Override
    public void tearDown() throws Exception {
        if (config != null) {
            config.stopRepNodeServices();
            config = null;
        }
        md = null;
        super.tearDown();
    }

    /* Starts up a 1x2 store */
    private void start1x2() throws Exception {
        config = new KVRepTestConfig(this,
                                     1, /* nDC */
                                     1, /* nSN */
                                     2, /* repFactor */
                                     10 /* nPartitions */);
        config.startRepNodeServices();
    }

    /* Starts up a 2x2 store */
    private void start2x2() throws Exception {
        config = new KVRepTestConfig(this,
                                     1, /* nDC */
                                     2, /* nSN */
                                     2, /* repFactor */
                                     10 /* nPartitions */);
        config.startRepNodeServices();
    }

    /**
     * Tests SIE handling during a client operation.
     */
    @Test
    public void testRequestHandling() throws Exception {
        start1x2();

        final int NUM_ROWS = 160;

        final TableImpl table1 = createTable("table1");
        final TableImpl table2 = createTable("table2");

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();
        TableTest.addRows(table1, NUM_ROWS, apiImpl);

        /* Insert a block to population */
        final WaitHook waitHook = new WaitHook();
        getMaster().getTableManager().populateHook = waitHook;

        corruptSecondaryDatabase(table1);

        /* Write to table 2 should suceed */
        TableTest.addRows(table2, NUM_ROWS, apiImpl);

        /* Writing to table1 should fail */
        try {
            TableTest.addRows(table1, NUM_ROWS, 1, apiImpl);
            fail("Table should be corrupted");
        } catch (FaultException e) {
            final String msg = e.getMessage();
            if (!msg.contains("Integrity problem")) {
                fail("Expected integrity message, got " + msg);
            }
        }

        /* Wait for popultion to start on the failed index */
        waitHook.waitForHook();

        /**
         * At this point, the secondary should not be available on any
         * node in the shard.
         */
        assert config.getTopology().getRepGroupIds().size() == 1;
        for (RepNode rn : config.getRNs()) {
            final Database db = rn.getTableManager().getIndexDB(null,
                                                          "FirstName",
                                                          table1.getFullName());
            if (db != null) {
                fail("Database should be unavailible on " + rn.getRepNodeId());
            }
        }

        /* Release population thread */
        getMaster().getTableManager().populateHook = null;
        waitHook.releaseAll();

        /* Eventually table1's index should be rebuilt */
        verifySecondaryRecords(table1, NUM_ROWS);
    }

    /**
     * Tests SIE handling during index population.
     */
    @Test
    public void testIndexPopulating() throws Exception {
        start1x2();

        final int NUM_ROWS = 160;

        final TableImpl table = addTable("table");
        /* Update for table */
        for (RepNode rn : config.getRNs()) {
            TableTest.updateMetadata(rn, md);
        }

        /* Add some rows */
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();
        TableTest.addRows(table, NUM_ROWS, apiImpl);

        addIndex(table);

        /* Set a hook for 1 SIE in the populate thread */
        getMaster().getTableManager().populateHook = new SIEHook(0, 1);

        /* Start updating */
        for (RepNode rn : config.getRNs()) {
            TableTest.updateMetadata(rn, md);
        }

        /* We should still be able to finish the population */
        for (RepNode rn : config.getRNs()) {
            TableTest.waitForPopulate(rn, "FirstName", table);
        }
        verifySecondaryRecords(table, NUM_ROWS);
    }

    /**
     * Tests index population retry limit.
     */
    @Test
    public void testIndexPopulatingFail() throws Exception {
        start1x2();

        final TableImpl table = addTable("table");
        /* Update for table */
        for (RepNode rn : config.getRNs()) {
            TableTest.updateMetadata(rn, md);
        }

        /* Add some rows */
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();
        TableTest.addRows(table, 4096, apiImpl);

        addIndex(table);

        /*
         * Set a hook for 10 SIEs in the populate thread which should
         * be over the retry limit. This will result in the index create
         * to fail.
         */
        getMaster().getTableManager().populateHook = new SIEHook(0, 10);

        /* Start updating */
        for (RepNode rn : config.getRNs()) {
            TableTest.updateMetadata(rn, md);
        }

        try {
            /* One of these will fail. */
            for (RepNode rn : config.getRNs()) {
                TableTest.waitForPopulate(rn, "FirstName", table);
            }
            fail("Populate should fail");
        } catch (Exception ex) {
            /* Expected */
        }
    }

    /**
     * Tests SIE handling during table metadata update.
     */
    @Test
    public void testMaintenanceThreadUpdate() throws Exception {
        start1x2();

        final int NUM_ROWS = 16;

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableImpl table1 = createTable("table1");
        TableTest.addRows(table1, NUM_ROWS, apiImpl);

        /* This will cause an SIE durning an update. */
        getMaster().getTableManager().updateHook = new SIEHook(0, 1);

        /* Add a table so that the MD is updated */
        addTable("table2");
        /* Update for table */
        for (RepNode rn : config.getRNs()) {
            TableTest.updateMetadata(rn, md);
        }
        verifySecondaryRecords(table1, NUM_ROWS);
    }

    /**
     * Tests SIE handling during secondary cleaning on the partition
     * migration source. Secondary cleaning takes place on the source
     * after a successful migration.
     */
    @Test
    public void testSecondaryCleaningOnSource() throws Exception {
        /* Need two groups for this test */
        start2x2();

        final int NUM_ROWS = 500;

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        TableImpl table1 = createTable("table1");
        TableTest.addRows(table1, NUM_ROWS, apiImpl);
        for (RepNode rn : config.getRNs()) {
            TableTest.waitForPopulate(rn, "FirstName", table1);
        }

        final PartitionId p1 = new PartitionId(1);
        final RepGroupId rg1 = new RepGroupId(1);
        final RepNode source = config.getRN(new RepNodeId(1, 1));
        final RepNode target = config.getRN(new RepNodeId(2, 1));

        /* This will cause an SIE durning secondary cleaning on source. */
        source.getTableManager().cleaningHook = new SIEHook(0, 1);

        target.migratePartition(p1, rg1);
        PartitionMigrationTest.waitForMigrationState(
            target, p1, PartitionMigrationState.SUCCEEDED);

        final Topology topo = source.getTopology().getCopy();
        topo.updatePartition(p1, new RepGroupId(target.getRepNodeId().
                                                getGroupId()));
        for (oracle.kv.impl.rep.RepNode rn : config.getRNs()) {
            rn.updateMetadata(topo);
        }
        verifySecondaryRecords(table1, NUM_ROWS);
    }

    /**
     * Tests SIE handling during secondary cleaning on the partition
     * migration target. Secondary cleaning takes place on the target
     * after a migration failure.
     */
    @Test
    public void testSecondaryCleaningOnTarget() throws Exception {
        /* Need two groups for this test */
        start2x2();

        final int NUM_ROWS = 500;

        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        final TableAPI apiImpl = kvs.getTableAPI();

        final TableImpl table1 = createTable("table1");
        TableTest.addRows(table1, NUM_ROWS, apiImpl);
        for (RepNode rn : config.getRNs()) {
            TableTest.waitForPopulate(rn, "FirstName", table1);
        }

        final PartitionId p1 = new PartitionId(1);
        final RepGroupId rg1 = new RepGroupId(1);
        final RepNode source = config.getRN(new RepNodeId(1, 1));
        final RepNode target = config.getRN(new RepNodeId(2, 1));

        /* This will cause an SIE durning secondary cleaning on target. */
        target.getTableManager().cleaningHook = new SIEHook(0, 1);

        /* Start and hold the migration. */
        final ReadHook readHook = new ReadHook();
        source.getMigrationManager().setReadHook(readHook);
        target.migratePartition(p1, rg1);

        /*
         * Let some records go through so there is something to clean on
         * the target after the migration fails.
         */
        for (int i = 0; i < 10; i++) {
            readHook.waitForHook();
            readHook.releaseHook();
        }
        /* Fail the migration */
        readHook.waitForHook();
        readHook.throwException(new RuntimeException("Failing source"));

        /* Wait for the target to notice */
        PartitionMigrationTest.waitForMigrationState(
            target, p1, PartitionMigrationState.PENDING);

        /**
         * The target should return to running state once the secondary is
         * cleaned
         */
        PartitionMigrationTest.waitForMigrationState(
            target, p1, PartitionMigrationState.RUNNING);

        /* Clear the hook so that the source can restart */
        source.getMigrationManager().setReadHook(null);
        readHook.releaseAll();

        verifySecondaryRecords(table1, NUM_ROWS);

        PartitionMigrationTest.waitForMigrationState(
            target, p1, PartitionMigrationState.SUCCEEDED);

        Thread.sleep(10000);
        verifySecondaryRecords(table1, NUM_ROWS);
    }

    private TableImpl createTable(String tableName) {
        TableImpl table = addTable(tableName);
        addIndex(table);
        for (RepNode rn : config.getRNs()) {
            TableTest.updateMetadata(rn, md);
        }
        for (RepNode rn : config.getRNs()) {
            TableTest.waitForPopulate(rn, "FirstName", table);
        }
        return table;
    }

    private TableImpl addTable(String tableName) {
        return TableTest.addTable(md,
                                  TableBuilder.createTableBuilder(tableName)
                                  .addInteger("id")
                                  .addString("firstName")
                                  .addString("lastName")
                                  .addInteger("age")
                                  .primaryKey("id")
                                  .shardKey("id")
                                  .buildTable());
    }

    /**
     * Verifies that the store has the specified number of secondary records
     * for the given table. The count will ignore exceptions to account for
     * the secondary being re-populated.
     */
    private void verifySecondaryRecords(Table table, int expectedRecords) {
        final AtomicInteger nRecords = new AtomicInteger(0);

        boolean success = new PollCondition(500, 15000) {
            @Override
            protected boolean condition() {
                try {
                    nRecords.set(0);
                    for (RepNode rn : config.getRNs()) {
                        /*
                         * getIndexDB() will thrown an exception if the index
                         * is not ready. In that case we retry.
                         */
                        if (rn.getIsAuthoritativeMaster()) {
                            SecondaryDatabase db =
                          rn.getIndexDB(null, "FirstName", table.getFullName());
                            int count = TableTest.countSecondaryRecords(db);
                            nRecords.addAndGet(count);
                        }
                    }
                    return true;
                } catch (Exception ire) {
                    return false;
                }
            }
        }.await();
        assertTrue(success);

        assertEquals("Number of records incorrect for secondary db",
                     expectedRecords, nRecords.get());
    }

    private void addIndex(TableImpl table) {
        TableTest.addIndex(md, "FirstName", table, "firstName");
        md.updateIndexStatus(null, "FirstName", table.getFullName(),
                             IndexImpl.IndexStatus.READY);
    }

    /**
     * Returns the first master found.
     */
    private RepNode getMaster() {
        for (RepNode rn : config.getRNs()) {
            if (rn.getIsAuthoritativeMaster()) {
                return rn;
            }
        }
        fail("can't find a master");
        return null;
    }

    /*
     * Simple hook to wait.
     */
    private static class WaitHook extends BaseHook<Database> {
        @Override
        public void doHook(Database db) {
            waitForRelease();
        }
    }

    /*
     * Hook to corrupt a secondary database so that an access to it
     * will throw a SecondaryIntegrityException.
     */
    private static class SIEHook implements TestHook<Database> {
        int count;
        int nSIEs;

        SIEHook(int count, int nSIEs) {
            this.count = count;
            this.nSIEs = nSIEs;
        }

        @Override
        public void doHook(Database db) {
            if (db == null) {
                return;
            }
            if (nSIEs <= 0) {
                return;
            }
            count--;
            if (count > 0) {
                return;
            }
            setSIE(db);
            nSIEs--;
        }
    }

    private void corruptSecondaryDatabase(Table table) throws Exception {
        final RepNode rn = getMaster();
        while (true) {
            SecondaryDatabase db = null;
            try {
                db = rn.getIndexDB(null, "FirstName", table.getFullName());
            } catch (RNUnavailableException e) {
                Thread.sleep(1000);
                continue;
            }
            if (db == null) {
                Thread.sleep(1000);
                continue;
            }
            setSIE(db);
            break;
        }
    }

    @SuppressWarnings("unused")
    private static void setSIE(Database db) {
        Transaction txn = db.getEnvironment().beginTransaction(null, null);
        /*
         * Creating an SIE on the secondary database so that it will
         * mark the database with the problem. This exact exception
         * will be set as the corrupt cause and thrown to the client.
         */
        new SecondaryIntegrityException(db, true, DbInternal.getLocker(txn),
                                        "", db.getDatabaseName(), "",
                                        null, null, 0, 0, null);
        txn.abort();
    }
}
