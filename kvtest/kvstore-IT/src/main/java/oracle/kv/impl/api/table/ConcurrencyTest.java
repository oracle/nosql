/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.ExecutionFuture;
import oracle.kv.StatementResult;
import oracle.kv.TestClassTimeoutMillis;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.table.Index;
import oracle.kv.table.Row;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.WriteOptions;
import oracle.kv.util.DDLTestUtils;

import org.junit.Test;

/*
 * A class to test concurrent operations on table metadata.
 */
/* Increase test timeout to 30 minutes -- test can take 15 minutes */
@TestClassTimeoutMillis(30*60*1000)
public class ConcurrencyTest extends TableTestBase {

    static final TableIteratorOptions UNORDERED_OPTIONS =
        new TableIteratorOptions(Direction.UNORDERED, Consistency.ABSOLUTE, 0,
                                 null);

    @Test
    public void testBasic() throws Exception {
        TableImpl t0;
        TableImpl t1;
        TableImpl t2;
        TableImpl t3;

        final CommandServiceAPI cs = createStore.getAdmin();
        TableImpl table = TableBuilder.createTableBuilder("foo")
            .addInteger("id")
            .addString("name")
            .primaryKey("id")
            .buildTable();
        TableImpl table1 = TableBuilder.createTableBuilder("foo1")
            .addInteger("id")
            .addString("name")
            .primaryKey("id")
            .buildTable();
        TableImpl table2 = TableBuilder.createTableBuilder("foo2")
            .addInteger("id")
            .addString("name")
            .primaryKey("id")
            .buildTable();
        TableImpl table3 = TableBuilder.createTableBuilder("foo3")
            .addInteger("id")
            .addString("name")
            .primaryKey("id")
            .buildTable();

        ExecutionFuture future1 = addTableAsync(table, true);
        ExecutionFuture future2 = addTableAsync(table1, true);
        ExecutionFuture future3 = addTableAsync(table2, true);
        ExecutionFuture future4 = addTableAsync(table3, true);

        StatementResult result1 = future1.get(50, TimeUnit.SECONDS);
        DDLTestUtils.checkSuccess(future1, result1);
        StatementResult result2 = future2.get(50, TimeUnit.SECONDS);
        DDLTestUtils.checkSuccess(future2, result2);
        StatementResult result3 = future3.get(50, TimeUnit.SECONDS);
        DDLTestUtils.checkSuccess(future3, result3);
        StatementResult result4 = future4.get(50, TimeUnit.SECONDS);
        DDLTestUtils.checkSuccess(future4, result4);


        t0 = getTable("foo");
        assertNotNull(t0);

        t1 = getTable("foo1");
        assertNotNull(t1);

        t2 = getTable("foo2");
        assertNotNull(t2);

        t3 = getTable("foo3");
        assertNotNull(t3);

        /*
         * Add some records to be sure they disappear
         */
        populateTables(500, "foo", "foo1", "foo2", "foo3");

        /* try to re-add a table with the same name. should fail */
        addTableAsync(table2, false);

        /*
         * Add indexes. Population will take time because the tables have data
         */
        int planId = addIndexAsync(t0, "foo", new String[] {"name"}, true, cs);

        /* add second index on same table - this should work */
        int planId1 = addIndexAsync(t0, "idxExtra",
                      new String[] {"id", "name"}, true, cs);

        waitForPlan(planId, true, cs);
        waitForPlan(planId1, true, cs);

        planId1 = addIndexAsync(t1, "foo1", new String[] {"name"}, true, cs);
        int planId2 = addIndexAsync(t2, "foo2", new String[] {"name"}, true, cs);
        int planId3 = addIndexAsync(t3, "foo3", new String[] {"name"}, true, cs);
        waitForPlan(planId2, true, cs);
        waitForPlan(planId1, true, cs);
        waitForPlan(planId, true, cs);
        waitForPlan(planId3, true, cs);

        countIndexRecords(500, "foo", "foo1", "foo2", "foo3");

        planId = removeTableAsync("foo", true, cs);
        planId1 = removeTableAsync("foo1", true, cs);
        planId2 = removeTableAsync("foo2", true, cs);
        planId3 = removeTableAsync("foo3", true, cs);

        waitForPlan(planId, true, cs);
        waitForPlan(planId1, true, cs);
        waitForPlan(planId2, true, cs);
        waitForPlan(planId3, true, cs);

        t2 = getTable("foo2");

        /*
        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
        Date dateobj = new Date();
        System.out.println(df.format(dateobj));
        */
        assertNull(t2);
        t0 = getTable("foo");
        assertNull(t0);
        t1 = getTable("foo1");
        assertNull(t1);
        t3 = getTable("foo3");
        assertNull(t3);
    }

//    @Test
//    public void testIndexPopulate() throws Exception {
//        final CommandServiceAPI cs = createStore.getAdmin();
//        final TableImpl table = TableBuilder.createTableBuilder("foo")
//            .addInteger("id")
//            .addString("name")
//            .primaryKey("id")
//            .buildTable();
//        int planId = addTableAsync(table, true, cs);
//        waitForPlan(planId, true, cs);
//
//        final TableImpl t0 = getTable("foo");
//        assertNotNull(t0);
//
//        populateTables(5000, "foo");
//    }

    private void populateTables(int numRecords, String... names) {
        final WriteOptions options =
            new WriteOptions().setDurability(Durability.COMMIT_NO_SYNC);
        for (String tableName : names) {
            final TableImpl table = getTable(tableName);
            assertNotNull(table);

            final Row row = table.createRow();
            for (int i = 0; i < numRecords; i++) {
                row.put("id", i)
                   .put("name", ("myname"+i));
                tableImpl.put(row, null, options);
            }
        }
    }

    /*
     * This goes with test above
     */
    private void countIndexRecords(int numExpected, String... names) {
        for (String tableName : names) {
            TableImpl table = getTable(tableName);
            if (table == null) {
                System.out.println("Populate: no such table: " + tableName);
                continue;
            }
            /* index name is same as table name */
            Index index = table.getIndex(tableName);
            assertEquals(numExpected,
                         countIndexRecords1(index.createIndexKey(), null));
        }
    }

    @Test
    public void testWithThreads() throws Exception {
        final int numThreads = 10;
        for (int i = 0; i < numThreads; i++) {
            /* 10 iterations, 10 tables */
            startThread(new MDThread(10, 10, ("thr_" + i)));
        }
        joinAllThreads();
    }

    @Test
    public void testConcurrentDDL() {
        final String createTable =
            "create table if not exists users(id integer, primary key(id))";

        final int numThreads = 4;
        for (int i = 0; i < numThreads; i++) {
            startThread(new TableThread(createTable));
        }
        joinAllThreads();
    }

    private class TableThread implements Runnable {
        private final String statement;

        TableThread(String statement) {
            this.statement = statement;

        }

        @Override
        public void run() {
            store.executeSync(addRegionsForMRTable(statement));
        }
    }

    /*
     * Over numIterations itereations:
     *   o create numTables tables
     *   o populate tables
     *   o add indexes
     *   o drop tables
     */
    private class MDThread implements Runnable {
        private final int numIterations;
        private final int numTables;
        private final String namePrefix;
        private final ExecutionFuture[] ddlFutures;
        private final int[] planIds;
        private final CommandServiceAPI cs;
        private final static int NUM_RECORDS = 500;

        MDThread(int numIterations, int numTables, String namePrefix) {
            this.numIterations = numIterations;
            this.numTables = numTables;
            this.namePrefix = namePrefix;
            planIds = new int[numTables];
            cs = createStore.getAdmin();
            ddlFutures = new ExecutionFuture[numTables];
        }

        @Override
        public void run() {
            try {
                for (int iter = 0; iter < numIterations; iter++) {
                    String prefix = namePrefix + "_iter_" + iter + "_";

                    /* Create tables */
                    for (int i = 0; i < numTables; i++) {
                        String tableName = (prefix + i);
                        TableImpl table = TableBuilder.createTableBuilder(
                            tableName)
                            .addInteger("id")
                            .addString("name")
                            .primaryKey("id")
                            .buildTable();
                        ddlFutures[i] = addTableAsync(table, true);
                    }
                    waitForFutures();

                    /* Add indexes */
                    for (int i = 0; i < numTables; i++) {
                        String tableName = (prefix + i);
                        TableImpl table = getTable(tableName);
                        assertNotNull(table);
                        planIds[i] =
                            addIndexAsync(table, ("idx" + i),
                                          new String[] {"name"}, true, cs);
                    }
                    waitForPlans();

                    /* Populate tables
                     * NOTE: this needs to be done with
                     * Durability.COMMIT_NO_SYNC or timeouts occur due to
                     * InsufficientAcksException, presumably because RN
                     * replicas are too busy.
                     */
                    for (int i = 0; i < numTables; i++) {
                        final String tableName = (prefix + i);
                        populateTables(NUM_RECORDS, tableName);
                    }

                    /* Count index data */
                    for (int i = 0; i < numTables; i++) {
                        final TableImpl table = getTable(prefix + i);
                        final Index index = table.getIndex(("idx" + i));
                        assertNotNull(index);
                        assertEquals(NUM_RECORDS,
                                     countIndexRecords(index.createIndexKey(),
                                                       null,
                                                       UNORDERED_OPTIONS));
                    }

                    /* Drop tables */
                    for (int i = 0; i < numTables; i++) {
                        String tableName = (prefix + i);
                        TableImpl table = getTable(tableName);
                        assertNotNull(table);
                        planIds[i] = removeTableAsync(tableName, true, cs);
                    }
                    waitForPlans();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void waitForPlans() throws Exception {
            for (int plan : planIds) {
                waitForPlan(plan, true, cs);
            }
        }

        private void waitForFutures()
            throws InterruptedException, ExecutionException,
                   TimeoutException {
            for (ExecutionFuture future : ddlFutures) {
                DDLTestUtils.checkSuccess(future,
                                          future.get(50, TimeUnit.SECONDS));
            }
        }
    }
 }
