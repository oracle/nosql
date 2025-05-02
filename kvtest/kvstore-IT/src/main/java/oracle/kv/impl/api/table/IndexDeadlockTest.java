/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import java.util.Random;
import java.util.TreeSet;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.junit.Test;

/**
 * This test attempts to generate a deadlock in JE by simultaneously
 * iterating over index records and updating them.  The iteration is
 * both forward and reverse.  The idea is to create deadlock by forcing
 * JE to acquire locks out of order.  The table code and JE code are
 * modified to handle this case.
 */
public class IndexDeadlockTest extends TableTestBase {
    final int numUsers = 2000;

    /**
     * Combine forward/reverse iteration with updates.
     */
    @SuppressWarnings("unused")
    @Test
    public void deadlock() throws Exception {
        final int startId = 0;
        final int updateIterations = 10;
        TableImpl userTable = buildUserTable();

        addUsers(tableImpl, userTable, startId, numUsers);


        /*
         * Do a run where the update thread is adding ever-increasing
         * values.
         */
        Thread updateThread =
            startThread(new UpdateThread(updateIterations, false));
        Thread readerThread =
            startThread(new IndexReaderThread(updateThread));
        joinAllThreads();

        /*
         * Do another run using random key updates.  The order of updates
         * has an effect on index iteration.
         */
        updateThread =
            startThread(new UpdateThread(updateIterations, true));
        readerThread =
            startThread(new IndexReaderThread(updateThread));
        joinAllThreads();
    }

    private class UpdateThread implements Runnable {
        private final int numIterations;
        private final boolean useRandom;
        private final Random random;

        UpdateThread(int numIterations, boolean useRandom) {
            this.numIterations = numIterations;
            this.useRandom = useRandom;
            random = new Random();
        }

        /**
         * Keep modifying the firstName field of all records.
         * TODO: maybe just do one?
         */
        @Override
        public void run() {
            int base = 0;
            final TableImpl userTable = getTable("User");
            for (int i = 0; i < numIterations; i++) {
                TableIterator<Row> iter = tableImpl.tableIterator
                    (userTable.createPrimaryKey(), null, null);
                while (iter.hasNext()) {
                    Row row = iter.next();
                    String name = row.get("firstName").asString().get();
                    name = modifyName(base, i);
                    row.put("firstName", name);
                    tableImpl.put(row, null, null);
                }
                iter.close();
                ++base;
            }
        }

        private String modifyName(int baseIter, int iter) {
            if (!useRandom) {
                return "first" + Integer.toString(baseIter) +
                        Integer.toString(iter);
            }
            return "first" + random.nextInt(numUsers);
        }

    }

    /**
     * A Runnable class that iterates an entire index, mixing forward and
     * reverse iteration, until the UpdateThread has ended.
     */
    private class IndexReaderThread implements Runnable {
        private final Thread updateThread;

        IndexReaderThread(Thread updateThread) {
            this.updateThread = updateThread;
        }

        @Override
        public void run() {
            boolean forward = true;
            final TableImpl userTable = getTable("User");
            final Index index = userTable.getIndex("FirstName");
            final TableIteratorOptions forwardOpts =
                new TableIteratorOptions(Direction.FORWARD,
                                         Consistency.ABSOLUTE,
                                         0, null, /* timeout, unit */
                                         0, 0);
            final TableIteratorOptions reverseOpts =
                new TableIteratorOptions(Direction.REVERSE,
                                         Consistency.ABSOLUTE,
                                         0, null, /* timeout, unit */
                                         0, 0);
            final IndexKey key = index.createIndexKey();
            while (updateThread.isAlive()) {
                TreeSet<Integer> rows = new TreeSet<Integer>();
                TableIteratorOptions itOpts =
                    (forward ? forwardOpts : reverseOpts);
                forward = !forward;
                TableIterator<Row> iter =
                    tableImpl.tableIterator(key, null, itOpts);
                while (iter.hasNext()) {
                    Row row = iter.next();
                    rows.add(Integer.valueOf(row.get("id").asInteger().get()));
                }
                iter.close();
            }
        }
    }

    static private RowImpl addUserRow(TableAPI impl,
                                      Table table,
                                      int id, String first, String last,
                                      int age) {
        RowImpl row = (RowImpl) table.createRow();
        row.put("id", id);
        row.put("firstName", first);
        row.put("lastName", last);
        row.put("age", age);
        if (impl != null) {
            impl.put(row, null, null);
        }
        return row;
    }

    private TableImpl buildUserTable() {
        try {
            TableBuilder builder = (TableBuilder)
                TableBuilder.createTableBuilder("User",
                                                "Table of Users",
                                                null)
                .addInteger("id")
                .addString("lastName")
                .addInteger("age")
                .addString("firstName")
                .primaryKey("id")
                .shardKey("id");
            addTable(builder, true);

            TableImpl t = getTable("User");
            t = addIndex(t, "FirstName",
                         new String[] {"firstName"}, true);
            t = addIndex(t, "Age",
                         new String[] {"age"}, true);
            t = addIndex(t, "LastAge",
                         new String[] {"lastName", "age"}, true);
            t = addIndex(t, "AgeLast",
                         new String[] {"age", "lastName"}, true);
            /* LastName will have a lot of duplicates */
            t = addIndex(t, "LastName",
                         new String[] {"lastName"}, true);
            return t;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add table: " + e, e);
        }
    }

    private void  addUsers(TableAPI impl, TableImpl table,
                           int startId, int num) {
        /*
         * add a number of users.  Differentiate on age, first name, and
         * id (the primary key).  Ensure that the first name and id fields
         * sort in opposite order to exercise sorting.
         */

        for (int i = 0; i < num; i++) {
            addUserRow(impl, table, startId + i,
                       ("Zach" + (num - i)),
                       "Zachary", startId + i);
        }
    }
}

