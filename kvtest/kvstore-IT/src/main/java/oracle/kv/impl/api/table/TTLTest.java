/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import oracle.kv.Direction;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.ParallelScanIterator;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.Version;
import oracle.kv.impl.api.bulk.BulkGetTest;
import oracle.kv.table.Index;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.TableOperationResult;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.WriteOptions;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Tests Time-To-Live feature against data store. <br>
 * <b>Note</b>: Because records expire in minimum of an hour, the feature can
 * not be tested unless the test waits for at least an hour. <br>
 *
 * TODO: Use TestHook to actually test record with expiration shorter than
 * an hour.
 * <p>
 * This test confines itself to ensure that records can be <code>put()</code>
 * with non-zero expiration duration and resultant row of <code>put()</code>
 * operation carry expected expiration time stamp.
 *
 *
 */
public class TTLTest extends TableTestBase {
    private Table table;
    private Table child1;
    private Table child2;
    private long idCounter = System.currentTimeMillis();

    private static final String CREATE_TABLE =
        "CREATE TABLE IF NOT EXISTS TTLTest(sk1 STRING, sk2 STRING, "
        + "pk2 STRING, s2 STRING, primary key(shard(sk1,sk2), pk2))";
    private static final String CREATE_CHILD1_TABLE =
        "CREATE TABLE IF NOT EXISTS TTLTest.TTLChildOne(child1 STRING, " +
        " primary key(child1))";
    private static final String CREATE_CHILD2_TABLE =
        "CREATE TABLE IF NOT EXISTS TTLTest.TTLChildOne.TTLChildTwo( " +
        "child2 STRING, primary key(child2))";
    private static final String CREATE_INDEX =
        "CREATE INDEX IF NOT EXISTS s2index ON TTLTest(s2)";
    private static final String CREATE_CHILD1_INDEX =
        "CREATE INDEX IF NOT EXISTS child1index ON TTLTest.TTLChildOne(child1)";

    private static final String TABLE_NAME = "TTLTest";
    private static final String SHARD_KEY_1 = "sk1";
    private static final String SHARD_KEY_2 = "sk2";
    private static final String PK2_FIELD = "pk2";
    private static final String STRING_FIELD  = "s2";

    @BeforeClass
    public static void staticSetUp() throws Exception {
        //TODO: remove this after MRTable support ttl.
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        TableTestBase.staticSetUp();
    }

    @Test
    public void testNewRowTTLModifiedOnPut() {
        createTable(null);
        Row row = createRow();
        TimeToLive ttl = TimeToLive.ofDays(42);

        row.setTTL(ttl);
        tableImpl.put(row, null, null);

        assertTimeToLive(ttl, row);
    }

    @Test
    public void testExistingRowTTLModifiedOnSimplePut() {
        createTable(null);
        Row row = createRow();
        TimeToLive userTTL1 = TimeToLive.ofDays(42);
        row.setTTL(userTTL1);
        tableImpl.put(row, null, null);

        assertTimeToLive(userTTL1, row);

        // Put same row again, but assertions about TTL change
        TimeToLive userTTL2 = TimeToLive.ofDays(56);
        ReturnRow prevRow = table.createReturnRow(Choice.ALL);
        row.setTTL(userTTL2);
        tableImpl.putIfPresent(row, prevRow, new WriteOptions().
                               setUpdateTTL(true));

        assertTimeToLive(userTTL2, row);
        assertTimeToLive(userTTL1, prevRow);
    }

    @Test
    public void testExistingRowTTLNotModifiedOnPut() {
        createTable(null);
        Row row = createRow();
        TimeToLive userTTL1 = TimeToLive.ofDays(42);
        row.setTTL(userTTL1);
        tableImpl.put(row, null, null);

        assertTimeToLive(userTTL1, row);

        // Put same row again, but do not update TTL
        TimeToLive userTTL2 = TimeToLive.ofDays(56);
        row.setTTL(userTTL2);
        tableImpl.putIfPresent(row, null, null);

        assertTimeToLive(userTTL1, row); // same TTL as before
    }

    @Test
    public void testNewRowTTLModifiedOnPutIfAbsent() {
        createTable(null);
        Row row = createRow();
        TimeToLive userTTL = TimeToLive.ofHours(42);

        row.setTTL(userTTL);
        tableImpl.putIfAbsent(row, null, null);

        assertTimeToLive(userTTL, row);
    }

    @Test
    public void testExistingRowTTLModifiedOnPutIfPresent() {
        createTable(null);
        Row row = createRow();
        TimeToLive userTTL1 = randomTTL();

        row.setTTL(userTTL1);
        tableImpl.put(row, null, null);

        // Put same row again, update TTL
        TimeToLive userTTL2 = randomTTL();
        ReturnRow prevRow = table.createReturnRow(Choice.ALL);
        row.setTTL(userTTL2);
        tableImpl.putIfPresent(row, prevRow, new WriteOptions().
                               setUpdateTTL(true));

        assertTimeToLive(userTTL2, row);
        assertTimeToLive(userTTL1, prevRow);
    }

    @Test
    public void testExistingRowTTLNotModifiedOnPutIfPresent() {
        createTable(null);
        Row row = createRow();
        TimeToLive userTTL1 = randomTTL();
        row.setTTL(userTTL1);
        tableImpl.put(row, null, null);
        assertTimeToLive(userTTL1, row);

        // Put same row again, do not update TTL
        TimeToLive userTTL2 = randomTTL();
        ReturnRow prevRow = table.createReturnRow(Choice.ALL);
        row.setTTL(userTTL2);
        tableImpl.putIfPresent(row, prevRow, new WriteOptions().
                               setUpdateTTL(false));

        assertTimeToLive(userTTL1, row);
        assertTimeToLive(userTTL1, prevRow);
    }

    @Test
    public void testNewRowTTLNotModifiedOnPutIfAbsent() {
        createTable(null);
        Row row = createRow();
        TimeToLive userTTL1 = randomTTL();
        row.setTTL(userTTL1);
        tableImpl.putIfAbsent(row, null, null);

        assertTimeToLive(userTTL1, row);
    }

    @Test
    public void testGetTTLOfExistingRow() {
        createTable(null);
        Row row = createRow();
        PrimaryKey pk = table.createPrimaryKey(row);
        TimeToLive userTTL = randomTTL();
        row.setTTL(userTTL);
        tableImpl.put(row, null, null);

        assertTimeToLive(userTTL, row);

        Row row2 = tableImpl.get(pk, null);
        assertNotNull(row2);
        assertTimeToLive(userTTL, row2);
    }


    @Test
    public void testTableTTLIsAppliedByDefault() {
        TimeToLive tableTTL = randomTTL();
        createTable(tableTTL);

        Row row = createRow();
        tableImpl.put(row, null, null /*WriteOptions*/);

        /* Fetch a row and verify that it matches as well */
        Row row1 = tableImpl.get(row.createPrimaryKey(),null);
        assertTrue(row.getExpirationTime() == row1.getExpirationTime());

        assertTimeToLive(tableTTL, row);
        tableImpl.put(row, null, new WriteOptions());
        assertTimeToLive(tableTTL, row);
    }

    @Test
    public void testTableTTLIsOverriddenByRowTTL() {
        TimeToLive tableTTL = randomTTL();
        TimeToLive userTTL = randomTTL();
        createTable(tableTTL);
        Row row = createRow();
        row.setTTL(userTTL);
        tableImpl.put(row, null, null);
        assertTimeToLive(userTTL, row);
    }

    @Test
    public void testBatchPutSameOption() throws Exception {
        createTable(null);
        List<TableOperation> puts = new ArrayList<TableOperation>();
        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        TableOperationFactory factory = tableImpl.getTableOperationFactory();

        TimeToLive userTTL = randomTTL();
        WriteOptions options = new WriteOptions();

        int N = 10;
        for (int i = 0; i < N; i++) {
            Row row = createRow();
            row.setTTL(userTTL);
            pks.add(row.createPrimaryKey());
            puts.add(factory.createPut(row, null, false));
        }

        tableImpl.execute(puts, options);

        for (PrimaryKey pk2 : pks) {
            Row row2 = tableImpl.get(pk2, null);
            assertTimeToLive(userTTL, row2);
        }
    }

    /*
     * Test execute(List<TableOperation>) to parent and child tables, tables
     * has different TTL.
     */
    @Test
    public void testBatchPutParentChildTables() throws Exception {

        TimeToLive tableTTL = randomTTL();
        createTable(tableTTL);

        TimeToLive child1TTL = TimeToLive.createTimeToLive(
                tableTTL.getValue() + 1,
                tableTTL.getUnit());

        TimeToLive child2TTL = TimeToLive.createTimeToLive(
                child1TTL.getValue() + 1,
                child1TTL.getUnit());
        createChildTables(child1TTL, child2TTL);

        TimeToLive userTTL = TimeToLive.createTimeToLive(
                child2TTL.getValue() + 1,
                child2TTL.getUnit());

        List<TableOperation> puts = new ArrayList<TableOperation>();
        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        TableOperationFactory factory = tableImpl.getTableOperationFactory();
        WriteOptions options = new WriteOptions();

        /* Use default table TTL */
        int N = 2;
        for (int i = 0; i < N; i++) {
            Row row = createRow();
            pks.add(row.createPrimaryKey());
            puts.add(factory.createPut(row, null, false));

            Row child1Row = child1.createRow(row);
            child1Row.put("child1", String.valueOf(N));
            pks.add(child1Row.createPrimaryKey());
            puts.add(factory.createPut(child1Row, null, false));

            Row child2Row = child2.createRow(child1Row);
            child2Row.put("child2", String.valueOf(N));
            pks.add(child2Row.createPrimaryKey());
            puts.add(factory.createPut(child2Row, null, false));
        }

        tableImpl.execute(puts, options);

        TimeToLive expTTL;
        for (PrimaryKey pk2 : pks) {
            Row row2 = tableImpl.get(pk2, null);
            String tableName = row2.getTable().getName();
            if (tableName.equals(table.getName())) {
                expTTL = tableTTL;
            } else if (tableName.equals(child1.getName())) {
                expTTL = child1TTL;
            } else {
                assertEquals(child2.getName(), tableName);
                expTTL = child2TTL;
            }
            assertTimeToLive(expTTL, row2);
        }

        /* Use user specified TTL */
        puts.clear();
        pks.clear();
        for (int i = 0; i < N; i++) {
            Row row = createRow();
            row.setTTL(userTTL);
            pks.add(row.createPrimaryKey());
            puts.add(factory.createPut(row, null, false));

            Row child1Row = child1.createRow(row);
            child1Row.put("child1", String.valueOf(N));
            child1Row.setTTL(userTTL);
            pks.add(child1Row.createPrimaryKey());
            puts.add(factory.createPut(child1Row, null, false));

            Row child2Row = child2.createRow(child1Row);
            child2Row.put("child2", String.valueOf(N));
            child2Row.setTTL(userTTL);
            pks.add(child2Row.createPrimaryKey());
            puts.add(factory.createPut(child2Row, null, false));
        }

        tableImpl.execute(puts, options);

        expTTL = userTTL;
        for (PrimaryKey pk2 : pks) {
            Row row2 = tableImpl.get(pk2, null);
            assertTimeToLive(expTTL, row2);
        }
    }

    /**
     * Test that index population that occurs when an index is added
     * to a table that has existing data the expiration time for the
     * data is propagated to the index records.
     * 1. create table with TTL
     * 2. add data
     * 3. add index
     * 4. iterate index using KeyPair to verify TTL.
     */
    @Test
    public void testIndexPopulation() throws Exception {
        final int numRows = 500;

        final String CREATE_TBL =
            "create table foo(id integer, primary key(id)) using ttl 5 days";
        final String CREATE_IDX =
            "create index idIndex on foo(id)";

        executeDdl(CREATE_TBL);

        Table fooTable = getTable("foo");
        assertNotNull(fooTable);

        /* populate table */
        for (int i = 0; i < numRows; i++) {
            Row row = fooTable.createRow();
            row.put("id", i);
            tableImpl.put(row, null, null);
        }

        /* create and populate the index */
        executeDdl(CREATE_IDX);
        fooTable = getTable("foo"); /* re-fetch to get the index */
        Index index = fooTable.getIndex("idIndex");
        assertNotNull(index);

        /*
         * Iterate the index, asserting that the expiration time is present.
         * This must use tableKeysIterate to be sure that the expiration is
         * obtained from the index record and not the primary record.
         */
        TableIterator<KeyPair> iter =
            tableImpl.tableKeysIterator(index.createIndexKey(), null, null);
        assertTTLKeyPairs(iter, numRows);
    }

    /**
     * This is a somewhat long set of tests in a single test case.
     * They exercise the access to expiration time via all of the
     * supported iteration paths. These include:
     * 1. table scan (both key and row)
     * 2. multi-get (both key and row)
     * 3. bulk get (both key and row)
     * 4. index scan (both key and row)
     * In addition it tests for expiration time in rows and keys returned
     * from ancestor and child tables.
     *
     * To perform this test there are 3 tables -- parent, child, and grandchild.
     * The basic iteration tests are done on the parent. The tests for
     * ancestor and child retrieval are done on the middle table -- the child.
     *
     * All tables have a default TTL. When populated all of the tables have the
     * same number of rows inserted.
     */
    @Test
    public void testTableIteratorExpirationTime() {
        final int numRows = 500;
        TimeToLive tableTTL = randomTTL();
        createTable(tableTTL);
        createChildTables(tableTTL, tableTTL);

        /*
         * Populate the parent table
         */
        for (int i = 0; i < numRows; i++) {
            Row row = createRow();
            tableImpl.put(row, null, null);
        }

        /*
         * Table iterator
         */
        TableIterator<Row> iter =
            tableImpl.tableIterator(table.createPrimaryKey(), null, null);
        assertTTLRows(iter, numRows);

        /*
         * Index iterator
         */
        Index index = table.getIndex("s2index");
        iter = tableImpl.tableIterator(index.createIndexKey(), null, null);
        assertTTLRows(iter, numRows);

        /*
         * Multi-get
         */
        PrimaryKey pkey = table.createPrimaryKey();
        pkey.put(SHARD_KEY_1, "sk1");
        pkey.put(SHARD_KEY_2, "sk2");
        List<Row> rows = tableImpl.multiGet(pkey, null, null);
        assertTrue(rows.size() == numRows);
        for (Row row : rows) {
            assertTTLExists(row);
        }

        /*
         * Index key iterator
         */
        index = table.getIndex("s2index");
        TableIterator<KeyPair> indexKeyIter =
            tableImpl.tableKeysIterator(index.createIndexKey(), null, null);
        assertTTLKeyPairs(indexKeyIter, numRows);

        /*
         * Table keys
         */
        TableIterator<PrimaryKey> pkIter =
            tableImpl.tableKeysIterator(table.createPrimaryKey(), null, null);
        assertTTLPrimaryKeys(pkIter, numRows);

        /*
         * Multi-get keys
         *
         * Use the result rows to populate the keys array used for bulk get.
         * Also use the results to populate the child and grandchild tables.
         * They are used for ancestor/child table record retrieval, below.
         */

        /* for bulk get */
        List<PrimaryKey> keys = new ArrayList<PrimaryKey>();

        pkey = table.createPrimaryKey();
        pkey.put(SHARD_KEY_1, "sk1");
        pkey.put(SHARD_KEY_2, "sk2");
        List<PrimaryKey> pkeys = tableImpl.multiGetKeys(pkey, null, null);
        assertTrue(pkeys.size() == numRows);
        int count = 0;
        for (PrimaryKey key : pkeys) {
            assertTTLExists(key);

            keys.add(key); /* bulk get */

            /*
             * Add rows to the child tables. They share the primary key of the
             * parent.
             */
            Row child1Row = child1.createRow(key);
            child1Row.put("child1", Integer.toString(count));
            tableImpl.put(child1Row, null, null);

            Row child2Row = child2.createRow(child1Row);
            child2Row.put("child2", Integer.toString(count));
            tableImpl.put(child2Row, null, null);

            ++count;
        }

        /*
         * Bulk get iterator
         */
        assertTrue(keys.size() == numRows);
        iter = tableImpl.tableIterator(keys.iterator(), null, null);
        assertTTLRows(iter, numRows);

        /*
         * Bulk get keys iterator
         */
        assertTrue(keys.size() == numRows);
        pkIter = tableImpl.tableKeysIterator(keys.iterator(), null, null);
        assertTTLPrimaryKeys(pkIter, numRows);

        /*
         * Test cases for parent/child tables
         */

        /*
         * assert that the child tables are populated as expected
         */
        assertTrue(countTableRecords(child1.createPrimaryKey(), child1) ==
                   numRows);
        assertTrue(countTableRecords(child2.createPrimaryKey(), child2) ==
                   numRows);

        /*
         * Iterate the middle child, adding both the parent and child tables
         * to the targets. This exercises code in both directions sufficiently.
         *
         * Test this for all of the same combinations above. Some of these share
         * code paths, but the extra coverage doesn't hurt.
         */

        /*
         * Table keys first
         */
        List<Table> ancestors = Arrays.asList(table);
        List<Table> children = Arrays.asList(child2);
        MultiRowOptions mro = new MultiRowOptions(null, ancestors, children);
        pkIter =
            tableImpl.tableKeysIterator(child1.createPrimaryKey(), mro, null);
        assertTTLPrimaryKeys(pkIter, numRows * 3);

        /*
         * Table rows
         */
        iter =
            tableImpl.tableIterator(child1.createPrimaryKey(), mro, null);
        assertTTLRows(iter, numRows * 3);

        /*
         * Multi-get
         */
        pkey = child1.createPrimaryKey();
        pkey.put(SHARD_KEY_1, "sk1");
        pkey.put(SHARD_KEY_2, "sk2");
        rows = tableImpl.multiGet(pkey, mro, null);
        assertTrue(rows.size() == numRows * 3);
        for (Row row : rows) {
            assertTTLExists(row);
        }

        /*
         * Multi-get keys
         *
         * Use results here to populate a list for multi-get testing.
         */
        keys = new ArrayList<PrimaryKey>(); /* bulk get, next */
        pkeys = tableImpl.multiGetKeys(pkey, mro, null);
        assertTrue(pkeys.size() == numRows * 3);
        for (PrimaryKey key : pkeys) {
            assertTTLExists(key);
            if (key.getTable().getName().equals("TTLChildOne")) {
                keys.add(key);
            }
        }

        /*
         * Bulk get iterator
         */
        assertTrue(keys.size() == numRows);
        iter = tableImpl.tableIterator(keys.iterator(), mro, null);
        assertTTLRows(iter, numRows * 3);

        /*
         * Bulk get keys iterator
         */
        assertTrue(keys.size() == numRows);
        pkIter =
            tableImpl.tableKeysIterator(keys.iterator(), mro, null);
        assertTTLPrimaryKeys(pkIter, numRows * 3);

        /*
         * Index iterator
         *
         * An index was created on "child1" in the middle child table.
         *
         * NOTE: index scans cannot return child tables, so modify the
         * MultiRowOptions to only contain an ancestor.
         */
        mro = new MultiRowOptions(null, ancestors, null);
        index = child1.getIndex("child1index");
        iter = tableImpl.tableIterator(index.createIndexKey(), mro, null);
        assertTTLRows(iter, numRows * 2);

        /*
         * Index keys iterator
         */
        indexKeyIter =
            tableImpl.tableKeysIterator(index.createIndexKey(), mro, null);
        assertTTLKeyPairs(indexKeyIter, numRows * 2);
    }


    @Test
    public void testPreviousRowOnPut() {
        createTable(null);

        Row row = createRow();
        TimeToLive ttl = randomTTL();
        row.setTTL(ttl);
        Version prevVersion = tableImpl.put(row, null, null);
        assertTimeToLive(ttl, row);

        TimeToLive ttl2 = randomTTL();
        for (Choice choice : Choice.values()) {
            ReturnRow rr = table.createReturnRow(choice);
            row.setTTL(ttl2);
            Version v = tableImpl.put(row, rr, null);
            assertPreviousRow(choice, rr, prevVersion, ttl);
            prevVersion = v;
        }
     }

    @Test
    public void testPreviousRowOnPutIfAbsent() {
        createTable(null);

        TimeToLive ttl = randomTTL();
        for (Choice choice : Choice.values()) {
            Row row = createRow();
            row.setTTL(ttl);
            ReturnRow rr = table.createReturnRow(choice);
            tableImpl.put(row, rr, null);

            assertNull(rr.getVersion());
            assertNoExpiry(rr);
        }
     }

    @Test
    public void testPreviousRowOnPutIfPresent() {
        createTable(null);

        Row row = createRow();
        TimeToLive ttl = randomTTL();
        row.setTTL(ttl);
        Version prevVersion = tableImpl.put(row, null, null);
        assertTimeToLive(ttl, row);

        TimeToLive ttl2 = randomTTL();
        for (Choice choice : Choice.values()) {
            ReturnRow rr = table.createReturnRow(choice);
            row.setTTL(ttl2);
            Version v = tableImpl.putIfPresent(row, rr, null);

            assertPreviousRow(choice, rr, prevVersion, ttl);
            prevVersion = v;
        }
     }

    @Test
    public void testPreviousRowOnPutIfVersion() {
        createTable(null);

        Row row = createRow();
        TimeToLive ttl = randomTTL();
        row.setTTL(ttl);
        Version prevVersion = tableImpl.put(row, null, null);
        assertTimeToLive(ttl, row);

        Version currentVersion = tableImpl.put(row, null, null);

        for (Choice choice : Choice.values()) {
            ReturnRow rr = table.createReturnRow(choice);

            Row newRow = tableImpl.get(row.createPrimaryKey(), null);
            assertTTLExists(newRow);

            /*
             * Use old version. This will fail, which means that return row
             * is available
             */
            Version newVersion =
                tableImpl.putIfVersion(row, prevVersion, rr, null);
            assertNull(newVersion);
            assertPreviousRow(choice, rr, currentVersion, ttl);

            /*
             * Use the correct version, which will succeed. Return row is empty
             */
            prevVersion = currentVersion;
            currentVersion =
                tableImpl.putIfVersion(row, currentVersion, rr, null);
            assertNotNull(currentVersion);
            assertPreviousRow(Choice.NONE, rr, null, null);
        }
     }



    @Test
    public void testPreviousRowOnPutInBatch() throws Exception {
        createTable(null);

        TimeToLive ttl = randomTTL();
        TableOperationFactory factory = tableImpl.getTableOperationFactory();

        for (Choice choice : Choice.values()) {
            List<TableOperation> puts = new ArrayList<TableOperation>();

            Row row = createRow();
            row.setTTL(ttl);
            Version prevVersion = tableImpl.put(row, null, null);
            assertNotNull(prevVersion);
            TableOperation op = factory.createPut(row, choice, false);
            puts.add(op);

            List<TableOperationResult> results =
                tableImpl.execute(puts, null);

            TableOperationResult r = results.get(0);
            assertTrue(r.getSuccess());
            Row rr = r.getPreviousRow();
            if (choice == Choice.NONE) {
                assertNull(rr);
            } else {
                assertTableOperationResult(choice, r, prevVersion, ttl);
            }
        }
    }

    @Test
    public void testPreviousRowOnPutIfPresentInBatch() throws Exception {
        createTable(null);

        TimeToLive ttl = randomTTL();
        TableOperationFactory factory = tableImpl.getTableOperationFactory();

        for (Choice choice : Choice.values()) {
            List<TableOperation> puts = new ArrayList<TableOperation>();

            Row row = createRow();
            row.setTTL(ttl);
            Version prevVersion = tableImpl.put(row, null, null);
            TableOperation op = factory.createPutIfPresent(row, choice, false);
            puts.add(op);

            List<TableOperationResult> results =
                tableImpl.execute(puts, null);

            TableOperationResult r = results.get(0);
            assertTrue(r.getSuccess());
            Row rr = r.getPreviousRow();

            if (choice == Choice.NONE) {
                assertNull(rr);
            } else {
                assertTableOperationResult(choice, r, prevVersion, ttl);
            }
        }
    }

    @Test
    public void testPreviousRowOnPutIfAbsentInBatch() throws Exception {
        createTable(null);

        TimeToLive ttl = randomTTL();
        TableOperationFactory factory = tableImpl.getTableOperationFactory();

        for (Choice choice : Choice.values()) {
            List<TableOperation> puts = new ArrayList<TableOperation>();
            Row row = createRow();
            row.setTTL(ttl);
            TableOperation op = factory.createPutIfAbsent(row, choice, false);
            puts.add(op);

            List<TableOperationResult> results =
                    tableImpl.execute(puts, null);
            TableOperationResult r = results.get(0);
            assertTrue(r.getSuccess());

            Row rr = r.getPreviousRow();
            assertNull(rr);
        }
    }

    @Test
    public void testPreviousRowOnPutIfVersionInBatch() throws Exception {
        createTable(null);

        TimeToLive ttl = randomTTL();
        TableOperationFactory factory = tableImpl.getTableOperationFactory();

        for (Choice choice : Choice.values()) {
            List<TableOperation> puts = new ArrayList<TableOperation>();

            Row row = createRow();
            row.setTTL(ttl);
            Version v = tableImpl.put(row, null, null);
            TableOperation op = factory.createPutIfVersion(row, v, choice, false);
            puts.add(op);

            List<TableOperationResult> results =
                    tableImpl.execute(puts, null);
            TableOperationResult r = results.get(0);
            assertTrue(r.getSuccess());
            /* ReturnRow is null on success */
            assertNull(r.getPreviousRow());
        }
    }

    /* this tests both delete and deleteIfVersion */
    @Test
    public void testPreviousRowOnDelete() {
        createTable(null);
        Row row = createRow();
        TimeToLive userTTL = TimeToLive.ofDays(42);
        row.setTTL(userTTL);
        Version prevVersion = tableImpl.put(row, null, null);
        Version currentVersion = tableImpl.put(row, null, null);
        for (Choice choice : Choice.values()) {
            ReturnRow rr = table.createReturnRow(choice);

            /* this will fail, which means ReturnRow is available */
            boolean deleted = tableImpl.deleteIfVersion(row.createPrimaryKey(),
                                                        prevVersion,
                                                        rr,
                                                        null);
            assertFalse(deleted);
            assertPreviousRow(choice, rr, currentVersion, userTTL);

            /* this will succeed, return row is available */
            deleted = tableImpl.delete(row.createPrimaryKey(),
                                       rr,
                                       null);
            assertTrue(deleted);
            assertPreviousRow(choice, rr, currentVersion, userTTL);

            /* put the record back */
            currentVersion = tableImpl.put(row, null, null);
        }
    }

    /**
     * Verifies state of a TableOperationResult against a given choice
     *
     * @param choice choice for a previous row
     * @param topRes the result
     * @param expectedV expected Version to validate that previous row does not
     * carry illegal version.
     * @param ttl expected TTL
     */
    void assertTableOperationResult(Choice choice, TableOperationResult topRes,
                                    Version expectedV, TimeToLive ttl) {
        switch (choice) {
        case VALUE:
            assertTimeToLive(ttl, topRes.getPreviousExpirationTime());
            assertNull(topRes.getPreviousVersion());
            assertNotNull(topRes.getPreviousRow());
            break;
        case VERSION:
            assertTimeToLive(ttl, topRes.getPreviousExpirationTime());
            assertTrue(expectedV.equals(topRes.getPreviousVersion()));
            assertNull(topRes.getPreviousRow());
            break;
        case ALL:
            assertTimeToLive(ttl, topRes.getPreviousExpirationTime());
            assertTrue(expectedV.equals(topRes.getPreviousVersion()));
            assertNotNull(topRes.getPreviousRow());
            assertTimeToLive(ttl, topRes.getPreviousRow());
            break;
        case NONE:
            assertNull(topRes.getPreviousRow());
            assertNull(topRes.getPreviousVersion());
            break;
        }
   }

    /**
     * Verifies state (the version and expiration time) of given previous row
     * against given choice.
     *
     * @param choice choice for a previous row
     * @param rr previous row whose state being tested
     * @param expectedV expected Version to validate that previous row does not
     * carry illegal version.
     * @param ttl expected TTL
     */
    void assertPreviousRow(Choice choice, Row rr, Version expectedV,
                           TimeToLive ttl) {
        switch (choice) {
        case VALUE:
            assertTimeToLive(ttl, rr);
            assertNull(rr.getVersion());
            break;
        case VERSION:
        case ALL:
            assertTimeToLive(ttl, rr);
            assertTrue(expectedV.equals(rr.getVersion()));
            break;
        case NONE:
            assertNoExpiry(rr);
            assertNull(rr.getVersion());
            break;
        }
   }

    private static void assertTTLPrimaryKeys(TableIterator<PrimaryKey> iter,
                                             int expected) {
        int count = 0;
        while (iter.hasNext()) {
            PrimaryKey key = iter.next();
            assertTTLExists(key);
            count++;
        }
        assertTrue(count == expected);
    }

    @Test
    public void testTimeToLive() {
        final String BAD_TTL =
            "create table foo(id integer, primary key(id)) using ttl -2 days";
        final String BAD_TTL1 =
            "create table foo(id integer, primary key(id)) using ttl 2 seconds";
        executeDdl(BAD_TTL, false);
        executeDdl(BAD_TTL1, false);

        TimeToLive ttlH = TimeToLive.ofHours(24);
        TimeToLive ttlD = TimeToLive.ofDays(5);
        assertFalse(ttlH.equals(ttlD));

        /* 24 hours != 1 day */
        ttlD = TimeToLive.ofDays(1);
        assertFalse(ttlH.equals(ttlD));

        /* test real equality */
        ttlD = TimeToLive.ofHours(24);
        assertTrue(ttlH.equals(ttlD));

        /* should be equal (0 means no expire) */
        ttlD = TimeToLive.ofHours(0);
        ttlH = TimeToLive.ofDays(0);
        assertTrue(ttlH.equals(ttlD));

        /* some asserts about 0 duration objects */
        assertTrue(ttlD.equals(TimeToLive.DO_NOT_EXPIRE));
        assertTrue(ttlH.equals(TimeToLive.DO_NOT_EXPIRE));

        assertTrue(TimeToLive.fromExpirationTime(0, System.currentTimeMillis()).
                   equals(TimeToLive.DO_NOT_EXPIRE));
        assertTrue(0 == ttlD.toExpirationTime(System.currentTimeMillis()));

        /*
         * Test TimeToLive.fromExpirationTime, the duration between
         * referenceTime and expirationTime is less than one hour.
         */
        ttlH = TimeToLive.fromExpirationTime(1527483600000L, 1527481131705L);
        assertEquals(ttlH, TimeToLive.ofHours(1));
        /*
         * Test TimeToLive.fromExpirationTime, the duration between
         * referenceTime and expirationTime is less than one day.
         */
        ttlD = TimeToLive.fromExpirationTime(1527552000000L, 1527481131705L);
        assertEquals(ttlD, TimeToLive.ofDays(1));

        /* coverage */
        long current = System.currentTimeMillis();
        TimeToLive.fromExpirationTime(current, current);

        try {
            ttlD = TimeToLive.ofHours(-1);
            fail("Bad TTL");
        } catch (IllegalArgumentException iae) {}

        try {
            ttlD = TimeToLive.ofDays(-1);
            fail("Bad TTL");
        } catch (IllegalArgumentException iae) {}

        /* use internal method */
        try {
            ttlD = TimeToLive.createTimeToLive(1, TimeUnit.SECONDS);
            fail("Bad TTL");
        } catch (IllegalArgumentException iae) {}

        /* create negative duration */
        try {
            ttlD = TimeToLive.createTimeToLive(-10, TimeUnit.DAYS);
            fail("Bad TTL");
        } catch (IllegalArgumentException iae) {}
    }

    /*
     * Tests access to expiration time via key/value store iteration.
     * This uses a hidden interface in KeyValueVersion.
     */
    /* This test has been disabled. Store iteration should no longer
     * be used in tests since system table data is present.
     */
    public void testTTLInStoreIterator() {
        final int numRows = 500;
        TimeToLive tableTTL = TimeToLive.ofDays(5);
        createTable(tableTTL);

        /*
         * Populate the table
         * Track the expiration time for comparison later. The use of days
         * ensures that all of the rows have the same expiration time.
         */
        long expirationTime = 0;
        for (int i = 0; i < numRows; i++) {
            Row row = createRow();
            tableImpl.put(row, null, null);
            if (expirationTime != 0) {
                assertTrue(expirationTime == row.getExpirationTime());
            }
            expirationTime = row.getExpirationTime();
            assertTrue(expirationTime != 0);
        }

        StoreIteratorConfig config = new StoreIteratorConfig();
        ParallelScanIterator<KeyValueVersion> iter =
            store.storeIterator(Direction.FORWARD,
                                0,      /* batch size */
                                null,   /* parent key */
                                null,   /* subRange */
                                null,   /* depth */
                                null,   /* consistency */
                                0, null,/* timeout */
                                config);

        /*
         * Use the results to create a list for use below in the BulkGet case
         */
        ArrayList<Key> keys = new ArrayList<Key>();
        int count = 0;
        while (iter.hasNext()) {
            KeyValueVersion kvv = iter.next();
            ++count;
            /* expiration time should match the one saved above */
            assertTrue(kvv.getExpirationTime() == expirationTime);
            keys.add(kvv.getKey());
        }
        assertTrue(count == numRows);


        /*
         * Do the same thing with a not-parallel iterator
         */
        Iterator<KeyValueVersion> kvvIter =
            store.storeIterator(Direction.UNORDERED, 0);
        count = 0;
        while (kvvIter.hasNext()) {
            KeyValueVersion kvv = kvvIter.next();
            ++count;
            /* expiration time should match the one saved above */
            assertTrue(kvv.getExpirationTime() == expirationTime);
        }
        assertTrue(count == numRows);

        /*
         * Now use BulkGet
         *
         * Split the keys into 10 iterators for bulk get
         */
        List<Iterator<Key>> keyIterList =
            BulkGetTest.createKeyIterators(keys, 10);

        iter = store.storeIterator(keyIterList,
                                   0,      /* batch size */
                                   null,   /* subRange */
                                   null,   /* depth */
                                   null,   /* consistency */
                                   0, null,/* timeout */
                                   config);
        count = 0;
        while (iter.hasNext()) {
            KeyValueVersion kvv = iter.next();
            ++count;
            /* expiration time should match the one saved above */
            assertTrue(kvv.getExpirationTime() == expirationTime);
        }
        assertTrue(count == numRows);
    }


    @Test
    public void testDDLAltersDefaultTableTTL() {
        String tableName = "TTL" + System.currentTimeMillis();
        TimeToLive defaultTTL = TimeToLive.ofDays(42);
        String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableName
                              + " (pk LONG, x STRING, primary key(pk)) "
                              + "USING TTL " + defaultTTL;

        executeDdl(createTableDDL);
        Table t1 = getTable(tableName);
        assertEquals(defaultTTL, t1.getDefaultTTL());


        TimeToLive alteredTTL = TimeToLive.ofDays(52);
        executeDdl("ALTER TABLE " + tableName + " USING TTL " + alteredTTL);

        Table t2 = getTable(tableName);
        assertEquals(alteredTTL, t2.getDefaultTTL());
    }

    @Test
    public void testAlteredTableTTLIsApplied() {
        String tableName = "TTL" + System.currentTimeMillis();
        TimeToLive defaultTTL = TimeToLive.ofDays(42);
        String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableName
                              + " (pk LONG, x STRING, primary key(pk)) "
                              + "USING TTL " + defaultTTL;

        executeDdl(createTableDDL);
        Table t1 = getTable(tableName);
        Row r1 = t1.createRow();
        long pk = System.currentTimeMillis();
        r1.put("pk", pk);

        tableImpl.put(r1, null, null);
        assertTimeToLive(defaultTTL, r1);

        TimeToLive alteredTTL = TimeToLive.ofDays(52);
        executeDdl("ALTER TABLE " + tableName + " USING TTL " + alteredTTL);

        Table t2 = getTable(tableName);
        Row r2 = t2.createRow();
        r2.put("pk", pk+1);

        tableImpl.put(r2, null, null);
        assertTimeToLive(alteredTTL, r2);
    }

    private static void assertTTLRows(TableIterator<Row> iter,
                                      int expected) {
        int count = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            assertTTLExists(row);
            count++;
        }
        assertTrue(count == expected);
    }

    private static void assertTTLKeyPairs(TableIterator<KeyPair> iter,
                                          int expected) {
        int count = 0;
        while (iter.hasNext()) {
            KeyPair kp = iter.next();
            assertTTLExists(kp.getPrimaryKey());
            count++;
        }
        assertTrue(count == expected);
    }

    /**
     * Defines a table with optional TTL default.
     */
    private Table createTable(TimeToLive defaultTTL) {
        String query = addUsingTTL(CREATE_TABLE, defaultTTL);

        executeDdl(query);
        executeDdl(CREATE_INDEX);
        table = getTable(TABLE_NAME);

        assertTrue(table != null);
        if (defaultTTL != null) {
            assertEquals(defaultTTL, table.getDefaultTTL());
        } else {
            assertTrue(table.getDefaultTTL() == null);
        }
        return table;
    }

    private void createChildTables(TimeToLive child1TTL, TimeToLive child2TTL) {
        String query = addUsingTTL(CREATE_CHILD1_TABLE, child1TTL);
        executeDdl(query);
        query = addUsingTTL(CREATE_CHILD2_TABLE, child2TTL);
        executeDdl(query);
        executeDdl(CREATE_CHILD1_INDEX);
        child1 = getTable("TTLTest.TTLChildOne");
        child2 = getTable("TTLTest.TTLChildOne.TTLChildTwo");
        assertTrue(child1 != null && child2 != null);
    }

    static private String addUsingTTL(final String query, TimeToLive ttl) {
        String newQuery = query;
        if (ttl != null) {
            newQuery += " using ttl " + ttl.getValue() + " " + ttl.getUnit();
        }
        return newQuery;
    }

    /**
     * Creates a new row and populates fields with values.
     */
    private Row createRow() {
        Row row = table.createRow();
        assertNoExpiry(row);
        long l = ++idCounter;
        return populate(row,
                        SHARD_KEY_1, "sk1",
                        SHARD_KEY_2, "sk2",
                        PK2_FIELD, "SSN-" + l,
                        STRING_FIELD, Long.toString(l));
    }

    private Row populate(final Row row, String...nvPairs) {
        for (int i = 0; nvPairs != null && i < nvPairs.length; i+=2) {
            row.put(nvPairs[i], nvPairs[i+1]);
        }
        return row;
    }

    /**
     * Asserts the give row has a no expiration time
     */
    private void assertNoExpiry(Row row) {
        assertTrue(row.getTTL() == null);
    }

    static private void assertTTLExists(Row row) {
        assertTrue(row.getExpirationTime() != 0);
    }

    private static Random rng = new Random();

    private TimeToLive randomTTL() {
        int next = rng.nextInt(100);
        while (next == 0) {
            next = rng.nextInt(100);
        }
        return rng.nextInt()%2 == 0 ? TimeToLive.ofDays(next) :
            TimeToLive.ofHours(next);
    }

    /**
     * Asserts given TTL duration and time-to-live duration of given row differs
     * not more than a day. The approximate comparison is to account for
     * rounding error and clock skew.
     */
    private void assertTimeToLive(TimeToLive ttl, Row row) {
        assertTimeToLive(ttl, row.getExpirationTime());
    }

    private void assertTimeToLive(TimeToLive ttl, long actual) {
        final long DAY_IN_MILLIS = 24*60*60*1000;
        long expected = ttl.toExpirationTime(System.currentTimeMillis());
        assertTrue("Actual TTL duration " + actual + "ms differs by "
                + "more than a day from expected duration of " + expected +"ms",
                Math.abs(actual - expected) < DAY_IN_MILLIS);
    }
}
