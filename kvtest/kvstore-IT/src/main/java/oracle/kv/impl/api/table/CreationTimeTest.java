/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

/**
 * Tests Creation-Time feature against data store. <br>
 */
public class CreationTimeTest extends TableTestBase {

    // todo: Uncomment these tests when creation time feature is enabled.
    
    // private Table table;
    // private Table child1;
    // private Table child2;
    // private long idCounter = System.currentTimeMillis();

    // private static final String CREATE_TABLE =
    //     "CREATE TABLE IF NOT EXISTS CreationTimeTest(sk1 STRING, sk2 STRING, "
    //         + "pk2 STRING, s2 STRING, primary key(shard(sk1,sk2), pk2))";
    // private static final String CREATE_CHILD1_TABLE =
    //     "CREATE TABLE IF NOT EXISTS CreationTimeTest.CTChildOne(child1 STRING, " +
    //         " primary key(child1))";
    // private static final String CREATE_CHILD2_TABLE =
    //     "CREATE TABLE IF NOT EXISTS CreationTimeTest.CTChildOne.CTChildTwo( " +
    //         "child2 STRING, primary key(child2))";
    // private static final String CREATE_INDEX =
    //     "CREATE INDEX IF NOT EXISTS s2index ON CreationTimeTest(s2)";
    // private static final String CREATE_CHILD1_INDEX =
    //     "CREATE INDEX IF NOT EXISTS child1index ON CreationTimeTest.CTChildOne(child1)";

    // private static final String TABLE_NAME = "CreationTimeTest";
    // private static final String SHARD_KEY_1 = "sk1";
    // private static final String SHARD_KEY_2 = "sk2";
    // private static final String PK2_FIELD = "pk2";
    // private static final String STRING_FIELD  = "s2";

    // private static final String CH1_TABLE_NAME = "CreationTimeTest.CTChildOne";
    // private static final String CH1_KEY = "child1";

    // private static final String CH2_TABLE_NAME = "CreationTimeTest.CTChildOne.CTChildTwo";
    // private static final String CH2_KEY = "child2";


    // private Table tableC;
    // private long idCounterC = System.currentTimeMillis();

    // private static final String CREATE_TABLEC =
    //     "CREATE TABLE IF NOT EXISTS CreationTimeTestC(sk1 STRING, sk2 STRING, "
    //         + "pk2 STRING, primary key(shard(sk1,sk2), pk2)) AS JSON COLLECTION";

    // private static final String CREATE_INDEXC =
    //     "CREATE INDEX IF NOT EXISTS s2indexC ON CreationTimeTestC(s2 as ANYATOMIC)";

    // private static final String TABLE_NAMEC = "CreationTimeTestC";


    // @BeforeClass
    // public static void staticSetUp() throws Exception {
    //     TableTestBase.staticSetUp(3,3,2, false, false, true);
    // }


    // @Test
    // public void testNewRowCTOnPut() {
    //     createTable();

    //     long startTime = System.currentTimeMillis();
    //     Row row = createRow();
    //     tableImpl.put(row, null, null);
    //     long interval = System.currentTimeMillis() - startTime;


    //     PrimaryKey pk = table.createPrimaryKey(row);
    //     Row dbRow = tableImpl.get(pk, null);
    //     long ct = dbRow.getCreationTime();
    //     assertTrue(ct >= startTime);
    //     assertTrue(ct - startTime <= interval);
    // }

    // @Test
    // public void testExistingRowCTOnSimplePut() {
    //     createTable();

    //     long startTime = System.currentTimeMillis();
    //     Row row = createRow();
    //     tableImpl.put(row, null, null);
    //     long interval = System.currentTimeMillis() - startTime;

    //     // Put same row again, check same creation date
    //     ReturnRow prevRow = table.createReturnRow(Choice.ALL);
    //     tableImpl.putIfPresent(row, prevRow, null);
    //     assertTrue(prevRow.getCreationTime() >= startTime);
    //     assertTrue(prevRow.getCreationTime() - startTime <= interval);

    //     // Put same row again, check same creation date
    //     prevRow = table.createReturnRow(Choice.VALUE);
    //     tableImpl.putIfPresent(row, prevRow, null);
    //     assertTrue(prevRow.getCreationTime() >= startTime);
    //     assertTrue(prevRow.getCreationTime() - startTime <= interval);

    //     // Put same row again, check same creation date
    //     prevRow = table.createReturnRow(Choice.ALL);
    //     tableImpl.putIfPresent(row, prevRow, null);
    //     assertTrue(prevRow.getCreationTime() >= startTime);
    //     assertTrue(prevRow.getCreationTime() - startTime <= interval);
    // }

    // @Test
    // public void testNewRowCTOnPutIfAbsent() {
    //     createTable();

    //     long startTime = System.currentTimeMillis();
    //     Row row = createRow();
    //     // putIfAbsent
    //     tableImpl.putIfAbsent(row, null, null);
    //     long interval = System.currentTimeMillis() - startTime;

    //     // get and check the creation time
    //     PrimaryKey pk = table.createPrimaryKey(row);
    //     Row dbRow = tableImpl.get(pk, null);
    //     assertTrue(dbRow.getCreationTime() >= startTime);
    //     assertTrue(dbRow.getCreationTime() - startTime <= interval);

    //     // Put same row again, check same creation date
    //     ReturnRow prevRow = table.createReturnRow(Choice.ALL);
    //     tableImpl.putIfPresent(row, prevRow, null);
    //     assertTrue(prevRow.getCreationTime() >= startTime);
    //     assertTrue(prevRow.getCreationTime() - startTime <= interval);


    //     // get the row again
    //     dbRow = tableImpl.get(pk, null);
    //     assertTrue(dbRow.getCreationTime() >= startTime);
    //     assertTrue(dbRow.getCreationTime() - startTime <= interval);
    // }


    // @Test
    // public void testBatchPutSameOption() throws Exception {
    //     createTable();
    //     List<TableOperation> puts = new ArrayList<>();
    //     List<PrimaryKey> pks = new ArrayList<>();
    //     TableOperationFactory factory = tableImpl.getTableOperationFactory();

    //     long startTime = System.currentTimeMillis();
    //     int N = 10;
    //     for (int i = 0; i < N; i++) {
    //         Row row = createRow();
    //         pks.add(row.createPrimaryKey());
    //         puts.add(factory.createPut(row, null, false));
    //     }

    //     tableImpl.execute(puts, null);
    //     long interval = System.currentTimeMillis() - startTime;

    //     for (PrimaryKey pk2 : pks) {
    //         Row dbRow = tableImpl.get(pk2, null);
    //         assertTrue(dbRow.getCreationTime() >= startTime);
    //         assertTrue(dbRow.getCreationTime() - startTime <= interval);
    //     }
    // }

    // /*
    //  * Test execute(List<TableOperation>) to parent and child tables, tables
    //  * has different TTL.
    //  */
    // @Test
    // public void testBatchPutParentChildTables() throws Exception {

    //     createTable();
    //     createChildTables();

    //     List<TableOperation> puts = new ArrayList<>();
    //     List<PrimaryKey> pks = new ArrayList<>();
    //     TableOperationFactory factory = tableImpl.getTableOperationFactory();
    //     WriteOptions options = new WriteOptions();

    //     long startTime = System.currentTimeMillis();
    //     /* Use default table TTL */
    //     int N = 2;
    //     for (int i = 0; i < N; i++) {
    //         Row row = createRow();
    //         pks.add(row.createPrimaryKey());
    //         puts.add(factory.createPut(row, null, false));

    //         Row child1Row = child1.createRow(row);
    //         child1Row.put("child1", String.valueOf(N));
    //         pks.add(child1Row.createPrimaryKey());
    //         puts.add(factory.createPut(child1Row, null, false));

    //         Row child2Row = child2.createRow(child1Row);
    //         child2Row.put("child2", String.valueOf(N));
    //         pks.add(child2Row.createPrimaryKey());
    //         puts.add(factory.createPut(child2Row, null, false));
    //     }

    //     tableImpl.execute(puts, options);
    //     long interval = System.currentTimeMillis() - startTime;

    //     for (PrimaryKey pk2 : pks) {
    //         Row dbRow = tableImpl.get(pk2, null);
    //         assertTrue(dbRow.getCreationTime() >= startTime);
    //         assertTrue(dbRow.getCreationTime() - startTime <= interval);
    //     }
    // }


    // /**
    //  * This is a somewhat long set of tests in a single test case.
    //  * They exercise the access to creation time via all the
    //  * supported iteration paths. These include:
    //  * 1. table scan (both key and row)
    //  * 2. multi-get (both key and row)
    //  * 3. bulk get (both key and row)
    //  * 4. index scan (both key and row)
    //  * In addition it tests for creation time in rows and keys returned
    //  * from ancestor and child tables.
    //  *
    //  * To perform this test there are 3 tables -- parent, child, and grandchild.
    //  * The basic iteration tests are done on the parent. The tests for
    //  * ancestor and child retrieval are done on the middle table -- the child.
    //  *
    //  * When populated all tables have the same number of rows inserted.
    //  */
    // @Test
    // public void testTableIterator() {
    //     final int numRows = 20;
    //     createTable();
    //     createChildTables();

    //     long startTime = System.currentTimeMillis();
    //     /*
    //      * Populate the parent table
    //      */
    //     for (int i = 0; i < numRows; i++) {
    //         Row row = createRow();
    //         tableImpl.put(row, null, null);
    //     }
    //     long interval = System.currentTimeMillis() - startTime;

    //     /*
    //      * Table iterator
    //      */
    //     TableIterator<Row> iter =
    //         tableImpl.tableIterator(table.createPrimaryKey(), null, null);
    //     assertCTRows(iter, startTime, interval, numRows);

    //     /*
    //      * Index iterator
    //      */
    //     Index index = table.getIndex("s2index");
    //     iter = tableImpl.tableIterator(index.createIndexKey(), null, null);
    //     assertCTRows(iter, startTime, interval, numRows);

    //     /*
    //      * Multi-get
    //      */
    //     PrimaryKey pkey = table.createPrimaryKey();
    //     pkey.put(SHARD_KEY_1, "sk1");
    //     pkey.put(SHARD_KEY_2, "sk2");
    //     List<Row> rows = tableImpl.multiGet(pkey, null, null);
    //     assertEquals(numRows, rows.size());
    //     for (Row row : rows) {
    //         assertTrue(row.getCreationTime() >= startTime);
    //         assertTrue(row.getCreationTime() - startTime <= interval);
    //     }

    //     /*
    //      * Multi-get keys
    //      *
    //      * Use the result rows to populate the keys array used for bulk get.
    //      * Also use the results to populate the child and grandchild tables.
    //      * They are used for ancestor/child table record retrieval, below.
    //      */
    //     /* for bulk get */
    //     List<PrimaryKey> keys = new ArrayList<>();

    //     pkey = table.createPrimaryKey();
    //     pkey.put(SHARD_KEY_1, "sk1");
    //     pkey.put(SHARD_KEY_2, "sk2");
    //     List<PrimaryKey> pkeys = tableImpl.multiGetKeys(pkey, null, null);
    //     assertTrue(pkeys.size() == numRows);
    //     int count = 0;
    //     for (PrimaryKey key : pkeys) {
    //         keys.add(key); /* bulk get */

    //         /*
    //          * Add rows to the child tables. They share the primary key of the
    //          * parent.
    //          */
    //         Row child1Row = child1.createRow(key);
    //         child1Row.put("child1", Integer.toString(count));
    //         tableImpl.put(child1Row, null, null);

    //         Row child2Row = child2.createRow(child1Row);
    //         child2Row.put("child2", Integer.toString(count));
    //         tableImpl.put(child2Row, null, null);

    //         ++count;
    //     }
    //     long intervalTotal = System.currentTimeMillis() - startTime;

    //     /*
    //      * Bulk get iterator
    //      */
    //     assertEquals(numRows, keys.size());
    //     iter = tableImpl.tableIterator(keys.iterator(), null, null);
    //     assertCTRows(iter, startTime, interval, numRows);

    //     assertEquals(numRows, keys.size());

    //     /*
    //      * Test cases for parent/child tables
    //      */

    //     /*
    //      * assert that the child tables are populated as expected
    //      */
    //     assertEquals(numRows, countTableRecords(child1.createPrimaryKey(), child1));
    //     assertEquals(numRows, countTableRecords(child2.createPrimaryKey(), child2));

    //     /*
    //      * Iterate the middle child, adding both the parent and child tables
    //      * to the targets. This exercises code in both directions sufficiently.
    //      *
    //      * Test this for all the same combinations above. Some of these share
    //      * code paths, but the extra coverage doesn't hurt.
    //      */

    //     /*
    //      * Table keys first
    //      */
    //     List<Table> ancestors = Collections.singletonList(table);
    //     List<Table> children = Collections.singletonList(child2);
    //     MultiRowOptions mro = new MultiRowOptions(null, ancestors, children);

    //     /*
    //      * Table rows
    //      */
    //     iter =
    //         tableImpl.tableIterator(child1.createPrimaryKey(), mro, null);
    //     assertCTRows(iter, startTime, intervalTotal, numRows * 3);

    //     /*
    //      * Multi-get
    //      */
    //     pkey = child1.createPrimaryKey();
    //     pkey.put(SHARD_KEY_1, "sk1");
    //     pkey.put(SHARD_KEY_2, "sk2");
    //     rows = tableImpl.multiGet(pkey, mro, null);
    //     assertEquals(numRows * 3, rows.size());
    //     for (Row row : rows) {
    //         assertTrue(row.getCreationTime() >= startTime);
    //         assertTrue(row.getCreationTime() - startTime <= intervalTotal);
    //     }

    //     /*
    //      * Index iterator
    //      *
    //      * An index was created on "child1" in the middle child table.
    //      *
    //      * NOTE: index scans cannot return child tables, so modify the
    //      * MultiRowOptions to only contain an ancestor.
    //      */
    //     mro = new MultiRowOptions(null, ancestors, null);
    //     index = child1.getIndex("child1index");
    //     iter = tableImpl.tableIterator(index.createIndexKey(), mro, null);
    //     assertCTRows(iter, startTime, intervalTotal, numRows * 2);
    // }

    // @Test
    // public void testPreviousRowOnPut() {
    //     createTable();

    //     Row row = createRow();
    //     long startTime = System.currentTimeMillis();
    //     tableImpl.put(row, null, null);
    //     long interval = System.currentTimeMillis() - startTime;

    //     ReturnRow rr = table.createReturnRow(Choice.ALL);

    //     tableImpl.put(row, rr, null);

    //     assertTrue(rr.getCreationTime() >= startTime);
    //     assertTrue(rr.getCreationTime() - startTime <= interval);
    // }

    // @Test
    // public void testPreviousRowOnPutIfAbsent() {
    //     createTable();

    //     for (Choice choice : Choice.values()) {
    //         Row row = createRow();
    //         ReturnRow rr = table.createReturnRow(choice);
    //         tableImpl.put(row, rr, null);

    //         assertNull(rr.getVersion());
    //         assertEquals(0, rr.getCreationTime());
    //     }
    // }

    // @Test
    // public void testPreviousRowOnPutIfPresent() {
    //     createTable();

    //     long startTime = System.currentTimeMillis();
    //     Row row = createRow();
    //     tableImpl.put(row, null, null);
    //     long interval = System.currentTimeMillis() - startTime;


    //     ReturnRow rr = table.createReturnRow(Choice.ALL);
    //     tableImpl.putIfPresent(row, rr, null);

    //     assertTrue(rr.getCreationTime() >= startTime);
    //     assertTrue(rr.getCreationTime() - startTime <= interval);
    // }

    // @Test
    // public void testPreviousRowOnPutIfVersion() {
    //     createTable();

    //     long startTime = System.currentTimeMillis();
    //     Row row = createRow();
    //     Version prevVersion = tableImpl.put(row, null, null);
    //     long interval = System.currentTimeMillis() - startTime;

    //     Version currentVersion = tableImpl.put(row, null, null);

    //     ReturnRow rr = table.createReturnRow(Choice.ALL);

    //     Row newRow = tableImpl.get(row.createPrimaryKey(), null);
    //     assertTrue(newRow.getCreationTime() >= startTime);
    //     assertTrue(newRow.getCreationTime() - startTime <= interval);

    //     /*
    //      * Use old version. This will fail, which means that return row
    //      * is available
    //      */
    //     Version newVersion =
    //         tableImpl.putIfVersion(row, prevVersion, rr, null);
    //     assertNull(newVersion);
    //     assertTrue(rr.getCreationTime() >= startTime);
    //     assertTrue(rr.getCreationTime() - startTime <= interval);

    //     /*
    //      * Use the correct version, which will succeed. Return row is empty
    //      */
    //     currentVersion =
    //         tableImpl.putIfVersion(row, currentVersion, rr, null);
    //     assertNotNull(currentVersion);
    //     // because the put went through there is no returnInfo
    //     assertTrue(rr.getCreationTime() == 0);
    // }

    // @Test
    // public void testPreviousRowOnPutInBatch() throws Exception {
    //     createTable();

    //     TableOperationFactory factory = tableImpl.getTableOperationFactory();

    //     List<TableOperation> puts = new ArrayList<TableOperation>();

    //     long startTime = System.currentTimeMillis();
    //     Row row = createRow();
    //     Version prevVersion = tableImpl.put(row, null, null);
    //     assertNotNull(prevVersion);
    //     TableOperation op = factory.createPut(row, Choice.ALL, false);
    //     puts.add(op);

    //     List<TableOperationResult> results =
    //         tableImpl.execute(puts, null);
    //     long interval = System.currentTimeMillis() - startTime;

    //     TableOperationResult r = results.get(0);
    //     assertTrue(r.getSuccess());
    //     Row rr = r.getPreviousRow();
    //     if (rr != null) {
    //         assertTrue(rr.getCreationTime() >= startTime);
    //         assertTrue(rr.getCreationTime() - startTime <= interval);
    //     }
    // }

    // @Test
    // public void testPreviousRowOnPutIfPresentInBatch() throws Exception {
    //     createTable();

    //     TableOperationFactory factory = tableImpl.getTableOperationFactory();

    //     List<TableOperation> puts = new ArrayList<TableOperation>();

    //     long startTime = System.currentTimeMillis();
    //     Row row = createRow();
    //     tableImpl.put(row, null, null);
    //     TableOperation op = factory.createPutIfPresent(row, Choice.ALL, false);
    //     puts.add(op);

    //     List<TableOperationResult> results =
    //         tableImpl.execute(puts, null);
    //     long interval = System.currentTimeMillis() - startTime;

    //     TableOperationResult r = results.get(0);
    //     assertTrue(r.getSuccess());
    //     Row rr = r.getPreviousRow();

    //     if (rr != null) {
    //         assertTrue(rr.getCreationTime() >= startTime);
    //         assertTrue(rr.getCreationTime() - startTime <= interval);
    //     }
    // }

    // /* this tests both delete and deleteIfVersion */
    // @Test
    // public void testPreviousRowOnDelete() {
    //     createTable();
    //     Row row = createRow();

    //     long startTime = System.currentTimeMillis();
    //     Version prevVersion = tableImpl.put(row, null, null);
    //     long interval = System.currentTimeMillis() - startTime;
    //     tableImpl.put(row, null, null);

    //     ReturnRow rr = table.createReturnRow(Choice.ALL);

    //     /* this will fail, which means ReturnRow is available */
    //     boolean deleted = tableImpl.deleteIfVersion(row.createPrimaryKey(),
    //         prevVersion,
    //         rr,
    //         null);
    //     assertFalse(deleted);
    //     assertTrue(rr.getCreationTime() >= startTime);
    //     assertTrue(rr.getCreationTime() - startTime <= interval);

    //     /* this will succeed, return row is available */
    //     deleted = tableImpl.delete(row.createPrimaryKey(),
    //         rr,
    //         null);
    //     assertTrue(deleted);
    //     assertTrue(rr.getCreationTime() >= startTime);
    //     assertTrue(rr.getCreationTime() - startTime <= interval);
    // }

    // @Test
    // public void testQueryFunction() {
    //     createTable();

    //     long startTime = System.currentTimeMillis();
    //     Row row = createRow();
    //     tableImpl.put(row, null, null);

    //     Row dbRow = tableImpl.get(row.createPrimaryKey(), null);

    //     long interval = System.currentTimeMillis() - startTime;
    //     assertTrue(dbRow.getCreationTime() >= startTime);
    //     assertTrue(dbRow.getCreationTime() - startTime <= interval);

    //     // query top level table
    //     try {
    //         int rows = 0;
    //         TableIterator<RecordValue> it =
    //             store.execute("SELECT creation_time($t) as ct, " +
    //                     "creation_time_millis($t) as ctm, " +
    //                     "modification_time($t) as mt " +
    //                     " FROM " + TABLE_NAME + " $t")
    //                 .get()
    //                 .iterator();
    //         while(it.hasNext()) {
    //             RecordValue r = it.next();
    //             long ctm = r.get("ctm").asLong().get();
    //             long ct  = r.get("ct").asTimestamp().get().getTime();
    //             long mt  = r.get("mt").asTimestamp().get().getTime();

    //             assertTrue("  ctm: " + (ctm - startTime) +
    //                                 " should be >= 0", ctm >= startTime);
    //             assertTrue("  creationTime: " + (ctm - startTime) +
    //                 " not in expected interval: " + interval,
    //                 ctm - startTime <= interval);

    //             assertTrue("  ct: " + (ct - startTime) +
    //                                 " should be >= 0", ct >= startTime);
    //             assertTrue("  ct: " + (ct - startTime) +
    //                 " not in expected interval: " + interval,
    //                 ct - startTime <= interval);

    //             assertTrue("  mt: " + (mt - startTime) +
    //                     " should be >= 0", mt >= startTime);
    //             assertTrue("  mt: " + (mt - startTime) +
    //                     " not in expected interval: " + interval,
    //                 mt - startTime <= interval);

    //             rows++;
    //         }
    //         assertEquals(1, rows);
    //     } catch (Throwable e) {
    //         e.printStackTrace();
    //         fail("Got exception during query iteration: " + e);
    //     }


    //     // child tables
    //     createChildTables();
    //     long startTimeCh = System.currentTimeMillis();

    //     /* for bulk get */
    //     List<PrimaryKey> keys = new ArrayList<PrimaryKey>();

    //     PrimaryKey pkey = table.createPrimaryKey();
    //     pkey.put(SHARD_KEY_1, "sk1");
    //     pkey.put(SHARD_KEY_2, "sk2");
    //     List<PrimaryKey> pkeys = tableImpl.multiGetKeys(pkey, null, null);
    //     assertEquals(1, pkeys.size());

    //     int count = 0;
    //     for (PrimaryKey key : pkeys) {
    //         assertTrue(key.getCreationTime() == 0);

    //         keys.add(key); /* bulk get */

    //         /*
    //          * Add rows to the child tables. They share the primary key of the
    //          * parent.
    //          */
    //         Row child1Row = child1.createRow(key);
    //         child1Row.put(CH1_KEY, Integer.toString(count));
    //         tableImpl.put(child1Row, null, null);

    //         Row child2Row = child2.createRow(child1Row);
    //         child2Row.put(CH2_KEY, Integer.toString(count));
    //         tableImpl.put(child2Row, null, null);

    //         ++count;
    //     }
    //     long intervalCh = System.currentTimeMillis() - startTimeCh;

    //     // query child tables
    //     try {
    //         int rows = 0;
    //         TableIterator<RecordValue> it =
    //             store.execute("SELECT creation_time($t) as ct, " +
    //                     "creation_time_millis($t) as ctm FROM " + CH1_TABLE_NAME +
    //                     " $t")
    //                 .get()
    //                 .iterator();
    //         while(it.hasNext()) {
    //             RecordValue r = it.next();
    //             long ctm = r.get("ctm").asLong().get();
    //             long ct  = r.get("ct").asTimestamp().get().getTime();

    //             assertTrue(ctm >= startTimeCh);
    //             assertTrue(ctm - startTimeCh <= intervalCh);
    //             assertTrue(ct >= startTimeCh);
    //             assertTrue(ct - startTimeCh <= intervalCh);
    //             rows++;
    //         }
    //         assertEquals(1, rows);

    //         rows = 0;
    //         it =
    //             store.execute("SELECT creation_time($t) as ct, " +
    //                     "creation_time_millis($t) as ctm FROM " + CH2_TABLE_NAME +
    //                     " $t")
    //                 .get()
    //                 .iterator();
    //         while(it.hasNext()) {
    //             RecordValue r = it.next();

    //             long ctm = r.get("ctm").asLong().get();
    //             long ct  = r.get("ct").asTimestamp().get().getTime();

    //             assertTrue(ctm >= startTimeCh);
    //             assertTrue(ctm - startTimeCh <= intervalCh);
    //             assertTrue(ct >= startTimeCh);
    //             assertTrue(ct - startTimeCh <= intervalCh);
    //             rows++;
    //         }
    //         assertEquals(1, rows);
    //     } catch (Throwable e) {
    //         e.printStackTrace();
    //         fail("Got exception during query iteration: " + e);
    //     }
    // }

    // @Test
    // public void testQueryFunctionCollections() {
    //     createTableC();

    //     long startTime = System.currentTimeMillis();
    //     Row row = createRowC();
    //     tableImpl.put(row, null, null);

    //     Row dbRow = tableImpl.get(row.createPrimaryKey(), null);

    //     long interval = System.currentTimeMillis() - startTime;
    //     assertTrue(dbRow.getCreationTime() >= startTime);
    //     assertTrue(dbRow.getCreationTime() - startTime <= interval);

    //     // query top level table
    //     try {
    //         int rows = 0;
    //         TableIterator<RecordValue> it =
    //             store.execute("SELECT creation_time($t) as ct, " +
    //                     "creation_time_millis($t) as ctm, " +
    //                     "modification_time($t) as mt " +
    //                     " FROM " + TABLE_NAMEC + " $t")
    //                 .get()
    //                 .iterator();
    //         while(it.hasNext()) {
    //             RecordValue r = it.next();
    //             long ctm = r.get("ctm").asLong().get();
    //             long ct  = r.get("ct").asTimestamp().get().getTime();
    //             long mt  = r.get("mt").asTimestamp().get().getTime();

    //             assertTrue("  ctm: " + (ctm - startTime) +
    //                 " should be >= 0", ctm >= startTime);
    //             assertTrue("  creationTime: " + (ctm - startTime) +
    //                     " not in expected interval: " + interval,
    //                 ctm - startTime <= interval);

    //             assertTrue("  ct: " + (ct - startTime) +
    //                 " should be >= 0", ct >= startTime);
    //             assertTrue("  ct: " + (ct - startTime) +
    //                     " not in expected interval: " + interval,
    //                 ct - startTime <= interval);

    //             assertTrue("  mt: " + (mt - startTime) +
    //                 " should be >= 0", mt >= startTime);
    //             assertTrue("  mt: " + (mt - startTime) +
    //                     " not in expected interval: " + interval,
    //                 mt - startTime <= interval);

    //             rows++;
    //         }
    //         assertEquals(1, rows);
    //     } catch (Throwable e) {
    //         e.printStackTrace();
    //         fail("Got exception during query iteration: " + e);
    //     }


    //     /* for bulk get */
    //     PrimaryKey pkey = tableC.createPrimaryKey();
    //     pkey.put(SHARD_KEY_1, "sk1");
    //     pkey.put(SHARD_KEY_2, "sk2");
    //     List<PrimaryKey> pkeys = tableImpl.multiGetKeys(pkey, null, null);
    //     assertEquals(1, pkeys.size());
    // }

    // private static void assertCTRows(TableIterator<Row> iter, long expectedTime,
    //     long interval,
    //     int expected) {
    //     int count = 0;
    //     while (iter.hasNext()) {
    //         Row row = iter.next();
    //         assertTrue("  creationTime: " +
    //             (row.getCreationTime() - expectedTime) +
    //             " not in expected interval: " + interval,(row.getCreationTime() - expectedTime) >= 0);
    //         assertTrue("  creationTime: " +
    //                 (row.getCreationTime() - expectedTime) +
    //                 " not in expected interval: " + interval,
    //             row.getCreationTime() - expectedTime <= interval);
    //         count++;
    //     }
    //     assertTrue(count == expected);
    // }

    // private Table createTable() {
    //     String query = CREATE_TABLE;

    //     executeDdl(query);
    //     executeDml("DELETE FROM " + TABLE_NAME);
    //     executeDdl(CREATE_INDEX);
    //     table = getTable(TABLE_NAME);

    //     assertTrue(table != null);
    //     assertTrue(table.getDefaultTTL() == null);

    //     return table;
    // }

    // private void createChildTables() {
    //     String query = CREATE_CHILD1_TABLE;
    //     executeDdl(query);
    //     query = CREATE_CHILD2_TABLE;
    //     executeDdl(query);
    //     executeDml("DELETE FROM CreationTimeTest.CTChildOne");
    //     executeDml("DELETE FROM CreationTimeTest.CTChildOne.CTChildTwo");
    //     executeDdl(CREATE_CHILD1_INDEX);
    //     child1 = getTable("CreationTimeTest.CTChildOne");
    //     child2 = getTable("CreationTimeTest.CTChildOne.CTChildTwo");
    //     assertTrue(child1 != null && child2 != null);
    // }

    // /**
    //  * Creates a new row and populates fields with values.
    //  */
    // private Row createRow() {
    //     Row row = table.createRow();
    //     long l = ++idCounter;
    //     return populate(row,
    //         SHARD_KEY_1, "sk1",
    //         SHARD_KEY_2, "sk2",
    //         PK2_FIELD, "SSN-" + l,
    //         STRING_FIELD, Long.toString(l));
    // }

    // private Table createTableC() {
    //     String query = CREATE_TABLEC;

    //     executeDdl(query);
    //     executeDml("DELETE FROM " + TABLE_NAMEC);
    //     executeDdl(CREATE_INDEXC);
    //     tableC = getTable(TABLE_NAMEC);

    //     assertTrue(tableC != null);
    //     assertTrue(tableC.getDefaultTTL() == null);

    //     return table;
    // }

    // private Row createRowC() {
    //     Row row = tableC.createRow();
    //     long l = ++idCounterC;
    //     return populate(row,
    //         SHARD_KEY_1, "sk1",
    //         SHARD_KEY_2, "sk2",
    //         PK2_FIELD, "SSN-" + l,
    //         STRING_FIELD, Long.toString(l));
    // }

    // private Row populate(final Row row, String...nvPairs) {
    //     for (int i = 0; nvPairs != null && i < nvPairs.length; i+=2) {
    //         row.put(nvPairs[i], nvPairs[i+1]);
    //     }
    //     return row;
    // }



    // @Test
    // public void testRegularTableWithIndex() throws InterruptedException {
    //     String q = "CREATE TABLE IF NOT EXISTS t (i integer, s string, PRIMARY KEY(i))";
    //     PreparedStatement ps = store.prepare(q);
    //     assertNotNull(ps);

    //     StatementResult sr = store.executeSync(ps);
    //     assertNotNull(sr);

    //     q = "CREATE INDEX t1idx1 ON t (s)";
    //     ps = store.prepare(q);
    //     assertNotNull(ps);

    //     sr = store.executeSync(ps);
    //     assertNotNull(sr);

    //     Thread.sleep(2000);

    //     TableAPI api = store.getTableAPI();

    //     Table tableT = api.getTable("t");

    //     Row row = tableT.createRow();
    //     row.put("i", 1);
    //     row.put("s", "string");

    //     // put a few more for query testing
    //     for(int i = 0; i < 10; i++) {
    //         Row tableRow = tableT.createRow();
    //         tableRow.put("i", 20 + i);
    //         tableRow.put("s", "string5");

    //         api.put(tableRow, null, null);
    //     }

    //     // query
    //     try {
    //         int rows = 0;
    //         TableIterator<RecordValue> it =
    //             store.execute("SELECT i, s, creation_time($t) as ct, " +
    //                     "creation_time_millis($t) as ctm, " +
    //                     "modification_time($t) as mt FROM t $t").get()
    //                 .iterator();
    //         while(it.hasNext()) {
    //             RecordValue r = it.next();

    //             assertEquals("string5", r.get("s").asString().get());
    //             assertTrue(r.get("ct").isTimestamp());
    //             assertTrue(r.get("ct").asTimestamp().get().getTime() > 0);
    //             assertTrue(r.get("ctm").isLong());
    //             assertTrue(r.get("ctm").asLong().get() > 0);
    //             assertTrue(r.get("mt").isTimestamp());
    //             assertTrue(r.get("mt").asTimestamp().get().getTime() > 0);

    //             rows++;
    //         }
    //         assertEquals(10, rows);
    //     } catch (Throwable e) {
    //         e.printStackTrace();
    //         fail("Got exception during query iteration: " + e);
    //     }
    // }

    // @Test
    // public void testRegularTableWithCreationTimeIndex()
    //     throws InterruptedException {
    //     String q = "CREATE TABLE IF NOT EXISTS tRegWithIdx (i integer, s string, PRIMARY KEY(i))";
    //     PreparedStatement ps = store.prepare(q);
    //     assertNotNull(ps);

    //     StatementResult sr = store.executeSync(ps);
    //     assertNotNull(sr);

    //     q = "CREATE INDEX t1idx2 ON tRegWithIdx ( creation_time() )";
    //     ps = store.prepare(q);
    //     assertNotNull(ps);

    //     sr = store.executeSync(ps);
    //     assertNotNull(sr);

    //     Thread.sleep(2000);

    //     TableAPI api = store.getTableAPI();

    //     Table tableTRegWithIdx = api.getTable("tRegWithIdx");

    //     // put a few more for query testing
    //     long startTime = System.currentTimeMillis();
    //     for(int i = 0; i < 10; i++) {
    //         Row tableRow = tableTRegWithIdx.createRow();
    //         tableRow.put("i", 20 + i);
    //         tableRow.put("s", "string5");

    //         api.put(tableRow, null, null);
    //     }
    //     long interval = System.currentTimeMillis() - startTime;

    //     // query
    //     try {
    //         int rows = 0;
    //         q = "SELECT creation_time($t) as ct " +
    //             "FROM tRegWithIdx $t ";
    //         ps = store.prepare(q);
    //         assertTrue(ps.toString().contains("t1idx2"));

    //         TableIterator<RecordValue> it = store.executeSync(ps).iterator();
    //         while(it.hasNext()) {
    //             RecordValue r = it.next();

    //             assertTrue(r.get("ct").isTimestamp());
    //             assertTrue(r.get("ct").asTimestamp().get().getTime() > 0);
    //             assertTrue(r.get("ct").asTimestamp().get().getTime() - startTime <= interval);

    //             rows++;
    //         }
    //         assertEquals(10, rows);
    //     } catch (Throwable e) {
    //         e.printStackTrace();
    //         fail("Got exception during query iteration: " + e);
    //     }
    // }

    // @Test
    // public void testCollectionTable() {
    //     String q = "CREATE TABLE t2 (i integer, " +
    //         " PRIMARY KEY(i)) AS  JSON COLLECTION ";
    //     PreparedStatement ps = store.prepare(q);
    //     assertNotNull(ps);

    //     StatementResult sr = store.executeSync(ps);
    //     assertNotNull(sr);

    //     TableAPI api = store.getTableAPI();

    //     Table tableT2 = api.getTable("t2");

    //     Row row = tableT2.createRow();
    //     row.put("i", 1);
    //     row.put("s", "string");

    //     // put a few more for query testing
    //     for(int i = 0; i < 10; i++) {
    //         Row tableRow = tableT2.createRow();
    //         tableRow.put("i", 20 + i);
    //         tableRow.put("s", "string5");
    //         api.put(tableRow, null, null);
    //     }

    //     // query
    //     try {
    //         int rows = 0;
    //         TableIterator<RecordValue> it =
    //             store.execute("SELECT i, $t.s, creation_time($t) as ct, " +
    //                     "creation_time_millis($t) as ctm, " +
    //                     "modification_time($t) as mt  FROM t2 $t").get()
    //                 .iterator();
    //         while(it.hasNext()) {
    //             RecordValue r = it.next();
    //             assertEquals("string5", r.get("s").asString().get());

    //             assertTrue(r.get("ct").isTimestamp());
    //             assertTrue(r.get("ct").asTimestamp().get().getTime() > 0);
    //             assertTrue(r.get("ctm").isLong());
    //             assertTrue(r.get("ctm").asLong().get() > 0);
    //             assertTrue(r.get("mt").isTimestamp());
    //             assertTrue(r.get("mt").asTimestamp().get().getTime() > 0);

    //             rows++;
    //         }
    //         assertEquals(10, rows);
    //     } catch (Throwable e) {
    //         fail("Got exception during query iteration: " + e);
    //     }

    //     // put null for row metadata and query again
    //     for(int i = 0; i < 10; i++) {
    //         Row tableRow = tableT2.createRow();
    //         tableRow.put("i", 20 + i);
    //         tableRow.put("s", "string6");

    //         api.put(tableRow, null, null);
    //     }

    //     // query
    //     try {
    //         int rows = 0;
    //         TableIterator<RecordValue> it =
    //             store.execute("SELECT i, $t.s," +
    //                     "creation_time($t) as ct, " +
    //                     "creation_time_millis($t) as ctm, " +
    //                     "modification_time($t) as mt FROM t2 $t").get()
    //                 .iterator();
    //         while(it.hasNext()) {
    //             RecordValue r = it.next();
    //             assertEquals("string6", r.get("s").asString().get());

    //             assertTrue(r.get("ct").isTimestamp());
    //             assertTrue(r.get("ct").asTimestamp().get().getTime() > 0);
    //             assertTrue(r.get("ctm").isLong());
    //             assertTrue(r.get("ctm").asLong().get() > 0);
    //             assertTrue(r.get("mt").isTimestamp());
    //             assertTrue(r.get("mt").asTimestamp().get().getTime() > 0);

    //             rows++;
    //         }
    //         assertEquals(10, rows);
    //     } catch (Throwable e) {
    //         fail("Got exception during query iteration: " + e);
    //     }
    // }


    // @Test
    // public void testCollectionTablewithIndex() {
    //     String q = "CREATE TABLE t2 (i integer, " +
    //         " PRIMARY KEY(i)) AS  JSON COLLECTION ";
    //     PreparedStatement ps = store.prepare(q);
    //     assertNotNull(ps);

    //     StatementResult sr = store.executeSync(ps);
    //     assertNotNull(sr);

    //     q = "CREATE INDEX t2idx1 ON t2 (s as ANYATOMIC)";
    //     ps = store.prepare(q);
    //     assertNotNull(ps);

    //     sr = store.executeSync(ps);
    //     assertNotNull(sr);

    //     TableAPI api = store.getTableAPI();

    //     Table tableT2 = api.getTable("t2");

    //     Row row = tableT2.createRow();
    //     row.put("i", 1);
    //     row.put("s", "string");

    //     // put a few more for query testing
    //     for(int i = 0; i < 10; i++) {
    //         Row tableRow = tableT2.createRow();
    //         tableRow.put("i", 20 + i);
    //         tableRow.put("s", "string5");

    //         api.put(tableRow, null, null);
    //     }

    //     // query
    //     try {
    //         int rows = 0;
    //         TableIterator<RecordValue> it =
    //             store.execute("SELECT i, $t.s, " +
    //                     "creation_time($t) as ct, " +
    //                     "creation_time_millis($t) as ctm, " +
    //                     "modification_time($t) as mt FROM t2 $t").get()
    //                 .iterator();
    //         while(it.hasNext()) {
    //             RecordValue r = it.next();
    //             assertEquals("string5", r.get("s").asString().get());

    //             assertTrue(r.get("ct").isTimestamp());
    //             assertTrue(r.get("ct").asTimestamp().get().getTime() > 0);
    //             assertTrue(r.get("ctm").isLong());
    //             assertTrue(r.get("ctm").asLong().get() > 0);
    //             assertTrue(r.get("mt").isTimestamp());
    //             assertTrue(r.get("mt").asTimestamp().get().getTime() > 0);

    //             rows++;
    //         }
    //         assertEquals(10, rows);
    //     } catch (Throwable e) {
    //         fail("Got exception during query iteration: " + e);
    //     }
    // }
}

