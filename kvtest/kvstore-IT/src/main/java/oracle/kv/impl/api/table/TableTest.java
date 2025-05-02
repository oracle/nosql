/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.systables.TableMetadataDesc.METADATA_TABLE_ID;
import static oracle.kv.table.TableUtils.getDataSize;
import static oracle.kv.table.TableUtils.getKeySize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.Direction;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.StoreIteratorException;
import oracle.kv.Value;
import oracle.kv.Value.Format;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.RequestDispatcherImpl;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.MapValue;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.ReturnRow.Choice;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/*
 * This is a catch-all class to test various table cases.
 */
public class TableTest extends TableTestBase {
    private static final int NUSERS = 50;
    private static TableIteratorOptions smallBatchOptions =
        new TableIteratorOptions(Direction.FORWARD, null, 0, null,
                                 0, 4);

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /**
         * Exclude tombstones because some unit tests count the number of
         * records in the store and tombstones will cause the result not
         * match the expected values.*/
        staticSetUp(true /* excludeTombstone */);
    }

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        clearTableImplTestConfig();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        clearTableImplTestConfig();
    }

    /*
     * Miscellaneous simple tests.
     */
    @Test
    public void testMisc()
        throws Exception {

        TableImpl userTable = buildUserTable();
        addUserRows(userTable, NUSERS);
        int numRecords = countTableRecords(userTable.createPrimaryKey(), null);
        assertEquals(NUSERS, numRecords);

        /* Verify the get by ID call */
        tableImpl.clearCache();
        TableImpl check = (TableImpl)tableImpl.getTableById(userTable.getId());
        assertNotNull(check);
        assertEquals(userTable.getId(), check.getId());

        tableImpl.clearCache();
        check = (TableImpl)tableImpl.getTableById(METADATA_TABLE_ID);
        assertNotNull(check);
        assertEquals(METADATA_TABLE_ID, check.getId());

        /*
         * Do some testing of RecordValue.copyFrom on derivative classes.
         */
        Index index = userTable.getIndex("FirstName");

        /*
         * Get a row
         */
        PrimaryKey key = userTable.createPrimaryKey();
        key.put("firstName", "first1");
        key.put("lastName", "last1");
        Row row = tableImpl.get(key, null);
        assertTrue(row != null);

        /*
         * Check to exercise Row.get{Key,Data}Size.
         */
        assertEquals("Key sizes don't match",
                     getKeySize(row), getKeySize(key));
        getDataSize(row);
        try {
            getDataSize(key);
            fail("getDataSize() should have thrown");
        } catch (IllegalArgumentException iae) {
        }
        PrimaryKey partKey = userTable.createPrimaryKey();
        partKey.put("firstName", "first");
        try {
            getKeySize(partKey);
            fail("getKeySize() should have thrown: out of order field");
        } catch (IllegalArgumentException iae) {
        }
        partKey.remove("firstName");
        partKey.put("lastName", "last");
        assertTrue(getKeySize(partKey) < getKeySize(key));

        /*
         * Copy from the retrieved row and assert equality
         */
        Row newRow = userTable.createRow();
        newRow.copyFrom(row);
        assertTrue(newRow.equals(row));

        /*
         * Copy to empty IndexKey and verify.
         */
        @SuppressWarnings("deprecation")
        IndexKey ikey = index.createIndexKey(row);
        assertFieldAbsent(ikey, "lastName");
        assertTrue(ikey.get("firstName").asString().get().equals("first1"));

        /*
         * key is the length of the string plus trailing null, if index is
         * allowed to contain null value, then plus a extra byte for null
         * indicator.
         */
        assertEquals("first1".length() + 1, getKeySize(ikey));
        /*
         * Copy to empty PrimaryKey and verify.
         */
        PrimaryKey newKey = userTable.createPrimaryKey();
        newKey.copyFrom(row);
        assertFieldAbsent(newKey, "age");
        assertTrue(row.get("age") != null);
        assertTrue(newKey.equals(key));
    }

    /*
     * Test semantics of various iterations.
     * o full key (primary or index) means a "get".  In the case of primary key
     * there will only be one result.  In the case of index iteration all
     * matching duplicates are returned.
     * o ranges are specified with a partial or empty key and/or a FieldRange.
     * The FieldRange is applied to the first "not specified" field in the key.
     */
    @Test
    public void testIteration()
        throws Exception {

        FieldRange range = null;
        MultiRowOptions mro = null;
        TableImpl userTable = buildUserTable();
        TableImpl intTable = buildIntTable();
        PrimaryKey key = null;
        int numRecords = 0;

        addUserRows(userTable, NUSERS);
        addIntRows(intTable, NUSERS);

        numRecords = countTableRecords(userTable.createPrimaryKey(), null);
        assertEquals(numRecords, NUSERS);

        /*
         * Iterate entire table
         */
        key = intTable.createPrimaryKey();
        numRecords = countTableRecords(key, null);
        assertEquals("Unexpected number of records", NUSERS, numRecords);

        /*
         * Iterate just one key
         */
        key = intTable.createPrimaryKey();
        key.put("id", 1);
        numRecords = countTableRecords(key, null);
        assertEquals("Unexpected number of records", 1, numRecords);

        /*
         * Iterate range of ids > 5 (exclusive)
         */
        key = intTable.createPrimaryKey();
        range = intTable.createFieldRange("id").setStart(5, false);
        range.toString(); /* code coverage */
        mro = range.createMultiRowOptions();
        numRecords = countTableRecords1(key, mro);
        assertEquals("Unexpected number of records", NUSERS - 6, numRecords);

        /*
         * Indexes
         */
        Index index = intTable.getIndex("int1");

        /*
         * Exact match on value 1
         */
        IndexKey ikey = index.createIndexKey();
        ikey.put("int1", 1);
        numRecords = countIndexRecords(ikey, null);
        assertEquals("Unexpected number of records", 1, numRecords);

        /*
         * Range > 5
         */
        ikey = index.createIndexKey();
        range = index.createFieldRange("int1").setStart(5, false);
        mro = range.createMultiRowOptions();
        numRecords = countIndexRecords1(ikey, mro);
        assertEquals("Unexpected number of records", NUSERS - 6, numRecords);

        /*
         * Range >= 5, < 10
         */
        range = index.createFieldRange("int1")
            .setStart(5, true).setEnd(10, true);
        mro = range.createMultiRowOptions();
        numRecords = countIndexRecords1(ikey, mro);
        assertEquals("Unexpected number of records", 6, numRecords);

        /*
         * Exact match where the index has no entry.  This should not find the
         * next >= record.  It should return nothing.
         */
        index = intTable.getIndex("allint");
        ikey = index.createIndexKey();
        ikey.put("int1", 1);
        ikey.put("int2", 10);
        ikey.put("int3", 99);
        numRecords = countIndexRecords1(ikey, null);
        assertEquals("Unexpected number of records", 0, numRecords);

        /*
         * Same information as above but in a range.  This will only
         * find one entry because of the exact match on int1 and int2.
         */
        ikey = index.createIndexKey();
        ikey.put("int1", 1);
        ikey.put("int2", 10);
        range = index.createFieldRange("int3").setStart(99, true);
        numRecords = countIndexRecords1(ikey, range.createMultiRowOptions());
        assertEquals("Unexpected number of records", 1, numRecords);

        /*
         * Add more records to the intTable, but using even numbered ids.
         * Ignore the indexed fields.
         */
        for (int i = 1; i < 10; i++) {
            Row row = intTable.createRow();
            row.put("id", NUSERS + (i*2));
            row.put("int1", i);
            row.put("int2", i);
            row.put("int3", i);
            tableImpl.put(row, null, null);
        }

        /*
         * This should match no keys
         */
        key = intTable.createPrimaryKey();
        key.put("id", NUSERS + 1);
        numRecords = countTableRecords(key, null);
        assertEquals("Unexpected number of records", 0, numRecords);

        /*
         * This is a range query and should match the new keys
         */
        key = intTable.createPrimaryKey();
        range = intTable.createFieldRange("id").setStart(NUSERS + 1, true);
        numRecords = countTableRecords1(key, range.createMultiRowOptions());
        assertEquals("Unexpected number of records", 9, numRecords);
    }

    @Test
    public void testReverseIteration()
        throws Exception {

        FieldRange range = null;
        TableImpl shardTable = buildShardedTable();
        PrimaryKey key = null;
        int numRecords = 0;
        final int NUM_PER_USER = 10;
        TableIteratorOptions reverseOptions =
            new TableIteratorOptions(Direction.REVERSE, null, 0, null);
        TableIteratorOptions forwardOptions =
            new TableIteratorOptions(Direction.FORWARD, null, 0, null);

        addShardRows(shardTable, NUSERS, NUM_PER_USER);

        /*
         * Iterate entire table, major part is not completed.
         */
        key = shardTable.createPrimaryKey();

        TableIterator<Row> iter =
            tableImpl.tableIterator(key, null, reverseOptions);
        Row lastRow = null;
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastRow != null) {
                assertTrue(compareKeys(row, lastRow) < 0);
            }
            lastRow = row;
        }
        assertEquals("Unexpected record count", NUSERS * NUM_PER_USER,
                     numRecords);
        iter.close();

        /*
         * Iterate again, FORWARD
         */
        numRecords = 0;
        lastRow = null;
        iter = tableImpl.tableIterator(key, null, forwardOptions);
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastRow != null) {
                assertTrue(compareKeys(row, lastRow) > 0);
            }
            lastRow = row;
        }
        assertEquals("Unexpected record count", NUSERS * NUM_PER_USER,
                     numRecords);
        iter.close();

        /*
         * Use a range on the second component of the primary key
         */
        range = shardTable.createFieldRange("id1").setStart(5, true);
        numRecords = 0;
        lastRow = null;
        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       forwardOptions);
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastRow != null) {
                assertTrue(compareKeys(row, lastRow) > 0);
            }
            lastRow = row;
        }
        assertEquals("Unexpected record count", (NUSERS-5) * NUM_PER_USER,
                     numRecords);
        iter.close();

        /*
         * Reverse again.  Add an end to the range.
         */
        numRecords = 0;
        lastRow = null;
        range.setEnd(8, false); /* 8, exclusive, total will be 3 */
        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       reverseOptions);
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastRow != null) {
                assertTrue(compareKeys(row, lastRow) < 0);
            }
            lastRow = row;
        }
        assertEquals("Unexpected record count", 3 * NUM_PER_USER, numRecords);
        iter.close();

        /*
         * Iterate a single user id, REVERSE.  Primary key is "id1", "id2"
         */
        numRecords = 0;
        FieldValue lastValue = null;
        key.put("id1", 1); /* set a major key to do reverse iteration */
        iter = tableImpl.tableIterator(key, null, reverseOptions);
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastValue != null) {
                assertTrue(row.get("id2").compareTo(lastValue) < 0);
            }
            lastValue = row.get("id2");
        }
        assertEquals("Unexpected record count", NUM_PER_USER, numRecords);
        iter.close();

        /*
         * Iterate again, FORWARD
         */
        numRecords = 0;
        lastValue = null;
        iter = tableImpl.tableIterator(key, null, forwardOptions);
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastValue != null) {
                assertTrue(row.get("id2").compareTo(lastValue) > 0);
            }
            lastValue = row.get("id2");
        }
        assertEquals("Unexpected record count", NUM_PER_USER, numRecords);
        iter.close();

        /*
         * Use a range on the second component of the primary key
         */
        range = shardTable.createFieldRange("id2").setStart(5, true);
        numRecords = 0;
        lastValue = null;
        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       forwardOptions);
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastValue != null) {
                assertTrue(row.get("id2").compareTo(lastValue) > 0);
                lastValue = row.get("id2");
            }
        }
        assertEquals("Unexpected record count", NUM_PER_USER-5, numRecords);
        iter.close();

        /*
         * Reverse again.  Add an end to the range.
         */
        numRecords = 0;
        lastValue = null;
        range.setEnd(8, false); /* 8, exclusive, total will be 3 */
        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       reverseOptions);
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastValue != null) {
                assertTrue(row.get("id2").compareTo(lastValue) < 0);
                lastValue = row.get("id2");
            }
        }
        assertEquals("Unexpected record count", 3, numRecords);
        iter.close();
    }

    /**
     * Test that changing the number of shards during a shard iteration
     * produces the expected exception [#27506]
     */
    @Test
    public void testIterationWithShardFailure()
        throws Exception {

        TableImpl userTable = buildUserTable();
        addUserRows(userTable, NUSERS);

        Index index = userTable.getIndex("Age");
        IndexKey ikey = index.createIndexKey();
        ikey.put("age", 10);

        TableIterator<Row> iter =
            tableImpl.tableIterator(ikey, null, countOpts);

        /*
         * Insert a synthetic topology update with more shards, which should
         * result in a StoreIteratorException with cause
         * UnsupportedOperationException
         */
        RequestDispatcherImpl dispatcher =
            (RequestDispatcherImpl) ((KVStoreImpl) store).getDispatcher();
        Topology topo = dispatcher.getTopologyManager().getTopology();
        Topology newTopo = topo.getCopy();
        newTopo.add(new RepGroup());
        dispatcher.getTopologyManager().update(newTopo);

        try {
            iter.next();
            fail("Expected StoreIteratorException");
        } catch (StoreIteratorException e) {
            assertTrue("Expected UnsupportedOperationException cause," +
                       " found: " + e.getCause(),
                       e.getCause() instanceof UnsupportedOperationException);
            String causeMsg = e.getCause().getMessage();
            assertTrue("Cause has unexpected message: " + causeMsg,
                       (causeMsg != null) &&
                       causeMsg.contains("number of shards"));
        }
    }

    /**
     * Async version of test that changing the number of shards during a shard
     * iteration produces the expected exception [#27506]
     */
    @Test
    public void testIterationWithShardFailureAsync()
        throws Exception {

        assumeTrue("Only when testing async", AsyncControl.serverUseAsync);

        TableImpl userTable = buildUserTable();
        addUserRows(userTable, NUSERS);

        Index index = userTable.getIndex("Age");
        IndexKey ikey = index.createIndexKey();
        ikey.put("age", 10);

        final Semaphore subscribed = new Semaphore(0);
        final AtomicReference<Subscription> subscription =
            new AtomicReference<>();
        final Semaphore done = new Semaphore(0);
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        tableImpl.tableIteratorAsync(ikey, null, smallBatchOptions)
            .subscribe(new Subscriber<Row>() {
                @Override
                public void onSubscribe(Subscription s) {
                    subscription.set(s);
                    subscribed.release();
                }
                @Override
                public void onNext(Row row) { }
                @Override
                public void onComplete() {
                    done.release();
                }
                @Override
                public void onError(Throwable t) {
                    exception.set(t);
                    done.release();
                }
            });
        assertTrue("Waiting for subscription",
                   subscribed.tryAcquire(30, SECONDS));

        /*
         * Insert a synthetic topology update with more shards, which should
         * result in a StoreIteratorException with cause
         * UnsupportedOperationException
         */
        RequestDispatcherImpl dispatcher =
            (RequestDispatcherImpl) ((KVStoreImpl) store).getDispatcher();
        Topology topo = dispatcher.getTopologyManager().getTopology();
        Topology newTopo = topo.getCopy();
        newTopo.add(new RepGroup());
        dispatcher.getTopologyManager().update(newTopo);

        subscription.get().request(100);
        assertTrue("Waiting for operation", done.tryAcquire(30, SECONDS));
        Throwable e = exception.get();
        assertTrue("Expected StoreIteratorException, found: " + e,
                   e instanceof StoreIteratorException);
        assertTrue("Expected UnsupportedOperationException cause, found: " +
                   e.getCause(),
                   e.getCause() instanceof UnsupportedOperationException);
        String causeMsg = e.getCause().getMessage();
        assertTrue("Cause has unexpected message: " + causeMsg,
                   (causeMsg != null) &&
                   causeMsg.contains("number of shards"));
    }

    /*
     * Test simple multiGet function using a single, sharded table with
     * 2 components in its primary key.
     */
    @Test
    public void testMultiGet()
        throws Exception {

        final int NUM_PER_USER = 200;
        TableImpl shardTable = buildShardedTable();
        addShardRows(shardTable, NUSERS, NUM_PER_USER);
        PrimaryKey pkey = shardTable.createPrimaryKey();
        pkey.put("id1", 1);
        List<Row> list = tableImpl.multiGet(pkey, null, null);

        /* Expect NUM_PER_USER records in this major key */
        assertEquals("Unexpected number of records", NUM_PER_USER,
                     list.size());

        /*
         * Use table iterator with complete shard key, and small batch size.
         * This tests the special-case code for iterating single partitions
         * as well as dealing with small batches, exercising resumption of
         * iteration.
         */
        TableIterator<Row> iter = tableImpl.tableIterator(pkey, null,
                                                          smallBatchOptions);
        int num = 0;
        while (iter.hasNext()) {
            assertTrue(iter.next().equals(list.get(num)));
            ++num;
        }
        assertTrue(iter.getPartitionMetrics().size() == 0);
        assertTrue(iter.getShardMetrics().size() == 0);
        iter.close();
        assertEquals("Unexpected number of records", NUM_PER_USER, num);

        /*
         * Start with the major key portion defined above and add a range
         * on the minor portion.
         */
        FieldRange range = shardTable.createFieldRange("id2")
            .setStart(1, true).setEnd(6, true);
        range.toString(); /* code coverage */
        assertTrue(range.check()); /* code coverage */
        List<PrimaryKey> keys =
            tableImpl.multiGetKeys(pkey,
                                   range.createMultiRowOptions(),
                                                       null);
        assertEquals("Unexpected number of records", 6, keys.size());
    }

    /**
     * Simple test of use of ReturnRow and putIfVersion()
     */
    @Test
    public void testReturnRow()
        throws Exception {
        TableImpl userTable = buildUserTable();
        TableImpl intTable = buildIntTable();
        PrimaryKey key = null;
        ReturnRow rr = null;

        addUserRows(userTable, NUSERS);
        addIntRows(intTable, NUSERS);

        rr = intTable.createReturnRow(ReturnRow.Choice.ALL);
        key = intTable.createPrimaryKey();
        key.put("id", 6);
        Row row = tableImpl.get(key, null);
        Version version = row.getVersion();
        tableImpl.put(row, rr, null);
        RowImpl newRow = new RowImpl((RowImpl)rr);
        assertTrue(newRow.equals(row));

        /*
         * This should fail because the version has changed
         */
        Version v1 = tableImpl.putIfVersion(row, version,
                                            rr, null);
        assertNull(v1);

        rr = intTable.createReturnRow(ReturnRow.Choice.ALL);
        boolean deleted = tableImpl.delete(key, rr, null);
        assertTrue(deleted);
        assertTrue(row.equals(new RowImpl((RowImpl)rr)));
    }

    /*
     * Test illegal states for iteration keys
     * o FieldRange set when key is complete
     * o out of order field specification
     * o mismatch of key and FieldRange (different indexes or tables)
     */
    @SuppressWarnings("unused")
    @Test
    public void testBadParameters()
        throws Exception {

        FieldRange range = null;
        MultiRowOptions mro = null;
        TableIterator<Row> iter;
        Index index = null;
        IndexKey ikey = null;
        PrimaryKey pkey = null;

        TableImpl userTable = buildUserTable();
        TableImpl intTable = buildIntTable();
        addUserRows(userTable, NUSERS);
        addIntRows(intTable, NUSERS);

        /*
         * out of order index key
         */
        index = intTable.getIndex("allint");
        ikey = index.createIndexKey();
        ikey.put("int1", 5);
        ikey.put("int3", 8);
        try {
            iter = tableImpl.tableIterator(ikey, null, null);
            fail("Out of order field fails");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Try to create a FieldRange on a field that isn't in the table/index
         */
        try {
            range = userTable.createFieldRange("age");
            fail("Bad table field for FieldRange");
        } catch (IllegalArgumentException iae) {
        }
        try {
            range = index.createFieldRange("firstName");
            fail("Bad index field for FieldRange");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * FieldRange set when primary key is complete
         */
        pkey = userTable.createPrimaryKey();
        pkey.put("lastName", "a");
        pkey.put("firstName", "a");
        range = userTable.createFieldRange("lastName").setStart("x", true);
        mro = range.createMultiRowOptions();
        try {
            iter = tableImpl.tableIterator(pkey, mro, null);
            fail("Can't use FieldRange with full key");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Try to use out of order PrimaryKey
         */
        pkey = userTable.createPrimaryKey();
        pkey.put("firstName", "a");
        try {
            iter = tableImpl.tableIterator(pkey, null, null);
            fail("Can't use out of order primary key");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Same as above but use a FieldRange instead of key only
         */
        pkey = userTable.createPrimaryKey();
        range = userTable.createFieldRange("firstName").setStart("x", true);
        mro = range.createMultiRowOptions();
        try {
            iter = tableImpl.tableIterator(pkey, mro, null);
            fail("Can't use out of order primary key");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * FieldRange set when index key is complete
         */
        index = userTable.getIndex("Age");
        ikey = index.createIndexKey();
        ikey.put("age", 10);
        range = index.createFieldRange("age").setStart(20, true);
        mro = range.createMultiRowOptions();
        try {
            iter = tableImpl.tableIterator(ikey, mro, null);
            fail("Can't use FieldRange with full index key");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Mismatched key and FieldRange.  IndexKey and PrimaryKey FieldRange
         */
        index = intTable.getIndex("allint");
        ikey = index.createIndexKey();
        ikey.put("int1", 1);
        range = intTable.createFieldRange("id").setStart(1, true);
        mro = range.createMultiRowOptions();
        try {
            iter = tableImpl.tableIterator(ikey, mro, null);
            fail("Can't use Primarykey FieldRange with Index");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Mismatched key and FieldRange.  PrimaryKey + index FieldRange
         */
        pkey = userTable.createPrimaryKey();
        pkey.put("lastName", "a");
        range = index.createFieldRange("int1").setStart(1, true);
        mro = range.createMultiRowOptions();
        try {
            iter = tableImpl.tableIterator(pkey, mro, null);
            fail("Can't use Index FieldRange with PrimaryKey");
        } catch (IllegalArgumentException iae) {
        }
    }

    /**
     * Test case-insensitive, case-preserving table names, index names,
     * and field names.
     */
    @Test
    public void testCaseSensitivity()
        throws Exception {

        TableImpl userTable = buildUserTable();

        /*
         * Check case-insensitive, preserving table names, index names.
         */
        TableImpl table = getTable("user");
        assertTrue(table != null);
        assertTrue(table.equals(userTable));
        assertEquals(table.getName(), userTable.getName());
        table = getTable("USER");
        assertTrue(table != null);
        assertTrue(table.equals(userTable));
        assertEquals(table.getName(), userTable.getName());

        Index index = table.getIndex("age");
        assertTrue(index != null);
        assertTrue(index.getName().equals("Age"));
        index = table.getIndex("AGE");
        assertTrue(index != null);
        assertTrue(index.getName().equals("Age"));

        /*
         * Try some case-insensitive field operations
         */
        FieldDef def = table.getField("firstname");
        assertTrue(def != null);
        assertTrue(def.equals(table.getField("FIRSTNAME")));
        Row row = table.createRow();
        /* this would throw for an invalid name */
        row.put("firstname", "joe");
        row.put("lastname", "jones");
        assertTrue(row.get("firstName") != null);
        assertTrue(row.get("firstname").equals(row.get("firstName")));
        tableImpl.put(row, null, null);

        PrimaryKey pkey = row.createPrimaryKey();
        pkey.put("firstName", "joe");
        pkey.put("LAstName", "jones");

        /*
         * test PrimaryKey.put(). This used to fail.
         */
        pkey = table.createPrimaryKey();
        pkey.put("firstname", "joe");

        /*
         * Case-insensitive index and table removal.
         */
        removeIndex(userTable, "FIRSTNAME", true);
        removeIndex(null, "USER", "age", true);
        removeTable(null, "UsEr", true);
    }

    /**
     * Test case-insensitive, case-preserving table names, index names,
     * and field names.
     */
    @Test
    public void testCaseSensitivity1()
        throws Exception {

        String ddl =
            "create table foo(id integer, s string, primary key(Id))";
        executeDdl(ddl);
        TableImpl table = getTable("foo");
        assertNotNull(table);

        Row row = table.createRow();
        /* this would throw for an invalid name */
        row.put("id", 1);
        row.put("s", "jones");
        tableImpl.put(row, null, null);

        PrimaryKey pkey = table.createPrimaryKey();
        pkey.put("Id", 1);
        row = tableImpl.get(pkey, null);
        String rowString = row.toJsonString(true);
        assertTrue(rowString.contains("id"));
        assertFalse(rowString.contains("Id"));
    }

    /**
     * Test missing primary key in child table.
     */
    @Test
    public void testMissingPrimaryKey()
        throws Exception {
        TableImpl parent = TableBuilder.createTableBuilder("Parent")
            .addInteger("id")
            .addString("lastName")
            .primaryKey("id")
            .buildTable();
        parent = addTable(parent);

        /* this should fail because primary key isn't specified */
        try {
            TableBuilder.createTableBuilder("Child",
                                            null,
                                            parent)
                .addInteger("addrId")
                .addString("street")
                .buildTable();
            fail("Build should have failed");
        } catch (Exception e) {
        }
    }

    /**
     * Use a table that has been removed.  Until the protocol upgrade to
     * include table name in get/put methods this worked.
     */
    @Test
    public void useRemovedTable()
        throws Exception {

        TableImpl userTable = buildUserTable();
        addUserRows(userTable, 10);

        /*
         * Save this Row for later.
         */
        PrimaryKey pkey = userTable.createPrimaryKey();
        pkey.put("firstName", ("first" + 0));
        pkey.put("lastName", ("last" + 0));
        Row savedRow = tableImpl.get(pkey, null);

        /*
         * Remove the table
         */
        removeTable(userTable, true);

        /*
         * Put some more records.  The client side still thinks that the
         * table is ok so it does the puts.
         */
        try {
            addUserRows(userTable, 10);
            fail("Attempt to use removed table should fail");
        } catch (MetadataNotFoundException mnfe) {
        }

        /*
         * Test other single key operations -- get, putIf*
         */
        Row row = userTable.createRow();
        row.put("firstName", ("first" + 0));
        row.put("lastName", ("last" + 0));
        row.put("age", 10);
        row.put("address", "10 Happy Lane");
        row.put("binary", new byte[0]);

        try {
            tableImpl.putIfAbsent(row, null, null);
            fail("Attempt to use removed table should fail");
        } catch (MetadataNotFoundException mnfe) {
        }

        try {
            tableImpl.putIfPresent(row, null, null);
            fail("Attempt to use removed table should fail");
        } catch (MetadataNotFoundException mnfe) {
        }

        try {
            tableImpl.putIfVersion(row, savedRow.getVersion(), null, null);
            fail("Attempt to use removed table should fail");
        } catch (MetadataNotFoundException mnfe) {
        }

        /*
         * Now try a get using the PrimaryKey from above.
         */
        try {
            tableImpl.get(pkey, null);
            fail("Attempt to use removed table should fail");
        } catch (MetadataNotFoundException mnfe) {
        }

        /*
         * Now try a delete
         */
        try {
            tableImpl.delete(pkey, null, null);
            fail("Attempt to use removed table should fail");
        } catch (MetadataNotFoundException mnfe) {
        }

        /*
         * deleteIfVersion
         */
        try {
            tableImpl.deleteIfVersion(pkey, savedRow.getVersion(), null, null);
            fail("Attempt to use removed table should fail");
        } catch (MetadataNotFoundException mnfe) {
        }

        /*
         * re-create the table using the same name, try to use it, and
         * and verify that this fails with MetadataNotFoundException
         * [#24361].
         */
        TableImpl newUserTable = buildUserTable();
        /* this uses the new metadata, so it works */
        addUserRows(newUserTable, 10);

        /* this uses old metadata, and will fail */
        try {
            addUserRows(userTable, 10);
            fail("Attempt to use old metadata should fail");
        } catch (MetadataNotFoundException mnfe) {
        }

        /* refresh MD and try again, which should succeed */
        userTable = getTable("User");
        addUserRows(userTable, 10);
    }

    @Test
    public void testNullTableNameIsNotFatal() {
        try {
            getTable(null);
            Assert.fail("Expected to raise exception with null table argument");
        } catch (IllegalArgumentException ex) {
            // expected error
        }

        try {
            Assert.assertNull(getTable("SomeArbitrayTableXYZ"));
        } catch (Exception ex) {
            fail("Expected to continue function after null table lookup");
        }
    }

    @Test
    public void testBooleanKey()
        throws Exception {

        executeDdl("create table foo(id integer, idb boolean, " +
                   " value boolean, primary key(id, idb))");
        TableImpl table = getTable("foo");
        table = addIndex(table, "boolean",
                         new String[] {"idb", "id"}, true);
        for (int i = 0; i < 5; i++) {
            Row row = table.createRow();

            row.put("id", i).put("idb", true).put("value", false);
            Version v = tableImpl.put(row, null, null);
            assertNotNull(v);
            assertNotNull(tableImpl.get(table.createPrimaryKey(row), null));

            /* same id, different idb */
            row.put("id", i).put("idb", false).put("value", false);
            v = tableImpl.put(row, null, null);
            assertNotNull(v);
            assertNotNull(tableImpl.get(table.createPrimaryKey(row), null));
        }

        /*
         * Test table iteration
         */
        TableIterator<Row> iter =
            tableImpl.tableIterator(table.createPrimaryKey(), null, null);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        assertEquals(count, 10);


        /*
         * Test index iteration
         */
        Index index = table.getIndex("boolean");
        iter = tableImpl.tableIterator(index.createIndexKey(), null, null);
        count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        assertEquals(count, 10);
    }

    @Test
    public void testReserializeRowValue()
        throws Exception {

        /* Table1: a table with a JSON field. */
        TableImpl tabJson = buildJsonTable();
        //TODO: allow MRTable mode after MRTable supports child tables.
        tabJson = addTable(tabJson, true, true/*noMRTableMode*/);
        tabJson = addIndex(tabJson, "idx1", new String[]{"s"}, true);

        /* Table2: a key-only table. */
        TableImpl tabKeyOnly = buildKeyonlyTable();
        tabKeyOnly = addTable(tabKeyOnly, true, true/*noMRTableMode*/);
        tabKeyOnly = addIndex(tabKeyOnly, "idx1", new String[]{"s"}, true);

        /* Table3: a child table of tabJson, it contains complex fields. */
        TableImpl tabComplex = buildComplexTable(tabJson);
        tabComplex = addTable(tabComplex, true, true/*noMRTableMode*/);
        tabComplex = addIndex(tabComplex, "idx1", new String[]{"s"}, true);

        /* Table4: a child table of tabComplex. */
        TableImpl tabChild = buildChildTable(tabComplex);
        tabChild = addTable(tabChild, true, true/*noMRTableMode*/);

        /* Refresh table and indexes */
        tabJson = getTable(tabJson.getFullName());
        Index idxTabJson = tabJson.getIndex("idx1");

        tabKeyOnly = getTable(tabKeyOnly.getFullName());
        Index idxTabKeyOnly = tabKeyOnly.getIndex("idx1");

        tabComplex = getTable(tabComplex.getFullName());
        Index idxTabComplex = tabComplex.getIndex("idx1");

        /* Generates rows for above 3 tables */
        List<Row> jsonRows = genJsonTableRows(tabJson, 10);
        List<Row> keyOnlyRows = genKeyOnlyTableRows(tabKeyOnly, 10);
        List<Row> complexRows = genComplexTableRows(tabComplex, 10);
        List<Row> childRows = genChildTableRows(tabChild, 10);

        final short current = SerialVersion.CURRENT;

        /*
         * Test on a table with JSON field.
         */

        Row rowWithLongStr = genJsonTableRow(tabJson, 0, 65535);

        /*
         * SerialVersion used:
         *  - Load rows: SerialVersion.CURRENT
         *  - Misc exercise: SerialVersion.CURRENT
         */
        runReserializeRowTest(current, current, tabJson, idxTabJson, jsonRows);
        runJsonWithLongStringTest(current, current, rowWithLongStr, true);

        /*
         * Test on a key-only table.
         */

        /*
         * SerialVersion used:
         *  - Load rows: SerialVersion.CURRENT
         *  - Misc exercise: SerialVersion.CURRENT
         */
        runReserializeRowTest(current, current, tabKeyOnly,
                              idxTabKeyOnly, keyOnlyRows);

        /*
         * Test on a table with parent and child
         */

        /*
         * SerialVersion used:
         *  - Load rows: SerialVersion.CURRENT
         *  - Misc exercise: SerialVersion.CURRENT
         */
        runReserializeRowTest(current, current, tabComplex,
                              idxTabComplex, complexRows,
                              tabJson, jsonRows, tabChild, childRows);
    }

    /*
     * Test the value format of table, test steps as below:
     *  - Create table with no JSON field, the value format used is
     *    Format.TABLE.
     *  - Evolve the table to add an new JSON field, the value format of the
     *    current version of table should be Format.TABLE_V1.
     *  - Evolve the table to remove the JSON field, then the value format of
     *    the current version of table should be Format.TABLE.
     *  - Evolve the table to add a record field that contains an nested JSON
     *    field, the value format used for the current version of table should
     *    be Format.TABLE_V1.
     */
    @Test
    public void testTableValueFormat()
        throws Exception {

        TableImpl table =
            TableBuilder.createTableBuilder("jsonTable")
                .addInteger("id")
                .addString("name")
                .primaryKey("id")
                .buildTable();
        /* Disable MRTable mode since it will cause the value formats
         * not match the expected formats.*/
        table = addTable(table, true, true/*noMRTableMode*/);

        Row row = (Row)table.createRow().put("id", 1).put("name", "v1");
        Value value = table.createValue(row);
        assertTrue(value.getFormat() == Format.TABLE);

        /* Add a JSON field, check the current value format used. */
        TableEvolver evovler = TableEvolver.createTableEvolver(table);
        evovler.addJson("json", null);
        evovler.evolveTable();

        table = evolveAndGet(evovler);
        row = (Row)table.createRow().put("id", 2).put("name", "v2")
                .putJson("json", "{\"s\":\""+ genString(1024 * 1024) +"\"}");
        value = table.createValue(row);
        assertTrue(value.getFormat() == Format.TABLE_V1);

        row = (Row)table.createRow().put("id", 3).put("name", "v2 null json");
        value = table.createValue(row);
        assertTrue(value.getFormat() == Format.TABLE_V1);

        /* Remove the JSON field, check the current value format used. */
        evovler = TableEvolver.createTableEvolver(table);
        evovler.removeField("json");
        evovler.evolveTable();
        table = evolveAndGet(evovler);

        row = (Row)table.createRow().put("id", 4).put("name", "v3");
        value = table.createValue(row);
        assertTrue(value.getFormat() == Format.TABLE);

        /* Add a nested JSON field, record(rid integer, a array(JSON)) */
        evovler = TableEvolver.createTableEvolver(table);
        FieldDef recField = TableBuilder.createRecordBuilder("r")
                .addInteger("i")
                .addField("a", TableBuilder.createArrayBuilder("a")
                                           .addJson().build())
                .build();
        evovler.addField("r", recField);
        evovler.evolveTable();
        table = evolveAndGet(evovler);

        row = (Row)table.createRow().put("id", 2).put("name", "v2");
        RecordValue rv = row.putRecord("r");
        rv.put("i", 1);
        ArrayValue av = rv.putArray("a");
        for (int i = 0; i < 3; i++) {
            String json = "{\"s\":\""+ genString((i + 1) * 102400) +"\"}";
            av.addJson(json);
        }
        value = table.createValue(row);
        assertTrue(value.getFormat() == Format.TABLE_V1);
    }

    private void runReserializeRowTest(short loadSerialVersion,
                                       short exerciseSerialVersion,
                                       TableImpl table,
                                       Index index,
                                       List<Row> rows) {
        runReserializeRowTest(loadSerialVersion, exerciseSerialVersion,
                              table, index, rows, null, null, null, null);
    }

    private void runReserializeRowTest(short loadSerialVersion,
                                       short exerciseSerialVersion,
                                       TableImpl table,
                                       Index index,
                                       List<Row> rows,
                                       TableImpl tabParent,
                                       List<Row> ancestorRows,
                                       TableImpl tabChild,
                                       List<Row> childRows) {

        /* Load rows to target tables */
        setTestSerialVersion(loadSerialVersion);
        loadToTable(rows);
        if (ancestorRows != null) {
            loadToTable(ancestorRows);
        }
        if (childRows != null) {
            loadToTable(childRows);
        }

        /* Exercise operations */
        setTestSerialVersion(exerciseSerialVersion);
        setCheckValueFormatHook(table, loadSerialVersion,
                                exerciseSerialVersion);
        if (tabParent != null) {
            setCheckValueFormatHook(tabParent, loadSerialVersion,
                                    exerciseSerialVersion);
        }
        if (tabChild != null) {
            setCheckValueFormatHook(tabChild, loadSerialVersion,
                                    exerciseSerialVersion);
        }
        runReserializeRowOps(table, index, rows, tabParent,
                             ancestorRows, tabChild, childRows);
    }

    /*
     * Test on json field with long string >= 64K
     */
    private void runJsonWithLongStringTest(short writeSerialVersion,
                                           short readSerialVersion,
                                           Row rowWithLongString,
                                           boolean expOKGetRowWithLongString) {

        setTestSerialVersion(writeSerialVersion);
        assertTrue(tableImpl.put(rowWithLongString, null, null) != null);

        setTestSerialVersion(readSerialVersion);
        setCheckValueFormatHook((TableImpl)rowWithLongString.getTable(),
                                writeSerialVersion, readSerialVersion);

        PrimaryKey key = rowWithLongString.createPrimaryKey();
        try {
            Row row = tableImpl.get(key, null);
            if (expOKGetRowWithLongString) {
                assertTrue(row != null && row.equals(row));
            }
        } catch (Exception ex) {
            if (expOKGetRowWithLongString) {
                fail("Caught unexpected exception when read a row with " +
                     "long string: " + ex.getMessage());
            }
        }
    }

    private void setTestSerialVersion(short opSerialVersion) {
        Request.setTestSerialVersion(opSerialVersion);
        TableImpl.setTestSerializationVersion(opSerialVersion);
    }

    private void setCheckValueFormatHook(TableImpl table,
                                         short origSerialVersion,
                                         short opSerialVesrion) {
        if (table.getValueRecordDef() != null) {
            table.setCheckDeserializeValueFormatHook
                (new CheckValueFormatHook(table,
                                          origSerialVersion,
                                          opSerialVesrion));
        } else {
            table.setCheckDeserializeValueFormatHook(new CheckFormatNoneHook());
        }
    }

    private void clearTableImplTestConfig() {
        if (TableImpl.getTestSerializationVersion() != 0) {
            TableImpl.setTestSerializationVersion((short)0);
        }
    }

    private void loadToTable(List<Row> rows) {
        for (Row row : rows) {
            Version v = tableImpl.put(row, null, null);
            assertTrue(v != null);
        }
    }

    /**
     * Run some operations which will return row or previous row, the
     * reserialization happens if the value format is newer than that for the
     * request serial version.
     */
    private void runReserializeRowOps(TableImpl table,
                                      Index index,
                                      List<Row> rows,
                                      TableImpl tabParent,
                                      List<Row> ancestorRows,
                                      TableImpl tabChild,
                                      List<Row> childRows) {

        boolean withParent = (tabParent != null && ancestorRows != null);
        boolean withChild = (tabChild != null && childRows != null);
        Row row;
        int cndAll = rows.size();
        int cntIncludeParent = rows.size();
        if (withParent) {
            assertNotNull(ancestorRows);
            cndAll += ancestorRows.size();
            cntIncludeParent += ancestorRows.size();
        }
        if (withChild) {
            assertNotNull(childRows);
            cndAll += childRows.size();
        }

        /* Get() */
        PrimaryKey key = rows.get(0).createPrimaryKey();
        Row rowOld = tableImpl.get(key, null);
        assertTrue(rowOld != null && rowOld.equals(rows.get(0)));

        /* TableIterator(PrimaryKey) */
        MultiRowOptions mro = null;
        if (withParent) {
            mro = new MultiRowOptions(null, Arrays.asList(tabParent), null);
        }
        if (withChild) {
            if (mro != null) {
                mro.setIncludedChildTables(Arrays.asList(tabChild));
            } else {
                mro = new MultiRowOptions(null, null, Arrays.asList(tabChild));
            }
        }
        TableIteratorOptions tio =
            new TableIteratorOptions(Direction.FORWARD, null, 0, null);
        TableIterator<Row> iter =
            tableImpl.tableIterator(table.createPrimaryKey(), mro, tio);
        int n = 0;
        while(iter.hasNext()) {
            row = iter.next();
            int id = row.get("id").asInteger().get();
            assertTrue(id >= 0 && id < rows.size());
            @SuppressWarnings("null")
            Row expRow = (withParent && row.getTable().equals(tabParent)) ?
                          ancestorRows.get(id) :
                          ((withChild && row.getTable().equals(tabChild)) ?
                           childRows.get(id) : rows.get(id));
            assertTrue(row.equals(expRow));
            n++;
        }
        assertTrue("The expected row# is " + cndAll + ", but got " + n,
                   n == cndAll);
        iter.close();

        /* TableIterator(List<PrimaryKey>) */
        List<PrimaryKey> keys = new ArrayList<PrimaryKey>(rows.size());
        for (Row r : rows) {
            keys.add(r.createPrimaryKey());
        }
        iter = tableImpl.tableIterator(keys.iterator(), mro, null);
        n = 0;
        while(iter.hasNext()) {
            row = iter.next();
            int id = row.get("id").asInteger().get();
            assertTrue(id >= 0 && id < rows.size());

            @SuppressWarnings("null")
            Row expRow = (withParent && row.getTable().equals(tabParent)) ?
                          ancestorRows.get(id) :
                          ((withChild && row.getTable().equals(tabChild)) ?
                          childRows.get(id) : rows.get(id));
            assertTrue(row.equals(expRow));
            n++;
        }
        assertTrue("The expected row# is " + cndAll + ", but got " + n,
                   n == cndAll);
        iter.close();

        /* multiGet() */
        int id = 5;
        key = rows.get(id).createPrimaryKey();
        List<Row> rows1 = tableImpl.multiGet(key, mro, null);
        int expCnt = 1;
        if (withParent) {
            expCnt++;
        }
        if (withChild) {
            expCnt++;
        }
        assertTrue(rows1.size() == expCnt);
        for (Row r : rows1) {
            @SuppressWarnings("null")
            Row expRow = (withParent && r.getTable().equals(tabParent)) ?
                          ancestorRows.get(id) :
                          ((withChild && r.getTable().equals(tabChild)) ?
                           childRows.get(id) : rows.get(id));
            assertTrue(r.equals(expRow));
        }

        /* TableIterator(IndexKey) */
        if (mro != null && mro.getIncludedChildTables() != null) {
            mro.setIncludedChildTables(null);
        }
        iter = tableImpl.tableIterator(index.createIndexKey(), mro, tio);
        n = 0;
        while(iter.hasNext()) {
            row = iter.next();
            id = row.get("id").asInteger().get();
            assertTrue(id >= 0 && id < rows.size());

            @SuppressWarnings("null")
            Row expRow = (withParent && row.getTable().equals(tabParent)) ?
                          ancestorRows.get(id) : rows.get(id);
            assertTrue(row.equals(expRow));
            n++;
        }
        assertTrue("The expected row# is " + cntIncludeParent +
                   ", but got " + n, n == cntIncludeParent);
        iter.close();

        row = rows.get(0);
        ReturnRow rr = table.createReturnRow(Choice.ALL);
        /* putIfAbsent() */
        Version v = tableImpl.putIfAbsent(row, rr, null);
        assertTrue(v == null);
        assertTrue(rr.compareTo(row) == 0);

        Version ifVersion =
            tableImpl.get(row.createPrimaryKey(), null).getVersion();
        assertTrue(ifVersion != null);

        /* putIfPresent() */
        rr.clear();
        v = tableImpl.putIfPresent(row, rr, null);
        assertTrue(v != null);
        assertTrue(rr.compareTo(row) == 0);

        /* putIfVersion() */
        rr.clear();
        row = rows.get(1);
        v = tableImpl.putIfVersion(row, ifVersion, rr, null);
        assertTrue(v == null);
        assertTrue(rr.compareTo(row) == 0);

        /* put() */
        rr.clear();
        v = tableImpl.put(row, rr, null);
        assertTrue(v != null);
        assertTrue(rr.compareTo(row) == 0);

        rr = table.createReturnRow(Choice.NONE);
        v = tableImpl.put(row, rr, null);
        assertTrue(v != null);
        assertTrue(rr.isEmpty());

        /* delete()*/
        rr = table.createReturnRow(Choice.VALUE);
        row = rows.get(2);
        boolean deleted = tableImpl.delete(row.createPrimaryKey(), rr, null);
        assertTrue(deleted);
        assertTrue(rr.compareTo(row) == 0);

        /* deleteIfVersion()*/
        rr.clear();
        row = rows.get(3);
        deleted = tableImpl.deleteIfVersion(row.createPrimaryKey(), ifVersion,
                                            rr, null);
        assertTrue(!deleted);
        assertTrue(rr.compareTo(row) == 0);
    }

    /*
     * Test TableImpl.findTargetTable(byte[] key) method with a key containing
     * EMPTY component(s).
     */
    @Test
    public void testFindTargetTable() {
        /* Primary key on a single string field */
        TableImpl table =
            TableBuilder.createTableBuilder("test")
                .addString("mk0")
                .addInteger("v")
                .primaryKey("mk0")
                .buildTable();
        PrimaryKeyImpl pKey = table.createPrimaryKey();
        pKey.put("mk0", "");
        assertEquals(table, table.findTargetTable(pKey.createKeyBytes()));

        /* Primary key on 2 string fields */
        table = TableBuilder.createTableBuilder("test")
                .addString("mk0")
                .addString("mk1")
                .addInteger("v")
                .primaryKey("mk0", "mk1")
                .buildTable();
        pKey = table.createPrimaryKey();
        pKey.put("mk0", "").put("mk1", "");
        assertEquals(table, table.findTargetTable(pKey.createKeyBytes()));

        pKey.put("mk0", "").put("mk1", "m1");
        assertEquals(table, table.findTargetTable(pKey.createKeyBytes()));

        /* Primary key on 4 string fields, 2 of them are shard key */
        table = TableBuilder.createTableBuilder("test")
            .addString("mk0")
            .addString("mk1")
            .addString("nk0")
            .addString("nk1")
            .addInteger("v")
            .primaryKey("mk0", "mk1", "nk0", "nk1")
            .shardKey("mk0", "mk1")
            .buildTable();
        pKey = table.createPrimaryKey();
        pKey.put("mk0", "").put("mk1", "").put("nk0", "").put("nk1", "");
        assertEquals(table, table.findTargetTable(pKey.createKeyBytes()));

        pKey.put("mk0", "").put("mk1", "m1").put("nk0", "n0").put("nk1", "n1");
        assertEquals(table, table.findTargetTable(pKey.createKeyBytes()));

        pKey.put("mk0", "").put("mk1", "m1").put("nk0", "").put("nk1", "n1");
        assertEquals(table, table.findTargetTable(pKey.createKeyBytes()));
    }

    /**
     * Test tableIterator()/tableKeysIterator() with resumePrimaryKey specified
     * in TableIteratorOptions.
     */
    @Test
    public void testResumeIteration()
        throws Exception {

        final int numSk1 = 4;
        final int numSk2PerSk1 = 5;
        final int numPkPerSk2 = 10;
        final int numRows = numSk1 * numSk2PerSk1 * numPkPerSk2;

        String ddl = "create table resumeTest(" +
                     "  sk1 integer, " +
                     "  sk2 integer, " +
                     "  pk1 integer, " +
                     "  name string, " +
                     "  primary key(shard(sk1, sk2), pk1))";
        executeDdl(ddl);

        TableImpl table = getTable("resumeTest");
        Row row;
        for (int s1 = 0; s1 < numSk1; s1++) {
            row = table.createRow();
            row.put("sk1", s1);
            for (int s2 = 0; s2 < numSk2PerSk1; s2++) {
                row.put("sk2", s2);
                for (int p1 = 0; p1 < numPkPerSk2; p1++) {
                    row.put("pk1", p1);
                    row.put("name", "n_" + s1 + "_" + s2 + "_" + p1);
                    assertNotNull(tableImpl.put(row, null, null));
                }
            }
        }

        PrimaryKey key = table.createPrimaryKey();
        PrimaryKey resumeKey = table.createPrimaryKey();

        /*
         * primaryKey = {}
         * resumeKey = {"sk1":3,"sk2":3,"pk1":0}
         */
        resumeKey.put("sk1", 3)
                 .put("sk2", 3)
                 .put("pk1", 0);
        runResumeIteration(key, Direction.FORWARD, resumeKey, 19);
        runResumeIteration(key, Direction.REVERSE, resumeKey, numRows - 20);

        /*
         * primaryKey = null
         * resumeKey = {"sk1":3,"sk2":3,"pk1":0}
         */
        runResumeIteration(null, Direction.FORWARD, resumeKey, 19);
        runResumeIteration(null, Direction.REVERSE, resumeKey, numRows - 20);

        /*
         * primaryKey = {"sk1": 2}
         * resumeKey = {"sk1":2,"sk2":3}
         */
        resumeKey = table.createPrimaryKey();
        resumeKey.put("sk1", 2)
                 .put("sk2", 3);
        runResumeIteration(key, Direction.FORWARD, resumeKey, 70);
        runResumeIteration(key, Direction.REVERSE, resumeKey, numRows - 70);

        /*
         * primaryKey = {"sk1": 2}
         * resumeKey = {"sk1":2,"sk2":2}
         */
        key.put("sk1", 2);
        resumeKey = table.createPrimaryKey();
        resumeKey.put("sk1", 2)
                 .put("sk2", 2);
        runResumeIteration(key, Direction.FORWARD, resumeKey, 30);
        runResumeIteration(key, Direction.REVERSE, resumeKey,
                          (numSk2PerSk1 * numPkPerSk2 - 30));

        /*
         * primaryKey = {"sk1":0,"sk2":4}
         * resumeKey = {"sk1":0,"sk2":4,"pk1":3}
         */
        key.put("sk1", 0)
           .put("sk2", 4);
        resumeKey = table.createPrimaryKey();
        resumeKey.put("sk1", 0)
                 .put("sk2", 4)
                 .put("pk1", 3);
        runResumeIteration(key, Direction.FORWARD, resumeKey, 6);
        runResumeIteration(key, Direction.REVERSE, resumeKey,
                           (numPkPerSk2 - 7));

        /*
         * Invalid cases
         */
        TableIteratorOptions tio;

        /* The resumePrimaryKey can not be used with Direction.UNORDERED */
        resumeKey = table.createPrimaryKey();
        resumeKey.put("sk1", 1)
                 .put("sk2", 1)
                 .put("pk1", 3);
        tio = new TableIteratorOptions(Direction.UNORDERED, null, 0, null);
        try {
            tio.setResumePrimaryKey(resumeKey);
            fail("expect to catch IAE but not");
        } catch (IllegalArgumentException expected) {
            /* expected */
        }

        /* The resumePrimaryKey is incomplete primary key: {"sk1":1} */
        tio = new TableIteratorOptions(Direction.FORWARD, null, 0, null);
        resumeKey = table.createPrimaryKey();
        resumeKey.put("sk1", 1);
        try {
            tio.setResumePrimaryKey(resumeKey);
            fail("expect to catch IAE but not");
        } catch (IllegalArgumentException expected) {
            /* expected */
        }

        resumeKey = table.createPrimaryKey();
        resumeKey.put("sk1", 1);
        resumeKey.putNull("sk2");
        try {
            tio.setResumePrimaryKey(resumeKey);
            fail("expect to catch IAE but not");
        } catch (IllegalArgumentException expected) {
            /* expected */
        }

        /* The resume primary key is not for target table 'resumeTest' */
        ddl = "create table resumeTest2(id integer, s string, primary key(id))";
        executeDdl(ddl);

        TableImpl table2 = getTable("resumeTest2");
        resumeKey = table2.createPrimaryKey();
        resumeKey.put("id", 1);
        tio.setResumePrimaryKey(resumeKey);
        try {
            tableImpl.tableIterator(table.createPrimaryKey(), null, tio);
            fail("expect to catch IAE but not");
        } catch (IllegalArgumentException expected) {
            /* expected */
        }
        try {
            tableImpl.tableKeysIterator(table.createPrimaryKey(), null, tio);
            fail("expect to catch IAE but not");
        } catch (IllegalArgumentException expected) {
            /* expected */
        }

        /*
         * The primary key must not be null if resume primary key is not
         * specified
         */
        key = null;
        try {
            tableImpl.tableIterator(key, null, null);
            fail("expect to catch IAE but not");
        } catch (IllegalArgumentException expected) {
            /* expected */
        }
        try {
            tableImpl.tableKeysIterator(key, null, null);
            fail("expect to catch IAE but not");
        } catch (IllegalArgumentException expected) {
            /* expected */
        }
    }

    private void runResumeIteration(PrimaryKey key,
                                    Direction direction,
                                    PrimaryKey resumeKey,
                                    int expCnt) {
        runIteratorWithResumeKey(key, direction, resumeKey, false, expCnt);
        runIteratorWithResumeKey(key, direction, resumeKey, true, expCnt);
    }

    private void runIteratorWithResumeKey(PrimaryKey key,
                                          Direction direction,
                                          PrimaryKey resumeKey,
                                          boolean keyOnly,
                                          int expCount) {
        TableIteratorOptions tio;
        TableIterator<?> iter;
        PrimaryKeyImpl rowKey;
        int count = 0;

        tio = new TableIteratorOptions(direction, null, 0, null);
        tio.setResumePrimaryKey(resumeKey);
        if (keyOnly) {
            iter = tableImpl.tableKeysIterator(key, null, tio);
        } else {
            iter = tableImpl.tableIterator(key, null, tio);
        }

        while(iter.hasNext()) {
            if (keyOnly) {
                rowKey = (PrimaryKeyImpl)iter.next();
            } else {
                RowImpl row = (RowImpl)iter.next();
                rowKey = (PrimaryKeyImpl)row.createPrimaryKey();
            }
            if (((PrimaryKeyImpl)resumeKey).isComplete()) {
                if (direction == Direction.FORWARD) {
                    assertTrue(rowKey.compareTo(resumeKey) > 0);
                } else {
                    assertTrue(rowKey.compareTo(resumeKey) < 0);
                }
            } else {
                if (!rowKeyContainsResumeKey(rowKey, resumeKey)) {
                    if (direction == Direction.FORWARD) {
                        assertTrue(rowKey.compareTo(resumeKey) > 0);
                    } else {
                        assertTrue(rowKey.compareTo(resumeKey) < 0);
                    }
                }
            }
            count++;
        }

        assertEquals(expCount, count);
        iter.close();
    }

    private boolean rowKeyContainsResumeKey(PrimaryKey pkey,
                                            PrimaryKey resumeKey) {
        FieldValue val;
        for (String name : resumeKey.getFieldNames()) {
            val = resumeKey.get(name);
            if (val == null) {
                break;
            }
            if (pkey.get(name).compareTo(val) != 0) {
                return false;
            }
        }
        return true;
    }

    private TableImpl buildJsonTable() {
        TableImpl table =
            TableBuilder.createTableBuilder("jsonTable")
                .addInteger("id")
                .addLong("l")
                .addString("s")
                .addBoolean("b")
                .addBinary("bi")
                .addJson("json", null)
                .primaryKey("id")
                .buildTable();
        return table;
    }

    private List<Row> genJsonTableRows(TableImpl table, int num) {
        List<Row> rows = new ArrayList<Row>(num);
        for (int i = 0; i < num; i++) {
            Row row = genJsonTableRow(table, i, (i + 1) * 10);
            rows.add(row);
        }
        return rows;
    }

    private Row genJsonTableRow(TableImpl table, int id, int jslen) {
        Row row = table.createRow();
        row.put("id", id);
        row.put("l", (long)id);
        row.put("s", "row #" + id);
        row.put("b", (id % 2 == 0));
        row.put("bi", genBytes(id + 1));
        row.putJson("json", genJson(id, jslen));
        return row;
    }

    private byte[] genBytes(int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte)(i % 256);
        }
        return bytes;
    }

    private String genJson(int id, int sLen) {
        String fmt = "{\"i\":%d, \"s\": \"%s\"}";
        return String.format(fmt, id, genString(sLen));
    }

    private String genString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append((char)('A' + (i % 26)));
        }
        String ret = sb.toString();
        return ret;
    }

    private TableImpl buildKeyonlyTable() {
        TableImpl table =
            TableBuilder.createTableBuilder("keyOnlyTable")
                .addInteger("id")
                .addString("s")
                .primaryKey("id", "s")
                .buildTable();
        return table;
    }

    private List<Row> genKeyOnlyTableRows(TableImpl table, int num) {
        List<Row> rows = new ArrayList<Row>(num);
        for (int i = 0; i < num; i++) {
            Row row = genKeyOnlyTableRow(table, i);
            rows.add(row);
        }
        return rows;
    }

    private Row genKeyOnlyTableRow(TableImpl table, int id) {
        Row row = table.createRow();
        row.put("id", id);
        row.put("s", "row #" + id);
        return row;
    }

    private TableImpl buildComplexTable(TableImpl parent) {
        TableImpl table =
            TableBuilder.createTableBuilder("complexTable", null, parent)
                .addString("cid")
                .addString("s")
                .addField("a", TableBuilder.createArrayBuilder("a")
                                  .addString().build())
                .addField("m", TableBuilder.createMapBuilder("m")
                                  .addString().build())
                .addField("r", TableBuilder.createRecordBuilder("r")
                                   .addInteger("ri").addString("rs").build())
                .primaryKey("cid")
                .buildTable();
        return table;
    }

    private List<Row> genComplexTableRows(TableImpl table, int num) {
        List<Row> rows = new ArrayList<Row>(num);
        for (int i = 0; i < num; i++) {
            Row row = genComplexTableRow(table, i);
            rows.add(row);
        }
        return rows;
    }

    private Row genComplexTableRow(TableImpl table, int id) {
        Row row = table.createRow();
        row.put("id", id);
        row.put("cid", "cid_" + id);
        row.put("s", "row #" + id);
        ArrayValue av = row.putArray("a");
        MapValue mv = row.putMap("m");
        for (int i = 0; i < 3; i++) {
            av.add("value_" + ((id * 3) + i));
            mv.put("k"+ i, "value_" + ((id * 3) + i));
        }
        RecordValue rv = row.putRecord("r");
        rv.put("ri", id);
        rv.put("rs", "rs_" + id);
        return row;
    }

    private TableImpl buildChildTable(TableImpl parent) {
        TableImpl table =
            TableBuilder.createTableBuilder("childTable", null, parent)
                .addString("ccid")
                .addString("s")
                .addJson("json", null)
                .primaryKey("ccid")
                .buildTable();
        return table;
    }

    private List<Row> genChildTableRows(TableImpl table, int num) {
        List<Row> rows = new ArrayList<Row>(num);
        for (int i = 0; i < num; i++) {
            Row row = genChildTableRow(table, i);
            rows.add(row);
        }
        return rows;
    }

    private Row genChildTableRow(TableImpl table, int id) {
        Row row = table.createRow();
        row.put("id", id);
        row.put("cid", "cid_" + id);
        row.put("ccid", "ccid" + id);
        row.put("s", "row #" + id);
        row.putJson("json", genJson(id, id + 100));
        return row;
    }

    private TableImpl buildUserTable()
        throws Exception {

        TableImpl table = TableBuilder.createTableBuilder("User")
            .addInteger("age")
            .addString("firstName")
            .addString("lastName")
            .addString("address")
            .addBinary("binary", null)
            // purposely change case of field from decl above
            .primaryKey("LastName", "FirstName")
            .shardKey("lastName")
            .buildTable();

        table = addTable(table);
        table = addIndex(table, "FirstName",
                         new String[] {"firstName"}, true);
        table = addIndex(table, "Age",
                         new String[] {"age"}, true);
        return table;
    }

    private void addUserRows(TableImpl table, int num) {
        for (int i = 0; i < num; i++) {
            byte[] bytes = new byte[0];
            Row row = table.createRow();
            row.put("firstName", ("first" + i));
            row.put("lastName", ("last" + i));
            row.put("age", i+10);
            row.put("address", "10 Happy Lane");
            row.put("binary", bytes);
            tableImpl.put(row, null, null);
        }
    }

    private TableImpl buildIntTable()
        throws Exception {

        TableImpl table = TableBuilder.createTableBuilder("Int")
            .addInteger("int1")
            .addInteger("id")
            .addInteger("int2")
            .addInteger("int3")
            .primaryKey("id")
            .buildTable();

        table = addTable(table);
        table = addIndex(table, "int1",
                             new String[] {"int1"}, true);
        table = addIndex(table, "allint",
                         new String[] {"int1", "int2", "int3"}, true);

        roundTripTable(table);
        return table;
    }

    private void addIntRows(TableImpl table, int num) {
        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("int1", i);
            row.put("int2", i*10);
            row.put("int3", i*100);
            tableImpl.put(row, null, null);
        }
    }

    private TableImpl buildShardedTable()
        throws Exception {

        /*
         * Declare fields out of order to exercise fix to #26769
         */
        TableImpl table = TableBuilder.createTableBuilder("Sharded")
            .addInteger("int1")
            .addInteger("int2")
            .addInteger("int3")
            .addInteger("id1")
            .addInteger("id2")
            .primaryKey("id1", "id2")
            .shardKey("id1")
            .buildTable();

        return addTable(table);
    }

    /*
     * Add rows that won't sort data in the same order as the key fields
     */
    private void addShardRows(TableImpl table, int num, int numPerMajor) {
        for (int i = 0; i < num; i++) {
            for (int j = 0; j < numPerMajor; j++) {
                Row row = table.createRow();
                row.put("id1", i);
                row.put("id2", j);
                row.put("int1", 1000 - i);
                row.put("int2", 1000 - i*10);
                row.put("int3", 10000 - i*100);
                tableImpl.put(row, null, null);
            }
        }
    }

    @Test
    public void testRowImpl() throws Exception {
        TableImpl userTable = buildUserTable();
        RowImpl rowUser = userTable.createRow();
        TableImpl userOthertable = TableBuilder.createTableBuilder("UserOther")
                .addInteger("age").addString("firstName").addString("lastName")
                .addString("address").addBinary("binary", null)
                .primaryKey("lastName", "firstName").shardKey("lastName")
                .buildTable();
        addUserRows(userOthertable, NUSERS);

        try {
            rowUser.getStorageSize();
            fail("Uninitilized Storage throw exception");
        } catch (IllegalStateException e) {
            /* success */
        }
        try {
            rowUser.getShard();
            fail("Uninitilized Shard throw exception");
        } catch (IllegalStateException e) {
            /* success */
        }
        try {
            rowUser.getPartition();
            fail("Uninitilized Partition throw exception");
        } catch (IllegalStateException e) {
            /* success */
        }

        try {
            @SuppressWarnings("unused")
            RowImpl rowRecDefNull = new RowImpl(null, userTable);
            fail("Not specifying Recorddef throw exception");
        } catch (NullPointerException e) {
            /* success */
        }
        try {
            @SuppressWarnings("unused")
            RowImpl rowRecDefTableNull = new RowImpl(null, null);
            fail("Not specifying TableImpl throw exception");
        } catch (NullPointerException e) {
            /* success */
        }

        for (Entry<String, Index> e : userTable.getIndexes().entrySet()) {
            Index indexUser = e.getValue();
            Index indexResult = userTable
                    .getSecondaryIndex(indexUser.getName());
            assertEquals(indexUser.getName(), indexResult.getName());
        }
        assertEquals(null, userTable.getSecondaryIndex("lastName"));
        assertEquals(null, userTable.getTextIndex("lastName"));

        try {
            userTable.getVersion(-1);
            fail("Table version does not exist throw exception");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        TableImpl intTable = buildIntTable();
        assertFalse(userTable.equals(intTable));

        /* must have at least one non-null parameter */
        try {
            @SuppressWarnings("unused")
            MultiRowOptions mro = userTable.createMultiRowOptions(null, null);
            fail("Not specifying TableName or field range throw exception");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* No such table */
        try {
            @SuppressWarnings("unused")
            MultiRowOptions mro = userTable
                    .createMultiRowOptions(Arrays.asList(""), null);
            fail("Not specifying TableName or field range throw exception");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /* Target table must not appear in included tables list */
        try {
            @SuppressWarnings("unused")
            MultiRowOptions mro = userTable
                    .createMultiRowOptions(Arrays.asList("User"), null);
            fail("Not specifying TableName or field range throw exception");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        Index index = userTable.getIndex("FirstName");
        IndexKey ikey = index.createIndexKey();
        assertFalse(ikey.equals(rowUser));
        try {
            ikey.putJsonNull(0);
            fail("Field FirstName is not json throw exception");
        } catch (IllegalArgumentException iae) {
            /* success */
        }
        StringBuilder sb = new StringBuilder();
        try {
            ((IndexKeyImpl) ikey).toStringBuilder(sb, null);
            fail("DisplayFormatter null throw excpeiton");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        IndexRange indexRange = new IndexRange((IndexKeyImpl) ikey, null,
                Direction.FORWARD);
        IndexRange indexRangeOther = indexRange;
        assertTrue(indexRange.equals(indexRangeOther));
        IntegerValueImpl ivi = new IntegerValueImpl(10);
        assertFalse(indexRange.equals(ivi));
    }

    /*
     * This test is related to verify iteration of AsyncTableIterator. classes:
     * class TableScan.BasicMultiGetIteratorWrapper<E> and
     * TableScan.MultiGetIteratorWrapper<E>.
     */
    @Test
    public void tesMultiGetIterationAsync() throws Exception {
        final int NUM_PER_USER = 10;
        PrimaryKey key = null;
        TableIteratorOptions forwardOptions = new TableIteratorOptions(
                Direction.FORWARD, null, 0, null);

        TableImpl shardTable = buildShardedTable();
        addShardRows(shardTable, NUSERS, NUM_PER_USER);

        key = shardTable.createPrimaryKey();

        key.put("id1", 1); /* set a major key to do forward iteration */

        final Semaphore subscribed = new Semaphore(0);
        final Semaphore done = new Semaphore(0);
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        tableImpl.tableIteratorAsync(key, null, forwardOptions)
                .subscribe(new Subscriber<Row>() {
                    Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        subscription = s;
                        subscription.request(1);
                        subscribed.release();
                    }

                    @Override
                    public void onNext(Row row) {
                        subscription.request(1);
                    }

                    @Override
                    public void onComplete() {
                        done.release();
                    }

                    @Override
                    public void onError(Throwable t) {
                        exception.set(t);
                        done.release();
                    }
                });
        assertTrue("Waiting for subscription",
                subscribed.tryAcquire(30, SECONDS));
    }

    /*
     * A class to check Value format.
     */
    private static class CheckValueFormatHook implements TestHook<Format> {

        private final short serialVersion;
        private final TableImpl table;

        CheckValueFormatHook(TableImpl table,
                             short loadSerilaVersion,
                             short opSerialVersion) {
            this.table = table;
            serialVersion = (short)Math.min(loadSerilaVersion, opSerialVersion);
        }

        @Override
        public void doHook(Format format) {
            Format expFormat = table.getValueFormat(serialVersion);
            assertTrue("The value format should be " + expFormat +
                       ", but " + format, format == expFormat);
        }
    }

    /*
     * A class to check Value format.
     */
    private static class CheckFormatNoneHook implements TestHook<Format> {

        @Override
        public void doHook(Format format) {
            Format expFormat = Format.NONE;
            assertTrue("The value format should be " + expFormat +
                       ", but " + format, format == expFormat);
        }
    }
}
