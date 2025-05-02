/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableLimits.NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.KeySizeLimitException;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.client.DdlJsonFormat;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.EnumDef;
import oracle.kv.table.FieldRange;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.table.WriteOptions;

import org.junit.Test;

public class IndexTest extends TableTestBase {

    final TableIteratorOptions forwardOpts;
    final TableIteratorOptions reverseOpts;
    final TableIteratorOptions unorderedOpts;
    final TableIteratorOptions smallBatchOpts;
    final TableIteratorOptions smallBatchReverseOpts;

    public IndexTest() {
        forwardOpts = new TableIteratorOptions(Direction.FORWARD,
                                               Consistency.ABSOLUTE,
                                               0, null, /* timeout, unit */
                                               0, 0);

        reverseOpts = new TableIteratorOptions(Direction.REVERSE,
                                               Consistency.ABSOLUTE,
                                               0, null, /* timeout, unit */
                                               0, 0);
        unorderedOpts = new TableIteratorOptions(Direction.UNORDERED,
                                                 Consistency.ABSOLUTE,
                                                 0, null, /* timeout, unit */
                                                 0, 0);
        smallBatchOpts = new TableIteratorOptions(Direction.FORWARD,
                                                  Consistency.ABSOLUTE,
                                                  0, null, /* timeout, unit */
                                                  0, 5); /* batch size 5 */
        smallBatchReverseOpts = new TableIteratorOptions(Direction.REVERSE,
                                                         Consistency.ABSOLUTE,
                                                         0, null,
                                                         0, 5);
    }

    @Test
    public void testOptions() {

        /* WriteOptions */
        @SuppressWarnings("unused")
		WriteOptions wOpts;

        /* Bad timeout */
        try {
            wOpts = new WriteOptions(null, -1, null);
            throw new AssertionError("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
        /* Missing unit */
        try {
            wOpts = new WriteOptions(null, 1, null);
            throw new AssertionError("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }

        /* ReadOptions */
        @SuppressWarnings("unused")
        ReadOptions rOpts;

        /* Bad timeout */
        try {
            rOpts = new ReadOptions(null, -1, null);
            throw new AssertionError("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
        /* Missing unit */
        try {
            rOpts = new ReadOptions(null, 1, null);
            throw new AssertionError("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }

        /* ReadOptions */
        @SuppressWarnings("unused")
        TableIteratorOptions tiOpts;

        /* Missing direction */
        try {
            tiOpts = new TableIteratorOptions(null, null, 0, null);
            throw new AssertionError("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
        /* Bad timeout */
        try {
            tiOpts = new TableIteratorOptions(Direction.FORWARD, null, -1,null);
            throw new AssertionError("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
        /* Missing unit */
        try {
            tiOpts = new TableIteratorOptions(Direction.FORWARD, null, 1, null);
            throw new AssertionError("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
        /* Bad iterator parameters */
        try {
            tiOpts = new TableIteratorOptions(Direction.FORWARD, null, 0, null,
                                              -1, 0);
            throw new AssertionError("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
        try {
            tiOpts = new TableIteratorOptions(Direction.FORWARD, null, 0, null,
                                              0, -1);
            throw new AssertionError("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
    }

    /**
     * Tests some basic index functions.
     */
    @Test
    public void simpleIndex() throws Exception {
        final int startId = 90;
        TableImpl userTable = buildUserTable();

        addUserRow(tableImpl, userTable, 76, "Joe", "Jones", 38);
        addUserRow(tableImpl, userTable, 87, "Jane", "Doe", 44);
        addUserRow(tableImpl, userTable, 56, "Sam", "Spade", 32);
        addUserRow(tableImpl, userTable, 57, "Amy", "Smith", 11);
        addUserRow(tableImpl, userTable, 55, "Amy", "Jones", 12);

        /*
         * Start at id 90 to avoid collision with above records
         */
        addUsers(tableImpl, userTable, startId, 25);

        assertEquals("Unexpected count", 30, countIndexRows
                     (userTable.getIndex("AgeId").createIndexKey(),
                      userTable));
        assertEquals("Unexpected count", 30, countIndexRecords
                     (userTable.getIndex("AgeId").createIndexKey(),
                      userTable));

        Index index = userTable.getIndex("FirstName");
        IndexKey key = index.createIndexKey();
        key.put("firstName", "Joe");
        /*
         * An index key without range means "equal" and is not a
         * range scan.
         */
        TableIterator<Row> iter = tableImpl.tableIterator(key, null,
                                                          unorderedOpts);
        int i = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            assertTrue(row.get("firstName").asString().get().
                       compareTo("Joe") == 0);
            ++i;
        }
        assertEquals("There should have been one match", 1, i);
        int numExpected = i;
        iter.close();

        /*
         * Test tableKeysIterator for basic iteration.
         */
        i = 0;
        KeyPair lastPair = null;
        TableIterator<KeyPair> kpIter =
            tableImpl.tableKeysIterator(key, null, forwardOpts);
        while (kpIter.hasNext()) {
            KeyPair currentPair = kpIter.next();
            ++i;
            if (lastPair != null) {
                assertTrue(lastPair.compareTo(currentPair) <= 0);
            }
            lastPair = currentPair;
        }
        assertEquals("Wrong number of records", numExpected, i);
        kpIter.close();

        /*
         * Iterate using a range.  Use an empty key.
         */
        key = index.createIndexKey();
        FieldRange range = index.createFieldRange("firstName")
            .setStart("Jane", true).setEnd("K", true);
        MultiRowOptions mro = new MultiRowOptions(range);
        iter = tableImpl.tableIterator(key, mro, forwardOpts);
        i = 0;
        IndexKeyImpl last = null;
        while (iter.hasNext()) {
            @SuppressWarnings("deprecation")
            IndexKeyImpl current =
                (IndexKeyImpl) index.createIndexKey(iter.next());
            if (last != null) {
                assertTrue(last.compareTo(current) <= 0);
            }
            last = current;
            ++i;
        }
        assertEquals("There should have been two matching records", 2, i);

        /*
         * Do the same iteration but use startExclusive, eliminating "Jane"
         */
        range = index.createFieldRange("firstName")
            .setStart("Jane", false).setEnd("K", true);

        mro.setFieldRange(range);
        iter = tableImpl.tableIterator(key, mro, unorderedOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        /* there should be 1 -- Joe */
        assertEquals("There should have been one matching record", 1, i);

        /*
         * Test range on the integer index
         */
        int numInRange = 5;
        index = userTable.getIndex("Age");
        key = index.createIndexKey();
        range = index.createFieldRange("age")
            .setStart(startId, true).setEnd(startId + numInRange, true);
        mro = new MultiRowOptions(range);
        iter = tableImpl.tableIterator(key, mro, unorderedOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        assertEquals("Wrong number in range", numInRange + 1, i);

        /*
         * Test a "don't iterate" query.  There are two "Amy" entries
         * in the table.
         */
        index = userTable.getIndex("FirstName");
        key = (IndexKey) index.createIndexKey().put("firstName", "Amy");
        iter = tableImpl.tableIterator(key, null, unorderedOpts);
        i = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            assertTrue(row.get("firstName").asString().get().
                       compareTo("Amy") == 0);
            ++i;
        }
        assertEquals("There should have been 2 records", i, 2);
    }

    /**
     * Test use of null value in indexed fields
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testNull() throws Exception {
        TableImpl userTable = buildUserTable();
        final int num = 25;

        /*
         * Add some users and validate the counts.
         */
        addUsers(tableImpl, userTable, 10, num);

        Index ageIndex = userTable.getIndex("Age");
        Index lastAgeIndex = userTable.getIndex("LastAge");
        assertEquals(num, countIndexRecords(ageIndex.createIndexKey(),
                                            userTable));
        assertEquals(num, countIndexRecords(lastAgeIndex.createIndexKey(),
                                            userTable));

        /*
         * Add a User table row with the age field defaulted.  It's indexed, which
         * means that it'll be null on the server side, so it should be skipped in
         * indexing and index iteration.  It affects both indexes counted above.
         */

        Row row = userTable.createRow();
        row.put("id", 10000);
        row.put("firstName", "jane");
        row.put("lastName", "doe");
        tableImpl.put(row, null, null);

        /*
         * The indexes should not have increased in size
         */
        int expNum = (((IndexImpl)ageIndex).supportsSpecialValues() ?
                      num + 1 : num);
        assertEquals(expNum, countIndexRecords(ageIndex.createIndexKey(),
                                               userTable));
        assertEquals(expNum, countIndexRecords(lastAgeIndex.createIndexKey(),
                                               userTable));

        /*
         * Delete the row to check that path.
         */
        PrimaryKeyImpl pkey = userTable.createPrimaryKey();
        pkey.put("id", 10000);
        tableImpl.delete(pkey, null, null);

        /*
         * Update the row to include age and make sure it's in the indexes.
         */
        row.put("age", 10);
        tableImpl.put(row, null, null);

        assertEquals(num + 1, countIndexRecords(ageIndex.createIndexKey(),
                                                userTable));
        assertEquals(num + 1, countIndexRecords(lastAgeIndex.createIndexKey(),
                                                userTable));

        /*
         * Test what happens if a null is used in the index key.
         * NOTE: before IndexKey was changed to disallow nulls there was a
         * server side issue with IndexRange that caused an assertion error.
         * This has been fixed.  Given changes in IndexKeyImpl it is not
         * possible for a well-behaved client to generate a bad IndexRange but
         * a direct protocol user could.
         */
        IndexKey ageKey = ageIndex.createIndexKey();
        try {
            ageKey.putNull("age");
            if (!((IndexImpl)ageIndex).supportsSpecialValues()) {
                fail("IndexKey can't take null");
            }
        } catch (IllegalArgumentException iae) {
            if (((IndexImpl)ageIndex).supportsSpecialValues()) {
                fail("IndexKey is allowed to take null but get exception: " +
                     iae.getMessage());
            }
        }

        /*
         * Try to create the IndexKey using JSON.  Put a null back in the Row's
         * age field, turn it to JSON and try to create an index key from that.
         */
        row.putNull("age");
        String rowString = row.toJsonString(true);
        try {
            lastAgeIndex.createIndexKeyFromJson(rowString,
                                                false);
            if (!((IndexImpl)ageIndex).supportsSpecialValues()) {
                fail("IndexKey can't take null");
            }
        } catch (IllegalArgumentException iae) {
            if (((IndexImpl)ageIndex).supportsSpecialValues()) {
                fail("IndexKey is allowed to take null but get exception: " +
                    iae.getMessage());
            }
        }

        /*
         * Try to create the IndexKey using a Row with a null value.
         */
        try {
            lastAgeIndex.createIndexKey(row);
            if (!((IndexImpl)ageIndex).supportsSpecialValues()) {
                fail("IndexKey can't take null");
            }
        } catch (IllegalArgumentException iae) {
            if (((IndexImpl)ageIndex).supportsSpecialValues()) {
                fail("IndexKey is allowed to take null but get exception: " +
                     iae.getMessage());
            }
        } catch (UnsupportedOperationException uoe) {
            if (((IndexImpl)ageIndex).supportsSpecialValues()) {
                fail("IndexKey is allowed to take null but get exception: " +
                     uoe.getMessage());
            }
        }
    }

    /**
     * Test the ability to do batches using the resume key
     */
    @Test
    public void testResume() throws Exception {
        TableImpl userTable = buildUserTable();
        int numUsers = 1000; /* must be > batch size */

        addUsers(tableImpl, userTable, 70, numUsers);

        Index index = userTable.getIndex("Age");
        IndexKey key = index.createIndexKey();
        TableIterator<Row> iter =
            tableImpl.tableIterator(key, null, smallBatchOpts);
        IndexKeyImpl last = null;
        int i = 0;
        while (iter.hasNext()) {
            @SuppressWarnings("deprecation")
            IndexKeyImpl current =
                (IndexKeyImpl) index.createIndexKey(iter.next());
            if (last != null) {
                assertTrue(last.compareTo(current) <= 0);
            }
            last = current;
            ++i;
        }
        assertEquals("Unexpected number of index entries", numUsers, i);
        iter.close();
    }

    /**
     * Test resume with duplicates.
     */
    @Test
    public void testResumeDups() throws Exception {
        TableImpl userTable = buildUserTable();
        int numUsers = 1000; /* must be > batch size */

        addUsers(tableImpl, userTable, 70, numUsers);

        /*
         * All entries above with have lastName "Zachary".  Add entries
         * before and after that to make sure that they are missed in
         * index scans for "Zachary" below.
         */
        addUserRow(tableImpl, userTable, 10000,
                   "joe", "Zach", 10);
        addUserRow(tableImpl, userTable, 10001,
                   "joe", "Zachary1", 10);

        /*
         * The LastName index is all duplicates.  Do the operations twice.
         * The first time iterate the entire index, but the second time
         * specify an exact match key (which will be the only entry).
         */
        Index index = userTable.getIndex("LastName");
        IndexKey key = index.createIndexKey();
        TableIteratorOptions itOpts =
            new TableIteratorOptions(Direction.FORWARD,
                                     Consistency.ABSOLUTE,
                                     0, null, /* timeout, unit */
                                     0, 5); /* batch size 5 */
        TableIteratorOptions reverseItOpts =
            new TableIteratorOptions(Direction.REVERSE,
                                     Consistency.ABSOLUTE,
                                     0, null, /* timeout, unit */
                                     0, 5); /* batch size 5 */

        for (int j = 0; j < 2; j++) {
            if (j > 0) {
                key.put("lastName", "Zachary");
            }
            TableIterator<Row> iter =
                tableImpl.tableIterator(key, null, itOpts);
            IndexKeyImpl last = null;
            Row lastRow = null;
            int i = 0;
            while (iter.hasNext()) {
                Row row = iter.next();
                @SuppressWarnings("deprecation")
                IndexKeyImpl current =
                    (IndexKeyImpl) index.createIndexKey(row);
                if (last != null) {
                    int comp = last.compareTo(current);

                    /*
                     * Duplicates will all be == but the rows themselves
                     * must be strictly < in this case.
                     */
                    assertTrue(comp <= 0);
                    if (comp == 0 && lastRow != null) {
                        assertTrue(lastRow.compareTo(row) < 0);
                    }
                }
                last = current;
                lastRow = row;
                ++i;
            }
            assertEquals("Unexpected number of index entries",
                         (j == 0 ? numUsers + 2 : numUsers), i);
            iter.close();

            /*
             * Do the same iteration in reverse
             */
            iter = tableImpl.tableIterator(key, null, reverseItOpts);
            last = null;
            lastRow = null;
            i = 0;
            while (iter.hasNext()) {
                Row row = iter.next();
                @SuppressWarnings("deprecation")
                IndexKeyImpl current =
                    (IndexKeyImpl) index.createIndexKey(row);
                if (last != null) {
                    int comp = last.compareTo(current);

                    /*
                     * Duplicates will all be == but the rows themselves
                     * must be strictly > in this case.
                     */
                    assertTrue(comp >= 0);
                    if (comp == 0 && lastRow != null) {
                        assertTrue(lastRow.compareTo(row) > 0);
                    }
                }
                last = current;
                lastRow = row;
                ++i;
            }
            assertEquals("Unexpected number of index entries",
                         (j == 0 ? numUsers + 2 : numUsers), i);
            iter.close();
        }
    }

    /**
     * Test reverse iteration
     */
    @Test
    public void testReverse() throws Exception {
        TableImpl userTable = buildUserTable();
        int numUsers = 10;

        addUsers(tableImpl, userTable, 70, numUsers);

        Index index = userTable.getIndex("Age");
        IndexKey key = index.createIndexKey();
        TableIterator<Row> iter = tableImpl.tableIterator(key, null,
                                                          reverseOpts);
        IndexKeyImpl last = null;
        int i = 0;
        while (iter.hasNext()) {
            @SuppressWarnings("deprecation")
            IndexKeyImpl current =
                (IndexKeyImpl) index.createIndexKey(iter.next());
            if (last != null) {
                assertTrue(last.compareTo(current) >= 0);
            }
            last = current;
            ++i;
        }
        assertEquals("Unexpected number of index entries", numUsers, i);
        iter.close();

        /*
         * Now use a range and a composite index
         */
        index = userTable.getIndex("LastAge");
        key = index.createIndexKey();
        key.put("lastName", "Zachary");
        /*
         * This will create a prefix and start only
         */
        FieldRange range = index.createFieldRange("age")
            .setStart(75, true);
        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       reverseOpts);
        last = null;
        i = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            @SuppressWarnings("deprecation")
            IndexKeyImpl current =
                (IndexKeyImpl) index.createIndexKey(row);
            if (last != null) {
                assertTrue(last.compareTo(current) >= 0);
            }
            last = current;
            ++i;
        }
        assertEquals("Unexpected number of index entries",
                     70 + numUsers - 75, i);
        iter.close();

        /*
         * Create a simple equality (reverse) iteration.  This will match
         * only one record.
         */
        index = userTable.getIndex("Age");
        key = index.createIndexKey();
        key.put("age", 72);
        iter = tableImpl.tableIterator(key, null, reverseOpts);
        last = null;
        i = 0;
        while (iter.hasNext()) {
            @SuppressWarnings("deprecation")
            IndexKeyImpl current =
                (IndexKeyImpl) index.createIndexKey(iter.next());
            if (last != null) {
                assertTrue(last.compareTo(current) >= 0);
            }
            last = current;
            ++i;
        }
        assertEquals("Unexpected number of index entries", 1, i);
        iter.close();

        /*
         * Make it an exclusive end-only range on the single-value index
         */
        range = index.createFieldRange("age").setEnd(76, false);
        key = index.createIndexKey();
        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       reverseOpts);
        last = null;
        i = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            @SuppressWarnings("deprecation")
            IndexKeyImpl current =
                (IndexKeyImpl) index.createIndexKey(row);
            if (last != null) {
                assertTrue(last.compareTo(current) >= 0);
            }
            last = current;
            ++i;
        }
        assertEquals("Unexpected number of index entries", 6, i);
        iter.close();
    }

    @Test
    public void enumIndex() {
        TableImpl enumTable = buildEnumTable();
        addEnumUsers(enumTable, 500);
        Index index = enumTable.getIndex("Weekday");
        IndexKey key = index.createIndexKey();

        /*
         * Scan all of the elements. This will return duplicate records
         * from different shards (SR 23421).
         */
        TableIterator<Row> iter = tableImpl.tableIterator(key, null, null);
        int i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();
        assertEquals("Unexpected number of index entries", 500, i);

        key.putEnum("day_of_week", "tuesday");

        /*
         * Forward
         */
        iter = tableImpl.tableIterator(key, null, forwardOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();
        assertEquals("Unexpected number of index entries", 72, i);

        /*
         * Reverse
         */
        iter = tableImpl.tableIterator(key, null, reverseOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();
        assertEquals("Unexpected number of index entries", 72, i);

        /*
         * Unordered
         */
        iter = tableImpl.tableIterator(key, null, unorderedOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();
        assertEquals("Unexpected number of index entries", 72, i);

        /*
         * Composite index and field range
         */
        index = enumTable.getIndex("WeekdayAge");
        key = index.createIndexKey();
        FieldRange range = index.createFieldRange("day_of_week")
            .setStartEnum("sunday", true);

        /*
         * Reverse iteration starting with the last enum value in a range.
         * This is a special case because there are not legitimate values
         * after this one for the field.
         */
        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       reverseOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();
        assertEquals("Unexpected number of index entries", 71, i);

        /*
         * Reverse iteration use the last enum value in a prefix.
         * This is a special case because there are not legitimate values
         * after this one for the field.
         */
        key.putEnum("day_of_week", "sunday");
        iter = tableImpl.tableIterator(key, null, reverseOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();
        assertEquals("Unexpected number of index entries", 71, i);

        /*
         * Reverse iteration using an exclusive range on the composite but not the
         * last value.  This is a > "friday" range query.
         */
        range.setStartEnum("friday", false);
        key = index.createIndexKey();
        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       reverseOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();

        /*
         * total of saturday and sunday
         */
        assertEquals("Unexpected number of index entries", 142, i);

        /*
         * Same thing, but forward.
         */
        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       forwardOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();

        /*
         * total of saturday and sunday
         */
        assertEquals("Unexpected number of index entries", 142, i);

        /*
         * Add some edge-case entries to test inclusivity/exclusivity and
         * wrapping of index key values.
         */
        addEdgeEnumUsers(enumTable);

        index = enumTable.getIndex("LongWeekdayAge");
        key = index.createIndexKey();
        key.put("l1", Long.MAX_VALUE);
        iter = tableImpl.tableIterator(key, null,
                                       reverseOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();
        assertEquals("Unexpected number of index entries", 2, i);

        /*
         * Add end range on the enumeration.  This still has 2 results.
         */
        range = index.createFieldRange("day_of_week")
            .setEndEnum("sunday", true);

        iter = tableImpl.tableIterator(key, range.createMultiRowOptions(),
                                       reverseOpts);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        iter.close();
        assertEquals("Unexpected number of index entries", 2, i);
    }

    /**
     * Test iteration order.  This test specifically tests a case that failed
     * before UNORDERED iterations were not properly handled.
     */
    @Test
    public void testIterationOrder()
        throws Exception {

        TableImpl userTable = buildUserTable();
        addUserRow(tableImpl, userTable, 1,
                   "joe", "cool", 10);
        Index index = userTable.getIndex("FirstName");
        IndexKey ikey = index.createIndexKey();
        ikey.put("firstName", "joe1");

        TableIterator<Row> iter = tableImpl.tableIterator(ikey, null,
                                                          unorderedOpts);
        while (iter.hasNext()) {
            fail("Iteration should have failed to find a match");
            iter.next();
        }
        iter.close();

        iter = tableImpl.tableIterator(ikey, null, forwardOpts);
        while (iter.hasNext()) {
            fail("Iteration should have failed to find a match");
            iter.next();
        }
        iter.close();

        iter = tableImpl.tableIterator(ikey, null, reverseOpts);
        while (iter.hasNext()) {
            fail("Iteration should have failed to find a match");
            iter.next();
        }
        iter.close();
    }

    /**
     * Test use of boolean index. No value comparisons are done. This just
     * exercises the simple paths for boolean -- forward and reverse iteration
     * using both Row and KeyPair. The index has 2 components
     * because it will be unlikely that a user will create a single-component
     * boolean index.
     */
    @Test
    public void testBooleanIndex()
        throws Exception {

        TableImpl userTable = buildUserTable();
        addUsers(tableImpl, userTable, 1, 26);
        Index index = userTable.getIndex("IdCitizen");
        IndexKey ikey = index.createIndexKey();

        TableIterator<Row> iter =
            tableImpl.tableIterator(ikey, null, forwardOpts);
        while (iter.hasNext()) {
            iter.next();
        }
        iter.close();

        TableIterator<KeyPair> keyIter =
            tableImpl.tableKeysIterator(ikey, null, forwardOpts);
        while (keyIter.hasNext()) {
            keyIter.next();
        }
        keyIter.close();

        iter =
            tableImpl.tableIterator(ikey, null, reverseOpts);
        while (iter.hasNext()) {
            iter.next();
        }
        iter.close();

        keyIter =
            tableImpl.tableKeysIterator(ikey, null, reverseOpts);
        while (keyIter.hasNext()) {
            keyIter.next();
        }
        keyIter.close();
    }

    /**
     * Tests that the index iterator detects changes in the topology.
     */
    @Test
    public void testIterationFail() {
        final int startId = 90;
        TableImpl userTable = buildUserTable();

        addUserRow(tableImpl, userTable, 76, "Joe", "Jones", 38);
        addUserRow(tableImpl, userTable, 22, "Joe", "Smith", 72);
        addUserRow(tableImpl, userTable, 87, "Jane", "Doe", 44);
        addUserRow(tableImpl, userTable, 56, "Sam", "Spade", 32);
        addUserRow(tableImpl, userTable, 57, "Amy", "Smith", 11);
        addUserRow(tableImpl, userTable, 55, "Amy", "Jones", 12);
        addUsers(tableImpl, userTable, startId, 25);

        Index index = userTable.getIndex("FirstName");
        IndexKey key = index.createIndexKey();
        key.put("firstName", "Joe");

        final TopologyManager mgr =
                    ((KVStoreImpl)store).getDispatcher().getTopologyManager();

        // Induce a failure by moving a partition
        TableIterator<KeyPair> iter =
                            tableImpl.tableKeysIterator(key, null, forwardOpts);

        final PartitionId p1 = new PartitionId(1);
        RepGroupId origGroup = null;
        try {
            iter.hasNext();
            iter.next();

            final Topology topo = mgr.getTopology().getCopy();
            origGroup = topo.get(p1).getRepGroupId();
            topo.updatePartition(p1, new RepGroupId(999));

            // The update should cause the iterator to be closed
            mgr.update(topo);

            iter.hasNext();
            fail("Expected StoreIteratorException");
        } catch (StoreIteratorException ste) {
            assertTrue(ste.getCause() instanceof UnsupportedOperationException);
        } finally {
            iter.close();

            // Restore the topo, modify it directly
            if (origGroup != null) {
                mgr.getTopology().updatePartition(p1, origGroup);
            }
        }

        // Induce a failure by adding a shard
        iter = tableImpl.tableKeysIterator(key, null, forwardOpts);

        RepGroup newGroup = null;
        try {
            iter.hasNext();
            iter.next();

            final Topology topo = mgr.getTopology().getCopy();
            newGroup = topo.add(new RepGroup());
            mgr.update(topo);

            iter.hasNext();
            fail("Expected StoreIteratorException");
        } catch (StoreIteratorException ste) {
            assertTrue(ste.getCause() instanceof UnsupportedOperationException);
        } finally {
            iter.close();

            // Restore the topo, modify it directly
            if (newGroup != null) {
                mgr.getTopology().remove(newGroup.getResourceId());
            }
        }
    }

    /**
     * This test case is specific to a bug where the system matched a
     * key to an index incorrectly because the key contained a string
     * field that matched the table id of another table containing
     * an index.
     */
    @Test
    public void testBadKey()
        throws Exception {

        TableImpl userTable = TableBuilder.createTableBuilder("User")
            .addString("id")
            .addString("username")
            .addString("password")
            .primaryKey("id")
            .buildTable();
        //TODO: allow MRTable mode after MRTable supports child tables.
        userTable = addTable(userTable, true, true/*noMRTableMode*/);
        userTable = addIndex(userTable, "index",
                             new String[] {"username","password"}, true);

        TableImpl imageTable = TableBuilder.createTableBuilder("Image")
            .addString("id")
            .addBinary("image")
            .primaryKey("id")
            .buildTable();
        imageTable = addTable(imageTable, true);


        /*
         * Create 2 child tables of User to test the same problem in a
         * nested environment.
         */
        TableImpl child1Table =
            TableBuilder.createTableBuilder("c1", null, userTable)
            .addString("id1")
            .addString("username")
            .addString("password")
            .primaryKey("id1")
            .buildTable();

        child1Table = addTable(child1Table, true, true/*noMRTableMode*/);
        child1Table = addIndex(child1Table, "index",
                               new String[] {"username","password"}, true);

        TableImpl child2Table =
            TableBuilder.createTableBuilder("c2", null, userTable)
            .addString("id2")
            .addBinary("image")
            .primaryKey("id2")
            .buildTable();
        child2Table = addTable(child2Table, true, true/*noMRTableMode*/);

        Row row = child2Table.createRow();
        /*
         * This triggered the problem.  The key
         * /<userTableId>/<id>/-/<child2TableId>/<child1TableId>
         * mistakenly matched child1Table and because it is the correct
         * length it matched the index and index extraction threw an exception.
         */
        row.put("id", "x");
        row.put("id2", child1Table.getIdString());
        row.put("image", new byte[] {1,2,3});
        tableImpl.put(row, null, null);

        row = imageTable.createRow();
        /*
         * This triggered the problem.  The key
         * /<imageTableId>/<userTableId> mistakenly matched the
         * User table and because it is the correct length it matched
         * the index and index extraction threw an exception.
         */
        row.put("id", userTable.getIdString());
        row.put("image", new byte[] {1,2,3});
        tableImpl.put(row, null, null);
    }

    /**
     * Tests use of nested fields in indexes.
     */
    @Test
    public void testComplexIndexes()
        throws Exception {

        /* this needs to be a number evenly divisible by 4 */
        /* TODO: make this larger (than chunk size) when 23977 is fixed */
        final int numRows = 60;

        final String[] indexes = new String[] {"city", "map_a", "homeEmail",
                                               "mapOfArray", "arrayIndex"};
        TableImpl userTable = buildComplexUserTable();
        addComplexUsers(userTable, 0, numRows);

        /* Make sure that the indexes have the expected number of entries */

        for (String s : indexes) {
            IndexImpl index = (IndexImpl) userTable.getIndex(s);
            int num = countIndexRecords(index.createIndexKey(), userTable);
            assertEquals("Unexpected number of index entries",
                         (index.isMultiKey() ? numRows * 2 : numRows), num);
        }

        /* Check some specific values in indexes */
        Index cityIndex = userTable.getIndex("city");
        IndexKey ikey = cityIndex.createIndexKey();
        ikey.put("address.city", "Chicago");
        int num = countIndexRecords(ikey, userTable);
        /* 5 entries for Chicago */
        assertEquals("Unexpected index count", numRows/4, num);

        Index mapIndex = userTable.getIndex("map_a");
        ikey = mapIndex.createIndexKey();
        ikey.put("map.a", 5);
        num = countIndexRecords(ikey, userTable);
        /* these map entries are unique */
        assertEquals("Unexpected index count", 1, num);

        Index mapOfArrayIndex = userTable.getIndex("mapOfArray");
        ikey = mapOfArrayIndex.createIndexKey();
        ikey.put("arrayMap.a[]", 19);
        num = countIndexRecords(ikey, userTable);
        /* these map entries are unique */
        assertEquals("Unexpected index count", 1, num);

        Index arrayIndex = userTable.getIndex("arrayIndex");
        ikey = arrayIndex.createIndexKey();
        ikey.put("recArray[].company", "Oracle");
        num = countIndexRecords(ikey, userTable);
        /* 10 entries for Oracle -- each row has 2 companies */
        assertEquals("Unexpected index count", numRows/2, num);

        /* Use a complex type in a FieldRange */
        FieldRange fr = arrayIndex.createFieldRange("recArray[].company");
        fr.setStart("Ora", true);
        fr.setEnd("Wool", false);
        MultiRowOptions mro = fr.createMultiRowOptions();
        ikey = arrayIndex.createIndexKey();
        num = countIndexRecords1(ikey, mro);
        /* 10 entries for Oracle -- it stops at "Wool" */
        assertEquals("Unexpected index count", numRows/2, num);

        /* Now a complex type and array in FieldRange */
        fr = mapOfArrayIndex.createFieldRange("arrayMap.a[]");
        fr.setStart(5, true);
        fr.setEnd(10, true);
        mro = fr.createMultiRowOptions();
        ikey = mapOfArrayIndex.createIndexKey();
        num = countIndexRecords1(ikey, mro);
        /* Range of 6 */
        assertEquals("Unexpected index count", 6, num);

        Index homeEmailIndex = userTable.getIndex("homeEmail");
        fr = homeEmailIndex.createFieldRange("email.home.isp");
        fr.setEnd("Sp", true);
        mro = fr.createMultiRowOptions();
        ikey = homeEmailIndex.createIndexKey();
        num = countIndexRecords1(ikey, mro);
        /* this counts everything except strings starting with "Sp" */
        assertEquals("Unexpected index count", numRows - (numRows/4), num);
        /* use reverse iteration, just because */
        TableIterator<Row> iter = tableImpl.tableIterator(ikey, mro,
                                                         reverseOpts);
        num = 0;
        while (iter.hasNext()) {
            iter.next();
            ++num;
        }
        iter.close();
        assertEquals("Unexpected index count", numRows - (numRows/4), num);

        /*
         * Test null values in intermediate complex fields in an index.  Use
         * existing rows and modify them.  There are indexes on:
         * email.home.isp (map.record.field)
         * arrayMap.a  (map.array)
         * recArray.company (array.recordfield)
         * address.city (record.field)
         * map.a (map.field)
         */
        PrimaryKey key = userTable.createPrimaryKey();
        key.put("id", 1);
        Row row = tableImpl.get(key, null);
        assertNotNull(row);
        FieldValue val = row.get("email");
        row.putNull("email");
        ikey = homeEmailIndex.createIndexKey();
        num = countIndexRecords(ikey, userTable);
        tableImpl.put(row, null, null);
        int expNum = (((IndexImpl)homeEmailIndex).supportsSpecialValues() ?
                      num : num - 1);
        assertEquals("Unexpected index count ", expNum,
                     countIndexRecords(ikey, userTable));
        row.put("email", val); /* restore it */

        val = row.get("recArray");
        assertNotNull(val);
        row.putNull("recArray");
        ikey = arrayIndex.createIndexKey();
        num = countIndexRecords(ikey, userTable);
        tableImpl.put(row, null, null);
        iter = tableImpl.tableIterator(ikey, null, null);
        expNum = ((IndexImpl)arrayIndex).supportsSpecialValues() ?
                 num - ((ArrayValue)val).size() + 1 :
                 num - ((ArrayValue)val).size();
        assertEquals("Unexpected index count ", expNum,
                     countIndexRecords(ikey, userTable));
        row.put("recArray", val); /* restore it */

        RecordValue rval = row.get("address").asRecord();
        rval.remove("city");
        ikey = cityIndex.createIndexKey();
        num = countIndexRecords(ikey, userTable);
        tableImpl.put(row, null, null);
        expNum = ((IndexImpl)cityIndex).supportsSpecialValues() ? num : num - 1;
        assertEquals("Unexpected index count ", expNum,
                     countIndexRecords(ikey, userTable));
        rval.put("city", "foo");
    }

    /**
     * Tests resume of index iteration on complex indexes (arrays, maps,
     * records).
     */
    @Test
    public void testResumeComplex()
        throws Exception {

        Index index = null;
        IndexKey ikey = null;
        TableIterator<Row> rowIter = null;
        int count = 0;

        final int numRows = 50;
        TableImpl userTable = buildComplexUserTable();
        addComplexUsers(userTable, 0, numRows);

        index = userTable.getIndex("mapValue");
        ikey = index.createIndexKey();
        rowIter = tableImpl.tableIterator(ikey, null, smallBatchOpts);
        while (rowIter.hasNext()) {
            rowIter.next();
            count++;
        }
        rowIter.close();
        /* there is a single unique map value for each map */
        assertEquals("Unexpected count", numRows, count);

        /* reverse iteration */
        count = 0;
        rowIter = tableImpl.tableIterator(ikey, null, smallBatchReverseOpts);
        while (rowIter.hasNext()) {
            rowIter.next();
            count++;
        }
        rowIter.close();
        /* there is a single unique map value for each map */
        assertEquals("Unexpected count", numRows, count);

        count = 0;
        index = userTable.getIndex("mapKey");
        ikey = index.createIndexKey();
        rowIter = tableImpl.tableIterator(ikey, null, smallBatchOpts);
        while (rowIter.hasNext()) {
            rowIter.next();
            count++;
        }
        rowIter.close();
        /* there are 2 map entries for each row, they have the same value */
        assertEquals("Unexpected count", numRows, count);

        count = 0;
        index = userTable.getIndex("arrayIndex");
        ikey = index.createIndexKey();
        rowIter = tableImpl.tableIterator(ikey, null, smallBatchOpts);
        while (rowIter.hasNext()) {
            rowIter.next();
            count++;
        }
        rowIter.close();
        /* there are 2 unique array entries for each row */
        assertEquals("Unexpected count", numRows, count);
    }

    /**
     * Tests scan NULLs in index key with simple indexes.
     */
    @Test
    public void testScanNullsSimpleIndex() {

        TableImpl userTable = buildUserTable();

        if (!((IndexImpl)userTable.getIndex("firstName")).
            supportsSpecialValues()) {
            /* Skip this test if index does not support NULLs */
            return;
        }

        final int startId = 20;
        final int nRowsWithNulls = 10;
        boolean ageIsNull = false;
        List<Integer> idsOfAgeNull = new ArrayList<Integer>();
        for (int i = 0; i < nRowsWithNulls; i++) {
            Integer age = ageIsNull ? null : 20;
            String last = !ageIsNull ? null : "last";
            addUserRow(tableImpl, userTable, i, null, last, age);
            if (ageIsNull) {
                idsOfAgeNull.add(i);
            }
            ageIsNull = !ageIsNull;
        }

        /*
         * Start at id 20 to avoid collision with above records
         */
        addUsers(tableImpl, userTable, startId, 25);

        /* Using FirstName index to retrieve the rows with NULL for firstName */
        IndexKey ikey = userTable.getIndex("firstName").createIndexKey();
        ikey.putNull("firstName");
        TableIterator<Row> iter = tableImpl.tableIterator(ikey, null,
                                                          forwardOpts);
        int idx = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            assertTrue(idx == row.get("id").asInteger().get());
            idx++;
        }
        assertTrue(idx == nRowsWithNulls);
        iter.close();

        /*
         * Using AgeLast index to retrieve the rows with NULL for Age
         */

        /* ikey: {"age":null} */
        ikey = userTable.getIndex("AgeLast").createIndexKey();
        ikey.putNull("age");
        iter = tableImpl.tableIterator(ikey, null, forwardOpts);
        idx = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            int id = row.get("id").asInteger().get();
            assertTrue(id == idsOfAgeNull.get(idx));
            idx++;
        }
        assertTrue(idx == idsOfAgeNull.size());
        iter.close();

        /* ikey: {"age":null, "lastName":"last"} */
        ikey.clear();
        ikey.putNull("age");
        ikey.put("lastName", "last");
        iter = tableImpl.tableIterator(ikey, null, forwardOpts);
        idx = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            int id = row.get("id").asInteger().get();
            assertTrue(id == idsOfAgeNull.get(idx));
            idx++;
        }
        assertTrue(idx == idsOfAgeNull.size());
        iter.close();

        /*
         * Using LastAge index to retrieve the rows with NULL for Age.
         *  ikey: {"lastName":"last", "age":null}
         */
        ikey = userTable.getIndex("LastAge").createIndexKey();
        ikey.put("lastName", "last");
        ikey.putNull("age");
        iter = tableImpl.tableIterator(ikey, null, forwardOpts);
        idx = 0;
        while (iter.hasNext()) {
            Row row = iter.next();
            int id = row.get("id").asInteger().get();
            assertTrue(id == idsOfAgeNull.get(idx));
            idx++;
        }
        assertTrue(idx == idsOfAgeNull.size());
        iter.close();
    }

    /**
     * Tests scan NULLs in index key with complex indexes.
     */
    @Test
    public void testScanNullsComplexIndexes() {
        final int numRows = 60;
        TableImpl userTable = buildComplexUserTable();

        if (!((IndexImpl)userTable.getIndex("mapOfArray")).
            supportsSpecialValues()) {
            /* Skip this test if index does not support NULLs */
            return;
        }

        addComplexUsers(userTable, 20, numRows);

        List<Integer> idsOfRowWithNulls = new ArrayList<Integer>();
        for (int i = 0; i < 10; i++) {
            addComplexUserRow(userTable, i, "first" + i, "last" + i,
                              true, true, true, true);
            idsOfRowWithNulls.add(i);
        }

        final String[] indexes = new String[] {"mapOfArray", "map_a",
                "mapValue", "mapKey", "homeEmail"};

        for (String name : indexes) {
            Index index = userTable.getIndex(name);
            IndexKey ikey = index.createIndexKey();
            ikey.putNull(ikey.getFieldNames().get(0));

            TableIterator<KeyPair> iter =
                tableImpl.tableKeysIterator(ikey, null, forwardOpts);

            int idx = 0;
            while (iter.hasNext()) {
                KeyPair row = iter.next();
                int id = row.getPrimaryKey().get("id").asInteger().get();
                ikey = row.getIndexKey();
                assertTrue(id == idsOfRowWithNulls.get(idx));
                assertTrue(ikey.get(0).isNull());
                idx++;
            }
            assertTrue(idx == idsOfRowWithNulls.size());
            iter.close();
        }
    }

    @Test
    public void testCreateDuplicateIndex()
        throws Exception {

        String ddl = "create table User (" +
                        "id integer, " +
                        "name record(first string, last string), " +
                        "address map(record(city string, street string)), " +
                        "primary key(id)" +
                     ")";
        executeDdl(ddl, true);

        ddl = "create index idxName on user(name.first, name.last)";
        executeDdl(ddl, true);

        ddl = "create index idxAddressCity on user(address.home.city)";
        executeDdl(ddl, true);

        /* Quotes should be ignored when comparing index fields */
        ddl = "create index idxNameDup on user(\"name\".first, name.\"last\")";
        executeDdl(ddl, false);
        ddl = "create index idxAddressCityDup on user(\"address\".home.city)";
        executeDdl(ddl, false);
        ddl = "create index idxAddressCityDup on user(address.\"home\".city)";
        executeDdl(ddl, false);
        ddl = "create index idxAddressCityDup on user(address.home.\"city\")";
        executeDdl(ddl, false);

        /* Case sensitivity test */

        /* Case-insensitive comparison */
        ddl = "create index idxNameDup on user(Name.First, NAME.\"LAST\")";
        executeDdl(ddl, false);
        ddl = "create index idxAddressCityDup on user(Address.home.City)";
        executeDdl(ddl, false);
        /* Comparison of Map.<key> is case-sensitive */
        ddl = "create index idxAddressCity1 on user(Address.Home.City)";
        executeDdl(ddl, true);

        /*
         * Test TableDdlOperation.indexExistsAndEqual() to compare the fields
         * for "create index if not exists.." statement.
         */
        ddl = "create index if not exists idxName on user(name.first, name.last)";
        StatementResult res = ((KVStoreImpl)store).executeSync(ddl, null);
        assertEquals(res.getInfo(), res.getInfo(), DdlJsonFormat.NOOP_STATUS);

        ddl = "create index if not exists idxName on user(\"name\".first, " +
                "name.\"last\")";
        res = ((KVStoreImpl)store).executeSync(ddl, null);
        assertEquals(res.getInfo(), res.getInfo(), DdlJsonFormat.NOOP_STATUS);

        ddl = "create index if not exists idxName on user(Name.First, " +
                "NAME.\"LAST\")";
        res = ((KVStoreImpl)store).executeSync(ddl, null);
        assertEquals(res.getInfo(), res.getInfo(), DdlJsonFormat.NOOP_STATUS);

        ddl = "create index if not exists idxAddressCity on user(" +
                "address.home.city)";
        res = ((KVStoreImpl)store).executeSync(ddl, null);
        assertEquals(res.getInfo(), res.getInfo(), DdlJsonFormat.NOOP_STATUS);

        ddl = "create index if not exists idxAddressCity on user(" +
                "\"address\".home.city)";
        res = ((KVStoreImpl)store).executeSync(ddl, null);
        assertEquals(res.getInfo(), res.getInfo(), DdlJsonFormat.NOOP_STATUS);

        ddl = "create index if not exists idxAddressCity on user(" +
                "address.\"home\".city)";
        res = ((KVStoreImpl)store).executeSync(ddl, null);
        assertEquals(res.getInfo(), res.getInfo(), DdlJsonFormat.NOOP_STATUS);

        ddl = "create index if not exists idxAddressCity on user(" +
                "address.home.\"city\")";
        res = ((KVStoreImpl)store).executeSync(ddl, null);
        assertEquals(res.getInfo(), res.getInfo(), DdlJsonFormat.NOOP_STATUS);

        ddl = "create index if not exists idxAddressCity on user(" +
                "Address.home.City)";
        res = ((KVStoreImpl)store).executeSync(ddl, null);
        assertEquals(res.getInfo(), res.getInfo(), DdlJsonFormat.NOOP_STATUS);
    }

    @Test
    public void testIndexLimit() throws Exception {
        /* Set a index limit to 0 */
        TableLimits limits = new TableLimits(NO_LIMIT /* readLimit */,
                                             NO_LIMIT /* writeLimit */,
                                             NO_LIMIT /* sizeLimit */,
                                             0        /* maxIndexes */,
                                             NO_LIMIT /* maxChildren */,
                                             NO_LIMIT /* indexKeySizeLimit */);
        assertTrue(limits.hasIndexLimit());

        final TableImpl t = buildUserTable(limits, false /* addIndexes */);

        /* Attempt to create the first index should fail */
        addIndex(t, "FirstName", new String[] {"firstName"}, false);

        /* Bump up the limit and retry */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 1        /* maxIndexes */,
                                 NO_LIMIT /* maxChildren */,
                                 NO_LIMIT /* indexKeySizeLimit */);
        setTableLimits(t, limits);

        /* Should be OK */
        addIndex(t, "FirstName", new String[] {"firstName"}, true);

        /* But not a second one */
        addIndex(t, "Age", new String[] {"age"}, false);

        /* Remove the limit and retry */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 NO_LIMIT /* maxIndexes */,
                                 NO_LIMIT /* maxChildren */,
                                 NO_LIMIT /* indexKeySizeLimit */);
        setTableLimits(t, limits);

        /* Now there will be two */
        addIndex(t, "Age", new String[] {"age"}, true);

        /* Attempt to reduce the limit to below the number of indexes */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 1        /* maxIndexes */,
                                 NO_LIMIT /* maxChildren */,
                                 NO_LIMIT /* indexKeySizeLimit */);
        /* Should fail */
        setTableLimits(t, limits, false);

        /* Set at the number of indexes */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 2        /* maxIndexes */,
                                 NO_LIMIT /* maxChildren */,
                                 NO_LIMIT /* indexKeySizeLimit */);
        /* Should work */
        setTableLimits(t, limits, true);
    }

    @Test
    public void testTableAccess() throws Exception {
        final TableImpl t = buildUserTable(null, false /* addIndexes */);

        /* Set table to r/o */
        TableLimits limits = TableLimits.NO_ACCESS;
        setTableLimits(t, limits);

        /* Attempt to create the index should fail */
        addIndex(t, "FirstName", new String[] {"firstName"}, false);

        /* Set table to r/o */
        limits = TableLimits.READ_ONLY;
        setTableLimits(t, limits);

        /* Still fail */
        addIndex(t, "FirstName", new String[] {"firstName"}, false);
    }

    @Test
    public void testKeySizeLimit() throws Exception {
        /* Set a index key size limit */
        TableLimits limits = new TableLimits(NO_LIMIT /* readLimit */,
                                             NO_LIMIT /* writeLimit */,
                                             NO_LIMIT /* sizeLimit */,
                                             NO_LIMIT /* maxIndexes */,
                                             NO_LIMIT /* maxChildren */,
                                             9        /* indexKeySizeLimit */);
        assertTrue(limits.hasIndexKeySizeLimit());

        final TableImpl t = buildUserTable(limits, true);

        /* Setting the limits to a smaller value should fail */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 NO_LIMIT /* maxIndexes */,
                                 NO_LIMIT /* maxChildren */,
                                 2        /* indexKeySizeLimit */);
        assertTrue(limits.hasIndexKeySizeLimit());
        setTableLimits(t, limits, false);

        /*
         * Write some short records that _just happen_ to end up different
         * shards. This will cause index populate to fail on both shards
         * during the next addIndex. See [#26975]
         */
        addUserRow(tableImpl, t, 76, "Short", "Jones", 38);
        addUserRow(tableImpl, t, 21, "Short", "Janes", 38);

        /* Attempt to write a record with a long key */
        try {
            addUserRow(tableImpl, t, 76, "LongishKey", "Jones", 38);
            fail("key should have exceeded limit");
        } catch (KeySizeLimitException ksle) {
            /* expected */
        }

        /*
         * Attempt to create an index with a long key so that existing records
         * will be over the limit. This should fail in the index population
         * phase of the plan.
         */
        addIndex(t, "FirstName2",
                 new String[] {"firstName", "id", "lastName"}, false);

        /* Up the limit and retry the write of the longish key */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 NO_LIMIT /* maxIndexes */,
                                 NO_LIMIT /* maxChildren */,
                                 20       /* indexKeySizeLimit */);
        setTableLimits(t, limits, true);

        addUserRow(tableImpl, t, 76, "LongishKey", "Jones", 38);

        /* Attempt to write a really long key */
        try {
            addUserRow(tableImpl, t, 76,
                       "SuperDupperVeryLongKey", "Jones", 38);
            fail("key should have exceeded limit");
        } catch (KeySizeLimitException ksle) {
            /* expected */
        }

        /* Remove the limit and retry */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 NO_LIMIT /* maxIndexes */,
                                 NO_LIMIT /* maxChildren */,
                                 NO_LIMIT /* indexKeySizeLimit */);
        assertFalse(limits.hasIndexKeySizeLimit());
        setTableLimits(t, limits, true);
        addUserRow(tableImpl, t, 76,
                   "SuperDupperVeryLongKey", "Jones", 38);

        /* We should be able to create the index with the long key now */
        addIndex(t, "FirstName2",
                 new String[] {"firstName", "id", "lastName"}, true);
    }

    @Test
    public void testUntypedIndex() throws Exception {
        final String createTable = "create table JsonTable " +
            "(id integer, json json, primary key(id))";
        final String createIndex = "create index untyped on JsonTable " +
            "(json.name as anyatomic)";
        executeDdl(createTable, true);
        executeDdl(createIndex, true);
        Table table = getTable("JsonTable");
        Row row = table.createRow();
        row.put("id", 1);
        row.putJson("json", "{\"name\":\"joe\"}");
        tableImpl.put(row, null, null);
        row.put("id", 2);
        tableImpl.put(row, null, null);
        IndexKey ikey = table.getIndex("untyped").createIndexKey();
        TableIterator<KeyPair> iter = tableImpl.tableKeysIterator(ikey, null, null);
        int numEntries = 0;
        while (iter.hasNext()) {
            iter.next();
            ++numEntries;
        }
        iter.close();
        assertTrue(numEntries == 2);

        RegionMapper rm = null;
        if (mrTableMode) {
            final TableAPIImpl tblAPI = (TableAPIImpl)store.getTableAPI();
            rm = tblAPI.getRegionMapper();
        }
        DDLGenerator gen = new DDLGenerator(table, rm);
        List<String> indexes = gen.getAllIndexDDL();
        assertEquals(1, indexes.size());
        assertTrue(indexes.get(0).toLowerCase().contains("anyatomic"));
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

    static private RowImpl addUserRow(TableAPI impl,
                                      Table table,
                                      int id, String first, String last,
                                      Integer age) {
        RowImpl row = (RowImpl) table.createRow();
        row.put("id", id);

        /* add the isCitizen field based on even/odd ids */
        boolean isCitizen = (id % 2 == 0 ? true : false);
        row.put("isCitizen", isCitizen);

        if (first != null) {
            row.put("firstName", first);
        } else {
            row.putNull("firstName");
        }

        if (last != null) {
            row.put("lastName", last);
        } else {
            row.putNull("lastName");
        }

        if (age != null) {
            row.put("age", age);
        } else {
            row.putNull("age");
        }

        if (impl != null) {
            impl.put(row, null, null);
        }
        return row;
    }

    private TableImpl buildUserTable() {
        return buildUserTable(null, true);
    }

    private TableImpl buildUserTable(TableLimits limits, boolean addIndexes) {
        try {
            TableBuilder builder = (TableBuilder)
                TableBuilder.createTableBuilder("User",
                                                "Table of Users",
                                                null)
                .addInteger("id")
                .addString("lastName")
                .addInteger("age")
                .addString("firstName")
                .addBoolean("isCitizen")
                .primaryKey("id")
                .shardKey("id");
            addTable(builder, true);

            TableImpl t = getTable("User");
            if (limits != null) {
                setTableLimits(t, limits);
            }

            if (!addIndexes) {
                return t;
            }
            t = addIndex(t, "FirstName",
                         new String[] {"firstName"}, true);
            t = addIndex(t, "Age",
                         new String[] {"age"}, true);
            t = addIndex(t, "LastAge",
                         new String[] {"lastName", "age"}, true);
            t = addIndex(t, "AgeLast",
                         new String[] {"age", "lastName"}, true);
            t = addIndex(t, "IdCitizen",
                         new String[] {"id", "isCitizen"}, true);

            /*
             * This uses the primary key field in an index.
             */
            t = addIndex(t, "AgeId",
                         new String[] {"age", "id"}, true);

            /* LastName will have a lot of duplicates */
            t = addIndex(t, "LastName",
                         new String[] {"lastName"}, true);
            return t;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add table: " + e, e);
        }
    }

    private TableImpl buildEnumTable() {
        try {
            TableBuilder builder = (TableBuilder)
                TableBuilder.createTableBuilder("Enum")
                .addInteger("id")
                .addEnum("day_of_week",
                         new String[]{"monday", "tuesday", "wednesday",
                                      "thursday", "friday", "saturday",
                                      "sunday"}, null, null, null)
                .addInteger("age")
                .addLong("l1")
                .primaryKey("id")
                .shardKey("id");
            addTable(builder, true);

            TableImpl t = getTable("Enum");
            t = addIndex(t, "Weekday",
                         new String[] {"day_of_week"}, true);
            t = addIndex(t, "WeekdayAge",
                         new String[] {"day_of_week", "age"}, true);
            t = addIndex(t, "LongWeekdayAge",
                         new String[] {"l1", "day_of_week", "age"}, true);
            return t;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add table: " + e, e);
        }
    }

    /**
     * Creates a user table with complex types to test nested indexes.
     */
    private TableImpl buildComplexUserTable() {
        try {
            TableBuilder builder = (TableBuilder)
                TableBuilder.createTableBuilder("User")
                .addInteger("id")
                .addString("lastName")
                .addString("firstName")
                /* Nested address record */
                .addField("address", TableBuilder.createRecordBuilder("address")
                          .addInteger("number")
                          .addInteger("zip")
                          .addString("street")
                          .addString("city")
                          .build())
                /* Map of integer */
                .addField("map", TableBuilder.createMapBuilder(null)
                          .addInteger().build())
                /* Map of record (email addresses), keyed by type */
                .addField("email", TableBuilder.createMapBuilder()
                          .addField(TableBuilder.createRecordBuilder("email")
                                    .addString("address")
                                    .addString("isp")
                                    .build())
                          .build())
                /* Map of array of integer */
                .addField("arrayMap", TableBuilder.createMapBuilder()
                          .addField(TableBuilder.createArrayBuilder()
                                    .addInteger().build())
                          .build())
                /* Array of record of jobs */
                .addField("recArray", TableBuilder.createArrayBuilder()
                          .addField(TableBuilder.createRecordBuilder("jobs")
                                    .addString("title")
                                    .addString("company")
                                .build())
                      .build())
                .primaryKey("id")
                .shardKey("id");
            addTable(builder, true);

            TableImpl t = getTable("User");

            /*
             * Add some complex indexes
             */
            t = addIndex(t, "homeEmail",
                         new String[] {"email.home.isp"}, true);
            t = addIndex(t, "mapOfArray",
                         new String[] {"arrayMap.a[]"}, true);
            t = addIndex(t, "arrayIndex",
                         new String[] {"recArray[].company"}, true);
            t = addIndex(t, "city",
                         new String[] {"address.city"}, true);
            t = addIndex(t, "map_a",
                         new String[] {"map.a"}, true);
            t = addIndex(t, "mapValue",
                         new String[] {"map.vaLUEs()"}, true);
            t = addIndex(t, "mapKey",
                         new String[] {"map.KEYs()"}, true);
            return t;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add table: " + e, e);
        }
    }

    private void  addComplexUsers(TableImpl table, int startId, int num) {
        /*
         * add a number of users.  Differentiate on age, first name, and
         * id (the primary key).  Ensure that the first name and id fields
         * sort in opposite order to exercise sorting.
         */

        for (int i = 0; i < num; i++) {
            addComplexUserRow(table, startId + i,
                              ("Zach" + (num - i)),
                              "Zachary");
        }
    }

    static private void addComplexUserRow(Table table, int id,
                                          String first, String last) {
        addComplexUserRow(table, id, first, last, false, false, false, false);
    }

    static private void addComplexUserRow(Table table, int id,
                                          String first, String last,
                                          boolean addressCityIsNull,
                                          boolean mapIsNull,
                                          boolean emailHostIspIsNull,
                                          boolean arrayMapIsNull) {
        String cities[] =
            {"Chicago", "Boston", "New York", "Los Angeles"};
        String isps[] =
            {"MyIsp", "FastNet", "SlowNet", "SpyNet"};
        String companies[] =
            {"Acme", "Oracle", "Woolworths", "Joe's Hardware"};
        RowImpl row = (RowImpl) table.createRow();
        row.put("id", id);
        row.put("firstName", first);
        row.put("lastName", last);

        /* address, include at least the indexed field */
        RecordValue rv = row.putRecord("address");
        if (addressCityIsNull) {
            rv.putNull("city");
        } else {
            rv.put("city", cities[id % cities.length]);
        }

        /*
         * add an indexed field from the map of integer as well as
         * a field that has an awkward name.
         */
        if (mapIsNull) {
            row.putNull("map");
        } else {
            row.putMap("map").put("a", id).put("a-7&foo", id);
        }

        /* add email address (map or record, index on record field) */
        String isp = isps[id % isps.length];
        RecordValue rec = row.putMap("email").putRecord("home");
        if (emailHostIspIsNull) {
            rec.putNull("address").putNull("isp");
        } else {
            rec.put("address", ("foo@" + isp)).put("isp", isp);
        }

        /* add map of array of integer, with 2 unique entries */
        if (arrayMapIsNull) {
            row.putNull("arrayMap");
        } else {
            row.putMap("arrayMap").putArray("a").add(id).add(id+500);
        }

        /* array or record of jobs -- add 2 */
        ArrayValue array = row.putArray("recArray");
        array.addRecord().put("title", "mts").
            put("company", companies[id % companies.length]);
        array.addRecord().put("title", "boss").
            put("company", companies[(id + 1) % companies.length]);

        tableImpl.put(row, null, null);
    }

    private void addEnumUsers(TableImpl table, int num) {
        EnumDef enumDef = (EnumDef) table.getField("day_of_week");
        String[] days = enumDef.getValues();
        for (int i = 0; i < num; i++) {
            RowImpl row = table.createRow();
            row.put("id", i);
            row.putEnum("day_of_week", days[i%7]);
            row.put("age", i);
            row.put("l1", i * 6000L);
            tableImpl.put(row, null, null);
        }
    }

    /**
     * Add some users to specifically test iteration in the LongWeekdayAge
     * index.
     */
    private void addEdgeEnumUsers(TableImpl table) {
        RowImpl row = table.createRow();
        row.put("id", 100);
        row.put("l1", Long.MAX_VALUE);  /* max */
        row.putEnum("day_of_week", "sunday"); /* max */
        row.put("age", 100); /* not max */
        tableImpl.put(row, null, null);

        row.put("id", 101);
        row.put("l1", Long.MAX_VALUE-1);  /* almost max */
        row.putEnum("day_of_week", "sunday"); /* max */
        row.put("age", 100); /* not max */
        tableImpl.put(row, null, null);

        row.put("id", 102);
        row.put("l1", Long.MAX_VALUE);  /* max */
        row.putEnum("day_of_week", "saturday"); /* almost max */
        row.put("age", 100); /* not max */
        tableImpl.put(row, null, null);
    }
}
