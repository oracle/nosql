/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableLimits.NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.junit.Test;

/*
 * Test nested tables.  This is intended to exercise
 * table creation and removal plans and use of TableMetadata.
 *
 * The hierarchy created is:
 * User (id)
 * User.Address (id, addrId)
 * User.Address.Email (id, addrId, emailAddress)
 * User.Info  (id, myid)
 *
 * Using this hierarchy exercises some of the server side code that resets
 * the cursor during iteration to skip tables.
 */
public class TableNestingTest extends TableNestingTestBase {

    @Test
    public void nestedTables()
        throws Exception {

        /*
         * Putting this up here makes it easier to selectively comment out
         * test sections.
         */
        PrimaryKey pkey = null;
        TableIterator<Row> iter = null;
        int numRecords = 0;
        FieldValue lastValue = null;

        /*
         * Create and populate tables.  See comment above for hierarchy.
         */
        createTables();

        int numInUsers = numUsers;
        int numInAddress = numUsers * numRows;
        int numInInfo = numUsers * numRows;
        int numInEmail = numInAddress * numRows;

        /* TODO - CANNOT USE STORE RECORD COUNT...
        assertEquals("Unexpected record count",
                     (numInUsers + numInAddress + numInInfo + numInEmail),
                     countStoreRecords());
         */
        assertTrue(countTableRecords(userTable.createPrimaryKey(), null) ==
                   numInUsers);
        assertTrue(countTableRecords(addressTable.createPrimaryKey(), null) ==
                   numInAddress);
        assertTrue(countTableRecords(infoTable.createPrimaryKey(), null) ==
                   numInInfo);
        assertTrue(countTableRecords(emailTable.createPrimaryKey(), null) ==
                   numInEmail);

        /*
         * Try some reverse iteration on a nested table.
         */
        pkey = emailTable.createPrimaryKey();
        pkey.put("id", 1);
        pkey.put("addrId", 1);
        iter = tableImpl.tableIterator(pkey, null, reverseOptions);
        numRecords = 0;
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastValue != null) {
                assertTrue(row.get("emailAddress").compareTo(lastValue) < 0);
                lastValue = row.get("emailAddress");
            }
        }
        assertEquals("Unexpected record count", numRows, numRecords);
        iter.close();

        /*
         * Use address table this time with partial primary key
         */
        pkey = addressTable.createPrimaryKey();
        pkey.put("id", 3);
        iter = tableImpl.tableIterator(pkey, null, reverseOptions);
        numRecords = 0;
        lastValue = null;
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastValue != null) {
                assertTrue(row.get("addrId").compareTo(lastValue) < 0);
                lastValue = row.get("addrId");
            }
        }
        assertEquals("Unexpected record count", numRows, numRecords);
        iter.close();

        /*
         * Use user table this time with full primary key but target the
         * address table in addition to the userTable used to create the
         * primary key.
         */
        MultiRowOptions mro =
            userTable.createMultiRowOptions(Arrays.asList("User.Address"),
                                            null);
        pkey = userTable.createPrimaryKey();
        pkey.put("id", 4);
        iter = tableImpl.tableIterator(pkey, mro, reverseOptions);
        numRecords = 0;
        lastValue = null;
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (lastValue != null) {
                assertTrue(row.get("addrId").compareTo(lastValue) < 0);
                lastValue = row.get("addrId");
            }
        }
        assertEquals("Unexpected record count", numRows + 1, numRecords);
        iter.close();

        /*
         * Do some table removal and validate state
         */
        /*
         * This will fail because it has child tables
         */
        removeTable(userTable, false);

        /*
         * Remove tables and data and count.  Use total store records and trust
         * the per-table counts for now.
         */
        removeTable(emailTable);

        /* TODO - CANNOT USE STORE RECORD COUNT...
        assertEquals(
            numInUsers + numInAddress + numInInfo, countStoreRecords());

        removeTable(addressTable);
        assertEquals(numInUsers + numInInfo, countStoreRecords());

        removeTable(infoTable);
        assertEquals(numInUsers, countStoreRecords());

        removeTable(userTable, true);
        assertEquals(0, countStoreRecords());

         */
    }

    /*
     * Do some testing using multiGet, multiDelete in nested tables.
     */
    @Test
    public void multiOperations()
        throws Exception {

        /*
         * Putting this up here makes it easier to selectively comment out
         * test sections.
         */
        PrimaryKey pkey = null;
        List<Row> rows = null;
        List<PrimaryKey> keys = null;
        FieldValue lastValue = null;

        /*
         * Create and populate tables.  See comment above for hierarchy.
         */
        createTables();

        /*
         * Try some reverse iteration on a nested table.
         */
        pkey = emailTable.createPrimaryKey();
        pkey.put("id", 1);
        pkey.put("addrId", 1);
        rows = tableImpl.multiGet(pkey, null, reverseOptions);
        for (Row row : rows) {
            if (lastValue != null) {
                assertTrue(row.get("emailAddress").compareTo(lastValue) < 0);
                lastValue = row.get("emailAddress");
            }
        }
        assertEquals("Unexpected record count", numRows, rows.size());

        /*
         * Return an ancestor table
         */
        MultiRowOptions mro =
            new MultiRowOptions(null,
                                Arrays.asList((Table)userTable),
                                null);

        pkey = emailTable.createPrimaryKey();
        pkey.put("id", 1);
        pkey.put("addrId", 1);
        rows = tableImpl.multiGet(pkey, mro, forwardOptions);
        assertEquals("Unexpected record count", numRows + 1, rows.size());

        /*
         * Add another ancestor, use keys, not rows
         */
        mro.setIncludedParentTables(Arrays.asList((Table)userTable,
                                                  (Table)addressTable));
        keys = tableImpl.multiGetKeys(pkey, mro, forwardOptions);
        assertEquals("Unexpected record count", numRows + 2, keys.size());

        /*
         * Use intermediate table this time, include ancestor, child.  This
         * results in the same count as above but using a different pattern.
         */
        pkey = addressTable.createPrimaryKey();
        pkey.put("id", 1);
        pkey.put("addrId", 1);
        mro.setIncludedParentTables(Arrays.asList((Table)userTable));
        mro.setIncludedChildTables(Arrays.asList((Table)emailTable));
        keys = tableImpl.multiGetKeys(pkey, mro, forwardOptions);
        assertEquals("Unexpected record count", numRows + 2, keys.size());

        /*
         * Test multiDelete with a child table.  Same primary key as above.
         */
        mro.setIncludedParentTables(null);
        /* child tables still includes emailTable */
        int numDeleted = tableImpl.multiDelete(pkey, mro, null);
        assertEquals("Unexpected record count", numRows + 1, numDeleted);

        /*
         * Add ancestor table, but since the target key is gone, nothing
         * should happen.
         */
        mro.setIncludedParentTables(Arrays.asList((Table)userTable));
        numDeleted = tableImpl.multiDelete(pkey, mro, null);
        assertEquals("Unexpected record count", 0, numDeleted);

        /*
         * Delete a single intermediate table (address) record and
         * use it as a target for further multi-operations with ancestors
         * and children.  This will still return the child table records
         * as well as the ancestor (user) record.
         */
        pkey.put("addrId", 2);
        assertTrue(tableImpl.delete(pkey, null, null));
        rows = tableImpl.multiGet(pkey, mro, forwardOptions);
        for (Row row : rows) {
            if (row.getTable().equals(addressTable)) {
                fail("There should be no addressTable records with this id");
            }
        }
        assertEquals("Unexpected record count", numRows + 1, rows.size());

        /*
         * Misc -- test direct getTable of child and make sure that its
         * parent is available and intact.  Use another KVStore and TableAPI
         * instance for this.
         */
        KVStore store1 = KVStoreFactory.getStore(createKVConfig(createStore));
        TableAPIImpl impl = (TableAPIImpl)store1.getTableAPI();
        Table t = impl.getTable("User.Address");
        assertNotNull(t);
        assertNotNull(t.getParent());
        assertTrue(t.getParent().getChildTable("Address").equals(t));
        store1.close();
    }

    /**
     * Test return of ancestor tables from iterator operation.
     */
    @Test
    public void ancestorTableTest()
        throws Exception {

        /*
         * Putting this up here makes it easier to selectively comment out
         * test sections.
         */
        PrimaryKey pkey = null;
        TableIterator<Row> iter = null;
        int numRecords = 0;

        /*
         * Create and populate tables.  See comment above for hierarchy.
         */
        createTables();

        /*
         * Use a partial key to address table (just user id) but specify
         * the user table in the return in addition to the target table.
         */
        MultiRowOptions mro =
            new MultiRowOptions(null,
                                Arrays.asList((Table)userTable),
                                null);

        pkey = addressTable.createPrimaryKey();
        pkey.put("id", 4);
        iter = tableImpl.tableIterator(pkey, mro, forwardOptions);
        numRecords = 0;
        boolean sawUserRecord = false;
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (row.getTable().equals(userTable)) {
                sawUserRecord = true;
            }
        }
        /*
         * The iteration should include the number of rows in the Address table
         * for a single user plus 1, for the User table record.
         */
        assertEquals("Unexpected record count", numRows + 1, numRecords);
        assertTrue(sawUserRecord);
        iter.close();

        /*
         * Do the same iteration, but keys this time, and in reverse.
         */
        TableIterator<PrimaryKey> pkIter =
            tableImpl.tableKeysIterator(pkey, mro, reverseOptions);
        numRecords = 0;
        sawUserRecord = false;
        while (pkIter.hasNext()) {
            ++numRecords;
            PrimaryKey key = pkIter.next();
            if (key.getTable().equals(userTable)) {
                sawUserRecord = true;
            }
        }
        assertEquals("Unexpected record count", numRows + 1, numRecords);
        assertTrue(sawUserRecord);
        pkIter.close();

        /*
         * Use another level of table.  Iterate on the Email table, asking
         * for the User table as an ancestor.
         */
        pkey = emailTable.createPrimaryKey();
        pkey.put("id", 2);
        pkey.put("addrId", 3);
        iter = tableImpl.tableIterator(pkey, mro, forwardOptions);
        numRecords = 0;
        while (iter.hasNext()) {
            ++numRecords;
            iter.next();
        }
        assertEquals("Unexpected record count", numRows + 1, numRecords);
        iter.close();

        /*
         * Same key and table but return both the User and Address table
         * entries, using key iteration.
         */
        mro.setIncludedParentTables(Arrays.asList((Table)userTable,
                                                  (Table)addressTable));
        pkIter = tableImpl.tableKeysIterator(pkey, mro, reverseOptions);
        numRecords = 0;
        sawUserRecord = false;
        boolean sawAddressRecord = false;
        while (pkIter.hasNext()) {
            ++numRecords;
            PrimaryKey key = pkIter.next();
            if (key.getTable().equals(userTable)) {
                sawUserRecord = true;
            }
            if (key.getTable().equals(addressTable)) {
                sawAddressRecord = true;
            }
        }
        assertTrue(sawUserRecord && sawAddressRecord);
        assertEquals("Unexpected record count", numRows + 2, numRecords);
        pkIter.close();

        /*
         * This is the same as above but with a minimal batch size.
         */
        TableIteratorOptions batchOptions =
            new TableIteratorOptions(Direction.REVERSE, null, 0, null,
                                     0, 2);
        pkIter = tableImpl.tableKeysIterator(pkey, mro, batchOptions);
        numRecords = 0;
        sawUserRecord = false;
        sawAddressRecord = false;
        while (pkIter.hasNext()) {
            ++numRecords;
            PrimaryKey key = pkIter.next();
            if (key.getTable().equals(userTable)) {
                sawUserRecord = true;
            }
            if (key.getTable().equals(addressTable)) {
                sawAddressRecord = true;
            }
        }
        assertTrue(sawUserRecord && sawAddressRecord);
        assertEquals("Unexpected record count", numRows + 2, numRecords);
        pkIter.close();

        /*
         * Another case, remove the User table (ancestor) record and ensure
         * that it is not returned and its absence does not cause a problem.
         */
        pkey = userTable.createPrimaryKey();
        pkey.put("id", 5);
        assertTrue(tableImpl.delete(pkey, null, null));

        mro.setIncludedParentTables(Arrays.asList((Table)userTable));
        pkey = addressTable.createPrimaryKey();
        pkey.put("id", 5);
        iter = tableImpl.tableIterator(pkey, mro, forwardOptions);
        numRecords = 0;
        sawUserRecord = false;
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (row.getTable().equals(userTable)) {
                sawUserRecord = true;
            }
        }
        assertTrue(!sawUserRecord);
        assertEquals("Unexpected record count", numRows, numRecords);
        iter.close();

        /*
         * Once again using key iteration, which has a different path.
         */
        pkIter = tableImpl.tableKeysIterator(pkey, mro, forwardOptions);
        numRecords = 0;
        while (pkIter.hasNext()) {
            ++numRecords;
            PrimaryKey key = pkIter.next();
            if (key.getTable().equals(userTable)) {
                sawUserRecord = true;
            }
        }
        assertTrue(!sawUserRecord);
        assertEquals("Unexpected record count", numRows, numRecords);
        pkIter.close();

        /*
         * Iterate on Address table and ask for both ancestor (User) and
         * child (Email) tables.  Use a small batch size and reverse
         * iteration to exercise that code and make sure that redundant
         * User table rows are not returned.
         */
        mro = addressTable.createMultiRowOptions
            (Arrays.asList("User", "User.Address.Email"), null);
        pkey = addressTable.createPrimaryKey();
        pkey.put("id", 4);
        iter = tableImpl.tableIterator(pkey, mro, batchOptions);
        numRecords = 0;
        int userRecordCount = 0;
        while (iter.hasNext()) {
            ++numRecords;
            Row row = iter.next();
            if (row.getTable().equals(userTable)) {
                ++userRecordCount;
            }
        }
        /*
         * The iteration should include:
         *  - the number of rows in the Address table plus
         *  - the number of rows in the Address * numRows (email table) plus
         *  - 1 for the single user
         */
        assertEquals("Unexpected record count", numRows + numRows*numRows + 1,
                     numRecords);
        assertEquals("Unexpected record count", 1, userRecordCount);
        iter.close();

        RowImpl rowAncestor = userTable.createRow();
        RowImpl rowDescender = addressTable.createRow();
        RowImpl rowOther = rowAncestor.clone();
        IntegerValueImpl intValImpl = FieldDefImpl.Constants.integerDef
                .createInteger(1);
        assertEquals(-1, rowAncestor.compareTo(rowDescender));
        assertEquals(0, rowOther.compareTo(rowAncestor));
        assertEquals(1, rowDescender.compareTo(rowAncestor));
        try {
            rowAncestor.compareTo(intValImpl);
            fail("Not having both row in comparison throw exception");
        } catch (IllegalArgumentException e) {
            /* success */
        }
        assertEquals(false, rowAncestor.equals(intValImpl));
        
        TableLimits childTableLimit = new TableLimits(100, 100, 1);
        try {
            addressTable.setTableLimits(childTableLimit);
            fail("Set table limit for child table throw exception");
        } catch (IllegalCommandException e) {
            /* success */
        }
    }

    /**
     * Tests child table limits.
     */
    @Test
    public void testChildTableLimits() throws Exception {
        createUserTable();

        /* Start with 0 limit (no children allowed) */
        TableLimits limits = new TableLimits(NO_LIMIT /* readLimit */,
                                             NO_LIMIT /* writeLimit */,
                                             NO_LIMIT /* sizeLimit */,
                                             NO_LIMIT /* maxIndexes */,
                                             0        /* maxChildren */,
                                             NO_LIMIT /* indexKeySizeLimit */);
        setTableLimits(userTable, limits);

        /* First should fail */
        createAddressTable(userTable, false);

        /* Up the limit and retry */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 NO_LIMIT /* maxIndexes */,
                                 1        /* maxChildren */,
                                 NO_LIMIT /* indexKeySizeLimit */);
        setTableLimits(userTable, limits);

        /* Should work now */
        createAddressTable(userTable, true);

        /* Second child will not */
        createInfoTable(false);

        /* Remove limit and retry */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 NO_LIMIT /* maxIndexes */,
                                 NO_LIMIT /* maxChildren */,
                                 NO_LIMIT /* indexKeySizeLimit */);
        setTableLimits(userTable, limits);

        /* Should now be two children */
        createInfoTable(true);

        /* Attempt to set limit less then number of children should fail */
        limits = new TableLimits(NO_LIMIT /* readLimit */,
                                 NO_LIMIT /* writeLimit */,
                                 NO_LIMIT /* sizeLimit */,
                                 NO_LIMIT /* maxIndexes */,
                                 1        /* maxChildren */,
                                 NO_LIMIT /* indexKeySizeLimit */);
        setTableLimits(userTable, limits, false);
    }
}
