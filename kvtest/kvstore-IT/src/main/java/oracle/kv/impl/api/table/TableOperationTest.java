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
import java.util.List;

import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.TableOperationResult;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test TableOperationFactory, TableOperation, etc.  These tests need to wrk
 * with a specific partion so parent/child tables are used to partition data.
 *
 * Operations:
 *   put, putIfPresent, putIfAbsent, putIfVersion
 *   delete, deleteIfVersion
 */
public class TableOperationTest extends TableTestBase {
    private static final int NUSERS = 50;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        TableTestBase.staticSetUp();
    }

    @Test
    public void testTableOperation()
        throws Exception {

        TableImpl userTable = buildUserTable();
        TableImpl addressTable = buildAddressTable(userTable);
        addUserRows(userTable, NUSERS);
        addAddressRows(addressTable, NUSERS);

        /* Get the home, work, and other address for user 5 */
        PrimaryKey key = addressTable.createPrimaryKey();
        key.put("id", 5);
        List<Row> getList = tableImpl.multiGet(key, null, null);
        assertEquals("Unexpected size", 3, getList.size());

        /*
         * Try deleting the three addresses for id 5 in an operation, then
         * see how many are left.
         */
        List<TableOperation> opList = new ArrayList<TableOperation>();
        for (Row row : getList) {
            TableOperationFactory factory = tableImpl.getTableOperationFactory();
            key.put("type", row.get("type"));
            opList.add(factory.createDelete(key.clone(),
                                            ReturnRow.Choice.ALL,
                                            true));
        }
        List<TableOperationResult> results = tableImpl.execute(opList, null);
        assertEquals("Unexpected size", 3, results.size());
        for (TableOperationResult res : results) {
            /* These are returned for deletes */
            assertNotNull(res.getPreviousRow());
            assertNotNull(res.getPreviousVersion());
            assertTrue(res.getSuccess());
        }

        /*
         * Do the multi-get again
         */
        key.remove("type");
        List<Row> list = tableImpl.multiGet(key, null, null);
        assertEquals("Unexpected size", 0, list.size());

        /*
         * Use the absence to test putIfPresent, putIfAbsent.  The original rows
         * still exist in the getList variable.
         */
        opList.clear();
        for (Row row : getList) {
            TableOperationFactory factory = tableImpl.getTableOperationFactory();
            opList.add(factory.createPutIfPresent(row.clone(), null, false));
        }
        results = tableImpl.execute(opList, null);
        assertEquals("Unexpected result size", 3, results.size());
        for (TableOperationResult res : results) {
            assertNull(res.getNewVersion());
            assertFalse(res.getSuccess());
        }

        /*
         * Use putIfAbsent to put them back.
         */
        opList.clear();
        for (Row row : getList) {
            TableOperationFactory factory = tableImpl.getTableOperationFactory();
            opList.add(factory.createPutIfAbsent(row.clone(),
                                                 ReturnRow.Choice.ALL,
                                                 false));
        }
        results = tableImpl.execute(opList, null);
        assertEquals("Unexpected result size", 3, results.size());
        for (TableOperationResult res : results) {
            assertNotNull(res.getNewVersion());
            assertTrue(res.getSuccess());
            /* there is no previous row */
            assertNull(res.getPreviousRow());
            assertNull(res.getPreviousVersion());
        }

        /*
         * Use putIfVersion to put again.
         */
        opList.clear();
        int i = 0;
        for (TableOperationResult res : results) {
            TableOperationFactory factory = tableImpl.getTableOperationFactory();
            opList.add(factory.createPutIfVersion(getList.get(i++).clone(),
                                                  res.getNewVersion(),
                                                  ReturnRow.Choice.ALL,
                                                  false));
        }
        results = tableImpl.execute(opList, null);
        assertEquals("Unexpected result size", 3, results.size());
        for (TableOperationResult res : results) {
            assertNotNull(res.getNewVersion());
            assertTrue(res.getSuccess());
        }

        /*
         * Check error reporting in TableOpExecuteException. Delete an invalid
         * users -10.
         */
        opList.clear();
        PrimaryKey userKey = userTable.createPrimaryKey();
        TableOperationFactory factory = tableImpl.getTableOperationFactory();
        userKey.put("id", -10);
        opList.add(factory.createDelete(userKey, ReturnRow.Choice.NONE, true));
        try {
            tableImpl.execute(opList, null);
            fail("Should have failed");
        } catch (TableOpExecutionException e) {
            assertEquals(-10,
                         e.getFailedOperation().getPrimaryKey().get("id").
                         asInteger().get());
            assertFalse(e.getFailedOperationResult().getSuccess());
            assertEquals(0, e.getFailedOperationIndex());
        }

        /*
         * Create a list with mixed shard keys, should fail
         */
        opList.clear();
        i = 0;
        for (Row row : getList) {
            factory = tableImpl.getTableOperationFactory();
            Row putRow = row.clone();
            putRow.put("id", i++);
            opList.add(factory.createPut(putRow,
                                         ReturnRow.Choice.ALL,
                                         false));
        }
        try {
            results = tableImpl.execute(opList, null);
            fail("execute should have failed with mixed shard keys");
        } catch (IllegalArgumentException iae) {
            // success
        }
    }

    private TableImpl buildUserTable()
        throws Exception {

        TableImpl table = TableBuilder.createTableBuilder("User")
            .addInteger("id")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .primaryKey("id")
            .buildTable();

        return addTable(table);
    }

    private TableImpl buildAddressTable(TableImpl userTable)
        throws Exception {

        TableImpl table =
            TableBuilder.createTableBuilder("Address", null, userTable)
            .addEnum("type",
                     new String[]{"home", "work", "other"}, null, null, null)
            .addString("street")
            .addString("city")
            .addString("state")
            .addInteger("zip")
            .primaryKey("type")
            .buildTable();

        return addTable(table, true, true);
    }

    private void addUserRows(TableImpl table, int num) {
        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("firstName", ("first" + i));
            row.put("lastName", ("last" + i));
            row.put("age", i+10);
            tableImpl.put(row, null, null);
        }
    }
    private void addAddressRows(TableImpl table, int num) {
        String[] types = new String[]{"home", "work", "other"};
        for (int i = 0; i < num; i++) {
            for (String s : types) {
                Row row = table.createRow();
                row.put("id", i);
                row.put("street", (i + " " + s + " St"));
                row.put("city", "Anywhere");
                row.put("state", "NM");
                row.put("zip", 11100 + i);
                row.putEnum("type", s);
                tableImpl.put(row, null, null);
            }
        }
    }
}
