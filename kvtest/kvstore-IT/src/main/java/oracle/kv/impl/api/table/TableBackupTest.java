/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * This is a catch-all class to test various table cases.
 */
public class TableBackupTest extends TableTestBase {
    private static final int NUSERS = 500;
    private static final int NINTS = 1000;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        //TODO: remove this after snapshot supports MRTables.
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        TableTestBase.staticSetUp();
        //TODO: remove this after snapshot supports erasure.
        modifyJEConfigProperties("je.env.runEraser +true",
            "je.env.runEraser false");
    }


    /**
     * 1.  Build and populate a couple of tables, each of which has indexes.
     * 2.  Take a snapshot
     * 3.  Create a new store, different topology
     * 4.  Load from the snapshot to the store
     * 5.  Validate the content of the new store's metadata and data
     */
    @Test
    public void testBackup()
        throws Exception {

        TableImpl userTable = buildUserTable();
        TableImpl intTable = buildIntTable();

        /*
         * Build a child table just to have a child table in the metadata.  It
         * isn't used directly.
         */
        buildChildTable(userTable);
        addUserRows(userTable, NUSERS, "");
        addIntRows(intTable, NINTS, 0);
        int numRecords = countTableRecords(userTable.createPrimaryKey(), null);
        assertEquals(NUSERS, numRecords);
        numRecords = countTableRecords(intTable.createPrimaryKey(), null);
        assertEquals(NINTS, numRecords);

        /*
         * Wait for store ready so that we know all system tables have
         * been created and populated before taking a snapshot.
         */
        waitForStoreReady(createStore.getAdmin(), true);

        /*
         * Create a snapshot, start a new store, load to the store.
         */
        String snapshotName = createSnapshot("table");

        /* Another test case may close the restore store when tear down */
        if (restoreStore == null) {
            startRestoreStore();
        }

        loadToRestoreStore(snapshotName);
        /*
         * Compare the two stores from the perspective of data and metadata.
         * Table data is not specifically checked, but in the interest of
         * a sanity check, one of the indexes is compared.
         */
        boolean equal = compareStoreTableMetadata(createStore, restoreStore);
        assertTrue(equal);
        equal = compareStoreKVData(store, rsStore);
        assertTrue(equal);

        /*
         * Check the contents of the allint index on the intTable.  There is
         * only one row for each index key for this particular index.
         */
        Index index = intTable.getIndex("allint");

        TableImpl intTable1 = (TableImpl) restoreTableImpl.getTable("Int");
        assertNotNull(intTable1);
        Index index1 = intTable1.getIndex("allint");

        TableIterator<Row> iter =
            tableImpl.tableIterator(index.createIndexKey(), null, null);

        while (iter.hasNext()) {

            /*
             * It's odd to create an IndexKey from a row from another store
             * but the field names match so it is ok.
             */
            @SuppressWarnings("deprecation")
            IndexKey ikey = index1.createIndexKey(iter.next());
            assertTrue(countIndexRecords1(ikey, null) == 1);
        }
        iter.close();
    }


    @Test
    public void testBackupAndDirectRestore()
        throws Exception {
        TableImpl userTable = buildUserTable();
        TableImpl intTable = buildIntTable();

        buildChildTable(userTable);
        addUserRows(userTable, NUSERS, "");
        addIntRows(intTable, NINTS, 0);
        int numRecords = countTableRecords(userTable.createPrimaryKey(), null);
        assertEquals(NUSERS, numRecords);
        numRecords = countTableRecords(intTable.createPrimaryKey(), null);
        assertEquals(NINTS, numRecords);

        /*
         * Wait for store ready so that we know all system tables have
         * been created and populated before taking a snapshot.
         */
        waitForStoreReady(createStore.getAdmin(), true);

        String snapshotName = createSnapshot("table");

        addUserRows(userTable, NUSERS, "new");
        addIntRows(intTable, NINTS, NINTS);
        numRecords = countTableRecords(userTable.createPrimaryKey(), null);
        assertEquals(NUSERS * 2, numRecords);
        numRecords = countTableRecords(intTable.createPrimaryKey(), null);
        assertEquals(NINTS * 2, numRecords);

        createStore.setRestoreSnapshot(snapshotName);
        createStore.restart();

        numRecords = countTableRecords(userTable.createPrimaryKey(), null);
        assertEquals(NUSERS, numRecords);
        numRecords = countTableRecords(intTable.createPrimaryKey(), null);
        assertEquals(NINTS, numRecords);
    }

    private TableImpl buildUserTable()
        throws Exception {

        TableImpl table = TableBuilder.createTableBuilder("User")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addString("address")
            .addBinary("binary", null)
            .primaryKey("lastName", "firstName")
            .shardKey("lastName")
            .buildTable();

        //TODO: allow MRTable mode after MRTable supports child tables.
        table = addTable(table, true, true/*noMRTableMode*/);
        table = addIndex(table, "FirstName",
                         new String[] {"firstName"}, true);
        table = addIndex(table, "Age",
                         new String[] {"age"}, true);
        return table;
    }

    private void addUserRows(TableImpl table, int num, String prefix) {
        for (int i = 0; i < num; i++) {
            byte[] bytes = new byte[0];
            Row row = table.createRow();
            row.put("firstName", (prefix + "first" + i));
            row.put("lastName", (prefix + "last" + i));
            row.put("age", i+10);
            row.put("address", "10 Happy Lane");
            row.put("binary", bytes);
            tableImpl.put(row, null, null);
        }
    }

    private TableImpl buildIntTable()
        throws Exception {
        TableImpl table = TableBuilder.createTableBuilder("Int")
            .addInteger("id")
            .addInteger("int1")
            .addInteger("int2")
            .addInteger("int3")
            .primaryKey("id")
            .buildTable();

        table = addTable(table);
        table = addIndex(table, "int1",
                             new String[] {"int1"}, true);
        table = addIndex(table, "allint",
                         new String[] {"int1", "int2", "int3"}, true);
        return table;
    }

    private void addIntRows(TableImpl table, int num, int addition) {
        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i + addition);
            row.put("int1", i);
            row.put("int2", i*10);
            row.put("int3", i*100);
            tableImpl.put(row, null, null);
        }
    }

    private void buildChildTable(TableImpl parent)
        throws Exception {

        TableImpl table = TableBuilder.createTableBuilder("child", null,
                                                          parent)
            .addInteger("child1")
            .addString("child2")
            .primaryKey("child1")
            .buildTable();
        //TODO: allow MRTable mode after MRTable supports child tables.
        addTable(table, true, true/*noMRTableMode*/);
    }
}
