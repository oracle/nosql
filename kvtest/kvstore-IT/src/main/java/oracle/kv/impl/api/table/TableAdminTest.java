/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.table.Index;

import org.junit.Test;

/*
 * Exercise administrative operations on tables and indexes, primarily creation
 * and removal.  TableEvolveTest exercises evolution.
 * TODO:
 *  o test parallel ops and idempotent plans.  May need hooks for this.
 *  o test table/index add where a competing add wins and tables/indexes are
 *  the same, as well as NOT the same.
 *  o make certain to test exceptional conditions heavily.
 * T
 */
public class TableAdminTest extends TableTestBase {

    @Test
    public void createRemoveTable()
        throws Exception {

        TableImpl userTable = buildUserTable();
        assertTableExists(userTable, true);

        /*
         * Try to create it again.  This should fail with an ICE.
         */
        addTable(userTable, false);

        /*
         * Restart...
         */

        restartStore();

        Thread.sleep(10000);

        assertNotNull(tableImpl);
        userTable = getTable("User");
        assertNotNull(userTable);

        removeTable(userTable, true);
        assertTableExists(userTable, false);

        /*
         * Try removing the table again.  This will fail
         */
        removeTable(userTable, false);
    }

    /* Run the test using the old RN APIs to get tables. */
    @Test
    public void createRemoveIndexLegacy() throws Exception {
        runCreateRemoveIndex(false);
    }

    /* Run the test using the table MD system table to get tables. */
    @Test
    public void createRemoveIndex() throws Exception {
        runCreateRemoveIndex(true);
    }

    private void runCreateRemoveIndex(boolean useMDSysTable) throws Exception {

        tableImpl.setEnableTableMDSysTable(useMDSysTable);
        waitForStoreReady(createStore.getAdmin(), useMDSysTable);

        TableImpl userTable = buildUserTable();
        assertTableExists(userTable, true);

        addIndex(userTable, "FirstName",
                 new String[] {"firstName"}, true);
        assertIndexExists(userTable, "FirstName", true);

        /* add again, this should fail */
        addIndex(userTable, "FirstName",
                 new String[] {"firstName"}, false);

        /* remove */
        removeIndex(userTable, "FirstName", true);
        assertIndexExists(userTable, "FirstName", false);

        /* remove again.  this will fail */
        removeIndex(userTable, "FirstName", false);

        /* re-add the same index */
        addIndex(userTable, "FirstName",
                 new String[] {"firstName"}, true);
        assertIndexExists(userTable, "FirstName", true);

        /* add an index on 2 fields -- a key and non-key */
        addIndex(userTable, "id_first",
                 new String[] {"id", "firstName"}, true);
        assertIndexExists(userTable, "id_first", true);

        /* attempt to add index on non-existent field */
        addIndex(userTable, "fail",
                 new String[] {"firstName", "nope"}, false);
        assertIndexExists(userTable, "fail", false);

        /* add a duplicate index under a different index name -- should fail */
        addIndex(userTable, "FirstName_duplicate",
                 new String[] {"firstName"}, false);
        assertIndexExists(userTable, "FirstName_duplicate", false);

        /* add an illegal field to an index, this should fail */
        addIndex(userTable, "map",
                 new String[] {"this_is_a_map"}, false);
        assertIndexExists(userTable, "map", false);

        /* try to add an index with null fields */
        addIndex(userTable, "fail",
                 new String[1], false);
        assertIndexExists(userTable, "fail", false);

        /* try to add an index with no fields */
        addIndex(userTable, "fail",
                 new String[0], false);
        assertIndexExists(userTable, "fail", false);

        /* Table has indexes but those are automatically removed */
        removeTable(userTable, true);
    }

    private TableImpl buildUserTable() {
        try {
            TableBuilder builder = (TableBuilder)
                TableBuilder.createTableBuilder("User",
                                                "Table of Users",
                                                null)
                .addInteger("id")
                .addString("firstName")
                .addString("lastName")
                .addInteger("age")
                .addEnum("type",
                         new String[]{"home", "work", "other"},
                         null, null, null)
                .addField("this_is_a_map", TableBuilder.createMapBuilder()
                          .addInteger(null).build())
                .primaryKey("id")
                .shardKey("id");
            addTable(builder, true);
            return getTable("User");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add table: " + e, e);
        }
    }

    private void assertTableExists(String tableName,
                                   boolean exists) {
        TableImpl table = getTable(tableName);
        assertTrue (exists == (table != null));
    }

    private void assertTableExists(TableImpl table, boolean exists) {
        assertTableExists(table.getFullName(), exists);
    }

    private void assertIndexExists(TableImpl table, String indexName,
                                   boolean exists) {
        table = getTable(table.getFullName());
        assertTableExists(table, true);
        Index index = table.getIndex(indexName);
        assertTrue(exists == (index != null));
    }
}
