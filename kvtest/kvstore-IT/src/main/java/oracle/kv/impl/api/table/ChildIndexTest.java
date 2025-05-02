/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test some cases of indexes on child tables.
 * This test should be expanded.  It's very simple at this point.
 */
public class ChildIndexTest extends TableTestBase {
    TableImpl userTable;
    TableImpl addressTable;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        TableTestBase.staticSetUp();
    }

    /**
     * Create a table, child table, add an index, insert a few records.
     */
    @Test
    public void simpleIndex() throws Exception {
        buildTables();
        addAddressRow(1, 1, "happy lane", "whoville");
        addAddressRow(2, 2, "elm street", "chicago");

        Index index = addressTable.getIndex("City");
        IndexKey key = index.createIndexKey();
        TableIterator<Row> iter = tableImpl.tableIterator(key, null, null);
        int i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        int expected = countTableRecords(addressTable.createPrimaryKey(),
                                         addressTable);
        assertEquals("Unexpected count", expected, i);
        iter.close();

        /* Iterate again with a restricted key */
        key.put("city", "chicago");
        iter = tableImpl.tableIterator(key, null, null);
        i = 0;
        while (iter.hasNext()) {
            iter.next();
            ++i;
        }
        assertEquals("Unexpected count", 1, i);
        iter.close();
    }

    private void addAddressRow(int userId,
                               int addrId,
                               String street,
                               String city) {
        Row row = addressTable.createRow();
        row.put("id", userId);
        row.put("addrId", addrId);
        row.put("street", street);
        row.put("city", city);
        tableImpl.put(row, null, null);
    }

    private void buildTables() {
        try {
            TableBuilder builder = (TableBuilder)
                TableBuilder.createTableBuilder(null, "User", null, null, null)
                .addInteger("id")
                .addString("lastName")
                .addInteger("age")
                .addString("firstName")
                .primaryKey("id");
            addTable(builder, true);
            userTable = getTable("User");
            assertNotNull(userTable);

            builder = (TableBuilder)
                TableBuilder.createTableBuilder(null, "Address", null,
                                                userTable, null)
                .addInteger("addrId")
                .addString("street")
                .addString("city")
                .primaryKey("addrId");
            addTable(builder, true, true);
            addressTable = getTable("User.Address");
            assertNotNull(addressTable);
            userTable = getTable("User");
            addressTable = addIndex(addressTable, "City",
                                    new String[] {"city"}, true);

            /* for coverage */
            userTable.toJsonString(true);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add table: " + e, e);
        }
    }
}
