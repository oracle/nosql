/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.junit.BeforeClass;

public class TableNestingTestBase extends TableTestBase {
    TableImpl userTable;
    TableImpl addressTable;
    TableImpl emailTable;
    TableImpl infoTable;
    TableIteratorOptions reverseOptions =
        new TableIteratorOptions(Direction.REVERSE, Consistency.ABSOLUTE, 0,
                                 null);
    TableIteratorOptions forwardOptions =
        new TableIteratorOptions(Direction.FORWARD, Consistency.ABSOLUTE, 0,
                                 null);
    TableIteratorOptions unorderedOptions =
        new TableIteratorOptions(Direction.UNORDERED, Consistency.ABSOLUTE, 0,
                                 null);
    final static int numUsers = 10;
    final static int numRows = 15;


    @BeforeClass
    public static void staticSetUp() throws Exception {
        TableTestBase.staticSetUp();
    }

    /* Creates the user parent table */
    protected void createUserTable() throws Exception {

        userTable = TableBuilder.createTableBuilder("User",
                                                    "Table of Users",
                                                    null)
                                .addInteger("id")
                                .addString("firstName")
                                .addString("lastName")
                                .addInteger("age")
                                .addFixedBinary("fixed_binary", 10)
                                .primaryKey("id")
                                .shardKey("id")
                                .buildTable();

        addTable(userTable);
    }

    /* Creates the address a child table */
    protected void createAddressTable(TableImpl parent, boolean shouldSucceed)
        throws Exception {
        addressTable =
            TableBuilder.createTableBuilder("Address",
                                            "Table of addresses for users",
                                            parent)
                        .addEnum("type",
                                 new String[]{"home", "work", "other"}, null, null, null)
                        .addString("street")
                        .addString("city")
                        .addString("state")
                        /* make zip nullable */
                        .addInteger("zip", null, true, null)
                        .addInteger("addrId")
                        .primaryKey("addrId")
                        .buildTable();
        addTable(addressTable, shouldSucceed, true);
    }

    /* Creates the info child table */
    protected void createInfoTable(boolean shouldSucceed) throws Exception {
        infoTable =
            TableBuilder.createTableBuilder("Info",
                                            "Table of info",
                                            userTable)
                        .addString("s1")
                        .addString("s2")
                        .addInteger("myid")
                        .primaryKey("myid")
                        .buildTable();
        addTable(infoTable, shouldSucceed, true);
    }

    /* Creates all tables */
    protected void createTables() throws Exception {
        createUserTable();

        TableImpl retrievedTable = getTable("User");
        userTable = retrievedTable;

        /*
         * Add a child table
         */
        createAddressTable(retrievedTable, true);

        /*
         * Get table from metadata and assert equality
         */
        retrievedTable = getTable("User.Address");
        addressTable = retrievedTable;

        /*
         * Add a sibling table to Address called Info.  It has no
         * meaningful fields, it's just a presence.
         */
        createInfoTable(true);

        retrievedTable = getTable("User.Info");
        infoTable.setId(retrievedTable.getId());
        assertEquals(retrievedTable.getNumKeyComponents(), 4);

        /*
         * Add another level of nesting -- a child of addressTable.
         * This is entirely fabricated and not intended to make sense.
         */
        emailTable =
            TableBuilder.createTableBuilder("Email",
                                            "email addresses",
                                            addressTable)
                        .addString("emailAddress")
                        .addEnum("emailType",
                                 new String[]{"home", "work", "other"}, null, null, null)
                        .addString("provider")
                        .primaryKey("emailAddress")
                        .buildTable();
        addTable(emailTable, true, true);

        retrievedTable = getTable("User.Address.Email");
        emailTable.setId(retrievedTable.getId());

        /*
         * re-retrieve major tables to make sure they are current and
         * add indexes to userTable and to addressTable.
         */
        userTable = getTable("User");
        userTable = addIndex(userTable, "Age",
                             new String[] {"age"}, true);

        addressTable = getTable("User.Address");
        addressTable = addIndex(addressTable, "City",
                                new String[] {"city"}, true);

        emailTable = getTable("User.Address.Email");
        infoTable = getTable("User.Info");
        addUserRows(userTable, numUsers);
        addAddressRows(addressTable, numRows);
        addEmailRows(emailTable, numRows);
        addInfoRows(infoTable, numRows);
    }

    protected void removeTable(TableImpl table)
        throws Exception {

        removeTable(table, true);
    }

    protected void addUserRows(TableImpl table, int num) {
        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("firstName", "first");
            row.put("lastName", "last");
            row.put("age", i+10);
            row.putFixed("fixed_binary",
                         new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
            tableImpl.put(row, null, null);
        }
    }

    protected void addAddressRows(TableImpl table, int num) {
        TableIterator<Row> iter =
            tableImpl.tableIterator(userTable.createPrimaryKey(), null,
                                    unorderedOptions);
        while (iter.hasNext()) {
            Row userRow = iter.next();
            for (int i = 0; i < num; i++) {
                Row row = table.createRow();
                row.put("id", userRow.get("id"));
                row.putEnum("type", "home");
                row.put("street", "happy lane");
                /*
                 * Make addrId of 0 be different
                 */
                if (i == 0) {
                    row.put("city", "Whoville");
                } else {
                    row.put("city", "Smallville");
                }
                row.put("zip", i);
                row.put("state", "MT");
                row.put("addrId", i);
                tableImpl.put(row, null, null);
            }
        }
        iter.close();
    }

    protected void addInfoRows(TableImpl table, int num) {
        TableIterator<Row> iter =
            tableImpl.tableIterator(userTable.createPrimaryKey(), null, null);
        while (iter.hasNext()) {
            Row userRow = iter.next();
            for (int i = 0; i < num; i++) {
                Row row = table.createRow();
                row.put("id", userRow.get("id"));
                row.put("myid", i);
                row.put("s1", "s1");
                row.put("s2", "s2");
                tableImpl.put(row, null, null);
            }
        }
        iter.close();
    }

    protected void addEmailRows(TableImpl table, int num) {
        TableIterator<Row> iter =
            tableImpl.tableIterator(addressTable.createPrimaryKey(), null, null);
        while (iter.hasNext()) {
            Row addrRow = iter.next();
            for (int i = 0; i < num; i++) {
                Row row = table.createRow();
                row.put("id", addrRow.get("id"));
                row.put("addrId", addrRow.get("addrId"));
                row.putEnum("emailType", "work");
                row.put("provider", "myprovider");
                String s = "joe" + i + "@myprovider.com";
                row.put("emailAddress", s);
                tableImpl.put(row, null, null);
            }
        }
        iter.close();
    }
}
