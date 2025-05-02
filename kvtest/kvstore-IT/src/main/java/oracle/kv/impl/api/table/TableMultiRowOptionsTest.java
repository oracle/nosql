/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;

import org.junit.BeforeClass;
import org.junit.Test;

public class TableMultiRowOptionsTest extends TableTestBase {

    private static final TableIteratorOptions reverseOptions =
        new TableIteratorOptions(Direction.REVERSE, Consistency.ABSOLUTE, 0,
                                 null);
    private static final TableIteratorOptions forwardOptions =
        new TableIteratorOptions(Direction.FORWARD, Consistency.ABSOLUTE, 0,
                                 null);
    private static final TableIteratorOptions unorderedOptions =
        new TableIteratorOptions(Direction.UNORDERED, Consistency.ABSOLUTE, 0,
                                 null);

    private static final String create_user_table =
        "CREATE TABLE %s (" +
        "id INTEGER, " +
        "firstName String, " +
        "lastName String, " +
        "age INTEGER, " +
        "fixed_binary BINARY(10), " +
        "PRIMARY KEY(SHARD(id)))";
    private static final String create_address_table =
        "CREATE TABLE %s (" +
        "type ENUM(home, work, other), " +
        "street STRING, " +
        "city STRING, " +
        "state STRING, " +
        "zip INTEGER, " +
        "addrId INTEGER, " +
        "PRIMARY KEY(addrId))";
    private static final String create_info_table =
        "CREATE TABLE %s (" +
        "s1 STRING, " +
        "s2 STRING, " +
        "myid INTEGER, " +
        "PRIMARY KEY(myid))";
    private static final String create_email_table =
        "CREATE TABLE %s (" +
        "emailAddress STRING, " +
        "emailType ENUM(home, work, other), " +
        "provider STRING, " +
        "PRIMARY KEY(emailAddress))";

    private static final int numUsers = 40;
    private static final int numAddresses = 10;
    private static final int numEmails = 15;
    private static final int numInfo = 20;
    private static final String testNs = "MultiRowTestNs";
    private String ns;
    private Table userTable;
    private Table addressTable;
    private Table emailTable;
    private Table infoTable;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        TableTestBase.staticSetUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (ns != null) {
            executeDdl("DROP NAMESPACE IF EXISTS " + ns + " CASCADE");
        }
    }

    @Test
    public void testMultiGet() throws Exception {
        ns = null;
        multiGetTest();
    }

    /**
     * Test multiGet with MultiRowOptions that includes namespace qualified
     * tables
     */
    @Test
    public void testMultiGetWithNs() throws Exception {
        ns = testNs;
        executeDdl("CREATE NAMESPACE IF NOT EXISTS " + ns);
        multiGetTest();
    }

    @Test
    public void testMultiDelete() throws Exception {
        ns = null;
        multiDeleteTest();
    }

    /**
     * Test multiDelete with MultiRowOptions that includes namespace qualified
     * tables
     */
    @Test
    public void testMultiDeleteWithNs() throws Exception {
        ns = testNs;
        executeDdl("CREATE NAMESPACE IF NOT EXISTS " + ns);
        multiDeleteTest();
    }

    private void multiGetTest() throws Exception {

        createTables();

        /**
         * Case 1:
         * "User" + child table "User.Address"
         */
        PrimaryKey pk = userTable.createPrimaryKey();
        pk.put("id", 1);
        MultiRowOptions mro = userTable.createMultiRowOptions(
            Arrays.asList("User.Address"), null);
        List<Row> rows = tableImpl.multiGet(pk, mro, null);
        int expect = numAddresses + 1;
        assertEquals("Unexpected record count", expect, rows.size());

        /* test with a namespace qualified table name */
        if (ns != null) {
            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Address"), null);
            rows = tableImpl.multiGet(pk, mro, forwardOptions);
            assertEquals("Unexpected record count", expect, rows.size());

            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns.toUpperCase() + ":" +
                              "User.Address".toLowerCase()),
                null);
            rows = tableImpl.multiGet(pk, mro, forwardOptions);
            assertEquals("Unexpected record count", expect, rows.size());
        }

        /**
         * Case 2:
         * "User" + child table "User.Info"
         */
        pk = userTable.createPrimaryKey();
        pk.put("id", 2);
        mro = userTable.createMultiRowOptions(
            Arrays.asList("User.Info"), null);
        rows = tableImpl.multiGet(pk, mro, null);
        expect = numInfo + 1;
        assertEquals("Unexpected record count", expect, rows.size());

        if (ns != null) {
            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Info"), null);
            rows = tableImpl.multiGet(pk, mro, forwardOptions);
            assertEquals("Unexpected record count", expect, rows.size());

            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Info".toUpperCase()), null);
            rows = tableImpl.multiGet(pk, mro, forwardOptions);
            assertEquals("Unexpected record count", expect, rows.size());
        }

        /**
         * Case 3:
         * "User" + descendant tables "User.Address" and "User.Address.Email"
         */
        pk = userTable.createPrimaryKey();
        pk.put("id", 3);
        mro = userTable.createMultiRowOptions(
            Arrays.asList("User.Address", "User.Address.Email"), null);
        rows = tableImpl.multiGet(pk, mro, null);
        expect = numAddresses * numEmails + numAddresses + 1;
        assertEquals("Unexpected record count", expect, rows.size());

        if (ns != null) {
            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Address",
                              ns + ":" + "User.Address.Email"),
                null);
            rows = tableImpl.multiGet(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows.size());

            /* namespace qualified/unqualified table name */
            mro = userTable.createMultiRowOptions(
                Arrays.asList("User.Address",
                              ns + ":" + "User.Address.Email"),
                null);
            rows = tableImpl.multiGet(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows.size());

            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns.toUpperCase() + ":" + "User.Address",
                              "User.Address.Email"),
                null);
            rows = tableImpl.multiGet(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows.size());
        }

        /**
         * Case 4:
         * "User.Address.Email" + ancestor table "User"
         */
        pk = emailTable.createPrimaryKey();
        pk.put("id", 4);
        pk.put("addrId", 1);
        mro = emailTable.createMultiRowOptions(Arrays.asList("User"), null);
        rows = tableImpl.multiGet(pk, mro, null);
        expect = numEmails + 1;
        assertEquals("Unexpected record count", expect, rows.size());

        if (ns != null) {
            mro = emailTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User"), null);
            rows = tableImpl.multiGet(pk, mro, forwardOptions);
            assertEquals("Unexpected record count", expect, rows.size());
        }

        /**
         * Case 5:
         * "User.Address.Email" + ancestor tables "User.Address" and "User"
         */
        pk = emailTable.createPrimaryKey();
        pk.put("id", 5);
        pk.put("addrId", 1);
        mro = emailTable.createMultiRowOptions(
            Arrays.asList("User.Address", "User"), null);
        rows = tableImpl.multiGet(pk, mro, reverseOptions);
        expect = numEmails + 2;
        assertEquals("Unexpected record count", expect, rows.size());

        if (ns != null) {
            mro = emailTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Address",
                              ns + ":" + "User"),
                null);
            rows = tableImpl.multiGet(pk, mro, forwardOptions);
            assertEquals("Unexpected record count", expect, rows.size());

            mro = emailTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Address",
                              "User"),
                null);
            rows = tableImpl.multiGet(pk, mro, reverseOptions);
            assertEquals("Unexpected record count", expect, rows.size());

            mro = emailTable.createMultiRowOptions(
                Arrays.asList("User.Address",
                              ns.toUpperCase() + ":" + "User"),
                null);
            rows = tableImpl.multiGet(pk, mro, reverseOptions);
            assertEquals("Unexpected record count", expect, rows.size());
        }

        /**
         * Case 6:
         * "User.Address" +
         * ancestor table "User" and descendant table "User.Address.Email"
         */
        pk = addressTable.createPrimaryKey();
        pk.put("id", 6);
        pk.put("addrId", 1);
        mro = addressTable.createMultiRowOptions(
            Arrays.asList("User",
                          "User.Address.Email"),
            null);
        rows = tableImpl.multiGet(pk, mro, reverseOptions);
        expect = numEmails + 2;
        assertEquals("Unexpected record count", expect, rows.size());

        if (ns != null) {
            mro = addressTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User",
                              ns + ":" + "User.Address.Email"),
                null);
            rows = tableImpl.multiGet(pk, mro, reverseOptions);
            assertEquals("Unexpected record count", expect, rows.size());

            mro = addressTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User",
                              "User.Address.Email"),
                null);
            rows = tableImpl.multiGet(pk, mro, reverseOptions);
            assertEquals("Unexpected record count", expect, rows.size());

            mro = addressTable.createMultiRowOptions(
                Arrays.asList("User",
                              ns.toUpperCase() + ":" + "User.Address.Email"),
                null);
            rows = tableImpl.multiGet(pk, mro, reverseOptions);
            assertEquals("Unexpected record count", expect, rows.size());
        }
    }

    private void multiDeleteTest() throws Exception {

        createTables();

        int id = 1;
        /**
         * Case 1:
         * "User" + child table "User.Address"
         */
        PrimaryKey pk = userTable.createPrimaryKey();
        pk.put("id", id++);
        MultiRowOptions mro = userTable.createMultiRowOptions(
            Arrays.asList("User.Address"), null);
        int rows = tableImpl.multiDelete(pk, mro, null);
        int expect = numAddresses + 1;
        assertEquals("Unexpected record count", expect, rows);

        /* test with a namespace qualified table name */
        if (ns != null) {
            pk = userTable.createPrimaryKey();
            pk.put("id", id++);
            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Address"), null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);

            pk = userTable.createPrimaryKey();
            pk.put("id", id++);
            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Address".toUpperCase()), null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);
        }

        /**
         * Case 2:
         * "User" + child table "User.Info"
         */
        pk = userTable.createPrimaryKey();
        pk.put("id", id++);
        mro = userTable.createMultiRowOptions(
            Arrays.asList("User.Info"), null);
        rows = tableImpl.multiDelete(pk, mro, null);
        expect = numInfo + 1;
        assertEquals("Unexpected record count", expect, rows);

        if (ns != null) {
            pk = userTable.createPrimaryKey();
            pk.put("id", id++);
            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Info"), null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);

            pk = userTable.createPrimaryKey();
            pk.put("id", id++);
            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns.toLowerCase() + ":" +
                              "User.Info".toUpperCase()),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);
        }

        /**
         * Case 3:
         * "User" + descendant tables "User.Address" and "User.Address.Email"
         */
        pk = userTable.createPrimaryKey();
        pk.put("id", id++);
        mro = userTable.createMultiRowOptions(
            Arrays.asList("User.Address", "User.Address.Email"), null);
        rows = tableImpl.multiDelete(pk, mro, null);
        expect = numAddresses * numEmails + numAddresses + 1;
        assertEquals("Unexpected record count", expect, rows);

        if (ns != null) {
            pk = userTable.createPrimaryKey();
            pk.put("id", id++);
            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Address",
                              ns + ":" + "User.Address.Email"),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);

            pk = userTable.createPrimaryKey();
            pk.put("id", id++);
            /* namespace qualified/unqualified table name */
            mro = userTable.createMultiRowOptions(
                Arrays.asList("User.Address",
                              ns + ":" + "User.Address.Email"),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);

            pk = userTable.createPrimaryKey();
            pk.put("id", id++);
            mro = userTable.createMultiRowOptions(
                Arrays.asList(ns.toUpperCase() + ":" + "User.Address",
                              "User.Address.Email"),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);
        }

        /**
         * Case 4:
         * "User.Address.Email" + ancestor table "User"
         */
        pk = emailTable.createPrimaryKey();
        pk.put("id", id++);
        pk.put("addrId", 1);
        mro = emailTable.createMultiRowOptions(Arrays.asList("User"), null);
        rows = tableImpl.multiDelete(pk, mro, null);
        expect = numEmails + 1;
        assertEquals("Unexpected record count", expect, rows);

        if (ns != null) {
            pk = emailTable.createPrimaryKey();
            pk.put("id", id++);
            pk.put("addrId", 1);
            mro = emailTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User"), null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);
        }

        /**
         * Case 5:
         * "User.Address.Email" + ancestor tables "User.Address" and "User"
         */
        pk = emailTable.createPrimaryKey();
        pk.put("id", id++);
        pk.put("addrId", 1);
        mro = emailTable.createMultiRowOptions(
            Arrays.asList("User.Address", "User"), null);
        rows = tableImpl.multiDelete(pk, mro, null);
        expect = numEmails + 2;
        assertEquals("Unexpected record count", expect, rows);

        if (ns != null) {
            pk = emailTable.createPrimaryKey();
            pk.put("id", id++);
            pk.put("addrId", 1);
            mro = emailTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Address",
                              ns + ":" + "User"),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);

            pk = emailTable.createPrimaryKey();
            pk.put("id", id++);
            pk.put("addrId", 1);
            mro = emailTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User.Address",
                              "User"),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);

            pk = emailTable.createPrimaryKey();
            pk.put("id", id++);
            pk.put("addrId", 1);
            mro = emailTable.createMultiRowOptions(
                Arrays.asList("User.Address",
                              ns.toUpperCase() + ":" + "User"),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);
        }

        /**
         * Case 6:
         * "User.Address" +
         * ancestor table "User" and descendant table "User.Address.Email"
         */
        pk = addressTable.createPrimaryKey();
        pk.put("id", id++);
        pk.put("addrId", 1);
        mro = addressTable.createMultiRowOptions(
            Arrays.asList("User",
                          "User.Address.Email"),
            null);
        rows = tableImpl.multiDelete(pk, mro, null);
        expect = numEmails + 2;
        assertEquals("Unexpected record count", expect, rows);

        if (ns != null) {
            pk = addressTable.createPrimaryKey();
            pk.put("id", id++);
            pk.put("addrId", 1);
            mro = addressTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User",
                              ns + ":" + "User.Address.Email"),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);

            pk = addressTable.createPrimaryKey();
            pk.put("id", id++);
            pk.put("addrId", 1);
            mro = addressTable.createMultiRowOptions(
                Arrays.asList(ns + ":" + "User",
                              "User.Address.Email"),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);

            pk = addressTable.createPrimaryKey();
            pk.put("id", id++);
            pk.put("addrId", 1);
            mro = addressTable.createMultiRowOptions(
                Arrays.asList("User",
                              ns.toUpperCase() + ":" + "User.Address.Email"),
                null);
            rows = tableImpl.multiDelete(pk, mro, null);
            assertEquals("Unexpected record count", expect, rows);
        }
    }

    private void createTables() throws Exception {
        final String user =
            NameUtils.makeQualifiedName(ns, "User");
        final String address =
            NameUtils.makeQualifiedName(ns, "User.Address");
        final String email =
            NameUtils.makeQualifiedName(ns, "User.Address.Email");
        final String info =
            NameUtils.makeQualifiedName(ns, "User.Info");
        executeDdl(String.format(create_user_table, user));
        executeDdl(String.format(create_address_table, address), true, true);
        executeDdl(String.format(create_info_table, info), true, true);
        executeDdl(String.format(create_email_table, email), true, true);

        userTable = tableImpl.getTable(user);
        addressTable = tableImpl.getTable(address);
        infoTable = tableImpl.getTable(info);
        emailTable = tableImpl.getTable(email);

        addUserRows(userTable, numUsers);
        addAddressRows(addressTable, numAddresses);
        addEmailRows(emailTable, numEmails);
        addInfoRows(infoTable, numInfo);
    }

    private void addUserRows(Table table, int num) {
        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("firstName", "first");
            row.put("lastName", "last");
            row.put("age", i + 10);
            row.putFixed("fixed_binary",
                         new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
            tableImpl.put(row, null, null);
        }
    }

    private void addAddressRows(Table table, int num) {
        final TableIterator<Row> iter =
            tableImpl.tableIterator(userTable.createPrimaryKey(),
                                    null,
                                    unorderedOptions);
        while (iter.hasNext()) {
            final Row userRow = iter.next();
            for (int i = 0; i < num; i++) {
                Row row = table.createRow();
                row.put("id", userRow.get("id"));
                row.putEnum("type", "home");
                row.put("street", "happy lane");
                /* Make addrId of 0 be different */
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

    private void addInfoRows(Table table, int num) {
        final TableIterator<Row> iter =
            tableImpl.tableIterator(userTable.createPrimaryKey(),
                                    null,
                                    forwardOptions);
        while (iter.hasNext()) {
            final Row userRow = iter.next();
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

    private void addEmailRows(Table table, int num) {
        final TableIterator<Row> iter =
            tableImpl.tableIterator(addressTable.createPrimaryKey(),
                                    null,
                                    reverseOptions);
        while (iter.hasNext()) {
            final Row addrRow = iter.next();
            for (int i = 0; i < num; i++) {
                Row row = table.createRow();
                row.put("id", addrRow.get("id"));
                row.put("addrId", addrRow.get("addrId"));
                row.putEnum("emailType", "work");
                row.put("provider", "myprovider");
                final String s = "joe" + i + "@myprovider.com";
                row.put("emailAddress", s);
                tableImpl.put(row, null, null);
            }
        }
        iter.close();
    }
}
