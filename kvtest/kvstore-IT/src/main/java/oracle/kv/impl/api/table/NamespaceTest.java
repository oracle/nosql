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

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import oracle.kv.StatementResult;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.junit.Test;

/**
 * Exercises the namespace feature of tables
 */
public class NamespaceTest extends TableTestBase {
    /*
     * Add some non-standard characters to make sure they work
     */
    static final String ns0 = "MY-NS0";
    static final String ns1 = "T@MYNS1";
    static final String foo0 = "MY-NS0:foo";
    static final String foo1 = "T@MYNS1:foo";
    static final String createFoo ="create table foo(id integer, name string," +
        "primary key(id))";
    static final String createFooChild = "create table foo.child(id1 integer," +
        "age integer, primary key(id1))";
    static final String createFooChildIndex =
        "create index idx on foo.child(age)";
    static final String dropFooChildIndex = "drop index idx on foo.child";

    static final String dropFooChild = "drop table foo.child";
    static final String dropFoo = "drop table foo";

    /*
     * Basic cases:
     *  o tables with same name in different namespaces
     *  o test alter and drop on them
     */

    /* Run the test using the old RN APIs to get tables. */
    @Test
    public void testSimpleNamespaceLegacy() throws Exception {
        runSimpleNamespace(false);
    }

    /* Run the test using the table MD system table to get tables. */
    @Test
    public void testSimpleNamespace() throws Exception {
        runSimpleNamespace(true);
    }

    private void runSimpleNamespace(boolean useMDSysTable) {
        tableImpl.setEnableTableMDSysTable(useMDSysTable);
        waitForStoreReady(createStore.getAdmin(), useMDSysTable);

        /* create the same table name in 3 namespaces */
        executeDdl(createFoo, ns0);
        executeDdl(createFoo, ns1);
        executeDdl(createFoo, null);

        TableImpl table = getTable(null, "foo");
        assertNotNull(table);

        TableImpl table0 = getTable(ns0, "foo");
        assertNotNull(table0);

        TableImpl table1 = getTable(ns1, "foo");
        assertNotNull(table1);

        /* test getTable() APIs */
        final String sysNs = TableAPI.SYSDEFAULT_NAMESPACE_NAME;
        assertNotNull(tableImpl.getTable("foo"));
        assertNotNull(tableImpl.getTable(sysNs, "foo"));
        assertNotNull(tableImpl.getTable(sysNs + ":" + "foo"));
        assertNotNull(
            tableImpl.getTable(sysNs.toUpperCase(Locale.ENGLISH), "foo"));
        assertNotNull(tableImpl.getTable(ns0 + ":" + "foo"));

        /* null and empty namespace is equlivent to single argument version */
        assertNotNull(tableImpl.getTable(null, ns0 + ":" + "foo"));
        assertNotNull(tableImpl.getTable("", ns0 + ":" + "foo"));

        assertNotNull(
            tableImpl.getTable(ns0.toLowerCase(Locale.ENGLISH) + ":" + "foo"));

        assertNull(tableImpl.getTable(sysNs, sysNs + ":" + "foo"));
        assertNull(tableImpl.getTable(ns0, ns0 + ":" + "foo"));
        assertNull(tableImpl.getTable("non_exist_ns:foo"));
        assertNull(tableImpl.getTable("non_exist_ns", "foo"));
        assertNull(tableImpl.getTable(" :foo"));
        assertNull(tableImpl.getTable(" ", "foo"));

        /* test listNamespaces() APIs */
        final Set<String> namespaces = tableImpl.listNamespaces();
        assertTrue(namespaces.contains(sysNs));
        assertFalse(namespaces.contains("non_exist_ns"));
        /* ns0 and ns1 are not created explicitly */
        assertFalse(namespaces.contains(ns0));
        assertFalse(namespaces.contains(ns1));

        /* add different fields to each */
        executeDdl("alter table foo(add nons string)", null);
        executeDdl("alter table foo(add ns0 string)", ns0);
        executeDdl("alter table foo(add ns1 string)", ns1);

        /* validate new field existence */
        table = getTable(null, "foo");
        assertTrue(table.getFields().contains("nons"));
        assertFalse(table.getFields().contains("ns0"));
        table0 = getTable(ns0, "foo");
        assertTrue(table0.getFields().contains("ns0"));
        assertFalse(table0.getFields().contains("nons"));
        table1 = getTable(ns1, "foo");
        assertTrue(table1.getFields().contains("ns1"));
        assertFalse(table1.getFields().contains("ns0"));

        /*
         * getTables() without a namespace returns all tables and the
         * tables in namespaces will be namespace-prefixed, e.g. MY-NS0:foo.
         */
        Map<String, Table> tables = tableImpl.getTables();
        assertTrue(tables.get("foo") != null);
        assertTrue(tables.get(foo0) != null);
        assertTrue(tables.get(foo1) != null);

        tables = tableImpl.getTables("sysdefault");
        assertTrue(tables.size() >= 1);
        assertTrue(tables.get("foo") != null);
        assertEquals("sysdefault", tables.get("foo").getNamespace());
        for(Map.Entry<String, Table> entry : tables.entrySet()) {
            assertFalse(entry.getKey().contains(":"));
            assertEquals("sysdefault", entry.getValue().getNamespace());
        }

        testListTable(null, "foo");
        testListTable(ns0, "foo");
        testListTable(ns1, "foo");

        /* test case-insensitivity */
        testListTable("my-ns0", "foo");
        testListTable("t@myns1", "foo");

        executeDdl("drop table foo", ns0);
        assertNull(getTable(ns0, "foo"));
    }

    /*
     * Indexes in namespaced tables
     */
    @Test
    public void testIndexesAndQueries() throws Exception {

        //TODO: allow MRTable mode after MRTable supports child tables.
        executeDdl(createFoo, ns0, true, true/*noMRTableMode*/);
        executeDdl(createFooChild, ns0, true, true/*noMRTableMode*/);
        executeDdl(createFooChildIndex, ns0, true, true/*noMRTableMode*/);

        TableImpl table = getTable(ns0, "foo");
        TableImpl childTable = getTable(ns0, "foo.child");
        assertNotNull(table);
        assertNotNull(childTable);

        /* validate namespace */
        assertTrue(table.getInternalNamespace().equalsIgnoreCase(ns0));
        assertTrue(childTable.getInternalNamespace().equalsIgnoreCase(ns0));

        /*
         * Add data
         */
        for (int i = 0; i < 10000; i++) {
            Row row = table.createRow();
            Row childRow = childTable.createRow();
            row.put("id", i)
                .put("name", ("some_name" + Integer.toString(i)));
            childRow.put("id", i)
                .put("id1", i)
                .put("age", i+10);
            assertNotNull(tableImpl.put(row, null, null));
            assertNotNull(tableImpl.put(childRow, null, null));
        }

        /*
         * Try some queries
         */
        final String selFromFoo = "select id from foo";
        final String selFromFooChild = "select id1 from foo.child where " +
            "age > 10 and age < 100";
        ExecuteOptions options = new ExecuteOptions().setNamespace(ns0, false);
        PreparedStatement ps = store.prepare(selFromFoo, options);
        assertNumResults(ps, options, 10000);
        /*
         * validate that the namespace is already in the prepared query and
         * has no effect during execution
         */
        assertNumResults(ps, null, 10000);

        /*
         * Use an indexed query on the child table
         */
        ps = store.prepare(selFromFooChild, options);
        assertNumResults(ps, null, 89);

        /*
         * query will fail without a namespace
         */
        try {
            ps = store.prepare(selFromFoo);
            fail("prepare should have failed -- table doesn't exist");
        } catch (IllegalArgumentException iae) {
            /* success */
        }

        /*
         * Validate state after dropping an index
         */
        executeDdl(dropFooChildIndex, ns0);
        table = getTable(ns0, "foo.child");
        assertNotNull(table);
        assertNull(table.getIndex("idx"));
    }

    /**
     * Test invalid cases and a few valid edge cases. Namespaces must:
     * o contain only alphanumerics plus "." plus "_"
     * o start with letter
     * o be < 128 chars
     *
     * Namespaces are case-insensitive.
     */
    @Test
    public void testNamespaceNames() throws Exception {
        executeDdl(createFoo, "*x", false);
        executeDdl(createFoo, "x&y", false);
        executeDdl(createFoo, "x y", false);
        executeDdl(createFoo, "_zzz", false);
        executeDdl(createFoo, "1x", false);
        executeDdl(createFoo, createLongName('x', 129), false);

        /* "." is allowed, and namespace limit is 128 chars (for now) */
        executeDdl(createFoo, "x.y.z", true);
        executeDdl(createFoo, createLongName('x', 128), true);
    }

    /**
     * Test case insensitivity of Namespace.
     */
    @Test
    public void testNamespaceCaseInsenstivity() {
        String nsLw = ns0.toLowerCase();
        String nsUp = ns0.toUpperCase();

        //TODO: allow MRTable mode after MRTable supports child tables.
        executeDdl(createFoo, nsUp, true, true/*noMRTableMode*/);
        assertNotNull(getTable(nsLw, "foo"));
        assertNotNull(getTable(nsUp, "foo"));

        executeDdl(createFooChild, nsLw, true, true/*noMRTableMode*/);
        assertNotNull(getTable(nsLw, "foo.child"));
        assertNotNull(getTable(nsUp, "foo"));

        executeDdl(createFooChildIndex, nsLw);
        assertNotNull(getIndex(nsLw, "foo.child", "idx"));
        assertNotNull(getTable(nsUp, "foo"));

        executeDdl(dropFooChildIndex, nsUp);
        assertNull(getIndex(nsUp, "foo.child", "idx"));
        assertNull(getIndex(nsLw, "foo.child", "idx"));

        executeDdl(dropFooChild, nsUp);
        assertNull(getTable(nsUp, "foo.child"));
        assertNull(getTable(nsLw, "foo.child"));

        executeDdl(dropFoo, nsLw);
        assertNull(getTable(nsLw, "foo"));
        assertNull(getTable(nsUp, "foo"));
    }

    /*
     * Local utilities
     */
    private void testListTable(String ns, String tableName) {
        /*
         * NOTE: the Map returned has a case-insensitive comparator
         */
        Map<String, Table> tables = tableImpl.getTables(ns);
        assertTrue(ns == null || tables.size() == 1);
        Table table = tables.get(tableName);
        assertNotNull(table);
        assertTrue(ns == null || table.getNamespace().equalsIgnoreCase(ns));
    }

    private void assertNumResults(PreparedStatement ps,
                                  ExecuteOptions options,
                                  int expected) {
        StatementResult res = store.executeSync(ps, options);
        int count = 0;
        for (Iterator<RecordValue> iterator =
                res.iterator(); iterator.hasNext();) {
            iterator.next();
            count++;
        }
        assertEquals(expected, count);
    }
}
