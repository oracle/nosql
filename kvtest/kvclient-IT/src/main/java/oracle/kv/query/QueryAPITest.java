/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.query;

import static oracle.kv.impl.api.table.NameUtils.SYSTEM_NAMESPACE_PREFIX;
import static oracle.kv.impl.query.runtime.ConcatenateStringsOpIter.STRING_MAX_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.StatementResult;
import oracle.kv.StaticClientTestBase;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Various tests using the QueryAPI.
 */
public class QueryAPITest extends StaticClientTestBase {
    @BeforeClass
    public static void mySetUp()
        throws Exception {
        staticSetUp("queryAPITestStore", SECURITY_ENABLE);
    }

    @AfterClass
    public static void myTearDown()
        throws Exception {
        staticTearDown();
    }

    @Test
    public void testPrepareCharArray() {
        String q = "SHOW TABLES";
        char[] query = q.toCharArray();
        PreparedStatement ps = store.prepare(query, null);
        assertNotNull(ps);

        StatementResult sr = store.executeSync(ps);
        assertNotNull(sr);

        cleanCharArray(query);

        assertTrue(sr.isSuccessful());
        assertTrue(sr.getKind() == StatementResult.Kind.DDL);

        assertNotNull(sr.getResult());
        assertTrue(sr.getResult().contains("SYS$"));

        assertTrue(ps.toString().trim().length() == 0);

        assertFalse(sr.toString().contains(q));
    }

    @Test
    public void testExecuteSyncCharArray() {
        String q = "SHOW TABLES";
        char[] query = q.toCharArray();
        StatementResult sr = store.executeSync(query , null);
        assertNotNull(sr);

        cleanCharArray(query);

        assertTrue(sr.isSuccessful());
        assertTrue(sr.getKind() == StatementResult.Kind.DDL);

        assertNotNull(sr.getResult());
        assertTrue(sr.getResult().contains("SYS$"));

        assertFalse(sr.toString().contains(q));
    }

    @Test
    public void testExecuteCharArray()
        throws ExecutionException, InterruptedException {
        String q = "SHOW TABLES";
        char[] query = q.toCharArray();
        ExecutionFuture ef = store.execute(query , null);
        assertNotNull(ef);

        StatementResult sr = ef.get();

        cleanCharArray(query);

        assertTrue(sr.isSuccessful());
        assertTrue(sr.getKind() == StatementResult.Kind.DDL);

        assertNotNull(sr.getResult());
        assertTrue(sr.getResult().contains("SYS$"));

        assertFalse(sr.toString().contains(q));
    }

    @Test
    public void testSysdefaultValue() {
        assertEquals("sysdefault", TableAPI.SYSDEFAULT_NAMESPACE_NAME);
    }

    @Test
    public void testRowEquals() {
        StatementResult sr = store.executeSync("CREATE NAMESPACE ns1");
        assertNotNull(sr);
        sr = store.executeSync("CREATE NAMESPACE ns2");
        assertNotNull(sr);

        sr = store.executeSync(
            "CREATE table ns1:t (i integer, s string, PRIMARY KEY(i))");
        assertNotNull(sr);

        sr = store.executeSync(
            "CREATE table ns2:t (i integer, s string, PRIMARY KEY(i))");
        assertNotNull(sr);

        sr = store.executeSync(
            "CREATE table t (i integer, s string, PRIMARY KEY(i))");
        assertNotNull(sr);

        TableAPI api = store.getTableAPI();
        Table ns1t = api.getTable("ns1:t");
        Table ns2t = api.getTable("ns2:t");
        Table sdt = api.getTable("t");

        assertNotNull(ns1t);
        assertNotNull(ns2t);
        assertNotNull(sdt);

        Row r1ns1t = ns1t.createRow();
        r1ns1t.put("i", 1).put("s", "1");
        Row r1ns2t = ns2t.createRow();
        r1ns2t.put("i", 1).put("s", "1");
        Row r1sdt = sdt.createRow();
        r1sdt.put("i", 1).put("s", "1");

        assertFalse(r1ns1t.equals(r1ns2t));
        assertFalse(r1ns1t.equals(r1sdt));
        assertFalse(r1ns2t.equals(r1sdt));

        Row r2ns1t = ns1t.createRow();
        r2ns1t.put("i", 1).put("s", "1");
        Row r2ns2t = ns2t.createRow();
        r2ns2t.put("i", 1).put("s", "1");
        Row r2sdt = sdt.createRow();
        r2sdt.put("i", 1).put("s", "1");

        assertTrue(r1ns1t.equals(r2ns1t));
        assertTrue(r1ns2t.equals(r2ns2t));
        assertTrue(r1sdt.equals(r2sdt));

        api.put(r1ns1t, null, null);
        api.put(r1ns2t, null, null);
        api.put(r1sdt, null, null);

        PrimaryKey pk1ns1t = ns1t.createPrimaryKey();
        pk1ns1t.put("i", 1);
        r2ns1t = api.get(pk1ns1t, null);
        PrimaryKey pk1ns2t = ns2t.createPrimaryKey();
        pk1ns2t.put("i", 1);
        r2ns2t = api.get(pk1ns2t, null);
        PrimaryKey pk1sdt = sdt.createPrimaryKey();
        pk1sdt.put("i", 1);
        r2sdt = api.get(pk1sdt, null);

        assertTrue(r1ns1t.equals(r2ns1t));
        assertTrue(r1ns2t.equals(r2ns2t));
        assertTrue(r1sdt.equals(r2sdt));

        assertFalse(r2ns1t.equals(r2ns2t));
        assertFalse(r2ns1t.equals(r2sdt));
        assertFalse(r2ns2t.equals(r2sdt));

        store.executeSync("DROP NAMESPACE ns1 CASCADE");
        store.executeSync("DROP NAMESPACE ns2 CASCADE");
        store.executeSync("DROP TABLE t");
    }

    @Test
    public void testNs() {
        String stmt = "SHOW TABLES";
        StatementResult sr = store.executeSync(stmt);
        assertNotNull(sr);
        assertTrue(sr.isSuccessful());

        TableAPI api = store.getTableAPI();
        Set<String> nses = api.listNamespaces();
        assertNotNull(nses);
        assertEquals(1, nses.size());
        assertEquals("sysdefault", nses.iterator().next());

        try {
            stmt = "CREATE table ns:t (i integer, PRIMARY KEY(i))";
            store.executeSync(stmt);
            fail("Expected FaultException");
        } catch (FaultException fe) {
            assertTrue(fe.getMessage().contains("Cannot create table. " +
                "Namespace does not exist: ns"));
        }

        stmt = "CREATE NAMESPACE ns";
        sr = store.executeSync(stmt);
        assertNotNull(sr);
        assertTrue(sr.isSuccessful());

        nses = api.listNamespaces();
        assertNotNull(nses);
        assertEquals(2, nses.size());
        assertTrue(nses.contains("sysdefault"));
        assertTrue(nses.contains("ns"));

        sr = store.executeSync("SHOW NAMESPACES");
        assertNotNull(sr);
        assertEquals("namespaces\n  ns\n  sysdefault", sr.getResult());

        sr = store.executeSync("SHOW AS JSON NAMESPACES");
        assertNotNull(sr);
        assertEquals("{\"namespaces\" : [\"ns\",\"sysdefault\"]}", sr.getResult());

        try {
            stmt = "CREATE NAMESPACE ns";
            store.executeSync(stmt);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals("Error: User error in query: CREATE NAMESPACE failed" +
                " for namespace 'ns': Namespace 'ns' already exists",
                iae.getMessage());
        }

        stmt = "CREATE NAMESPACE IF NOT EXISTS ns";
        sr = store.executeSync(stmt);
        assertNotNull(sr);
        assertTrue(sr.isSuccessful());

        stmt = "CREATE table ns:t (i integer, PRIMARY KEY(i))";
        sr = store.executeSync(stmt);
        assertNotNull(sr);
        assertTrue(sr.isSuccessful());

        stmt = "SHOW TABLES";
        sr = store.executeSync(stmt);
        assertNotNull(sr);
        assertTrue(sr.isSuccessful());

        Table t = store.getTableAPI().getTable("ns:t");
        assertNotNull(t);
        assertEquals("ns", t.getNamespace());
        assertEquals("t", t.getName());
        assertEquals("t", t.getFullName());
        assertEquals("ns:t", t.getFullNamespaceName());

        /* Wait for Admin to create system table */
        waitForTable(store.getTableAPI(), "SYS$TableStatsPartition");

        Table sp = store.getTableAPI().getTable
            ("SYS$TableStatsPartition");
        assertNotNull(sp);
        Table sdsp = store.getTableAPI().getTable
            ("sysdefault:SYS$TableStatsPartition");
        assertNotNull(sdsp);
        assertTrue(sp.equals(sdsp));
        assertEquals("sysdefault", sp.getNamespace());
        assertEquals("SYS$TableStatsPartition", sp.getName());
        assertEquals("SYS$TableStatsPartition", sp.getFullName());
        assertEquals("SYS$TableStatsPartition",
            sp.getFullNamespaceName());


        try {
            stmt = "DROP NAMESPACE ns";
            sr = store.executeSync(stmt);
            assertNotNull(sr);
            assertTrue(sr.isSuccessful());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals("Error: User error in query: DROP NAMESPACE failed " +
                "for namespace 'ns': Failed to remove namespace: 'ns': " +
                "Namespace 'ns' cannot be removed because it is not empty. " +
                "Remove all tables contained in this namespace or use " +
                "CASCADE option.",
                iae.getMessage());
        }

        try {
            stmt = "DROP TABLE t";
            sr = store.executeSync(stmt);
            assertNotNull(sr);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals("Error: User error in query: DROP TABLE failed for "
             + "table t: Table does not exist: t", iae.getMessage());
        }

        stmt = "DROP TABLE nS:t";
        sr = store.executeSync(stmt);
        assertNotNull(sr);
        assertTrue(sr.isSuccessful());

        stmt = "DROP NAMESPACE nS";
        sr = store.executeSync(stmt);
        assertNotNull(sr);
        assertTrue(sr.isSuccessful());

        nses = api.listNamespaces();
        assertNotNull(nses);
        assertEquals(1, nses.size());
        assertEquals("sysdefault", nses.iterator().next());

        try {
            stmt = "DROP NAMESPACE ns";
            store.executeSync(stmt);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals("Error: User error in query: DROP NAMESPACE failed " +
                "for namespace 'ns': Namespace 'ns' does not exist.",
                iae.getMessage());
        }

        stmt = "DROP NAMESPACE IF EXISTS ns";
        sr = store.executeSync(stmt);
        assertNotNull(sr);
        assertTrue(sr.isSuccessful());
    }

    @Test
    public void testNsErrors() {

        String stmt;
        StatementResult sr;

        try {
            stmt = "CREATE NAMESPACE sysdefault";
            store.executeSync(stmt);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals("Error: User error in query: CREATE NAMESPACE failed" +
                " for namespace 'sysdefault': Invalid namespace name, this " +
                "namespace name is reserved: sysdefault", iae.getMessage());
        }

        try {
            stmt = "CREATE NAMESPACE sYS001";
            store.executeSync(stmt);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals("Error: User error in query: CREATE NAMESPACE failed" +
                " for namespace 'sYS001': Invalid namespace name, names " +
                "starting with " + SYSTEM_NAMESPACE_PREFIX + " are reserved: " +
                "sYS001", iae.getMessage());
        }

        try {
            stmt = "DROP NAMESPACE sysdefault";
            sr = store.executeSync(stmt);
            assertNotNull(sr);
            assertTrue(sr.isSuccessful());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals("Error: User error in query: DROP NAMESPACE failed " +
                "for namespace 'sysdefault': Cannot remove the system " +
                "reserved namespace: sysdefault", iae.getMessage());
        }

        try {
            stmt = "DROP NAMESPACE Sys002";
            sr = store.executeSync(stmt);
            assertNotNull(sr);
            assertTrue(sr.isSuccessful());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals("Error: User error in query: DROP NAMESPACE failed " +
                "for namespace 'Sys002': Namespace 'Sys002' does not exist.",
                iae.getMessage());
        }

        try {
            stmt = "CREATE NAMESPACE illegal@name!";
            store.executeSync(stmt);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("mismatched input '@'"));
        }

        try {
            stmt = "DROP NAMESPACE illegal@name!";
            sr = store.executeSync(stmt);
            assertNotNull(sr);
            assertTrue(sr.isSuccessful());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("mismatched input '@'"));
        }

        try {
            stmt = "CREATE NAMESPACE sysReserved";
            store.executeSync(stmt);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("names starting with sys are" +
                " reserved"));
        }

        try {
            stmt = "CREATE NAMESPACE ''";
            store.executeSync(stmt);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("mismatched input ''''"));
        }

        try {
            stmt = "CREATE NAMESPACE ' '";
            store.executeSync(stmt);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("mismatched input '' ''"));
        }

        /* test with invalid namespace and table names */
        assertEquals(null, store.getTableAPI().getTable("n@mespace:t@ble"));
    }

    private void cleanCharArray(char[] carr) {
        for (int i = 0; i < carr.length; i++) {
            carr[i] = ' ';
        }
    }


    @Test
    public void testConcatReplaceFn() {
        String stmt;
        StatementResult sr;

        stmt = "create table if not exists stringsTable ( id integer, str1 " +
            "string, str2 string, str3 string, primary key(id))";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());

        stmt = "insert into stringsTable values( 1, 'abc', 'bcd', 'cde')";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());

        stmt = "insert into stringsTable values( 2, null, 'bcd', 'cde')";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());

        stmt = "insert into stringsTable values( 3, null, null, 'cde')";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());

        stmt = "insert into stringsTable values( 4, null, null, null)";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());

        stmt = "insert into stringsTable values( 5, null, '', null)";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());


        stmt = "SELECT id, str1 || str2 || str3 FROM stringsTable ORDER BY id";
        sr = store.executeSync(stmt);

        assertTrue(sr.isSuccessful());
        for (RecordValue rec : sr) {
            switch (rec.get("id").asInteger().get()) {
            case 1:
                assertEquals("abcbcdcde", rec.get(1).asString().get());
                break;
            case 2:
                assertEquals("bcdcde", rec.get(1).asString().get());
                break;
            case 3:
                assertEquals("cde", rec.get(1).asString().get());
                break;
            case 4:
                assertTrue(rec.get(1).isNull());
                break;
            case 5:
                assertEquals("", rec.get(1).asString().get());
                break;
            default:
                fail("Unknown data");
                break;
            }
        }

        // concat param not of type string
        stmt = "select id, str2, id || 000 || '-' || str2 from stringsTable";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());
        for (RecordValue rec : sr) {
            switch (rec.get("id").asInteger().get()) {
            case 1:
                assertEquals("10-bcd",
                    rec.get("Column_3").asString().get());
                break;
            case 2:
                assertEquals("20-bcd",
                    rec.get("Column_3").asString().get());
                break;
            case 3:
                assertEquals("30-",
                    rec.get("Column_3").asString().get());
                break;
            case 4:
                assertEquals("40-",
                    rec.get("Column_3").asString().get());
                break;
            case 5:
                assertEquals("50-",
                    rec.get("Column_3").asString().get());
                break;
            }
        }

        // Check Errors
        String longStr = "1234567890abcdef";
        // make a string of size 131072 which will trigger QueryException
        // because 131072 + 131072 = 262144 which is bigger than 262143 the
        // STRING_MAX_SIZE.
        for (int i = 4 ; i < 17; i++) {
            longStr += longStr;
        }
        stmt = "insert into stringsTable values( 10, '" + longStr + "', '" +
            longStr + "', '" + longStr + "')";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());

        stmt = "select id, str1 || str2 from stringsTable where id = 10";
        PreparedStatement ps  = store.prepare(stmt);
        try {
            sr = store.executeSync(ps);
            assertTrue(sr.isSuccessful());

            /* Must go through results to properly execute query */
            for (@SuppressWarnings("unused") RecordValue rec : sr) {
                //System.out.println(rec);
            }
            fail();
        } catch (IllegalArgumentException iae) {
            //System.out.println("  iae: " + iae.getMessage());
            //System.out.println("  Caught iae: SUCCESS");
        }


        // check replace function errors out on long strings
        longStr += longStr.substring(0, longStr.length() - 1);
        assertEquals(STRING_MAX_SIZE, longStr.length());

        stmt = "insert into stringsTable values( 20, '" + longStr + "', " +
            " null, null)";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());

        stmt = "select id, replace(str1, 'abc', 'ABCD') from stringsTable " +
            "where id = 20";
        ps  = store.prepare(stmt);
        try {
            sr = store.executeSync(ps);
            assertTrue(sr.isSuccessful());

            /* Must go through results to properly execute query */
            for (@SuppressWarnings("unused") RecordValue rec : sr) {
                //System.out.println(rec);
            }
            fail();
        } catch (IllegalArgumentException iae) {
            //System.out.println("  iae: " + iae.getMessage());
            //System.out.println("  Caught iae: SUCCESS");
        }

        stmt = "drop table if exists stringsTable";
        sr = store.executeSync(stmt);
        assertTrue(sr.isSuccessful());
    }
}
