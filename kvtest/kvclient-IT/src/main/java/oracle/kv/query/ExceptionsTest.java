/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.query;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.StatementResult;
import oracle.kv.StaticClientTestBase;
import oracle.kv.impl.query.compiler.EscapeUtil;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test class houses simple query language and query engine tests that
 * operate on persistent stores.  The general test pattern is:
 * 1. create and populate a table
 * 2. run some interesting queries
 * 3. verify that results are as expected
 *
 * Each new category of queries should be in its own test case.  The store
 * is clean of data and tables between each test case (but not re-created).
 */
public class ExceptionsTest extends StaticClientTestBase {

    private static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS Users" +
        "(id INTEGER, firstName STRING, lastName STRING, age INTEGER," +
        "primary key (id))";

    @BeforeClass
    public static void mySetUp()
        throws Exception {
        staticSetUp("exceptionTestStore", SECURITY_ENABLE);
    }

    @AfterClass
    public static void myTearDown()
        throws Exception {
        staticTearDown();
    }

    void addUsers(int num) {
        TableAPI tableApi = store.getTableAPI();
        Table table = tableApi.getTable("Users");

        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("firstName", ("first " + i));
            row.put("lastName", ("last " + i));
            row.put("age", i + 10);
            tableApi.put(row, null, null);
        }
    }


    @Test
    public void testPrepareInvalidStatement() {
        try {
            store.prepare("abracadabra");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
            assertTrue("Different exception message, got: '" + e.getMessage
                    () + "'",
                e.getMessage().contains("mismatched input " +
                    "'abracadabra'"));
        } catch (Throwable e) {
            assertTrue("Wrong exception thrown: " + e, false);
        }
    }

    @Test
    public void testPrepareInvalidStatementLocation() {
        store.executeSync(CREATE_TABLE);
        try {
            store.prepare("select * from Users where age");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
            String msg = e.toString();
            assertTrue("Different exception message",
                e.toString().contains(
                    "(1, 26) Cannot promote type "));
            assertTrue("Unexpected location in error",
                       msg.contains("1,"));
            assertTrue("Unexpected location in error",
                       msg.contains("26)"));
        } catch (Throwable e) {
            assertTrue("Wrong exception thrown: " + e, false);
        }
    }

    @Test
    public void testPrepareInvalidTable() {
        try {
            store.prepare("select * from table_does_not_exist");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
            assertTrue("Different exception message",
                e.getMessage().contains("Table table_does_not_exist " +
                    "does not exist"));
        } catch (Throwable e) {
            assertTrue("Wrong exception thrown: " + e, false);
        }
    }

    @Test
    public void testExecSyncStrInvalidStatement() {
        try {
            store.executeSync("abracadabra");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
            assertTrue("Different exception message, got: '" + e.getMessage
                () + "'",
                e.getMessage().contains("mismatched input " +
                    "'abracadabra'"));
        } catch (Throwable e) {
            assertTrue("Wrong exception thrown: " + e, false);
        }
    }

    @Test
    public void testExecSyncStrInvalidTable() {
        try {
            store.executeSync("select * from table_does_not_exist");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
            assertTrue("Different exception message",
                e.getMessage().contains("Table table_does_not_exist " +
                    "does not exist"));
        } catch (Throwable e) {
            assertTrue("Wrong exception thrown: " + e, false);
        }
    }

    @Test
    public void testExecStrInvalidStatement() {
        try {
            store.execute("abracadabra");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
            assertTrue("Different exception message, got: '" + e.getMessage
                () + "'",
                e.getMessage().contains("mismatched input " +
                    "'abracadabra'"));
        } catch (Throwable e) {
            assertTrue("Wrong exception thrown: " + e, false);
        }
    }

    @Test
    public void testExecStrInvalidTable() {
        try {
            store.execute("select * from table_does_not_exist");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
            assertTrue("Different exception message",
                e.getMessage().contains("Table table_does_not_exist " +
                    "does not exist"));
        } catch (Throwable e) {
            assertTrue("Wrong exception thrown: " + e, false);
        }
    }

    @Test
    public void testCheckDdl() {
        StatementResult res = store.executeSync(CREATE_TABLE);

        assertTrue(res.isSuccessful());
        assertNull(res.getErrorMessage());
        assertEquals(StatementResult.Kind.DDL, res.getKind());
        assertNotNull(res.getInfo());
        assertNotNull(res.getInfoAsJson());
        assertTrue(res.getPlanId() > 0);
        assertNull(res.getResult());
        assertNull(res.getResultDef());
        assertFalse(res.isCancelled());
        assertTrue(res.isDone());

        // check the empty iterator
        TableIterator<RecordValue> iterator = res.iterator();
        assertNotNull(iterator);
        assertFalse(iterator.hasNext());

        try {
            iterator.next();
            assertTrue(false);
        } catch (NoSuchElementException e) {
            assertTrue(true);
        }

        assertTrue(iterator.getPartitionMetrics().isEmpty());
        assertTrue(iterator.getShardMetrics().isEmpty());

        iterator.close();

        res.close();
    }

    @Test
    public void testCheckDml() {
        store.executeSync(CREATE_TABLE);
        int noOfUsers = 3;
        addUsers(noOfUsers);

        StatementResult res = store.executeSync("SELECT * FROM Users");

        assertTrue(res.isSuccessful());
        assertNull(res.getErrorMessage());
        assertEquals(StatementResult.Kind.QUERY, res.getKind());
        assertNull(res.getInfo());
        assertNull(res.getInfoAsJson());
        assertTrue(res.getPlanId() == 0);
        assertNull(res.getResult());
        assertNotNull(res.getResultDef());
        assertFalse(res.isCancelled());
        assertFalse(res.isDone());

        // check the empty iterator
        TableIterator<RecordValue> iterator = res.iterator();
        assertNotNull(iterator);

        for (int i = 0; i < noOfUsers; i++) {
            assertTrue(iterator.hasNext());
            RecordValue recordValue = iterator.next();
            assertNotNull(recordValue);
        }

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            assertTrue(false);
        } catch (NoSuchElementException e) {
            assertTrue(true);
        }

        assertTrue(iterator.getPartitionMetrics().size() > 0);
        assertTrue(iterator.getShardMetrics().size() > 0);

        iterator.close();

        res.close();
    }

    @Test
    public void testCheckDmlExecOpts() {
        store.executeSync(CREATE_TABLE);
        int noOfUsers = 3;
        addUsers(noOfUsers);

        ExecuteOptions opts = new ExecuteOptions();
        opts.setDurability(Durability.COMMIT_SYNC);
        opts.setConsistency(Consistency.ABSOLUTE);
        opts.setMaxConcurrentRequests(100);
        opts.setResultsBatchSize(101);
        opts.setTimeout(3, TimeUnit.SECONDS);

        assertEquals(Durability.COMMIT_SYNC, opts.getDurability());
        assertEquals(Consistency.ABSOLUTE, opts.getConsistency());
        assertEquals(100, opts.getMaxConcurrentRequests());
        assertEquals(101, opts.getResultsBatchSize());
        assertEquals(3, opts.getTimeout());
        assertEquals(TimeUnit.SECONDS, opts.getTimeoutUnit());

        StatementResult res = store.executeSync("SELECT * FROM Users", opts);

        assertTrue(res.isSuccessful());
        assertNull(res.getErrorMessage());
        assertEquals(StatementResult.Kind.QUERY, res.getKind());
        assertNull(res.getInfo());
        assertNull(res.getInfoAsJson());
        assertTrue(res.getPlanId() == 0);
        assertNull(res.getResult());
        assertNotNull(res.getResultDef());
        assertFalse(res.isCancelled());
        assertFalse(res.isDone());

        // check the empty iterator
        TableIterator<RecordValue> iterator = res.iterator();
        assertNotNull(iterator);

        for (int i = 0; i < noOfUsers; i++) {
            assertTrue(iterator.hasNext());
            RecordValue recordValue = iterator.next();
            assertNotNull(recordValue);
        }

        assertFalse(iterator.hasNext());
        try {
            iterator.next();
            assertTrue(false);
        } catch (NoSuchElementException e) {
            assertTrue(true);
        }

        assertTrue(iterator.getPartitionMetrics().size() > 0);
        assertTrue(iterator.getShardMetrics().size() > 0);

        iterator.close();

        res.close();
    }

    @Test
    public void testCheckDdlClose() {
        StatementResult res = store.executeSync(CREATE_TABLE);

        // check the empty iterator
        TableIterator<RecordValue> iterator = res.iterator();
        assertNotNull(iterator);

        res.close();

        // check resultDef for ddl
        try {
            assertNull(res.getResultDef());
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }

        // check iterator for ddl
        try {
            assertFalse(iterator.hasNext());
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }

        try {
            iterator.next();
            assertTrue(false);
        } catch (NoSuchElementException e) {
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }

        assertTrue(res.isSuccessful());
        assertNull(res.getErrorMessage());
        assertEquals(StatementResult.Kind.DDL, res.getKind());
        assertNotNull(res.getInfo());
        assertNotNull(res.getInfoAsJson());
        assertTrue(res.getPlanId() > 0);
        assertNull(res.getResult());
        assertNull(res.getResultDef());
        assertFalse(res.isCancelled());
        assertTrue(res.isDone());

        assertTrue(iterator.getPartitionMetrics().isEmpty());
        assertTrue(iterator.getShardMetrics().isEmpty());
    }

    @Test
    public void testCheckDmlClose() {
        store.executeSync(CREATE_TABLE);
        int noOfUsers = 3;
        addUsers(noOfUsers);
        StatementResult res = store.executeSync("SELECT * FROM Users");

        // check the iterator
        TableIterator<RecordValue> iterator = res.iterator();
        assertNotNull(iterator);

        // Close the result
        res.close();

        // check resultDef for dml
        assertTrue(res.getResultDef().isRecord());

        // check iterator for dml

        try {
            assertFalse(iterator.hasNext());
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }

        try {
            iterator.next();
            assertTrue(false);
        } catch (NoSuchElementException e) {
            assertTrue(true);
        } catch (Exception e) {
            assertTrue(false);
        }

        assertTrue(res.isSuccessful());
        assertNull(res.getErrorMessage());
        assertEquals(StatementResult.Kind.QUERY, res.getKind());
        assertNull(res.getInfo());
        assertNull(res.getInfoAsJson());
        assertTrue(res.getPlanId() == 0);
        assertNull(res.getResult());
        assertFalse(res.isCancelled());
        assertTrue(res.isDone());

        assertTrue(iterator.getPartitionMetrics().size() > 0);
        assertTrue(iterator.getShardMetrics().size() > 0);
    }

    @Test
    public void testBindVars() {
        store.executeSync(CREATE_TABLE);
        int noOfUsers = 3;
        addUsers(noOfUsers);

        PreparedStatement preparedStatement = store.prepare
            ("declare $v1 integer; \n" +
                "$v2 boolean; \n" +
                "$v3 binary; \n" +
                "$v4 double; \n" +
                "$v5 float; \n" +
                "$v6 long; \n" +
                "$v7 string; \n" +
                "SELECT * FROM Users WHERE id = $v1");

        RecordDef resDef = preparedStatement.getResultDef();

        assertNotNull(resDef);

        assertEquals(resDef.isRecord(), true);
        assertNotNull(resDef.getFieldNames());
        assertEquals(resDef.getFieldNames().size(), 4);
        assertArrayEquals(resDef.getFieldNames().toArray(), new String[] {"id",
            "firstName", "lastName", "age"});

        BoundStatement boundStatement =
            preparedStatement.createBoundStatement();

        assertNotNull(boundStatement);

        boundStatement.setVariable("$v1", 1);

        try {
            boundStatement.setVariable("v1", 2);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        try {
            boundStatement.setVariable("$v1", "2");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        try {
            boundStatement.setVariable("$v1", true);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        try {
            boundStatement.setVariable("$v1", 2.2);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        try {
            boundStatement.setVariable("$v1", 2.3f);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        try {
            boundStatement.setVariable("$v1", 3l);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        try {
            boundStatement.setVariable("$v1", new byte[] {});
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }

        boundStatement.setVariable("$v2", true);
        boundStatement.setVariable("$v3", new byte[] {});
        boundStatement.setVariable("$v4", 2.3d);
        boundStatement.setVariable("$v5", 2.4f);
        boundStatement.setVariable("$v6", 3l);
        boundStatement.setVariable("$v7", "4");
    }
    
    @Test
    public void testBindVarsByPosition() {
        store.executeSync(CREATE_TABLE);
        int noOfUsers = 3;
        addUsers(noOfUsers);

        PreparedStatement preparedStatement = 
                store.prepare("declare $v1 integer; \n" +
                        "$v2 boolean; \n" +
                        "$v3 binary; \n" +
                        "$v4 double; \n" +
                        "$v5 float; \n" +
                        "$v6 long; \n" +
                        "$v7 string; \n" +
                        "SELECT * FROM Users WHERE id = $v1");
        RecordDef resDef = preparedStatement.getResultDef();
        assertNotNull(resDef);

        assertEquals(resDef.isRecord(), true);
        assertNotNull(resDef.getFieldNames());
        assertEquals(resDef.getFieldNames().size(), 4);
        assertArrayEquals(
                resDef.getFieldNames().toArray(), new String[] { "id",
                        "firstName", "lastName", "age" });
        BoundStatement boundStatement = preparedStatement.createBoundStatement();
        assertNotNull(boundStatement);

        try {
            boundStatement.setVariable(10, true);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        try {
            boundStatement.setVariable(2, 1);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        try {
            boundStatement.setVariable(1, "2");
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        try {
            boundStatement.setVariable(1, true);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        try {
            boundStatement.setVariable(1, 2.2);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        try {
            boundStatement.setVariable(1, 2.3f);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        try {
            boundStatement.setVariable(1, 3l);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        try {
            boundStatement.setVariable(1, new byte[] {});
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
        boundStatement.setVariable(1, 2);
        boundStatement.setVariable(2, true);
        boundStatement.setVariable(3, new byte[] {});
        boundStatement.setVariable(4, 2.3d);
        boundStatement.setVariable(5, 2.4f);
        boundStatement.setVariable(6, 3l);
        boundStatement.setVariable(7, "4");
    }

    @Test
    public void testUnicode() {
        store.executeSync("create table patientinfo(id integer, " +
                "patient json, primary key(id))");

        TableAPI tapi = store.getTableAPI();
        Table tableH = tapi.getTable("patientinfo");

        String[] strs = {"no unicode",
            "- \u2202 \u0218 \\u0219 \\u021a \\u021b \\u03a6 \u03a8 \u03a9 -",
            "",
            "\\\\",
            "\\\\\\\\",
            "\\u2202",
            "\\\\u2202",
            "\\\"",
            "\\\\'",
            "\\\\\\\\'",
            "\'",
            "\\bdog\\b",
            "\\n",       //  \n 
            "\\t",       //  \t 
        };

        Row row;
        int i = 0;

        for (String str : strs) {
            row = tableH.createRow();
            row.put("id", ++i);
            row.putJson("patient", "{\"a\":\"" + str + "\"}");
            tapi.put(row, null, null);
        }


        for (String str : strs) {
            StatementResult sr = store.executeSync("select * from patientinfo"
                + " where patientinfo.patient.a = " +
                " \"" + str + "\""
            );

            int count = 0;

            for (RecordValue rec : sr) {
                assertEquals(EscapeUtil.inlineEscapedChars(str),
                    rec.get(1).asMap().get("a").asString().get());
                count++;
            }

            assertEquals(1, count);
        }

        // Errors
        String[] errStrs = {
            "\\\\\\",    //  \\\     \x
            "\\",        //  \       x
            "\\\\\"",    //  \\"     "\""
            "\\_",       //  \_      x
            "\\a",       //  \a      x
            "\\u",       //  \ u     x
            "\\u0",      //  \ u0    x
            "\\u00",     //  \ u00   x
            "\\u000",    //  \ u000  x
            "\\'",       //  \'      x
            "\\\'",      //  \'      x
            "\\\\\\'"    //  \\\'    \x
        };

        for (String str : errStrs) {
            try {
                StatementResult sr =
                    store.executeSync("select * from patientinfo"
                        + " where patientinfo.patient.a = " +
                        " \"" + str + "\""
                    );

                for (RecordValue rec : sr) {
                    System.out.print("   " + rec.getFieldName(0) + ": " + rec
                        .get(0));
                    System.out.println("   " + rec.getFieldName(1) + ": " + rec
                        .get(1));
                }

                assertTrue(false);

            } catch (RuntimeException e) {
                assertTrue(true);
            }
        }
    }

    @Test
    public void testMathContext() {
        store.executeSync("CREATE TABLE IF NOT EXISTS Users" +
            "(id INTEGER, a NUMBER, b NUMBER," +
            "primary key (id))");

        TableAPI tableApi = store.getTableAPI();
        Table table = tableApi.getTable("Users");

        Row row = table.createRow();
        row.put("id", 1);
        row.putNumber("a", BigDecimal.ONE);
        row.putNumber("b", BigDecimal.valueOf(3));
        tableApi.put(row, null, null);


        // Use default math context: MathContext.DECIMAL32
        StatementResult res = store.executeSync("SELECT a/b FROM Users");
        assertNotNull(res);

        BigDecimal bdRes = res.iterator().next().get(0).asNumber().get();

        assertEquals(
            BigDecimal.ONE.divide(BigDecimal.valueOf(3), MathContext.DECIMAL32),
            bdRes);


        // Use MathContext.UNLIMITED, expecting error.
        ExecuteOptions executeOptions = new ExecuteOptions().setMathContext(
            MathContext.UNLIMITED);

        try {
            res = store.executeSync("SELECT a/b FROM Users",
                executeOptions);
            assertNotNull(res);

            bdRes = res.iterator().next().get(0).asNumber().get();

            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().
                contains("no exact representable decimal result"));
        }
    }

    @Test
    public void testNumberConstruct() {
        store.executeSync("CREATE TABLE IF NOT EXISTS Users" +
            "(id INTEGER, a NUMBER, b NUMBER," +
            "primary key (id))");

        TableAPI tableApi = store.getTableAPI();
        Table table = tableApi.getTable("Users");

        Row row = table.createRow();
        row.put("id", 1);
        row.putNumber("a", BigDecimal.ONE);
        row.putNumber("b", BigDecimal.valueOf(3));
        tableApi.put(row, null, null);


        StatementResult sr = store.executeSync("SELECT 1 + 1n FROM Users");
        assertTrue(sr.isSuccessful());
        Iterator<RecordValue> it = sr.iterator();
        assertTrue(it.hasNext());
        RecordValue rv = it.next();
        assertTrue(rv.get(0).isNumber());
    }
}
