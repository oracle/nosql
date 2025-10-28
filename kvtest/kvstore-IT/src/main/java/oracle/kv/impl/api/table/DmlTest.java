/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static oracle.kv.impl.api.table.TableLimits.NO_LIMIT;
import static oracle.kv.impl.util.SerialVersion.CURRENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.ExecutionFuture;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KeySizeLimitException;
import oracle.kv.PrepareQueryException;
import oracle.kv.ReadThroughputException;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ResourceLimitException;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorException;
import oracle.kv.TestBase;
import oracle.kv.ValueSizeLimitException;
import oracle.kv.Version;
import oracle.kv.WriteThroughputException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.query.QueryPublisher.QuerySubscription;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.query.PreparedStatementImpl.DistributionKind;
import oracle.kv.impl.api.query.QueryStatementResultImpl;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.query.runtime.CloudSerializer.FieldValueWriter;
import oracle.kv.impl.query.runtime.PlanIter;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.PollCondition;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.xregion.XRegionTestBase;
import oracle.kv.query.BoundStatement;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PrepareCallback;
import oracle.kv.query.PrepareCallback.QueryOperation;
import oracle.kv.query.PreparedStatement;
import oracle.kv.query.Statement;
import oracle.kv.stats.OperationMetrics;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.TimestampValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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
public class DmlTest extends TableTestBase {

    private static final int POLL_CONDITION_INTERVAL = 1000;

    boolean debugging = false;

    boolean showResult = false;

    boolean showPlan = false;

    RuntimeException theCompilerException;
    RuntimeException theRuntimeException;

    final static String userTableStatement =
        "CREATE TABLE Users" +
        "(id INTEGER, firstName STRING, lastName STRING, age INTEGER," +
        "primary key (id))";

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /**
         * Exclude tombstones because some unit tests count the number of
         * records in the store and tombstones will cause the result not
         * match the expected values.*/
        staticSetUp(true/*excludeTombstone*/);
    }

    @Override
    protected String getNamespace() {
        return "dmltest";
    }

    @Test
    public void parallelPrepare() {

        String tableDDL = "CREATE TABLE foo(" +
            "shardId INTEGER, " +
            "pkString STRING, " +
            "colBoolean BOOLEAN, " +
            "colInteger INTEGER, " +
            "colLong LONG, " +
            "colFloat FLOAT, " +
            "colDouble DOUBLE, " +
            "colNumber NUMBER, " +
            "colNumber2 NUMBER, " +
            "colBinary BINARY, " +
            "colFixedBinary BINARY(64), " +
            "colEnum ENUM(enumVal1, enumVal2, enumVal3, enumVal4, enumVal5), " +
            "colTimestamp TIMESTAMP(9), " +
            "colRecord RECORD(fldString STRING, fldNumber NUMBER, fldArray ARRAY(INTEGER)), " +
            "colArray ARRAY(TIMESTAMP(6)), " +
            "colArray2 ARRAY(JSON), " +
            "colMap MAP(LONG), " +
            "colMap2 MAP(BINARY), " +
            "colJSON JSON, " +
            "colJSON2 JSON, " +
            "colIden LONG GENERATED ALWAYS AS IDENTITY, " +
            "PRIMARY KEY (SHARD(shardId), pkString)) USING TTL 3 days";

        String idx1DDL = "create index array2JsonYIdx on foo " +
            "(colArray2[].y as integer)";

        String idx2DDL = "create index enumIdx on foo(colEnum)";

        String idx3DDL = "create index idenIdx on foo(colIden)";

        String idx4DDL = "create index jsonUIdenIdx on foo " +
            "(colJSON.u as anyAtomic, colIden)";

        String idx5DDL = "create index jsonXzbIdx on foo " +
            "(colJSON.x as string, colJSON.z as string, colJSON.b as boolean)";

        String queryDML = "DECLARE $sid INTEGER; $z STRING; " +
                          "SELECT * " +
                          "FROM foo t " +
                          "WHERE t.shardId = $sid AND t.colJSON.z <= $z " +
                          "ORDER BY t.colJSON.u, t.colIden";

        executeDdl(tableDDL);
        executeDdl(idx1DDL);
        executeDdl(idx2DDL);
        executeDdl(idx3DDL);
        executeDdl(idx4DDL);
        executeDdl(idx5DDL);

        int numThreads = 4;

        String[] queryPlans = new String[numThreads];

        for (int i = 0; i < numThreads; ++i) {
            queryPlans[i] = null;
            startPrepareThread(queryDML, queryPlans, i);
        }

        final long timeoutMillis = 20 * 60 * 1000;
        PollCondition.await(
            POLL_CONDITION_INTERVAL,
            timeoutMillis,
            () -> areThreadsDone(queryPlans));

        boolean success = true;

        for (int i = 0; i < numThreads; ++i) {
            if (queryPlans[i].indexOf("index used\" : \"primary index") < 0) {
                System.out.println("COMPILATION FAILED for thread " + i +
                                   "\n" + queryPlans[i]);
                success = false;
            }
        }

        assertTrue(success);
    }

    private void startPrepareThread(
        final String queryDML,
        final String[] queryPlans,
        final int id) {

        new Thread(() -> {
            ExecuteOptions options = new ExecuteOptions();
            options.setNamespace(getNamespace(), false);
            options.setQueryName("Q-" + id);
            PreparedStatement prep = store.prepare(queryDML, options);
            queryPlans[id] = prep.toString();
        }).start();
    }

    private boolean areThreadsDone(String[] queryPlans) {

        for (int i = 0; i < queryPlans.length; ++i) {
            if (queryPlans[i] == null) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testAlterTable() {

        executeDdl(userTableStatement);

        executeDdl("create index idx1 on users (firstName)");

        String insert1Str =
            "insert into users values (1, \"Emmanuel\", \"Macron\", 50)";
        String insert2Str =
            "insert into users values (2, \"Joe\", \"Biden\", 80) set ttl 3 days";

        executeDML(insert1Str, 1);
        executeDML(insert2Str, 2);

        String[] queries = {
            "select * from users",
            "select firstName, lastName from users",
            "select firstName, age from users where firstName > \"A\""
        };
        int numQueries = queries.length;

        PreparedStatement[] preps = new PreparedStatement[numQueries];
        ExecuteOptions[] options = new ExecuteOptions[numQueries];

        for (int i = 0; i < numQueries; ++i) {
            options[i] = new ExecuteOptions();
            options[i].setNamespace(getNamespace(), false);
            options[i].setQueryName("Q-" + i);
            options[i].setAsync(false);
            preps[i] = store.prepare(queries[i], options[i]);
        }

        executeDdl("drop index idx1 on users");
        executeDdl("create index idx1 on users (age)");

        try {
            store.executeSync(preps[2]);
            assertTrue(false);
        } catch (PrepareQueryException pqe) {
            preps[2] = store.prepare(queries[2], options[2]);
            store.executeSync(preps[2]);
        }

        String alterTable = "alter table users (add col1 integer)";

        executeDdl(alterTable);

        String insert3Str =
            "insert into users values (3, \"Bernie\", \"Sander\", 81, 567)";
        executeDML(insert3Str, 3);

        for (int i = 0; i < numQueries; ++i) {
            try {
                store.executeSync(preps[i]);
            } catch (PrepareQueryException pqe) {
                preps[i] = store.prepare(queries[i], options[i]);
                store.executeSync(preps[i]);
            }
        }
     }

    @Test
    public void testUpdateTTL() {

        executeDdl(userTableStatement);

        String insert1Str =
            "insert into users values (1, \"Emmanuel\", \"Macron\", 50)";
        String insert2Str =
            "insert into users values (2, \"Joe\", \"Biden\", 80) set ttl 3 days";

        executeDML(insert1Str, 1);
        executeDML(insert2Str, 2);

        String q1 = "select id, expiration_time($u) as exptime from users $u order by id";
        List<RecordValue> res = executeDML(q1, 3);
        TimestampValue exptime12 = res.get(1).get("exptime").asTimestamp();

        String upd = "update users set ttl using table default where id = 2";
        executeDML(upd, 4);

        res = executeDML(q1, 5);
        TimestampValueImpl exptime21 = (TimestampValueImpl)res.get(0).get("exptime");
        TimestampValueImpl exptime22 = (TimestampValueImpl)res.get(1).get("exptime");

        assertTrue(exptime21.equals(exptime22));
        assertFalse(exptime12.equals(exptime22));
    }

    @Test
    public void testMapNames() {
        PrepareCB callback = new PrepareCB(null,
                                           null,
                                           null,
                                           null,
                                           null) {
            @Override
            public String mapTableName(String tableName) {
                return tableName + "_mapped";
            }

            @Override
            public String mapNamespaceName(String namespaceName) {
                return namespaceName + "_mapped";
            }
        };
        callback.setPreparedNeeded(true);

        String ns = "ns_mapped";
        /*
         * Add a map to test aliases. Don't bother populating it in
         * addUsersToTable, it defaults to null.
         */
        String ddl =
            "CREATE TABLE users_mapped" +
            "(id INTEGER, firstName STRING, lastName STRING, age INTEGER," +
            "map map(integer), primary key (id))";

        executeDdl("DROP NAMESPACE IF EXISTS " + ns + " CASCADE", null);

        executeDdl("CREATE NAMESPACE " + ns, null);
        executeDdl(ddl, ns);
        addUsersToTable(tableImpl.getTable(ns, "users_mapped"), 10);

        String query = "SELECT * FROM users";

        /* without callback no results */
        try {
            executeDml(query, "ns");
            fail("expected");
        } catch (IllegalArgumentException iae) {
        }

        /* with callback */
        StatementResult sr = executeDml(query, callback, "ns");
        assertEquals("Unexpected number of results", 10, countRecords(sr));

        query = "INSERT INTO users VALUES(101, \"fn01\", \"ln101\", 101," +
            "{\"a\":1})";

        /* without callback no results */
        try {
            executeDml(query, "ns");
            fail("expected");
        } catch (IllegalArgumentException iae) {
        }
        sr = executeDml(query, callback, "ns");
        assertTrue(sr.isSuccessful());
        assertEquals("Failed to insert", 1, queryUser("101", "ns", callback));

        query = "UPSERT INTO users VALUES(102, \"fn02\", \"ln102\", 102," +
            "{\"a\":1})";
        try {
            executeDml(query, "ns");
            fail("expected");
        } catch (IllegalArgumentException iae) {
        }
        sr = executeDml(query, callback, "ns");
        assertTrue(sr.isSuccessful());
        assertEquals("Failed to upsert", 1, queryUser("102", "ns", callback));

        query = "UPDATE users u set u.firstName = 'changed' where id = 102";
        try {
            executeDml(query, "ns");
            fail("expected");
        } catch (IllegalArgumentException iae) {
        }
        sr = executeDml(query, callback, "ns");
        assertTrue(sr.isSuccessful());
        query = "SELECT * FROM USERS where id = 102";
        sr = executeDml(query, callback, "ns");
        TableIterator<RecordValue> iter = sr.iterator();
        int count = 0;
        RecordValue value = null;
        while (iter.hasNext()) {
            value = iter.next();
            ++count;
        }
        iter.close();
        assertEquals(count, 1);
        assertNotNull(value);
        assertEquals(value.get("firstName").asString().get(), "changed");

        query = "DELETE from users where id = 102";
        try {
            executeDml(query, "ns");
            fail("expected");
        } catch (IllegalArgumentException iae) {
        }
        sr = executeDml(query, callback, "ns");
        assertTrue(sr.isSuccessful());
        assertEquals("Failed to delete", 0, queryUser("102", "ns", callback));

        /*
         * Exercise alias path in select. In the first query the table name
         * becomes the alias. In the second query it's explicitly "$u"
         */
        query = "select * from users where users.map.a = 1";
        sr = executeDml(query, callback, "ns");
        assertTrue(sr.isSuccessful());

        query = "select * from users $u where $u.map.a = 1";
        sr = executeDml(query, callback, "ns");
        assertTrue(sr.isSuccessful());

        /* table name in index hint */
        query = "select /*+ PREFER_PRIMARY_INDEX(users) */ id from users u " +
            " where u.map.a = 10 and u.map.c = 3";
        sr = executeDml(query, callback, "ns");
        assertTrue(sr.isSuccessful());

        /* non-existent index in index hint */
        query = "select /*+ FORCE_INDEX(users index_not_existing) 'no index' */ " +
            "id from users";
        sr = executeDml(query, callback, "ns");
        assertTrue(sr.isSuccessful());

        query = "select /*+ FORCE_INDEX(not_a_table not_an_index) */ id1 " +
            "from users";
        try {
            executeDml(query, "ns");
            fail("expected");
        } catch (IllegalArgumentException iae) {
        }
    }

    private int queryUser(String id, String ns, PrepareCallback callback) {
        String query = "SELECT * FROM USERS where id = " + id;
        StatementResult sr = executeDml(query, callback, ns);
        return countRecords(sr);
    }

    @Test
    public void testJoinWithUpdate1() {
        testJoinWithUpdate(true);
    }

    @Test
    public void testJoinWithUpdate2() {
        testJoinWithUpdate(false);
    }

    public void testJoinWithUpdate(boolean useIndex) {

        String A_DDL =
            "CREATE TABLE A(               \n" +
            "    ida1 INTEGER,             \n" +
            "    ida2 INTEGER,             \n" +
            "    inta INTEGER,             \n" +
            "primary key (shard(ida1), ida2))";

        String B_DDL =
            "CREATE TABLE A.B(             \n" +
            "    idb INTEGER,              \n" +
            "    intb INTEGER,             \n" +
            "primary key (idb))";

        String C_DDL =
            "CREATE TABLE A.B.C(           \n" +
            "    idc INTEGER,              \n" +
            "    intc INTEGER,             \n" +
            "primary key (idc))";

        String idx_DDL = "create index idx on A(inta)";

        executeDdl(A_DDL);
        executeChildDdl(B_DDL);
        executeChildDdl(C_DDL);
        if (useIndex) {
            executeDdl(idx_DDL);
        }

        Table tableA = tableImpl.getTable(getNamespace(), "A");
        Table tableB = tableImpl.getTable(getNamespace(), "A.B");
        Table tableC = tableImpl.getTable(getNamespace(), "A.B.C");

        Row rowA = tableA.createRow();
        Row rowB = tableB.createRow();
        Row rowC = tableC.createRow();

        rowA.put("ida1", 1);

        for (int i = 0; i < 10; ++i) {

            rowA.put("ida2", i);
            if (i < 3) {
                rowA.put("inta", 10);
            } else if (i < 6) {
                rowA.put("inta", 20);
            } else {
                rowA.put("inta", 30);
            }

            tableImpl.put(rowA, null, null);

            for (int j = 0; j < 10; ++j) {

                rowB.put("ida1", 1);
                rowB.put("ida2", i);
                rowB.put("idb", j);
                rowB.put("intb", j);
                tableImpl.put(rowB, null, null);

                for (int k = 0; k < 10; ++k) {

                    rowC.put("ida1", 1);
                    rowC.put("ida2", i);
                    rowC.put("idb", j);
                    rowC.put("idc", k);
                    rowC.put("intc", k);
                    tableImpl.put(rowC, null, null);
                }
            }
        }

        String queryStr =
            "declare $a integer; " +
            "select a.ida2, b.idb, c.idc " +
            "from nested tables(A a descendants(A.B b, A.B.C c)) " +
            "where a.ida1 = 1 and inta = $a";

        String deleteAStr =
            "delete from A " +
            "where ida1 = 1 and ida2 = 0";

        String deleteBStr =
            "delete from A.B " +
            "where ida1 = 1 and ida2 = 1 and idb = 0";

        int numResults = 0;

        ExecuteOptions options = new ExecuteOptions();
        options.setNamespace(getNamespace(), false);
        options.setConsistency(Consistency.ABSOLUTE);
        options.setDoPrefetching(false);
        options.setResultsBatchSize(8);
        options.setTraceLevel((byte)0);
        options.setAsync(false);

        PreparedStatement prep = store.prepare(queryStr, options);
        BoundStatement bs = prep.createBoundStatement();
        bs.setVariable("$a", 10);

        //System.out.println("\nBefore delete A row:\n");

        StatementResult sr = store.executeSync(bs, options);
        Iterator<RecordValue> iter = sr.iterator();
        RecordValue res;

        for (int i = 0; i < 7; ++i) {
            res = iter.next();
            //System.out.println(res);
            ++numResults;
        }

        PreparedStatement delAprep = store.prepare(deleteAStr, options);
        StatementResult delAres = store.executeSync(delAprep, options);
        delAres.close();

        //System.out.println("\nAfter delete A row:\n");

        /* Consume the last result from the 1st batch */
        iter.next();
        ++numResults;

        /* Fetch the next batch of results and make sure the 1st result in the
        * batch is the correct one */
        res = iter.next();
        assertTrue(res.toJsonString(false).equals("{\"ida2\":1,\"idb\":0,\"idc\":0}"));
        ++numResults;

        while (iter.hasNext()) {
            iter.next();
            ++numResults;
        }

        assertTrue(numResults == 208);
        sr.close();

        //System.out.println("\nBefore delete B row:\n");

        numResults = 0;
        sr = store.executeSync(bs, options);
        iter = sr.iterator();

        for (int i = 0; i < 7; ++i) {
            res = iter.next();
            //System.out.println(res);
            ++numResults;
        }

        PreparedStatement delBprep = store.prepare(deleteBStr, options);
        StatementResult delBres = store.executeSync(delBprep, options);
        delBres.close();

        //System.out.println("\nAfter delete B row:\n");

        /* Consume the last result from the 1st batch */
        res = iter.next();
        ++numResults;

        res = iter.next();
        assertTrue(res.toJsonString(false).equals("{\"ida2\":1,\"idb\":1,\"idc\":0}"));
        ++numResults;

        while (iter.hasNext()) {
            iter.next();
            ++numResults;
        }

        assertTrue(numResults == 198);
        sr.close();

    }

    @Test
    public void testTimeout() {

        boolean verbose = false;

        String tableDDL =
            "CREATE TABLE Foo(               \n" +
            "    id1 INTEGER,                \n" +
            "    id2 INTEGER,                \n" +
            "    id3 INTEGER,                \n" +
            "    firstName STRING,           \n" +
            "    lastName STRING,            \n" +
            "    age INTEGER,                \n" +
            "    id4 STRING,                 \n" +
            "primary key (shard(id1, id2), id3, id4))";

        Random rand = new Random(1);

        int num1 = 100;
        int num2 = 40;
        int num3 = 20;

        executeDdl(tableDDL);

        Table table = tableImpl.getTable(getNamespace(), "foo");

        for (int i = 0; i < num1; i++) {

            for (int j = 0; j < num2; ++j) {

                for (int k = 0; k < num3; ++k) {

                    Row row = table.createRow();
                    row.put("id1", rand.nextInt(num1));
                    row.put("id2", rand.nextInt(num2));
                    row.put("id3", rand.nextInt(num3));
                    row.put("id4", ("id4-" + i));
                    row.put("firstName", ("first" + rand.nextInt(10)));
                    row.put("lastName", ("last" + rand.nextInt(10)));
                    row.put("age",  rand.nextInt(100));

                    tableImpl.putIfAbsent(row, null, null);
                }
            }
        }

        String[] queries = {

             "select * from foo",
             // 1 : This was a runaway query without the timeout fix. After
             //     the fix the query should complete without timeout
             "select * from foo",
             // 2 : This was a runaway query without the timeout fix. After
             //     the fix the query should complete without timeout
             "select * from foo $f where partition($f) = 1",
             // 3 : This was a runaway query without the timeout fix. After
             //     the fix the query should complete without timeout
             "select * from foo $f where age < 0",
             // 4 : Without the timeout fix, the query completes but does not
             //     respect the timeout. With fix, it times out at the driver
             "select * from foo order by age",
             // 5 : Without the timeout fix, the query completes but does not
             //     respect the timeout. With fix, it times out at the driver
             "select age, count(*) from foo group by age",
             // 6 : This was a runaway query without the timeout fix. After
             //     the fix the query should complete without timeout
             "select * from foo order by id1",
        };

        int[] numExpectedResults = { 79568, 79568, 8704, 0, 79568, 100, 79568 };

        testTimeoutSync(queries, numExpectedResults, verbose);

        if (verbose) {
            System.out.println("\n-----------------------------------------\n");
        }

        if (KVStoreConfig.getDefaultUseAsync()) {
            testTimeoutAsync(queries, numExpectedResults, verbose);
        }
    }

    private void testTimeoutSync(
        String[] queries,
        int[] numExpectedResults,
        boolean verbose) {

        int[] timeouts = { 5000, 300,  200,  300, 200, 200, 300 };

        int[] batchSizes = { 100, 3000, 8000, 100, 100, 100, 3000 };

        ExecuteOptions options = new ExecuteOptions();
        options.setNamespace(getNamespace(), false);
        long t = 0;
        boolean inIterLoop = false;

        for (int qid = 0; qid < queries.length; ++qid) {

            options.setTimeout(timeouts[qid], TimeUnit.MILLISECONDS);
            options.setResultsBatchSize(batchSizes[qid]);
            options.setDoPrefetching(false);
            inIterLoop = false;

            try {
                if (qid == 6) {
                    options.setTraceLevel((byte)0);
                } else {
                    options.setTraceLevel((byte)0);
                }

                PreparedStatement ps = store.prepare(queries[qid], options);

                t = System.currentTimeMillis();

                StatementResult sr = store.executeSync(ps, options);

                int numResults = 0;
                Iterator<RecordValue> iter = sr.iterator();
                inIterLoop = true;

                while (iter.hasNext()) {
                    iter.next();
                    ++numResults;
                }

                t = System.currentTimeMillis() - t;

                sr.close();

                assertTrue(qid == 2 || numResults == numExpectedResults[qid]);

                if (qid == 4 || qid == 5) {
                    fail("Query " + qid + " should have timed out");
                }

                if (verbose) {
                    System.out.println("Query " + qid + " completed in " +
                                       t + " msec and produced " +
                                       numResults + " results");
                }

            } catch (RequestTimeoutException e) {

                t = System.currentTimeMillis() - t;
                if (verbose) {
                    System.out.println("Query " + qid + " timed out after " +
                                       t + " msec. Got in iter loop = " +
                                       inIterLoop + "\n" + e.getMessage());
                }
            } catch (StoreIteratorException e) {
                // the subscription iterator wraps RTE in StoreIteratorException
                if (options.isAsync() &&
                    (e.getCause() instanceof RequestTimeoutException)) {
                    if (verbose) {
                        System.out.println("Query " + qid + " timed out." +
                                " Got in iter loop = " +
                                inIterLoop + "\n" + e.getMessage());
                    }
                } else {
                  throw e;
                }
            }
        }
    }

    private void testTimeoutAsync(
        String[] queries,
        int[] numExpectedResults,
        boolean verbose) {

        final ExecuteOptions options = new ExecuteOptions();
        options.setNamespace(getNamespace(), false);
        options.setDoPrefetching(false);

        class TestSubscriber implements Subscriber<RecordValue> {

            int theBatchSize;

            long theTimeout;

            Subscription theSubscription;

            int theNumResults;

            Throwable theError;

            boolean theIsDone;

            long theTime;

            TestSubscriber(ExecuteOptions opts) {
                theBatchSize = opts.getResultsBatchSize();
                theTimeout = opts.getTimeout();
            }

            @Override
            public void onSubscribe(Subscription s) {

                theSubscription = s;
                theTime = System.currentTimeMillis();
                s.request(theBatchSize);
            }

            @Override
            public void onNext(RecordValue result) {

                ++theNumResults;

                long t = System.currentTimeMillis();

                if (t - theTime > theTimeout * 1.2) {
                    throw new RuntimeException(
                        "Time between onNext() calls was " + (t - theTime) +
                        " msec, which exceeded the timeout of " + theTimeout);
                }

                theTime = t;

                if (theNumResults % theBatchSize == 0) {
                    theSubscription.request(theBatchSize);
                }

                return;
            }

            @Override
            public synchronized void onError(Throwable e) {

                theError = e;
                theIsDone = true;
                notifyAll();
            }

            @Override
            public synchronized void onComplete() {
                theIsDone = true;
                notifyAll();
            }
        }

        int[] timeouts = { 5000, 300,  200,  200, 200, 100, 300 };

        int[] batchSizes = { 100, 3000, 8000, 100, 100, 100, 3000 };

        long t = 0;

        for (int qid = 0; qid < queries.length; ++qid) {

            options.setTimeout(timeouts[qid], TimeUnit.MILLISECONDS);
            options.setResultsBatchSize(batchSizes[qid]);

            if (qid == 3) {
                options.setTraceLevel((byte)0);
            } else {
                options.setTraceLevel((byte)0);
            }

            PreparedStatement ps = store.prepare(queries[qid], options);

            t = System.currentTimeMillis();

            Publisher<RecordValue> publisher = store.executeAsync(ps, options);
            TestSubscriber subscriber = new TestSubscriber(options);
            publisher.subscribe(subscriber);

            synchronized (subscriber) {
                while (!subscriber.theIsDone) {
                    try {
                        subscriber.wait();
                    } catch (InterruptedException e) {
                        System.out.println(
                            "Test thread interrupted while waiting " +
                                           "for async query");
                    }
                }
            }

            t = System.currentTimeMillis() - t;

            if (subscriber.theError != null &&
                !(subscriber.theError instanceof RequestTimeoutException)) {
                fail("Query " + qid + " failed with exception:\n" +
                     subscriber.theError);
                return;
            }

            if (subscriber.theError != null) {
                if (verbose) {
                    System.out.println(
                        "Query " + qid + " timed out after " + t +
                        " msec. " + subscriber.theError.getMessage());
                }
                continue;
            }

            if (qid == 4 || qid == 5) {
                fail("Query " + qid + " completed in " + t +
                     " msec but should have timed out");
            }

            if (verbose) {
                System.out.println("Query " + qid + " completed in " +
                                   t + " msec and produced " +
                                   subscriber.theNumResults + " results");
            }

            assertTrue(qid == 2 ||
                       subscriber.theNumResults == numExpectedResults[qid]);
        }
    }

    @Test
    public void test27144() {

        String query =
            "select id, expiration_time($f) as exptime from foo $f";

        //TODO: allow MRTable mode after MRTable supports TTL.
        executeDdl("create table foo(id integer, int integer, primary key(id))",
                   true, true/*noMRTableMode*/);

        Table table = tableImpl.getTable(getNamespace(), "foo");

        Row row = table.createRow();
        row.put("id", 1);
        row.put("int", 10);
        tableImpl.put(row, null, null);

        executeDML(query, 0);

        executeDdl("alter table foo using ttl 1 hours");

        table = tableImpl.getTable(getNamespace(), "foo");

        row = table.createRow();
        row.put("id", 2);
        row.put("int", 20);
        tableImpl.put(row, null, null);

        executeDML(query, 1);

        executeDML("upsert into foo values(1, 60)", 2);

        ExecuteOptions options = new ExecuteOptions().
                                 setNamespace(getNamespace(), false);
        StatementResult sr = store.executeSync(query, options);
        Iterator<RecordValue> iter = sr.iterator();

        RecordValue res1 = iter.next();
        RecordValue res2 = iter.next();

        int id1 = res1.get(0).asInteger().get();
        int id2 = res2.get(0).asInteger().get();
        String exptime;

        if (id1 == 1) {
            exptime = res1.get(1).asTimestamp().toJsonString(false);
            assertTrue(exptime.equals("\"1970-01-01T00:00:00.000Z\""));
            return;
        }

        assertEquals(id2, 1);

        exptime = res2.get(1).asTimestamp().toJsonString(false);
        assertTrue(exptime.equals("\"1970-01-01T00:00:00.000Z\""));
    }

    @Test
    public void testEvolveKeyOnlyTable() {

        executeDdl("create table foo(id1 integer, id2 integer," +
                   " primary key(id1, id2))");

        Table table = tableImpl.getTable(getNamespace(), "foo");

        Row row = table.createRow();
        row.put("id1", 1);
        row.put("id2", 1);
        tableImpl.put(row, null, null);

        executeDdl("alter table foo (add str string)");

        table = tableImpl.getTable(getNamespace(), "foo");
        row = table.createRow();
        row.put("id1", 1);
        row.put("id2", 2);
        row.put("str", "abc");
        tableImpl.put(row, null, null);

        executeDML("select * from foo", 0);

        executeDdl("create index idx on foo (str)");

        executeDML("select * from foo where str >= \"a\"", 1);

        executeDML("select * from foo where str IS NULL", 2);

        executeDdl("alter table foo (add dbl double)");

        table = tableImpl.getTable(getNamespace(), "foo");
        row = table.createRow();
        row.put("id1", 1);
        row.put("id2", 3);
        row.put("str", "abc");
        row.put("dbl", 1.5);
        tableImpl.put(row, null, null);

        executeDML("select * from foo", 3);
    }

    @Test
    public void testArrayIndex() {
        executeDdl("create table fooArrIdx1(id integer, primary key(id), " +
                   "array array(integer))");

        executeDdl("create index idx1 on fooArrIdx1(array)", false);
        executeDdl("create index idx2 on fooArrIdx1(array[])");


        executeDdl("create table fooArrIdx2(id integer, primary key(id), " +
                   "array array(array(integer)))");

        executeDdlErrContainsMsg("create index idx3 on fooArrIdx2(array)",
                                 "Missing [] after array field");

        executeDdlErrContainsMsg("create index idx4 on fooArrIdx2(array[])",
                                 "Missing [] after array field");
    }

    @Test
    public void testMapIndex() {
        executeDdl("create table fooMapIdx1(id integer, primary key(id), " +
            "map MAP(integer))");

        executeDdl("create index idx11 on fooMapIdx1(map)", false);
        executeDdl("create index idx12 on fooMapIdx1(map.values())");

        executeDdl(
            "create table fooMapIdx2(id integer, primary key(id), " +
            "map MAP(MAP(integer)))");

        executeDdlErrContainsMsg(
            "create index idx13 on fooMapIdx2(map)",
            "Can not index a map as a whole; use .values() to index the " +
            "elements of the map or .keys() to index the keys of the map");

        executeDdlErrContainsMsg(
            "create index idx14 on fooMapIdx2(map.values())",
            "Can not index a map as a whole; use .values() to index the " +
            "elements of the map or .keys() to index the keys of the map");
    }

    @Test
    public void testUserTable() throws Exception {

        executeDdl(userTableStatement);
        addUsers(10);

        String query1 = "select lastName, age from Users";

        String query2 = "select id from Users where age > 13";

        String query3 =
            "select firstName from Users where lastName = \"last3\"";

        String query4 = "select * from Users";

        String query5 = "select * from Users where age = 10";

        String query6 =
            "select * from Users " +
            "where (age > 13 and age < 17) or \n" +
            "lastName = \"last9\" and firstName > \"first\"";

        String query7 = "select * from Users where age > 12.5";

        /*
         * TODO:
         * o add assertions about the size/shape of the RecordValues returned
         * o push expected number into executeDML for assertions?
         * o add more complex queries.
         */

        List<RecordValue> results = executeDML(query1, 1);
        assertEquals("Unexpected number of results", 10, results.size());

        results = executeDML(query2, 2);
        assertEquals("Unexpected number of results", 6, results.size());

        results = executeDML(query3, 3);
        assertEquals("Unexpected number of results", 1, results.size());

        results = executeDML(query4, 4);
        assertEquals("Unexpected number of results", 10, results.size());

        results = executeDML(query5, 5);
        assertEquals("Unexpected number of results", 1, results.size());

        results = executeDML(query6, 6);
        assertEquals("Unexpected number of results", 4, results.size());

        results = executeDML(query7, 7);
        assertEquals("Unexpected number of results", 7, results.size());
    }

    @Test
    public void testQueryWithNs() throws Exception {

        final String ns1 = "ns001";
        final String ns2 = "ns002";
        executeDdl("DROP NAMESPACE IF EXISTS " + ns1 + " CASCADE", null);
        executeDdl("DROP NAMESPACE IF EXISTS " + ns2 + " CASCADE", null);

        executeDdl("CREATE NAMESPACE " + ns1, null);
        executeDdl(userTableStatement, ns1);
        addUsersToTable(tableImpl.getTable(ns1, "Users"), 10);

        executeDdl("CREATE NAMESPACE " + ns2, null);
        executeDdl(userTableStatement, ns2);
        addUsersToTable(tableImpl.getTable(ns2, "Users"), 20);

        executeDdl(userTableStatement, null);
        addUsersToTable(tableImpl.getTable(null, "Users"), 30);

        /**
         * case 1: neither user query statement nor ExecuteOptions specifies a
         * namespace. The target table should be "Users".
         */
        String query = "SELECT * FROM Users";
        StatementResult sr = executeDml(query, (String) null);
        assertEquals("Unexpected number of results", 30, countRecords(sr));

        /**
         * case 2: user query statement does not specify a namespace, while
         * ExecuteOptions does. The target table should be "ns1:Users".
         */
        sr = executeDml(query, ns1);
        assertEquals("Unexpected number of results", 10, countRecords(sr));

        /**
         * case 3: user query statement and ExecuteOptions both specify the
         * namespace "ns1". The target table should be "ns1:Users".
         */
        query = "SELECT * FROM " + ns1 + ":" + "Users";
        sr = executeDml(query, ns1);
        assertEquals("Unexpected number of results", 10, countRecords(sr));

        /**
         * case 4: user query statement and the ExecuteOptions both specify a
         * namespace "ns1", but the namespace names are not the same case.
         */
        query = "SELECT * FROM " +
            ns1.toUpperCase(Locale.ENGLISH) + ":" + "Users";
        sr = executeDml(query, ns1);
        assertEquals("Unexpected number of results", 10, countRecords(sr));

        /**
         * case 5: user query statement and the ExecuteOptions both specify a
         * namespace, but the namespace names are different.
         */
        query = "SELECT * FROM " + ns2 + ":" + "Users";
        sr = executeDml(query, ns1);
        assertEquals("Unexpected number of results", 20, countRecords(sr));

        executeDdl("DROP NAMESPACE IF EXISTS " + ns1 + " CASCADE", null);
        executeDdl("DROP NAMESPACE IF EXISTS " + ns2 + " CASCADE", null);
    }

    /**
     * This calls the public API, which now sends queries to the server for
     * execution. There will be other unit tests for the API in general.
     */
    @Test
    public void testAPI() throws Exception {

        int numUsers = 30;

        executeDdl(userTableStatement);
        addUsers(numUsers);

        String query1 = "select lastName, age from Users where age > 10";
        String queryWithKey =
            "select id, lastName, age from Users where id > 10 and id < 15";

        StatementResult sr = executeDml(query1);
        Iterator<RecordValue> iter = sr.iterator();

        int numResults = 0;
        while (iter.hasNext()) {
            RecordValue rv = iter.next();
            ++numResults;
            if (showResult) {
                System.out.println(rv);
            }
        }
        sr.close();

        assertEquals("Unexpected number of results", numUsers - 1, numResults);

        sr = executeDml(queryWithKey);
        iter = sr.iterator();

        numResults = 0;
        while (iter.hasNext()) {
            RecordValue rv = iter.next();
            ++numResults;
            if (showResult) {
                System.out.println(rv);
            }
        }
        sr.close();
        assertEquals("Unexpected number of results", 4, numResults);
    }

    @Test
    public void testUpdateQuery() throws Exception {
        final String tableName = "testUpdateQuery";
        final String tableDdl =
            "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                "sid INTEGER, " +
                "id INTEGER, " +
                "i INTEGER, " +
                "PRIMARY KEY(SHARD(sid), id))";
        TableLimits limits = new TableLimits(5000, 5000, 1);
        final int numSids = 2;
        final int numIdsPerSid = 10;

        final String updBySid = "UPDATE " + tableName +
                                " SET i = i + 1 WHERE sid = 0";
        final String qryBySid = "SELECT id, i FROM " + tableName +
                                " WHERE sid = 0";
        final int updRKB = 20;
        final int updWKB = 20;

        final ExecuteOptions noChargeOptions = new ExecuteOptions();
        noChargeOptions.setNoCharge(true);

        /* Create table with limits */
        ExecutionFuture f =
            ((KVStoreImpl)store).execute(tableDdl.toCharArray(),
                                         null, /* ExecuteOptions*/
                                         limits);
        StatementResult sr = f.get();
        assertTrue(sr.isSuccessful());

        /* Load rows to table */
        Table table = tableImpl.getTable(tableName);
        for (int sid = 0; sid < numSids; sid++) {
            for (int id = 0; id < numIdsPerSid; id++) {
                Row row = table.createRow();
                row.put("sid", sid).put("id", id).put("i", id);
                Version ver = tableImpl.put(row, null, null);
                assertNotNull(ver);
            }
        }

        PreparedStatement stmtUpd = store.prepare(updBySid);
        PreparedStatement stmtQry = store.prepare(qryBySid);

        ExecuteOptions options;
        List<RecordValue> results;
        int inc = 0;

        /*
         * Test maxReadKB/maxWriteKB
         */

        /*
         * maxReadKB = the required readKB/2, update should fail
         */
        options = new ExecuteOptions().setMaxReadKB(updRKB/2);
        try {
            execQuerySync(stmtUpd, options, updRKB, updWKB);
            fail("expect to fail");
        } catch (RuntimeException ex) {
            /* check current rows with sid = 0 */
            results = execQuerySync(stmtQry, noChargeOptions, 0, 0);
            assertEquals(numIdsPerSid, results.size());
            for (RecordValue val : results) {
                assertEquals(val.get("id").asInteger().get() + inc,
                             val.get("i").asInteger().get());
            }
        }

       /*
        * maxReadKB = the required readKB
        */
        inc++;
        options = new ExecuteOptions().setMaxReadKB(updRKB);
        results = execQuerySync(stmtUpd, options, updRKB, updWKB);
        assertEquals(1, results.size());
        assertTrue(results.get(0).toJsonString(false)
                          .contains("\"NumRowsUpdated\":" + numIdsPerSid));

        /* check current rows with sid = 0 */
        results = execQuerySync(stmtQry, noChargeOptions, 0, 0);
        assertEquals(numIdsPerSid, results.size());
        for (RecordValue val : results) {
            assertEquals(val.get("id").asInteger().get() + inc,
                         val.get("i").asInteger().get());
        }

        /*
         * maxWriteKB = the required writeKB/2, update should fail
         */
        options = new ExecuteOptions()
                    .setMaxReadKB(updRKB)
                    .setMaxWriteKB(updWKB/2);
        try {
            execQuerySync(stmtUpd, options, 0, 0);
            fail("expect to fail");
        } catch (RuntimeException ex) {
            /* check current rows with sid = 0 */
            results = execQuerySync(stmtQry, noChargeOptions, 0, 0);
            assertEquals(numIdsPerSid, results.size());
            for (RecordValue val : results) {
                assertEquals(val.get("id").asInteger().get() + inc,
                             val.get("i").asInteger().get());
            }
        }

        /*
         * maxWriteKB = the required writeKB
         * maxReadKB = the required readKB
         */
        inc++;
        options = new ExecuteOptions()
                    .setMaxReadKB(updRKB)
                    .setMaxWriteKB(updWKB);
        results = execQuerySync(stmtUpd, options, updRKB, updWKB);
        assertTrue(results.get(0).toJsonString(false)
                          .contains("\"NumRowsUpdated\":" + numIdsPerSid));

        results = execQuerySync(stmtQry, noChargeOptions, 0, 0);
        assertEquals(numIdsPerSid, results.size());
        for (RecordValue val : results) {
            assertEquals(val.get("id").asInteger().get() + inc,
                         val.get("i").asInteger().get());
        }

        /*
         * Test ExecuteOptions.updateLimit
         */
        inc++;
        options = new ExecuteOptions();
        options.setUpdateLimit(10);
        results = execQuerySync(stmtUpd, options, 0, 0);

        options.setUpdateLimit(9);
        try {
            execQuerySync(stmtUpd, options, 0, 0);
            fail("expect to fail");
        } catch (RuntimeException ex) {
            /* check current rows with sid = 0 */
            results = execQuerySync(stmtQry, noChargeOptions, 0, 0);
            assertEquals(numIdsPerSid, results.size());
            for (RecordValue val : results) {
                assertEquals(val.get("id").asInteger().get() + inc,
                             val.get("i").asInteger().get());
            }
        }

        /*
         * Test throttling
         */

        /*
         * TableLimits(10, 1000, 1), update query should be throttled.
         */
        limits = new TableLimits(10, 5000, 1);
        f = ((KVStoreImpl)store).setTableLimits(null, tableName, limits);
        assertTrue(f.get().isSuccessful());
        Thread.sleep(2000);

        final int numRounds = 3;
        boolean throttled = false;

        try {
            for (int i = 0; i < numRounds; i++) {
                execQuerySync(stmtUpd, null, 0, 0);
                inc++;
            }
        } catch (ReadThroughputException ex) {
            throttled = true;
        }
        assertTrue(throttled);

        /*
         * TableLimits(1000, 10, 1), update query should be throttled.
         */
        limits = new TableLimits(5000, 10, 1);
        f = ((KVStoreImpl)store).setTableLimits(null, tableName, limits);
        assertTrue(f.get().isSuccessful());
        Thread.sleep(2000);

        throttled = false;
        try {
            for (int i = 0; i < numRounds; i++) {
                execQuerySync(stmtUpd, null, 0, 0);
                inc++;
            }
        } catch (WriteThroughputException ex) {
            throttled = true;
        }
        assertTrue(throttled);

        /* Check current rows with sid = 0 */
        results = execQuerySync(stmtQry, noChargeOptions, 0, 0);
        assertEquals(numIdsPerSid, results.size());
        for (RecordValue val : results) {
            assertEquals(val.get("id").asInteger().get() + inc,
                         val.get("i").asInteger().get());
        }
    }

    private List<RecordValue> execQuerySync(PreparedStatement stmt,
                                            ExecuteOptions options,
                                            int expRB,
                                            int expWB) {
        if (showResult) {
            System.out.println("\n" +
                String.valueOf(((PreparedStatementImpl)stmt).getQueryString()));
        }

        if (options == null) {
            options = new ExecuteOptions();
        } else {
            options.setContinuationKey(null);
        }
        options.setAsync(false);

        List<RecordValue> results = new ArrayList<>();
        int rkb = 0;
        int wkb = 0;
        int batches = 0;
        int num = 0;
        int batchSize = options.getResultsBatchSize();

        while(true) {
            QueryStatementResultImpl result =
                (QueryStatementResultImpl)store.executeSync(stmt, options);

            rkb += result.getReadKB();
            wkb += result.getWriteKB();
            batches++;

            num = 0;
            for (RecordValue r : result) {
                results.add(r);
                if (showResult) {
                    String s = r.toJsonString(false);
                    if (s.length() > 30) {
                        s = s.substring(0, 30) + "...";
                    }
                    System.out.println("[" + batches + "-" + results.size() +
                                       "] " + s);
                }
                num++;
            }
            if (batchSize > 0) {
                assertTrue(num <= batchSize);
            }

            if (showResult) {
                System.out.println("\t" + batches + ": rkb=" + result.getReadKB() +
                                   ", wkb=" + result.getWriteKB());
            }
            if (result.getContinuationKey() == null) {
                break;
            }
            options.setContinuationKey(result.getContinuationKey());
        }

        if (showResult) {
            System.out.println(batches + "-" + results.size() +
                               " rows returned, kb(r|w): " + rkb + "|" + wkb);
        }
        if (expRB > 0) {
            assertEquals(expRB, rkb);
        }
        if (expWB > 0) {
            assertEquals(expWB, wkb);
        }
        return results;
    }

    final static String complexTableStatement =
        "CREATE TABLE Complex" +
        "(id INTEGER, firstName STRING, lastName STRING, age INTEGER," +
        " ptr STRING," +
        " address RECORD( " +
        "             city STRING, " +
        "             state STRING, " +
        "             phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ), " +
        "             ptr STRING), " +
        " children MAP( RECORD( age LONG, friends ARRAY(STRING) ) )," +
        "primary key (id))";

    String json0 =
        "{                                                        \n" +
        "   \"city\"   : \"San Fransisco\",                       \n" +
        "   \"state\"  : \"CA\",                                  \n" +
        "   \"phones\" : [                                        \n" +
        "                  { \"work\" : 504,  \"home\" : 50 },    \n" +
        "                  { \"work\" : 518,  \"home\" : 51 },    \n" +
        "                  { \"work\" : 528,  \"home\" : 52 },    \n" +
        "                  { \"work\" : 538,  \"home\" : 53 },    \n" +
        "                  { \"work\" : 548,  \"home\" : 54 }     \n" +
        "                ],                                       \n" +
        "   \"ptr\"    : \"city\"                                 \n" +
        "}";

    String json1 =
        "{                                                        \n" +
        "   \"city\"   : \"Boston\",                              \n" +
        "   \"state\"  : \"MA\",                                  \n" +
        "   \"phones\" : [                                        \n" +
        "                  { \"work\" : 304,  \"home\" : 30 },    \n" +
        "                  { \"work\" : 318,  \"home\" : 31 }     \n" +
        "                ],                                       \n" +
        "   \"ptr\"    : \"state\"                                \n" +
        "}";

    String json2 =
        "{                                                        \n" +
        "   \"city\"   : \"Portland\",                            \n" +
        "   \"state\"  : \"OR\",                                  \n" +
        "   \"phones\" : [                                        \n" +
        "                  { \"work\" : 104,  \"home\" : 10 },    \n" +
        "                  { \"work\" : 118,  \"home\" : 11 }     \n" +
        "                ],                                       \n" +
        "   \"ptr\"    : \"phones\"                               \n" +
        "}";

    String json3 =
        "{                                                        \n" +
        "   \"city\"   : \"Seattle\",                             \n" +
        "   \"state\"  : \"WA\",                                  \n" +
        "   \"phones\" : [                                        \n" +
        "                ],                                       \n" +
        "   \"ptr\"    : \"phones\"                               \n" +
        "}";

    String children0_doc =
        "{                                                        \n" +
        "  \"John\" : { \"age\" : 10, \"friends\" : [\"Anna\", \"John\", \"Maria\"]},\n" +
        "  \"Lisa\" : { \"age\" : 12, \"friends\" : [\"Ada\"]}    \n" +
        "}";

    String children1_doc =
        "{                                                                 \n" +
        "  \"Anna\" : { \"age\" : 9, \"friends\" : [\"Bobby\", \"John\"]}, \n" +
        "  \"Mark\" : { \"age\" : 4, \"friends\" : [\"George\"]}           \n" +
        "}";

    String children2_doc =  "{}";

    String children3_doc =
        "{                                                                  \n" +
        "  \"George\" : { \"age\" : 7, \"friends\" : [\"Bill\", \"Mark\"]}, \n" +
        "  \"Matt\" : { \"age\" : 14, \"friends\" : [\"Bill\"]}             \n" +
        "}";

    String address_docs[] = { json0, json1, json2, json3 };

    String children_docs[] = {
        children0_doc, children1_doc, children2_doc, children3_doc
    };

    @Test
    public void testFieldSteps() throws Exception {

        executeDdl(complexTableStatement);
        addComplex(4);

        String q0 = "select lastName, C.address.city from Complex C";

        String[] expected0 = {
            "{\"lastName\":\"last2\",\"city\":\"Portland\"}",
            "{\"lastName\":\"last0\",\"city\":\"San Fransisco\"}",
            "{\"lastName\":\"last1\",\"city\":\"Boston\"}",
            "{\"lastName\":\"last3\",\"city\":\"Seattle\"}"
        };

        String q1 = "select lastName, Complex.address.city from Complex";

        String[] expected1 = {
            "{\"lastName\":\"last2\",\"city\":\"Portland\"}",
            "{\"lastName\":\"last0\",\"city\":\"San Fransisco\"}",
            "{\"lastName\":\"last1\",\"city\":\"Boston\"}",
            "{\"lastName\":\"last3\",\"city\":\"Seattle\"}"
        };

        String q2 = "select lastName, $C.address.city from Complex $C";

        String[] expected2 = {
            "{\"lastName\":\"last2\",\"city\":\"Portland\"}",
            "{\"lastName\":\"last0\",\"city\":\"San Fransisco\"}",
            "{\"lastName\":\"last1\",\"city\":\"Boston\"}",
            "{\"lastName\":\"last3\",\"city\":\"Seattle\"}"
        };

        String q3 = "select lastName, [ C.address.phones.work ] from Complex C";

        String[] expected3 = {
            "{\"lastName\":\"last2\",\"Column_2\":[104,118]}",
            "{\"lastName\":\"last0\",\"Column_2\":[504,518,528,538,548]}",
            "{\"lastName\":\"last1\",\"Column_2\":[304,318]}",
            "{\"lastName\":\"last3\",\"Column_2\":[]}"
        };

        String q4 = "select lastName, $C.address.($.ptr) from Complex $C";

        String[] expected4 = {
            "{\"lastName\":\"last2\",\"Column_2\":[{\"work\":104,\"home\":10},{\"work\":118,\"home\":11}]}",
            "{\"lastName\":\"last0\",\"Column_2\":\"San Fransisco\"}",
            "{\"lastName\":\"last1\",\"Column_2\":\"MA\"}",
            "{\"lastName\":\"last3\",\"Column_2\":[]}"
        };

        String q5 = "select lastName, C.address.city2 from Complex C";

        String[] expected5 = {
            "Compiler Exception"
        };

        String q6 = "select lastName, C.address.($.ptr).foo from Complex C";

        String[] expected6 = {
            "Runtime Exception"
        };

        String q7 = "select lastName, ($C).($.ptr) from Complex $C";

        String[] expected7 = {
            "{\"lastName\":\"last2\",\"Column_2\":\"first2\"}",
            "{\"lastName\":\"last0\",\"Column_2\":\"first0\"}",
            "{\"lastName\":\"last1\",\"Column_2\":\"last1\"}",
            "{\"lastName\":\"last3\",\"Column_2\":\"last3\"}"
        };

        String[] queries = { q0, q1, q2, q3, q4, q5, q6, q7 };

        String[][] expectedResults = {
            expected0, expected1, expected2, expected3, expected4,
            expected5, expected6, expected7
        };

        for (int i = 0; i < queries.length; ++i) {
            List<RecordValue> results = executeDML(queries[i], i);
            checkResults(i, expectedResults[i], results, false);
        }
    }

    @Test
    public void testPrimaryIndex() throws Exception {

        String tableStatement =
            "CREATE TABLE Users(             \n" +
            "    id1 INTEGER,                \n" +
            "    id2 DOUBLE,                 \n" +
            "    id3 ENUM(tok0, tok1, tok2), \n" +
            "    firstName STRING,           \n" +
            "    lastName STRING,            \n" +
            "    age INTEGER,                \n" +
            "    id4 STRING,                 \n" +
            "primary key (shard(id1), id2, id3, id4))";

        executeDdl(tableStatement);

        Table table = tableImpl.getTable(getNamespace(), "Users");

        for (int i = 1; i < 6; i++) {

            for (int j = 0; j < 3; ++j) {

                Row row = table.createRow();
                row.put("id1", i);
                row.put("id2", i * 10.0 + j);
                row.putEnum("id3", ("tok" + (i % 3)));
                row.put("id4", ("id4-" + i));
                row.put("firstName", ("first" + i));
                row.put("lastName", ("last" + i));
                row.put("age", i+10);

                tableImpl.put(row, null, null);
            }
        }

        // partial key and range, plus always-true preds
        String q0 =
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where id2 < 42 and id1 = 4 and id1 > 0 and id2 < 50 and id4 > \"id4\"";

        String[] expected0 = {
            "{\"id1\":4,\"id2\":41.0,\"id4\":\"id4-4\"}",
            "{\"id1\":4,\"id2\":40.0,\"id4\":\"id4-4\"}"
        };

        // partial key and range, plus always-true preds
        String q1 =
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where 42 > id2 and 0 < id1 and id1 = 4 and id2 < 50 and id4 > \"id4\"";

        String[] expected1 = {
            "{\"id1\":4,\"id2\":41.0,\"id4\":\"id4-4\"}",
            "{\"id1\":4,\"id2\":40.0,\"id4\":\"id4-4\"}"
        };

        // always false
        String q2 =
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where 42 > id2 and 0 < id1 and id1 = 4 and id2 > 50 and id4 > \"id4\"";

        String[] expected2 = {
        };

        // complete primary key
        String q3 =
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where id2 = 30 and id1 = 3 and id1 > 0 and id2 < 50 and \n" +
            "      id4 = \"id4-3\" and id3 = \"tok0\" ";

        String[] expected3 = {
            "{\"id1\":3,\"id2\":30.0,\"id4\":\"id4-3\"}",
        };

        // primary key gap
        String q4 =
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where id2 = 30 and id1 = 3 and id4 = \"id4-3\" ";

        String[] expected4 = {
            "{\"id1\":3,\"id2\":30.0,\"id4\":\"id4-3\"}",
        };

        // equality via two range preds
        String q5 =
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where id2 <= 30 and id1 = 3 and id4 = \"id4-3\" and 30 <= id2";

        String[] expected5 = {
            "{\"id1\":3,\"id2\":30.0,\"id4\":\"id4-3\"}",
        };

        // nothing pushed
        String q6 =
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where id2 <= 30";

        String[] expected6 = {
            "{\"id1\":2,\"id2\":20.0,\"id4\":\"id4-2\"}",
            "{\"id1\":2,\"id2\":22.0,\"id4\":\"id4-2\"}",
            "{\"id1\":1,\"id2\":10.0,\"id4\":\"id4-1\"}",
            "{\"id1\":1,\"id2\":12.0,\"id4\":\"id4-1\"}",
            "{\"id1\":1,\"id2\":11.0,\"id4\":\"id4-1\"}",
            "{\"id1\":2,\"id2\":21.0,\"id4\":\"id4-2\"}",
            "{\"id1\":3,\"id2\":30.0,\"id4\":\"id4-3\"}"
        };

        // range only
        String q7 =
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where 1 < id1 and id1 <= 3 and id2 = 20";

        String[] expected7 = {
            "{\"id1\":2,\"id2\":20.0,\"id4\":\"id4-2\"}"
        };

        // range only and always true pred
        String q8 =
            "select id1   \n" +
            "from Users C \n" +
            "where 4 <= id1 and 4 < id1";

        String[] expected8 = {
            "{\"id1\":5}",
            "{\"id1\":5}",
            "{\"id1\":5}"
        };

        // range only and always false pred
        String q9 =
            "select id1   \n" +
            "from Users C \n" +
            "where 4 > id1 and 4 <= id1";

        String[] expected9 = {
        };

        // range only and always false pred
        String q10 =
            "select id1   \n" +
            "from Users C \n" +
            "where 4 >= id1 and 4 < id1";

        String[] expected10 = {
        };

        // range only and always true pred
        String q11 =
            "select id1   \n" +
            "from Users C \n" +
            "where 2 >= id1 and 2 > id1";

        String[] expected11 = {
            "{\"id1\":1}",
            "{\"id1\":1}",
            "{\"id1\":1}"
        };

        // range only and always true pred
        String q12 =
            "select id1   \n" +
            "from Users C \n" +
            "where 4 < id1 and 4 <= id1";

        String[] expected12 = {
            "{\"id1\":5}",
            "{\"id1\":5}",
            "{\"id1\":5}"
        };

        // complete primary key, with no remaining preds
        String q13 =
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where id2 = 30 and id1 = 3 and id4 = \"id4-3\" and id3 = \"tok0\" ";

        String[] expected13 = {
            "{\"id1\":3,\"id2\":30.0,\"id4\":\"id4-3\"}",
        };


        // range only and orderby
        String q14 =
            "select id1, id2 \n" +
            "from Users C \n" +
            "where 4 <= id1\n" +
            "order by id1, id2";

        String[] expected14 = {
            "{\"id1\":4,\"id2\":40.0}",
            "{\"id1\":4,\"id2\":41.0}",
            "{\"id1\":4,\"id2\":42.0}",
            "{\"id1\":5,\"id2\":50.0}",
            "{\"id1\":5,\"id2\":51.0}",
            "{\"id1\":5,\"id2\":52.0}"
        };

        // range only and orderby
        String q15 =
            "select id1, age \n" +
            "from Users C \n" +
            "where 4 <= id1\n" +
            "order by id1, id2";

        String[] expected15 = {
            "{\"id1\":4,\"age\":14}",
            "{\"id1\":4,\"age\":14}",
            "{\"id1\":4,\"age\":14}",
            "{\"id1\":5,\"age\":15}",
            "{\"id1\":5,\"age\":15}",
            "{\"id1\":5,\"age\":15}"
        };

        /*
        String[] queries = { q15 };
        String[][] expectedResults = { expected15 };
        boolean[] expectedPushedKey = { false };
        boolean[] expectedPushedRange = { true };
        */
        String[] queries = { q0,  q1, q2, q3, q4, q5, q6, q7, q8, q9, q10,
                             q11, q12, q13, q14, q15 };
        String[][] expectedResults = {
            expected0,  expected1,  expected2, expected3, expected4,
            expected5,  expected6,  expected7, expected8, expected9,
            expected10, expected11, expected12, expected13,
            expected14, expected15
        };

        boolean[] sorted = {
            false, false, false, false, false, false, false, false, false,
            false, false, false, false, false, true, true
        };

        for (int i = 0; i < queries.length; ++i) {
            List<RecordValue> results = executeDML(queries[i], i);
            checkResults(i, expectedResults[i], results, sorted[i]);
        }

        // partial key and range, plus always-true preds
        String q20 =
            "declare $ext integer;\n" +
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where id2 < 42 and id1 = 4 and id1 > 0 and \n" +
            "      id2 < $ext and id4 > \"id4\"";

        String[] expected20 = {
            "{\"id1\":4,\"id2\":40.0,\"id4\":\"id4-4\"}"
        };

        HashMap<String, FieldValue> vars20 = new HashMap<String, FieldValue>();
        vars20.put("$ext", FieldDefImpl.Constants.integerDef.createValue(41));

        // partial key and range, plus always-true preds
        String q21 =
            "declare \n" +
            "$ext1 integer;\n" +
            "$ext2 integer;\n" +
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where id2 < 42 and id1 = $ext1 and id1 > 0 and id2 < $ext2 and\n" +
            "      id4 > \"id4\"";

        String[] expected21 = {
            "{\"id1\":4,\"id2\":40.0,\"id4\":\"id4-4\"}"
        };

        HashMap<String, FieldValue> vars21 = new HashMap<String, FieldValue>();
        vars21.put("$ext1", FieldDefImpl.Constants.integerDef.createValue(4));
        vars21.put("$ext2", FieldDefImpl.Constants.integerDef.createValue(41));

        // partial key and range, plus always-true preds
        String q22 =
            "declare \n" +
            "$ext1 integer;\n" +
            "$ext2 integer;\n" +
            "$ext3 integer;\n" +
            "select id1, id2, id4\n" +
            "from Users C\n" +
            "where id2 < $ext3 + $ext2 - 41 and id1 = $ext1 + 2 - 2 and \n" +
            "      id1 > 0 and id2 < $ext2 and id4 > \"id4\"";

        String[] expected22 = {
            "{\"id1\":4,\"id2\":40.0,\"id4\":\"id4-4\"}"
        };

        HashMap<String, FieldValue> vars22 = new HashMap<String, FieldValue>();
        vars22.put("$ext1", FieldDefImpl.Constants.integerDef.createValue(4));
        vars22.put("$ext2", FieldDefImpl.Constants.integerDef.createValue(41));
        vars22.put("$ext3", FieldDefImpl.Constants.integerDef.createValue(42));

        String[] queries2 = { q20, q21, q22 };
        String[][] expectedResults2 = {
            expected20, expected21, expected22
        };

        ArrayList<HashMap<String, FieldValue>> vars =
            new ArrayList<HashMap<String, FieldValue>>();

        vars.add(vars20);
        vars.add(vars21);
        vars.add(vars22);

        for (int i = 0; i < queries2.length; ++i) {

            List<RecordValue> results = executeDML(
                queries2[i], i+20, null, vars.get(i));

            checkResults(i, expectedResults2[i], results, false);
        }
    }

    @Test
    public void testSecondaryIndex() throws Exception {

        executeDdl(complexTableStatement);

        String index1 =
            "CREATE INDEX idx_state_city_age ON Complex" +
                          "(address.state, address.city, age)";

        executeDdl(index1);
        addComplex(4);

        // partial key and range, plus always-true preds
        String q0 =
            "select id, age\n" +
            "from Complex C\n" +
            "where C.address.state = \"CA\" and \n" +
            "      C.address.city > \"F\" and \n" +
            "      C.address.city > \"G\"";

        String[] expected0 = {
            "{\"id\":0,\"age\":10}"
        };

        // always false
        String q1 =
            "select id, age\n" +
            "from Complex C\n" +
            "where \"M\" > C.address.state and \n" +
            "      \"S\" < C.address.city and \n" +
            "      C.address.city = \"Boston\" and \n" +
            "      age > 50";

        String[] expected1 = {
        };

        // always false
        String q2 =
            "select id, age\n" +
            "from Complex C\n" +
            "where \"S\" < C.address.city and \n" +
            "      C.address.city = \"Boston\" and \n" +
            "      \"MA\" = C.address.state and \n" +
            "      age > 50";

        String[] expected2 = {
        };

        // complete key
        String q3 =
            "select id, age\n" +
            "from Complex C\n" +
            "where C.address.state = \"CA\" and \n" +
            "      C.address.city = \"San Fransisco\" and \n" +
            "      C.age = 10";

        String[] expected3 = {
            "{\"id\":0,\"age\":10}"
        };

        // key gap
        String q4 =
            "select id, age\n" +
            "from Complex C\n" +
            "where C.address.state = \"CA\" and \n" +
            "      C.age = 10";

        String[] expected4 = {
            "{\"id\":0,\"age\":10}"
        };

        // equality via two range preds
        String q5 =
            "select id, age\n" +
            "from Complex C\n" +
            "where \"MA\" <= C.address.state and \n" +
            "      C.address.city = \"Boston\" and \n" +
            "      \"MA\" >= C.address.state and \n" +
            "      age >= 11";

        String[] expected5 = {
            "{\"id\":1,\"age\":11}"
        };

        // nothing pushed
        String q6 =
            "select id, age\n" +
            "from Complex C\n" +
            "where C.age > 10";

        String[] expected6 = {
            "{\"id\":1,\"age\":11}",
            "{\"id\":2,\"age\":12}",
            "{\"id\":3,\"age\":13}",
        };

        // range only
        String q7 =
            "select id, age\n" +
            "from Complex C\n" +
            "where \"MA\" <= C.address.state";

        String[] expected7 = {
            "{\"id\":1,\"age\":11}",
            "{\"id\":2,\"age\":12}",
            "{\"id\":3,\"age\":13}",
        };

        // orderby
        String q8 =
            "select id, age\n" +
            "from Complex C\n" +
            "where C.age > 10\n" +
            "order by C.address.state, C.address.city, C.age";

        String[] expected8 = {
            "{\"id\":1,\"age\":11}",
            "{\"id\":2,\"age\":12}",
            "{\"id\":3,\"age\":13}",
        };

        String[] queries = { q0, q1, q2, q3, q4, q5, q6, q7, q8 };
        String[][] expectedResults = {
            expected0, expected1, expected2, expected3, expected4,
            expected5, expected6, expected7, expected8
        };

        boolean[] sorted = {
            false, false, false, false, false, false, false, false, true
        };

        for (int i = 0; i < queries.length; ++i) {
            List<RecordValue> results = executeDML(queries[i], i);
            checkResults(i, expectedResults[i], results, sorted[i]);
        }
    }

    @Test
    public void testArrayIndex1() throws Exception {

        String tableStatement =
            "CREATE TABLE Foo("                           +
            "    id INTEGER, "                            +
            "    rec RECORD(a INTEGER, "                  +
            "               b ARRAY(INTEGER),"            +
            "               c ARRAY(RECORD(ca INTEGER))," +
            "               d ARRAY(MAP(INTEGER)),"       +
            "               f FLOAT), "                   +
            "    primary key(id))";

        String doc0 =
            "{                                   \n" +
            "   \"a\" : 10,                      \n" +
            "   \"b\" : [ 3, 20, 10, 10 ],       \n" +
            "   \"c\" : [ {\"ca\" :  3 },        \n" +
            "             {\"ca\" : 20 },        \n" +
            "             {\"ca\" : 10 },        \n" +
            "             {\"ca\" : 10 } ],      \n" +
            "   \"d\" : [ ],                     \n" +
            "   \"f\" : 4.5                      \n" +
            "}";

        String doc1 =
            "{                                   \n" +
            "   \"a\" : -10,                     \n" +
            "   \"b\" : [ 3, 10, -50 ],          \n" +
            "   \"c\" : [ {\"ca\" :   3 },       \n" +
            "             {\"ca\" :  10 },       \n" +
            "             {\"ca\" : -50 } ],     \n" +
            "   \"d\" : [ ],                     \n" +
            "   \"f\" : 4.5                      \n" +
            "}";

        String doc2 =
            "{                                   \n" +
            "   \"a\" : 10,                      \n" +
            "   \"b\" : [ 1, 3, 6 ],             \n" +
            "   \"c\" : [ {\"ca\" : 1 },         \n" +
            "             {\"ca\" : 3 },         \n" +
            "             {\"ca\" : 6 } ],       \n" +
            "   \"d\" : [ ],                     \n" +
            "   \"f\" : -4.5                     \n" +
            "}";

        String doc3 =
            "{                                   \n" +
            "   \"a\" : 10,                      \n" +
            "   \"b\" : [ 1, 3, -6 ],            \n" +
            "   \"c\" : [ {\"ca\" :  1 },        \n" +
            "             {\"ca\" :  3 },        \n" +
            "             {\"ca\" : -6 } ],      \n" +
            "   \"d\" : [ ],                     \n" +
            "   \"f\" : 4.5                      \n" +
            "}";

        String docs[] = { doc0, doc1, doc2, doc3 };

        TableAPI tapi = store.getTableAPI();

        executeDdl(tableStatement);
        Table table = tapi.getTable(getNamespace(), "Foo");

        String index1ddl = "CREATE INDEX idx_a_b_f ON Foo (rec.a, rec.b[], " +
            "rec.f)";
        executeDdl(index1ddl);

        for (int i = 0; i < 4; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.putRecordAsJson("rec",  docs[i], true);

            tapi.put(row, null, null);
        }

        // partial key and range
        String q0 =
            "select id from Foo t where t.rec.a = 10 and t.rec.b[] =any 3";

        String[] expected0 = {
            "{\"id\":0}",
            "{\"id\":2}",
            "{\"id\":3}"
        };

        String[] queries = { q0 };
        String[][] expectedResults = {
            expected0
        };

        for (int i = 0; i < queries.length; ++i) {
            List<RecordValue> results = executeDML(queries[i], i);
            checkResults(i, expectedResults[i], results, false);
        }

        executeDdl("drop index idx_a_b_f on Foo");

        String index2ddl = "CREATE INDEX idx_a_c_f ON Foo (rec.a, rec.c[]" +
            ".ca, rec.f)";
        executeDdl(index2ddl);

        // partial key
        String q10 =
            "select id from Foo t where t.rec.a = 10 and t.rec.c.ca =any 3";

        String[] expected10 = {
            "{\"id\":0}",
            "{\"id\":2}",
            "{\"id\":3}"
        };

        // complete key
        String q11 =
            "select id from " +
            "Foo t " +
            "where t.rec.a = 10 and t.rec.c.ca =any 3 and t.rec.f = 4.5";

        String[] expected11 = {
            "{\"id\":0}",
            "{\"id\":3}"
        };

        // key gap
        String q12 =
            "select id from " +
            "Foo t " +
            "where t.rec.a = 10 and t.rec.f = 4.5";

        String[] expected12 = {
            "{\"id\":0}",
            "{\"id\":3}"
        };

        // partial key and range
        String q13 =
            "select id from Foo t where t.rec.a = 10 and t.rec.c.ca >any 6";

        String[] expected13 = {
            "{\"id\":0}",
        };

        // partial key and range; only one multi-key pred pushed
        String q14 =
            "select id from " +
            "Foo t " +
            "where t.rec.a = 10 and 10 <=any t.rec.c.ca and t.rec.c.ca <any 10";

        String[] expected14 = {
            "{\"id\":0}",
        };

        // partial key and range; only one multi-key pred pushed. Notice that
        // compared to q14, we have only changed the order of 2 predicates, but
        // we get different result w.r.t. duplicates.
        String q15 =
            "select id from " +
            "Foo t " +
            "where t.rec.a = 10 and t.rec.c.ca <any 10 and 10 <=any t.rec.c.ca";

        String[] expected15 = {
            "{\"id\":0}"
        };

        // partial key and range; only one multi-key pred pushed, eq preferred.
        String q16 =
            "select id from " +
            "Foo t " +
            "where t.rec.a = 10 and " +
            "      t.rec.c.ca >=any 3 and t.rec.c.ca =any 20 and " +
            "      t.rec.f = 4.5" ;

        String[] expected16 = {
            "{\"id\":0}"
        };

        // partial key and range; only one multi-key pred pushed, the other
        // is always true
        String q17 =
            "select id from " +
            "Foo t " +
            "where t.rec.a = 10 and t.rec.c.ca >=any 6 and t.rec.c.ca >any 6";

        String[] expected17 = {
            "{\"id\":0}",
        };

        String[] queries2 = { q10, q11, q12, q13, q14, q15, q16, q17 };
        String[][] expectedResults2 = {
            expected10, expected11, expected12, expected13, expected14,
            expected15, expected16, expected17
        };

        for (int i = 0; i < queries2.length; ++i) {
            List<RecordValue> results = executeDML(queries2[i], i);
            checkResults(i, expectedResults2[i], results, false);
        }
    }

    @Test
    public void testParentChildTables() throws Exception {

        //TODO: allow MRTable mode after MRTable supports child tables.
        executeDdl(userTableStatement, true, true/*noMRTableMode*/);
        addUsers(10);

        String addressTableStatement =
            "create table Users.address( " +
            "    street STRING, " +
            "    city STRING, " +
            "    state STRING, " +
            "    zip INTEGER, " +
            "    primary key (zip))";

        executeDdl(addressTableStatement, true, true/*noMRTableMode*/);

        String query = "select * from Users";

        Table addressTable = tableImpl.getTable(getNamespace(), "Users.address");

        for (int i = 0; i < 10; ++i) {
            Row row = addressTable.createRow();
            row.put("id", i);
            row.put("zip", 95000 + i);
            row.put("street", "Terrace");
            row.put("city", "SF");
            row.put("state", "CA");
            tableImpl.put(row, null, null);
        }

        List<RecordValue> results = executeDML(query, 1);
        assertTrue(results.size() == 10);
    }

    /*
     * Test duplicate results in a normal table query.
     */
    @Test
    public void testDuplicates() throws Exception {

        final String dupQuery = "select firstName from Users";

        executeDdl(userTableStatement);
        addUsers(10);

        /* add some duplicate fields */
        Table table = tableImpl.getTable(getNamespace(), "Users");

        for (int i = 0; i < 5; i++) {
            Row row = table.createRow();
            row.put("id", i + 10); /* avoid existing records */
            row.put("firstName", ("first" + i));
            row.put("lastName", ("last" + i));
            row.put("age", i+10);
            tableImpl.put(row, null, null);
        }

        List<RecordValue> results = executeDML(dupQuery, 0);
        assertTrue(results.size() == 15);
    }

    /*
     * Test query-related operation statistics
     */
    @Test
    public void testStats() throws Exception {

        final String allPartQuery = "select firstName from Users";
        final String onePartQuery = "select firstName from Users where id = 1";
        final String allShardQuery = "select firstName from Users where age = 18";

        /*
         * This test will fail if executing queries on the client.
         */
        String onClient = System.getProperty("test.queryonclient");
        if (onClient != null) {
            return;
        }

        executeDdl(userTableStatement);
        /* add an index to create a case for an indexed operation */
        executeDdl("create index Age on Users (age)");
        addUsers(10);

        /*
         * Baseline -- no query operations.
         */
        List<OperationMetrics> metrics = store.getStats(true).getOpMetrics();
        for (OperationMetrics om : metrics) {
            if (om.getOperationName().contains("query")) {
                assertTrue(om.getTotalOpsLong() == 0);
            }
        }

        List<RecordValue> results = executeDML(allPartQuery, 0);
        assertTrue(results.size() == 10);
        assertMetric("queryMultiPartition", 10); /* 10 partitions */

        results = executeDML(allShardQuery, 0);
        assertTrue(results.size() == 1);
        assertMetric("queryMultiShard", 1); /* only one shard */

        results = executeDML(onePartQuery, 0);
        assertTrue(results.size() == 1);
        assertMetric("querySinglePartition", 1);
    }

    private void assertMetric(String metric, int numOps) {
        List<OperationMetrics> metrics = store.getStats(true).getOpMetrics();
        for (OperationMetrics om : metrics) {
            if (om.getOperationName().contains(metric)) {
                assertTrue(om.getTotalOpsLong() == numOps);
                return;
            }
        }
        throw new IllegalStateException("No such metric: " + metric);
    }

    /*
     * Test some simple operations on a key-only table.
     */
    @Test
    public void testKeyOnly() throws Exception {

        /*
         * Use the same field names as in the non-key-only table to share
         * code.
         */
        final String keyOnlyTable = "create table keyOnly(firstName string, " +
            "lastName string, age integer, id integer, " +
            "primary key(shard(lastName,firstName),age, id))";

        final String indexQuery = "select * from keyOnly where firstName > 'first1'";
        final String shardQuery = "select * from keyOnly where " +
            "firstName = 'first1' and lastName = 'last1'";
        final String fullKeyQuery = "select * from keyOnly where " +
            "firstName = 'first1' and lastName = 'last1' and " +
            " age = 11 and id = 1";

        executeDdl(keyOnlyTable);
        executeDdl("create index Age on keyOnly (age)");
        executeDdl("create index First on keyOnly (firstName)");
        addUsersToTable(tableImpl.getTable(getNamespace(), "keyOnly"), 10);

        List<RecordValue> results = execute("select * from keyOnly");
        assertTrue(results.size() == 10);

        /* use an index */
        results = execute(indexQuery);
        assertTrue(results.size() == 8);
        queryPlanContains(indexQuery, "First"); /* uses index named "First" */

        /* full shard key */
        results = execute(shardQuery);
        assertTrue(results.size() == 1);

        /* full key */
        results = execute(fullKeyQuery);
        assertTrue(results.size() == 1);
    }

    @Test
    public void testPkeySizeStatements() {
        String b1 = "CREATE TABLE foo (id INTEGER, PRIMARY KEY(id(6)))";
        String b2 = "CREATE TABLE foo (id STRING, PRIMARY KEY(id(3)))";
        String ok1 = "CREATE TABLE foo(id1 INTEGER, id2 STRING, " +
            "PRIMARY KEY(id1(3), id2))";

        executeDdl(b1, false);
        executeDdl(b2, false);
        executeDdl(ok1, true);
    }

    /*
     * Test improper use of default value on a primary key
     */
    @Test
    public void testBadPkeyStatements() {
        String q1 = "CREATE TABLE foo (id INTEGER not null default 5, " +
            "PRIMARY KEY(id))";
        String q2 = "CREATE TABLE foo (id INTEGER not null, " +
            "PRIMARY KEY(id))";
        String q3 = "CREATE TABLE foo (id INTEGER default 5, " +
            "PRIMARY KEY(id))";

        executeDdl(q1, false);
        executeDdl(q2, false);
        executeDdl(q3, false);
    }

    @SuppressWarnings("unused")
    @Test
    public void testNoPrefetching() {

        executeDdl(userTableStatement);
        addUsers(500);

        ExecuteOptions options = new ExecuteOptions();
        options.setDoPrefetching(false);
        options.setResultsBatchSize(10);
        //options.setTraceLevel((byte)1);

        String query = "select * from Users order by id";

        int count = 0;
        StatementResult sr = executeDml(query, options);
        for (RecordValue record : sr) {
            ++count;
        }
        assertTrue(count == 500);
        sr.close();
    }

    /*
     * Batch size test cases for limit and offset
     */
    @SuppressWarnings("unused")
    @Test
    public void testBatchSizeLimitOffset() {

        final int numRecords = 50;

        /* use batch size 1, which can uncover problems */
        ExecuteOptions options = new ExecuteOptions().setResultsBatchSize(1);
        executeDdl(userTableStatement);
        executeDdl("create index Age on Users (age)");
        addUsers(numRecords);


        /*
         * Test offset with no sort
         */
        for (int i = 0; i < numRecords; i++) {
            String query = "select * from Users offset " + i;
            int count = 0;
            StatementResult sr = executeDml(query, options);
            for (RecordValue record : sr) {
                ++count;
            }
            assertTrue(count == (numRecords - i));
            sr.close();
        }

        for (String sort : new String[] {"asc", "desc"})  {
            /*
             * Test offset with primary key sort
             */
            for (int i = 0; i < numRecords; i++) {
                final String query = "select * from Users order by id " +
                    sort + " offset " + i;
                int count = 0;
                StatementResult sr = executeDml(query, options);
                for (RecordValue record : sr) {
                    ++count;
                }
                assertTrue(count == (numRecords - i));
                sr.close();
            }

            /*
             * Test offset with index sort
             */
            for (int i = 0; i < numRecords; i++) {
                final String query = "select * from Users order by age " +
                    sort + " offset " + i;
                int count = 0;
                StatementResult sr = executeDml(query, options);
                for (RecordValue record : sr) {
                    ++count;
                }
                assertTrue(count == (numRecords - i));
                sr.close();
            }
        }

        /*
         * Simple test on limit. Go beyond the number of records.
         */
        for (int i = 0; i < numRecords + 10; i++) {
            String query = "select * from Users limit " + i;
            int count = 0;
            StatementResult sr = executeDml(query, options);
            for (RecordValue record : sr) {
                ++count;
            }
            assertTrue(count == i ||
                       i > numRecords && count == numRecords);
            sr.close();
        }

        /*
         * Set offset beyond the number of records
         */
        String query = "select * from Users offset " + numRecords;
        int count = 0;
        StatementResult sr = executeDml(query, options);
        for (RecordValue record : sr) {
            ++count;
        }
        assertTrue(count == 0);
        sr.close();
    }

    @Test
    public void testMetadataUpdate() {
        final String parentTableQuery =
            "create table if not exists parent(id integer, primary key(id))";
        final String childTableQuery =
            "create table if not exists parent.child(cid integer, primary key(cid))";
        final String parentIndexQuery =
            "create index if not exists ididx on parent(id)";
        final String childIndexQuery =
            "create index if not exists ididx on parent.child(cid)";

        for (int i = 0; i < 5; i++) {
            //TODO: allow MRTable mode after MRTable supports TTL.
            executeDdl(parentTableQuery, true, true/*noMRTableMode*/);
            executeDdl(childTableQuery, true, true/*noMRTableMode*/);
            executeDdl(parentIndexQuery);
            executeDdl(childIndexQuery);
            executeDdl("drop table if exists parent.child");
            executeDdl("drop table if exists parent");
        }
    }

    @Test
    public void testPreparedStatementState() {
        final String createTable =
            "create table foo(first string, last string, age integer, " +
            "phone string, primary key(shard(last), first))";
        final String createIndex = "create index AgeIndex on foo(age)";

        final String queryWithShardKey = "select * from foo where last = 'jones'";

        final String queryNoShardKey1 = "select * from foo where first = 'joe'";
        final String queryNoShardKey2 = "select * from foo where phone = '1234'";
        final String queryWithIndex = "select * from foo where age > 5";

        executeDdl(createTable);
        executeDdl(createIndex);

        /* shard key */
        PreparedStatementImpl prep =
            (PreparedStatementImpl) prepare(queryWithShardKey);
        assertEquals("expected shard key", DistributionKind.SINGLE_PARTITION,
                     prep.getDistributionKind());

        assertFalse(prep.getPartitionId().isNull());

        /* index query */
        prep = (PreparedStatementImpl) prepare(queryWithIndex);
        assertEquals("expected shard distr", DistributionKind.ALL_SHARDS,
                     prep.getDistributionKind());
        assertTrue(prep.getPartitionId() == null);

        /* no shard key 1 -- uses a pkey field */
        prep = (PreparedStatementImpl) prepare(queryNoShardKey1);
        assertEquals("did not expect shard key", DistributionKind.ALL_PARTITIONS,
                     prep.getDistributionKind());
        assertTrue(prep.getPartitionId() == null);

        /* no shard key 2 */
        prep = (PreparedStatementImpl) prepare(queryNoShardKey2);
        assertEquals("did not expect shard key", DistributionKind.ALL_PARTITIONS,
                     prep.getDistributionKind());
        assertTrue(prep.getPartitionId() == null);
    }

    @Test
    public void testNumberType() {
        final String bigdTableDDL =
            "create table bigd(" +
                "id INTEGER, " +
                "bigd0 NUMBER, " +
                "bigd1 NUMBER default 0, " +
                "bigd2 NUMBER default -1234567890123456789, " +
                "bigd3 NUMBER default -1.1E-10, " +
                "bigd4 NUMBER default 9.223372036854775807E+2147483647, " +
                "arr ARRAY(number), " +
                "map MAP(number), " +
                "rec RECORD(rid integer, rbigd number), " +
                "primary key(shard(id), bigd0))";

        final String[] bigdIndexesDDL = new String[] {
            "create index idx_bigd1 on bigd(bigd1)",
            "create index idx_bigds on bigd(bigd1, bigd2, bigd3)",
            "create index idx_arr on bigd(arr[])",
            "create index idx_map on bigd(map.values())",
            "create index idx_misc on bigd(rec.rbigd, arr[], bigd4)"
        };

        executeDdl(bigdTableDDL);
        for (String idxDDL : bigdIndexesDDL) {
            executeDdl(idxDDL);
        }

        TableImpl bigdTable = (TableImpl) tableImpl.getTable(getNamespace(),
                                                             "bigd");
        assertTrue(bigdTable.getFields().size() == 9);
        assertTrue(bigdTable.getIndexes().size() == bigdIndexesDDL.length);
    }

    @Test
    public void testExecuteOnClosedHandle() {

        final KVStore handle =
            KVStoreFactory.getStore(createKVConfig(createStore));
        handle.close();

        /*
         * Test create table DDL
         */
        try {
            handle.executeSync(
                TestBase.addRegionsForMRTable("CREATE TABLE IF NOT EXISTS " +
                    "T1 (ID INTEGER, PRIMARY KEY(ID))"));
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(
                "Cannot execute request on a closed store handle"));
        }

        /*
         * Test query DML
         */
        try {
            handle.executeSync(
                "SELECT * FROM T1");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(
                "Cannot execute request on a closed store handle"));
        }
    }

    /*
     * Test the use of PrepareCallback to extract information during query
     * compilation.
     */
    @Test
    public void testPrepareCallback() {

        ExecuteOptions options = new ExecuteOptions();

        /*
         * Set expectations
         */
        PrepareCB callback = new PrepareCB(QueryOperation.CREATE_TABLE,
                                           "mytable",
                                           true,
                                           null,
                                           tableImpl.getTableMetadataHelper());
        options.setPrepareCallback(callback);
        callback.setExpectedFieldNum(1);

        prepareInternal(
            "create table if not exists mytable(id integer, primary key(id))",
            options);
        /* validate expectations */
        callback.validate();

        /*
         * Test field limit
         */
        callback = new PrepareCB(QueryOperation.CREATE_TABLE,
                                 "limittable",
                                 null,
                                 null,
                                 tableImpl.getTableMetadataHelper());
        callback.setFieldLimit(9);
        options.setPrepareCallback(callback);
        try {
            prepareInternal(  /* 10 fields */
                "create table limittable(" +
                "id integer, " +
                "id1 integer, " +
                "id2 integer, " +
                "rec record( " +
                "rec1 record( " +
                "rec1id integer), " +
                "recid integer), " +
                "map map(integer), " +
                "array array(record( " +
                "rec2id integer)), " +
                "primary key(id))", options);
            fail("Validation should have failed for number of fields");
        } catch (IllegalArgumentException iae) {
            // success
        }

        /*
         * Test schema evolution #fields limit
         */
        //TODO: allow MRTable mode after MRTable supports child tables.
        executeDdl("create table evotable(id integer, primary key(id))",
                   true, true/*noMRTableMode*/);

        callback = new PrepareCB(QueryOperation.ALTER_TABLE,
                                 "evotable",
                                 null,
                                 null,
                                 tableImpl.getTableMetadataHelper());
        callback.setFieldLimit(4);
        options.setPrepareCallback(callback);
        options.setNamespace(getNamespace(), false);
        try {
            prepareInternal("alter table evotable(" +
                            "add id1 integer, " +
                            "add id2 integer, " +
                            "add id3 integer, " +
                            "add id4 integer)", options);
        } catch (IllegalArgumentException iae) {
            // success
        }

        /*
         * Test ability to prepare a create of a child table using the
         * TableMetadataHelper from the PrepareCB. Use the PrepareCB created
         * above.
         */
        callback.setExpectedFieldNum(2);
        prepareInternal("create table evotable.child(evokey integer," +
                        "primary key(evokey))", options, true/*hasChildTables*/);
        assertTrue(callback.tableNames.contains("evotable.child"));

        /*
         * Test nested complex types and field count
         */
        callback = new PrepareCB(QueryOperation.CREATE_TABLE,
                                 "counttable",
                                 null,
                                 null,
                                 tableImpl.getTableMetadataHelper());
        callback.setExpectedFieldNum(9);
        options.setPrepareCallback(callback);
        prepareInternal(
            "create table counttable(" +
            "id integer, " +
            "mapf map(" +
            "map(" +
            "record(" +
            "recid integer)))," +
            "arrayf array(" +
            "array(" +
            "record(" +
            "aid integer)))," +
            "primary key(id))", options);

        /*
         * Test drop table
         */
        callback = new PrepareCB(QueryOperation.DROP_TABLE,
                                 "foo.bar",
                                 null,
                                 true,
                                 tableImpl.getTableMetadataHelper());
        options.setPrepareCallback(callback);
        prepareInternal("drop table if exists foo.bar", options);
        /* validate expectations */
        callback.validate();

        /*
         * Create a query that has 2 tables in a parent/child join and
         * validate that they turned up in the callback.
         */
        //TODO: allow MRTable mode after MRTable supports child tables.
        executeDdl("create table parent(id integer, primary key(id))",
                   true, true/*noMRTableMode*/);
        executeDdl("create table parent.child(cid integer, primary key(cid))",
                   true, true/*noMRTableMode*/);
        callback = new PrepareCB(QueryOperation.SELECT,
                                 null, /* table name */
                                 null,
                                 null,
                                 null);
        callback.setPreparedNeeded(true);
        options.setPrepareCallback(callback);
        executeDml("select p.id, c.cid from nested tables(parent p "
                        +"descendants(parent.child c))", options);
        assertTrue(callback.tableNames.contains("parent"));
        assertTrue(callback.tableNames.contains("parent.child"));
        callback.validate();

        /*
         * Verify all the tables used in the inner join query can be collected
         * using PrepareCallback with PreparedNeeded = false.
         */
        executeDdl("create table parent.child.grand(" +
                        "gid integer, primary key(gid))",
                   true, true /*noMRTableMode*/);
        String query = "select p.id, c.cid, g.gid " +
                       "from parent p, parent.child c, parent.child.grand g " +
                       "where p.id = c.id and c.id = g.id";
        callback = new PrepareCB(QueryOperation.SELECT,
                                 null, /* table name */
                                 null,
                                 null,
                                 null);
        options.setPrepareCallback(callback);
        prepareInternal(query, options, true /* hasChildTables */);
        assertEquals(Arrays.asList("parent",
                                   "parent.child",
                                   "parent.child.grand"),
                     callback.tableNames);

        query = "select p.id, c.cid, g.gid " +
                "from nested tables(parent.child.grand g " +
                                   "ancestors(parent.child c)), " +
                     "parent p " +
                "where g.id = c.id and g.id = p.id";
        callback = new PrepareCB(QueryOperation.SELECT,
                                 null, /* table name */
                                 null,
                                 null,
                                 null);
        options.setPrepareCallback(callback);
        prepareInternal(query, options, true /* hasChildTables */);
        assertEquals(Arrays.asList("parent.child.grand",
                                   "parent.child",
                                   "parent"),
                     callback.tableNames);
    }

    @Test
    public void testWithLimits() {
        final String statementString =
            TestBase.addRegionsForMRTable("create table foo(id integer, " +
                "primary key(id))");
        final char[] statement = statementString.toCharArray();

        TableLimits limits = new TableLimits(50, 50, 50, 1, 2, 3);
        ExecutionFuture future =
            ((KVStoreImpl) store).execute(statement,
                                          new ExecuteOptions().setLogContext(
                                              new LogContext()),
                                          limits);
        try {
            StatementResult res = future.get();
            assertTrue(res.isSuccessful());

            TableImpl table = getTable("foo");
            assertNotNull(table);

            /* test JSON roundTrip */
            roundTripTable(table);

            TableLimits limits1 = table.getTableLimits();
            if (limits.getReadLimit() != limits1.getReadLimit() ||
                limits.getWriteLimit() != limits1.getWriteLimit() ||
                limits.getSizeLimit() != limits1.getSizeLimit() ||
                limits.getChildTableLimit() != limits1.getChildTableLimit() ||
                limits.getIndexLimit() != limits1.getIndexLimit() ||
                limits.getIndexKeySizeLimit() !=
                                            limits1.getIndexKeySizeLimit()) {
                fail("Wrong TableLimits, expected " + limits + ", but was " +
                    limits1);
            }

            exerciseLimitsRoundTrip(table);
        } catch (Exception ee) {
            fail("Exception: " + ee);
        }
    }

    private void exerciseLimitsRoundTrip(TableImpl table) throws Exception {
        String name = table.getName();
        /*
         * exercise roundTrip with various combinations of TableLimits.
         * Order of args is: read, write, size, indexes, children, index key
         * NOTE: index populate limit is not used in JSON right now
         */
        setTableLimits(table, new TableLimits(5, NO_LIMIT, NO_LIMIT,
                                              NO_LIMIT, NO_LIMIT, NO_LIMIT));
        table = getTable(name);
        roundTripTable(table);

        setTableLimits(table, new TableLimits(NO_LIMIT, 6, NO_LIMIT,
                                              NO_LIMIT, NO_LIMIT, NO_LIMIT));
        table = getTable(name);
        roundTripTable(table);

        setTableLimits(table, new TableLimits(NO_LIMIT, NO_LIMIT, 8,
                                              NO_LIMIT, NO_LIMIT, NO_LIMIT));
        table = getTable(name);
        roundTripTable(table);

        setTableLimits(table, new TableLimits(NO_LIMIT, NO_LIMIT, NO_LIMIT,
                                              6, NO_LIMIT, NO_LIMIT));
        table = getTable(name);
        roundTripTable(table);

        setTableLimits(table, new TableLimits(NO_LIMIT, NO_LIMIT, NO_LIMIT,
                                              NO_LIMIT, 10, NO_LIMIT));
        table = getTable(name);
        roundTripTable(table);

        setTableLimits(table, new TableLimits(NO_LIMIT, NO_LIMIT, NO_LIMIT,
                                              NO_LIMIT, NO_LIMIT, 50));
        table = getTable(name);
        roundTripTable(table);
    }

    @Test
    public void testSerializedQueryPlan() throws IOException {
        final String createTable =
            "create table foo(first string, last string, age integer, " +
            "phone string, address record(city string, zipcode string)," +
            "primary key(shard(last), first))";
        final String createIndex = "create index idxAge on foo(age)";
        final String query = "select age, phone, last from foo where age < 10";
        final String query1 = "select age, phone, first from foo where last = 'b0'";
        final String query2 = "select first from foo";
        final String queryWithVars = "declare $ext string; $ext1 string; " +
                  "select age, phone, first from foo where last = $ext and phone = $ext1";
        /*        final String queryWithVars = "declare $ext string; " +
                  "select age, phone, first from foo where last = $ext"; */

        final String query3 = "select * from foo where age is null and age = 20";
        final String query4 = "select first, last from foo order by age desc";
        final String query5 = "select first, last from foo limit 5";
        final String query6 = "select f.address from foo f " +
            "where last = 'b0' and first = 'a0'";
        /*
        final String query7= "select * from foo where age > 10 and " +
            "phone is not null order by age limit 10";
        */

        /* no namespace */
        executeDdl(createTable, null, true);
        executeDdl(createIndex, null, true);

        Table table = getTable("foo");
        assertNotNull(table);

        /* add a bit of data */
        for (int i = 0; i < 10; i++) {
            Row row = table.createRow();
            row.put("first", ("a" + i)).put("last", ("b" + i)).put("age", i).
                put("phone", ("555-111" + i));
            tableImpl.put(row, null, null);
        }

        /* select with unindexed predicate -- all partitions */
        testSerializedPrepare(query, 10, null);

        /* select with equality predicate using key -- single partition */
        testSerializedPrepare(query1, 1, null);

        /* The result record contains single field */
        testSerializedPrepare(query2, 10, null);

        /* select with equality predicate using a variable */
        HashMap<String, FieldValue> vars = new HashMap<String, FieldValue>();
        vars.put("$ext", FieldDefImpl.Constants.stringDef.createValue("b1"));
        vars.put("$ext1",
                 FieldDefImpl.Constants.stringDef.createValue("555-1111"));
        testSerializedPrepare(queryWithVars, 1, vars);

        /* The root PlanIter is ConcateIter, it always 0 row */
        testSerializedPrepare(query3, 0, null);

        /* select with sorting spec */
        testSerializedPrepare(query4, 10, null);

        /* select with limit clause */
        testSerializedPrepare(query5, 5, null);

        /* The result record contains single record value, its value is null */
        testSerializedPrepare(query6, 1, null);

        /* Test on truncated serialized statements */
        PreparedStatementImpl prep = (PreparedStatementImpl)store.prepare(query);

        byte[] serStmt = serializePreparedStmt(prep);

        deserializeBadSerStatement(new byte[0]);
        deserializeBadSerStatement(Arrays.copyOf(serStmt, serStmt.length - 1));
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            int len = rand.nextInt(serStmt.length);
            byte[] bytes = Arrays.copyOf(serStmt, len);
            deserializeBadSerStatement(bytes);
        }

        /*
         * Test on corrupted serialized prepared statement. [disabled]
         *
         * The serialized prepared statement is used for cloud, proxy(kvclient)
         * does a cryptographic hash to check corruption, SSL detects the data
         * corruption on the wire, so below test scenario seems never happen in
         * actual production environment, we just disable it.
         *
         * More note on this test:
         * o the query deserialization code does not protect itself from bad
         *   array sizes and count, leading to memory problems.
         * o this test is testing an application (client) that is only hurting
         *   itself.
         *
         * It's also possible this will never be fixed in the system because it
         * means the client is only hurting themselves.
         */
        /*
        prep = (PreparedStatementImpl)store.prepare(query7);
        serStmt = serializePreparedStmt(prep);

        ExecuteOptions options = new ExecuteOptions()
            .setTimeout(1000, TimeUnit.MILLISECONDS);
        for (int i = 0; i < serStmt.length; i++) {
            byte[] bytes = Arrays.copyOf(serStmt, serStmt.length);
            bytes[i] = (bytes[i] != (byte)0xFF) ? (byte)0xFF : (byte)0x00;
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInput in = new DataInputStream(bais);
                PreparedStatementImpl prep1 = new PreparedStatementImpl(in);
                prep1.executeSync((KVStoreImpl)store, options);
            } catch (Exception ex) {
            }
        }
        */
    }

    private void deserializeBadSerStatement(byte[] bytes) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            DataInput in = new DataInputStream(bais);
            PreparedStatementImpl prep1 = new PreparedStatementImpl(in);
            prep1.executeSync((KVStoreImpl)store, null);
            fail("The query should have failed");
        } catch (IOException ioe) {
        } catch (Exception ex) {
        }
    }

    @SuppressWarnings("unused")
    private void testSerializedPrepare(String statement,
                                       int expectedResults,
                                       Map<String, FieldValue> vars)
        throws IOException {

        PreparedStatementImpl prep =
            (PreparedStatementImpl) store.prepare(statement);

        /* serialize */
        byte[] serStatement = serializePreparedStmt(prep);

        ByteArrayInputStream bais = new ByteArrayInputStream(serStatement);
        DataInput in = new DataInputStream(bais);

        /* deserialize */
        PreparedStatementImpl prep1 = new PreparedStatementImpl(in);

        PreparedStatement execStatement = prep1;

        /* include variables by making a BoundStatement, if present */
        if (vars != null) {
            BoundStatement bs = prep1.createBoundStatement();
            for (Map.Entry<String, FieldValue> entry : vars.entrySet()) {
                    bs.setVariable(entry.getKey(), entry.getValue());
            }
            execStatement = bs;
        }

        /* execute the newly-constructed statement */
        StatementResult res = store.executeSync(execStatement);
        assertTrue(res.isSuccessful());
        int numResults = 0;
        for (RecordValue val : res) {
            ++numResults;
        }
        assertEquals(expectedResults, numResults);
    }

    @Test
    public void testVariableWithNullValue() {
        String createTableDdl =
            "create table t1(id integer, name string, primary key(id))";
        executeDdl(createTableDdl, null, true);

        Table table = tableImpl.getTable("t1");
        assertNotNull(table);

        Row row = table.createRow();
        row.put("id", 0);
        assertNotNull(tableImpl.put(row, null, null));

        String query = "declare $name string;" +
            "select * from t1 where name = $name";
        PreparedStatement prepStmt = store.prepare(query);
        BoundStatement bs = prepStmt.createBoundStatement();

        /* Set Sql Null to $name, expect 0 row returned. */
        bs.setVariable("$name", NullValueImpl.getInstance());

        StatementResult results = store.executeSync(bs);
        assertFalse(results.iterator().hasNext());
    }

    /**
     * Tests use of regions and PrepareCallback, which is not a normal
     * part of query testing, but is important to the http proxy
     */
    @Test
    public void testRegionWithCallback() throws Exception {

        String regionStatement = "create region myregion";
        String regionStatementDrop = "drop region myregion";
        String namespaceStatement = "create namespace myns";
        String tableStatementNS =
            "create table myns:mytable(id integer, primary key(id))";
        String tableStatementRegion =
            "create table mytable(id integer, primary key(id)) " +
            " in regions myregion,myregion1";

        ExecuteOptions opts = new ExecuteOptions();

        XRegionTestBase.waitForMRSystemTables(tableImpl);

        /* create region first */
        StatementResult res =
            ((KVStoreImpl)store).executeSync(regionStatement, opts);
        assertTrue(res.isSuccessful());

        /* create namespace */
        res = ((KVStoreImpl)store).executeSync(namespaceStatement, opts);
        assertTrue(res.isSuccessful());

        /*
         * try prepare callback for various statements with namespace and
         * regions, make some simple assertions on results.
         */
        PrepareCB pc = new PrepareCB();
        opts.setPrepareCallback(pc);
        CompilerAPI.prepare(null, tableStatementNS.toCharArray(), opts);

        assertEquals(1, pc.getTableNames().size());
        assertEquals(QueryOperation.CREATE_TABLE, pc.getQueryOp());
        pc.getTableNames().clear();

        CompilerAPI.prepare(null, tableStatementRegion.toCharArray(), opts);
        assertEquals(2, pc.getRegionNames().size());
        assertEquals(QueryOperation.CREATE_TABLE, pc.getQueryOp());

        pc.getRegionNames().clear();
        pc.getTableNames().clear();

        CompilerAPI.prepare(null, regionStatement.toCharArray(), opts);
        assertEquals(1, pc.getRegionNames().size());
        assertEquals(QueryOperation.CREATE_REGION, pc.getQueryOp());
        assertEquals(0, pc.getTableNames().size());

        pc.getRegionNames().clear();

        CompilerAPI.prepare(null, regionStatementDrop.toCharArray(), opts);
        assertEquals(1, pc.getRegionNames().size());
        assertEquals(QueryOperation.DROP_REGION, pc.getQueryOp());
    }

    static class PrepareCB implements PrepareCallback {
        private final Boolean ifNotExistsExpected;
        private final Boolean ifExistsExpected;
        private final String tableNameExpected;
        private final QueryOperation opExpected;
        private final TableMetadataHelper mdHelper;
        private final List<String> tableNames = new ArrayList<String>();
        private final List<String> regionNames = new ArrayList<String>();
        private Boolean ifNotExists;
        private Boolean ifExists;
        private Boolean freeze;
        private Boolean freezeForce;
        private Boolean unfreeze;
        private Boolean alterTtl;
        private Boolean jsonCollection;
        private QueryOperation op;
        private int fieldLimit;
        private int expectedFieldNum;
        private boolean prepareNeeded = false;

        PrepareCB() {
            this(null, null, null, null, null);
        }

        PrepareCB(QueryOperation opExpected,
                  String tableNameExpected,
                  Boolean ifNotExistsExpected,
                  Boolean ifExistsExpected,
                  TableMetadataHelper mdHelper) {
            this.opExpected = opExpected;
            this.tableNameExpected = tableNameExpected;
            this.ifNotExistsExpected = ifNotExistsExpected;
            this.ifExistsExpected = ifExistsExpected;
            this.mdHelper = mdHelper;
        }

        @Override
        public void tableName(String name) {
            tableNames.add(name);
        }

        @Override
        public void indexName(String name) {
            // ignore -- not tested at this time
        }

        @Override
        public void regionName(String regionName) {
            regionNames.add(regionName);
        }

        @Override
        public void namespaceName(String namespaceName) {
            // ignore -- not tested at this time
        }

        @Override
        public void queryOperation(QueryOperation queryOperation) {
            this.op = queryOperation;
        }

        @Override
        public void ifNotExistsFound() {
            ifNotExists = true;
        }

        @Override
        public void ifExistsFound() {
            ifExists = true;
        }

        @Override
        public boolean prepareNeeded() {
            return prepareNeeded;
        }

        @Override
        public void isTextIndex() {
            // ignore -- not tested at this time
        }

        @Override
        public void newTable(Table table) {
            int numFields = ((TableImpl)table).countTypes();
            if (fieldLimit != 0 && fieldLimit < numFields) {
                throw new IllegalArgumentException(
                    "Too many fields: " + numFields + ", limit is " + fieldLimit);
            }
            if (expectedFieldNum != 0 && numFields != expectedFieldNum) {
                throw new IllegalArgumentException(
                    "Unexpected number of fields: " + numFields +
                    ", expected " + expectedFieldNum);
            }
        }

        @Override
        public TableMetadataHelper getMetadataHelper() {
            return mdHelper;
        }

        @Override
        public void freezeFound() {
            freeze = true;
        }

        @Override
        public void freezeFound(boolean force) {
            freeze = true;
            freezeForce = force;
        }

        @Override
        public void unfreezeFound() {
            unfreeze = true;
        }

        @Override
        public void alterTtlFound() {
            alterTtl = true;
        }

        @Override
        public void jsonCollectionFound() {
            jsonCollection = true;
        }

        private void setFieldLimit(int limit) {
            fieldLimit = limit;
        }

        private void setExpectedFieldNum(int num) {
            expectedFieldNum = num;
        }

        private void setPreparedNeeded(boolean value) {
            prepareNeeded = value;
        }

        void validate() {
            if (tableNameExpected != null) {
                assertTrue(tableNames.contains(tableNameExpected));
            }
            assertEquals(ifNotExistsExpected, ifNotExists);
            assertEquals(ifExistsExpected, ifExists);
            assertEquals(opExpected, op);
        }

        private QueryOperation getQueryOp() {
            return op;
        }

        private List<String> getTableNames() {
            return tableNames;
        }

        private List<String> getRegionNames() {
            return regionNames;
        }

        private Boolean getFreeze() {
            return freeze;
        }

        private Boolean getFreezeForce() {
            return freezeForce;
        }

        private Boolean getUnfreeze() {
            return unfreeze;
        }

        private Boolean getAlterTtl() {
            return alterTtl;
        }

        Boolean getJsonCollection() {
            return jsonCollection;
        }

        @Override
        public String toString() {

            return "op=" + op +
                ",tables=" + tableNames +
                ",regions=" + regionNames +
                ",ifExists=" + ifExists +
                ",ifNotExists=" + ifNotExists +
                ",freeze=" + freeze +
                ",unfreeze=" + unfreeze +
                ",alterTtl=" + alterTtl;
        }
    }

    /*
     * Test utilities
     */

    void checkResults(
        int qid,
        String[] expected,
        List<RecordValue> results,
        boolean sorted) {

        String result;

        if (theCompilerException != null) {
            if (expected.length == 0 ||
                !expected[0].equals("Compiler Exception")) {
                result = theCompilerException.getMessage();
                System.out.println(
                    "Unexpected exception for query " + qid + "\n" + result);
                theCompilerException.printStackTrace();
                assertTrue(false);
            }
            return;
        }

        if (theRuntimeException != null) {
            if (!expected[0].equals("Runtime Exception")) {
                result = theRuntimeException.getMessage();
                System.out.println(
                    "Unexpected exception for query " + qid + "\n" + result);
                theRuntimeException.printStackTrace();
                assertTrue(false);
            }
            return;
        }

        int numMatched = 0;

        for (RecordValue value : results) {

            result = value.toString();

            boolean found = false;

            if (sorted) {
                if (result.equals(expected[numMatched])) {
                    ++numMatched;
                    found = true;
                }
            } else {
                for (int i = 0; i < expected.length; ++i) {
                    if (result.equals(expected[i])) {
                        expected[i] = null;
                        ++numMatched;
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                System.out.println(
                    "Unexpected result for query " + qid + "\n" + result);
                assertTrue(false);
                break;
            }
        }

        if (numMatched != expected.length) {
            System.out.println(
                "Fewer than expected results for query " + qid + "\n" +
                "Expected: " + expected.length + " Got: " + numMatched);
            assertTrue(false);
        }
    }

    /*
     * Simple execute, return a list of results. These queries are
     * expected to succeed.
     */

    List<RecordValue> execute(String query) {
        StatementResult sr = executeDml(query);
        assertTrue(sr.isSuccessful());
        List<RecordValue> list = new ArrayList<RecordValue>();
        for (RecordValue val : sr) {
            list.add(val);
        }
        return list;
    }

    /*
     * Assert that the toString() query plan contains the string. This may
     * fail if/when the format of the output changes, which means that
     * test cases must change.
     */
    private void queryPlanContains(String query, String s) {
        PreparedStatement ps = prepare(query);
        assertTrue(("Query plan does not contain string: " + s),
                   ps.toString().contains(s));
    }

    /*
     * Execute the query. Failure to compile the query returns an empty
     * result list.
     */
    List<RecordValue> executeDML(String query, int qid) {
        return executeDML(query, qid, null);
    }

    List<RecordValue> executeDML(String query, int qid,
                                 ExecuteOptions options) {
        return executeDML(query, qid, options, null);
    }

    List<RecordValue> executeDML(
        String query,
        int qid,
        ExecuteOptions options,
        HashMap<String, FieldValue> vars) {

        if (showResult || showPlan) {
            System.out.println(
            "---------------------------------------------------------------");
            System.out.println("RAW query " + qid + " :\n" + query);
            System.out.println(
            "---------------------------------------------------------------\n\n");
        }

        theRuntimeException = null;
        theCompilerException = null;

        PreparedStatementImpl ps = null;
        BoundStatement bs = null;
        List<RecordValue> list = new ArrayList<RecordValue>();
        StatementResult sr = null;

        if (options == null) {
            options = new ExecuteOptions();
        }
        options.setNamespace(getNamespace(), false);

        try {
            ps = (PreparedStatementImpl) prepare(query);

            if (vars != null) {
                bs = ps.createBoundStatement();
                for (Map.Entry<String, FieldValue> entry : vars.entrySet()) {
                    String name =  entry.getKey();
                    FieldValue value = entry.getValue();
                    bs.setVariable(name, value);
                }
            }

        } catch (IllegalArgumentException iae) {
            Throwable cause = iae.getCause();
            if (cause != null && cause instanceof RuntimeException) {
                theCompilerException = (RuntimeException)cause;
            } else {
                theCompilerException = iae;
            }

            if (showResult) {
                System.out.println("Compiler Exception: " + theCompilerException);
                System.out.println("\n\n");
            }
            if (debugging) {
                theCompilerException.printStackTrace();
            }
            return list;
        }

        if (showPlan) {
            System.out.println(ps.getQueryPlan().display());
        }

        try {
            sr = store.executeSync((bs != null ? bs : ps), options);
            Iterator<RecordValue> iter = sr.iterator();

            while (iter.hasNext()) {
                list.add(iter.next());
            }

            if (showResult) {
                for (RecordValue res : list) {
                    System.out.println(res);
                }
                System.out.println("\n\n");
            }
        } catch (RuntimeException re) {
            theRuntimeException = re;
            if (showResult) {
                System.out.println(re.getMessage());
                System.out.println("\n\n");
            }
            if (debugging) {
                re.printStackTrace();
            }
        } finally {
            if (sr != null) {
                sr.close();
            }
        }
        return list;
    }

    protected void addUsers(int num) {
        addUsersToTable(tableImpl.getTable(getNamespace(), "Users"), num);
    }

    private void addUsersToTable(Table table, int num) {

        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("firstName", ("first" + i));
            row.put("lastName", ("last" + i));
            row.put("age", i+10);
            tableImpl.put(row, null, null);
        }
    }

    private void addComplex(int num) {

        Table table = tableImpl.getTable(getNamespace(),"Complex");

        for (int i = 0; i < num; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("firstName", ("first" + i));
            row.put("lastName", ("last" + i));
            row.put("age", i+10);
            if (i % 2 == 0) {
                row.put("ptr", "firstName");
            } else {
                row.put("ptr", "lastName");
            }
            row.putRecordAsJson("address",  address_docs[i], true);
            row.putMapAsJson("children", children_docs[i], true);

            tableImpl.put(row, null, null);
        }
    }

    /*
     * An internal test to make sure that the query plan serialization code is
     * working correctly for executed queries.
     */
    @SuppressWarnings("unused")
    private static void testSerialization(PlanIter iter, String query)
            throws IOException {

        /* serialize */
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);
        PlanIter.serializeIter(iter, out, CURRENT);
        byte[] bytes = baos.toByteArray();

        /* deserialize */
        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        final DataInputStream in = new DataInputStream(bais);
        PlanIter newIter = PlanIter.deserializeIter(in, CURRENT);
        assertEquals("Expected EOF after reading serialized object",
                     -1, in.read());
        assertSame(iter.getKind(), newIter.getKind());

        /* serialize new plan and compare byte arrays */
        final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        final DataOutput out2 = new DataOutputStream(baos2);
        PlanIter.serializeIter(newIter, out2, CURRENT);
        byte[] newBytes = baos2.toByteArray();
        assertArrayEquals(bytes, newBytes);
    }


    /*
     * Use the static method to mimic the case where the store is not known
     * for DDL statements.
     */
    private static PreparedStatement prepareInternal(String statement,
                                                     ExecuteOptions options,
                                                     boolean hasChildTables) {
        if (!hasChildTables) {
            statement = TestBase.addRegionsForMRTable(statement);
        }
        return CompilerAPI.prepare(null, statement.toCharArray(), options);
    }

    static PreparedStatement prepareInternal(String statement,
                                             ExecuteOptions options) {
        return prepareInternal(statement, options, false);
    }

    private static final int NO_OF_THREADS = 10;

    /*
     * Tests concurrent DELETE using WHERE clause: see SR [27636]
     */
    @Test
    public void testConcurrentDeletes() throws Exception {
        StatementResult sr = store.executeSync(
            TestBase.addRegionsForMRTable(
                "CREATE TABLE IF NOT EXISTS " +
                "jobs(" +
                "  job_id STRING," +
                "  command STRING," +
                "  created_by STRING," +
                "  PRIMARY KEY (job_id)" +
                "  ) "));
        assertTrue(sr.isSuccessful());


        Table pTable = store.getTableAPI().getTable("jobs");
        assertNotNull(pTable);

        ExecutorService executor = Executors.newFixedThreadPool(NO_OF_THREADS);

        for (int i = 0; i < NO_OF_THREADS; i++) {
            Runnable worker = new Runnable() {
                @Override
                public void run() {
                    putIterateDelete(pTable);
                }
            };
            executor.execute(worker);
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }

    @Test
    public void testIdentity() throws Exception {

        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);

        executeDdl("create table foo(id integer, str string, primary key(id))");

        executeDML("insert into foo values(1, \"abc\")", 1);

        executeDdl("alter table foo(add idcol integer generated always as identity)");

        executeDML("insert into foo (id) values(2)", 2);
        executeDML("insert into foo values(3, \"xyx\", default)", 3);

        List<RecordValue> results = executeDML("select * from foo", 4);
        assertTrue(results.size() == 3);
    }

    /* Tests STRING AS UUID */
    @Test
    public void testUUID() throws Exception {
        /* test SQL syntax */
        executeDdl("create table foo(id integer as uuid, primary key(id))",
                   false);
        executeDdl("create table foo(id string generated by default, " +
                    "primary key(id))", false);
        executeDdl("create table test(id string as uuid not null, " +
                    "primary key(id))", false);
        executeDdl("create table test(id string as uuid default 'xzy', " +
                    "primary key(id))", false);

        /* only one generated value is allowed in one table */
        executeDdl("CREATE Table uuidTable (id STRING AS UUID GENERATED BY " +
            "DEFAULT, id2 STRING AS UUID GENERATED BY DEFAULT, PRIMARY KEY " +
            "(id))", false);
        executeDdl("CREATE Table uuidTable  (id integer generated by " +
            "default as identity, id2 STRING AS UUID GENERATED BY DEFAULT, " +
            "PRIMARY KEY (id))", false);
        executeDdl("CREATE Table uuidTable (id STRING AS UUID GENERATED "  +
            "BY DEFAULT, id2 integer generated by default as identity, " +
            "PRIMARY KEY (id))", false);

        /* test table evolve */
        testUUIDevolve();

        /* "STRING AS UUID" column is primary key */
        testUUIDCase(1);
        /* "STRING AS UUID" column is non-primary key */
        testUUIDCase(2);
        /* "STRING AS UUID GENERATED BY DEFAULT" column is primary key */
        testUUIDCase(3);
        /* "STRING AS UUID GENERATED BY DEFAULT" column is non-primary key */
        testUUIDCase(4);
    }

    @Test
    public void testUUID2() {
        String ddl =
            "create table foo (" +
            "    id integer," +
            "    desc String," +
            "    primary key(id)" +
            ")";

        executeDdl(ddl);

        String addUUID = "alter table foo(add uuids string as uuid " +
                         "generated by default)";
        executeDdl(addUUID);

        String insert = "insert into foo values(1, \"test\", random_uuid())";
        executeDML(insert, 0);

        String addString = "alter table foo(add newString string)";
        executeDdl(addString);

        Table table = tableImpl.getTable(getNamespace(), "foo");
        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 1);

        Row r = tableImpl.get(pk, null);
        assertNotNull(r);
    }

    private void testUUIDevolve() {
        StatementResult sr;

        /* add a STRING AS UUID column */
        sr = store.executeSync(
            "create table evolveTable(id integer, primary key(id))");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
        sr = store.executeSync("INSERT INTO evolveTable values(1)");
        assertTrue(sr.isSuccessful());
        sr = store.executeSync(
            "alter table evolveTable(add uid string as uuid)");
        assertTrue(sr.isSuccessful());

        sr = store.executeSync("SELECT uid FROM evolveTable");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.iterator().next().get("uid").isNull());

        sr = store.executeSync(
            "alter table evolveTable(add uid2 string as uuid generated by " +
            "default)");
        assertTrue(sr.isSuccessful());

        sr = store.executeSync("SELECT uid2 FROM evolveTable");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.iterator().next().get("uid2").isNull());

        /* drop a STRING AS UUID column */
        sr = store.executeSync("alter table evolveTable(drop uid)");
        assertTrue(sr.isSuccessful());
        sr = store.executeSync("alter table evolveTable(drop uid2)");
        assertTrue(sr.isSuccessful());
        sr = store.executeSync("SELECT id FROM evolveTable");
        assertTrue(sr.isSuccessful());
        assertEquals(sr.iterator().next().get("id").asInteger().get(), 1);

        sr = store.executeSync("drop table if exists evolveTable");
    }

    private void testUUIDCase(int no) {

        StatementResult sr =
            store.executeSync("drop table if exists uuidTable");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        String query;
        switch (no) {
        case 1:
            query = "CREATE Table uuidTable " +
                "(id STRING AS UUID, name STRING, PRIMARY KEY (id))";
            break;
        case 2:
            query = "CREATE Table uuidTable " +
                "(id STRING AS UUID, name STRING, PRIMARY KEY (name))";
            break;
        case 3:
            query = "CREATE Table uuidTable " +
                "(id STRING AS UUID GENERATED BY DEFAULT, name STRING, " +
                "PRIMARY KEY (id))";
            break;
        case 4:
            query = "CREATE Table uuidTable " +
                "(id STRING AS UUID GENERATED BY DEFAULT, name STRING, " +
                "PRIMARY KEY (name))";
            break;
        default:
            throw new IllegalStateException("Unknown test case: " + no);
        }
        sr = store.executeSync(query);
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());

        sr = store.executeSync("INSERT INTO uuidTable(id, name) " +
            "VALUES (random_uuid(), 'dave')");
        assertTrue(sr.isSuccessful());

        /* get the generated value from a SQL INSERT using RETURNING */
        PreparedStatement ps = store.prepare("INSERT INTO uuidTable(id, name) " +
            "VALUES (random_uuid(), 'foe') RETURNING id");
        sr = store.executeSync(ps);
        assertTrue(sr.isSuccessful());
        String id = sr.iterator().next().get("id").asString().get();
        query = "SELECT name FROM uuidTable WHERE id='" + id +"'";
        sr = store.executeSync(query);
        assertTrue(sr.isSuccessful());
        String name =  sr.iterator().next().get("name").asString().get();
        assertEquals(name, "foe");

        /* execute the prepared insert again and make sure the inserted row
         * has a different uuid */
        if (no == 1 || no == 3) {
            sr = store.executeSync(ps);
            assertTrue(sr.isSuccessful());
            String id2 = sr.iterator().next().get("id").asString().get();
            assertTrue(!id.equals(id2));
        }

        /* test table API */
        Table table = store.getTableAPI().getTable("uuidTable");

        roundTripTable((TableImpl)table);

        Row row = table.createRow();
        String uuidStr = UUID.randomUUID().toString();
        row.put("name", "Jerry");
        row.put("id", uuidStr);
        tableImpl.put(row, null, null);
        query = "SELECT * FROM uuidTable WHERE name='Jerry'";
        sr = store.executeSync(query);
        assertTrue(sr.isSuccessful());
        RecordValue rv = sr.iterator().next();
        assertEquals(uuidStr, rv.get("id").asString().toString());

        /* the non-primary key field can be null */
        if (no == 2) {
            sr = store.executeSync("INSERT INTO uuidTable values(null, 'nullValue')");
            assertTrue(sr.isSuccessful());

            Row nullRow = table.createRow();
            nullRow.put("name",  "nullName");
            tableImpl.put(nullRow, null, null);
            query = "SELECT id FROM uuidTable WHERE name='nullName'";
            sr = store.executeSync(query);
            assertTrue(sr.isSuccessful());
            assertTrue(sr.iterator().next().get("id").isNull());
        }

        /* Empty string '' is invalid UUID string */
        try {
            executeDml("INSERT INTO uuidTable values('', 'Adam')");
            fail("Unexpected! UUID string is an empty string");
        } catch (IllegalArgumentException iae) {
        }

        /* test default value */
        if (no == 3 || no == 4) {
            sr = store.executeSync("INSERT INTO uuidTable " +
                "VALUES (default, 'Lucy') RETURNING id");
            assertTrue(sr.isSuccessful());
            id = sr.iterator().next().get("id").asString().get();
            assertNotNull(id);
            query = "SELECT name FROM uuidTable WHERE id='" + id +"'";
            sr = store.executeSync(query);
            assertTrue(sr.isSuccessful());
            name =  sr.iterator().next().get("name").asString().get();
            assertEquals(name, "Lucy");

            /* verify the generated by default value in column id */
            Row row1 = table.createRow();
            row1.put("name", "Tom");
            tableImpl.put(row1, null, null);
            String fromRow = row1.get("id").asString().toString();
            assertNotNull(fromRow);
            query = "SELECT * FROM uuidTable WHERE name='Tom'";
            sr = store.executeSync(query);
            assertTrue(sr.isSuccessful());
            rv = sr.iterator().next();
            String fromQuery =  rv.get("id").asString().get();
            assertEquals(fromRow, fromQuery);

            /* verify the result.getGeneratedValue() */
            Row row2 = table.createRow();
            row2.put("name", "Katie");
            Result result = tableImpl.putInternal((RowImpl)row2, null, null);
            String fromResult = result.getGeneratedValue().toString();
            assertNotNull(fromResult);
            query = "SELECT * FROM uuidTable WHERE name='Katie'";
            sr = store.executeSync(query);
            assertTrue(sr.isSuccessful());
            rv = sr.iterator().next();
            fromQuery =  rv.get("id").asString().get();
            assertEquals(fromResult, fromQuery);
        }
        sr = store.executeSync("drop table if exists uuidTable");
        assertTrue(sr.isSuccessful());
        assertTrue(sr.isDone());
    }

    private static void putIterateDelete(Table pTable) {
        StatementResult sr;
        final int numRows = 10;
        final int numIterations = 50;
        for (int iter = 0; iter < numIterations; iter++) {
            for (int i = 0; i < numRows; i++) {
                Row pRow = pTable.createRow();
                pRow.put("job_id", "_job_id_" + i);
                pRow.put("command", "_command_" + i);
                pRow.put("created_by", "_created_by_" + i);
                store.getTableAPI().put(pRow, null, null);
            }

            sr = store.executeSync("DELETE FROM jobs " +
                "WHERE job_id < '_job_id_9'");
            assertTrue(sr.isSuccessful());

            TableIterator<Row> it =
                store.getTableAPI().tableIterator(pTable.createPrimaryKey()
                    , null
                    , null);

            /* iterate the rows */
            while ( it.hasNext()) {
                @SuppressWarnings("unused")
				Row r = it.next();
            }
            it.close();
        }
    }

    public byte[] serializePreparedStmt(PreparedStatementImpl stmt)
        throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);

        stmt.serializeForProxy(out);
        return baos.toByteArray();
    }

    @Test
    public void testKvstore1370() {

        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);

        executeDdl("create table foo(id integer , primary key(id))");

        executeDML("insert into foo values(1)", 1);

        executeDdl("alter table foo " +
                   "(add idcol integer generated always as identity)");

        List<RecordValue> res = executeDML("select * from foo", 2);

        assertTrue(res.size() == 1);
    }

    @Test
    public void testBigCollect() {

        String ddl =
            "create table foo (" +
            "id integer, grp1 integer, grp2 integer, str string, " +
            "primary key(id))";

        executeDdl(ddl);

        executeDdl("create index idx on foo(grp1)");

        Table table = tableImpl.getTable(getNamespace(), "foo");
        Row row = table.createRow();

        byte[] data = new byte[1024];
        for (int i = 0; i < 1024; ++ i) {
            data[i] = 'a';
        }
        String str = new String(data);

        int id = 0;
        int numGroups = 10;
        int numRowsPerGroup = 128;

        for (int grp = 0; grp < numGroups; ++grp) {

            for (int i = 0; i < numRowsPerGroup; ++i) {

                row.put("id", id);
                row.put("grp1", grp);
                row.put("grp2", grp);
                row.put("str", (id + str));

                tableImpl.put(row, null, null);
                ++id;
            }
        }

        ExecuteOptions options = new ExecuteOptions();
        //options.setTraceLevel((byte)2);
        options.setMaxServerMemoryConsumption(10 * 1024);

        String query = "select grp1, count(distinct str) as cnt from foo group by grp1";

        List<RecordValue> results = executeDML(query, 0, options);
        assert(results.size() == 10);

        for (RecordValue res : results) {
            int cnt = res.get("cnt").asInteger().get();
            assert(cnt == 128);
        }

        query = "select grp2, count(distinct str) as cnt from foo group by grp2";
        //options.setTraceLevel((byte)3);

        results = executeDML(query, 0, options);
        assert(results.size() == 10);

        for (RecordValue res : results) {
            int cnt = res.get("cnt").asInteger().get();
            assert(cnt == 128);
        }
    }

    @Test
    public void testInsertKeyRowSize() {
        final int maxKeySize = 64;
        final int maxRowSize = 1024;

        ExecuteOptions execOpts = new ExecuteOptions()
                .setNamespace(getNamespace())
                .setMaxPrimaryKeySize(maxKeySize)
                .setMaxRowSize(maxRowSize);

        String ddl;
        ddl = "create table if not exists foo(" +
                "pk string, s string, primary key(pk))";
        executeDdl(ddl);

        ddl = "create table if not exists fooID(" +
                "id integer generated always as identity, pk string, " +
                "s string, primary key(id, pk))";
        executeDdl(ddl);

        ddl = "create table if not exists foo2pk(" +
               "sk string, pk string, s string, primary key(shard(sk), pk))";
        executeDdl(ddl);

        String s64 = makeString(maxKeySize);
        String s65 = s64 + 'a';

        String s1020 = makeString(maxRowSize - 4); /* 4 bytes - overhead */
        String s1021 = s1020 + 'a';

        String fmt;
        String insert;

        /*
         * Simple insert
         */

        fmt = "insert into foo values('%s', '%s')";
        insert = String.format(fmt, s64, s1020);
        runPrepareSizeLimit(insert, execOpts, false /* shouldFail */, false);
        runQuerySizeLimit(insert, null, execOpts, false /* shouldFail */, false);

        /* Prepare failed, key size 65 > the limit of 64 */
        insert = String.format(fmt, s65, s1020);
        runPrepareSizeLimit(insert, execOpts, true /* shouldFail */,
                            true /* keySizeExceeded */);

        if (!mrTableMode) {
            /*
             * Don't run this case in multi-region table mode, because in the
             * preparing phase, the region id of the Row is not set yet, which
             * result in failure when construct Value object to get its size,
             * so pass through the check.
             */
            /* Prepare should have failed, row size 1025 > the limit of 1024 */
            insert = String.format(fmt, s64, s1021);
            runPrepareSizeLimit(insert, execOpts, true /* shouldFail */,
                                false /* keySizeExceeded */);
        }

        /*
         * Insert to table with identity column
         */
        String s59 = makeString(59);
        fmt = "insert into fooID(pk, s) values ('%s', '%s')";
        insert = String.format(fmt, s59, s1020);
        runQuerySizeLimit(insert, null, execOpts, false/* shouldFail */, false);

        /*
         * Row size 1025 > the limit of 1024, this should be detected during
         * preparing.
         */
        insert = String.format(fmt, s59, s1021);
        runPrepareSizeLimit(insert, execOpts, true /* shouldFail */,
                            false /* keySizeExceeded */);

        /* Key size 65 > the limit of 64, failed in runtime */
        insert = String.format(fmt, s59 + 'a', s1020);
        runQuerySizeLimit(insert, null, execOpts, true /* shouldFail */,
                          true /* keySizeExceeded */);

        /* Row size 1025 > the limit of 1024, failed in runtime */
        insert = String.format(fmt, s59, s1021);
        runQuerySizeLimit(insert, null, execOpts, true /* shouldFail */,
                          false /* keySizeExceeded */);

        /*
         * Insert with bound variables
         */
        Map<String, String> values = new HashMap<>();
        insert = "declare $pk string; $s string; insert into foo values($pk, $s)";
        values.put("$pk", s64);
        values.put("$s", s1020);
        runQuerySizeLimit(insert, values, execOpts, false /* shouldFail */,
                          false);

        /* Key size 65 > the limit of 64, failed in runtime */
        values.clear();
        values.put("$pk", s65);
        values.put("$s", s1020);
        runQuerySizeLimit(insert, values, execOpts, true /* shouldFail */,
                          true /* keySizeExceeded */);

        /* Row size 1025 > the limit of 1024, failed in runtime */
        values.clear();
        values.put("$pk", s64);
        values.put("$s", s1021);
        runQuerySizeLimit(insert, values, execOpts, true /* shouldFail */,
                          false /* keySizeExceeded */);

        /*
         * Test on table with 2 pk fields.
         */
        String s32 = makeString(32);
        String s33 = makeString(33);
        fmt = "insert into foo2pk values ('%s', '%s', 'a')";
        insert = String.format(fmt, s32, s32);
        runQuerySizeLimit(insert, null, execOpts, false/* shouldFail */, false);

        /* Key size 65 > the limit of 64, failed in compiling */
        insert = String.format(fmt, s33, s32);
        runPrepareSizeLimit(insert, execOpts, true/* shouldFail */,
                            true /* keySizeExceeded */);

        insert = String.format(fmt, s32, s33);
        runPrepareSizeLimit(insert, execOpts, true/* shouldFail */,
                            true /* keySizeExceeded */);

        /* Key size 65 > the limit of 64, failed in runtime */
        insert = "declare $sk string; $pk string; " +
                 "insert into foo2pk values($sk, $pk, 'a')";
        values.clear();
        values.put("$sk", s33);
        values.put("$pk", s32);
        runQuerySizeLimit(insert, values, execOpts, true /* shouldFail */,
                          true /* keySizeExceeded */);

        values.clear();
        values.put("$sk", s32);
        values.put("$pk", s33);
        runQuerySizeLimit(insert, values, execOpts, true /* shouldFail */,
                          true /* keySizeExceeded */);
    }

    @Test
    public void testGeom() {
        final String points =
            "create table points (id integer, info json, primary key(id))";
        final String poly =
            "create table polygons (id integer, info json, primary key(id))";
        final String pointIndex =
            "create index idx_kind_ptn on points(info.kind as string," +
            "info.point as point)";
        final String geoIndex =
            "create index idx_geom on polygons(info.geom as geometry " +
            "{\"max_covering_cells\":400})";

        executeDdl(points);
        executeDdl(poly);
        executeDdl(pointIndex);
        executeDdl(geoIndex);
        TableImpl ptable = getTable(getNamespace(), "points");
        TableImpl gtable = getTable(getNamespace(), "polygons");
        assertNotNull(ptable);
        assertNotNull(gtable);
        final RegionMapper regionMapper =
            ((TableAPIImpl) store.getTableAPI()).getRegionMapper();
        DDLGenerator gen1 = new DDLGenerator(ptable, regionMapper);
        DDLGenerator gen2 = new DDLGenerator(gtable, regionMapper);
        assertTrue(gen1.getDDL().contains("points"));
        assertTrue(gen2.getDDL().contains("polygons"));
        for (String s : gen1.getAllIndexDDL()) {
            assertTrue(s.contains("Point"));
        }
        for (String s : gen2.getAllIndexDDL()) {
            assertTrue(s.contains("Geometry"));
        }
    }

    /*
     * Verify that setting query tracing works, assuming that the
     * store (CreateStore) has been set to allow un-hiding of user data.
     * This is done in TableTestBase when creating the store
     */
    @Test
    public void testHideData() throws Exception {
        final String createTable =
            "create table foo(" +
            "pk string, name string, primary key(pk))";
        ExecuteOptions options = new ExecuteOptions()
            .setNamespace(getNamespace(), false)
            .setTraceLevel((byte)3)
            .setDoLogFileTracing(false);

        executeDdl(createTable);
        final String query = "insert into foo values (2, \"joe\")";
        StatementResult sr = store.executeSync(query, options);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, "UTF8")) {
            sr.printTrace(ps);
            String output = baos.toString("UTF8");
            /* NOTE: if tracing output changes this may need to change */
            assertTrue(output.contains("Row to insert"));

        }
    }

    /*
     * Test frozen table, freeze, unfreeze
     */
    @Test
    public void testFrozen() {

        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);

        final String frozen = "create table frozen(id integer, name string, " +
            "primary key(id)) WITH SCHEMA FROZEN";
        final String frozen_force = frozen + " FORCE";
        final String unfreeze = "alter table frozen unfreeze schema";
        final String freeze = "alter table frozen freeze schema";
        final String freeze_force = freeze + " force";

        final String frozen_child = "create table frozen.child(cid integer, " +
            "info json, primary key(cid)) WITH SCHEMA FROZEN";
        final String frozen_child_force = frozen_child + " FORCE";
        final String unfreeze_child = "alter table frozen.child unfreeze schema";
        final String freeze_child = "alter table frozen.child freeze schema";
        final String freeze_child_force = freeze_child + " force";

        /*
         * Operations should succeed, even if freezing is a no-op
         */
        executeDdl(frozen);
        executeDdl(unfreeze);
        executeDdl(freeze);
        executeDdl(unfreeze);
        executeDdl(freeze_force);

        executeDdl(frozen_child);
        executeDdl(unfreeze_child);
        executeDdl(freeze_child);
        executeDdl(unfreeze_child);
        executeDdl(freeze_child_force);

        /*
         * Use a prepare callback and assert callback state
         */
        /* args = (statement, tableName, freezeTrue, unfreezeTrue, freezeForceTrue) */
        doPrepareWithCallback(frozen, "frozen", true, false, false);
        doPrepareWithCallback(frozen_force, "frozen", true, false, true);
        doPrepareWithCallback(freeze, "frozen", true, false, false);
        doPrepareWithCallback(freeze_force, "frozen", true, false, true);
        doPrepareWithCallback(unfreeze, "frozen", false, true, false);

        doPrepareWithCallback(frozen_child, "frozen.child", true, false, false);
        doPrepareWithCallback(frozen_child_force, "frozen.child",
                              true, false, true);
        doPrepareWithCallback(freeze_child, "frozen.child", true, false, false);
        doPrepareWithCallback(freeze_child_force, "frozen.child",
                              true, false, true);
        doPrepareWithCallback(unfreeze_child, "frozen.child",
                              false, true, false);
    }

    private void doPrepareWithCallback(String statement,
                                       String tableName,
                                       boolean freezeTrue,
                                       boolean unfreezeTrue,
                                       boolean freezeForce) {

        PrepareCB pc = getPrepareCB(statement);

        if (freezeTrue) {
            assertTrue(pc.getFreeze());
            assertNull(pc.getUnfreeze());
            assertEquals(freezeForce, pc.getFreezeForce());
        }
        if (unfreezeTrue) {
            assertTrue(pc.getUnfreeze());
            assertNull(pc.getFreeze());
            assertNull(pc.getFreezeForce());
        }
        if (tableName != null) {
            assertEquals(pc.getTableNames().get(0), tableName);
        }
    }

    /*
     * Test PrepareCallback.alterTtlFound()
     */
    @Test
    public void testPrepareAlterTtl() {
        final String tableDdl = "create table testTtl(id integer, s string, " +
                                "primary key(id)) using ttl 3 days";
        final String alterTtl = "alter table testTtl using ttl 0 days";
        final String addField = "alter table testTtl(add age integer)";

        PrepareCB pc = getPrepareCB(tableDdl);
        assertNull(pc.getAlterTtl());

        pc = getPrepareCB(alterTtl);
        assertTrue(pc.getAlterTtl());

        pc = getPrepareCB(addField);
        assertNull(pc.getAlterTtl());
    }

    private PrepareCB getPrepareCB(String statement) {

        ExecuteOptions opts = new ExecuteOptions();
        PrepareCB pc = new PrepareCB();
        //pc.setPreparedNeeded(true);
        opts.setPrepareCallback(pc);
        /*
         * prepare will fail for many operations because there is no
         * table metadata easily available. Let the callbacks happen
         * and fill in what is available
         */
        try {
            CompilerAPI.prepare(null, statement.toCharArray(), opts);
        } catch (IllegalArgumentException iae) {
            // ignore
        }

        return pc;
    }

    @Test
    public void testQueryStatementResultImpl() {
        String createTableDdl = "CREATE TABLE IF NOT EXISTS TPosition" +
                "(id INTEGER, isActive BOOLEAN, datBinary BINARY,"+
                "datDouble DOUBLE, datFloat FLOAT, datLong LONG," +
                "datString STRING, "+
                "primary key (id))";
        String query = "declare $v1 integer; \n" +
                "$v2 boolean; \n" +
                "$v3 binary; \n" +
                "$v4 double; \n" +
                "$v5 float; \n" +
                "$v6 long; \n" +
                "$v7 string; \n" +
                "SELECT count(*) as Count FROM TPosition WHERE id = $v1 " +
                "AND isActive = $v2 " +
                "AND datBinary = $v3 " +
                "AND datDouble = $v4 " +
                "AND datFloat = $v5 " +
                "AND datLong = $v6 " +
                "AND datString = $v7 ";

        /* Prepare table */
        executeDdl(createTableDdl, null, true);
        Table table = tableImpl.getTable("TPosition");
        assertNotNull(table);
        for (int i = 0; i < 10; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("isActive", true);
            row.put("datBinary", new byte[] { (byte) i });
            row.put("datDouble", Double.valueOf(i));
            row.put("datFloat", Float.valueOf(i));
            row.put("datLong", Long.valueOf(i));
            row.put("datString", ("a string: " + i));
            assertNotNull(tableImpl.put(row, null, null));
        }

        /* Set pos1 (id) 2 to 2, expect 1 row returned
        * {"id":2,"isActive":true,"datBinary":"Ag==",
        * "datDouble":2.0,"datFloat":2.0,
        * "datLong":2,"datString":"a string: 2"}
        */
        ExecuteOptions options = new ExecuteOptions();
        options.setNamespace(getNamespace(), false);
        options.setAsync(false);
        options.setTraceLevel((byte) 2);
        PreparedStatementImpl prepStmt =
                (PreparedStatementImpl) store.prepare(query);
        BoundStatement bs = prepStmt.createBoundStatement();
        bs.setVariable(1, 2);
        bs.setVariable(2, true);
        bs.setVariable(3, new byte[] { 2 });
        bs.setVariable(4, 2d);
        bs.setVariable(5, 2f);
        bs.setVariable(6, 2l);
        bs.setVariable(7, "a string: 2");
        QueryStatementResultImpl sr =
                (QueryStatementResultImpl) store.executeSync(bs, options);
        Iterator<RecordValue> iter = sr.iterator();
        List<RecordValue> results = new ArrayList<RecordValue>();

        /* test block */
        assertTrue(prepStmt.getExternalVarsTypes().size() == 7);
        assert(prepStmt.getVariableType("$v1").isInteger());
        assertFalse(sr.isDone());
        while (iter.hasNext()) {
            results.add(iter.next());
        }
        assertTrue(results.size() == 1);
        assertTrue(sr.isDone());
        assertNull(sr.getErrorMessage());
        assertNull(sr.getInfo());
        assertNull(sr.getInfoAsJson());
        assertTrue((sr.getKind().toString()).equals("QUERY"));
        assertTrue(sr.getReadKB() == 1);
        assertTrue(sr.getWriteKB() == 0);
        assertNull(sr.getResult());
        assertNotNull(sr.getResultDef());
        assertTrue(sr.isSuccessful());
        assertFalse(sr.isCancelled());
        assertEquals(sr.getPlanId(), 0);
        assertNull(sr.getPublisher());
        assertFalse(sr.hasSortPhase1Result());
        assertFalse(sr.reachedLimit());
        assertNull(sr.getContinuationKey());
        assertNotNull(sr.getRCB());
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             PrintStream ps =
                new PrintStream(baos,true, "UTF8")) {
            sr.printTrace(ps);
            assert(baos.toString().contains("CLIENT"));
        } catch (Exception e) {
            fail("Exception not expected: " + e.getMessage());
        }
        try {
            sr.close();
            sr.iterator();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
        try {
            HashMap<String, FieldValue> vars = new HashMap<String, FieldValue>();
            vars.put("$v1", null);
            prepStmt.getExternalVarsArray(vars);
            fail("Expected to fail becaise $v1 value is null");
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    @Test
    public void testPreparedStatementImpl() {
        executeDdl(complexTableStatement, null, true);

        String query = "SELECT * FROM Complex WHERE id = 2 ";
        PreparedStatementImpl prepStmt =
                (PreparedStatementImpl) store.prepare(query);

        /* test block */
        assertNotNull(prepStmt.executeSync((KVStoreImpl)store, null));
        assertTrue(prepStmt.isSimpleQuery());
        PreparedStatementImpl.setTestSerialVersion(CURRENT);
        assertEquals(0, prepStmt.getExternalVarsTypes().size());
        assertNull(prepStmt.getExternalVarInfo("x"));
        assertEquals(0, prepStmt.getVariableTypes().size());
        assertNull(prepStmt.getVariableType("x"));
        assertFalse(prepStmt.isJsonCollectionSelectStar());
        assertNull(prepStmt.getExternalVarType("x"));
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DataOutput out = new DataOutputStream(baos);
            prepStmt.toByteArray(out);
        } catch (IOException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
        /* serializeForDriver expecting an assertion error
        * from PlanIter.writeForCloud
        */
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DataOutput out = new DataOutputStream(baos);
            FieldValueWriter valWriter = new FieldValueWriter() {
                @Override
                public void writeFieldValue(DataOutput dout, FieldValue value)
                throws IOException {//Nothing to do
                }
            };
            prepStmt.serializeForDriver(out,
                    Integer.valueOf(1).shortValue(),
                    valWriter);
        } catch (Exception e) {
            fail("Exception not expected :" + e.getMessage());
        } catch (AssertionError ae) {
            assertTrue(true);
        }
        /* IllegalArgumentException expected */
        try {
            prepStmt.varPosToName(100);
            fail("IllegalArgumentException expected");
        } catch (Exception e) {
            assertTrue(true);
        }

        /* complex query block */
        String complexQuery =
                "SELECT count(*) as Count FROM Complex WHERE id = 1 ";
        prepStmt = (PreparedStatementImpl) store.prepare(complexQuery);
        assertFalse(prepStmt.isSimpleQuery());

        try {
            HashMap<String, FieldValue> vars = new HashMap<String, FieldValue>();
            vars.put("$v1",
                    FieldDefImpl.Constants.integerDef.createValue(1));
            prepStmt.getExternalVarsArray(vars);
            fail("Expected to fail because $v1 does not appear in query");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(),
                    "Variable $v1 does not appear in query");
        }
    }

    @Test
    public void testPreparedStatementImplDeserializeException() {
        /* version case */
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream daos = new DataOutputStream(baos)) {
            daos.writeShort(SerialVersion.CURRENT + 1);
            daos.flush();
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            DataInputStream in = new DataInputStream(bais);
            PreparedStatementImpl prepStmtExp = new PreparedStatementImpl(in);
            in.close();
            bais.close();
            fail("Expected IllegalArgumentException, but got: " + prepStmtExp);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
        /* ordinal case */
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream daos = new DataOutputStream(baos)) {
            daos.writeShort(SerialVersion.CURRENT);
            daos.writeByte(-2);
            daos.flush();
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            DataInputStream in = new DataInputStream(bais);
            PreparedStatementImpl prepStmtExp = new PreparedStatementImpl(in);
            in.close();
            bais.close();
            fail("Expected IllegalArgumentException, but got: " + prepStmtExp);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
        /* RuntimeException case */
        try {
            PreparedStatementImpl prepStmtExp = new PreparedStatementImpl(null);
            fail("Expected IllegalArgumentException, but got: " + prepStmtExp);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException || e instanceof NullPointerException);
        }
    }

    @Test
    public void testSetVariablesByPosition() {
        String createTableDdl = "CREATE TABLE IF NOT EXISTS TPosition" +
        "(id INTEGER, isActive BOOLEAN, datBinary BINARY, datDouble DOUBLE," +
        "datFloat FLOAT, datLong LONG, datString STRING, primary key (id))";
        executeDdl(createTableDdl, null, true);
        Table table = tableImpl.getTable("TPosition");
        assertNotNull(table);

        for (int i = 0; i < 10; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("isActive", true);
            row.put("datBinary", new byte[] { (byte) i });
            row.put("datDouble", Double.valueOf(i));
            row.put("datFloat", Float.valueOf(i));
            row.put("datLong", Long.valueOf(i));
            row.put("datString", ("a string: " + i));
            assertNotNull(tableImpl.put(row, null, null));
        }

        String query = "declare $v1 integer; \n" +
                        "$v2 boolean; \n" +
                        "$v3 binary; \n" +
                        "$v4 double; \n" +
                        "$v5 float; \n" +
                        "$v6 long; \n" +
                        "$v7 string; \n" +
                        "SELECT * FROM TPosition WHERE id = $v1 " +
                        "AND isActive = $v2 " +
                        "AND datBinary = $v3 " +
                        "AND datDouble = $v4 " +
                        "AND datFloat = $v5 " +
                        "AND datLong = $v6 " +
                        "AND datString = $v7 ";
        PreparedStatement prepStmt = store.prepare(query);
        BoundStatement bs = prepStmt.createBoundStatement();

        /* Set pos1 (id) 2 to 2, expect 1 row returned
        {"id":2,"isActive":true,"datBinary":"Ag==","datDouble":2.0,"datFloat":2.0,
        "datLong":2,"datString":"a string: 2"}*/
        bs.setVariable(1, 2);
        bs.setVariable(2, true);
        bs.setVariable(3, new byte[] {2});
        bs.setVariable(4, 2d);
        bs.setVariable(5, 2f);
        bs.setVariable(6, 2L);
        bs.setVariable(7, "a string: 2");

        StatementResult sr = store.executeSync(bs);
        Iterator<RecordValue> iter = sr.iterator();
        List<RecordValue> results = new ArrayList<RecordValue>();

        while (iter.hasNext()) {
            results.add(iter.next());
        }
        assertTrue(results.size() == 1);
    }

    @Test
    public void testPartitionedQueries() throws Exception {
        assumeTrue("Only when testing async", AsyncControl.serverUseAsync);
        final int numRows = 1000;
        final int partitionsPerSplit = 2;
        final String tableName = "ParallelQuery";
        final String createTable = "create table " + tableName +
            "(id integer, primary key(id)) as json collection";
        final String createIndex = "create index idx on " + tableName +
            "(name as string)";
        final String query = "select * from " + tableName;
        final String indexQuery = "select * from " + tableName +
            " where name > 'm'";
        final String forcePrimary = "select /*+ FORCE_PRIMARY_INDEX(" +
            tableName + ") */  * from " + tableName + " where name > 'm'";
        executeDdl(createTable, null, true);
        executeDdl(createIndex, null, true);
        TableLimits limits = new TableLimits(5000, 10000, 1);
        ExecutionFuture f =
            ((KVStoreImpl)store).setTableLimits(null, tableName, limits);
        assertTrue(f.get().isSuccessful());
        TableImpl table = (TableImpl) tableImpl.getTable(tableName);
        assertNotNull(table);

        for (int i = 0; i < numRows; i++) {
            Row row = table.createRow();
            row.put("id", i);
            row.put("name", ("name_" + i));
            row.put("age", (i % 25));
            assertNotNull(tableImpl.put(row, null, null));
        }

        /*
         * driver version and cloud query are needed to track throughput.
         * setting is simple query is needed for use of the execute variant
         * that uses Set<RepGroupId>
         */
        ExecuteOptions options =
            new ExecuteOptions().setResultsBatchSize(1).setAsync(false).
            setDriverQueryVersion(5).setIsCloudQuery(true).setIsSimpleQuery(true);
        PreparedStatementImpl ps =
            (PreparedStatementImpl) store.prepare(query, options);
        assertTrue(ps.getDistributionKind().equals(
                       PreparedStatementImpl.DistributionKind.ALL_PARTITIONS));
        assertTrue(ps.isSimpleQuery());
        int numPartitions =
            ((KVStoreImpl)store).getTopology().getNumPartitions();

        ArrayList<HashSet<Integer>> splits = new ArrayList<>();
        HashSet<Integer> currentSplit = new HashSet<>();
        splits.add(currentSplit);
        for (int i = 0; i < numPartitions; i++) {
            if (i > 0 && (i % partitionsPerSplit) == 0) {
                currentSplit = new HashSet<>();
                splits.add(currentSplit);
            }
            currentSplit.add(i + 1); /* partition ids are 1-based */
        }

        int count = 0;
        int readKB = 0;
        for (HashSet<Integer> s : splits) {
            QueryStatementResultImpl result =
                (QueryStatementResultImpl) ps.executeSyncPartitions(
                    (KVStoreImpl)store, options, s);
            Iterator<RecordValue> iter = result.iterator();
            while (iter.hasNext()) {
                count++;
                iter.next();
            }
            readKB += result.getReadKB();
        }
        assertEquals(numRows, count);
        assertEquals(numRows, readKB);

        /*
         * all shard query
         */
        ps = (PreparedStatementImpl) store.prepare(indexQuery, options);
        assertTrue(ps.getDistributionKind().equals(
                       PreparedStatementImpl.DistributionKind.ALL_SHARDS));
        assertTrue(ps.isSimpleQuery());
        int numShards =
            ((KVStoreImpl)store).getTopology().getNumRepGroups();

        /* use one set of size numShards */
        HashSet<RepGroupId> currentSsplit = new HashSet<>();
        for (int i = 0; i < numShards; i++) {
            currentSsplit.add(new RepGroupId(i+1));
        }

        MySubscriber qsub = new MySubscriber(options.getResultsBatchSize());
        execShardsAsync(ps, options, currentSsplit, qsub);
        assertEquals(numRows, qsub.numResults);
        assertEquals(numRows, qsub.readKB);

        /* use numShard splits, each with a single entry */
        count = 0;
        readKB = 0;
        for (int i = 0; i < numShards; i++) {
            HashSet<RepGroupId> split = new HashSet<>();
            split.add(new RepGroupId(i+1));
            qsub = new MySubscriber(options.getResultsBatchSize());
            execShardsAsync(ps, options, split, qsub);
            count += qsub.numResults;
            readKB += qsub.readKB;
        }
        assertEquals(numRows, count);
        assertEquals(numRows, readKB);

        /* make sure forcing use of primary index results in ALL_PARTITIONS */
        ps = (PreparedStatementImpl) store.prepare(forcePrimary, options);
        assertTrue(ps.getDistributionKind().equals(
                       PreparedStatementImpl.DistributionKind.ALL_PARTITIONS));
    }

    private void execShardsAsync(PreparedStatementImpl ps,
                                 ExecuteOptions options,
                                 HashSet<RepGroupId> split,
                                 MySubscriber qsub) {

        Publisher<RecordValue> qpub =
            ((KVStoreImpl)store).executeAsync(ps, options, split);

        qpub.subscribe(qsub);

        synchronized (qsub) {
            while (!qsub.isDone) {
                try {
                    qsub.wait();
                } catch (InterruptedException e) {
                    fail("Interrupted while waiting for async query");
                }
            }
        }
        if (qsub.err != null) {
            throw new RuntimeException(qsub.err);
        }
    }

    private class MySubscriber implements Subscriber<RecordValue> {
        int numResults;
        int readKB;
        Throwable err;
        Subscription subscription;
        final int batchSize;
        volatile boolean isDone = false;

        MySubscriber(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void onSubscribe(Subscription s) {
            subscription = s;
            s.request(batchSize);
        }

        @Override
        public void onNext(RecordValue value) {
            numResults++;
            if (numResults % batchSize == 0) {
                subscription.request(batchSize);
            }
        }

        @Override
        public void onError(Throwable e) {
            err = e;
            isDone = true;
            notifyAll();
        }

        @SuppressWarnings("null")
        @Override
        public synchronized void onComplete() {
            QueryStatementResultImpl qres =
                ((QuerySubscription)subscription).
                getAsyncIterator().getQueryStatementResult();
            readKB = qres.getReadKB();
            isDone = true;
            notifyAll();
        }
    }

    private void runPrepareSizeLimit(String statement,
                                     ExecuteOptions options,
                                     boolean shouldFail,
                                     boolean keySizeExceeded) {
        try {
            store.prepare(statement, options);
            if (shouldFail) {
                fail("Prepare should fail but succeed");
            }
        } catch (ResourceLimitException ex) {
            if (!shouldFail) {
                fail("Prepare failed: " + ex.getMessage());
            }
            if (keySizeExceeded) {
                assertTrue(ex instanceof KeySizeLimitException);
            } else {
                assertTrue(ex instanceof ValueSizeLimitException);
            }
        }
    }

    private void runQuerySizeLimit(String statement,
                                   Map<String, String> values,
                                   ExecuteOptions options,
                                   boolean shouldFail,
                                   boolean keySizeExceeded) {
        Statement stmt = null;
        if (values != null) {
            PreparedStatement prep = store.prepare(statement, options);
            BoundStatement bs = prep.createBoundStatement();
            for (Map.Entry<String, String> e : values.entrySet()) {
                bs.setVariable(e.getKey(), e.getValue());
            }
            stmt = bs;
        }

        try {
            if (stmt != null) {
                store.executeSync(stmt, options);
            } else {
                store.executeSync(statement, options);
            }

            if (shouldFail) {
                fail("Query should fail but succeed");
            }
        } catch (ResourceLimitException ex) {
            if (!shouldFail) {
                fail("Query failed: " + ex.getMessage());
            }
            if (keySizeExceeded) {
                assertTrue(ex instanceof KeySizeLimitException);
            } else {
                assertTrue(ex instanceof ValueSizeLimitException);
            }
        }
    }

    private static String makeString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append((char)((i % 10) + 'a'));
        }
        return sb.toString();
    }
}
