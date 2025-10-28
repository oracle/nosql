/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Test;

import oracle.kv.table.FieldDef.Type;
import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.PrepareQueryException;
import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.StringValue;

/**
 * Test queries
 */
public class QueryTest extends ProxyTestBase {
    final static int READ_KB_LIMIT = rlimits.getRequestReadKBLimit();

    private static byte traceLevel = 0;
    private static boolean showResults = false;

    private final static int MIN_QUERY_COST = 2;

    final static String tableName = "scanTable";
    final static String indexName = "idxName";
    final static String jsonTable = "jsonTable";
    /* timeout for all table operations */
    final static int timeout = 20000;

    /* Create a table */
    final static String createTableDDL =
        "CREATE TABLE IF NOT EXISTS scanTable (" +
        "sid INTEGER, " +
        "id INTEGER, " +
        "name STRING, " +
        "age INTEGER, " +
        "state STRING, " +
        "salary LONG, " +
        "array ARRAY(INTEGER), " +
        "longString STRING," +
        "PRIMARY KEY(SHARD(sid), id))";

    /* Create an index on scanTable(name) */
    final String createIdxNameDDL =
        "CREATE INDEX IF NOT EXISTS idxName on scanTable(name)";

    /* Create an index on scanTable(sid, age)*/
    final String createIdxSidAgeDDL =
        "CREATE INDEX IF NOT EXISTS idxSidAge ON scanTable(sid, age)";

    /* Create an index on scanTable(state, age)*/
    final String createIdxStateAgeDDL =
        "CREATE INDEX IF NOT EXISTS idxStateAge ON scanTable(state, age)";

   /* Create an index on scanTable(state, age)*/
    final String createIdxArrayDDL =
        "CREATE INDEX IF NOT EXISTS idxArray ON scanTable(array[])";

    /* Create a table with Json field */
    final static String createJsonTableDDL =
        "CREATE TABLE IF NOT EXISTS jsonTable (id INTEGER, info JSON, " +
        "PRIMARY KEY(id))";

    /* Create a table with 2 major keys, used in testIllegalQuery() */
    final static String createTestTableDDL =
        "CREATE TABLE IF NOT EXISTS test (" +
            "sid1 INTEGER, " +
            "sid2 INTEGER, " +
            "id INTEGER, " +
            "name STRING, " +
            "PRIMARY KEY(SHARD(sid1, sid2), id))";

    final static String createIdxSid1NameDDL =
        "CREATE INDEX IF NOT EXISTS idxSid1Name ON test(sid1, name)";

    final static String createIdxNameSid1Sid2DDL =
        "CREATE INDEX IF NOT EXISTS idxNameSid1Sid2 ON test(name, sid1, sid2)";

    @Override
    public void setUp() throws Exception {
        super.setUp();

        if (!testName.getMethodName().equals("testTimeout") &&
            !testName.getMethodName().equals("testShortTimeouts")) {
            tableOperation(handle, createTableDDL,
                           new TableLimits(45000, 15000, 50),
                           TableResult.State.ACTIVE, timeout);

            tableOperation(handle, createIdxNameDDL, null,
                           TableResult.State.ACTIVE, timeout);
        }
    }

    @Test
    public void testTimeout() {
        /*
         * This test requires large table read/write throughput, it is not
         * applicable in cloud test.
         */
        assumeTrue(!useCloudService);
        runTimeoutTest(1, false);
    }

    @Test
    public void testShortTimeouts() {
       /*
        * This test requires large table read/write throughput, it is not
        * applicable in cloud test.
        */
        assumeTrue(!useCloudService);
        runTimeoutTest(5, true);
    }

    private void runTimeoutTest(int numLoops, boolean shortTimeouts) {

        /* Run this test against kv >= 21.3.1 */
        assumeKVVersion("testTimeout", 21, 3, 1);

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
        int rus = (cloudRunning ?
                   tenantLimits.getStandardTableLimits().getTableReadUnits():
                   100000);
        int wus = (cloudRunning ?
                   tenantLimits.getStandardTableLimits().getTableWriteUnits():
                   100000);

        tableOperation(handle, tableDDL, new TableLimits(rus, wus, 50),
                       TableResult.State.ACTIVE, timeout);

        MapValue row = new MapValue();
        PutRequest putRequest = new PutRequest()
            .setValue(row)
            .setTableName("foo");

        for (int i = 0; i < num1; i++) {

            for (int j = 0; j < num2; ++j) {

                for (int k = 0; k < num3; ++k) {

                    row.put("id1", rand.nextInt(num1));
                    row.put("id2", rand.nextInt(num2));
                    row.put("id3", rand.nextInt(num3));
                    row.put("id4", ("id4-" + i));
                    row.put("firstName", ("first" + rand.nextInt(10)));
                    row.put("lastName", ("last" + rand.nextInt(10)));
                    row.put("age",  rand.nextInt(100));

                    PutResult res = getNextHandle().put(putRequest);
                    assertNotNull("Put failed", res.getVersion());
                }
            }
        }

        for (int x=0; x<numLoops; x++) {
            verbose("LOOP " + x + ": shortTimeouts=" + shortTimeouts);
            runTimeoutLoop(shortTimeouts);
        }
    }

    private void runTimeoutLoop(boolean shortTimeouts) {
        /*
         * All of these queries timeout before the timeout fix (kvstore-353)
         * With the fix, they usually all succeed (and if the time out, they
         * are not a runaway query anymore)
         */
        String[] queries = {
             // 0
             "select * from foo",
             // 1
             "select * from foo",
             // 2
             "select * from foo $f where partition($f) = 1",
             // 3
             "select * from foo $f where age < 0",
             // 4
             "select * from foo order by age",
             // 5
             "select age, count(*) from foo group by age",
             // 6
             "select * from foo order by id1",
        };

        /* these time limits cause a lot of timeouts in queries below */
        int[] shortTimes = { 1000,  80,  80,  100, 80,  100, 100 };
        /* these limits generally don't timeout much */
        int[] regularTimes = { 5000,  100,  100,  300, 100,  200, 300 };

        int[] timeouts = (shortTimeouts)?shortTimes:regularTimes;

        int[] batchSizes = { 1000, 8000, 8000, 100, 3000, 1000, 3000 };

        int[] numExpectedResults = { 79568, 79568, 8706, 0, 79568, 100, 79568 };

        long t = 0;
        PrepareRequest preq = new PrepareRequest();

        for (int qid = 0; qid < queries.length; ++qid) {

            NoSQLHandle curHandle = getNextHandle();

            QueryRequest qreq = new QueryRequest();
            qreq.setTimeout(timeouts[qid]);
            qreq.setLimit(batchSizes[qid]);

            /* to allow manual setting of tracing */
            if (qid == 3) {
                qreq.setTraceLevel((byte)0);
            } else {
                qreq.setTraceLevel((byte)0);
            }

            preq.setStatement(queries[qid]);
            PrepareResult pres = curHandle.prepare(preq);

            qreq.setPreparedStatement(pres);

            t = System.currentTimeMillis();
            int numResults = 0;

            long bt = System.currentTimeMillis();

            try {
                int numBatches = 0;
                do {
                    bt = System.currentTimeMillis();

                    QueryResult qres = curHandle.query(qreq);
                    numResults += qres.getResults().size();
                    ++numBatches;

                    long batchTime = System.currentTimeMillis() - bt;

                    /*
                     * Queries that do local post-processing (i.e. sorting)
                     * may take more time in the getResults() call. So we
                     * don't check batch time here for those (query #4).
                     * TODO: some way to get the time taken in post-processing
                     *       from the QueryResult object.
                     */
                    if (qid != 4) {
                        assertTrue("The time consumed per batch should < " +
                                   (timeouts[qid] * 1.4) + "ms but actual " +
                                   batchTime + "ms: " + queries[qid],
                                   batchTime < timeouts[qid] * 1.4);
                    }
                    verbose("Batch [" + qid + "-" + numBatches +
                            "] completed in " + batchTime +
                            "ms and produced " + qres.getResults().size() +
                            " results");
                } while (!qreq.isDone());

                assertTrue(qid == 2 || numResults == numExpectedResults[qid]);
                t = System.currentTimeMillis() - t;
                verbose("Query " + qid + " completed in " + t +
                        "ms and produced " + numResults + " results");
            } catch (RequestTimeoutException e) {

                t = System.currentTimeMillis() - t;
                long batchTime = System.currentTimeMillis() - bt;
                verbose("Query " + qid + " timed out after " + t +
                        "ms (last batch took " + batchTime + "ms)\n" +
                        e.getMessage());
            }
        }
    }

    @Test
    public void testQuery() {

        final String query1 = "select * from scanTable";
        final String query2 = "select * from scanTable where sid >= 8";
        final String query3 =
            "update scanTable f set f.name = 'joe' where sid = 9 and id = 9 ";
        final String query4 =
            "select name from scanTable where sid = 9 and id = 9 ";
        final String query5 =
            "declare $sid integer; $id integer;" +
            "select name from scanTable where sid = $sid and id >= $id";
        final String query6 =
            "select * from scanTable where sid = 0 order by sid, id";
        final String query7 =
            "select * from scanTable where id = 0";
        final String query8 =
            "select sid, id from scanTable where id = 0";
        final String query9 =
            "select * from scanTable where name = 'name_0'";
        final String query10 =
            "select id, name from scanTable where name = 'name_0'";
        final String query11 =
            "select * from scanTable where length(name) = 1";

        final int numMajor = 10;
        final int numPerMajor = 10;
        final int numRows = numMajor * numPerMajor;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        /*
         * Perform a simple query
         */
        executeQuery(query2, null, 20, 0, false);

        /*
         * Perform an update query
         */
        QueryRequest queryRequest = new QueryRequest().setStatement(query3);
        QueryResult queryRes = getNextHandle().query(queryRequest);

        /*
         * Use a simple get query to validate the update
         */
        queryRequest = new QueryRequest().setStatement(query4);
        queryRes = getNextHandle().query(queryRequest);
        assertEquals(1, queryRes.getResults().size());
        assertEquals("joe",
            queryRes.getResults().get(0).get("name").getString());

        /* full scan to count rows */
        executeQuery(query1, null, numRows, 0, false /* usePrepStmt */);
        executeQuery(query1, null, numRows, 0, true /* usePrepStmt */);

        /*
         * Query with external variables
         */
        Map<String, FieldValue> variables = new HashMap<String, FieldValue>();
        variables.put("$sid", new IntegerValue(9));
        variables.put("$id", new StringValue("3"));
        executeQuery(query5, variables, 7, 0, true);

        /* Query with sort */
        executeQuery(query6, null, numPerMajor, 0,
                     false /* usePrepStmt */);
        executeQuery(query6, null, numPerMajor, 0,
                     true /* usePrepStmt */);

        /*
         * Query cost test
         */
        int cost;
        int count;

        /* query1: select * from scanTable */
        cost = getExpReadKB(false /* keyOnly */, recordKB, numRows, numRows);
        count = numRows;
        executeQuery(query1, false /* keyOnly */, false /* indexScan */,
                     count, cost, 0, 0, recordKB);

        /* query2: select * from scanTable where sid >= 8 */
        count = (numMajor - 8) * numPerMajor;
        cost = getExpReadKB(false /* keyOnly */, recordKB, count, count);
        executeQuery(query2, false /* keyOnly */, false /* indexScan */,
                     count, cost, 0, 0, recordKB);

        /* query4: select name from scanTable where sid = 9 and id = 9 */
        cost = getExpReadKB(false /* keyOnly */, recordKB, 1, 1);
        count = 1;
        executeQuery(query4, false /* keyOnly */, false /* indexScan */,
                     count, cost, 0, 0, recordKB);

        /* query7: select * from scanTable where id = 0 */
        cost = getExpReadKB(false /* keyOnly */, recordKB, numMajor, numRows);
        count = numMajor;
        executeQuery(query7, false /* keyOnly */, false /* indexScan */,
                     count, cost, 0, 0, recordKB);

        /* query8: select sid, id from scanTable where id = 0 */
        cost = getExpReadKB(true /* keyOnly */, recordKB, 0, numRows);
        count = numMajor;
        executeQuery(query8, true /* keyOnly */, false /* indexScan */,
                     count, cost, 0, 0, recordKB);

        /* query9: select * from scanTable where name = 'name_0'"*/
        cost = getExpReadKB(false /* keyOnly */, recordKB, numMajor, numMajor);
        count = numMajor;
        executeQuery(query9, false /* keyOnly */, true /* indexScan */,
                     count, cost, 0, 0, recordKB);

        /* query10: select id, name from scanTable where name = 'name_0' */
        cost = getExpReadKB(true /* keyOnly */, recordKB, 0, numMajor);
        count = numMajor;
        executeQuery(query10, true /* keyOnly */, true /* indexScan */,
                     count, cost, 0, 0, recordKB);

        /* query11: select * from scanTable where length(name) = 1 */
        cost = getExpReadKB(false /* keyOnly */, recordKB,
                            (dontDoubleChargeKey() ? 0 : numRows), numRows);
        count = 0;
        executeQuery(query11, false /* keyOnly */, true /* indexScan */,
                     count, cost, 0, 0, recordKB);
    }

    /**
     * Test query with numeric-base and size-based limits
     */
    @Test
    public void testLimits() {
        final int numMajor = 10;
        final int numPerMajor = 101;
        final int numRows = numMajor * numPerMajor;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        /*
         * number-based limit
         */

        /* Read rows from all partitions with number-based limit. */
        String query = "select * from scanTable";
        int expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                     numRows /* numReadRows */,
                                     numRows /* numReadKeys */);
        int expCnt = numRows;
        int[] limits = new int[] {0, 20, 100, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, limit, 0, recordKB);
        }

        /* Read rows from single partition with number-based limit. */
        query = "select * from scanTable where sid = 5";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numPerMajor /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = numPerMajor;
        limits = new int[] {0, 20, 100, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0, recordKB);
        }

        /* Read rows from all shards with number-based limit. */
        query = "select * from scanTable where name = 'name_1'";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numMajor /* numReadRows */,
                                 numMajor /* numReadKeys */);
        expCnt = numMajor;
        limits = new int[] {0, 5, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */, recordKB);
        }

        /*
         * Size-based limit
         */

        /* Read rows from all partitions with size limit. */
        query = "select * from scanTable";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numRows;
        int[] maxReadKBs = new int[] {0, 500, 1000, 2000};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /* Read rows from single partition with size limit. */
        query = "select * from scanTable where sid = 5";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numPerMajor /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = numPerMajor;
        maxReadKBs = new int[] {0, 50, 100, 250};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /* Read rows from all shards with size limit. */
        query = "select * from scanTable where name = \"name_1\"";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numMajor /* numReadRows */,
                                 numMajor /* numReadKeys */);
        expCnt = numMajor;
        maxReadKBs = new int[] {0, 5, 10, 25};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /*
         * Number-based and size-based limit
         */

        /* Read rows from all partitions with number and size limit. */
        query = "select * from scanTable";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numRows;
        executeQuery(query, false /* keyOnly */, false/* indexScan */, expCnt,
                     expReadKB, 50 /* numLimit */, 100 /* sizeLimit */,
                     recordKB);

        /* Read rows from single partition with number and size limit. */
        query = "select * from scanTable where sid = 5";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numPerMajor /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = numPerMajor;
        executeQuery(query, false /* keyOnly */, false/* indexScan */, expCnt,
                     expReadKB, 10 /* numLimit */, 20 /* sizeLimit */, recordKB);

        /* Read rows from all shards with number and size limit. */
        query = "select * from scanTable where name = \"name_1\"";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numMajor /* numReadRows */,
                                 numMajor /* numReadKeys */);
        expCnt = numMajor;
        executeQuery(query, false /* keyOnly */, true/* indexScan */,
                     expCnt, expReadKB, 5 /* numLimit */, 10 /* sizeLimit */,
                     recordKB);
    }

    @Test
    public void testDupElim() {
        final int numMajor = 10;
        final int numPerMajor = 40;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        tableOperation(handle, createIdxArrayDDL, null,
                       TableResult.State.ACTIVE, 10000);

        String query =
            "select sid, id, t.array[size($)-2:] " +
            "from scanTable t " +
            "where t.array[] >any 11";

        /* Prepare first, then execute */
        executeQuery(query, null, 200, 20, true);
    }

    @Test
    public void testOrderByPartitions() {
        final int numMajor = 5;
        final int numPerMajor = 10;
        final int numRows = numMajor * numPerMajor;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        tableOperation(handle, createIdxStateAgeDDL, null,
                       TableResult.State.ACTIVE, 20000);

        String query;
        int expReadKB, expCnt;
        int[] maxReadKBs;

        //traceLevel = 3;
        //showResults = true;

        /*
         * Case 1: partial key
         */
        query = "select sid, id, name, state " +
                "from scanTable " +
                "order by sid ";

        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numRows;
        maxReadKBs = new int[] {0, 4, 25, 37, 66};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, false/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL);
        }

        /*
         * Case 2: partial key offset limit
         */
       query = "select sid, id, name, state " +
                "from scanTable " +
                "order by sid " +
                "limit 10 offset 4";

        expCnt = 10;
        maxReadKBs = new int[] {0, 5, 6, 7, 8, 9, 20, 44, 81};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, false/* indexScan */,
                        expCnt, -1 /*expReadKB*/, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL);
        }

        traceLevel = 0;
        showResults = false;

        /*
         * Case 3: partial key offset limit
         */
       query = "select sid, id, name, state " +
                "from scanTable " +
                "order by sid " +
                "limit 5 offset 44";

        expCnt = 5;
        maxReadKBs = new int[] {0, 5, 14, 51, 88};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, false/* indexScan */,
                        expCnt, -1 /*expReadKB*/, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL);
        }
    }

    @Test
    public void testGroupByPartitions() {

        final int numMajor = 5;
        final int numPerMajor = 10;
        final int numRows = numMajor * numPerMajor;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        tableOperation(handle, createIdxStateAgeDDL, null,
                       TableResult.State.ACTIVE, 20000);

        String query;
        int expReadKB, expCnt;
        int[] maxReadKBs;

        //traceLevel = 3;
        //showResults = true;

        /*
         * Case 1
         */
        query = "select sid, count(*) as cnt, sum(salary) as sum " +
                "from scanTable " +
                "group by sid";

        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = 5;
        //maxReadKBs = new int[] {0, 4, 25, 37, 66};
        maxReadKBs = new int[] {0};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, false/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL);
        }
    }

    @Test
    public void testOrderByShards() {

        final int numMajor = 10;
        final int numPerMajor = 40;
        final int recordKB = 2;
        final int numRows = numMajor * numPerMajor;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        tableOperation(handle, createIdxStateAgeDDL, null,
                       TableResult.State.ACTIVE, 20000);

        String query;
        int expReadKB, expCnt;
        int[] maxReadKBs;

        /*
         * Case 1: multi-shard, covering index
         */
        query = "select sid, id, state " +
                "from scanTable " +
                "order by state";
        /*
         * TODO: NOSQL-719
         * Enable the cost check in cloud test after fix it
         *
         * The cases after this one may run into the same problem, since
         * the case1 is executed multiple times, it is likely the table in
         * KV table cache on all the proxies may be refreshed, so just disable
         * the cost check in this one.
         */
        if (useCloudService) {
            expReadKB = -1;
        } else {
            expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                     0 /* numReadRows */,
                                     numRows /* numReadKeys */);
        }
        expCnt = numRows;
        maxReadKBs = new int[] {0, 5, 7, 11};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, true /* keyOnly */, true/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL);
        }

        /*
         * Case 2: multi-shard, non-covering index
         */
        query = "select sid, id, state, salary " +
                "from scanTable " +
                "order by state";

        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numRows;
        maxReadKBs = new int[] {6, 7, 8};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, true/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL);
        }

        /*
         * Case 3: single-partition, non-covering index
         */
        query = "select sid, id, state, salary " +
                "from scanTable " +
                "where sid = 3 " +
                "order by sid, id " +
                "limit 27 offset 5";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 32 /* numReadRows */,
                                 32 /* numReadKeys */);
        expCnt = 27;
        maxReadKBs = new int[] {4, 5, 12};
        for (int maxReadKB : maxReadKBs) {
           executeQuery(query, false /* keyOnly */, true/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB, Consistency.EVENTUAL);
        }
    }

    @Test
    public void testGroupByShards() {
        final int numMajor = 10;
        final int numPerMajor = 101;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        String query;
        int expReadKB, expCnt;
        int[] maxReadKBs;

        tableOperation(handle, createIdxStateAgeDDL, null,
                       TableResult.State.ACTIVE, 20000);
        /*
         * Case 1.
         */
        query = "select count(*) from scanTable where state = \"CA\"";

        /*
         * TODO: NOSQL-719
         * Enable the cost check in cloud test after fix it
         *
         * The cases after this one may run into the same problem, since
         * the case1 is executed multiple times, it is likely the table in
         * KV table cache on all the proxies may be refreshed, so just disable
         * the cost check in this one.
         */
        if (useCloudService) {
            expReadKB = -1;
        } else {
            expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                     0 /* numReadRows */,
                                     210);
        }
        expCnt = 1;
        /* size-based limit */
        maxReadKBs = new int[] {10, 17, 23, 37, 209, 210, 500};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /*
         * Case 2.
         * sum(salary) = 165000
         */
        query = "select count(*), sum(salary) from scanTable " +
                "where state = \"VT\"";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 200 /* numReadRows */,
                                 200 /* numReadKeys */);
        expCnt = 1;
        /* size-based limit */
        maxReadKBs = new int[] {9, 19, 31, 44, 200, 500};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /* Prepare first, then execute */
        executeQuery(query, null, 1, 22, true);

        /*
         * Case 3.
         */
        query = "select state, count(*) from scanTable group by state";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 1010);
        expCnt = 5;
        /* size-based limit */
        maxReadKBs = new int[] {30};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /*
         * Case 4.
         */
        query =
            "select state, "              +
            "       count(*) as cnt, "    +
            "       sum(salary) as sum, " +
            "       avg(salary) as avg "  +
            "from scanTable "+
            "group by state";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 1010 /* numReadRows */,
                                 1010);
        expCnt = 5;
        /* size-based limit */
        maxReadKBs = new int[] {34};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

    }

    /**
     * Test group-by query with numeric-base limit and/or size-based limits
     *
     *  1. Single partition scan, key-only
     *      select count(*) from scanTable where sid = 1
     *
     *  2. Single partition scan, key + row
     *      select min(name), min(age) from scanTable where sid = 1
     *
     *  3. All partitions scan, key only
     *      select count(*) from scanTable group by sid
     *
     *  4. All partitions scan, key + row
     *      select min(name) from scanTable group by sid
     *
     *  5. All shards scan, key only
     *      select count(*) from scanTable group by sid, name
     *
     *  6. All shards scan, key + row
     *      select max(name) from scanTable group by sid, name
     *
     *  7. All partitions scan, key only, single row returned.
     *      select count(*) from scanTable
     *
     *  8. All shards scan, key only, single row returned.
     *      select min(name) from scanTable
     */
    @Test
    public void testGroupByWithLimits() {
        final int numMajor = 10;
        final int numPerMajor = 101;
        final int numRows = numMajor * numPerMajor;
        final int recordKB = 2;

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        String query;
        int expReadKB, expCnt;
        int[] limits, maxReadKBs;

        tableOperation(handle, createIdxSidAgeDDL, null,
                       TableResult.State.ACTIVE, timeout);

long startMs = System.currentTimeMillis();

        /*
         * Case: Single partition scan, key only
         */
        query = "select count(*) from scanTable where sid = 1";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = 1;
        /* number-based limit */
        limits = new int[] {0, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 50, 100, 101};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based and size-based limit */
        executeQuery(query, false /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 50 /* maxReadKB */,
                     recordKB);

        /*
         * Case 2: Single partition scan, key + row
         */
        query = "select min(salary), min(age) from scanTable where sid = 1";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numPerMajor /* numReadRows */,
                                 numPerMajor /* numReadKeys */);
        expCnt = 1;
        /* number-based limit */
        limits = new int[] {0, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 300, 303};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, false /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 200 /* maxReadKB */,
                     recordKB);

        /*
         * Case 3: All partitions scan, key only
         */
        query = "select count(*) from scanTable group by sid";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numMajor;
        /* number-based limit */
        limits = new int[] {0, 5, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 1010};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, true /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 200 /* maxReadKB */,
                     recordKB);
        executeQuery(query, true /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 2 /* numLimit */, 200 /* maxReadKB */,
                     recordKB);
        executeQuery(query, true /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 5 /* numLimit */, 200 /* maxReadKB */,
                     recordKB);

        /*
         * Case 4: All partitions scan, key + row
         */
        query = "select min(salary) from scanTable group by sid";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numMajor;
        /* number-based limit */
        limits = new int[] {0, 5, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 2047};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, false/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, false /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 400 /* maxReadKB */,
                     recordKB);
        executeQuery(query, false /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 3 /* numLimit */, 400 /* maxReadKB */,
                     recordKB);

        /*
         * Case 5: All shards can, key only
         */
        query = "select count(*) from scanTable group by sid, age";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numMajor * 10;

        /* number-based limit */
        limits = new int[] {0, 5, 50, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 1010};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        /* number-based and size-based limit */
        executeQuery(query, true /* keyOnly */, true/* indexScan */,
                     expCnt, expReadKB, 10 /* numLimit */, 100, recordKB);

        /*
         * Case 6: All shards can, key + row
         */
        query = "select max(salary) from scanTable group by sid, age";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = numMajor * 10;

        /* number-based limit */
        limits = new int[] {0, 5, 50, expCnt, expCnt + 1};
        for (int limit : limits) {
            executeQuery(query, false /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }

        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 2047};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }

        executeQuery(query, false /* keyOnly */, true/* indexScan */,
                     expCnt, expReadKB, 10 /* numLimit */, 300 /* maxReadKB */,
                     recordKB);

        /*
         * Case 7: All partitions scan, key only. Single row returned.
         */
        query = "select count(*) from scanTable";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = 1;
        /* number-based limits */
        limits = new int[] {0, 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, false /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 1010 };
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, false/* indexScan */,
                        expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                        recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, true /* keyOnly */, false/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 500 /* maxReadKB */,
                     recordKB);

        /*
         * Case 8: All shards scan, key only. Single row returned.
         */
        query = "select min(name) from scanTable";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 numRows /* numReadKeys */);
        expCnt = 1;
        /* number-based limits */
        limits = new int[] {0, 1};
        for (int limit : limits) {
            executeQuery(query, true /* keyOnly */, true /* indexScan */,
                         expCnt, expReadKB, limit, 0 /* maxReadKB */,
                         recordKB);
        }
        /* size-based limit */
        maxReadKBs = new int[] {0, 10, 100, 500, 1000, 1010 };
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB);
        }
        /* number-based limit + size-based limit */
        executeQuery(query, true /* keyOnly */, true/* indexScan */,
                     expCnt, expReadKB, 1 /* numLimit */, 500 /* maxReadKB */,
                     recordKB);
if (verbose) System.out.println("WithLimits took " + (System.currentTimeMillis() - startMs) + "ms");
    }

    @Test
    public void testDelete() {
        final int numMajor = 5;
        final int numPerMajor = 100;
        final int recordKB = 4;

        tableOperation(handle, createIdxStateAgeDDL, null,
                       TableResult.State.ACTIVE, 20000);

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        int expReadKB, expCnt;
        int[] maxReadKBs;
        String query;

        /*
         * Case 1. ALL_SHARDS delete, without RETURNING, covering index
         * 100 rows will be deleted. 200 key-reads will be performed
         */
        query = "delete from scanTable where state = \"CA\"";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 200/*numReadKeys*/);

        expCnt = 1;
        maxReadKBs = new int[] {10};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB, Consistency.ABSOLUTE);
        }

        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        /*
         * Case 2. ALL_SHARDS delete, with RETURNING, covering index
         * 100 rows will be deleted. 200 key-reads will be performed
         */
        query = "delete from scanTable where state = \"CA\" returning id";
        expReadKB = getExpReadKB(true /* keyOnly */, recordKB,
                                 0 /* numReadRows */,
                                 200/*numReadKeys*/);
        expCnt = 100;
        maxReadKBs = new int[] {10};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, true /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB, Consistency.ABSOLUTE);
        }

        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        /*
         * Case 3 ALL_SHARDS delete, with RETURNING, non-covering index
         * 100 rows will be deleted. 200 key-reads will be performed
         */
        query = "delete from scanTable where state = \"CA\" " +
                "returning sid, id, name";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 100 /* numReadRows */,
                                 200/*numReadKeys*/);
        expCnt = 100;
        maxReadKBs = new int[] {10};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB, Consistency.ABSOLUTE);
        }

        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        /*
         * Case 4. ALL_SHARDS delete, without RETURNING, non-covering index
         * 100 rows will be deleted. 200 key-reads will be performed
         */
        query = "delete from scanTable where state = \"CA\" and name != \"abc\"";
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 100 /* numReadRows */,
                                 200/*numReadKeys*/);
        expCnt = 1;
        maxReadKBs = new int[] {13};
        for (int maxReadKB : maxReadKBs) {
            executeQuery(query, false /* keyOnly */, true/* indexScan */,
                         expCnt, expReadKB, 0 /* numLimit */, maxReadKB,
                         recordKB, Consistency.ABSOLUTE);
        }
    }

    @Test
    public void testInsert() {
        final int numMajor = 1;
        final int numPerMajor = 10;
        final int recordKB = 2;
        final int prepCost = getMinQueryCost(); // = 2

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        QueryRequest req;
        QueryResult ret;

        /* Insert a new row */
        int newRecordKB = 8;
        String longString = genString(newRecordKB * 1024);
        String query =
            "insert into scanTable values " +
            "(1, 15, \"myname\", 23, \"WI\", 2500, [], \"" +
            longString + "\")";

        req = new QueryRequest().setStatement(query);
        ret = getNextHandle().query(req);

        assertQueryReadKB(ret, 1, prepCost, true /* isAbsolute */);
        assertWriteKB(ret, newRecordKB + 2);
        assertTrue(ret.getResults().size() == 1);

        query = "select sid, id, name from scanTable where id = 15";
        req = new QueryRequest().setStatement(query);
        ret = getNextHandle().query(req);
        assertTrue(ret.getResults().size() == 1);
        MapValue res = ret.getResults().get(0);
        FieldValue name = res.get("name");
        assertTrue(name.getString().equals("myname"));
        //System.out.println("Result = " + res);
    }

    @Test
    public void testUpdate() {
        final int numMajor = 1;
        final int numPerMajor = 10;
        final int recordKB = 2;
        final int minRead = getMinRead();
        final int prepCost = getMinQueryCost();

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        QueryRequest req;
        QueryResult ret;
        int expReadKB;

        /* Update a row */
        int newRecordKB = 1;
        String longString = genString((newRecordKB - 1) * 1024);
        String query = "update scanTable set longString = \"" + longString +
                       "\" where sid = 0 and id = 0";
        req = new QueryRequest().setStatement(query);
        ret = getNextHandle().query(req);
		expReadKB = dontDoubleChargeKey() ? recordKB : (minRead + recordKB);
        assertQueryReadKB(ret, expReadKB, prepCost, true /* isAbsolute */);
        assertWriteKB(ret, (recordKB + newRecordKB));

        /* Update non-existing row */
        query = "update scanTable set longString = \"test\" " +
                "where sid = 100 and id = 0";
        req = new QueryRequest().setStatement(query);
        ret = getNextHandle().query(req);
        assertQueryReadKB(ret, minRead, prepCost, true /* isAbsolute */);
        assertWriteKB(ret, 0);

        /* Update using preparedStatement */
        query = "declare $sval string; $sid integer; $id integer;" +
                "update scanTable set longString = $sval " +
                "where sid = $sid and id = $id";
        PrepareRequest prepReq = new PrepareRequest()
            .setStatement(query).setGetQuerySchema(true);
        PrepareResult prepRet = getNextHandle().prepare(prepReq);
        if (!testV3) {
            assertNotNull(prepRet.getPreparedStatement().getQuerySchema());
        }
        assertNull(prepRet.getPreparedStatement().getQueryPlan());
        assertNotNull(prepRet.getPreparedStatement());
        assertReadKB(prepRet, prepCost, false /* isAbsolute */);
        assertWriteKB(prepRet, 0);

        prepRet.getPreparedStatement()
            .setVariable("$sval", new StringValue(longString))
            .setVariable("$sid", new IntegerValue(0))
            .setVariable("$id", new IntegerValue(1));

        req = new QueryRequest().setPreparedStatement(prepRet);
        ret = getNextHandle().query(req);
        expReadKB = dontDoubleChargeKey() ? recordKB : (minRead + recordKB);
        assertReadKB(ret, expReadKB, true /* isAbsolute */);
        assertWriteKB(ret, (recordKB + newRecordKB));
    }

    @Test
    public void testQueryWithSmallLimit() {
        final int numMajor = 1;
        final int numPerMajor = 5;
        final int recordKB = 2;
        final int minRead = getMinRead();
        final int prepCost = getMinQueryCost();

        /* Load rows to table */
        loadRowsToScanTable(numMajor, numPerMajor, recordKB);

        String query;
        QueryRequest req;
        QueryResult ret;
        int expReadKB;

        /* Update with number-based limit of 1 */
        int newRecordKB = 1;
        String longString = genString((newRecordKB - 1) * 1024);
        query = "update scanTable set longString = \"" + longString +
                "\" where sid = 0 and id = 0";
        req = new QueryRequest().setStatement(query).setLimit(1);
        ret = getNextHandle().query(req);
        assertNull(ret.getContinuationKey());
        expReadKB = dontDoubleChargeKey() ? recordKB : (minRead + recordKB);
        assertQueryReadKB(ret, expReadKB, prepCost, true /* isAbsolute */);
        assertWriteKB(ret, (recordKB + newRecordKB));

        /* Update with maxReadKB of 1, expect an IAE */
        expReadKB = dontDoubleChargeKey() ? recordKB : (minRead + recordKB);
        query = "update scanTable set longString = \"" + longString +
            "\" where sid = 0 and id = 1";

        if (checkKVVersion(21, 3, 6)) {
            /* Query should always make progress with small limit */
            req = new QueryRequest().setStatement(query).setMaxReadKB(1);
            ret = getNextHandle().query(req);
            assertQueryReadKB(ret, expReadKB, prepCost, true /* isAbsolute */);
            assertWriteKB(ret, (recordKB + newRecordKB));
        } else {
            for (int kb = 1; kb <= expReadKB; kb++) {
                req = new QueryRequest().setStatement(query).setMaxReadKB(kb);
                try {
                    ret = getNextHandle().query(req);
                    if (kb < expReadKB) {
                        fail("Expect to catch IAE but not");
                    } else {
                        assertQueryReadKB(ret, expReadKB, prepCost,
                                          true /* isAbsolute */);
                        assertWriteKB(ret, (recordKB + newRecordKB));
                    }
                } catch (IllegalArgumentException iae) {
                    assertTrue("Expect to succeed with maxReadKB of " + kb +
                               ", but fail: " + iae.getMessage(),
                               kb < expReadKB);
                }
            }
        }

        /* Update with maxReadKB of 1, 0 row updated */
        query = "update scanTable set longString = \"" + longString +
            "\" where sid = 100 and id = 1";
        req = new QueryRequest().setStatement(query).setMaxReadKB(1);
        ret = getNextHandle().query(req);
        assertNull(ret.getContinuationKey());
        assertQueryReadKB(ret, minRead, prepCost, true /* isAbsolute */);
        assertWriteKB(ret, 0);

        /* Query with number limit of 1 */
        query = "select * from scanTable where sid = 0 and id > 1";
        int numRows = numMajor * (numPerMajor - 2);
        expReadKB = getExpReadKB(false /* keyOnly */, recordKB,
                                 numRows /* numReadRows */,
                                 numRows /* numReadKeys */);
        executeQuery(query, false /* keyOnly */, false/* indexScan */,
                     numRows, expReadKB, 1 /* limit */, 0 /* maxReadKB */,
                     recordKB);

        //traceLevel = 3;
        //showResults = true;

        /* Query with maxReadKB of 1, expect an IAE */
        NoSQLHandle curHandle = getNextHandle();
        query = "select * from scanTable where sid = 0 and id > 1";
        if (checkKVVersion(21, 3, 6)) {
            for (int kb = 1; kb <= 3; kb++) {
                executeQuery(query, false, false, numRows, expReadKB, 0,
                             kb, recordKB);
            }
        } else {
            int numExec = 0;
            req = new QueryRequest().setStatement(query).setMaxReadKB(1);
            try {
                do {
                    numExec++;
                    ret = handle.query(req);
                } while (!req.isDone());
                fail("Expect to catch IAE but not");
            } catch (IllegalArgumentException iae) {
                assertEquals(2, numExec);
            }
        }
    }

    /**
     * Returns the estimated readKB.
     */
    private int getExpReadKB(boolean keyOnly,
                             int recordKB,
                             int numReadRows,
                             int numReadKeys) {
        final int minRead = getMinRead();
        int readKB = numReadKeys * minRead;
        if (!keyOnly) {
            if (dontDoubleChargeKey()) {
                recordKB -= minRead;
            }
            readKB += numReadRows * recordKB;
        }
        return readKB == 0 ? minRead : readKB;
    }

    /*
     * Test illegal cases -- both prepared statement and string
     */
    @Test
    public void testIllegalQuery() {

        PrepareRequest prepReq;
        QueryRequest queryReq;
        String query;

        final String queryWithVariables =
            "declare $sid integer; $id integer;" +
            "select name from scanTable where sid = $sid and id >= $id";

        /* Syntax error */
        prepReq = new PrepareRequest().setStatement("random string");
        try {
            getNextHandle().prepare(prepReq);
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {}

        queryReq = new QueryRequest().setStatement("random string");
        try {
            getNextHandle().query(queryReq);
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {}

        /* Try a query that requires external variables that are missing */
        queryReq = new QueryRequest().setStatement(queryWithVariables);
        try {
            getNextHandle().query(queryReq);
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {
        }

        prepReq = new PrepareRequest().setStatement(queryWithVariables).
            setGetQueryPlan(true);
        PrepareResult prepRes = getNextHandle().prepare(prepReq);
        queryReq = new QueryRequest().setPreparedStatement(prepRes);
        try {
            getNextHandle().query(queryReq);
            fail("query should have failed");
        } catch (IllegalArgumentException iae)  {
        }

        /* Wrong name of variables */
        prepReq = new PrepareRequest().setStatement(queryWithVariables);
        prepRes = getNextHandle().prepare(prepReq);
        PreparedStatement prepStmt = prepRes.getPreparedStatement();
        prepStmt.setVariable("sid", new IntegerValue(9));
        prepStmt.setVariable("id", new IntegerValue(3));
        queryReq = new QueryRequest().setPreparedStatement(prepRes);
        try {
            getNextHandle().query(queryReq);
            fail("query should have failed");
        } catch (IllegalArgumentException ex) {
        }

        /* Wrong type for variables */
        prepReq = new PrepareRequest().setStatement(queryWithVariables);
        prepRes = getNextHandle().prepare(prepReq);
        prepStmt = prepRes.getPreparedStatement();
        prepStmt.setVariable("$sid", new DoubleValue(9.1d));
        prepStmt.setVariable("$id", new IntegerValue(3));
        queryReq = new QueryRequest().setPreparedStatement(prepRes);
        try {
            getNextHandle().query(queryReq);
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {
        }

        /* Table not found */
        query = "select * from invalidTable";
        prepReq = new PrepareRequest().setStatement(query);
        try {
            getNextHandle().prepare(prepReq);
            fail("prepare should have failed");
        } catch (TableNotFoundException tnfe) {
        }

        queryReq = new QueryRequest().setStatement(query);
        try {
            getNextHandle().query(queryReq);
            fail("query should have failed");
        } catch (TableNotFoundException tnfe) {
        }

        /* Invalid column */
        query = "select * from scanTable where invalidColumn = 1";
        prepReq = new PrepareRequest().setStatement(query);
        try {
            getNextHandle().prepare(prepReq);
            fail("prepare should have failed");
        } catch (IllegalArgumentException iae) {
        }

        queryReq = new QueryRequest().setStatement(query);
        try {
            getNextHandle().query(queryReq);
            fail("query should have failed");
        } catch (IllegalArgumentException tnfe) {
        }

        /* Prepare or execute Ddl statement */
        query = "create table t1(id integer, name string, primary key(id))";
        prepReq = new PrepareRequest().setStatement(query);
        try {
            getNextHandle().prepare(prepReq);
            fail("prepare should have failed");
        } catch (IllegalArgumentException iae) {
        }

        queryReq = new QueryRequest().setStatement(query);
        try {
            getNextHandle().query(queryReq);
            fail("query should have failed");
        } catch (IllegalArgumentException iae) {
        }

        queryReq = new QueryRequest().setStatement(query);
        try {
            queryReq.setLimit(-1);
            getNextHandle().query(queryReq);
            fail("QueryRequest.setLimit() should fail with IAE");
        } catch (IllegalArgumentException iae) {
        }
        queryReq.setLimit(0);

        try {
            queryReq.setMaxReadKB(-1);
            fail("QueryRequest.setMaxReadKB() should fail with IAE");
        } catch (IllegalArgumentException iae) {
        }

        if (!cloudRunning) { // Compartment path is support in cloud
            /*
             * Namespaces, child tables and identity columns are not
             * yet supported
             */
            String statement =
                "create table ns:foo(id integer, primary key(id))";
            try {
                tableOperation(handle, statement,
                               new TableLimits(10, 10, 10),
                               TableResult.State.ACTIVE, 10000);
                fail("Namespaces not supported in table names");
            } catch (Exception e) {
                assertTrue(e.getMessage().toLowerCase().contains("namespace"));
            }

            statement = "drop table ns:foo";
            try {
                tableOperation(handle, statement,
                               new TableLimits(10, 10, 10),
                               TableResult.State.ACTIVE, 10000);
                fail("Namespaces not supported in table names");
            } catch (Exception e) {
                if (onprem) {
                    assertTrue(e instanceof TableNotFoundException);
                } else {
                    assertTrue(e.getMessage().toLowerCase()
                               .contains("namespace"));
                }
            }

            statement = "select * from ns:foo";
            try {
                executeQuery(statement, null, 0, 0, false);
                fail("Query with namespaced table not supported");
            } catch (Exception e) {
                if (onprem) {
                    assertTrue(e instanceof TableNotFoundException);
                } else {
                    assertTrue(e.getMessage().toLowerCase()
                               .contains("namespace"));
                }
            }
        }

        String statement = "create namespace myns";
        try {
            tableOperation(handle, statement,
                           new TableLimits(10, 10, 10),
                           TableResult.State.ACTIVE, 10000);
            if (!onprem) {
                fail("Creating namespaces not supported");
            }
        } catch (Exception e) {
            assertTrue(e.getMessage().toLowerCase().contains("namespace"));
        }

        statement = "drop namespace myns";
        try {
            tableOperation(handle, statement,
                           new TableLimits(10, 10, 10),
                           TableResult.State.ACTIVE, 10000);
            if (!onprem) {
                fail("Dropping namespaces not supported");
            }
        } catch (Exception e) {
            assertTrue(e.getMessage().toLowerCase().contains("namespace"));
        }
    }

    @Test
    public void testJson() {
        final String[] jsonRecords = {
            "{" +
            " \"id\":0," +
            " \"info\":" +
            "  {" +
            "    \"firstName\":\"first0\", \"lastName\":\"last0\",\"age\":10," +
            "    \"address\":" +
            "    {" +
            "      \"city\": \"San Fransisco\"," +
            "      \"state\"  : \"CA\"," +
            "      \"phones\" : [" +
            "                     { \"areacode\" : 408, \"number\" : 50," +
            "                       \"kind\" : \"home\" }," +
            "                     { \"areacode\" : 650, \"number\" : 51," +
            "                       \"kind\" : \"work\" }," +
            "                     \"650-234-4556\"," +
            "                     650234455" +
            "                   ]" +
            "    }," +
            "    \"children\":" +
            "    {" +
            "      \"Anna\" : { \"age\" : 10, \"school\" : \"sch_1\"," +
            "               \"friends\" : [\"Anna\", \"John\", \"Maria\"]}," +
            "      \"Lisa\" : { \"age\" : 12, \"friends\" : [\"Ada\"]}" +
            "    }" +
            "  }" +
            "}",

            "{" +
            "  \"id\":1," +
            "  \"info\":" +
            "  {" +
            "    \"firstName\":\"first1\", \"lastName\":\"last1\",\"age\":11," +
            "    \"address\":" +
            "    {" +
            "      \"city\"   : \"Boston\"," +
            "      \"state\"  : \"MA\"," +
            "      \"phones\" : [ { \"areacode\" : 304, \"number\" : 30," +
            "                       \"kind\" : \"work\" }," +
            "                     { \"areacode\" : 318, \"number\" : 31," +
            "                       \"kind\" : \"work\" }," +
            "                     { \"areacode\" : 400, \"number\" : 41," +
            "                       \"kind\" : \"home\" }]" +
            "    }," +
            "    \"children\":" +
            "    {" +
            "      \"Anna\" : { \"age\" : 9,  \"school\" : \"sch_1\"," +
            "                   \"friends\" : [\"Bobby\", \"John\", null]}," +
            "      \"Mark\" : { \"age\" : 4,  \"school\" : \"sch_1\"," +
            "                   \"friends\" : [\"George\"]}," +
            "      \"Dave\" : { \"age\" : 15, \"school\" : \"sch_3\"," +
            "                   \"friends\" : [\"Bill\", \"Sam\"]}" +
            "    }" +
            "  }" +
            "}",

            "{" +
            "  \"id\":2," +
            "  \"info\":" +
            "  {" +
            "    \"firstName\":\"first2\", \"lastName\":\"last2\",\"age\":12," +
            "    \"address\":" +
            "    {" +
            "      \"city\"   : \"Portland\"," +
            "      \"state\"  : \"OR\"," +
            "      \"phones\" : [ { \"areacode\" : 104, \"number\" : 10," +
            "                       \"kind\" : \"home\" }," +
            "                     { \"areacode\" : 118, \"number\" : 11," +
            "                       \"kind\" : \"work\" } ]" +
            "    }," +
            "    \"children\":" +
            "    {" +
            "    }" +
            "  }" +
            "}",

            "{ " +
            "  \"id\":3," +
            "  \"info\":" +
            "  {" +
            "    \"firstName\":\"first3\", \"lastName\":\"last3\",\"age\":13," +
            "    \"address\":" +
            "    {" +
            "      \"city\"   : \"Seattle\"," +
            "      \"state\"  : \"WA\"," +
            "      \"phones\" : null" +
            "    }," +
            "    \"children\":" +
            "    {" +
            "      \"George\" : { \"age\" : 7,  \"school\" : \"sch_2\"," +
            "                     \"friends\" : [\"Bill\", \"Mark\"]}," +
            "      \"Matt\" :   { \"age\" : 14, \"school\" : \"sch_2\"," +
            "                     \"friends\" : [\"Bill\"]}" +
            "    }" +
            "  }" +
            "}"
        };

        String query;
        Map<String, FieldValue> bindValues = new HashMap<String, FieldValue>();

        tableOperation(handle, createJsonTableDDL,
                       new TableLimits(15000, 15000, 50),
                       TableResult.State.ACTIVE, timeout);

        /* create an index on a field that won't exist to test GetIndexes */
        tableOperation(handle, "create index JsonIndex on jsonTable " +
                       "(info.a.b.c as integer, info.d as string)", null,
                       TableResult.State.ACTIVE, 20000);

        /* Simple test of GetIndexesRequest */
        GetIndexesRequest getIndexes = new GetIndexesRequest()
            .setTableName("jsonTable");

        /* GetIndexesRquest returns GetIndexesResult */
        GetIndexesResult giRes = handle.getIndexes(getIndexes);
        /* there is 1 index with 2 fields, each with a type */
        assertEquals(1, giRes.getIndexes().length);
        for (GetIndexesResult.IndexInfo info : giRes.getIndexes()) {
            assertEquals(2, info.getFieldNames().length);
            for (int i = 0; i < info.getFieldNames().length; i++) {
                assertNotNull(info.getFieldNames()[i]);
                if (!testV3) {
                    assertNotNull(info.getFieldTypes()[i]);
                }
            }
        }

        loadRowsToTable(jsonTable, jsonRecords);

        /* Basic query on a table with JSON field */
        query = "select id, f.info from jsonTable f";
        executeQuery(query, null, 4, 0, false /* usePrepStmt */);

        /* Test JsonNull */
        query = "select id from jsonTable f where f.info.address.phones = null";
        executeQuery(query, null, 1, 0, false /* usePrepStmt */);

        /* Bind JsonNull value */
        query = "declare $phones json;" +
            "select id, f.info.address.phones " +
            "from jsonTable f " +
            "where f.info.address.phones != $phones";
        bindValues.put("$phones", JsonNullValue.getInstance());
        executeQuery(query, bindValues, 3, 0, true /* usePrepStmt */);

        /* Bind 2 String values */
        query = "declare $city string;$name string;" +
            "select id, f.info.address.city, f.info.children.keys() " +
            "from jsonTable f " +
            "where f.info.address.city = $city and " +
            "      not f.info.children.keys() =any $name";
        bindValues.clear();
        bindValues.put("$city", new StringValue("Portland"));
        bindValues.put("$name", new StringValue("John"));
        executeQuery(query, bindValues, 1, 0, true /* usePrepStmt */);

        /* Bind MapValue */
        query = "declare $child json;" +
                "select id, f.info.children.values() " +
                "from jsonTable f " +
                "where f.info.children.values() =any $child";
        String json = "{\"age\":14, \"school\":\"sch_2\", " +
                      " \"friends\":[\"Bill\"]}";
        bindValues.clear();
        bindValues.put("$child", JsonUtils.createValueFromJson(json, null));
        executeQuery(query, bindValues, 1, 0, true /* usePrepStmt */);

        /* Bind ArrayValue */
        query = "declare $friends json;" +
            "select id, f.info.children.values() " +
            "from jsonTable f " +
            "where f.info.children.values().friends =any $friends";

        ArrayValue friends = new ArrayValue();
        friends.add("Bill");
        friends.add("Mark");
        bindValues.clear();
        bindValues.put("$friends", friends);
        executeQuery(query, bindValues, 1, 0, true /* usePrepStmt */);
    }

    @Test
    public void testPrepare() {
        String query;
        PrepareRequest req;
        PrepareResult ret;

        query = "select * from scanTable";
        req = new PrepareRequest().setStatement(query).setGetQueryPlan(true);
        ret = getNextHandle().prepare(req);
        assertNull(ret.getPreparedStatement().getQuerySchema());
        assertNotNull(ret.getPreparedStatement().getQueryPlan());

        if (!onprem) {
            assertEquals(ret.getReadKB(), getMinQueryCost());
            assertEquals(ret.getWriteKB(), 0);
        }

        query = "declare $sval string; $sid integer; $id integer;" +
                "update scanTable set longString = $sval " +
                "where sid = $sid and id = $id";
        req = new PrepareRequest().setStatement(query);
        ret = getNextHandle().prepare(req);
        if (!onprem) {
            assertEquals(ret.getReadKB(), getMinQueryCost());
            assertEquals(ret.getWriteKB(), 0);
        }
    }

    @Test
    public void testOldPreparedPlan() {

        Class<?> prepstmtClass = null;

        try {
            prepstmtClass = Class.forName(
                "oracle.kv.impl.api.query.PreparedStatementImpl");
        } catch (Throwable e) {
            System.out.println("Could not find PreparedStatementImpl class:" +
                               e);
            prepstmtClass = null;
        }

        assertTrue(prepstmtClass != null);

        Method setVersionFunction = null;

        try {
            setVersionFunction = prepstmtClass.getMethod("setTestSerialVersion",
                                                         short.class);
        } catch (Throwable e) {
            System.out.println(
                "Warning: Could not find PreparedStatementImpl.setTestSerialVersion() " +
                "function: skipping testOldPreparedPlan()");
            return;
        }

        String tableDDL =
            "create table if not exists users(" +
            "    acct_id integer,"      +
            "    user_id integer,"      +
            "    info json,"           +
            "    primary key(acct_id, user_id))";

        String queries[] = {

            "select count(*) as cnt " +
            "from users u " +
            "where u.info.country = \"USA\" and " +
            "      u.info.shows.showId =any 16",
        };

        tableOperation(handle, tableDDL,
                       new TableLimits(15000, 15000, 50),
                       TableResult.State.ACTIVE, timeout);

        // serial version 21 is 19.3
        short[] versions = { 21, 22, 23, 24 };

        try {
            for (short v : versions) {

                try {
                    setVersionFunction.invoke(null, v);
                } catch (Throwable e) {
                    System.out.println(
                        "Failed to invoke " +
                        "PreparedStatementImpl.setTestSerialVersion() " +
                        "function: " + e);
                    return;
                }

                /* Prepare the queries */
                PreparedStatement[] prepStmts =
                    new PreparedStatement[queries.length];

                for (int i = 0; i < queries.length; ++i) {
                    PrepareRequest req = new PrepareRequest()
                                                 .setStatement(queries[i]);
                    PrepareResult res = handle.prepare(req);
                    prepStmts[i] = res.getPreparedStatement();
                }

                /* Execute the queries */
                for (int i = 0; i < queries.length; ++i) {

                    QueryRequest queryReq = new QueryRequest();
                    queryReq.setPreparedStatement(prepStmts[i]);
                    do {
                        QueryResult queryRes = handle.query(queryReq);
                        List<MapValue> results = queryRes.getResults();
                        assertTrue(results.size() == 1);
                    } while (!queryReq.isDone());
                }
            }
        } finally {
            try {
                short v = -1;
                setVersionFunction.invoke(null, v);
            } catch (Throwable e) {
                System.out.println(
                    "Failed to invoke " +
                    "PreparedStatementImpl.setTestSerialVersion() " +
                    "function: " + e);
                return;
            }
        }
    }

    /**
     * Prepare a query, use it, evolve table, try again.
     */
    @Test
    public void testEvolution() {
        /*
         * TODO: NOSQL-719
         * Enable this test in cloud test after fix it
         */
        if (useCloudService) {
            return;
        }

        /* Load rows to table */
        loadRowsToScanTable(1, 10, 2);
        String query = "select age from scanTable";
        PrepareRequest prepReq = new PrepareRequest().setStatement(query);
        PrepareResult prepRet = getNextHandle().prepare(prepReq);
        assertNotNull(prepRet.getPreparedStatement());

        QueryRequest qreq = new QueryRequest().setPreparedStatement(prepRet);
        QueryResult qres = getNextHandle().query(qreq);
        assertEquals(10, qres.getResults().size());

        /*
         * add a column and try the query again. It should fail with
         * a message including "prepared with a different version of the table"
         *
         * NOTE: in the future, this should "just work", when the proxy
         *       manages re-preparing queries automatically
         */
        tableOperation(handle, "alter table scanTable (add username string)",
                       null, null, TableResult.State.ACTIVE, null);
        try {
            qres = handle.query(qreq);
            fail("Query should have failed");
        } catch (PrepareQueryException iae) {
        }

        prepRet = handle.prepare(prepReq);
        assertNotNull(prepRet.getPreparedStatement());
        qreq.setPreparedStatement(prepRet);

        /*
         * remove a field and try the query again. It will fail because the
         * field referenced does not exist anymore.
         */
        tableOperation(handle, "alter table scanTable(drop age)",
                       null, null, TableResult.State.ACTIVE, null);
        try {
            qres = getNextHandle().query(qreq);
            fail("Query should have failed");
        } catch (PrepareQueryException iae) {
            /* success */
        }
    }

    @Test
    public void testIdentityAndUUID() {
        String idName = "testSG";
        String uuidName = "testUUID";
        String createTableId =
            "CREATE TABLE " + idName +
                "(id INTEGER GENERATED ALWAYS AS IDENTITY, " +
                 "name STRING, " +
                 "PRIMARY KEY(id))";
        String createTableUUID =
            "CREATE TABLE " + uuidName +
                "(id STRING AS UUID GENERATED BY DEFAULT, " +
                 "name STRING, " +
                 "PRIMARY KEY(id))";

        tableOperation(handle, createTableId, new TableLimits(100, 100, 1),
                       null, TableResult.State.ACTIVE, 10000);
        tableOperation(handle, createTableUUID, new TableLimits(100, 100, 1),
                       null, TableResult.State.ACTIVE, 10000);

        /*
         * Putting a row with a value for "id" should fail because always
         * generated identity column should not has value.
         */
        MapValue value = new MapValue().put("id", 100).put("name", "abc");
        PutRequest putReq = new PutRequest().setTableName(idName);
        try {
            putReq.setValue(value);
            getNextHandle().put(putReq);
            fail("Expected IAE; a generated always identity " +
                 "column should not have a value");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Putting a row without "id" field should succeed.
         */
        value = new MapValue().put("name", "abc");
        putReq.setValue(value);
        PutResult putRet = getNextHandle().put(putReq);
        assertNotNull(putRet.getVersion());
        assertNotNull(putRet.getGeneratedValue());

        /*
         * Now the UUID table
         */
        value = new MapValue().put("id", "abcde").put("name", "abc");
        putReq = new PutRequest().setTableName(uuidName);
        try {
            putReq.setValue(value);
            getNextHandle().put(putReq);
            fail("Expected IAE; the uuid value set was not a uuid");
        } catch (IllegalArgumentException iae) {
        }

        /*
         * Putting a row without "id" field should succeed.
         */
        value = new MapValue().put("name", "abc");
        putReq.setValue(value);
        putRet = getNextHandle().put(putReq);
        assertNotNull(putRet.getVersion());
        assertNotNull(putRet.getGeneratedValue());
    }

    @Test
    public void testQueryOrder() {

        final String[] declOrder = {
            "sid", "id", "name", "age", "state","salary", "array", "longString"
        };

        /* Load rows to table */
        loadRowsToScanTable(10, 10, 1);

        QueryRequest queryReq = new QueryRequest().
            setStatement("select * from scanTable where id = 1 and sid = 1");

        QueryResult queryRes = getNextHandle().query(queryReq);

        /*
         * For each result, assert that the fields are all there and in the
         * expected order.
         */
        for (MapValue v : queryRes.getResults()) {
            assertEquals(declOrder.length, v.size());
            int i = 0;
            for (Map.Entry<String, FieldValue> entry : v.entrySet()) {
                assertEquals(declOrder[i++], entry.getKey());
            }

            /* perform a get and validate that it also is in decl order */
            GetRequest getReq = new GetRequest()
                .setTableName(tableName)
                .setKey(v);
            GetResult getRes = getNextHandle().get(getReq);
            i = 0;
            for (Map.Entry<String, FieldValue> entry :
                     getRes.getValue().entrySet()) {
                assertEquals(declOrder[i++], entry.getKey());
            }
        }
    }

    @Test
    public void testLowThroughput() {
        final int numRows = 20;
        String name = "testThroughput";
        String createTableDdl =
            "CREATE TABLE " + name +
            "(id INTEGER, bin binary, json json, primary key(id))";

        tableOperation(handle, createTableDdl, new TableLimits(2, 20000, 1),
                       null, TableResult.State.ACTIVE, 10000);

        MapValue value = new MapValue()
            .put("bin", new byte[500])
            .put("json", "abc");
        PutRequest putReq = new PutRequest().setTableName(name);

        /* add rows */
        for (int i = 0; i < numRows; i++) {
            value.put("id", i);
            putReq.setValue(value);
            PutResult putRet = getNextHandle().put(putReq);
            assertNotNull(putRet.getVersion());
        }

        /*
         * Ensure that this query completes
         */
        QueryRequest queryReq = new QueryRequest().
            setStatement("select * from " + name).
            setMaxReadKB(2);
        int numRes = 0;
        long stime = System.currentTimeMillis();
        int RUs = 0;
        do {
            QueryResult queryRes = getNextHandle().query(queryReq);
            numRes += queryRes.getResults().size();
            RUs += queryRes.getReadUnits();
            verbose("  RUs=" + RUs);
        } while (!queryReq.isDone());
        if (verbose) {
            long diffMs = System.currentTimeMillis() - stime;
            System.out.println("Got " + RUs + " RUs in " + diffMs + "ms");
        }
        assertEquals(numRows, numRes);
    }

    /*
     * Tests that a query with a V2 sort (geo_near) can operate against
     * query versions 2 and 3
     */
    @Test
    public void testQueryCompat() {
        final String geoTable = "create table points (id integer, " +
            "info json, primary key(id))";
        final String geoIndex =
            "create index idx_ptn on points(info.point as point)";
        final String geoQuery =
            "select id from points p " +
            "where geo_near(p.info.point, " +
            "{ \"type\" : \"point\", \"coordinates\" : [24.0175, 35.5156 ]}," +
            "5000)";

        TableResult tres = tableOperation(handle, geoTable,
                                          new TableLimits(4, 1, 1),
                                          TableResult.State.ACTIVE, 10000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        tres = tableOperation(handle, geoIndex, null,
                              TableResult.State.ACTIVE, 10000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        PrepareRequest prepReq = new PrepareRequest().setStatement(geoQuery);
        PrepareResult prepRet = getNextHandle().prepare(prepReq);
        assertNotNull(prepRet.getPreparedStatement());
    }

    /*
     * Test use of large query strings for insert/update/upsert
     */
    @Test
    public void testLargeQueryStrings() {
        final String tableName = "LargeQuery";
        final String createTable = "create table " + tableName +
            "(id integer, data json, primary key(id))";
        final int[] stringSizes = {10, 500, 5000, 20000, 500000};

        tableOperation(handle, createTable,
                                          new TableLimits(4, 1000, 1000),
                                          TableResult.State.ACTIVE, 10000);
        /* create a large JSON data string */
        for (int size : stringSizes) {
            String data = createLargeJson(size);
            String iquery = "insert into " + tableName + " values(1," +
                data + ") returning id";
            String uquery = "update " + tableName + " t " +
                "set t.data = " + data + "where id = 1 returning id";

            /* insert, then update */
            QueryRequest req = new QueryRequest().setStatement(iquery);
            QueryResult res = getNextHandle().query(req);
            assertEquals(1, res.getResults().get(0).get("id").getInt());
            req = new QueryRequest().setStatement(uquery);
            res = getNextHandle().query(req);
            assertEquals(1, res.getResults().get(0).get("id").getInt());
        }

        /* validate that select fails */
        final String squery = "select * from " + tableName +
            " t where t.data.data = " + makeString(15000);
        QueryRequest req = new QueryRequest().setStatement(squery);
        try {
            getNextHandle().query(req);
            fail("Query should have failed");
        } catch (IllegalArgumentException iae) {
            // success
        }
    }

    @Test
    public void testBindArrayValue() {
        final String tableName = "testBindArrayValue";
        final String createTable = "create table if not exists " + tableName +
                "(id integer, " +
                 "info record(name string, age integer, " +
                             "address record(street string, room integer)), " +
                 "primary key(id))";

        tableOperation(handle, createTable, new TableLimits(100, 100, 1),
                       TableResult.State.ACTIVE, 10000);

        String stmt = "declare $id integer;" +
                      "$info record(name string, age integer, " +
                                   "address record(street string, " +
                                                  "room integer));" +
                      "upsert into " + tableName + " values($id, $info)";
        PrepareRequest prepReq = new PrepareRequest().setStatement(stmt);
        PrepareResult prepRet = getNextHandle().prepare(prepReq);
        PreparedStatement pstmt = prepRet.getPreparedStatement();

        MapValue mapVal;
        int id = 0;

        /* Case1: all fields are specified with non-null value */
        ArrayValue adVal = new ArrayValue()
                .add("35 Network drive")
                .add(203);
        ArrayValue arrVal = new ArrayValue()
                .add("Jack Wang")
                .add(40)
                .add(adVal);
        mapVal = new MapValue()
                .put("name", arrVal.get(0))
                .put("age", arrVal.get(1))
                .put("address",
                     new MapValue().put("street", adVal.get(0))
                                   .put("room", adVal.get(1)));
        execInsertAndCheckInfo(pstmt, ++id, arrVal, tableName, mapVal);

        /* Case2: address = NULL*/
        arrVal = new ArrayValue()
                .add("Jack Wang")
                .add(40)
                .add(NullValue.getInstance());
        mapVal = new MapValue()
                .put("name", arrVal.get(0))
                .put("age", arrVal.get(1))
                .put("address", NullValue.getInstance());
        execInsertAndCheckInfo(pstmt, ++id, arrVal, tableName, mapVal);

        /*
         * Case3: age = "40" and address.room = "203" which are castable to
         *        integer
         */
        adVal = new ArrayValue()
                .add("35 Network drive")
                .add("203");
        arrVal = new ArrayValue()
                .add("Jack Wang")
                .add("40")
                .add(adVal);
        mapVal = new MapValue()
                .put("name", arrVal.get(0))
                .put("age", 40)
                .put("address",
                     new MapValue().put("street", adVal.get(0))
                                   .put("room", 203));
        execInsertAndCheckInfo(pstmt, ++id, arrVal, tableName, mapVal);

        /*
         * Negative cases
         */
        /* info.name: Type mismatch on input. Expected STRING, got INTEGER */
        arrVal = new ArrayValue()
                .add(40)
                .add("Jack Wang")
                .add(NullValue.getInstance());
        pstmt.setVariable("$id", new IntegerValue(id));
        pstmt.setVariable("$info", arrVal);

        QueryRequest req = new QueryRequest().setPreparedStatement(pstmt);
        try {
            getNextHandle().query(req);
            fail("Expect fail with IAE but not");
        } catch(IllegalArgumentException ex) {
        }

        /*
         * Invalid Array value for Record Value, it has 1 element but
         * the Record Value contains 3 fields
         */
        arrVal = new ArrayValue()
                .add("Jack Wang");
        pstmt.setVariable("$id", new IntegerValue(id));
        pstmt.setVariable("$info", arrVal);

        req = new QueryRequest().setPreparedStatement(pstmt);
        try {
            getNextHandle().query(req);
            fail("Expect fail with IAE but not");
        } catch(IllegalArgumentException ex) {
        }
    }

    @Test
    public void testCRDT() {
        if (!onprem) {
            return;
        }

        final String setRegion = "set local region localRegion";
        getNextHandle().doSystemRequest(setRegion, 20000, 1000);

        /* Test reading different types of CRDT. */
        FieldValue val = testCRDT(Type.INTEGER);
        assertTrue(val.getInt() == 3);

        val = testCRDT(Type.LONG);
        assertTrue(val.getLong() == 3);

        val = testCRDT(Type.NUMBER);
        assertTrue(val.getNumber().equals(BigDecimal.valueOf(3)));
    }

    @Test
    public void testUpdateMultipleRows() {

        assumeKVVersion("testUpdateMultipleRows", 24, 4, 0);

        String tableName = "testUpdateQuery";
        String tableDdl = "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                              "sid INTEGER, " +
                              "id INTEGER, " +
                              "i INTEGER, " +
                              "s STRING, " +
                              "PRIMARY KEY(SHARD(sid), id))";
        TableLimits limits = new TableLimits(100, 100, 1);

        String updBySid = "UPDATE " + tableName + " SET i = i + 1 WHERE sid = 0";
        String updBySidRet = updBySid + " RETURNING *";
        String qryBySid = "SELECT id, i FROM " + tableName + " WHERE sid = 0";

        int numSids = 2;
        int numIdsPerSid = 10;
        int updRKB = 10;
        int updWKB = 20;
        int ddlWaitMs = 10000;
        String str = genString(512);

        tableOperation(handle, tableDdl, limits, ddlWaitMs);

        /* load rows */
        PutRequest preq = new PutRequest().setTableName(tableName);
        PutResult pret;
        for (int sid = 0; sid < numSids; sid++) {
            for (int id = 0; id < numIdsPerSid; id++) {
                MapValue row = new MapValue()
                        .put("sid", sid)
                        .put("id", id)
                        .put("i", id)
                        .put("s", str);
                preq.setValue(row);
                pret = handle.put(preq);
                assertNotNull(pret.getVersion());
            }
        }

        PreparedStatement update =
            handle.prepare(new PrepareRequest().setStatement(updBySid))
                .getPreparedStatement();

        PreparedStatement query =
            handle.prepare(new PrepareRequest().setStatement(qryBySid))
                .getPreparedStatement();

        QueryRequest req;
        QueryResult ret;
        int inc = 0;
        TableLimits newLimits;

        req = new QueryRequest().setPreparedStatement(update);
        ret = handle.query(req);
        inc++;
        assertEquals(1, ret.getResults().size());
        assertEquals("{\"NumRowsUpdated\":10}",
                     ret.getResults().get(0).toJson());
        if (!onprem) {
            assertEquals(updRKB, ret.getReadKB());
            assertEquals(2 * updRKB, ret.getReadUnits());
            assertEquals(updWKB, ret.getWriteKB());
        }

        /*
         * Test maxReadKB/maxWriteKB
         *
         * Update should fail if data read or write during query is less than
         * maxReadKB or maxWriteKB
         */
        req = new QueryRequest()
                .setPreparedStatement(update)
                .setMaxReadKB(updRKB - 1);
        try {
            ret = handle.query(req);
            fail("expect to fail");
        } catch (IllegalArgumentException ex) {
            req = req.copy().setMaxReadKB(updRKB);
            ret = handle.query(req);
            assertEquals("{\"NumRowsUpdated\":" + numIdsPerSid + "}",
                         ret.getResults().get(0).toJson());
            inc++;
        }

        req = new QueryRequest()
                .setPreparedStatement(update)
                .setMaxWriteKB(updWKB - 3);
        try {
            handle.query(req);
            fail("expect to fail");
        } catch (IllegalArgumentException ex) {
            req = req.copy().setMaxWriteKB(updWKB);
            ret = handle.query(req);
            assertEquals("{\"NumRowsUpdated\":" + numIdsPerSid + "}",
                         ret.getResults().get(0).toJson());
            inc++;
        }

        /*
         * Returning clause is not supported if complete primary key is not
         * provided.
         */
        try {
            handle.prepare(new PrepareRequest().setStatement(updBySidRet));
            fail("expect to fail");
        } catch (IllegalArgumentException ex) {
        }

        /* Verify rows */
        req = new QueryRequest().setPreparedStatement(query);
        ret = handle.query(req);
        for (MapValue r : ret.getResults()) {
            assertEquals(inc, r.get("i").getInt() - r.get("id").getInt());
        }

        /*
         * Test the QueryRequest.limit on update query.
         */

        /*
         * Use QueryRequest.limit that is smaller than the number of rows to
         * update:
         *  - In onprem, update query should fail.
         *  - In cloud, the limit doesn't apply, query should succeed.
         */
        req = new QueryRequest()
                .setPreparedStatement(update)
                .setLimit(numIdsPerSid - 1);
        if (onprem) {
            try {
                ret = handle.query(req);
                fail("Query should have failed due to exceeding the limit " +
                     "for the max number of records can be updated");
            } catch (IllegalArgumentException ex) {
            }

            /* Increase the limit, query succeed */
            req.setLimit(numIdsPerSid);
            ret = handle.query(req);
            inc++;
            assertEquals(1, ret.getResults().size());
            assertEquals("{\"NumRowsUpdated\":" + numIdsPerSid + "}",
                         ret.getResults().get(0).toJson());
        } else {
            /* The limit doesn't apply to cloud */
            ret = handle.query(req);
            inc++;
            assertEquals(1, ret.getResults().size());
            assertEquals("{\"NumRowsUpdated\":" + numIdsPerSid + "}",
                         ret.getResults().get(0).toJson());
        }

        if (onprem) {
            return;
        }

        /*
         * Test throttling update query
         */

        /*
         * Create a new handle configured with no retries
         */
        NoSQLHandleConfig config = new NoSQLHandleConfig(getProxyEndpoint());
        setHandleConfig(config);
        /* no retries */
        config.configureDefaultRetryHandler(0, 0);
        NoSQLHandle handleNoRetry = getHandle(config);

        /* Small readUnits */
        newLimits = new TableLimits(1, 100, 1);
        tableOperation(handle, null /* ddlStatement */, newLimits, tableName,
                       TableResult.State.ACTIVE, ddlWaitMs);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }

        boolean throttled = false;
        try {
            for (int i = 0; i < 3; i++) {
                req = new QueryRequest().setPreparedStatement(update);
                ret = handleNoRetry.query(req);
                inc++;
            }
        } catch (ReadThrottlingException te) {
            throttled = true;
        }
        assertTrue(throttled);

        /* Small writeUnits */
        newLimits = new TableLimits(100, 1, 1);
        tableOperation(handle, null /* ddlStatement */, newLimits, tableName,
                       TableResult.State.ACTIVE, ddlWaitMs);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }

        throttled = false;
        try {
            for (int i = 0; i < 3; i++) {
                req = new QueryRequest().setPreparedStatement(update);
                ret = handleNoRetry.query(req);
                inc++;
            }
        } catch (WriteThrottlingException te) {
            throttled = true;
        }
        assertTrue(throttled);

        /* Verify rows */
        req = new QueryRequest().setPreparedStatement(query);
        ret = handle.query(req);
        for (MapValue r : ret.getResults()) {
            assertEquals(inc, r.get("i").getInt() - r.get("id").getInt());
        }
    }

    private FieldValue testCRDT(Type type) {
        String tableName = "mrtable" + type;
        final String createTable = "create table " + tableName +
            "(id integer, count "  + type + " as mr_counter" +
            ", primary key(id)) in regions localRegion";

        getNextHandle().doSystemRequest(createTable, 20000, 1000);

        /* Insert a row with CRDT.  */
        String insertStmt = "insert into " + tableName +
            " values (1, default)";
        QueryRequest req = new QueryRequest().setStatement(insertStmt);
        getNextHandle().query(req);

        String updateStmt = "Update " + tableName +
            " set count = count + 3 where id = 1";
        req = new QueryRequest().setStatement(updateStmt);
        getNextHandle().query(req);

        String selectStmt = "select * from " + tableName + " where id = 1";
        req = new QueryRequest().setStatement(selectStmt);
        QueryResult ret = getNextHandle().query(req);
        assertTrue(ret.getResults().size() == 1);
        MapValue res = ret.getResults().get(0);
        return res.get("count");
    }

    private void execInsertAndCheckInfo(PreparedStatement pstmt,
                                        int id,
                                        FieldValue info,
                                        String tableName,
                                        MapValue expInfo) {

        pstmt.setVariable("$id", new IntegerValue(id));
        pstmt.setVariable("$info", info);

        QueryRequest req;
        QueryResult ret;

        req = new QueryRequest().setPreparedStatement(pstmt);
        ret = getNextHandle().query(req);
        assertEquals(1, ret.getResults().get(0).asMap()
                           .get("NumRowsInserted").getInt());

        String stmt = "select info from " + tableName + " where id = " + id;
        req = new QueryRequest().setStatement(stmt);
        ret = getNextHandle().query(req);
        assertEquals(1, ret.getResults().size());
        assertEquals(expInfo, ret.getResults().get(0).get("info"));
    }

    private String createLargeJson(int size) {
        MapValue map = new MapValue();
        map.put("data", makeString(size));
        return map.toString();
    }

    private void executeQuery(String statement,
                              boolean keyOnly,
                              boolean indexScan,
                              int expNumRows,
                              int expReadKB,
                              int numLimit,
                              int sizeLimit,
                              int recordKB) {
        executeQuery(statement, keyOnly, indexScan, expNumRows, expReadKB,
                     numLimit, sizeLimit, recordKB, Consistency.EVENTUAL);
        executeQuery(statement, keyOnly, indexScan, expNumRows, expReadKB,
                     numLimit, sizeLimit, recordKB, Consistency.ABSOLUTE);
    }

    private void executeQuery(String statement,
                              boolean keyOnly,
                              boolean indexScan,
                              int expNumRows,
                              int expReadKB,
                              int numLimit,
                              int sizeLimit,
                              int recordKB,
                              Consistency consistency) {

        if (traceLevel >= 2) {
            System.out.println("Executing query : " + statement);
        }

        final int minRead = getMinRead();
        final boolean isAbsolute = (consistency == Consistency.ABSOLUTE);
        boolean isDelete = statement.contains("delete");

        final QueryRequest queryReq = new QueryRequest()
            .setStatement(statement)
            .setLimit(numLimit)
            .setConsistency(consistency)
            .setMaxReadKB(sizeLimit)
            .setTraceLevel(traceLevel);

        if (consistency != null) {
            queryReq.setConsistency(consistency);
        }

        int expReadUnits = expReadKB;
        int expBatchReadUnits = (sizeLimit > 0) ? sizeLimit : READ_KB_LIMIT;
        if (checkKVVersion(21, 3, 6)) {
            /*
             * Query should suspend after read the table row or key
             * (for key only query) if current read cost exceeded size limit,
             * so at most the readKB over the size limit
             */
            expBatchReadUnits += (!keyOnly) ? recordKB : minRead;
        } else {
            expBatchReadUnits += (indexScan && !keyOnly) ? recordKB : minRead;
        }
        expBatchReadUnits += (isDelete ? minRead : 0);
        if (isAbsolute) {
            expBatchReadUnits <<= 1;
            expReadUnits <<= 1;
        }

        int numRows = 0;
        int readKB = 0;
        int writeKB = 0;
        int readUnits = 0;
        int numBatches = 0;
        int totalPrepCost = 0;
        long startMs = System.currentTimeMillis();

        do {
            if (traceLevel >= 2) {
                System.out.println("Starting BATCH " + numBatches);
            }

            QueryResult queryRes = getNextHandle().query(queryReq);

            if (traceLevel >= 2) {
                System.out.println(" BATCH " + numBatches +
                    " after handle.query(), calling getResults()");
            }
            List<MapValue> results = queryRes.getResults();
            if (traceLevel >= 2) {
                System.out.println(" BATCH " + numBatches +
                                   " after getResults()");
            }

            int cnt = results.size();
            if (numLimit > 0) {
                assertTrue("Unexpected number of rows returned, expect <= " +
                           numLimit + ", but get " + cnt + " rows",
                           cnt <= numLimit);
            }

            int rkb = queryRes.getReadKB();
            int runits = queryRes.getReadUnits();
            int wkb = queryRes.getWriteKB();
            int prepCost = (numBatches == 0 ? getMinQueryCost() : 0);

            /*
             * Make sure we didn't exceed the read limit. The "+ recordKB" is
             * needed because at the RNs we allow the limit to be exceeded by
             * 1 row, if we have already read the index entry for that row. The
             * "+ 1" is needed for DELETE queries, because a row that satisfies
             * the DELETE conditions, we read its primary-index once again to
             * do the delete.
             */
            assert(queryRes.getReadKB() <=
                   prepCost + getEffectiveMaxReadKB(queryReq) + recordKB + 1);

            if (showResults) {
                for (int i = 0; i < results.size(); ++i) {
                    System.out.println("Result " + (numRows + i) + " :");
                    System.out.println(results.get(i));
                }
            }

            if (traceLevel >= 2) {
                System.out.println("Batch ReadKB = " + rkb +
                                   " Batch ReadUnits = " + runits +
                                   " Batch WriteKB = " + wkb);
            }

            numRows += cnt;

            assertTrue("Unexpected readUnits, expect <= " +
                       (expBatchReadUnits + prepCost) + ", but get " + runits,
                       runits <= (expBatchReadUnits + prepCost));
            readKB += rkb;
            readUnits += runits;
            writeKB += wkb;
            totalPrepCost += prepCost;

            numBatches++;
        } while (!queryReq.isDone());

        if (traceLevel >= 2) {
            System.out.println("Total ReadKB = " + readKB +
                               " Total ReadUnits = " + readUnits +
                               " Total WriteKB = " + writeKB);
        }

        if (verbose) {
            long diffMs = System.currentTimeMillis() - startMs;
            if (diffMs == 0) {
                diffMs = 1; /* avoid /0 error */
            }
            System.out.println("query used " + readUnits + "RUs in " +
                               diffMs + "ms (" +
                               ((readUnits * 1000) / diffMs) + " RUs/s)" +
                               " maxReadKB=" + sizeLimit +
                               " limit=" + numLimit);
            System.out.println(" query: " + statement);
        }

        if (!onprem) {
            assertTrue("Read KB and Read units should be > 0",
                       readKB > 0 && readUnits > 0);
        }
        assertEquals("Wrong number of rows returned, expect " + expNumRows +
                     ", but get " + numRows, expNumRows, numRows);

        if (expReadKB >= 0 && onprem == false) {
            int delta = 0;
            if (numBatches == 1) {
                /*
                 * For ALL_PARTITIONS query, it might charge additional empty
                 * read cost(1KB) if all matched keys are all in the last shard
                 * but not 1st partition.
                 */
                delta = 1;
            } else {
                /* If a batch must resume after the resume key, but there are no
                 * more keys in the index range, an empty read is charged. */
                delta = 1;
            }

            if (isAbsolute) {
                delta <<= 1;
            }

            expReadUnits += totalPrepCost;

            if (traceLevel >= 2) {
                queryReq.printTrace(System.out);
            }

            assertTrue("Unexpected read units, exp in range[" +
                       expReadUnits + ", " + (expReadUnits + delta) +
                       "] actual " + readUnits,
                       readUnits >= expReadUnits &&
                       readUnits <= expReadUnits + delta);
            /* Verify the readKB with readUnits */
            assertQueryReadKB(readKB, readUnits, totalPrepCost, isAbsolute);
        }
    }

    private void executeQuery(String query,
                              Map<String, FieldValue> bindValues,
                              int expNumRows,
                              int maxReadKB,
                              boolean usePrepStmt) {

        final QueryRequest queryReq;

        if (bindValues == null || !usePrepStmt) {
            queryReq = new QueryRequest().
                       setStatement(query).
                       setMaxReadKB(maxReadKB).
                       setTraceLevel(traceLevel);
        } else {
            PrepareRequest prepReq = new PrepareRequest().setStatement(query);
            PrepareResult prepRes = getNextHandle().prepare(prepReq);
            PreparedStatement prepStmt = prepRes.getPreparedStatement();
            if (bindValues != null) {
                for (Entry<String, FieldValue> entry : bindValues.entrySet()) {
                    prepStmt.setVariable(entry.getKey(), entry.getValue());
                }
            }

            queryReq = new QueryRequest().
                       setPreparedStatement(prepStmt).
                       setMaxReadKB(maxReadKB).
                       setTraceLevel(traceLevel);
        }

        queryReq.setTraceLevel(traceLevel);

        QueryResult queryRes;
        int numRows = 0;
        int totalReadKB = 0;
        int totalReadUnits = 0;

        long startMs = System.currentTimeMillis();

        do {
            queryRes = getNextHandle().query(queryReq);
            numRows += queryRes.getResults().size();

            if (showResults) {
                List<MapValue> results = queryRes.getResults();
                for (int i = 0; i < results.size(); ++i) {
                    System.out.println("Result " + i + " :");
                    System.out.println(results.get(i));
                }
                System.out.println("ReadKB = " + queryRes.getReadKB() +
                                   " ReadUnits = " + queryRes.getReadUnits());
            }

            /*
             * Note: in some rare cases we may get zero readKB with 1 result.
             * From Markos:
             *
             * When we do index/sort based group by, if we reach the read limit
             * in the middle of computing a group, we include the partially
             * computed group row in the continuation key. When we send the
             * continuation key back, we may discover (without reading any
             * bytes) that the group was actually fully computed, and we now
             * send it back as a result.
             *
             * So we take this rare case into account by allowing zero readKB
             * if the numresults is 1 and we've already accumulated readKBs.
             */
            if (!onprem &&
                (queryRes.getResults().size() > 1 || totalReadKB == 0)) {
                assertTrue(queryRes.getReadKB() > 0);
            }

            totalReadKB += queryRes.getReadKB();
            totalReadUnits += queryRes.getReadUnits();
        } while (!queryReq.isDone());

        assertTrue("Wrong number of rows returned, expect " + expNumRows +
                   ", but get " + numRows, numRows == expNumRows);

        if (verbose) {
            long diffMs = System.currentTimeMillis() - startMs;
            if (diffMs == 0) {
                diffMs = 1; /* avoid /0 error */
            }
            System.out.println("query used " + totalReadUnits + "RUs in " +
                               diffMs + "ms (" +
                               ((totalReadUnits * 1000) / diffMs) + " RUs/s)");
            System.out.println(" maxReadKB=" + maxReadKB);
            System.out.println(" query: " + query);
        }
    }

    private void loadRowsToScanTable(int numMajor, int numPerMajor, int nKB) {

        MapValue value = new MapValue();
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);

        String states[] = { "CA", "OR", "WA", "VT", "NY" };
        int[] salaries = { 1000, 15000, 8000, 9000 };
        ArrayValue[] arrays = new ArrayValue[4];

        for (int i = 0; i < 4; ++i) {
            arrays[i] = new ArrayValue(4);
        }
        arrays[0].add(1).add(5).add(7).add(10);
        arrays[1].add(4).add(7).add(7).add(11);
        arrays[2].add(3).add(8).add(17).add(21);
        arrays[3].add(3).add(8).add(12).add(14);

        int slen = (nKB - 1) * 1024;
        /* Load rows */
        for (int i = 0; i < numMajor; i++) {
            value.put("sid", i);
            for (int j = 0; j < numPerMajor; j++) {
                value.put("id", j);
                value.put("name", "name_" + j);
                value.put("age", j % 10);
                value.put("state", states[j % 5]);
                value.put("salary", salaries[j % 4]);
                value.put("array", arrays[j % 4]);
                value.put("longString", genString(slen));
                PutResult res = getNextHandle().put(putRequest);
                assertNotNull("Put failed", res.getVersion());
            }
        }
    }

    private String genString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append((char)('A' + i % 26));
        }
        return sb.toString();
    }

    private void loadRowsToTable(String tabName, String[] jsons) {

        for (String json : jsons) {
            MapValue value = (MapValue)JsonUtils.createValueFromJson(json, null);
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tabName);
            PutResult res = getNextHandle().put(putRequest);
            assertNotNull("Put failed", res.getVersion());
        }
    }

    private static int getMinQueryCost() {
        return MIN_QUERY_COST;
    }

    /*
     * Note that the "expQueryKB" is the expected read KB for executing query,
     * it doesn't include the cost of preparing query.
     */
    private void assertQueryReadKB(int expQueryKB,
                                   int actualKB,
                                   int actualUnits,
                                   int prepCost,
                                   boolean isAbsolute) {
        assertReadKB(expQueryKB,
                     (actualKB - prepCost),
                     (actualUnits - prepCost),
                     isAbsolute);
    }

    /*
     * Note that the "expQueryKB" is the expected read KB for executing query,
     * it doesn't include the cost of preparing query.
     */
    private void assertQueryReadKB(Result result,
                                   int expQueryKB,
                                   int prepCost,
                                   boolean isAbsolute) {
        assertQueryReadKB(expQueryKB,
                          result.getReadKBInternal(),
                          result.getReadUnitsInternal(),
                          prepCost,
                          isAbsolute);
    }

    private void assertQueryReadKB(int actualKB,
                                   int actualUnits,
                                   int prepCost,
                                   boolean isAbsolute) {
        /*
         * Check on actual readKB and actual read units with the specified
         * isAsbolute only.
         */
        assertReadKBUnits((actualKB - prepCost),
                          (actualUnits - prepCost),
                          isAbsolute);
    }
}
