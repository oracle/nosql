/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.proxy.security.SecureTestUtil;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Some test cases that exercise issues with queries, max read
 * sizes, throttling and ability to make forward progress on
 * small capacity tables.
 *
 * These tests only runs against a local server and not minicloud.
 */
public class QueryThrottlingTest extends ProxyTestBase {

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        /* This test is for cloudsim only */
        assumeTrue("Skip QueryThrottlingTest if not cloud sim test",
                   !Boolean.getBoolean(ONPREM_PROP) &&
                   !Boolean.getBoolean(USEMC_PROP) &&
                   !Boolean.getBoolean(USECLOUD_PROP));

        ProxyTestBase.staticSetUp();
    }

    @Test
    public void throttleTest() throws Exception {

        final String tableName = "testQueryThrottle";
        /*
         * don't use too many records as it slows the test because of the
         * delay in the query loop, but use enough to cause some looping.
         */
        final int numRecords = 500;
        final int recordSize = 1000;
        final int readLimit = 100;

        /*
         * Create a new handle configured with no retries
         */
        NoSQLHandleConfig config = new NoSQLHandleConfig(getEndpoint());
        SecureTestUtil.setAuthProvider(config, isSecure(), getTenantId());
        config.configureDefaultRetryHandler(0, 0);

        /*
         * Open the handle
         */
        NoSQLHandle myhandle = getHandle(config);

        /*
         * Use high write throughput for loading, read throughput is what
         * is being tested.
         */
        TableResult tres = tableOperation(
            myhandle,
            "create table " + tableName + "(id integer, " +
            "load string, primary key(id))",
            new TableLimits(readLimit, 30000, 50),
            TableResult.State.ACTIVE,
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /*
         * Load the table
         */
        MapValue value = new MapValue().put("load", makeString(recordSize));
        PutRequest putRequest = new PutRequest().setTableName(tableName).
            setValue(value);
        for (int i = 0; i < numRecords; i++) {
            value.put("id", i);
            PutResult res = myhandle.put(putRequest);
            assertNotNull(res.getVersion());
        }

        List<MapValue> res;
        String query = "select count(*) from " + tableName;
        QueryRequest qr = new QueryRequest().setStatement(query);
        res = runQuery(myhandle, qr);
        assertEquals(1, res.size());

        delay(1000);

        query = "select * from " + tableName;
        qr = new QueryRequest().setStatement(query);
        res = runQuery(myhandle, qr);
        assertEquals(numRecords, res.size());
    }

    /**
     * Run the query in a loop until no more results.
     * return the number of results.
     */
    private List<MapValue> runQuery(NoSQLHandle myhandle, QueryRequest qr) {

        List<MapValue> results = new ArrayList<MapValue>();

        try {
            do {
                QueryResult res = myhandle.query(qr);
                int num = res.getResults().size();
                if (num > 0) {
                    results.addAll(res.getResults());
                }
                /*
                 * Do approximate rate-limiting to prevent throttling. This test
                 * is intended to ensure forward progress and not directly
                 * test throttling behavior.
                 *
                 * This should ensure that throttling does not occur,
                 * assuming that the proxy has halved the default
                 * throughput for the table. While this test could modify
                 * the requested KBs itself that is not the point of the
                 * test -- the proxy should do the reduction
                 */
                delay(1000);
            } while (!qr.isDone());
        } catch (ReadThrottlingException rte) {
            fail("Test should not have been throttled. Check the delay as " +
                 "it may be an environmental issue");
        }
        return results;
    }
}
