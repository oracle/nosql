/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;

import oracle.nosql.proxy.security.SecureTestUtil;
import oracle.nosql.proxy.util.CreateStore;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class QueryResumeTest extends ProxyTestBase {

    private final Logger logger = Logger.getLogger(getClass().getName());

    private CreateStore createStore = null;
    private KVStore kvstore = null;
    private Proxy proxy;
    private NoSQLHandle handle = null;

    private int maxReadKB = 19;

    private static final String tableName = "users";

    private static final String usersDDL =
        "CREATE TABLE IF NOT EXISTS users ( " +
        "   uid integer, "   +
        "   name string, "   +
        "   int integer, "   +
        " PRIMARY KEY (uid))";

    private static final String idxIntDDL =
        "CREATE INDEX IF NOT EXISTS idx_int ON users(int)";

    private static final String queryDML =
        "select * from users where 1 <= int and int <= 2 order by int desc";

    private static final String delQueryDML =
        "delete from users where uid = 10";

    /* these override the Before/AfterClass methods in ProxyTestBase */
    @BeforeClass
    public static void staticSetUp() {
        Assume.assumeTrue("Skipping QueryResumeTest in minicloud or cloud test",
                          !Boolean.getBoolean(USEMC_PROP) &&
                          !Boolean.getBoolean(USECLOUD_PROP));
    }

    @AfterClass
    public static void staticTearDown() {}

    @Override
    @Before
    public void setUp() throws Exception {
        cleanupTestDir();
    }

    @Override
    @After
    public void tearDown() throws Exception {

        if (proxy != null) {
            proxy.shutdown(3, TimeUnit.SECONDS);
            proxy = null;
        }

        if (handle != null) {
            handle.close();
            handle = null;
        }

        if (kvstore != null) {
            kvstore.close();
            kvstore = null;
        }

        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
    }

     private void createStore() throws Exception {

        int port = getKVPort();
        String rootDir = getTestDir();
        createStore =
            new CreateStore(
                rootDir,
                getStoreName(),
                port,
                3, /* nsns */
                3, /* rf */
                10, /*partitions*/
                1, /*capacity*/
                2, /* mb */
                false, /* use threads */
                null);
        final File root = new File(rootDir);
        root.mkdirs();
        createStore.start();

        kvstore = KVStoreFactory.getStore(
            new KVStoreConfig(getStoreName(),
                              String.format("%s:%s", getHostName(), port)));

        proxy = ProxyTestBase.startProxy();

        handle = createHandle();
    }

    private NoSQLHandle createHandle() {

        NoSQLHandleConfig hconfig =
            new NoSQLHandleConfig(ProxyTestBase.getProxyEndpoint());

        /* 5 retries, default retry algorithm */
        hconfig.configureDefaultRetryHandler(5, 0);

        hconfig.setRequestTimeout(30000);
        //hconfig.setNumThreads(20);

        SecureTestUtil.setAuthProvider(hconfig,
                                       ProxyTestBase.SECURITY_ENABLED,
                                       ProxyTestBase.onprem(),
                                       ProxyTestBase.getTenantId());
        hconfig.setLogger(logger);

        /* Open the handle */
        NoSQLHandle h = NoSQLHandleFactory.createNoSQLHandle(hconfig);

        /* do a simple op to set the protocol version properly */
        try {
            GetTableRequest getTable =
                new GetTableRequest().setTableName("noop");
            h.getTable(getTable);
        } catch (TableNotFoundException e) {}

        return h;
    }

    private void createTableAndIndex() {

        TableLimits limits = new TableLimits(90000, 15000, 50);
        int timeout = 20000;

        ProxyTestBase.tableOperation(handle, usersDDL, limits,
                                     TableResult.State.ACTIVE, timeout);
        ProxyTestBase.tableOperation(handle, idxIntDDL, null,
                                     TableResult.State.ACTIVE, timeout);
    }

    private void populateTable() {

        int numRows = 100;
        int numRowsPerKey = 10;
        MapValue row = new MapValue();
        int uid = 0;
        int key = 0;

        PutRequest putRequest = new PutRequest().
                                setValue(row).
                                setTableName(tableName);

        while (uid < numRows) {

            for (int i = 0; i < numRowsPerKey; ++i) {

                row.put("uid", uid);
                row.put("int", key);
                row.put("name", ("name." + key));

                PutResult res = handle.put(putRequest);
                assertNotNull("Put failed", res.getVersion());

                ++uid;
            }

            ++key;
        }

    }

    @Test
    public void testQueryResume() throws Exception {

        /* Create a 1x3 store with 10 partitions */
        createStore();

        /* Create the table and index */
        createTableAndIndex();

        /* Populate the table */
        populateTable();

        /* Prepare the query */
        PrepareRequest preq = new PrepareRequest();
        preq.setGetQueryPlan(true);
        preq.setStatement(queryDML);
        PrepareResult pres = handle.prepare(preq);
        PreparedStatement prep = pres.getPreparedStatement();

        /* Execute the 1st query batch */
        QueryRequest qreq = new QueryRequest();
        qreq.setPreparedStatement(prep);
        qreq.setQueryName("testQueryResume");
        qreq.setMaxReadKB(maxReadKB);
        qreq.setTraceLevel(3);

        int numResults = 0;
        QueryResult res = handle.query(qreq);
        numResults += res.getResults().size();

        /* Delete the row on which the query is supposed to resume from */
        QueryRequest delreq = new QueryRequest();
        delreq.setStatement(delQueryDML);
        do {
            res = handle.query(delreq);
            res.getResults();
        } while (!delreq.isDone());

        /* Execute the rest of the query */
        do {
            res = handle.query(qreq);
            numResults += res.getResults().size();
        } while (!qreq.isDone());

        assertTrue(numResults == 19);
    }
}
