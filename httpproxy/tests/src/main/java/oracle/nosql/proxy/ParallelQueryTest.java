/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
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
import oracle.nosql.driver.values.MapValue;

import oracle.nosql.proxy.kv.KVTenantManager;
import oracle.nosql.proxy.sc.TenantManager;
import oracle.nosql.proxy.sc.LocalTenantManager;
import oracle.nosql.proxy.security.AccessCheckerFactory;
import oracle.nosql.proxy.security.SecureTestUtil;

/**
 * Test parallel queries. This is a separate test from QueryTest
 * because it requires use of a multi-shard store to test parallel
 * indexed queries
 */
public class ParallelQueryTest extends ProxyTestBase {

    protected static TenantManager tm;
    protected static String proxyEndpoint;
    protected static String parallelStoreName = "ParallelQueryStore";


    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        assumeTrue("Skip ParallelQuery in minicloud or cloud test",
                   !Boolean.getBoolean("usemc") &&
                   !Boolean.getBoolean("usecloud"));
        startKV();
        startLocalProxy();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        stopProxy();

        if (kvlite != null) {
            kvlite.stop(false);
        }

        cleanupTestDir();
    }

    private static void startKV() {
        verbose = Boolean.getBoolean(VERBOSE_PROP);
        /*
         * use a different store name to avoid conflicts in topology
         */
        kvlite = startKVLite(hostName,
                             parallelStoreName,
                             false, /* don't useThreads */
                             verbose,
                             true, /* multishard true */
                             0, /* default memory MB */
                             false, /* not secure */
                             getKVPort(), /* default */
                             getPortRange(), /* default */
                             getTestDir());
    }

    protected static void startLocalProxy() {
        onprem = Boolean.getBoolean(ONPREM_PROP);

        Properties commandLine = new Properties();

        commandLine.setProperty(Config.STORE_NAME.paramName,
                                parallelStoreName);
        commandLine.setProperty(Config.HELPER_HOSTS.paramName,
                                (hostName + ":" + getKVPort()));
        Config.ProxyType ptype = (onprem ? Config.ProxyType.KVPROXY :
                                  Config.ProxyType.CLOUDTEST);
        commandLine.setProperty(Config.PROXY_TYPE.paramName, ptype.name());
        commandLine.setProperty(Config.VERBOSE.paramName,
                                Boolean.toString(verbose));
        /* use a non-privileged port */
        commandLine.setProperty(Config.HTTP_PORT.paramName, "8095");

        /* allow query tracing */
        commandLine.setProperty(Config.QUERY_TRACING.paramName, "true");

        /* use defaults for thread pools and sizes */

        /* simple access checker */
        ac = AccessCheckerFactory.createInsecureAccessChecker();
        Config cfg = new Config(commandLine);
        /* create an appropriate TenantManager */
        if (onprem) {
            /* note: in KVPROXY mode the proxy *requires* a KVTenantManager */
            tm  = KVTenantManager.createTenantManager(cfg);
        } else {
            tm = LocalTenantManager.createTenantManager(cfg);
        }
        proxy = Proxy.initialize(cfg, tm, ac, null);
        proxyEndpoint = "http://" + hostName + ":" + cfg.getHttpPort();
    }

    protected static void stopProxy() throws Exception {
        if (proxy != null) {
            proxy.shutdown(3, TimeUnit.SECONDS);
            proxy = null;
        }
        if (tm != null) {
            tm.close();
            tm = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        /*
         * Configure the endpoint
         */
        if (handle == null) {
            NoSQLHandleConfig config =
                new NoSQLHandleConfig(proxyEndpoint);
            SecureTestUtil.setAuthProvider(config, false,
                                           onprem, getTenantId());
            handle = getHandle(config); /* see ProxyTestBase */
        }
        dropAllTables(handle, true);
    }

    @After
    public void tearDown() throws Exception {
        if (handle != null) {
            dropAllTables(handle, true);
            handle.close();
            handle = null;
        }
    }

    @Test
    public void testParallelQueryArgs() {
        final String tableName = "ParallelQuery";
        final String createTable = "create table " + tableName +
            "(id integer, primary key(id)) as json collection";
        final String query = "select * from " + tableName;
        tableOperation(handle, createTable,
                       new TableLimits(4, 1000, 1000),
                       TableResult.State.ACTIVE, 10000);
        PreparedStatement ps =
            handle.prepare(new PrepareRequest().setStatement(query))
            .getPreparedStatement();

        QueryRequest qr = new QueryRequest().setStatement(query).
            setNumberOfOperations(1);
        failParallelQuery(qr, "not prepared1", IllegalArgumentException.class);
        qr.setNumberOfOperations(0).setOperationNumber(1);
        failParallelQuery(qr, "not prepared2", IllegalArgumentException.class);

        /* use prepared statement now to check other params */
        qr.setPreparedStatement(ps).setStatement(null);
        failParallelQuery(qr, "numops set, opnum not set",
                          IllegalArgumentException.class);

        qr.setNumberOfOperations(1).setOperationNumber(0);
        failParallelQuery(qr, "opnum set, numops not set",
                          IllegalArgumentException.class);

        qr.setNumberOfOperations(1).setOperationNumber(2);
        failParallelQuery(qr, "opnum too large",
                          IllegalArgumentException.class);

        qr.setNumberOfOperations(ps.getMaximumParallelism() + 1);
        failParallelQuery(qr, "numops too large",
                          IllegalArgumentException.class);

        qr.setNumberOfOperations(-1);
        failParallelQuery(qr, "negative numops",
                          IllegalArgumentException.class);

        qr.setNumberOfOperations(1).setOperationNumber(-1);
        failParallelQuery(qr, "negative opnum",
                          IllegalArgumentException.class);

        String upd = "insert into " + tableName + "(id) values (2000)";
        ps = handle.prepare(new PrepareRequest().setStatement(upd))
            .getPreparedStatement();
        assertEquals(0, ps.getMaximumParallelism());
        /* any non-zero value is illegal for updates */
        qr.setPreparedStatement(ps).setOperationNumber(1).
            setNumberOfOperations(1);
        failParallelQuery(qr, "cannot insert/update",
                          IllegalArgumentException.class);
    }

    @Test
    public void testParallelMisc() {
        final int numRows = 1000;
        final String tableName = "ParallelQuery";
        final String createTable = "create table " + tableName +
            "(id integer, primary key(id)) as json collection";
        String createIndex = "create index idx on " + tableName +
            "(name as string)";
        tableOperation(handle, createTable,
                       new TableLimits(4, 1000, 1000),
                       TableResult.State.ACTIVE, 10000);
        tableOperation(handle, createIndex, null, null,
                       TableResult.State.ACTIVE, null);

        final String query1 = "select * from " + tableName; /* yes */
        final String query2 = "select * from " + tableName + /* no */
            " order by id";
        final String query3 = "select * from " + tableName + /* yes */
            " where name = 'joe'";
        final String query4 = "select count(*) from " + tableName; /* no */

        final String[] queries = new String[]{query1, query2, query3, query4};
        /*
         * These answers rely on the default configuration of a multishard
         * KVLite
         */
        final int[] answers = new int[]{multishardPartitions,
                                        0, multishardShards, 0};
        for (int i = 0; i < queries.length; i++) {
            assertEquals(answers[i], maxParallel(queries[i]));
        }
    }

    /*
     * Use query parallelism.
     * 1. in a non-threaded fashion to test that the use of subsets of a
     * table return complete, non-intersecting results
     * 2. in a threaded, truly parallel scenario
     *
     * Start with all partition parallelism and use a JSON collection table
     * TODO:
     *  o parallel indexed queries
     *  o queries that cannot be parallel (max 1)
     */
    @Test
    public void testParallelQuery() {
        final int numRows = 1000;
        final String tableName = "ParallelQuery";
        final String createTable = "create table " + tableName +
            "(id integer, primary key(id)) as json collection";
        String createIndex = "create index idx on " + tableName +
            "(name as string)";
        final String query = "select * from " + tableName;
        /* use an index query that will still return all results */
        final String indexQuery = "select * from " + tableName +
            " where name > 'm'";
        tableOperation(handle, createTable,
                       new TableLimits(10000, 10000, 1000),
                       TableResult.State.ACTIVE, 10000);
        tableOperation(handle, createIndex, null, null,
                       TableResult.State.ACTIVE, null);
        PreparedStatement ps =
            handle.prepare(new PrepareRequest().setStatement(query))
            .getPreparedStatement();
        int max = ps.getMaximumParallelism();
        assertEquals(multishardPartitions, max);

        /* load rows sufficient to cover all partitions */
        PutRequest preq = new PutRequest().setTableName(tableName);
        putRowsInParallelTable(preq, numRows);

        final AtomicInteger readKB = new AtomicInteger();
        final Set<Integer> keys = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < max; i++) {
            doSubsetQuery(ps, max, i + 1, keys, readKB);
        }
        /* did all of the results get read and are they unique? */
        assertEquals(numRows, keys.size());
        assertEquals(numRows, readKB.get());

        /*
         * do another "parallel" query but with 3 subsets and
         * make sure that all rows are read, with no duplicates
         */
        readKB.set(0);
        keys.clear();
        for (int i = 0; i < 3; i++) {
            doSubsetQuery(ps, 3, i + 1, keys, readKB);
        }
        assertEquals(numRows, keys.size());
        assertEquals(numRows, readKB.get());

        /* use indexed, all shard query that returns all results */
        PreparedStatement psIndex =
            handle.prepare(new PrepareRequest().setStatement(indexQuery))
            .getPreparedStatement();
        max = psIndex.getMaximumParallelism();
        assertEquals(multishardShards, max);

        readKB.set(0);
        keys.clear();
        for (int i = 0; i < max; i++) {
            doSubsetQuery(psIndex, max, i + 1, keys, readKB);
        }
        assertEquals(numRows, keys.size());
        assertEquals(numRows, readKB.get());

        /*
         * this is all shards use max of 2
         */
        readKB.set(0);
        keys.clear();
        for (int i = 0; i < 2; i++) {
            doSubsetQuery(psIndex, 2, i + 1, keys, readKB);
        }
        assertEquals(numRows, keys.size());
        assertEquals(numRows, readKB.get());

        /*
         * Rather than create a new test, tables, etc. reuse the existing
         * table and data and run these queries in threads vs sequentially
         */
        doQueryInThreads(ps, numRows);
        doQueryInThreads(psIndex, numRows);
    }

    private void doQueryInThreads(final PreparedStatement ps, int numRows) {
        /*
         * If the max is < 10, use it as num operations. If > 10 use
         * 10
         */
        final AtomicInteger readKB = new AtomicInteger();
        final Set<Integer> keys = ConcurrentHashMap.newKeySet();
        final int max = Math.min(ps.getMaximumParallelism(), 10);
        assertTrue(max >= multishardShards);

        ExecutorService executor = Executors.newFixedThreadPool(max);
        /* create a list of callables and start them at the same time */
        Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        for (int i = 0; i < max; i++) {
            final int opNum = i + 1;
            tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() {
                        doSubsetQuery(ps, max, opNum, keys, readKB);
                        return null;
                    }
                });
        }
        try {
            List<Future<Void>> futures = executor.invokeAll(tasks);
            for(Future<Void> f : futures) {
                f.get();
            }
        } catch (Exception e) {
            fail("Exception: " + e);
        }
        /* did all of the results get read and are they unique? */
        assertEquals(numRows, keys.size());
        assertEquals(numRows, readKB.get());
    }

    /*
     * Do a single portion (operation) of a parallel query
     */
    private void doSubsetQuery(PreparedStatement ps,
                               int numOperations,
                               int operationNumber,
                               Set<Integer> keys,
                               final AtomicInteger readKB) {
        QueryRequest qr = new QueryRequest().setPreparedStatement(ps);
        qr.setNumberOfOperations(numOperations);
        qr.setOperationNumber(operationNumber);
        QueryResult qres = null;
        do {
            qres = handle.query(qr);
            for (MapValue v : qres.getResults()) {
                keys.add(v.get("id").getInt());
            }
        } while (!qr.isDone());
        readKB.addAndGet(qres.getReadKB());
    }

    private void putRowsInParallelTable(PutRequest preq, int numRows) {
        for (int id = 0; id < numRows; id++) {
            MapValue row = new MapValue()
                .put("id", id)
                .put("name", ("name_" + id))
                .put("age", (id % 25));
            preq.setValue(row);
            PutResult pret = handle.put(preq);
            assertNotNull(pret.getVersion());
        }
    }

    /*
     * Return the max amount of parallelism
     */
    private int maxParallel(String query) {
        return handle.prepare(new PrepareRequest().setStatement(query))
            .getPreparedStatement().getMaximumParallelism();
    }

    private void failParallelQuery(QueryRequest qr,
                                   final String msg,
                                   Class<? extends Exception> expected) {
        try {
            handle.query(qr);
            fail("Expected exception on parallel query for : " +
                 msg);
        } catch (Exception e) {
            if (!expected.equals(e.getClass())) {
                fail("Unexpected exception. Expected " + expected + ", got " +
                     e + " for case: " + msg);
            }
        }
    }
}
