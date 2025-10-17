/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.fail;

import java.net.URL;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.impl.api.Request;
import oracle.kv.impl.api.RequestHandlerImpl;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.sna.ManagedRepNode;
import oracle.kv.impl.sna.ManagedService;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.util.kvlite.KVLite;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.proxy.security.SecureTestUtil;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;


/**
 * A Base to use for running tests with artificially injected latency.
 *
 * These tests only runs against a local server and not minicloud.
 *
 * The tests use a KVLite that has a test hook that injects
 * latencies (specified in latencySetUp) into all requests
 */
public class LatencyTestBase extends ProxyTestBase implements TestHook<Request> {

    private static int requestDelayMs;
    private static int previousRequestThreads;
    private static int previousPoolThreads;
    private static boolean previousUseThreads;
    private static boolean previousUseAsync;

    // note this hides the superclass static method so it won't be called
    // and we don't do anything, instead provide a method for subclasses to
    // call after their BeforeClass method.
    @BeforeClass
    public static void staticSetUp()
        throws Exception {
    }

    // This can be called in subclass BeforeClass methods
    public static void latencySetUp(boolean useAsync, int delayMs)
        throws Exception {

        requestDelayMs = delayMs;

        /*
         * Run kvlite in this jvm, so we can set a testHook on each
         * request to inject latency.
         * Note: it is currently impossible to create a thread-based kvlite
         * in anything other than a 1x1 (single node, single shard)
         * configuration. But that's OK for the purposes of this test.
         */
        previousUseThreads = Boolean.getBoolean(KVLITE_USETHREADS_PROP);
        System.setProperty(KVLITE_USETHREADS_PROP, "true");

        /*
         * configure the proxy to use sync or async calls (note this
         * overrides any given cmdline parameters, which will get reset after
         * this testcase finishes)
         */
        previousUseAsync = Boolean.getBoolean(PROXY_ASYNC_PROP);
        System.setProperty(PROXY_ASYNC_PROP, Boolean.toString(useAsync));

        /*
         * set the number of request processing threads very low, so
         * we can verify no additional latency when RCn > PTn
         * (RCn = number of concurrent requests)
         * (PTn = number of proxy request threads)
         *
         * store the old value so we can reset it, if it was set
         */
        previousRequestThreads = Integer.getInteger(
                                 PROXY_REQUEST_THREADS_PROP, 0);
        System.setProperty(PROXY_REQUEST_THREADS_PROP, "2");

        previousPoolThreads = Integer.getInteger(
            PROXY_REQUEST_POOL_SIZE_PROP, 0);
        System.setProperty(PROXY_REQUEST_POOL_SIZE_PROP, "0");

        /* this will silence stderr from kvlite */
        TestStatus.setActive(true);

        staticSetUp(tenantLimits);
    }

    @AfterClass
    public static void resetProperties() {
        System.setProperty(KVLITE_USETHREADS_PROP,
                           Boolean.toString(previousUseThreads));
        System.setProperty(PROXY_ASYNC_PROP,
                           Boolean.toString(previousUseAsync));
        if (previousRequestThreads <= 0) {
            System.clearProperty(PROXY_REQUEST_THREADS_PROP);
        } else {
            System.setProperty(PROXY_REQUEST_THREADS_PROP,
                               Integer.toString(previousRequestThreads));
        }
        System.setProperty(PROXY_REQUEST_POOL_SIZE_PROP,
                           Integer.toString(previousPoolThreads));
    }

    @Before
    public void asyncSetUp()
        throws Exception {

        // set a test hook such that every request takes at least Nms
        setRequestDelayHook(kvlite);
    }

    @Override
    public void doHook(Request r) {
        // this will be run at the beginning of each request in kvlite
        try {
            Thread.sleep(requestDelayMs);
        } catch (InterruptedException e) {
        }
    }

    /**
     * set a per-request test hook, if defined.
     */
    private void setRequestDelayHook(KVLite kvlite) {
        if (requestDelayMs <= 0) {
            return;
        }

        /*
         * KVLite runs one RN with useThreads=true. So we can get its
         * ManagedRepNode service from the static ManagedService class.
         */
        ManagedRepNode mrn = (ManagedRepNode)ManagedService.getMainService();
        if (mrn == null) {
            throw new RuntimeException(
               "Error: can't set request delay hook: no ManagedRepNode");
        }

        RepNodeService rns = mrn.getRepNodeService();
        if (rns == null) {
            throw new RuntimeException(
               "Error: can't set request delay hook: no RepNodeService");
        }

        RequestHandlerImpl rhi = rns.getReqHandler();
        if (rhi == null) {
            throw new RuntimeException(
               "Error: can't set request delay hook: no RequestHandlerImpl");
        }

        rhi.setTestHook(this);
        if (verbose) {
            System.out.println("Set request delay hook " +
                "for " + requestDelayMs + "ms delay on " +
                rns.getRepNodeParams().getRepNodeId().getFullName());
        }
    }


    protected static NoSQLHandle createClientHandleAndTestTable(
                              String tableName, int numThreads)
                              throws Exception {

        URL serviceURL =
            new URL("http", getProxyHost(), getProxyPort(), "/");
        NoSQLHandleConfig config = new NoSQLHandleConfig(serviceURL);
        SecureTestUtil.setAuthProvider(config, isSecure(),
                                       onprem, getTenantId());
        config.configureDefaultRetryHandler(0, 0);
        NoSQLHandle myhandle = NoSQLHandleFactory.createNoSQLHandle(config);

        /*
         * Create a simple table with an integer key and a single
         * string field
         */
        final String createTableStatement =
            "CREATE TABLE IF NOT EXISTS " + tableName +
            "(cookie_id LONG, audience_data STRING, PRIMARY KEY(cookie_id))";

        TableRequest tableRequest = new TableRequest()
            .setStatement(createTableStatement)
            .setTableLimits(new TableLimits(100000, 100000, 50));
        TableResult tres = myhandle.tableRequest(tableRequest);
        if (verbose) {
            System.out.println("Creating table " + tableName);
        }
        /*
         * The table request is asynchronous, so wait for the operation
         * to complete.
         */
        tres.waitForCompletion(myhandle,
                               60000, /* wait 60 sec */
                               100); /* delay ms for poll */
        if (verbose) {
            System.out.println("Created table " + tableName);
        }
        /*
         * Ideally this would be done earlier but at this time kv
         * requires that a table be created before the system table
         * is initialized. TODO: watch kv for changes in this area
         */
        waitForStoreInit(20); // wait 20s for init
        return myhandle;
    }

    protected static void dropTableAndCloseHandle(
                   NoSQLHandle myhandle, String tableName)
                   throws Exception {

        // drop the table
        if (verbose) {
            System.out.println("Dropping table " + tableName);
        }
        TableRequest tableRequest = new TableRequest()
            .setStatement("DROP TABLE IF EXISTS " + tableName);
        myhandle.tableRequest(tableRequest);

        // close handle
        if (verbose) {
            System.out.println("Closing handle...");
        }
        myhandle.close();
    }


    protected static class LatencyCollector {
        String ltype;
        long[] latencies;
        AtomicInteger sampleNum;
        AtomicLong total_us;

        LatencyCollector(String ltype, int numSamples) {
            this.ltype = ltype;
            this.latencies = new long[numSamples];
            this.sampleNum = new AtomicInteger(0);
            this.total_us = new AtomicLong(0);
        }

        void collect(long lat_ns) {
            if (lat_ns==0) return;
            int sample = sampleNum.incrementAndGet();
            latencies[sample % latencies.length] = lat_ns;
            total_us.addAndGet(lat_ns / 1000);
        }

        long avgLatencyUs() {
            return total_us.get() / totalSamples();
        }

        int totalSamples() {
            return sampleNum.get();
        }

        long avgLatencyMs() {
            return avgLatencyUs() / 1000;
        }

        void dumpLatencies() {
            int totalSamples = totalSamples();
            if (totalSamples > latencies.length) {
                totalSamples = latencies.length;
            }
            System.out.println("latencies: " +  totalSamples + " samples:");
            for (int i=0; i<totalSamples; i++) {
                System.out.println("  [" + latencies[i] + "]");
            }
        }

        long percentileLatencyUs(int pct) {
            int totalSamples = totalSamples();
            if (totalSamples > latencies.length) {
                totalSamples = latencies.length;
            }
            Arrays.sort(latencies, 0, totalSamples);
            if (pct >= 100) {
                return latencies[(totalSamples - 1)] / 1000;
            }
            return latencies[(totalSamples * pct) / 100] / 1000;
        }

        long percentileLatencyMs(int pct) {
            return percentileLatencyUs(pct) / 1000;
        }
    }

    protected static class RunConfig {
        NoSQLHandle handle;
        String tableName;
        int runSeconds;
        long maxID;
        int maxSize;
        int readTimeoutMs;
        int writeTimeoutMs;
        LatencyCollector readLatencyCollector;
        LatencyCollector writeLatencyCollector;
        LatencyCollector queryLatencyCollector;

        protected RunConfig(
            NoSQLHandle handle,
            String tableName,
            int runSeconds,
            long maxID,
            int maxSize,
            int readTimeoutMs,
            int writeTimeoutMs) {
                this.handle = handle;
                this.tableName = tableName;
                this.runSeconds = runSeconds;
                this.maxID = maxID;
                this.maxSize = maxSize;
                this.readTimeoutMs = readTimeoutMs;
                this.writeTimeoutMs = writeTimeoutMs;
                readLatencyCollector = new LatencyCollector("get", 100000);
                writeLatencyCollector = new LatencyCollector("put", 100000);
                queryLatencyCollector = new LatencyCollector("query", 100000);
        }
    }

    protected static void runClient(RunConfig rc, int get_pct, int put_pct) {
        try {
            runOneClient(rc, get_pct, put_pct);
        } catch (IOException e) {
        }
    }

    protected static void runOneClient(RunConfig rc, int get_pct, int put_pct)
        throws IOException {

        Random rand = new Random(System.currentTimeMillis());

        // generate random data for puts
        final int leftLimit = 32; // space
        final int rightLimit = 126; // tilde
        String generatedString = rand.ints(leftLimit, rightLimit + 1)
              .limit(rc.maxSize)
              .collect(StringBuilder::new,
                       StringBuilder::appendCodePoint, StringBuilder::append)
              .toString();

        MapValue value = new MapValue();
        MapValue key = new MapValue();

        PutRequest putRequest = new PutRequest()
            .setTableName(rc.tableName);
        putRequest.setTimeout(rc.writeTimeoutMs);
        GetRequest getRequest = new GetRequest()
            .setTableName(rc.tableName);
        getRequest.setTimeout(rc.readTimeoutMs);

        if (verbose) {
            System.out.println("Driver thread " +
                Thread.currentThread().getId() + " performing " +
                get_pct + "% get, " + put_pct + "% put operations...");
        }

        /* factor out proxy warmup for table and store */
        boolean done = false;
        while (!done) {
            try {
                key.put("cookie_id", 0L);
                getRequest.setKey(key);
                rc.handle.get(getRequest);
                done = true;
            } catch (Exception e) {
                // ignore
            }
        }

        long endMillis = System.currentTimeMillis() + (rc.runSeconds * 1000);

        while (System.currentTimeMillis() < endMillis) {

            boolean do_get = (rand.nextInt(100) > (100 - get_pct));
            boolean do_put = (rand.nextInt(100) > (100 - put_pct));

            // if neither, load next line and continue
            if (do_get==false && do_put==false) {
                continue;
            }

            // set up random data
            long id = rand.nextLong() % rc.maxID;

            if (do_put) {
                value.put("cookie_id", id);
                int begin = rand.nextInt(rc.maxSize / 4);
                int end = begin + rand.nextInt((rc.maxSize * 3) / 4);
                String sub = generatedString.substring(begin, end);
                value.put("audience_data", sub);

                long start = System.nanoTime();
                putRequest.setValue(value);
                try {
                    PutResult putRes = rc.handle.put(putRequest);
                    if (putRes.getVersion() == null) {
                        System.err.println("put failed!");
                    }
                } catch (Exception e) {
                    System.err.println(System.currentTimeMillis() + " PUT E");
                    if (verbose) {
                        System.err.println("  " + e);
                    }
                }
                long elapsed = System.nanoTime() - start;
                rc.writeLatencyCollector.collect(elapsed);
            }

            if (do_get) {
                long start = System.nanoTime();
                key.put("cookie_id", id);
                getRequest.setKey(key);
                try {
                    rc.handle.get(getRequest);
                } catch (Exception e) {
                    System.err.println(System.currentTimeMillis() + " GET E");
                    if (verbose) {
                        System.err.println("  " + e);
                    }
                }
                long elapsed = System.nanoTime() - start;
                rc.readLatencyCollector.collect(elapsed);
            }

        }

    }

    protected static void runQueries(RunConfig rc) {
        try {
            runOneQueryClient(rc);
        } catch (IOException e) {
        } catch (InterruptedException ie) {
            return;
        }
    }

    private static void runQuery(RunConfig rc, String query) {
        try {
            List<MapValue> allResults = new ArrayList<MapValue>();
            /* factor out the one-time cost of prepare */
            PrepareRequest preq = new PrepareRequest().setStatement(query);
            PrepareResult pres = rc.handle.prepare(preq);
            QueryRequest qreq = new QueryRequest().
                setPreparedStatement(pres.getPreparedStatement());
            long start = System.nanoTime();
            do {
                QueryResult qr = rc.handle.query(qreq);
                List<MapValue> results = qr.getResults();
                for (MapValue mv : results) {
                    // need to walk values, in case iteration triggers
                    // more requests internally
                    allResults.add(mv);
                }
            } while (!qreq.isDone());
            long elapsed = System.nanoTime() - start;
            rc.queryLatencyCollector.collect(elapsed);
            //System.err.println("query '" + query + "' ran to completion, " +
                //"numResults=" + allResults.size());
        } catch (RequestTimeoutException rte) {
            System.err.println("query '" + query + "' timed out: " + rte);
        } catch (Exception e) {
            System.err.println("query '" + query + "' got error: " + e);
        }
    }

    private static void runOneQueryClient(RunConfig rc)
        throws IOException, InterruptedException {

        if (verbose) {
            System.out.println("Driver thread " +
                Thread.currentThread().getId() +
                " performing query operations...");
        }

        long endMillis = System.currentTimeMillis() + (rc.runSeconds * 1000);

        while (System.currentTimeMillis() < endMillis) {

            // simple count
            runQuery(rc, "select count(*) from " + rc.tableName);

            // full scan/dump
            runQuery(rc, "select * from " + rc.tableName);

            // more complex, with sort
            runQuery(rc, "select audience_data from " + rc.tableName +
                " where cookie_id > 1000 and cookie_id < 10000" +
                " order by audience_data");

            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    private static void checkOpLatencies(
        long minLatencyMs, long maxLatencyMs,
        final String opType, LatencyCollector lc) {

        if (minLatencyMs <= 0 || maxLatencyMs <= 0) {
            return;
        }

        long latencyMs = lc.avgLatencyMs();
        if (latencyMs < minLatencyMs || latencyMs > maxLatencyMs) {
            if (verbose) {
                lc.dumpLatencies();
            }
            long max = lc.percentileLatencyMs(100);
            long lat99 = lc.percentileLatencyMs(99);
            long lat95 = lc.percentileLatencyMs(95);
            fail(opType + " latency of " + latencyMs +
                "ms is out of range.\n" +
                "Expected average latency is between " +
                    minLatencyMs + "ms and " +
                    maxLatencyMs + "ms. 95th=" + lat95 + " 99th=" + lat99 +
                    " max=" + max + " (samples=" + lc.totalSamples() + ")");
        }
    }

    protected static void testLatency(
            String tableName,
            int readThreads,
            int writeThreads,
            int rwThreads,
            int qThreads,
            int runSeconds,
            int minReadLatencyMs,
            int maxReadLatencyMs,
            int minWriteLatencyMs,
            int maxWriteLatencyMs,
            int minQueryLatencyMs,
            int maxQueryLatencyMs)
            throws Exception {

        // skip this test if running on minicloud
        assumeTrue(cloudRunning == false);

        /*
         * create threads, have them all hit the proxy as fast as
         * possible with get/put requests for about 10 seconds.
         * Verify that the resultant latency average is just over 100ms.
         * (in the sync case, this will be much higher)
         */

        final int totalThreads =
            readThreads + writeThreads + rwThreads + qThreads;

        NoSQLHandle myhandle =
            createClientHandleAndTestTable(tableName, totalThreads);

        RunConfig rc = new RunConfig(
            myhandle,
            tableName,
            runSeconds,
            10000 /*maxID*/,
            5000 /*maxSize*/,
            2000 /*readTimeoutMs*/,
            2000 /*writeTimeoutMs*/);

        Thread threads[] = new Thread[totalThreads];

        if (qThreads == totalThreads) {
            // run puts to prepopulate data
            for(int x=0; x<qThreads; x++) {
                threads[x] = new Thread(() -> {runClient(rc, 0, 100);});
                threads[x].start();
            }
            for(int x=0; x<qThreads; x++) {
                  threads[x].join();
            }
        }


        int numThreads = 0;
        for(int x=0; x<readThreads; x++) {
            threads[numThreads] = new Thread(() -> {runClient(rc, 100, 0);});
            threads[numThreads].start();
            numThreads++;
        }
        for(int x=0; x<writeThreads; x++) {
            threads[numThreads] = new Thread(() -> {runClient(rc, 0, 100);});
            threads[numThreads].start();
            numThreads++;
        }
        for(int x=0; x<rwThreads; x++) {
            threads[numThreads] = new Thread(() -> {runClient(rc, 50, 50);});
            threads[numThreads].start();
            numThreads++;
        }
        for(int x=0; x<qThreads; x++) {
            threads[numThreads] = new Thread(() -> {runQueries(rc);});
            threads[numThreads].start();
            numThreads++;
        }

        // wait for threads to finish
        for(int x=0; x<threads.length; x++) {
              threads[x].join();
        }

        dropTableAndCloseHandle(myhandle, tableName);


        // Verify latencies
        checkOpLatencies(minReadLatencyMs, maxReadLatencyMs,
            "Read", rc.readLatencyCollector);
        checkOpLatencies(minWriteLatencyMs, maxWriteLatencyMs,
            "Write", rc.writeLatencyCollector);
        checkOpLatencies(minQueryLatencyMs, maxQueryLatencyMs,
            "Query", rc.queryLatencyCollector);

        if (verbose == false) {
            return;
        }

        if (readThreads > 0 || rwThreads > 0) {
            System.out.println("average latency for get ops: " +
                rc.readLatencyCollector.avgLatencyMs() + "ms");
            System.out.println("99th percentile latency for get ops: " +
                rc.readLatencyCollector.percentileLatencyMs(99) + "ms");
        }
        if (writeThreads > 0 || rwThreads > 0) {
            System.out.println("average latency for put ops: " +
                rc.writeLatencyCollector.avgLatencyMs() + "ms");
            System.out.println("99th percentile latency for put ops: " +
                rc.writeLatencyCollector.percentileLatencyMs(99) + "ms");
        }
        if (qThreads > 0) {
            System.out.println("average latency for query ops: " +
                rc.queryLatencyCollector.avgLatencyMs() + "ms");
            System.out.println("99th percentile latency for query ops: " +
                rc.queryLatencyCollector.percentileLatencyMs(99) + "ms");
        }

    }
}
