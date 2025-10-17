/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.fail;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.RetryStats;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.proxy.security.SecureTestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for distributed rate limiting.
 *
 * These tests use many client threads and multiple
 * client handles to simulate many different clients.
 * In all cases, overall read/write units are maintained for
 * each table used, and compared periodically and at the
 * end of each test to verify they are reasonably close to
 * the specified table limits.
 * They also verify that there are very few or no throttling
 * exceptions.
 *
 * Note that this test does NOT use driver-side rate limiters.
 * TODO: add that as a config, verify use of both works OK
 */
public class DistributedRateLimitingTest extends ProxyTestBase {

    protected static int numTables = Integer.getInteger("test.numtables", 3);
    protected static int baseUnits = Integer.getInteger("test.baseunits", 10);
    protected static Random rand = new Random(System.currentTimeMillis());
    protected static int maxRowSize = 20000;
    protected static int maxRows = 10000;
    protected static int readTimeoutMs = 3000;
    protected static int writeTimeoutMs = 3000;

    protected static int writerIntervalMs =
                         Integer.getInteger("test.outintervalms", 1000);
    protected static PrintWriter printWriter;
    static {
        String outFile = System.getProperty("test.outfile");
        if (outFile == null) {
            printWriter = null;
        } else {
            try {
                printWriter = new PrintWriter(new FileWriter(outFile));
            } catch (Exception e) {
                printWriter = null;
            }
        }
    }

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        assumeTrue("Skipping DistributedRateLimitingTest for minicloud or " +
                   "cloud or onprem runs",
                   !Boolean.getBoolean(USEMC_PROP) &&
                   !Boolean.getBoolean(USECLOUD_PROP) &&
                   !Boolean.getBoolean(ONPREM_PROP));

        staticSetUp(tenantLimits);
    }

    /* note this overrides base class @Before */
    @Override
    @Before
    public void setUp() throws Exception {

        /*
         * Configure the endpoint
         */
        if (handles == null) {
            handles = new NoSQLHandle[numProxies];
            for (int x=0; x<numProxies; x++) {
                handles[x] = configHandle(getProxyEndpoint(x));
            }
            createTables();
            populateTables();
            setTableLimits();
        }

    }

    /* note this overrides base class @After */
    @Override
    @After
    public void tearDown() throws Exception {
        if (handles != null) {
            /* dropTables(handles); */
        }
    }

    @Override
    protected NoSQLHandle setupHandle(NoSQLHandleConfig hconfig) {
        /* up to 5 retries, 100ms fixed delay */
        hconfig.configureDefaultRetryHandler(5, 100);
        hconfig.setNumThreads(20); /* TODO */
        hconfig.setConnectionPoolMinSize(5);
        SecureTestUtil.setAuthProvider(hconfig, false,
                                       onprem, "RLTestTenant");
        return getHandle(hconfig);
    }

    /* ThroughPutCollector */
    protected static class TPCollector {
        AtomicLong currentUnits;
        AtomicLong totalUnits;
        AtomicInteger totalErrors; // throughput exceptions
        long startedMs;
        long currentMs;

        TPCollector() {
            this.startedMs = System.currentTimeMillis();
            this.currentMs = this.startedMs;
            this.totalUnits = new AtomicLong(0);
            this.currentUnits = new AtomicLong(0);
            this.totalErrors = new AtomicInteger(0);
        }

        void collect(int units) {
            totalUnits.addAndGet((long)units);
            currentUnits.addAndGet((long)units);
        }

        void addError() {
            totalErrors.addAndGet(1);
        }

        void addErrors(int errs) {
            totalErrors.addAndGet(errs);
        }

        int getErrors() {
            return totalErrors.get();
        }

        double getOverallRate() {
            long diff = System.currentTimeMillis() - startedMs;
            if (diff <= 0) diff = 1;
            return ((double)totalUnits.get() * 1000) / (double)diff;
        }

        double getCurrentRate() {
            long diff = System.currentTimeMillis() - currentMs;
            if (diff <= 0) diff = 1;
            return ((double)currentUnits.get() * 1000) / (double)diff;
        }

        void reset(boolean all) {
            currentUnits.set(0);
            currentMs = System.currentTimeMillis();
            if (all) {
                totalErrors.set(0);
                totalUnits.set(0);
                this.startedMs = System.currentTimeMillis();
            }
        }
    }

    protected static void verbose(String msg) {
        if (verbose) {
            long millis = System.currentTimeMillis() % 100000;
            System.out.println(millis + " " + msg);
        }
    }

    protected static int getTableUnits(int tableNum) {
        int units = baseUnits;
        for (int x=0; x<tableNum; x++, units *= 10);
        return units;
    }


    protected static void populateTables()
        throws IOException, InterruptedException {

        verbose("Starting population threads...");
        Thread threads[] = new Thread[numTables];
        for (int x=0; x<numTables; x++) {
            final int tNum = x;
            threads[x] = new Thread(() -> {doPopulateThread(tNum);});
            threads[x].start();
        }
        /* wait for threads to finish */
        verbose("Waiting for population threads to finish...");
        for(int x=0; x<numTables; x++) {
              threads[x].join();
        }
    }

    protected static void doPopulateThread(int tableNum) {
        try {
            doPopulation(tableNum);
        } catch (IOException e) {
        }
    }

    protected static void doPopulation(int tableNum)
        throws IOException {

        /* generate random data for puts */
        final int leftLimit = 32; /* space */
        final int rightLimit = 126; /* tilde */
        String generatedString = rand.ints(leftLimit, rightLimit + 1)
              .limit(maxRowSize+1)
              .collect(StringBuilder::new,
                       StringBuilder::appendCodePoint, StringBuilder::append)
              .toString();

        MapValue value = new MapValue();
        PutRequest putRequest = new PutRequest();
        putRequest.setTimeout(writeTimeoutMs);

        /* populate 3/4ths of rows */
        long maxID = (long)((maxRows * 3) / 4);
        for(long id=0; id < maxID; id++) {
            /* set up random data */
            value.put("cookie_id", id);
            int begin = 0;
            int end = begin + rand.nextInt(maxRowSize);
            String sub = generatedString.substring(begin, end);
            value.put("audience_data", sub);

            NoSQLHandle handle = handles[rand.nextInt(handles.length)];
            putRequest.setTableName("RLTable" + tableNum);
            putRequest.setValue(value);
            try {
                PutResult putRes = handle.put(putRequest);
                if (putRes.getVersion() == null) {
                    verbose("put failed!");
                }
            } catch (Exception e) {
                verbose("  " + e);
            }
            if ((id % 1000) == 0) {
                verbose("T" + tableNum + " rows=" + id);
            }
        }
    }

    protected static void createTables() {
        /*
         * Create simple tables with an integer key and a single
         * string field, with various limits
         */
        for (int x=0; x<numTables; x++) {

            final String createTableStatement =
                "CREATE TABLE IF NOT EXISTS RLTable" + x +
                "(cookie_id LONG, audience_data STRING, PRIMARY KEY(cookie_id))";

            final int units = getTableUnits(x);
            TableRequest tableRequest = new TableRequest()
                .setStatement(createTableStatement)
                .setTableLimits(new TableLimits(20000, 20000, 5));
            NoSQLHandle handle = handles[(x % handles.length)];
            TableResult tres = handle.tableRequest(tableRequest);
            verbose("Creating table RLTable" + x);
            /*
             * The table request is asynchronous, so wait for the operation
             * to complete.
             */
            tres.waitForCompletion(handle,
                                   60000, /* wait 60 sec */
                                   100); /* delay ms for poll */
            verbose("Created table RLTable" + x);
        }
    }

    protected static void dropTables(NoSQLHandle[] handles)
                   throws Exception {
        for (int x=0; x<numTables; x++) {
            /* drop the table */
            verbose("Dropping table RLTable" + x);
            TableRequest tableRequest = new TableRequest()
                .setStatement("DROP TABLE IF EXISTS RLTable" + x);
            NoSQLHandle handle = handles[(x % handles.length)];
            handle.tableRequest(tableRequest);
        }
    }


    protected static void setTableLimits() {
        for (int x=0; x<numTables; x++) {
            final String tableName = "RLTable" + x;
            final int units = getTableUnits(x);
            TableRequest tableRequest = new TableRequest()
                .setTableName(tableName)
                .setTableLimits(new TableLimits(units, units, 5));
            NoSQLHandle handle = handles[(x % handles.length)];
            TableResult tres = handle.tableRequest(tableRequest);
            verbose("Updating table RLTable" + x + " with " +
                    "RUs/WUs=" + units);
            /*
             * The table request is asynchronous, so wait for the operation
             * to complete.
             */
            tres.waitForCompletion(handle,
                                   60000, /* wait 60 sec */
                                   100); /* delay ms for poll */
        }
    }

    protected static class RunConfig {
        int runSeconds;
        long maxID;
        int maxSize;
        int readTimeoutMs;
        int writeTimeoutMs;
        PrintWriter writer;
        int writeIntervalMs;
        TPCollector[] readCollectors;
        TPCollector[] writeCollectors;
        boolean preferThrottling;

        protected RunConfig(
            int runSeconds,
            long maxID,
            int maxSize,
            int readTimeoutMs,
            int writeTimeoutMs,
            PrintWriter writer,
            int writeIntervalMs,
            TPCollector[] readCollectors,
            TPCollector[] writeCollectors,
            boolean preferThrottling) {
                this.runSeconds = runSeconds;
                this.maxID = maxID;
                this.maxSize = maxSize;
                this.writer = writer;
                this.writeIntervalMs = writeIntervalMs;
                this.readTimeoutMs = readTimeoutMs;
                this.writeTimeoutMs = writeTimeoutMs;
                this.readCollectors = readCollectors;
                this.writeCollectors = writeCollectors;
                this.preferThrottling = preferThrottling;
        }
    }

    protected static void collecterWatcher(RunConfig rc) {
        long endMillis = System.currentTimeMillis() +
                         (rc.runSeconds * 1000) + 1000;
        if (rc.writer != null) {
            rc.writer.printf("===== file\n");
            rc.writer.printf("time");
            for (int x=0; x<numTables; x++) {
                int units = getTableUnits(x);
                rc.writer.printf(",%dR,%dW", units, units);
            }
            rc.writer.printf("\n");
        }
        int ms = 0;
        while (System.currentTimeMillis() < endMillis) {
            try {
                TimeUnit.MILLISECONDS.sleep(rc.writeIntervalMs);
            } catch (Exception e) {
                break;
            }
            if (rc.writer != null) {
                rc.writer.printf("%d,", ms);
                ms += rc.writeIntervalMs;
            }
            for (int x=0; x<numTables; x++) {
                double rrate = rc.readCollectors[x].getCurrentRate();
                double wrate = rc.writeCollectors[x].getCurrentRate();
                rc.readCollectors[x].reset(false);
                rc.writeCollectors[x].reset(false);
                if (rc.writer == null) {
                    verbose("RR[" + x + "] = " + rrate);
                    verbose("WR[" + x + "] = " + wrate);
                } else {
                    if (x>0) rc.writer.printf(",");
                    rc.writer.printf("%.2f,%.2f", rrate, wrate);
                }
            }
            if (rc.writer != null) {
                rc.writer.printf("\n");
                rc.writer.flush();
            }
        }
    }

    protected static void doReadThread(RunConfig rc, int tableNum) {
        try {
            doReads(rc, tableNum);
        } catch (IOException e) {
        }
    }

    protected static void doReads(RunConfig rc, int tableNum)
        throws IOException {

        MapValue key = new MapValue();
        GetRequest getRequest = new GetRequest();
        getRequest.setTimeout(rc.readTimeoutMs);
        if (rc.preferThrottling) {
            if (setPreferThrottling(getRequest) == false) {
                return;
            }
        }

        long endMillis = System.currentTimeMillis() + (rc.runSeconds * 1000);

        while (System.currentTimeMillis() < endMillis) {
            long id = rand.nextLong() % rc.maxID;
            key.put("cookie_id", id);
            getRequest.setTableName("RLTable" + tableNum);
            getRequest.setKey(key);
            NoSQLHandle handle = handles[rand.nextInt(handles.length)];
            try {
                GetResult getRes = handle.get(getRequest);
                rc.readCollectors[tableNum].collect(getRes.getReadUnits());
                rc.writeCollectors[tableNum].collect(getRes.getWriteUnits());
            } catch (Exception e) {
                verbose("  " + e);
                if (e instanceof ReadThrottlingException) {
                    rc.readCollectors[tableNum].addError();
                }
            }
            RetryStats rs = getRequest.getRetryStats();
            if (rs != null) {
                rc.readCollectors[tableNum].addErrors(
                    rs.getNumExceptions(ReadThrottlingException.class));
            }
        }

    }

    protected static void doWriteThread(RunConfig rc, int tableNum) {
        try {
            doWrites(rc, tableNum);
        } catch (IOException e) {
        }
    }

    protected static void doWrites(RunConfig rc, int tableNum)
        throws IOException {

        /* generate random data for puts */
        final int leftLimit = 32; /* space */
        final int rightLimit = 126; /* tilde */
        String generatedString = rand.ints(leftLimit, rightLimit + 1)
              .limit(rc.maxSize+1)
              .collect(StringBuilder::new,
                       StringBuilder::appendCodePoint, StringBuilder::append)
              .toString();

        MapValue value = new MapValue();
        PutRequest putRequest = new PutRequest();
        putRequest.setTimeout(rc.writeTimeoutMs);
        if (rc.preferThrottling) {
            if (setPreferThrottling(putRequest) == false) {
                return;
            }
        }

        long endMillis = System.currentTimeMillis() + (rc.runSeconds * 1000);

        while (System.currentTimeMillis() < endMillis) {
            /* set up random data */
            long id = rand.nextLong() % rc.maxID;

            value.put("cookie_id", id);
            int begin = 0;
            int end = begin + rand.nextInt(rc.maxSize);
            //int begin = rand.nextInt(rc.maxSize / 4);
            //int end = begin + rand.nextInt((rc.maxSize * 3) / 4);
            String sub = generatedString.substring(begin, end);
            value.put("audience_data", sub);

            putRequest.setTableName("RLTable" + tableNum);
            putRequest.setValue(value);
            NoSQLHandle handle = handles[rand.nextInt(handles.length)];
            try {
                PutResult putRes = handle.put(putRequest);
                if (putRes.getVersion() == null) {
                    verbose("put failed!");
                }
                rc.readCollectors[tableNum].collect(putRes.getReadUnits());
                rc.writeCollectors[tableNum].collect(putRes.getWriteUnits());
            } catch (Exception e) {
                verbose("  " + e);
                if (e instanceof WriteThrottlingException) {
                    rc.writeCollectors[tableNum].addError();
                }
            }
            RetryStats rs = putRequest.getRetryStats();
            if (rs != null) {
                rc.writeCollectors[tableNum].addErrors(
                    rs.getNumExceptions(WriteThrottlingException.class));
            }
        }
    }

    protected static void doQueryThread(RunConfig rc, int tableNum) {
        try {
            runOneQueryClient(rc, tableNum);
        } catch (IOException e) {
        } catch (InterruptedException ie) {
            return;
        }
    }

    private static void runQuery(RunConfig rc,
                                 String query,
                                 int tableNum,
                                 NoSQLHandle handle,
                                 long endMillis) {
        QueryRequest qreq = null;
        try {
            List<MapValue> allResults = new ArrayList<MapValue>();
/* TODO: in proxy: check current rate for table. if over, reduce maxReadKB */
            int maxKB = getTableUnits(tableNum) / 10;
            if (maxKB < 5) maxKB = 5;
            qreq = new QueryRequest().setStatement(query)
                                     .setTimeout(10000)
                                     .setMaxReadKB(maxKB);
            if (rc.preferThrottling) {
                if (setPreferThrottling(qreq) == false) {
                    return;
                }
            }
            do {
                QueryResult qr = handle.query(qreq);
                List<MapValue> results = qr.getResults();
                for (MapValue mv : results) {
                    /* need to walk values, in case iteration triggers */
                    /* more requests internally */
                    allResults.add(mv);
                }
                rc.readCollectors[tableNum].collect(qr.getReadUnits());
                rc.writeCollectors[tableNum].collect(qr.getWriteUnits());
                /* this must be called _after_ getResults() */
                RetryStats rs = qr.getRetryStats();
                if (rs != null) {
                    int ne = rs.getNumExceptions(ReadThrottlingException.class);
                    if (ne > 0) {
                        rc.readCollectors[tableNum].addErrors(ne);
                    }
                }
                if (System.currentTimeMillis() > endMillis) {
                    break;
                }
            } while (!qreq.isDone());
        } catch (RequestTimeoutException rte) {
            verbose("query '" + query + "' timed out: " + rte);
        } catch (Exception e) {
            verbose("query '" + query + "' got error: " + e);
            RetryStats rs = qreq.getRetryStats();
            if (rs != null) {
                int ne = rs.getNumExceptions(ReadThrottlingException.class);
                if (ne > 0) {
                    rc.readCollectors[tableNum].addErrors(ne);
                }
            }
        }
    }


    private static void runOneQueryClient(RunConfig rc, int tableNum)
        throws IOException, InterruptedException {

        verbose("Driver thread " + Thread.currentThread().getId() +
                " performing query operations...");

        long endMillis = System.currentTimeMillis() + (rc.runSeconds * 1000);

        NoSQLHandle handle;

        while (System.currentTimeMillis() < endMillis) {

            /* simple count */
            handle = handles[rand.nextInt(handles.length)];
            runQuery(rc, "select count(*) from RLTable" + tableNum,
                     tableNum, handle, endMillis);

            /* full scan/dump */
            handle = handles[rand.nextInt(handles.length)];
            runQuery(rc, "select * from RLTable" + tableNum,
                     tableNum, handle, endMillis);

            /* more complex, with sort */
            handle = handles[rand.nextInt(handles.length)];
            runQuery(rc, "select audience_data from RLTable" + tableNum +
                " where cookie_id > 1000 and cookie_id < 10000" +
                " order by audience_data", tableNum, handle, endMillis);
        }
    }

    protected static void runTest(
            int readThreads,
            int writeThreads,
            int qThreads,
            int runSeconds)
            throws Exception {

        /* skip this test if running on minicloud */
        assumeTrue(cloudRunning == false);

        boolean preferThrottling = Boolean.getBoolean("test.preferthrottling");

        final int totalThreads =
            readThreads + writeThreads + qThreads;

        TPCollector[] readCollectors = new TPCollector[numTables];
        TPCollector[] writeCollectors = new TPCollector[numTables];
        for (int x=0; x<numTables; x++) {
            readCollectors[x] = new TPCollector();
            writeCollectors[x] = new TPCollector();
        }
        RunConfig rc = new RunConfig(
            runSeconds,
            maxRows /*maxID*/,
            maxRowSize /*maxSize*/,
            readTimeoutMs,
            writeTimeoutMs,
            printWriter,
            writerIntervalMs,
            readCollectors,
            writeCollectors,
            preferThrottling);

        /* sleep for 1 second to accrue credit in rate limiters */
        TimeUnit.MILLISECONDS.sleep(1000);

        Thread threads[] = new Thread[totalThreads + 1];

        int numThreads = 0;
        /* fire off watcher thread to poll rates */
        threads[numThreads] = new Thread(() -> {collecterWatcher(rc);});
        threads[numThreads].start();
        numThreads++;

        for(int x=0; x<writeThreads; x++) {
            final int tNum = x % numTables;
            threads[numThreads] = new Thread(() -> {doWriteThread(rc, tNum);});
            threads[numThreads].start();
            numThreads++;
        }
        for(int x=0; x<readThreads; x++) {
            final int tNum = x % numTables;
            threads[numThreads] = new Thread(() -> {doReadThread(rc, tNum);});
            threads[numThreads].start();
            numThreads++;
        }
        for(int x=0; x<qThreads; x++) {
            final int tNum = x % numTables;
            threads[numThreads] = new Thread(() -> {doQueryThread(rc, tNum);});
            threads[numThreads].start();
            numThreads++;
        }

        /* wait for threads to finish */
        for(int x=0; x<numThreads; x++) {
              threads[x].join();
        }

        /* check resultant rates */
        /* TODO: per second, max, min, burst, etc */
        StringBuilder sb = new StringBuilder();
        for (int x=0; x<numTables; x++) {
            int units = getTableUnits(x);
            double max = (double)units * 1.5;
            double min = (double)units * 0.5;
            /* allow a bit more error for very low limits */
            if (units < 20) {
                max = max * 3;
            }
            if (readThreads > 0 || qThreads > 0) {
                double RUs = readCollectors[x].getOverallRate();
                System.out.println("RUs=" + getTableUnits(x) + " actual=" + RUs);
                System.out.println("  read throttling errors = " +
                        readCollectors[x].getErrors());
                if (RUs > max || RUs < min) {
                    sb.append("RUs for " + getTableUnits(x) +
                              "RUs table failed: " + "min=" + min + ", max=" +
                              max + ", actual=" + RUs + "\n");
                }
            }
            if (writeThreads > 0) {
                double WUs = writeCollectors[x].getOverallRate();
                System.out.println("WUs=" + getTableUnits(x) + " actual=" + WUs);
                System.out.println("  write throttling errors = " +
                        writeCollectors[x].getErrors());
                if (WUs > max || WUs < min) {
                    sb.append("WUs for " + getTableUnits(x) +
                              "WUs table failed: " + "min=" + min + ", max=" +
                              max + ", actual=" + WUs + "\n");
                }
// TODO: error counts, maybe less than 1/sec? some threshold?
// Only if preferThrottling == false
            }
        }
        if (sb.length() > 0) {
            fail(sb.toString());
        }
    }

    private static boolean setPreferThrottling(Request req) {
        Class<?> requestClass = null;
        try {
            requestClass = Class.forName("oracle.nosql.driver.ops.Request");
        } catch (Throwable e) {
            System.out.println("Could not find Request class:" + e);
            return false;
        }
        Method setThrottleFunction = null;
        try {
            setThrottleFunction = requestClass.getMethod(
                                      "setPreferThrottlingExceptions",
                                      boolean.class);
        } catch (Throwable e) {
            verbose("Could not find " +
                    "Request.setPreferThrottlingExceptions(): " + e);
            verbose("Skipping test");
            return false;
        }
        try {
            setThrottleFunction.invoke(req, true);
        } catch (Exception e) {
            verbose("Could not invoke " +
                    "Request.setPreferThrottlingExceptions(): " + e);
            verbose("Skipping test");
            return false;
        }
        return true;
    }

    @Test
    public void basicWriteTest() throws Exception {
        runTest(0, 15, 0, 15);
    }

    @Test
    public void basicReadTest() throws Exception {
        runTest(numTables * 5, 0, 0, 15);
    }

    @Test
    public void basicReadWriteTest() throws Exception {
        runTest(numTables * 5, numTables * 5, 0, 15);
    }

    @Test
    public void basicQueryTest() throws Exception {
        runTest(0, 0, numTables * 5, 20);
    }

    @Test
    public void readWriteQueryTest() throws Exception {
        runTest(numTables * 4, numTables * 4, numTables * 4, 30);
    }
}
