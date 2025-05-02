/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.async.perf;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import java.util.logging.FileHandler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.impl.async.EndpointGroup;
import oracle.kv.impl.async.IOBufferPool;
import oracle.kv.impl.async.dialog.nio.NioChannelThreadPoolPerfTracker;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.WatcherNames;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.stats.KVStats;
import oracle.kv.util.CreateStore;
import oracle.nosql.common.contextlogger.LogFormatter;
import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.sklogger.measure.LatencyElement;
import oracle.nosql.common.sklogger.measure.ThroughputElement;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Tests the perf metrics of the async transport layer.
 */
public class AsyncTransportPerfTest extends TestBase {

    private static final String KVSTATS_MONITOR_LOG =
        (new File(TestUtils.getTestDir(), "kvstats.log")).toString();
    private static final long logIntervalMillis = 1000;
    private static final int latencyBreakdownSampleRate = 1;
    private static final int defaultInfoRecordSampleRate =
        DialogEndpointPerfTracker.INFO_RECORD_INITIAL_SAMPLE_RATE;
    private static final int infoRecordSampleRate = 1;
    private static final int handlerLatencySampleRate = 1;

    private CreateStore createStore;
    private KVStore kvstore;
    private FileHandler fileHandler;

    /**
     * Only runs the test cases of this suite when the async is enabled.
     */
    @BeforeClass
    public static void ensureAsyncEnabled() {
        assumeTrue("Requires async", AsyncControl.serverUseAsync);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        /* Adjust the sample rates so that we have enough sample */
        System.setProperty(
            DialogEndpointPerfTracker.LATENCY_BREAKDOWN_SAMPLE_RATE,
            Integer.toString(latencyBreakdownSampleRate));
        DialogEndpointPerfTracker.INFO_RECORD_INITIAL_SAMPLE_RATE =
            infoRecordSampleRate;
        System.setProperty(
            NioEndpointHandlerPerfTracker.LATENCY_SAMPLE_RATE,
            Integer.toString(handlerLatencySampleRate));
        DialogEndpointGroupPerfTracker.enableRateLimiting = false;
        NioChannelThreadPoolPerfTracker.enableRateLimiting = false;
        createStore();
        createClient();
    }

    private void createStore() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000, /* startPort */
                                      1, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      10, /* numPartitions */
                                      1, /* capacity */
                                      CreateStore.MB_PER_SN, /* memoryMB */
                                      true, /* useThreads */
                                      null, /* mgmtImpl */
                                      false, /* mgmtPortsShared */
                                      false); /* secure */
        createStore.start();
    }

    private void createClient() throws Exception {
        final KVStoreConfig kvConfig =
            createStore.createKVConfig(false /* secure */);
        kvConfig.setEnableStatsMonitor(true);
        kvConfig.setStatsMonitorLogIntervalMillis(logIntervalMillis);
        kvstore = KVStoreFactory.getStore(kvConfig);
        final Logger kvStatsMonitorLogger =
            Logger.getLogger(KVStats.class.getName());
        if (fileHandler != null) {
            fileHandler.close();
        }
        fileHandler =
            new FileHandler(KVSTATS_MONITOR_LOG, 0, 1, false);
        fileHandler.setFormatter(new LogFormatter());
        kvStatsMonitorLogger.addHandler(fileHandler);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (kvstore != null) {
            kvstore.close();
            kvstore = null;
        }
        if (createStore != null) {
            createStore.shutdown();
            createStore = null;
        }
        if (fileHandler != null) {
            fileHandler.close();
            fileHandler = null;
        }
        final EndpointGroup endpointGroup =
            AsyncRegistryUtils.getEndpointGroupOrNull();
        if (endpointGroup != null) {
            endpointGroup.getDialogEndpointGroupPerfTracker()
                .obtain(WatcherNames.getKVStoreGetStatsWatcherName(),
                        true);
            endpointGroup.getDialogEndpointGroupPerfTracker()
                .obtain(WatcherNames.KVSTATS_MONITOR,
                        true);
        }
        NioChannelThreadPoolPerfTracker.enableRateLimiting = true;
        DialogEndpointGroupPerfTracker.enableRateLimiting = true;
        System.clearProperty(
            DialogEndpointPerfTracker.LATENCY_BREAKDOWN_SAMPLE_RATE);
        DialogEndpointPerfTracker.INFO_RECORD_INITIAL_SAMPLE_RATE =
            defaultInfoRecordSampleRate;
        System.clearProperty(
            NioEndpointHandlerPerfTracker.LATENCY_SAMPLE_RATE);
    }

    /**
     * Tests the endpoint metric counts and latency stats are approximately
     * right.
     */
    @Test
    public void testDialogEndpointGroupCountAndLatency() throws Exception {
        final int niter = 4;
        final int numPutRequests = 128;
        final int sleepTimeMs = 1000;
        for (int i = 0; i < niter; ++i) {
            kvstore.getStats(true);
            for (int j = 0; j < numPutRequests; ++j) {
                kvstore.put(
                    Key.createKey(String.format("key-CL-%s.%s", i, j)),
                    Value.createValue(
                        String.format("value-CL-%s.%s", i, j).getBytes()));
            }
            Thread.sleep(sleepTimeMs);
            final ObjectNode dialogEndpointGroupPerf =
                kvstore.getStats(true /* clear */).getDialogEndpointGroupPerf();
            checkGroupMetrics(
                dialogEndpointGroupPerf,
                (m) -> checkMetricsCountAndLatency(
                    m, numPutRequests, sleepTimeMs));
        }
    }

    private void checkGroupMetrics(ObjectNode payload,
                                   Consumer<ObjectNode> metricsChecker) {
        final ObjectNode creatorPerfs = payload
            .get(DialogEndpointGroupPerf.PerfValue.CREATOR_PERFS)
            .asObject();
        creatorPerfs.entrySet().stream()
            .filter(e -> !e.getKey().contains(":TEST"))
            .filter(e -> !e.getKey().contains(":ADMIN"))
            .filter(e -> e.getKey().contains("rg1-rn1"))
            .map(e -> e.getValue().asObject())
            .forEach(metricsChecker);
    }

    private void checkMetricsCountAndLatency(ObjectNode perfJson,
                                             int numPutRequests,
                                             int sleepTimeMs) {
        final DialogEndpointPerf perf = new DialogEndpointPerf(perfJson);
        final double expectedThroughput =
            numPutRequests / (sleepTimeMs / 1000.0);
        assertThroughputEquals(perfJson, expectedThroughput,
                               perf.getDialogStartThroughput());
        assertThroughputEquals(perfJson, 0.0, perf.getDialogDropThroughput());
        assertThroughputEquals(perfJson, expectedThroughput,
                               perf.getDialogFinishThroughput());
        assertThroughputEquals(perfJson, 0.0,
                               perf.getDialogAbortThroughput());
        assertEquals(perfJson.toString(),
                     numPutRequests,
                     perf.getFinishedDialogLatency().getOperationCount(),
                     numPutRequests * 0.5);
        assertEquals(
            perfJson.toString(), 100.0,
            convert(perf.getFinishedDialogLatency().getMin()), 100.0);
        assertEquals(
            perfJson.toString(), 100.0,
            convert(perf.getFinishedDialogLatency().getMax()), 500.0);
        assertEquals(
            perfJson.toString(), 100.0,
            convert(perf.getFinishedDialogLatency().getAverage()), 100.0);
        assertEquals(
            perfJson.toString(), 100.0,
            convert(perf.getFinishedDialogLatency().getPercent95()), 500.0);
        assertEquals(
            perfJson.toString(), 100.0,
            convert(perf.getFinishedDialogLatency().getPercent99()), 500.0);
        assertEquals(perfJson.toString(),
                     perf.getAbortedDialogLatency().getOperationCount(), 0);
        assertEquals(perfJson.toString(),
                     perf.getAbortedDialogLatency().getMin(), 0);
        assertEquals(perfJson.toString(),
                     perf.getAbortedDialogLatency().getMax(), 0);
        assertEquals(perfJson.toString(),
                     perf.getAbortedDialogLatency().getAverage(), 0);
        assertEquals(perfJson.toString(),
                     perf.getAbortedDialogLatency().getPercent95(), 0);
        assertEquals(perfJson.toString(),
                     perf.getAbortedDialogLatency().getPercent99(), 0);
        assertEquals(
            perfJson.toString(), numPutRequests,
            perf.getDialogConcurrency().getCount(), numPutRequests * 0.5);
        assertEquals(perfJson.toString(),
                     1, perf.getDialogConcurrency().getMin(), 1);
        assertEquals(perfJson.toString(),
                     1, perf.getDialogConcurrency().getMax(), 5);
        assertEquals(perfJson.toString(),
                     1.0, perf.getDialogConcurrency().getAverage(), 1);
        assertEquals(perfJson.toString(),
                     1, perf.getDialogConcurrency().getPercent95(), 5);
        assertEquals(perfJson.toString(),
                     1, perf.getDialogConcurrency().getPercent99(), 5);
    }

    private void assertThroughputEquals(ObjectNode perfJson,
                                        double expected,
                                        ThroughputElement.Result result) {
        assertEquals(perfJson.toString(),
                     expected,
                     result.getThroughputPerSecond(),
                     0.5 * expected);
    }

    /**
     * Convers nanos to millis.
     */
    private static long convert(long nanos) {
        return NANOSECONDS.toMillis(nanos);
    }

    /**
     * Tests the dialog event perf display.
     */
    @Test
    public void testDialogEvent() throws Exception {
        final int numPutRequests = 4096;
        for (int i = 0; i < numPutRequests; ++i) {
            kvstore.put(
                Key.createKey(String.format("key-DE-%s", i)),
                Value.createValue(
                    String.format("value-DE-%s", i).getBytes()));
        }
        final ObjectNode dialogEndpointGroupPerf =
            kvstore.getStats(true /* clear */).getDialogEndpointGroupPerf();
        checkGroupMetrics(
            dialogEndpointGroupPerf,
            (m) -> checkMetricsEventPerf(m, numPutRequests));
    }

    private void checkMetricsEventPerf(ObjectNode endpointPerf,
                                       int numPutRequests) {
        checkLatencyEventOrder(endpointPerf.toString());
        checkLatencyBreakdown(endpointPerf, numPutRequests);
    }

    private void checkLatencyEventOrder(String string) {
        final int initWritePos = string.indexOf("INIT-WRITE");
        assertTrue("No INIT-WRITE events found", initWritePos >= 0);
        final int writeSendPos = string.indexOf("WRITE-SEND");
        final int sendRecvPos = string.indexOf("SEND-RECV");
        final int recvReadPos = string.indexOf("RECV-READ");
        final int readFinPos = string.indexOf("READ-FIN");
        assertTrue(
            String.format(
                "INIT-WRITE should display before WRITE-SEND: %s", string),
            initWritePos < writeSendPos);
        assertTrue(
            String.format(
                "WRITE-SEND should display before SEND-RECV: %s", string),
            writeSendPos < sendRecvPos);
        assertTrue(
            String.format(
                "SEND-RECV should display before RECV-READ: %s", string),
            sendRecvPos < recvReadPos);
        assertTrue(
            String.format(
                "RECV-READ should display before READ-FIN: %s", string),
            recvReadPos < readFinPos);
    }

    private void checkLatencyBreakdown(ObjectNode perfJson,
                                       int numPutRequests)
    {
        final DialogEndpointPerf perf = new DialogEndpointPerf(perfJson);
        /*
         * Sample count consists of two part: latency sample count and record
         * sample count. Latency sample count is fixed to
         * latencyBreakdownSampleRate requests per sample. Event record is
         * affected by the KVStats monioring.
         */
        final long count = numPutRequests / latencyBreakdownSampleRate;
        final Map<DialogEvent.Span, LatencyElement.Result> map =
            perf.getDialogLatencyBreakdown();
        final String mesg = perf.toString();
        checkLatencyValue(map, "INIT-WRITE", count, 50.0, mesg);
        checkLatencyValue(map, "WRITE-SEND", count, 50.0, mesg);
        checkLatencyValue(map, "SEND-RECV", count, 50.0, mesg);
        checkLatencyValue(map, "RECV-READ", count, 50.0, mesg);
        checkLatencyValue(map, "READ-FIN", count, 50.0, mesg);
    }

    private void checkLatencyValue(
        Map<DialogEvent.Span, LatencyElement.Result> map,
        String event,
        long count,
        double max,
        String mesg)
    {
        final LatencyElement.Result stats =
            map.get(DialogEvent.Span.create(event));
        /* delta for inaccuracy in estimate the event record sample count */
        assertTrue(String.format("Expected %s larger than %s, %s",
                                 stats.getOperationCount(), count, mesg),
                   stats.getOperationCount() > count);
        assertEquals(mesg, 0.0, convert(stats.getMin()), max);
        assertEquals(mesg, 0.0, convert(stats.getMax()), max);
        assertEquals(mesg, 0.0, convert(stats.getAverage()), max);
        assertEquals(mesg, 0.0, convert(stats.getPercent95()), max);
        assertEquals(mesg, 0.0, convert(stats.getPercent99()), max);
    }

    /**
     * Tests NioChannelThreadPoolPerf duration, throughput and latency.
     */
    @Test
    public void testNioChannelExecutorPerf() throws Exception {
        final int niter = 4;
        final int numPutRequests = 128;
        final int sleepTimeMs = 1000;
        for (int i = 0; i < niter; ++i) {
            kvstore.getStats(true);
            for (int j = 0; j < numPutRequests; ++j) {
                kvstore.put(
                    Key.createKey(String.format("key-CL-%s.%s", i, j)),
                    Value.createValue(
                        String.format("value-CL-%s.%s", i, j).getBytes()));
            }
            Thread.sleep(sleepTimeMs);
            final ObjectNode nioChannelThreadPoolPerf =
                kvstore.getStats(true /* clear */).getNioChannelThreadPoolPerf();
            checkNioChannelThreadPoolPerf(nioChannelThreadPoolPerf, numPutRequests);
        }
    }

    private void checkNioChannelThreadPoolPerf(
        ObjectNode nioChannelThreadPoolPerf,
        int numPutRequests)
    {
        StreamSupport
            .stream(nioChannelThreadPoolPerf
                    .get(NioChannelThreadPoolPerf.PerfValue.EXECUTOR_PERFS)
                    .asArray()
                    .spliterator(),
                    false)
            .map((e) -> new NioChannelExecutorPerf(e.asObject()))
            .forEach((p) -> checkExecutorPerf(p, numPutRequests));
        checkHandlerPerf(nioChannelThreadPoolPerf, numPutRequests);
    }

    private void checkExecutorPerf(NioChannelExecutorPerf p,
                                   int numPutRequests) {
        assertTrue(String.format("wrong idle time: %s", p),
                   p.getIdleTimeNanos().get() > 700_000_000L);
        assertTrue(String.format("wrong io process time: %s", p),
                   p.getIOProcessTimeNanos().get() > 1_000_000L);
        assertTrue(String.format("wrong task process time: %s", p),
                   p.getTaskProcessTimeNanos().get() > 100_000L);
        assertTrue(String.format("wrong num of task executed: %s", p),
                   p.getNumTasksExecuted().get()
                   >= numPutRequests / 2);
        assertTrue(String.format("wrong num of immediate task: %s", p),
                   p.getNumImmediateTasksSubmitted().get()
                   >= numPutRequests / 2);
        assertTrue(String.format("wrong num of delayed task: %s", p),
                   p.getNumDelayedTasksSubmitted().get()
                   >= numPutRequests);
    }

    private void checkHandlerPerf(ObjectNode threadPoolPerf,
                                  int numPutRequests)
    {
        assertTrue(String.format("no handler read: %s", threadPoolPerf),
                   getSumOverHandlerPerf(
                       threadPoolPerf,
                       (p) -> p.getHandlerReadThroughput().getCount()) >=
                   numPutRequests);
        /*
         * handler write may be missing because it is only called when the
         * channel was previous busy.
         */
        assertTrue(String.format("no channel read: %s", threadPoolPerf),
                   getSumOverHandlerPerf(
                       threadPoolPerf,
                       (p) -> p.getChannelReadThroughput().getCount()) >=
                   numPutRequests);
        assertTrue(String.format("no channel write: %s", threadPoolPerf),
                   getSumOverHandlerPerf(
                       threadPoolPerf,
                       (p) -> p.getChannelWriteThroughput().getCount()) >=
                   numPutRequests);
        assertTrue(String.format("no handler read latency: %s", threadPoolPerf),
                   getSumOverHandlerPerf(
                       threadPoolPerf,
                       (p) -> p.getHandlerReadLatency().getOperationCount()) >=
                   numPutRequests);
        assertTrue(String.format("no channel read latency: %s", threadPoolPerf),
                   getSumOverHandlerPerf(
                       threadPoolPerf,
                       (p) -> p.getChannelReadLatency().getOperationCount()) >=
                   numPutRequests);
        assertTrue(String.format("no channel write latency: %s", threadPoolPerf),
                   getSumOverHandlerPerf(
                       threadPoolPerf,
                       (p) -> p.getChannelWriteLatency().getOperationCount()) >=
                   numPutRequests);
        assertTrue(String.format("wrong handler read latency: %s", threadPoolPerf),
                   getMaxOverHandlerPerf(
                       threadPoolPerf,
                       (p) -> p.getHandlerReadLatency().getMax()) < 100_000_000L);
        assertTrue(String.format("wrong channel read latency: %s", threadPoolPerf),
                   getMaxOverHandlerPerf(
                       threadPoolPerf,
                       (p) -> p.getChannelReadLatency().getMax()) < 100_000_000L);
        assertTrue(String.format("wrong channel write latency: %s", threadPoolPerf),
                   getMaxOverHandlerPerf(
                       threadPoolPerf,
                       (p) -> p.getChannelWriteLatency().getMax()) < 100_000_000L);
    }

    private long getSumOverHandlerPerf(
        ObjectNode perf,
        ToLongFunction<NioEndpointHandlerPerf> extractor)
    {
        return StreamSupport
            .stream(perf
                    .get(NioChannelThreadPoolPerf.PerfValue.EXECUTOR_PERFS)
                    .asArray()
                    .spliterator(),
                    false)
            .map((e) -> new NioChannelExecutorPerf(e.asObject()))
            .flatMapToLong((p) -> p.getHandlerPerfs().stream().mapToLong(extractor))
            .sum();
    }

    private long getMaxOverHandlerPerf(
        ObjectNode perf,
        ToLongFunction<NioEndpointHandlerPerf> extractor)
    {
        return StreamSupport
            .stream(perf
                    .get(NioChannelThreadPoolPerf.PerfValue.EXECUTOR_PERFS)
                    .asArray()
                    .spliterator(),
                    false)
            .map((e) -> new NioChannelExecutorPerf(e.asObject()))
            .flatMapToLong((p) -> p.getHandlerPerfs().stream().mapToLong(extractor))
            .max().orElse(-1);
    }


    /**
     * Tests the kv stats monitor log.
     */
    @Test
    public void testKVStatsMonitorLog() throws Exception {
        final long testRunTimeMillis = 10000;
        final AtomicBoolean done = new AtomicBoolean(false);
        (new Thread(() -> {
            while (!done.get()) {
                kvstore.put(Key.createKey("key"),
                            Value.createValue("value".getBytes()));
            }
        })).start();
        Thread.sleep(testRunTimeMillis);
        done.set(true);
        checkStatsMonitorLog(testRunTimeMillis);
    }

    private void checkStatsMonitorLog(long testRunTimeMillis) throws Exception {
        final List<Map.Entry<String, ObjectNode>> logRecords = readLogRecords();
        /*
         * We do logging every logIntervalMillis, due to timing issues, we
         * might get less than records
         */
        assertTrue("not enough log records",
            logRecords.size() > testRunTimeMillis / logIntervalMillis / 2);
        checkLogRecords(logRecords);
    }

    private List<Map.Entry<String, ObjectNode>> readLogRecords() throws Exception {
        final List<Map.Entry<String, ObjectNode>> result = new ArrayList<>();
        try (final Scanner scanner =
             new Scanner(new File(KVSTATS_MONITOR_LOG))) {
            while (scanner.hasNext()) {
                final String line = scanner.nextLine();
                if (!line.startsWith("JL")) {
                    continue;
                }
                final String[] parts = line.split("\\|", 4);
                try {
                    final ObjectNode object = JsonUtils.parseJsonObject(parts[3]);
                    result.add(new AbstractMap.SimpleImmutableEntry<>(parts[1], object));
                } catch (Exception e) {
                    System.err.println(String
                        .format("Error parsing string to json: %s", parts[3]));
                    throw e;
                }
            }
            return result;
        }
    }

    private void checkLogRecords(List<Map.Entry<String, ObjectNode>> logRecords) {
        boolean kvstatsRecordCheckPassed = false;
        boolean dialogEndpointGroupPerfCheckPassed = false;
        boolean nioChannelThreadPoolPerfCheckPassed = false;
        /* Skip the first a few records. */
        for (int i = 3; i < logRecords.size(); ++i) {
            final Map.Entry<String,ObjectNode> entry = logRecords.get(i);
            final String type = entry.getKey();
            final ObjectNode object = entry.getValue();
            if (type.equals("KVStats")) {
                try {
                    checkKVStatsRecord(object);
                    kvstatsRecordCheckPassed = true;
                } catch (AssertionError e) {
                    /* ignore */
                }
            } else if (entry.getKey().equals("DialogEndpointGroupPerf")){
                try {
                    checkDialogEndpointGroupPerfRecord(object);
                    dialogEndpointGroupPerfCheckPassed = true;
                } catch (AssertionError e) {
                    /* ignore */
                }
            } else if (entry.getKey().equals("NioChannelThreadPoolPerf")){
                try {
                    checkNioChannelThreadPoolPerf(
                        object,
                        /* assume 100ms latency */
                        (int) (logIntervalMillis / 100));
                    nioChannelThreadPoolPerfCheckPassed = true;
                } catch (AssertionError e) {
                    /* ignore */
                }
            } else {
                throw new AssertionError(
                    String.format("Unknown type of stats: %s", type));
            }
        }
        if (!kvstatsRecordCheckPassed) {
            fail("KVStats check failed");
        }
        if (!dialogEndpointGroupPerfCheckPassed) {
            fail("DialogEndpointGroupPerf check failed");
        }
        if (!nioChannelThreadPoolPerfCheckPassed) {
            fail("NioChannelThreadPoolPerf check failed");
        }
    }

    private void checkKVStatsRecord(ObjectNode record) {
        /* sample check fields are there */
        assertNotNull(record.toString(),
                      find(record, "operationMetrics"));
        assertNotNull(record.toString(),
                      find(record, "nodeMetrics"));
        assertNotNull(record.toString(),
                      find(record, "dialogPerfSummary"));
        assertNotNull(record.toString(),
                      find(record, "channelExecutorPerfSummary"));
        assertNotNull(record.toString(),
                      find(record,
                           DialogEndpointSummarizedPerf.CREATOR_SUMMARY));
        assertNotNull(record.toString(),
                      find(record,
                           DialogEndpointSummarizedPerf.RESPONDER_SUMMARY));
        final ObjectNode creatorEndpointSummary =
            find(record, DialogEndpointSummarizedPerf.CREATOR_SUMMARY)
            .asObject();
        assertNotNull(record.toString(), creatorEndpointSummary);
        checkCreatorEndpointSummary(creatorEndpointSummary);
        final ObjectNode executorSummary =
            record.get("channelExecutorPerfSummary").asObject();
        assertNotNull(record.toString(), executorSummary);
        checkExecutorSummary(executorSummary);
    }

    private void checkCreatorEndpointSummary(
        ObjectNode creatorEndpointSummary)
    {
        final double latencyNanos = creatorEndpointSummary
            .get("latencyNanos").get("finishedDialogAvg")
            .get("max").asDouble();
        /* Max 100 ms latency, should be enought even for slow machines */
        assertEquals(50_000_000, latencyNanos, 50_000_000);
        final double throughput = creatorEndpointSummary
            .get("throughputPerSecond").get("dialogStart")
            .get("max").asDouble();
        /* Corresponding throughput */
        assertTrue(String.format("Throughput %s is too low", throughput),
                   throughput > 0);
    }

    private void checkExecutorSummary(ObjectNode executorSummary) {
        checkExecutorTimeSummary(executorSummary);
        checkExecutorNumTasksSummary(executorSummary);
        checkHandlerThroughputSummary(executorSummary);
        checkHandlerLatencySummary(executorSummary);
    }

    private void checkExecutorTimeSummary(ObjectNode executorSummary) {
        final ObjectNode executorTime = executorSummary
            .get(NioChannelExecutorSummarizedPerf.EXECUTOR_TIME)
            .asObject();
        assertTrue("wrong idle time",
                   executorTime
                   .get(NioChannelExecutorSummarizedPerf.IDLE_TIME)
                   .get("avg").asLong()> 700_000_000L);
        assertTrue("wrong io time",
                   executorTime
                   .get(NioChannelExecutorSummarizedPerf.IO_TIME)
                   .get("avg").asLong() > 1_000_000L);
        assertTrue("wrong task time",
                   executorTime
                   .get(NioChannelExecutorSummarizedPerf.TASK_TIME)
                   .get("avg").asLong() > 1_000_000L);
    }

    private void checkExecutorNumTasksSummary(ObjectNode executorSummary) {
        final ObjectNode executorNumTasks = executorSummary
            .get(NioChannelExecutorSummarizedPerf.EXECUTOR_NUM_TASKS)
            .asObject();
        assertTrue("wrong num executed tasks",
                   executorNumTasks
                   .get(NioChannelExecutorSummarizedPerf.EXECUTED)
                   .get("avg").asLong() > 100);
        assertTrue("wrong num submitted immediate tasks",
                   executorNumTasks
                   .get(NioChannelExecutorSummarizedPerf.SUBMITTED_IMMEDIATE)
                   .get("avg").asLong() > 100);
        assertTrue("wrong num submitted delayed tasks",
                   executorNumTasks
                   .get(NioChannelExecutorSummarizedPerf.SUBMITTED_DELAYED)
                   .get("avg").asLong() > 100);
    }

    private void checkHandlerThroughputSummary(ObjectNode executorSummary) {
        final ObjectNode handlerLatency = executorSummary
            .get(NioChannelExecutorSummarizedPerf.HANDLER_THROUGHPUT)
            .asObject();
        assertTrue(
            handlerLatency.get(NioChannelExecutorSummarizedPerf.HANDLER_READ)
            .get("avg").asLong() > 1);
        assertTrue(
            handlerLatency.get(NioChannelExecutorSummarizedPerf.CHANNEL_READ)
            .get("avg").asLong() > 1);
        assertTrue(handlerLatency
                   .get(NioChannelExecutorSummarizedPerf.CHANNEL_BYTES_READ)
                   .get("avg").asLong() > 100);
        /*
         * handler write can be missing since it is only called if previously
         * the channel is busy.
         */
        assertTrue(
            handlerLatency
            .get(NioChannelExecutorSummarizedPerf.CHANNEL_WRITE)
            .get("avg").asLong() > 1);
        assertTrue(
            handlerLatency
            .get(NioChannelExecutorSummarizedPerf.CHANNEL_BYTES_WRITE)
            .get("avg").asLong() > 100);
    }

    private void checkHandlerLatencySummary(ObjectNode executorSummary) {
        final ObjectNode handlerLatency =
            executorSummary
            .get(NioChannelExecutorSummarizedPerf.HANDLER_LATENCY).asObject();
        /* handler write may be missing */
        assertLatency(handlerLatency,
                      NioChannelExecutorSummarizedPerf.AVG_HANDLER_READ);
        assertLatency(handlerLatency,
                      NioChannelExecutorSummarizedPerf.AVG_CHANNEL_READ);
        assertLatency(handlerLatency,
                      NioChannelExecutorSummarizedPerf.AVG_CHANNEL_WRITE);
        assertLatency(handlerLatency,
                      NioChannelExecutorSummarizedPerf.P95_HANDLER_READ);
        assertLatency(handlerLatency,
                      NioChannelExecutorSummarizedPerf.P95_CHANNEL_READ);
        assertLatency(handlerLatency,
                      NioChannelExecutorSummarizedPerf.P95_CHANNEL_WRITE);
    }

    private void assertLatency(ObjectNode payload, String name) {
        assertTrue(String.format("assertLatency, payload=%s, name=%s",
                                 payload, name),
                   payload.get(name).get("avg").asLong() >= 10_000L);
        assertTrue(String.format("assertLatency, payload=%s, name=%s",
                                 payload, name),
                   payload.get(name).get("avg").asLong() < 10_000_000L);
        assertTrue(String.format("assertLatency, payload=%s, name=%s",
                                 payload, name),
                   payload.get(name).get("min").asLong() >= 10_000L);
        assertTrue(String.format("assertLatency, payload=%s, name=%s",
                                 payload, name),
                   payload.get(name).get("min").asLong() < 10_000_000L);
    }

    private void checkDialogEndpointGroupPerfRecord(ObjectNode record) {
        /* sample check fields are there */
        assertNotNull(
            record.toString(),
            find(record, DialogEndpointGroupPerf.PerfValue.CREATOR_PERFS));
        assertNotNull(
            record.toString(),
            find(record, DialogEndpointGroupPerf.PerfValue.RESPONDER_PERFS));
        assertNotNull(
            record.toString(),
            find(record, DialogEndpointPerf.DIALOG_CONCURRENCY));
        final ObjectNode creatorPerfs =
            find(record, DialogEndpointGroupPerf.PerfValue.CREATOR_PERFS)
            .asObject();
        assertNotNull(record.toString(), creatorPerfs);
        for (Map.Entry<String, JsonNode> entry:
             creatorPerfs.entrySet()) {
            final String name = entry.getKey();
            final ObjectNode perf = entry.getValue().asObject();
            if (name.contains("rg1-rn1")) {
                final JsonNode latencyBreakdown =
                    perf.get(DialogEndpointPerf.DIALOG_LATENCY_BREAKDOWN);
                if (latencyBreakdown == null) {
                    fail(String.format("No latency events: %s", perf));
                    return;
                }
                final int breakdownSampleCount = latencyBreakdown
                    .get(0).get("INIT-WRITE").get("operationCount").asInt();
                /* should have latency event data */
                assertTrue("no latency events", breakdownSampleCount > 0);
                final JsonNode records =
                    perf.get(DialogEndpointPerf.DIALOG_INFO_RECORDS);
                if (records == null) {
                    fail(String.format("No records: %s", perf));
                    return;
                }
                /* should have four records per collection */
                assertEquals(4, records.asArray().size());
                return;
            }
        }
        fail("No rg1-rn1 record present");
    }


    /**
     * Tests the io buffer pool stats are approximately correct.
     */
    @Test
    public void testIOBufferPoolStats() throws Exception {
        final int nthreads = 8;
        final int numPutRequests = 128;
        IOBufferPool.CHNL_IN_POOL.setMaxPoolSize(10);
        IOBufferPool.CHNL_OUT_POOL.setMaxPoolSize(10);
        IOBufferPool.MESG_OUT_POOL.setMaxPoolSize(10);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);
        final Thread[] threads = new Thread[nthreads];
        for (int i = 0; i < nthreads; ++i) {
            final int index = i;
            final Thread thread = new Thread(() -> {
                try {
                    for (int j = 0; j < numPutRequests; ++j) {
                        kvstore.put(
                            Key.createKey(
                                String.format("key-CL-%s.%s", index, j)),
                            Value.createValue(
                                String.format("value-CL-%s.%s", index, j)
                                .getBytes()));
                    }
                } catch (Throwable t) {
                    error.set(t);
                }
            });
            thread.start();
            threads[i] = thread;
        }
        for (Thread t : threads) {
            t.join();
        }
        Thread.sleep(logIntervalMillis);
        final ObjectNode dialogEndpointGroupPerf =
            kvstore.getStats(true /* clear */).getDialogEndpointGroupPerf();
        checkIOBufferPoolStats(dialogEndpointGroupPerf);
    }

    private void checkIOBufferPoolStats(ObjectNode dialogEndpointGroupPerf) {
        final ObjectNode result = dialogEndpointGroupPerf
            .get(DialogEndpointGroupPerf.PerfValue.IO_BUFFER_POOL_USAGE_PERF)
            .asObject();
        assertTrue("ChannelInput in use percentage must be larger than zero",
                   result
                   .get(IOBufferPoolUsagePerf.CHANNEL_INPUT_USE_PERCENTAGE)
                   .get("count").asInt() > 0);
        assertTrue("ChannelOutput in use percentage must be larger than zero",
                   result
                   .get(IOBufferPoolUsagePerf.CHANNEL_OUTPUT_USE_PERCENTAGE)
                   .get("count").asInt() > 0);
        assertTrue("MessageOutput in use percentage must be larger than zero",
                   result
                   .get(IOBufferPoolUsagePerf.MESSAGE_OUTPUT_USE_PERCENTAGE)
                   .get("count").asInt() > 0);
    }

    /**
     * Recursively find an element of the specified name in the json object.
     *
     * @param object the JSON object to search
     * @param name the field name to find (case-sensitive)
     * @return the underlying JsonNode (gson) or null if not found
     */
    private static JsonNode find(ObjectNode object, String name) {
        JsonNode result = object.get(name);
        if (result != null) {
            return result;
        }
        for (Map.Entry<String, JsonNode> entry : object.entrySet()) {
            final JsonNode e = entry.getValue();
            result = find(e, name);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private static JsonNode find(JsonNode e, String name) {
        if (e instanceof ObjectNode) {
            return find((ObjectNode) e, name);
        }
        if (e instanceof ArrayNode) {
            return find((ArrayNode) e, name);
        }
        return null;
    }

    /**
     * Recursively find an element of the specified name in the json array.
     */
    private static JsonNode find(ArrayNode array, String name) {
        JsonNode result;
        final Iterator<JsonNode> iter = array.iterator();
        while (iter.hasNext()) {
            final JsonNode e = iter.next();
            result = find(e, name);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /** Tests the rate limiting. */
    @Test
    public void testRateLimiting() throws Exception {
        DialogEndpointGroupPerfTracker.enableRateLimiting = true;
        NioChannelThreadPoolPerfTracker.enableRateLimiting = true;
        final Queue<LogRecord> endpointGroupLogRecords =
            new ConcurrentLinkedQueue<>();
        final Logger endpointGroupLogger =
            ((NioEndpointGroup) AsyncRegistryUtils.getEndpointGroup())
            .getLogger();
        endpointGroupLogger.addHandler(new StreamHandler() {
            @Override
            public synchronized void publish(@Nullable LogRecord record) {
                endpointGroupLogRecords.add(record);
            }
        });
        final int count0 = getNumOfVerboseAsyncStats(readLogRecords());
        final long testRunTimeMillis = 10000;
        final AtomicBoolean done = new AtomicBoolean(false);
        (new Thread(() -> {
            while (!done.get()) {
                kvstore.put(Key.createKey("key"),
                            Value.createValue("value".getBytes()));
            }
        })).start();
        Thread.sleep(testRunTimeMillis);
        done.set(true);
        final int count1 = getNumOfVerboseAsyncStats(readLogRecords());
        /*
         * Check that we have at most two more records, one for
         * DialogEndpointGroupPerf and one for NioChannelThreadPoolPerf.
         */
        assertTrue(
            String.format(
                "unexpected incremented number of records: %s",
                count1 - count0),
            count1 <= count0 + 2);
        assertEquals(2, endpointGroupLogRecords.size());
        final StringBuilder sb = new StringBuilder();
        for (LogRecord r : endpointGroupLogRecords) {
            sb.append(r.getMessage()).append("\n");
        }
        final String endpointGroupLog = sb.toString();
        final Matcher matcher1 = Pattern.compile(
            "Obtained DialogEndpointGroupPerf with "
            + "[0-9]+ endpoints and estimated [0-9]+ bytes; "
            + "next obtain after (?<delay>[0-9]+) seconds")
            .matcher(endpointGroupLog);
        assertTrue(endpointGroupLog, matcher1.find());
        int delay = Integer.parseInt(matcher1.group("delay"));
        assertTrue(String.format("Unexpected delay: %s", delay),
                   200 <= delay && delay <= 400);
        final Matcher matcher2 = Pattern.compile(
            "Obtained NioChannelThreadPoolPerf "
            + "with [0-9]+ executors, [0-9]+ handlers "
            + "and estimated [0-9]+ bytes; "
            + "next obtain after (?<delay>[0-9]+) seconds")
            .matcher(endpointGroupLog);
        assertTrue(endpointGroupLog, matcher2.find());
        delay = Integer.parseInt(matcher2.group("delay"));
        assertTrue(String.format("Unexpected delay: %s", delay),
                   100 <= delay && delay <= 200);
    }

    private int getNumOfVerboseAsyncStats(
        List<Map.Entry<String, ObjectNode>> records)
    {
        int count = 0;
        for (Map.Entry<String, ObjectNode> entry : records) {
            final String key = entry.getKey();
            final ObjectNode val = entry.getValue();
            if (key.equals("NioChannelThreadPoolPerf")) {
                count++;
            }
            if (key.equals("DialogEndpointGroupPerf")
                /*
                 * We still collect IOBufferPoolUsagePerf and
                 * resourceManagerPerf when we do rate limiting.
                 */
                && (val.size() > 2)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Prints out the async dialog layer performance data.
     *
     * This test is for the purpose of manually inspecting the performance
     * data: it simply print things out.
     */
    @Test
    public void testAsyncDialogPerfData() throws Exception {
        assumeTrue("Requires running alone",
                   "testAsyncDialogPerfData".equals(
                       System.getProperty("testcase.methods", null)));
        final int numPutRequests = 128;
        final int sleepTimeMs = 1000;
        for (int i = 0; i < numPutRequests; ++i) {
            kvstore.put(
                Key.createKey(String.format("key-CL-%s", i)),
                Value.createValue(
                    String.format("value-CL-%s", i).getBytes()));
        }
        Thread.sleep(sleepTimeMs);
        final KVStats kvstats = kvstore.getStats(true /* clear */);
        final ObjectNode dialogEndpointGroupPerf =
            kvstats.getDialogEndpointGroupPerf();
        final ObjectNode nioChannelThreadPoolPerf =
            kvstats.getNioChannelThreadPoolPerf();
        printObjectNodeDirectly(
            "dialogEndpointGroupPerf", dialogEndpointGroupPerf);
        printReferencePrimitive(
            "dialogEndpointGroupPerf", dialogEndpointGroupPerf);
        printObjectNodeDirectly(
            "nioChannelThreadPoolPerf", nioChannelThreadPoolPerf);
        printReferencePrimitive(
            "nioChannelThreadPoolPerf", nioChannelThreadPoolPerf);
    }

    private void printObjectNodeDirectly(String name, ObjectNode node) {
        System.out.println("========");
        System.out.println(String.format("ObjectNode: %s", name));
        System.out.println("========");
        System.out.println(node);
        System.out.println();
    }

    private void printReferencePrimitive(String name, JsonNode node) {
        System.out.println("========");
        System.out.println(String.format("ObjectNode: %s", name));
        System.out.println("========");
        final Iterator<Map.Entry<JsonReference, JsonNode>> iter =
            PerfUtil.depthFirstSearchIterator(node);
        while (iter.hasNext()) {
            final Map.Entry<JsonReference, JsonNode> entry = iter.next();
            final JsonReference ref = entry.getKey();
            final JsonNode val = entry.getValue();
            if (!val.isPrimitive()) {
                continue;
            }
            System.out.println(String.format(
                "%s %s", ref.getString("_"), val));
        }
        System.out.println();
    }
}
