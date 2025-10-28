/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static oracle.nosql.proxy.MonitorStats.ACTIVE_REQUEST_NAME;
import static oracle.nosql.proxy.MonitorStats.DATA_RESPONSE_READ_SIZE_NAME;
import static oracle.nosql.proxy.MonitorStats.DATA_RESPONSE_WRITE_SIZE_NAME;
import static oracle.nosql.proxy.MonitorStats.REQUEST_LABELS;
import static oracle.nosql.proxy.MonitorStats.REQUEST_LATENCY_NAME;
import static oracle.nosql.proxy.MonitorStats.REQUEST_SERVER_FAILED_NAME;
import static oracle.nosql.proxy.MonitorStats.REQUEST_THROTTLING_FAILED_NAME;
import static oracle.nosql.proxy.MonitorStats.REQUEST_TOTAL_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import oracle.nosql.common.sklogger.Counter;
import oracle.nosql.common.sklogger.LongGauge;
import oracle.nosql.common.sklogger.MetricFamilySamples;
import oracle.nosql.common.sklogger.MetricFamilySamples.Sample;
import oracle.nosql.common.sklogger.MetricRegistry;
import oracle.nosql.common.sklogger.PerfQuantile;
import oracle.nosql.common.sklogger.SizeQuantile;
import oracle.nosql.common.sklogger.StatsData;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutRequest.Option;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.proxy.MonitorStats.OperationType;

import org.junit.Test;

/**
 * This is a Proxy concurrent smoke test along with monitor stats checking.
 * It can also be used to generate monitor data samples by setting log
 * configuration through System property java.util.logging.config.file and
 * a longer test time through System property monitorSeconds.
 */
public class MonitorStatsTest extends ProxyTestBase {

    private static final String WATCHER_NAME = "MonitorStatsTest";
    /*
     * The number of threads to run smoke test. This number should match the
     * connection pool size in the NoSQLHandle config.
     */
    private static final int CONCURRENT_NUM = 3;
    private static final String TABLE_PREFIX = "userStats";
    private ExecutorService executor =
        Executors.newFixedThreadPool(CONCURRENT_NUM);

    /*
     * Total requests for each type
     */
    private final Map<OperationType, AtomicLong> requestTotal;
    /*
     * Failed data requests for each type
     */
    private final Map<OperationType, AtomicLong> serverFailed;
    private final Map<OperationType, AtomicLong> userFailed;
    private final Map<OperationType, AtomicLong> throttlingFailed;
    /*
     * Total data operations for each type. Failed data request count 0 ops,
     * and a multiple request count N ops.
     */
    private final Map<OperationType, AtomicLong> operationTotal;
    /*
     * Data operation charged metrics
     */
    private final AtomicLong writeKBCharged = new AtomicLong(0);
    private final AtomicLong readKBCharged = new AtomicLong(0);

    public MonitorStatsTest() {
        requestTotal = new HashMap<OperationType, AtomicLong>();
        serverFailed = new HashMap<OperationType, AtomicLong>();
        userFailed = new HashMap<OperationType, AtomicLong>();
        throttlingFailed = new HashMap<OperationType, AtomicLong>();
        operationTotal = new HashMap<OperationType, AtomicLong>();
        reset();
    }

    @Test
    public void smokeTest() {

        final long startTime = System.nanoTime();
        /*
         * Set monitorSeconds to a longer time if this test is used to generate
         * monitor data samples that collected by MetricRegistry at background.
         */
        int seconds = Integer.parseInt(
            System.getProperty("monitorSeconds", "30"));
        long totalTime = seconds * 1_000_000_000L;

        while(true) {
            /*
             * Submit number of smokeTest tasks.
             */
            Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
            for (int k = 0; k < CONCURRENT_NUM; k++) {
                final String tableName = TABLE_PREFIX + k;
                tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() {
                        smokeTest(tableName);
                        return null;
                    }
                });
            }
            try {
                reset();
                List<Future<Void>> futures = executor.invokeAll(tasks);
                for(Future<Void> f : futures) {
                    f.get();
                }
            } catch (InterruptedException e) {
                fail("unexpected interrupt");
            } catch (ExecutionException e) {
                fail("unexpected ExecutionException: " + e.getCause());
            }
            checkMonitorData();
            if (System.nanoTime() - startTime > totalTime) {
                break;
            }
        }
    }

    private void reset() {
        for(OperationType type : OperationType.values()) {
            requestTotal.put(type, new AtomicLong(0));
            serverFailed.put(type, new AtomicLong(0));
            userFailed.put(type, new AtomicLong(0));
            throttlingFailed.put(type, new AtomicLong(0));
            operationTotal.put(type, new AtomicLong(0));
        }
        writeKBCharged.set(0);
        readKBCharged.set(0);

        // Reset metrics by using the same watcher name to get metrics.
        MetricRegistry.defaultRegistry.getAllMetricFactory(WATCHER_NAME);
    }

    private void smokeTest(String tableName) {

        try {

            MapValue key = new MapValue().put("id", 10);

            MapValue value = new MapValue().put("id", 10).put("name", "jane");

            TableResult tres;
            /* DDL language error */
            try {
                tres = tableOperation(handle,
                                      "adrop table if exists " + tableName,
                                      null, 20000);
                fail("Expected IAE");
            } catch (IllegalArgumentException iae) {
            }
            requestTotal.get(OperationType.TABLE_REQUEST).incrementAndGet();
            userFailed.get(OperationType.TABLE_REQUEST).incrementAndGet();

            /* drop a table */
            tres = tableOperation(handle,
                                  "drop table if exists " +
                                  tableName,
                                  null, TableResult.State.DROPPED, 40000);
            requestTotal.get(OperationType.TABLE_REQUEST).incrementAndGet();
            operationTotal.get(OperationType.TABLE_REQUEST).incrementAndGet();
            assertNotNull(tres.getTableName());

            /* Create a table */
            tres = tableOperation(
                handle,
                "create table if not exists " + tableName + "(id integer, " +
                "name string, primary key(id))",
                new TableLimits(1000, 500, 50),
                TableResult.State.ACTIVE,
                30000);
            requestTotal.get(OperationType.TABLE_REQUEST).incrementAndGet();
            operationTotal.get(OperationType.TABLE_REQUEST).incrementAndGet();
            /*
             * TODO
             * There is a loop to get and wait table status, so we don't know
             * the exact get table request count.
             */
            requestTotal.get(OperationType.GET_TABLE).incrementAndGet();
            operationTotal.get(OperationType.GET_TABLE).incrementAndGet();
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* Create an index */
            tres = tableOperation(
                handle,
                "create index if not exists Name on " + tableName + "(name)",
                null,
                TableResult.State.ACTIVE,
                50000);
            requestTotal.get(OperationType.TABLE_REQUEST).incrementAndGet();
            operationTotal.get(OperationType.TABLE_REQUEST).incrementAndGet();
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /* GetTableRequest for table that doesn't exist */
            try {
                GetTableRequest getTable =
                    new GetTableRequest()
                    .setTableName("not_a_table");
                tres = handle.getTable(getTable);
                fail("Table should not be found");
            } catch (TableNotFoundException tnfe) {}
            requestTotal.get(OperationType.GET_TABLE).incrementAndGet();
            userFailed.get(OperationType.GET_TABLE).incrementAndGet();

            /* list tables */
            ListTablesRequest listTables = new ListTablesRequest();

            /* ListTablesRequest returns ListTablesResult */
            ListTablesResult lres = handle.listTables(listTables);
            assertNotNull(lres.toString());
            requestTotal.get(OperationType.LIST_TABLES).incrementAndGet();
            operationTotal.get(OperationType.LIST_TABLES).incrementAndGet();

            /* Get indexes */
            GetIndexesRequest getIndexes = new GetIndexesRequest()
                .setTableName(tableName);

            /* GetIndexesRquest returns GetIndexesResult */
            GetIndexesResult giRes = handle.getIndexes(getIndexes);
            if (testV3) {
                /*
                 * TODO: GetIndexesResult.toString() in v5.4 might need enhance
                 * to handle null for String[] fieldTypes. Otherwise, when force
                 * V3 protocol, giRes.toString() causes NPE.
                 */
                assertNotNull(giRes);
            } else {
                assertNotNull(giRes.toString());
            }
            requestTotal.get(OperationType.GET_INDEXES).incrementAndGet();
            operationTotal.get(OperationType.GET_INDEXES).incrementAndGet();

            /* PUT */
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tableName);

            PutResult res = handle.put(putRequest);
            requestTotal.get(OperationType.PUT).incrementAndGet();
            operationTotal.get(OperationType.PUT).incrementAndGet();
            readKBCharged.addAndGet(res.getReadUnitsInternal());
            writeKBCharged.addAndGet(res.getWriteUnitsInternal());
            assertNotNull("Put failed", res.getVersion());
            assertWriteKB(res);
            assertNull(res.getExistingValue());
            assertNull(res.getExistingVersion());

            /* put a few more. set TTL to test that path */
            putRequest.setTTL(TimeToLive.ofHours(2));
            for (int i = 20; i < 30; i++) {
                value.put("id", i);
                res = handle.put(putRequest);
                requestTotal.get(OperationType.PUT).incrementAndGet();
                operationTotal.get(OperationType.PUT).incrementAndGet();
                readKBCharged.addAndGet(res.getReadUnitsInternal());
                writeKBCharged.addAndGet(res.getWriteUnitsInternal());
            }

            /*
             * Test ReturnRow for simple put of a row that exists. 2 cases:
             * 1. unconditional (will return info)
             * 2. if absent (will return info)
             */
            value.put("id", 20);
            putRequest.setReturnRow(true);
            PutResult pr = handle.put(putRequest);
            requestTotal.get(OperationType.PUT).incrementAndGet();
            operationTotal.get(OperationType.PUT).incrementAndGet();
            readKBCharged.addAndGet(pr.getReadUnitsInternal());
            writeKBCharged.addAndGet(pr.getWriteUnitsInternal());
            assertNotNull(pr.getVersion()); // success
            assertNotNull(pr.getExistingVersion());
            assertNotNull(pr.getExistingValue());
            assertTrue(pr.getExistingModificationTime() != 0);
            assertReadKB(pr);

            putRequest.setOption(Option.IfAbsent);
            pr = handle.put(putRequest);
            requestTotal.get(OperationType.PUT).incrementAndGet();
            operationTotal.get(OperationType.PUT).incrementAndGet();
            readKBCharged.addAndGet(pr.getReadUnitsInternal());
            writeKBCharged.addAndGet(pr.getWriteUnitsInternal());
            assertNull(pr.getVersion()); // failure
            assertNotNull(pr.getExistingVersion());
            assertNotNull(pr.getExistingValue());
            assertTrue(pr.getExistingModificationTime() != 0);
            assertReadKB(pr);

            /* clean up */
            putRequest.setReturnRow(false);
            putRequest.setOption(null);

            /* GET */
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName(tableName);

            GetResult res1 = handle.get(getRequest);
            requestTotal.get(OperationType.GET).incrementAndGet();
            operationTotal.get(OperationType.GET).incrementAndGet();
            readKBCharged.addAndGet(res1.getReadUnitsInternal());
            writeKBCharged.addAndGet(res1.getWriteUnitsInternal());
            assertNotNull("Get failed", res1.getJsonValue());
            assertReadKB(res1);

            /* DELETE */
            DeleteRequest delRequest = new DeleteRequest()
                .setKey(key)
                .setTableName(tableName);

            DeleteResult del = handle.delete(delRequest);
            requestTotal.get(OperationType.DELETE).incrementAndGet();
            operationTotal.get(OperationType.DELETE).incrementAndGet();
            readKBCharged.addAndGet(del.getReadUnitsInternal());
            writeKBCharged.addAndGet(del.getWriteUnitsInternal());
            assertTrue("Delete failed", del.getSuccess());
            assertWriteKB(del);

            /* GET -- no row, it was removed above */
            getRequest.setTableName(tableName);
            res1 = handle.get(getRequest);
            requestTotal.get(OperationType.GET).incrementAndGet();
            operationTotal.get(OperationType.GET).incrementAndGet();
            readKBCharged.addAndGet(res1.getReadUnitsInternal());
            writeKBCharged.addAndGet(res1.getWriteUnitsInternal());
            assertNull(res1.getValue());
            assertReadKB(res1);

            /* MULTIDELETE */
            MultiDeleteRequest multiDelRequest = new MultiDeleteRequest();
            multiDelRequest.setKey(new MapValue().put("id", 21));
            multiDelRequest.setTableName(tableName);
            MultiDeleteResult multiRes = handle.multiDelete(multiDelRequest);
            requestTotal.get(OperationType.MULTI_DELETE).incrementAndGet();
            operationTotal.get(OperationType.MULTI_DELETE).incrementAndGet();
            readKBCharged.addAndGet(multiRes.getReadUnitsInternal());
            writeKBCharged.addAndGet(multiRes.getWriteUnitsInternal());
            assertWriteKB(multiRes);

            /* MULTIDELETE -- no table */
            multiDelRequest.setKey(new MapValue().put("id", 0));
            multiDelRequest.setTableName("InvalidTable");
            try {
                handle.multiDelete(multiDelRequest);
                fail("Attempt to access missing table should have thrown");
            } catch (TableNotFoundException nse) {
                // success
            }
            requestTotal.get(OperationType.MULTI_DELETE).incrementAndGet();
            userFailed.get(OperationType.MULTI_DELETE).incrementAndGet();

            /* GET -- no table */
            try {
                getRequest.setTableName("foo");
                res1 = handle.get(getRequest);
                fail("Attempt to access missing table should have thrown");
            } catch (TableNotFoundException nse) {
                // success
            }
            requestTotal.get(OperationType.GET).incrementAndGet();
            userFailed.get(OperationType.GET).incrementAndGet();

            /* PUT -- invalid row -- this will throw */
            try {
                value.remove("id");
                value.put("not_a_field", 1);
                res = handle.put(putRequest);
                fail("Attempt to put invalid row should have thrown");
            } catch (IllegalArgumentException iae) {
                // success
            }
            requestTotal.get(OperationType.PUT).incrementAndGet();
            userFailed.get(OperationType.PUT).incrementAndGet();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test: " + e);
        }
    }

    /*
     * Check collected metrics are expected after executing some SC requests
     * and data requests.
     */
    @SuppressWarnings("unchecked")
    private void checkMonitorData() {
        for (MetricFamilySamples<?> metricFamily :
                MetricRegistry.defaultRegistry.getAllMetricFactory(
                    WATCHER_NAME)) {
            final String metricName = metricFamily.getName();
            if (metricName.equals(ACTIVE_REQUEST_NAME)) {
                assertEquals(StatsData.Type.LONG_GAUGE, metricFamily.getType());
                assertEquals(0, metricFamily.getLabelNames().size());
                for (Sample<?> s : metricFamily.getSamples()) {
                    Sample<LongGauge.GaugeResult> sample =
                        (Sample<LongGauge.GaugeResult>) s;
                    assertEquals(0, sample.labelValues.size());
                    assertEquals(0, sample.dataValue.getGaugeVal());
                }
            } else if (metricName.equals(REQUEST_TOTAL_NAME)) {
                assertEquals(StatsData.Type.COUNTER, metricFamily.getType());
                assertArrayEquals(REQUEST_LABELS,
                                  metricFamily.getLabelNames().toArray());
                for (Sample<?> s : metricFamily.getSamples()) {
                    Sample<Counter.RateResult> sample =
                        (Sample<Counter.RateResult>) s;
                    assertEquals(REQUEST_LABELS.length,
                                 sample.labelValues.size());
                    OperationType opType = getOperationType(sample.labelValues);
                    if (opType.equals(OperationType.GET_TABLE)) {
                        /*
                         * TODO
                         * There is a loop to get and wait table status, so we
                         * don't know the exact get table request count.
                         */
                        assertTrue(sample.labelValues + " reqTotal error",
                                   sample.dataValue.getCount() >=
                                       operationTotal.get(opType).get());
                    } else {
                        assertEquals(sample.labelValues + " reqTotal error",
                                     requestTotal.get(opType).get(),
                                     sample.dataValue.getCount());
                    }
                }
            } else if (metricName.equals(REQUEST_LATENCY_NAME)) {
                assertEquals(metricFamily.getType(),
                             StatsData.Type.PERF_QUANTILE);
                assertArrayEquals(REQUEST_LABELS,
                                  metricFamily.getLabelNames().toArray());
                for (Sample<?> s : metricFamily.getSamples()) {
                    Sample<PerfQuantile.RateResult> sample =
                        (Sample<PerfQuantile.RateResult>) s;
                    assertEquals(REQUEST_LABELS.length,
                                 sample.labelValues.size());
                    assertEquals(0, sample.dataValue.getOverflowCount());
                    assertTrue("95th can't be negative",
                               sample.dataValue.get99th() >= 0);
                    assertTrue("99th can't be negative",
                               sample.dataValue.get95th() >= 0);
                    assertTrue("min can't be negative",
                               sample.dataValue.getMin() >= 0);
                    assertTrue("max can't be negative",
                               sample.dataValue.getMax() >=
                                   sample.dataValue.getMin());
                    OperationType opType = getOperationType(sample.labelValues);
                    if (opType.equals(OperationType.GET_TABLE)) {
                        /*
                         * TODO
                         * There is a loop to get and wait table status, so we
                         * don't know the exact get table request count.
                         */
                        assertTrue(sample.labelValues + " opsTotal error",
                                   sample.dataValue.getOperationCount() >=
                                       operationTotal.get(opType).get());
                        assertTrue(sample.labelValues + " requestCount error",
                                   sample.dataValue.getRequestCount() >=
                                       requestTotal.get(opType).get() -
                                           serverFailed.get(opType).get() -
                                           throttlingFailed.get(opType).get() -
                                           userFailed.get(opType).get());
                    } else {
                        assertEquals(sample.labelValues + " opsTotal error",
                                     operationTotal.get(opType).get(),
                                     sample.dataValue.getOperationCount());
                        assertEquals(sample.labelValues + " requestCount error",
                                     requestTotal.get(opType).get() -
                                         serverFailed.get(opType).get() -
                                         throttlingFailed.get(opType).get() -
                                         userFailed.get(opType).get(),
                                     sample.dataValue.getRequestCount());
                    }
                }
            } else if (metricName.equals(DATA_RESPONSE_READ_SIZE_NAME)) {
                assertEquals(StatsData.Type.SIZE_QUANTILE,
                             metricFamily.getType());
                assertEquals(0, metricFamily.getLabelNames().size());
                for (Sample<?> s : metricFamily.getSamples()) {
                    Sample<SizeQuantile.RateResult> sample =
                        (Sample<SizeQuantile.RateResult>) s;
                    assertEquals(0, sample.labelValues.size());
                    assertEquals(readKBCharged.get(),
                                 sample.dataValue.getSum());
                    for(double perfVal : sample.dataValue.getQuantileValues()) {
                        assertTrue("perf value can't be negative",
                                   perfVal >= 0);
                    }
                }
            } else if (metricName.equals(DATA_RESPONSE_WRITE_SIZE_NAME)) {
                assertEquals(StatsData.Type.SIZE_QUANTILE,
                             metricFamily.getType());
                assertEquals(0, metricFamily.getLabelNames().size());
                for (Sample<?> s : metricFamily.getSamples()) {
                    Sample<SizeQuantile.RateResult> sample =
                        (Sample<SizeQuantile.RateResult>) s;
                    assertEquals(0, sample.labelValues.size());
                    assertEquals(writeKBCharged.get(),
                                 sample.dataValue.getSum());
                    for(double perfVal : sample.dataValue.getQuantileValues()) {
                        assertTrue("perf value can't be negative",
                                   perfVal >= 0);
                    }
                }
            } else if (metricName.equals(REQUEST_SERVER_FAILED_NAME)) {
                assertEquals(StatsData.Type.COUNTER, metricFamily.getType());
                assertArrayEquals(REQUEST_LABELS,
                                  metricFamily.getLabelNames().toArray());
                for (Sample<?> s : metricFamily.getSamples()) {
                    Sample<Counter.RateResult> sample =
                        (Sample<Counter.RateResult>) s;
                    assertEquals(REQUEST_LABELS.length,
                                 sample.labelValues.size());
                    OperationType opType = getOperationType(sample.labelValues);
                    assertEquals(serverFailed.get(opType).get(),
                                 sample.dataValue.getCount());
                }
            } else if (metricName.equals(REQUEST_THROTTLING_FAILED_NAME)) {
                assertEquals(StatsData.Type.COUNTER, metricFamily.getType());
                assertArrayEquals(REQUEST_LABELS,
                                  metricFamily.getLabelNames().toArray());
                for (Sample<?> s : metricFamily.getSamples()) {
                    Sample<Counter.RateResult> sample =
                        (Sample<Counter.RateResult>) s;
                    assertEquals(REQUEST_LABELS.length,
                                 sample.labelValues.size());
                    OperationType opType = getOperationType(sample.labelValues);
                    assertEquals(throttlingFailed.get(opType).get(),
                                 sample.dataValue.getCount());
                }
            } else if (metricName.startsWith(KVHandleStats.KV_HANDLE_NAME)) {
                /* TODO: check kvstore handle metrics? */
            } else {
                fail("unkown metric name: " + metricName);
            }
        }
    }

    /*
     * Check and convert operation label values to OperationType.
     */
    private OperationType getOperationType(List<String> opLabelValues) {
        assertEquals(1, opLabelValues.size());
        for(OperationType type : OperationType.values()) {
            if (type.getValue()[0].equals(opLabelValues.get(0))) {
                return type;
            }
        }
        fail("Unknown label values: " + opLabelValues.get(0));
        return null;
    }
}
