/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.nosql.proxy;

import java.lang.reflect.Method;
import java.util.logging.Level;

import oracle.kv.KVStore;
import oracle.nosql.common.kv.drl.LimiterManager;
import oracle.nosql.common.sklogger.Counter;
import oracle.nosql.common.sklogger.LongGauge;
import oracle.nosql.common.sklogger.MetricProcessor;
import oracle.nosql.common.sklogger.MetricRegistry;
import oracle.nosql.common.sklogger.PerfQuantile;
import oracle.nosql.common.sklogger.SizeQuantile;
import oracle.nosql.common.sklogger.SkLogger;


/**
 * All registered Monitor metrics
 */
public class MonitorStats {

    private static String PROXY_OCI_PROCESSOR_FACTORY =
        "oracle.nosql.proxy.t2.TelemetryProcessorFactory";

    /*
     * Proxy active request metrics
     */
    public static final String ACTIVE_REQUEST_NAME = "activeRequests";

    /*
     * Proxy request operation metrics
     */
    public static final String[] REQUEST_LABELS =
        new String[] { "opType" };
    public static final String[] DRIVER_REQUEST_LABELS =
        new String[] { "lang", "proto" };
    public static final String REQUEST_TOTAL_NAME =
        "totalOps";
    public static final String DRIVER_REQUEST_TOTAL_NAME =
        "driverOps";
    public static final String REQUEST_LATENCY_NAME =
        "latencyMs";
    public static final String FAILED_REQUEST_LATENCY_NAME =
        "failedLatencyMs";
    public static final String REQUEST_SERVER_FAILED_NAME =
        "failedOps";
    public static final String REQUEST_THROTTLING_FAILED_NAME =
        "throttledOps";
    public static final String ACTIVE_WORKER_THREADS_NAME =
        "activeWorkThreads";

    /*
     * Proxy data operation charged metrics
     */
    public static final String DATA_RESPONSE_READ_SIZE_NAME =
        "readKB";
    public static final String DATA_RESPONSE_WRITE_SIZE_NAME =
        "writeKB";

    /* The ceiling of tracked value in a Perf histogram array */
    private static int LATENCY_MAX_TRACKED = 5000;

    /*
     * Proxy active request metrics
     */
    private LongGauge activeRequest;

    /*
     * number of currently active worker threads. This is different
     * from number of active requests when running in async mode.
     */
    private SizeQuantile activeWorkerThreads;

    /*
     * Proxy request operation metrics
     */
    private Counter requestTotal;
    private PerfQuantile requestLatency;
    private PerfQuantile failedRequestLatency;
    private Counter requestServerFailed;
    private Counter requestThrottlingFailed;

    /*
     * Requests tracked by driver type (java, python, etc)
     */
    private Counter driverRequestTotal;

    /*
     * Proxy data operation charged metrics
     */
    private SizeQuantile dataResponseReadSize;
    private SizeQuantile dataResponseWriteSize;

    /*
     * Proxy kvstore clients metrics
     */
    private KVHandleStats kvHandleStats;
    private boolean isKVHandleStatsEnabled = true;

    /*
     * DDoS metrics
     */
    public static final String DELAYED_RESPONSES = "delayedResponses";
    public static final String DELAYED_RESPONSE_IPS = "delayedResponseIPs";
    public static final String DID_NOT_RESPONDS = "didNotResponds";
    public static final String DID_NOT_RESPOND_IPS = "didNotRespondIPs";
    private LongGauge delayedResponses;
    private LongGauge delayedResponseIPs;
    private LongGauge didNotResponds;
    private LongGauge didNotRespondIPs;

    /*
     * Distributed Rate Limiting (DRL) metrics
     */
    private LimiterManagerStats limiterManagerStats;

    /*
     * Internal retry metrics
     */
    public static final String INTERNAL_RETRIES = "internalRetries";
    private Counter internalRetries;

    public MonitorStats(SkLogger metricLogger) {

        /*
         * Proxy active request metrics
         */
        activeRequest = MetricRegistry.getLongGauge(ACTIVE_REQUEST_NAME);

        /*
         * Proxy request operation metrics
         */
        requestTotal =
            MetricRegistry.getCounter(REQUEST_TOTAL_NAME,
                                      REQUEST_LABELS);
        requestLatency =
            getPerfQuantile(REQUEST_LATENCY_NAME,
                            LATENCY_MAX_TRACKED,
                            REQUEST_LABELS);
        failedRequestLatency =
            getPerfQuantile(FAILED_REQUEST_LATENCY_NAME,
                            LATENCY_MAX_TRACKED,
                            REQUEST_LABELS);
        requestServerFailed =
            MetricRegistry.getCounter(REQUEST_SERVER_FAILED_NAME,
                                      REQUEST_LABELS);
        requestThrottlingFailed =
            MetricRegistry.getCounter(REQUEST_THROTTLING_FAILED_NAME,
                                      REQUEST_LABELS);
        internalRetries =
            MetricRegistry.getCounter(INTERNAL_RETRIES, REQUEST_LABELS);
        for(OperationType type : OperationType.values()) {
            requestTotal.labels(type.getValue());
            requestLatency.labels(type.getValue());
            failedRequestLatency.labels(type.getValue());
            requestServerFailed.labels(type.getValue());
            requestThrottlingFailed.labels(type.getValue());
            internalRetries.labels(type.getValue());
        }

        driverRequestTotal =
            MetricRegistry.getCounter(DRIVER_REQUEST_TOTAL_NAME,
                                      DRIVER_REQUEST_LABELS);
        for (DriverLang dLang : DriverLang.values()) {
            for (DriverProto dProto : DriverProto.values()) {
                String[] arr =
                    new String[]{dLang.getValue(), dProto.getValue()};
                driverRequestTotal.labels(arr);
            }
        }

        activeWorkerThreads =
            MetricRegistry.getSizeQuantile(ACTIVE_WORKER_THREADS_NAME);

        /*
         * Proxy data operation charged metrics
         */
        dataResponseReadSize = MetricRegistry.getSizeQuantile(
            DATA_RESPONSE_READ_SIZE_NAME);
        dataResponseWriteSize = MetricRegistry.getSizeQuantile(
            DATA_RESPONSE_WRITE_SIZE_NAME);

        if (isKVHandleStatsEnabled) {
            kvHandleStats = new KVHandleStats(
                metricLogger, MetricRegistry.defaultRegistry);
        }

        delayedResponses = MetricRegistry.getLongGauge(DELAYED_RESPONSES);
        delayedResponseIPs = MetricRegistry.getLongGauge(DELAYED_RESPONSE_IPS);
        didNotResponds = MetricRegistry.getLongGauge(DID_NOT_RESPONDS);
        didNotRespondIPs = MetricRegistry.getLongGauge(DID_NOT_RESPOND_IPS);
    }

    public void setLimiterManager(LimiterManager lm) {
        if (limiterManagerStats == null && lm != null) {
            limiterManagerStats = new LimiterManagerStats(
                MetricRegistry.defaultRegistry, lm);
        }
    }

    /**
     * Register KVStore handle to start monitoring that kvclient metrics.
     */
    public void register(String storeName, KVStore store) {
        if (kvHandleStats == null) {
            return;
        }
        kvHandleStats.register(storeName, store);
    }

    /**
     * Unregister to stop monitoring that kvclient metrics.
     */
    public void unregister(String storeName) {
        if (kvHandleStats == null) {
            return;
        }
        kvHandleStats.unregister(storeName);
    }

    /**
     * Mark an operation started.
     */
    public void markOpStart() {
        activeRequest.incrValue();
    }

    /**
     * Mark the data operation succeeded with the data operation start time,
     * the type of DataRequest, how many data operations processed, read
     * and write KB size is charged. Negative dataOpCount means the data
     * operation is failed.
     */
    public void markDataOpSucceeded(long startTime,
                                    OperationType type,
                                    int dataOpCount,
                                    int readKB,
                                    int writeKB,
                                    DriverLang dLang,
                                    DriverProto dProto,
                                    int retries) {
        activeRequest.decrValue();
        final String[] opLabelValues = type.getValue();
        requestTotal.labels(opLabelValues).incrValue();
        if (retries > 0) {
            internalRetries.labels(opLabelValues).incrValue(retries);
        }
        incrementDriverRequestTotal(dLang, dProto);
        if (readKB > 0) {
            dataResponseReadSize.observe(readKB);
        }
        if (writeKB > 0) {
            dataResponseWriteSize.observe(writeKB);
        }
        trackOpPerf(startTime, dataOpCount, requestLatency,
                    requestServerFailed, opLabelValues);
    }

    public void markOpActiveWorkerThreads(int threads) {
        activeWorkerThreads.observe(threads);
    }

    /**
     * Mark the TM operation succeeded with the TM operation start time, the
     * type of TMRequest, how many TM operations processed. Negative
     * tmOpCount means the TM operation is failed.
     */
    public void markTMOpSucceeded(long startTime,
                                  OperationType type,
                                  DriverLang dLang,
                                  DriverProto dProto,
                                  int tmOpCount,
                                  int retries) {
        /*
         * measure TM operation metrics
         * TODO
         * 1) Drill down OP_TABLE_REQUEST to detailed table ddl operation?
         * 2) Latency for OP_LIST_TABLES is better to per request or table?
         */
        String[] opLabelValues = type.getValue();
        activeRequest.decrValue();
        requestTotal.labels(opLabelValues).incrValue();
        if (retries > 0) {
            internalRetries.labels(opLabelValues).incrValue(retries);
        }
        incrementDriverRequestTotal(dLang, dProto);
        trackOpPerf(startTime, tmOpCount, requestLatency,
                    requestServerFailed, opLabelValues);
    }

    /**
     * Mark the operation failed.
     * @param failureType 1 means serverFailure, 2 means userThrottling, 3 means
     * userFailure.
     */
    public void markOpFailed(long startTime,
                             OperationType type,
                             int failureType,
                             DriverLang dLang,
                             DriverProto dProto,
                             int retries) {
        activeRequest.decrValue();
        incrementDriverRequestTotal(dLang, dProto);
        if (type == null) {
            return;
        }
        final String[] opLabelValues = type.getValue();
        requestTotal.labels(opLabelValues).incrValue();
        if (retries > 0) {
            internalRetries.labels(opLabelValues).incrValue(retries);
        }
        /* keep latency measurements for failed ops */
        trackOpPerf(startTime, 1, failedRequestLatency,
                    requestServerFailed, opLabelValues);
        if (failureType == 1) {
            requestServerFailed.labels(opLabelValues).incrValue();
        } else if (failureType == 2) {
            requestThrottlingFailed.labels(opLabelValues).incrValue();
        } else {
            /* don't track user failure type or unknown failure type */
        }
    }

    public void trackDelayedResponses(long num) {
        delayedResponses.setValue(num);
    }

    public void trackDidNotResponds(long num) {
        didNotResponds.setValue(num);
    }

    public void trackDelayedResponseIPs(long numIPs) {
        delayedResponseIPs.setValue(numIPs);
    }

    public void trackDidNotRespondIPs(long numIPs) {
        didNotRespondIPs.setValue(numIPs);
    }

    /**
     * We use reflection to get an instance of the cloud-specific processor
     * in order to confine all third party OCI libraries to the spartakv
     * repo, and to keep them out of this httpproxy repo.
     */
    public static MetricProcessor getCloudProcessor(SkLogger logger) {
        Class<?> monitorClass;
        try {
            monitorClass = Class.forName(PROXY_OCI_PROCESSOR_FACTORY);
        } catch (ClassNotFoundException cnfe) {
            logger.log(Level.SEVERE,
                       "Unable to find " + PROXY_OCI_PROCESSOR_FACTORY +
                       " class", cnfe);
            throw new IllegalArgumentException(
                       "Unable to find " + PROXY_OCI_PROCESSOR_FACTORY +
                       " class", cnfe);
        }

        try {
            Method m = monitorClass.getMethod("getProcessor",
                                              String.class,
                                              SkLogger.class);
            return (MetricProcessor) m.invoke(null, "proxy", logger);
        } catch (Throwable t) {
            logger.log(Level.SEVERE,
                       "Unable to call " + PROXY_OCI_PROCESSOR_FACTORY +
                       ".getProcessor", t);
            throw new IllegalArgumentException(
                       "Unable to call " + PROXY_OCI_PROCESSOR_FACTORY +
                       ".getProcessor", t);
        }
    }

    /*
     * Track operation latency perf. Negative opCount means the operation is
     * failed.
     */
    private void trackOpPerf(long startTime,
                             int opCount,
                             PerfQuantile perf,
                             Counter failedCounter,
                             String[] opLabelValues) {

        final long elapsed = System.nanoTime() - startTime;
        if (opCount > 0) {
            final long avgElapsed = elapsed / opCount;
            perf.labels(opLabelValues).observeNanoLatency(avgElapsed, opCount);
        } else if (opCount == 0) {
            perf.labels(opLabelValues).observeNanoLatency(elapsed, 1);
        } else {
            failedCounter.labels(opLabelValues).incrValue();
        }
    }

    private static PerfQuantile getPerfQuantile(String name,
                                                int maxTracked,
                                                String... labelNames) {
        PerfQuantile pq = new PerfQuantile(name, maxTracked, labelNames);
        MetricRegistry.defaultRegistry.register(name, pq);
        return pq;
    }

    /**
     * For request operations metric labels.
     */
    public enum OperationType {
        DELETE("delete"),
        GET("get"),
        PUT("put"),
        QUERY("query"),
        PREPARE("prepare"),
        SCAN("scan"),
        INDEX_SCAN("indexScan"),
        WRITE_MULTIPLE("writeMultiple"),
        MULTI_DELETE("multiDelete"),
        TABLE_REQUEST("tableRequest"),
        GET_TABLE("getTable"),
        GET_TABLE_USAGE("tableUsage"),
        LIST_TABLES("listTable"),
        GET_INDEXES("getIndex"),
        /* new in V2 */
        SYSTEM_REQUEST("systemRequest"),
        SYSTEM_STATUS_REQUEST("systemStatusRequest"),
        UNKNOWN("unknown"),
        GET_WORKREQUEST("getWorkRequest"),
        LIST_WORKREQUESTS("listWorkRequests"),
        LIST_WORKREQUEST_LOGS("listWorkRequestLogs"),
        LIST_WORKREQUEST_ERRORS("listWorkRequestErrors"),
        CANCEL_WORKREQUEST("cancelWorkRequest");

        private final String[] values;

        private OperationType(final String ...values) {
            this.values = values;
        }

        public String[] getValue() {
            return values;
        }
    }

    private void incrementDriverRequestTotal(final DriverLang dLang,
                                             final DriverProto dProto) {
        String[] arr =
            new String[]{dLang.getValue(), dProto.getValue()};
        driverRequestTotal.labels(arr).incrValue();
    }

    public enum DriverLang {
        JAVA("java"),
        SPRING("spring"),
        PYTHON("python"),
        GO("go"),
        NODE("node"),
        CSHARP("csharp"),
        CURL("curl"),
        BROWSER("browser"),
        OTHER("other");

        private final String value;

        private DriverLang(final String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        /**
         * Get the driver type from the User-Agent header.
         */
        public static DriverLang parse(String userAgent) {
            if (userAgent == null || userAgent.isEmpty()) {
                return DriverLang.OTHER;
            }
            /*
             * These work for both the binary protocol and for OCI SDKs.
             * Binary protocol sets "NoSQL-<Lang>SDK", and
             * OCI sets "Oracle-<Lang>SDK".
             */
            if (userAgent.contains("JavaSDK")) {
                if (userAgent.contains("SpringSDK")) {
                    return DriverLang.SPRING;
                }
                return DriverLang.JAVA;
            }
            if (userAgent.contains("PythonSDK")) {
                return DriverLang.PYTHON;
            }
            if (userAgent.contains("NodeSDK")) {
                return DriverLang.NODE;
            }
            if (userAgent.contains("GoSDK")) {
                return DriverLang.GO;
            }
            if (userAgent.contains("DotNetSDK")) {
                return DriverLang.CSHARP;
            }
            if (userAgent.contains("curl")) {
                return DriverLang.CURL;
            }
            /*
             * At this point, it's most likely a browser. We could
             * do more parsing, and look for strings like "Safari" and
             * "Firefox" and "Chrome", etc... but that starts to get
             * pretty ugly. Maybe a FUTURE TODO.
             */
            return DriverLang.BROWSER;
        }
    }

    public enum DriverProto {
        V2("v2"), /* binary protocol V2 */
        V3("v3"), /* binary protocol V3 */
        V4("v4"), /* binary protocol V4 */
        REST("rest");

        private final String value;
        private DriverProto(final String value) {
            this.value = value;
        }
        public String getValue() {
            return value;
        }
    }
}
