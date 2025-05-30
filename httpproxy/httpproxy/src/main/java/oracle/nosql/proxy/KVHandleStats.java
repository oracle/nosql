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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import oracle.kv.KVStore;
import oracle.kv.stats.KVStats;
import oracle.kv.stats.NodeMetrics;
import oracle.kv.stats.OperationMetrics;
import oracle.nosql.common.sklogger.CustomGauge;
import oracle.nosql.common.sklogger.CustomGauge.GaugeCalculator;
import oracle.nosql.common.sklogger.CustomGauge.GaugeResult;
import oracle.nosql.common.sklogger.MetricRegistry;
import oracle.nosql.common.sklogger.SkLogger;

/**
 * Collect registered KVStore client metrics to monitoring.
 */
public class KVHandleStats {

    /*
     * KVStore handle metric name
     */
    public static final String KV_HANDLE_NAME = "kvHandle";

    private final SkLogger metricLogger;
    /*
     * store name => handle
     * All registered KVStore handles that will be used to collect their client
     * metrics to monitoring.
     */
    private final Map<String, KVStore> handleMap;

    public KVHandleStats(SkLogger metricLogger, MetricRegistry registry) {
        this.metricLogger = metricLogger;
        handleMap = new ConcurrentHashMap<String, KVStore>();
        /*
         * Register the handleStatsGauge metric so that SKLogger will output
         * it at background automatically.
         */
        CustomGauge handleStatsGauge =
            new CustomGauge(KV_HANDLE_NAME,
                            new HandleStatsCalculator());
        registry.register(handleStatsGauge);
    }

    public void register(String storeName, KVStore store) {
        handleMap.put(storeName, store);
    }

    public void unregister(String storeName) {
        handleMap.remove(storeName);
    }

    /*
     * Get the latest KVStats from all KVStores and then map them to a list of
     * metrics maps.
     */
    private class HandleStatsCalculator implements GaugeCalculator {

        @Override
        public List<GaugeResult> getValuesList() {
            List<GaugeResult> resultList = new ArrayList<GaugeResult>();
            for(Entry<String, KVStore> entry : handleMap.entrySet()) {
                String storeName = entry.getKey();
                KVStore handle = entry.getValue();
                KVStats stats = null;
                try {
                    stats = handle.getStats(true /* clear */);
                } catch(Exception e) {
                    /*
                     * Just skip if any exception, such as a race condition
                     * where the store is closing.
                     */
                    continue;
                }
                /* add each node metrics */
                List<NodeMetrics> nodeMetrics = stats.getNodeMetrics();
                for (NodeMetrics metric : nodeMetrics) {
                    Map<String, Object> map = getMap(metric);
                    addStoreName(map, storeName);
                    resultList.add(new GaugeResult(map));
                }

                /* add client total retry count */
                final long requestRetryCount = stats.getRequestRetryCount();
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("requestRetryCount", requestRetryCount);
                addStoreName(map, storeName);
                resultList.add(new GaugeResult(map));

                /* add each operation metrics */
                for (OperationMetrics operationMetrics : stats.getOpMetrics()) {
                    if (operationMetrics.getTotalOpsLong() == 0) {
                        /*
                         * Operations with no count are skipped, partly because
                         * the proxy doesn't even call many of the operations
                         * available.
                         */
                        continue;
                    }
                    map = getMap(operationMetrics);
                    addStoreName(map, storeName);
                    resultList.add(new GaugeResult(map));
                }

                if (metricLogger.isLoggable(Level.FINE)) {
                    /*
                     * These metrics counts are large, only log at FINE level.
                     */
                    metricLogger.severe(
                        stats.getDialogEndpointGroupPerf().toString());
                    metricLogger.severe(
                        stats.getNioChannelThreadPoolPerf().toString());
                }
            }

            return resultList;
        }

        private Map<String, Object> getMap(NodeMetrics metric) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("resource", metric.getNodeName());
            map.put("zoneName", metric.getZoneName());
            map.put("isActive", metric.isActive());
            map.put("isMaster", metric.isMaster());
            map.put("maxActiveRequest",
                    metric.getMaxActiveRequestCount());
            map.put("avgRespTime", metric.getAvLatencyMs());
            map.put("requestCount", metric.getRequestCount());
            map.put("failedRequestCount",
                    metric.getFailedRequestCount());
            return map;
        }

        private Map<String, Object> getMap(OperationMetrics metric) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("operationName", metric.getOperationName());
            map.put("requestCount", metric.getTotalRequestsLong());
            map.put("operationCount", metric.getTotalOpsLong());
            map.put("quantile_0.95", metric.get95thLatencyMs());
            map.put("quantile_0.99", metric.get99thLatencyMs());
            map.put("operationAvg", metric.getAverageLatencyMs());
            map.put("operationMax", metric.getMaxLatencyMs());
            map.put("operationMin", metric.getMinLatencyMs());
            return map;
        }

        private void addStoreName(Map<String, Object> map,
                                  String storeName) {
            map.put("storeName", storeName);
        }
    }
}
