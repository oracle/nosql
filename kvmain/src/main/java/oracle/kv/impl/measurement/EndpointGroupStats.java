/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.measurement;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import oracle.kv.impl.async.IOBufferPool;
import oracle.kv.impl.async.perf.DialogResourceManagerMetrics;
import oracle.kv.impl.async.perf.EndpointMetricsComparator;
import oracle.kv.impl.async.perf.EndpointMetricsImpl;
import oracle.kv.impl.util.ObjectUtil;
import oracle.nosql.common.sklogger.measure.ThroughputElement;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.sleepycat.utilint.Latency;


/**
 * Endpoint group stats.
 */
@SuppressWarnings("deprecation")
public class EndpointGroupStats
    implements oracle.kv.stats.EndpointGroupMetrics,
               ConciseStats, Serializable {

    private static final long serialVersionUID = 1L;

    private static final long ONE_SECOND_IN_NANOS = 1_000_000_000L;

    private final long intervalStart;
    private final long intervalEnd;
    /*
     * TODO: Make these field final and remove readObject method with the 23.1
     * release.
     */
    private volatile Map<String, EndpointMetricsImpl> creatorMetricsMap;
    private volatile List<EndpointMetricsImpl> sortedCreatorMetricsList;
    private volatile Map<String, EndpointMetricsImpl> responderMetricsMap;
    private volatile List<EndpointMetricsImpl> sortedResponderMetricsList;
    private volatile DialogResourceManagerMetrics dialogResourceManagerMetrics;

    private final IOBufferPool.InUsePercentMetrics
        ioBufferPoolInUsePercentMetrics;

    /*
     * TODO: Remove the following fields with the 23.1 release.
     */
    @SuppressWarnings("unused")
    private final Map<String, oracle.kv.stats.EndpointMetrics>
        creatorMetrics = Collections.emptyMap();
    @SuppressWarnings("unused")
    private final List<oracle.kv.stats.EndpointMetrics>
        sortedCreatorMetrics = Collections.emptyList();
    @SuppressWarnings("unused")
    private final Map<String, oracle.kv.stats.EndpointMetrics>
        responderMetrics = Collections.emptyMap();
    @SuppressWarnings("unused")
    private final List<oracle.kv.stats.EndpointMetrics>
        sortedResponderMetrics = Collections.emptyList();
    @SuppressWarnings("unused")
    private final Map<String, Latency> latencyInfoMap = Collections.emptyMap();
    @SuppressWarnings("unused")
    private final Map<String, CounterGroup> counterInfoMap =
        Collections.emptyMap();
    @SuppressWarnings("unused")
    private final Map<String, List<String>> sampledDialogInfoMap =
        Collections.emptyMap();

    public static class CounterGroup implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    public EndpointGroupStats(
        long intervalStart,
        long intervalEnd,
        Map<String, EndpointMetricsImpl> creatorMetricsMap,
        Map<String, EndpointMetricsImpl> responderMetricsMap,
        DialogResourceManagerMetrics dialogResourceManagerMetrics,
        IOBufferPool.InUsePercentMetrics ioBufPoolInUsePercentMetrics) {

        ObjectUtil.checkNull("creatorMetricsMap", creatorMetricsMap);
        ObjectUtil.checkNull("responderMetricsMap", responderMetricsMap);
        ObjectUtil.checkNull("dialogResourceManagerMetrics",
                             dialogResourceManagerMetrics);

        this.intervalStart = intervalStart;
        this.intervalEnd = intervalEnd;
        this.creatorMetricsMap = creatorMetricsMap;
        this.sortedCreatorMetricsList =
            new ArrayList<>(creatorMetricsMap.values());
        Collections.sort(
            sortedCreatorMetricsList,
            new EndpointMetricsComparator(sortedCreatorMetricsList));
        this.responderMetricsMap = responderMetricsMap;
        this.sortedResponderMetricsList =
            new ArrayList<>(responderMetricsMap.values());
        Collections.sort(
            sortedResponderMetricsList,
            new EndpointMetricsComparator(sortedResponderMetricsList));
        this.dialogResourceManagerMetrics = dialogResourceManagerMetrics;
        this.ioBufferPoolInUsePercentMetrics = ioBufPoolInUsePercentMetrics;
    }

    @Override
    public long getStart() {
        return intervalStart;
    }

    @Override
    public long getEnd() {
        return intervalEnd;
    }

    @Override
    public String getFormattedStats() {
        return getFormattedStats(Integer.MAX_VALUE);
    }

    public String getFormattedStats(int maxNumEndpoints) {
        return toJson(maxNumEndpoints).toString();
    }

    public JsonObject toJson() {
        return toJson(Integer.MAX_VALUE);
    }

    public JsonObject toJson(int maxNumEndpoints) {
        final JsonObject result = new JsonObject();
        result.add("creatorEndpointMetrics",
                   getCreatorEndpointMetrics().stream()
                   .limit(maxNumEndpoints)
                   .map((m) ->
                        new AbstractMap.SimpleImmutableEntry
                        <String, JsonElement>(
                            m.getName(), m.toJson()))
                   .collect(StatsUtils.getObjectCollector()));
        result.add("responderEndpointMetrics",
                   getResponderEndpointMetrics().stream()
                   .limit(maxNumEndpoints)
                   .map((m) ->
                        new AbstractMap.SimpleImmutableEntry
                        <String, JsonElement>(
                            m.getName(), m.toJson()))
                   .collect(StatsUtils.getObjectCollector()));
        /*
         * TODO: remove null check with the 24.1 release.
         */
        if (ioBufferPoolInUsePercentMetrics != null) {
            result.add("ioBufferPoolInUsePercentMetrics",
                       ioBufferPoolInUsePercentMetrics.toJson());
        }
        return result;
    }

    @Override
    public String getSummarizedStats() {
        return toSummarizedJson().toString();
    }

    public JsonObject toSummarizedJson() {
        final JsonObject result = new JsonObject();
        result.add("creatorEndpointSummary",
                   getEndpointsSummaryJson(getCreatorEndpointMetrics()));
        result.add("responderEndpointSummary",
                   getEndpointsSummaryJson(getResponderEndpointMetrics()));
        return result;
    }

    private JsonObject getEndpointsSummaryJson(
        List<EndpointMetricsImpl> metrics)
    {
        final JsonObject result = new JsonObject();
        result.addProperty("numEndpoints", metrics.size());
        if (metrics.size() > 0) {
            result.add("summary",
                       computeEndpointsSummaryJson(metrics));
        }
        return result;
    }

    private JsonObject computeEndpointsSummaryJson(
        List<EndpointMetricsImpl> metrics)
    {
        final JsonObject result = new JsonObject();
        result.add("dialogStartThroughput",
                   computeThroughputSummaryJson(
                       metrics, m -> m.getDialogStartThroughput()));
        result.add("dialogDropThroughput",
                   computeThroughputSummaryJson(
                       metrics, m -> m.getDialogDropThroughput()));
        result.add("dialogFinishThroughput",
                   computeThroughputSummaryJson(
                       metrics, m -> m.getDialogFinishThroughput()));
        result.add("dialogAbortThroughput",
                   computeThroughputSummaryJson(
                       metrics, m -> m.getDialogAbortThroughput()));
        result.add("avgFinishedDialogLatencyNanos",
                   computeLongValueSummaryJson(
                       metrics,
                       m -> m.getFinishedDialogLatencyNanos().getAverage()));
        result.add("avgAbortedDialogLatencyNanos",
                   computeLongValueSummaryJson(
                       metrics,
                       m -> m.getAbortedDialogLatencyNanos().getAverage()));
        result.add("avgDialogConcurrency",
                   computeLongValueSummaryJson(
                       metrics, m -> m.getDialogConcurrency().getAverage()));
        return result;
    }

    private JsonElement computeThroughputSummaryJson(
        List<EndpointMetricsImpl> metrics,
        Function<oracle.kv.stats.EndpointMetrics, Double> mapper)
    {
        final List<Double> values = new ArrayList<>();
        metrics.forEach((m) -> values.add(mapper.apply(m)));
        final int n = values.size();
        if (n == 0) {
            return JsonNull.INSTANCE;
        }
        final JsonObject result = new JsonObject();
        Collections.sort(values);

        final double min = values.get(0);
        final double avg = values.stream().reduce(0.0, Double::sum) / n;
        final double mean = values.get((n % 2 == 0) ? n / 2 - 1 : n / 2);
        final double max = values.get(n - 1);

        result.add(
            "min",
            (new ThroughputElement.Result(
                (long) min, ONE_SECOND_IN_NANOS))
            .toJson().getElement());
        result.add(
            "avg",
            (new ThroughputElement.Result(
                (long) avg, ONE_SECOND_IN_NANOS))
            .toJson().getElement());
        result.add(
            "mean",
            (new ThroughputElement.Result(
                (long) mean, ONE_SECOND_IN_NANOS))
            .toJson().getElement());
        result.add(
            "max",
            (new ThroughputElement.Result(
                (long) max, ONE_SECOND_IN_NANOS))
            .toJson().getElement());
        return result;
    }

    private JsonElement computeLongValueSummaryJson(
        List<EndpointMetricsImpl> metrics,
        Function<oracle.kv.stats.EndpointMetrics, Long> mapper)
    {
        final List<Long> values = new ArrayList<>();
        metrics.forEach((m) -> values.add(mapper.apply(m)));
        final int n = values.size();
        if (n == 0) {
            return JsonNull.INSTANCE;
        }
        final JsonObject result = new JsonObject();
        Collections.sort(values);

        final long min = values.get(0);
        final long avg = Math.round(
            values.stream()
            /* Compute with double to avoid overflow. */
            .map((v) -> (double) v)
            .reduce(0.0, Double::sum) / n);
        final long mean = values.get((n % 2 == 0) ? n / 2 - 1 : n / 2);
        final long max = values.get(n - 1);

        result.addProperty("min", min);
        result.addProperty("avg", avg);
        result.addProperty("mean", mean);
        result.addProperty("max", max);
        return result;
    }

    @Override
    public String toString() {
        return getSummarizedStats();
    }

    public List<EndpointMetricsImpl> getCreatorEndpointMetrics() {
        return sortedCreatorMetricsList;
    }

    public List<EndpointMetricsImpl> getResponderEndpointMetrics() {
        return sortedResponderMetricsList;
    }

    @Override
    public List<oracle.kv.stats.EndpointMetrics> getEndpointMetricsList() {
        final List<oracle.kv.stats.EndpointMetrics> ret = new ArrayList<>();
        ret.addAll(getCreatorEndpointMetrics());
        ret.addAll(getResponderEndpointMetrics());
        return ret;
    }

    @Override
    public oracle.kv.stats.EndpointMetrics getEndpointMetrics(String address) {
        if (creatorMetricsMap.containsKey(address)) {
            return creatorMetricsMap.get(address);
        }
        return responderMetricsMap.get(address);
    }

    public DialogResourceManagerMetrics getDialogResourceManagerMetrics() {
        return dialogResourceManagerMetrics;
    }

    /**
     * Initialize the null fields because the object was serialized from a
     * version prior to 20.2 when the field was added.
     */
    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {

        in.defaultReadObject();
        if (creatorMetricsMap == null) {
            creatorMetricsMap = Collections.emptyMap();
        }
        if (sortedCreatorMetricsList == null) {
            sortedCreatorMetricsList = Collections.emptyList();
        }
        if (responderMetricsMap == null) {
            responderMetricsMap = Collections.emptyMap();
        }
        if (sortedResponderMetricsList == null) {
            sortedResponderMetricsList = Collections.emptyList();
        }
        if (dialogResourceManagerMetrics == null) {
            dialogResourceManagerMetrics =
                new DialogResourceManagerMetrics(0, 0);
        }
    }
}
