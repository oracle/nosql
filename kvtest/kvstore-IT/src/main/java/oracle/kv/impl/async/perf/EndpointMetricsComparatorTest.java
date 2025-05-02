/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.async.perf;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import oracle.kv.TestBase;

import org.junit.Test;


/**
 * Test the {@link EndpointMetricsComparator}.
 */
public class EndpointMetricsComparatorTest extends TestBase {

    /**
     * Tests the comparator works fine with zeros values.
     */
    @Test
    public void testWithZeroValues() {
        final List<EndpointMetricsImpl> metricsList = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            metricsList.add(
                new EndpointMetricsImpl(
                    "name", true, 0.0f, 0.0f, 0.0f, 0.0f,
                    new MetricStatsImpl(0, 0, 0, 0, 0, 0),
                    new MetricStatsImpl(0, 0, 0, 0, 0, 0),
                    new MetricStatsImpl(0, 0, 0, 0, 0, 0),
                    Collections.emptyMap(), 1, Collections.emptyList()));
        }
        Collections.sort(metricsList,
                         new EndpointMetricsComparator(metricsList));
    }

    /**
     * Tests the comparator sorts as expected.
     */
    @Test
    public void testSort() {
        final List<EndpointMetricsImpl> metricsList = new ArrayList<>();
        /* frequent abort case */
        metricsList.add(
            new EndpointMetricsImpl(
                "caseFrequentAbort", true, 0.0f, 0.0f, 0.0f, 0.0f,
                new MetricStatsImpl(10, 1, 1, 1, 1, 1),
                new MetricStatsImpl(10, 1000, 1000, 1000, 1000, 1000),
                new MetricStatsImpl(20, 1, 1, 1, 1, 1),
                Collections.emptyMap(), 1, Collections.emptyList()));
        /* long latency case */
        metricsList.add(
            new EndpointMetricsImpl(
                "caseLongLatency", true, 0.0f, 0.0f, 0.0f, 0.0f,
                new MetricStatsImpl(10, 1000, 1000, 1000, 1000, 1000),
                new MetricStatsImpl(0, 0, 0, 0, 0, 0),
                new MetricStatsImpl(10, 1, 1, 1, 1, 1),
                Collections.emptyMap(), 1, Collections.emptyList()));
        /* normal case */
        metricsList.add(
            new EndpointMetricsImpl(
                "caseNormal", true, 0.0f, 0.0f, 0.0f, 0.0f,
                new MetricStatsImpl(10, 1, 1, 1, 1, 1),
                new MetricStatsImpl(1, 10, 10, 10, 10, 10),
                new MetricStatsImpl(11, 1, 1, 1, 1, 1),
                Collections.emptyMap(), 1, Collections.emptyList()));
        final EndpointMetricsComparator comparator =
            new EndpointMetricsComparator(metricsList);
        Collections.sort(metricsList, comparator);

        assertEquals(metricsScoresToString(metricsList, comparator),
                     "caseFrequentAbort", metricsList.get(0).getName());
        assertEquals(metricsScoresToString(metricsList, comparator),
                     "caseLongLatency", metricsList.get(1).getName());
        assertEquals(metricsScoresToString(metricsList, comparator),
                     "caseNormal", metricsList.get(2).getName());
    }

    private String metricsScoresToString(List<EndpointMetricsImpl> list,
                                         EndpointMetricsComparator c) {
        final StringBuilder sb = new StringBuilder();
        for (EndpointMetricsImpl m : list) {
            sb.append(String.format(
                "%s(%s, finished=%s, aborted=%s) ",
                m.getName(), c.score(m),
                m.getFinishedDialogLatencyNanos(),
                m.getAbortedDialogLatencyNanos()));
        }
        return sb.toString();
    }
}

