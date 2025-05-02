/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.sklogger;

import java.util.ArrayList;
import java.util.List;

import oracle.nosql.common.sklogger.CustomGauge.GaugeCalculator;
import oracle.nosql.common.sklogger.measure.MeasureElement;
import oracle.nosql.common.sklogger.measure.MeasureRegistry;

/**
 * Registry metrics to MetricRegistry, and install {@link MetricProcessor} such
 * as {@link SkLogger} to handle these registered metrics repeatedly at given
 * interval.
 * <pre>
 * Usage example:
 * {@code
 * static Counter requestCount = MetricRegistry.getCounter("requestCount");
 * static PerfQuantile requestLatency =
 *      MetricRegistry.getPerfQuantile("requestLatency");
 * MetricRegistry.defaultRegistry.addMetricProcessor(SkLogger);
 * MetricRegistry.defaultRegistry.startProcessors(MONITOR_INTERVAL);
 * }
 * It will use SkLogger to repeat logging requestCount and requestLatency on
 * MONITOR_INTERVAL interval.
 * </pre>
 */
public class MetricRegistry extends MeasureRegistry {
    /**
     * The default registry.
     */
    public static final MetricRegistry defaultRegistry = new MetricRegistry();

    /* Convenience methods for defaultRegistry */

    /**
     * Get LongGauge and register it to {@link #defaultRegistry}.
     * @see LongGauge#LongGauge(String, String...)
     */
    public static LongGauge getLongGauge(final String name,
                                         String... labelNames) {
       return defaultRegistry.register(
           name, new LongGauge(name, labelNames));
    }

    /**
     * Get CustomGauge and register it to {@link #defaultRegistry}.
     * @see CustomGauge#CustomGauge(String, GaugeCalculator)
     */
    public static CustomGauge getCustomGauge(String name,
                                             GaugeCalculator gaugeCalculator) {
        return defaultRegistry.register(
            name, new CustomGauge(name, gaugeCalculator));
    }

    /**
     * Get Counter and register it to {@link #defaultRegistry}.
     * @see Counter#Counter(String, String...)
     */
    public static Counter getCounter(String name, String... labelNames) {
        return defaultRegistry.register(
            name, new Counter(name, labelNames));
    }

    /**
     * Get SizeQuantile and register it to {@link #defaultRegistry}.
     * @see SizeQuantile#SizeQuantile(String, String...)
     */
    public static SizeQuantile getSizeQuantile(String name,
                                               String... labelNames) {
        return defaultRegistry.register(
            name, new SizeQuantile(name, labelNames));
    }

    /**
     * Get SizeQuantile and register it to {@link #defaultRegistry}.
     * @see SizeQuantile#SizeQuantile(String, double[], String...)
     */
    public static SizeQuantile getSizeQuantile(String name,
                                               double[] quantiles,
                                               String... labelNames) {
        return defaultRegistry.register(
            name, new SizeQuantile(name, quantiles, labelNames));
    }

    /**
     * Get Histogram and register it to {@link #defaultRegistry}.
     * @see Histogram#Histogram(String, long[], String...)
     */
    public static Histogram getHistogram(String name,
                                         long[] upperBounds,
                                         String... labelNames) {
        return defaultRegistry.register(
            name, new Histogram(name, upperBounds, labelNames));
    }

    public LongGauge register(LongGauge metric) {
        return register(metric.getStatsName(), metric);
    }

    public Counter register(Counter metric) {
        return register(metric.getStatsName(), metric);
    }

    public Histogram register(Histogram metric) {
        return register(metric.getStatsName(), metric);
    }

    public SizeQuantile register(SizeQuantile metric) {
        return register(metric.getStatsName(), metric);
    }

    public CustomGauge register(CustomGauge metric) {
        return register(metric.getStatsName(), metric);
    }

    /**
     * Register a metric.
     */
    public void addMetricData(Metric<?, ?> metric) {
        final String statsName = metric.getStatsName();
        register(statsName, metric);
    }

    /**
     * Add {@link MetricProcessor} to handle registered metrics.
     */
    public void addMetricProcessor(MetricProcessor processor) {
        /*
         * In the old code, there is only one watcherName that processors
         * associated to
         */
        add(MetricRegistry.class.getSimpleName(), processor);
    }

    /**
     * Start all installed {@link MetricProcessor} to handle registered metrics
     * repeatedly at the given configuredIntervalMs interval.
     */
    public synchronized void startProcessors(long configuredIntervalMs) {
        start(MetricRegistry.class.getSimpleName(), configuredIntervalMs);
    }

    public synchronized void stopProcessors() {
        stop(MetricRegistry.class.getSimpleName());
    }

    //TODO support stream API instead of getAll?
    public List<MetricFamilySamples<?>>
        getAllMetricFactory(String watcherName) {

        final List<MetricFamilySamples<?>> metricFamilies = new ArrayList<>();
        for (MeasureElement<?> element : elementMap.values()) {
            if (!(element instanceof Metric)) {
                /* Check exists due to API compatibility reasons */
                continue;
            }
            final Metric<?,?> metric = (Metric<?,?>) element;
            metricFamilies.add(metric.obtain(watcherName, true));
        }
        return metricFamilies;
    }
}
