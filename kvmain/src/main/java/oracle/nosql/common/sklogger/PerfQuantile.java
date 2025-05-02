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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A PerfQuantile as one kind of {@link RateMetric} is to calculate
 * avg/95th/99th of observed values. As we need set a maxTracked value for
 * PerfQuantile, and we create a histogram array that length is the maxTracked
 * value, so the maxTracked can't be too big or unknown. And the observed value
 * need be positive integer.
 * The most common usage scenario is for latency in millisecond.
 * <pre>
 * Usage example:
 * {@code
 * PerfQuantile requestLatency = new PerfQuantile("requestLatency", 5000,
 *                                                "opType");
 * add one observed read request latency.
 * requestLatency.labels("read").observe(latency);
 *
 * add one observed write request latency.
 * requestLatency.labels("write").observe(latency);
 *
 * get read request avg/95th/99th latency since last call for watcherName.
 * requestCount.labels("read").rateSinceLastTime("watcherName")
 * }
 * </pre>
*/
public class PerfQuantile
    extends RateMetric<PerfQuantile.Element, PerfQuantile.RateResult> {

    /*
     * Ceiling of tracked value
     */
    private final int maxTracked;

    /**
     *
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param maxTracked set the ceiling of tracked value in a histogram array
     * which length is maxTracked value.
     * @param labelNames to set metric tag names that is optional for Metric
     */
    public PerfQuantile(final String name,
                        int maxTracked,
                        String... labelNames) {

        super(name, labelNames);
        if (maxTracked <= 0) {
            throw new IllegalArgumentException(
                "maxTracked value must be positive.");
        }
        this.maxTracked = maxTracked;
    }

    @Override
    public Type getType() {
        return Type.PERF_QUANTILE;
    }

    @Override
    protected Element newElement() {
        return new Element(maxTracked);
    }

    public static final class Element implements RateMetric.Element<RateResult> {
        /*
         * The min, max, 95% and 99% values will be 0 if if there is no
         * observed value.
         */
        private static final int DEFAULT_VAL = 0;

        private final long initialTime;
        private final int maxTracked;
        //TODO change to LongAdder if this package move to JAVA 8.
        private final AtomicLong totalRequests;
        private final AtomicLong overflowRequests;
        private final AtomicLongArray cumulativeCounts;
        private final Max overallMax;
        private final ConcurrentHashMap<String, HistoryItem> watchers;

        private Element(int maxTracked) {
            this.maxTracked = maxTracked;
            totalRequests = new AtomicLong();
            overflowRequests = new AtomicLong();
            cumulativeCounts = new AtomicLongArray(maxTracked + 1);
            overallMax = new Max();
            watchers = new ConcurrentHashMap<String, HistoryItem>();
            initialTime = System.nanoTime();
        }

        /**
         * Round Latency from nanosecond to millisecond and observe millisecond
         * value
         * TODO move it to an utility class as a static method?
         */
        public void observeNanoLatency(long nanoLatency, int times) {
            if (nanoLatency < 0) {
                return;
            }
            /* Round the latency to determine where to mark the histogram. */
            final int millisRounded =
                (int) ((nanoLatency + (1000000L / 2)) / 1000000L);
            observe(millisRounded, times);
        }

        /**
         * Observe the given value.
         */
        public void observe(int val) {
            observe(val, 1);
        }

        /**
         * Observe the given value with N times.
         */
        public void observe(int val, int times) {
            if (val < 0) {
                return;
            }
            totalRequests.incrementAndGet();
            if (val < maxTracked) {
                cumulativeCounts.addAndGet(val, times);
            } else {
                cumulativeCounts.addAndGet(maxTracked, times);
                overflowRequests.incrementAndGet();
            }
            updateMax(val);
        }

        /*
         * Update overall max and each watcher interval max.
         */
        private void updateMax(int val) {
            overallMax.updateMax(val);
            for (HistoryItem item : watchers.values()) {
                item.max.updateMax(val);
            }
        }

        /*
         * Calculate 95th/99th from histogramCounts
         */
        private RateResult calculate(long duration,
                                     long requestCount,
                                     long overflowCount,
                                     long operationCount,
                                     long[] histogramCounts,
                                     int max) {
            int percent95 = DEFAULT_VAL;
            int percent99 = DEFAULT_VAL;
            int min = -1;

            final long percent95Count;
            final long percent99Count;
            if (operationCount == 1) {
                /* For one request, always include it in the 95% and 99%. */
                percent95Count = 1;
                percent99Count = 1;
            } else {
                /*
                 * Otherwise truncate: never include the last/highest request.
                 */
                percent95Count = (int) (operationCount * .95);
                percent99Count = (int) (operationCount * .99);
            }
            long numRequestsSeen = 0;
            long totalLatency = 0;
            for (int latency = 0; latency < histogramCounts.length; latency++) {
                if (histogramCounts[latency] == 0) {
                    continue;
                }
                if (numRequestsSeen < percent95Count) {
                    percent95 = latency;
                }
                if (numRequestsSeen < percent99Count) {
                    percent99 = latency;
                }
                if (min == -1) {
                    min = latency;
                }
                while (max < latency) {
                    max = latency;
                }

                numRequestsSeen += histogramCounts[latency];
                totalLatency += histogramCounts[latency] * latency;
            }

            if (min == -1) {
                min = DEFAULT_VAL;
            }
            return new RateResult(maxTracked, duration, requestCount,
                                  operationCount, overflowCount,
                                  min, max, percent95, percent99, totalLatency);
        }

        /**
         * {@inheritDoc}
         * Updating different fields are not an atomic operation, so they will
         * be a bit inconsistent when calculating rate and observing new value
         * concurrently. This tradeoff is made in order to avoid the cost of
         * synchronization.
         */
        @Override
        public RateResult obtain(boolean clear) {
            final long[] intervalCounts = new long[maxTracked + 1];
            long intervalTotalCount = 0;
            for (int i = 0; i < intervalCounts.length; i++) {
                intervalCounts[i] = cumulativeCounts.get(i);
                intervalTotalCount += intervalCounts[i];
            }
            return calculate(System.nanoTime() - initialTime,
                             totalRequests.get(), overflowRequests.get(),
                             intervalTotalCount, intervalCounts,
                             overallMax.val);
        }

        /**
         * {@inheritDoc}
         * Updating different fields are not an atomic operation, so they will
         * be a bit inconsistent when calculating rate and observing new value
         * concurrently. This tradeoff is made in order to avoid the cost of
         * synchronization.
         */
        @Override
        public RateResult obtain(String watcherName, boolean clear) {
            final long currentTime = System.nanoTime();
            final long currentTotalRequests = totalRequests.get();
            final long currentOverflowRequests = overflowRequests.get();
            long lastTime = initialTime;
            long lastRequestCount = 0;
            long lastOverflowCount = 0;
            long[] lastHistogramCounts;
            int max = DEFAULT_VAL;
            final HistoryItem historyItem = watchers.get(watcherName);
            if (historyItem != null) {
                lastTime = historyItem.lastTime;
                lastHistogramCounts = historyItem.lastHistogramCounts;
                lastRequestCount = historyItem.lastRequestCount;
                lastOverflowCount = historyItem.lastOverflowCount;
                max = historyItem.max.val;
                historyItem.lastTime = currentTime;
                historyItem.lastRequestCount = currentTotalRequests;
                historyItem.lastOverflowCount = currentOverflowRequests;
                historyItem.max.reset();
            } else {
                lastHistogramCounts = new long[maxTracked + 1];
                watchers.put(watcherName,
                             new HistoryItem(currentTime,
                                             currentTotalRequests,
                                             currentOverflowRequests,
                                             lastHistogramCounts));
            }
            final long[] intervalCounts = new long[maxTracked + 1];
            long cumulativeICount = 0;
            long intervalTotalCount = 0;
            for (int i = 0; i < intervalCounts.length; i++) {
                cumulativeICount = cumulativeCounts.get(i);
                intervalCounts[i] = cumulativeICount - lastHistogramCounts[i];
                lastHistogramCounts[i] = cumulativeICount;
                intervalTotalCount += intervalCounts[i];
            }

            return calculate(currentTime - lastTime,
                             currentTotalRequests - lastRequestCount,
                             currentOverflowRequests - lastOverflowCount,
                             intervalTotalCount, intervalCounts,
                             max);
        }

        /* A structure to record PerfQuantile history item for each watcher */
        private static final class HistoryItem {
            private long lastTime;
            private long lastRequestCount;
            private long lastOverflowCount;
            private long[] lastHistogramCounts;
            private Max max;

            private HistoryItem(long time,
                                long requestCount,
                                long overflowCount,
                                long[] histogramCounts) {
                lastTime = time;
                lastRequestCount = requestCount;
                lastOverflowCount = overflowCount;
                lastHistogramCounts = histogramCounts;
                max = new Max();
            }
        }

        private static final class Max {
            private volatile int val;

            private Max() {
                reset();
            }

            private void reset() {
                val = DEFAULT_VAL;
            }

            private void updateMax(int next) {
                /*
                 * Update the max if necessary. This is not atomic, so we loop
                 * to account for lost updates.
                 */
                while (val < next) {
                    val = next;
                }
            }
        }
    }

    /**
     * Saving collected {@link PerfQuantile} rate change result.
     */
    public static class RateResult extends RateMetric.RateResult {

        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unused")
	private final int maxTracked;
        private final long requestCount;
        private final long operationCount;
        private final long overflowCount;
        private final int min;
        private final int max;
        private final int percent95;
        private final int percent99;
        private final long totalLatency;

        public RateResult(int maxTracked,
                          long duration,
                          long requestCount,
                          long operationCount,
                          long overflowCount,
                          int min,
                          int max,
                          int percent95,
                          int percent99,
                          long totalLatency) {
            super(duration);
            this.maxTracked = maxTracked;
            this.requestCount = requestCount;
            this.operationCount = operationCount;
            this.overflowCount = overflowCount;
            this.min = min;
            this.max = max;
            this.percent95 = percent95;
            this.percent99 = percent99;
            this.totalLatency = totalLatency;
        }

        public long getRequestCount() {
            return requestCount;
        }

        public long getOperationCount() {
            return operationCount;
        }

        public long getOverflowCount() {
            return overflowCount;
        }

        public int getMin() {
            return min;
        }

        public int getMax() {
            return max;
        }

        public int get95th() {
            return percent95;
        }

        public int get99th() {
            return percent99;
        }

        public float getAvgMs() {
            if (operationCount == 0) {
                return 0;
            }
            return totalLatency / (float) operationCount;
        }

        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = super.toMap();
            map.put(REQUEST_COUNT_NAME, requestCount);
            map.put(OPERATION_COUNT_NAME, operationCount);
            map.put(OVERFLOW_COUNT_NAME, overflowCount);
            map.put(OPERATION_MIN_NAME, min);
            map.put(OPERATION_MAX_NAME, max);
            map.put(OPERATION_AVG_NAME, getAvgMs());
            map.put(QUANTILE_NAME + DELIMITER + "0.95", percent95);
            map.put(QUANTILE_NAME + DELIMITER + "0.99", percent99);
            return map;
        }
    }

    /*
     * Override the collect method to use the special Element#obtain method to
     * allow null watcherName.
     */
    @Override
    public MetricFamilySamples<RateResult> collect() {
        return new MetricFamilySamples<>(
            name, getType(), labelNames,
            Collections.unmodifiableList(
                elementMap.entrySet().stream()
                .collect(
                    () -> new ArrayList<MetricFamilySamples.Sample<RateResult>>(),
                    (l, e) -> addWrappedElement(l, e.getValue()),
                    (l1, l2) -> l1.addAll(l2))));
    }

    private void addWrappedElement(List<MetricFamilySamples.Sample<RateResult>> list,
                                   WrappedElement elem) {

        final WrappedResult<RateResult> result = elem.obtain(false);
        /* Old code allows result to be null */
        if (result == null) {
            return;
        }
        list.add(new MetricFamilySamples.Sample<>(result));
    }

    // Convenience methods.

    /**
     * {@link Element#observe(int)}
     * Note: as a convenience method, it only works for no label PerfQuantile.
     */
    public void observe(int val) {
        labels().observe(val);
    }

    /**
     * {@link Element#observe(int, int)}
     * Note: as a convenience method, it only works for no label PerfQuantile.
     */
    public void observe(int val, int times) {
        labels().observe(val, times);
    }

    /**
     * {@link Element#observeNanoLatency(long, int)}
     * Note: as a convenience method, it only works for no label PerfQuantile.
     */
    public void observeNanoLatency(long nanoLatency, int times) {
        labels().observeNanoLatency(nanoLatency, times);
    }
}
