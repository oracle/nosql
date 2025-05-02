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

import java.util.Arrays;
import java.util.Map;

import oracle.nosql.common.sklogger.measure.HistogramElement;

/**
 * <pre>
 * A Histogram as one kind of {@link RateMetric} is Long array buckets that
 * record counts in each buckets. If observed value is between
 * ( bucket[N-1], bucket[N] ] and then the observed value fall into N bucket
 * and add one count in bucket N. <p>
 * If your service runs replicated with a number of instances, and you want to
 * get an overview 95th/99th of some metrics, then you might want to use this
 * Histogram metric to help estimate 95th/99th value.
 * You obtain Histogram metric from every single one of them, and then you
 * want to aggregate everything into an overall 95th percentile.
 *
 * For example:
 * we create exponential 12 buckets: 1, 2, 4, 8, 16, 32, ... 1024, 2048, 4096
 * for each proxy instance. And we query each proxy instance at the same time
 * to get the count values for these 12 buckets.
 * Let's say:
 * Instance 1:
 * 0-1    1000
 * 1-2    2000
 * 2-4    3000
 * 4-8    500
 * 8-16   100
 * 16-
 * ...    50
 * 4096
 *
 * Total: 6650
 *
 * Instance 2:
 * 0-1    3000
 * 1-2    1000
 * 2-4    500
 * 4-8    100
 * 8-16   100
 * 16-
 * ...    30
 * 4096
 *
 * Total: 4730
 *
 * The total is 6650 + 4730 = 11380, 95th count is 11380 * 0.95 = 10811,
 * 0-1 count is 3000+1000=4000, 1-2 count is 1000+2000=3000,
 * 2-4 count is 3000+ 500=3500, so 0-4 count is 4000+3000+3500 = 10500;
 * Still less than target 95th count 10811. 4-8 count is 500 + 100 = 600;
 * so 0-8 count is 10500 + 600 = 11000 that is bigger than
 * 10811(target 95th count). Now we know 95th latency is between 4-8, and if we
 * assume the latency values are even, then we can estimate 95th/99th latency.
 * (10811 - 10500) / 600 * (8-4) + 4  that approximate to 6. So we can say the
 * two proxy instances 95th latency is 6 ms.
 *
 * </pre>
 * TODO implement class to estimate 95th/99th from multiple Histogram.
 * <pre>
 * Usage example:
 * {@code
 * Histogram requestLatency = new Histogram("requestLatency", "opType");
 * add one observed read request latency.
 * requestLatency.label("read").observe(latency);
 *
 * add one observed write request latency.
 * requestLatency.label("write").observe(latency);
 *
 * get read request latency each buckets values since last call for watcherName.
 * requestCount.label("read").rateSinceLastTime("watcherName")
 * }
 * </pre>
*/
public class Histogram extends
    RateMetric<Histogram.Element, Histogram.RateResult> {

    private final long[] upperBounds;

    /**
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param upperBounds set the upper bounds for each buckets. If observed
     * value is between ( bucket[N-1], bucket[N] ] and then the observed fall
     * into N bucket and increase one in bucket N.
     * @param labelNames to set metric tag names that is optional for Metric
     *
     * {@link RateMetric#RateMetric(String, String...)}
     */
    public Histogram(final String name,
                     long[] upperBounds,
                     String... labelNames) {

        super(name, labelNames);
        this.upperBounds = normalizeUpperBounds(upperBounds);
    }

    private long[] normalizeUpperBounds(long[] ubs) {
        if (ubs == null || ubs.length == 0) {
            throw new IllegalArgumentException("upperBounds cannot be empty");
        }
        /* Append a max bucket if it's not already there. */
        final long lastValue = ubs[ubs.length - 1];
        final int size = (lastValue == Long.MAX_VALUE) ?
            ubs.length : ubs.length + 1;
        final long[] result = new long[size];
        System.arraycopy(ubs, 0, result, 0, ubs.length);
        result[size - 1] = Long.MAX_VALUE;
        return result;
    }

    @Override
    public Type getType() {
        return Type.HISTOGRAM;
    }

    @Override
    protected Element newElement() {
        return new Element(upperBounds);
    }

    private static int getBucketIndex(long value, long[] upperBounds) {
        for (int i = 0; i < upperBounds.length; ++i) {
            if (value <= upperBounds[i]) {
                return i;
            }
        }
        throw new IllegalStateException();
    }

    public static final class Element
        extends HistogramElement<RateResult, Element.Current, Element.Start>
        implements RateMetric.Element<RateResult> {

        private final long[] upperBounds;
        private final Current current;

        private Element(long[] upperBounds) {
            this.upperBounds = upperBounds;
            this.current = new Current(upperBounds);
        }

        @Override
        protected Current getCurrent() {
            return current;
        }

        @Override
        protected Start createStartHistory() {
            return new Start(upperBounds.length);
        }

        /**
         * Observe the given value.
         */
        public void observe(long v) {
            observe(v, 1);
        }

        /**
         * Observe the given value with N times.
         */
        public void observe(long v, int times) {
            final Observation ob = observationThreadLocal.get();
            ob.value = v;
            ob.count = times;
            observe(ob);
        }

        private static class Current
            extends HistogramElement.Current<RateResult, Current, Start> {

            private final long[] upperBounds;
            private long obtainTimeNanos = System.nanoTime();

            private Current(long[] upperBounds) {
                super(upperBounds.length);
                this.upperBounds = upperBounds;
            }

            @Override
            protected int getIndex(long val) {
                return getBucketIndex(val, upperBounds);
            }

            @Override
            public RateResult compare(Start start) {
                final long duration =
                    obtainTimeNanos - start.obtainTimeNanos;
                return new RateResult(
                    duration, upperBounds,
                    setSnapshotAndReturnDiff(start));
            }
        }

        private static class Start
            extends HistogramElement.Start<RateResult, Current, Start> {

            private long obtainTimeNanos = System.nanoTime();

            private Start(int numBuckets) {
                super(numBuckets);
            }

            @Override
            public void set(Current current) {
                super.set(current);
                obtainTimeNanos = current.obtainTimeNanos;
            }
        }
    }

    public static class RateResult extends RateMetric.RateResult {

        private static final long serialVersionUID = 1L;

        private final long totalCount;
        private final long[] upperBounds;
        private final long[] histogramCounts;

        public RateResult(long duration,
                          long[] upperBounds,
                          long[] histogramCounts) {
            super(duration);
            this.totalCount = Arrays.stream(histogramCounts).sum();
            this.upperBounds = upperBounds;
            this.histogramCounts = histogramCounts;
        }

        public long[] getHistogramCounts() {
            return histogramCounts;
        }

        public long getTotalCount() {
            return totalCount;
        }

        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = super.toMap();
            for (int i = 0; i < upperBounds.length; i++) {
                final String key = HISTOGRAM_NAME + DELIMITER + upperBounds[i];
                map.put(key, histogramCounts[i]);
            }
            return map;
        }
    }

    /* Convenience methods */

    public void observe(long v) {
        labels().observe(v);
    }

    public void observe(long v, int times) {
        labels().observe(v, times);
    }

    /**
     * Set the upper bounds of buckets for the histogram with a linear
     * sequence.
     */
    public static long[] linearBuckets(long start,
                                       long width,
                                       int count) {
        final long[] buckets = new long[count];
        for (int i = 0; i < count; i++) {
            buckets[i] = start + i * width;
        }
        return buckets;
    }

    /**
     * Set the upper bounds of buckets for the histogram with an exponential
     * sequence.
     */
    public static long[] exponentialBuckets(long start,
                                            long factor,
                                            int count) {
        final long[] buckets = new long[count];
        for (int i = 0; i < count; i++) {
            buckets[i] = start * (long) Math.pow(factor, i);
        }
        return buckets;
    }
}
