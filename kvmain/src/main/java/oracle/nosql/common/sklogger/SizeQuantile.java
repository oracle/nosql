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

import java.util.HashMap;
import java.util.Map;

import oracle.nosql.common.sklogger.measure.LongFrugal2UQuantileElement;

/**
 * A SizeQuantile as one kind of {@link RateMetric} is to estimate quantiles
 * values of observed values. It doesn't have limitation to observed value
 * range. But it only get estimated quantiles. The default quantiles are
 * median/95th/99th.
 * <pre>
 * Usage example:
 * {@code
 * SizeQuantile requestSize = new SizeQuantile("requestSize", "opType");
 * add one observed read request size.
 * requestSize.label("read").observe(size);
 *
 * add one observed write request size.
 * requestSize.label("write").observe(size);
 *
 * get read request avg/95th/99th size since last call for watcherName.
 * requestSize.label("read").rateSinceLastTime("watcherName")
 * }
 * </pre>
 */
public class SizeQuantile
    extends RateMetric<SizeQuantile.Element, SizeQuantile.RateResult> {

    private static final double[] DEFAULT_QUANTILES =
        new double[] { 0.50, 0.95, 0.99 };

    private final double[] quantiles;

    /**
     * Create a SizeQuantile to estimate default quantiles: 0.50, 0.95, 0.99.
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param labelNames to set metric tag names that is optional for Metric
     */
    public SizeQuantile(final String name, String... labelNames) {
        this(name, DEFAULT_QUANTILES, labelNames);
    }

    /**
     * Create a SizeQuantile to estimate specified quantiles.
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param quantiles that SizeQuantile to calculate
     * @param labelNames to set metric tag names that is optional for Metric
     */
    public SizeQuantile(final String name,
                        double[] quantiles,
                        String... labelNames) {
        super(name, labelNames);
        ensureQuantilesValid(quantiles);
        this.quantiles = quantiles;
    }

    private void ensureQuantilesValid(double[] quantileArray) {
        if (quantileArray == null || quantileArray.length == 0) {
            throw new IllegalArgumentException("quantiles cannot be empty");
        }
    }

    @Override
    public Type getType() {
        return Type.SIZE_QUANTILE;
    }

    @Override
    protected Element newElement() {
        return new Element(quantiles);
    }

    public static final class Element
        implements RateMetric.Element<RateResult> {

        private final LongFrugal2UQuantileElement quantileElement;
        private final Map<String, Long> obtainTimeNanosMap = new HashMap<>();
        private final long initialTimeNanos = System.nanoTime();

        private Element(double[] quantiles) {
            this.quantileElement = new LongFrugal2UQuantileElement(quantiles);
        }

        /**
         * Observe the given value with N times.
         */
        public void observe(long v, int times) {
            for (int i = 0; i < times; i++) {
                observe(v);
            }
        }

        /**
         * Observe the given value with N times.
         */
        public void observe(long v) {
            quantileElement.observe(v);
        }

        @Override
        public synchronized RateResult obtain(String watcherName,
                                              boolean clear) {
            final long obtainTimeNanos = System.nanoTime();
            final long lastCollectTimeNanos =
                obtainTimeNanosMap.computeIfAbsent(
                    watcherName, (w) -> initialTimeNanos);
            final long duration = obtainTimeNanos - lastCollectTimeNanos;
            obtainTimeNanosMap.put(watcherName, obtainTimeNanos);
            final LongFrugal2UQuantileElement.Result result =
                quantileElement.obtain(watcherName, clear);
            return new RateResult(duration, result.getCount(), result.getSum(),
                                  result.getQuantiles(), result.getValues());
        }
    }

    /**
     * Saving obtained {@link SizeQuantile} rate change result.
     */
    public static class RateResult extends RateMetric.RateResult {

        private static final long serialVersionUID = 1L;

        private final long count;
        private final long sum;
        private final double[] quantiles;
        private final long[] quantileValues;

        public RateResult(long duration,
                          long count,
                          long sum,
                          double[] quantiles,
                          long[] quantileValues) {

            super(duration);
            this.count = count;
            this.sum = sum;
            this.quantiles = quantiles;
            this.quantileValues = quantileValues;
        }

        public long getCount() {
            return count;
        }

        public long getSum() {
            return sum;
        }

        public double[] getQuantile() {
            return quantiles;
        }

        public long[] getQuantileValues() {
            return quantileValues;
        }

        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = super.toMap();
            map.put(COUNT_NAME, count);
            map.put(SUM_NAME, sum);
            for (int i = 0; i < quantiles.length; i++) {
                final String key = QUANTILE_NAME + DELIMITER +
                                   quantiles[i];
                map.put(key, quantileValues[i]);
            }
            return map;
        }
    }

    /* Convenience methods */

    /*
     * Note: as a convenience method, it only works for no label SizeQuantile.
     */
    public void observe(long v) {
        labels().observe(v);
    }

    /*
     * Note: as a convenience method, it only works for no label SizeQuantile.
     */
    public void observe(long v, int times) {
        labels().observe(v, times);
    }
}
