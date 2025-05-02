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

import java.util.Map;

import oracle.nosql.common.sklogger.measure.ThroughputElement;

/**
 * A Counter as one kind of {@link RateMetric} is an Long value that
 * typically increases over time.
 * <pre>
 * Usage example:
 * {@code
 * Counter requestCount = new Counter("requestCount", "opType");
 * add one read request.
 * requestCount.label("read").incrValue();
 *
 * add one write request.
 * requestCount.label("write").incrValue();
 *
 * get total read request count.
 * requestCount.label("read").getValue();
 *
 * get delta read request count change since last call for watcherName.
 * requestCount.label("read").rateSinceLastTime("watcherName")
 * }
 * </pre>
 */
public class Counter extends RateMetric<Counter.Element, Counter.RateResult> {

    /**
     * Constructs the counter.
     */
    public Counter(String name, String... labelNames) {
        super(name, labelNames);
    }

    @Override
    public Type getType() {
        return Type.COUNTER;
    }

    @Override
    protected Element newElement() {
        return new Element();
    }

    public static final class Element
        implements RateMetric.Element<RateResult> {

        private final ThroughputElement elem = new ThroughputElement();

        /**
         * Return the Counter Element current value.
         */
        public long getValue() {
            return elem.getCurrentCount();
        }

        /**
         * Add one to the Counter Element.
         */
        public void incrValue() {
            elem.observe(1L);
        }

        /**
         * Add delta value to Counter Element.
         */
        public void incrValue(long delta) {
            elem.observe(delta);
        }

        @Override
        public RateResult obtain(String watcherName, boolean clear) {
            final ThroughputElement.Result measureResult =
                elem.obtain(watcherName, clear);
            return new RateResult(measureResult);
        }
    }

    /**
     * Saving obtained {@link Counter} rate change result.
     */
    public static class RateResult extends RateMetric.RateResult {

        private static final long serialVersionUID = 1L;

        private final ThroughputElement.Result measureResult;

        /**
         * Adapts to the {@code ThroughputElement.Result}.
         */
        public RateResult(ThroughputElement.Result measureResult) {
            super(measureResult.getDurationNanos());
            this.measureResult = measureResult;
        }

        public long getCount() {
            return (long) measureResult.getCount();
        }

        public double getThroughputPerSecond() {
            return measureResult.getThroughputPerSecond();
        }

        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = super.toMap();
            map.put(COUNT_NAME, getCount());
            map.put(THROUGHPUT_NAME, getThroughputPerSecond());
            return map;
        }
    }

    /* Convenience methods */

    /**
     * {@link Element#getValue()}
     * Note: as a convenience method, it only works for no label Counter.
     */
    public long getValue() {
        return labels().getValue();
    }

    /**
     * {@link Element#incrValue()}
     * Note: as a convenience method, it only works for no label Counter.
     */
    public void incrValue() {
        labels().incrValue();
    }

    /**
     * {@link Element#incrValue(long)}
     * Note: as a convenience method, it only works for no label Counter.
     */
    public void incrValue(long delta) {
        labels().incrValue(delta);
    }
}
