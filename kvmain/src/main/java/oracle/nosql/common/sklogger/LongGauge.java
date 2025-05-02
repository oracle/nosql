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

import oracle.nosql.common.sklogger.measure.LongStateProbe;

/**
 * A LongGauge as one kind of {@link Metric} is a long value that fluctuates
 * over time, whose value is unrelated to a previously reported value.
 * <pre>
 * Usage example:
 * {@code
 * LongGauge activeRequest = new LongGauge("activeRequest", "opType");
 * add one read request.
 * activeRequest.label("read").incrValue();
 *
 * Decrease one read request.
 * activeRequest.label("read").decrValue();
 *
 * add one write request.
 * activeRequest.label("write").incrValue();
 *
 * Decrease one write request.
 * activeRequest.label("write").decrValue();
 *
 * get current active read request.
 * activeRequest.label("read").getValue();
 * }
 * </pre>
 */
public class LongGauge
    extends Metric<LongGauge.Element, LongGauge.GaugeResult> {

    public LongGauge(final String name, String... labelNames) {
        super(name, labelNames);
    }

    @Override
    public Type getType() {
        return Type.LONG_GAUGE;
    }

    @Override
    protected Element newElement() {
        return new Element();
    }

    public static final class Element
        extends LongStateProbe<GaugeResult> {

        private Element() {
            super((v) -> new GaugeResult(v));
        }

        public void setValue(long newValue) {
            observe(newValue);
        }

    }

    /**
     * Saving obtained {@link LongGauge} result.
     */
    public static class GaugeResult extends Metric.Result {

        private static final long serialVersionUID = 1L;

        private long gaugeVal;

        public GaugeResult(long gaugeVal) {
            this.gaugeVal = gaugeVal;
        }

        public long getGaugeVal() {
            return gaugeVal;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = new HashMap<String, Object>();
            map.put(GAUGE_NAME, gaugeVal);
            return map;
        }
    }

    /* Convenience methods */

    /**
     * {@link Element#setValue(long)}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void setValue(long newValue) {
        labels().setValue(newValue);
    }

    /**
     * {@link Element#getValue}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public long getValue() {
        return labels().getValue();
    }

    /**
     * {@link Element#incrValue}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void incrValue() {
        labels().incrValue();
    }

    /**
     * {@link Element#incrValue(long)}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void incrValue(long delta) {
        labels().incrValue(delta);
    }

    /**
     * {@link Element#decrValue}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void decrValue() {
        labels().decrValue();
    }

    /**
     * {@link Element#decrValue(long)}
     * Note: as a convenience method, it only works for no label LongGauge.
     */
    public void decrValue(long delta) {
        labels().decrValue(delta);
    }
}
