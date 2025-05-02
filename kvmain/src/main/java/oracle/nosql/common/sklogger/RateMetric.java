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

import oracle.nosql.common.jss.JsonSerializable;
import oracle.nosql.common.sklogger.measure.MeasureElement;

/**
 * A special {@link Metric} data that has rate of change.
 */
public abstract class RateMetric<E extends RateMetric.Element<R>,
                                 R extends RateMetric.RateResult>
    extends Metric<E, R> {

    /**
     * Constructs a rate metric.
     */
    public RateMetric(String name, String... labelNames) {
        super(name, labelNames);
    }

    /**
     * Place holder interface for compatibility.
     */
    public interface Element<R extends JsonSerializable>
        extends MeasureElement<R> {

        default R rate() {
            return obtain(false);
        }

        default R rateSinceLastTime(String watcherName) {
            return obtain(watcherName);
        }
    }

    /**
     * Place holder interface for compatibility.
     */
    public static abstract class RateResult extends Metric.Result {

        private static final long serialVersionUID = 1L;

        protected final long duration;

        public RateResult(long duration) {
            this.duration = duration;
        }

        public long getDuration() {
            return duration;
        }

        @Override
        public Map<String, Object> toMap() {
            final Map<String, Object> map = new HashMap<String, Object>();
            map.put(DURATION_NAME, duration);
            return map;
        }
    }

    /**
     * {@link Element#rate()}
     * Note: as a convenience method, it only works for no label metric.
     */
    public R rate() {
        return labels().rate();
    }

    /**
     * {@link Element#rateSinceLastTime(String)}
     * Note: as a convenience method, it only works for no label metric.
     */
    public R rateSinceLastTime(String watcherName) {
        return labels().rateSinceLastTime(watcherName);
    }

    /**
     * Old method for compatibility.
     */
    public MetricFamilySamples<R> collectSinceLastTime(String watcherName) {
        return obtain(watcherName);
    }
}
