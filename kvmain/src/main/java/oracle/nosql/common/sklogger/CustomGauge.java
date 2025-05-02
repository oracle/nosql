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
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import oracle.nosql.common.sklogger.measure.StateProbe;

/**
 * A CustomGauge as one kind of {@link Metric} is useful when Gauge value is
 * not possible to directly instrument code, as it is not in your control, such
 * as gauge System memory value.
 * <pre>
 * Usage example:
 * <code>
 * private static class TestCalculator implements GaugeCalculator {
 *     &#64;Override
 *     public List<GaugeResult> getValuesList() {
 *         {@literal Map<String, Object> metrics = new HashMap<>(2);}
 *         metrics.put("free", Runtime.getRuntime().freeMemory());
 *         metrics.put("total", Runtime.getRuntime().maxMemory());
 *         return Arrays.asList(new GaugeResult(metrics));
 *     }
 * }
 * CustomGauge memory = new CustomGauge("memory", new TestCalculator());
 * memory.getValuesList();
 * </code>
 * </pre>
 */
public class CustomGauge
    extends Metric<CustomGauge.Element, CustomGauge.GaugeResult> {

    private final GaugeCalculator gaugeCalculator;

    /**
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param gaugeCalculator is used to get the current gauge value map.
     */
    public CustomGauge(String name,
                       GaugeCalculator gaugeCalculator) {

        super(name);
        this.gaugeCalculator = gaugeCalculator;
    }

    /**
     * Implement the interface to get current GaugeResult list.
     */
    public interface GaugeCalculator extends Supplier<List<GaugeResult>> {

        @Override
        default List<GaugeResult> get() {
            return getValuesList();
        }

        /**
         * Get current GaugeResult list.
         */
        List<GaugeResult> getValuesList();
    }

    @Override
    protected Element newElement() {
        return new Element();
    }

    @Override
    public Type getType() {
        return Type.CUSTOM_GAUGE;
    }

    public static class Element extends StateProbe<GaugeResult, GaugeResult> {

        public Element() {
            super(Function.identity());
        }
    }

    /**
     * Saving obtained {@link CustomGauge} result.
     */
    public static class GaugeResult extends Metric.Result {

        private static final long serialVersionUID = 1L;

        private Map<String, Object> map;

        public GaugeResult(Map<String, Object> map) {
            this.map = map;
        }

        @Override
        public Map<String, Object> toMap() {
            return map;
        }
    }

    @Override
    public MetricFamilySamples<GaugeResult> obtain(String watcherName,
                                                   boolean clear) {
        /*
         * For compability reasons, we observe the state with index as labels.
         * The old code structured in a way that there is only one element, but
         * returning a list of samples each with one gauge result, which does
         * not fit the metric interface. A better implementation would be each
         * element, returning one gauge result, has a label; or that there is
         * only one element returning a list of gauge result.
         */
        List<MetricFamilySamples.Sample<GaugeResult>> samples =
            new ArrayList<>();
        final List<GaugeResult> values = gaugeCalculator.get();
        for (int i = 0; i < values.size(); ++i) {
            labels().observe(values.get(i));
            samples.addAll(getAllSamples(watcherName, clear));
        }
        return new MetricFamilySamples<>(name, getType(), labelNames, samples);
    }

    /* Convenience methods. */

    /**
     * {@link GaugeCalculator#getValuesList}.
     */
    public List<GaugeResult> getValuesList() {
        try {
            return gaugeCalculator.getValuesList();
        } catch (Exception e) {
        }
        return null;
    }
}
