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

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.jss.JsonSerializable;
import oracle.nosql.common.jss.JsonSerializationUtils;
import oracle.nosql.common.sklogger.measure.MeasureElement;

/**
 * A wrapper class around {@link FlatMeasureGroup} for compability reasons.
 */
public abstract class Metric<E extends MeasureElement<R>,
                             R extends JsonSerializable>
    extends FlatMeasureGroup<E, R, MetricFamilySamples<R>>
    implements StatsData {

    /**
     * @param name is metric name that will be used when metric is registered
     * and/or processed.
     * @param labelNames to set metric tag names that is optional for Metric
     */
    public Metric(String name, String... labelNames) {
        super(name, labelNames);
    }

    @Override
    public String getStatsName() {
        return name;
    }

    public abstract Type getType();

    /**
     * Wraps {@link FlatMeasureGroup#getElement}.
     */
    public E labels(String... labelValues) {
        return getElement(labelValues);
    }

    /**
     * Wraps {@link FlatMeasureGroup#getElement}.
     */
    public E labels() {
        return getElement();
    }

    /**
     * Parent class for saving obtained {@link Metric} result.
     */
    public abstract static class Result
        implements JsonSerializable, Serializable {

        public static final long serialVersionUID = 1L;

        /**
         * Produce a key/value pairs map for Result.
         */
        public abstract Map<String, Object> toMap();

        @Override
        public JsonNode toJson() {
            return toMap().entrySet().stream()
                .map(
                    (e) -> {
                        final String key = e.getKey();
                        final Object val = e.getValue();
                        return new AbstractMap.SimpleEntry<String, JsonNode>(
                            key,
                            (val instanceof JsonSerializable)
                            ? ((JsonSerializable) val).toJson()
                            : JsonUtils.createJsonNode(val.toString()));
                    })
                .collect(JsonSerializationUtils.getObjectCollector());
        }

        @Override
        public boolean isDefault() {
            return toMap().isEmpty();
        }
    }

    /**
     * Collects the measure result and wraps it into a {@link
     * MetricFamilySamples}.
     */
    @Override
    public MetricFamilySamples<R> obtain(String watcherName, boolean clear) {
        return new MetricFamilySamples<>(
            name, getType(), labelNames, getAllSamples(watcherName, clear));
    }

    protected List<MetricFamilySamples.Sample<R>>
        getAllSamples(String watcherName, boolean clear) {

        return Collections.unmodifiableList(
            elementMap.entrySet().stream()
            .collect(
                () -> new ArrayList<MetricFamilySamples.Sample<R>>(),
                (l, e) -> addWrappedElement(
                    l, e.getValue(), watcherName, clear),
                (l1, l2) -> l1.addAll(l2)));
    }

    protected void addWrappedElement(List<MetricFamilySamples.Sample<R>> list,
                                     WrappedElement elem,
                                     String watcherName,
                                     boolean clear) {

        final FlatMeasureGroup.WrappedResult<R> result =
            elem.obtain(watcherName, clear);
        /* Old code allows result to be null */
        if (result == null) {
            return;
        }
        list.add(new MetricFamilySamples.Sample<>(result));
    }

    public MetricFamilySamples<R> collect() {
        return obtain(false);
    }

    /**
     * Register the Collector with the given registry.
     */
     public Metric<E, R> registerTo(MetricRegistry registry) {
         return registry.register(getName(), this);
     }
}
