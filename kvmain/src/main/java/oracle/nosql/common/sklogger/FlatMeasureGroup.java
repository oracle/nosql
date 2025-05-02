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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.JsonSerializable;
import oracle.nosql.common.jss.JsonSerializationUtils;
import oracle.nosql.common.sklogger.measure.MeasureElement;

/**
 * Represents a group of the same simple {@linkplain MeasureElement measure
 * elements} associated with an array of labels.
 *
 * <p>Comparing with {@code MeasureGroup}, a flatterned hierarchy may be
 * desired where a group consists of simple measure elements of the same type.
 * Each of the elements is associated with an array of labels.
 *
 * <p>The measure result of the group is serialized into a json array. The
 * measure results of the elements are sorted according to the label values.
 * Each measure results are serialized into a separate record which is a json
 * object. The labels associated are serialized into the record as attributes
 * with label names prefixed by the group name as the key. The measure result,
 * if can be serialized as an object is flatterned into the record object,
 * otherwise, it is serialized as an attribute with "result" as the key.
 */
public abstract class FlatMeasureGroup<
    E extends MeasureElement<R>, /* element */
    R extends JsonSerializable, /* element result */
    T extends JsonSerializable> /* group result */
    implements MeasureElement<T> {

    /* The delimiter separating the names */
    private static final String FLATTERN_DELIMITER = "_";

    /* The group name */
    protected final String name;
    /* The label names */
    protected final List<String> labelNames;
    /*
     * The element map.
     *
     * Access to the map is always in the synchronized block of this object.
     *
     * TODO: we use a TreeMap and String[] as the key for the elementMap. This
     * has at least the following two issues: (1) Using String[] as the key is
     * dangerous as String[] is mutable; (2) Using labels(String...) is less
     * desired due to typos may occur in the string arguments. We have to adopt
     * the current approach is because labels() method is called in the
     * critical path and it is required that we do not create new objects (I
     * have doubts on this requirement as well, since we create a lot of
     * objects on that paths).
     */
    protected volatile SortedMap<String[], WrappedElement> elementMap =
        new TreeMap<>(new StringArrayComparator());

    private static class StringArrayComparator
        implements Comparator<String[]> {

        @Override
        public int compare(String[] s1, String[] s2) {
            if ((s1 == null) && (s2 == null)) {
                return 0;
            }
            if (s1 == null) {
                return -1;
            }
            if (s2 == null) {
                return 1;
            }
            if (s1.length != s2.length) {
                throw new IllegalStateException(String.format(
                    "Size not equal: s1=%s, s2=%s",
                    Arrays.toString(s1), Arrays.toString(s2)));
            }
            if (s1.equals(s2)) {
                return 0;
            }
            for (int i = 0; i < s1.length; ++i) {
                final int result = s1[i].compareTo(s2[i]);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }

    /**
     * Constructs the group.
     *
     * <p>Label names cannot be {@code null} or contains ".";
     */
    public FlatMeasureGroup(String name,
                            String...labelNames) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(labelNames, "labelNames");
        requireItemsNonNull(labelNames);
        this.name = name;
        this.labelNames =
            Collections.unmodifiableList(Arrays.asList(labelNames));
    }

    private void requireItemsNonNull(String[] names) {
        Arrays.stream(names).forEach((n) -> Objects.requireNonNull(n));
    }

    /**
     * Returns the name of the group.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns an element associated with the specified label values, creates a
     * new one if necessary.
     *
     * @param labelValues the label values, must not be {@code null}, which
     * contains non-{@code null} items and have the same length as label names
     */
    public E getElement(String...labelValues) {
        ensureLabelValuesValid(labelValues);
        WrappedElement element = elementMap.get(labelValues);
        if (element != null) {
            return element.get();
        }
        /*
         * The metric elementMap should be very small and rarely updated. So
         * we use copy-on-write to make read very efficient.
         */
        synchronized(this) {
            element = elementMap.get(labelValues);
            if (element != null) {
                return element.get();
            }
            SortedMap<String[], WrappedElement> newMap = new TreeMap<>(elementMap);
            element = new WrappedElement(labelValues, newElement());
            newMap.put(labelValues, element);
            elementMap = newMap;
            return element.get();
        }

    }

    private void ensureLabelValuesValid(String[] labelValues) {
        /*
         * TODO: commented out the following check due to a performance
         * degradation. I do not think this optimization matters though.
         */
        //Objects.requireNonNull(labelValues, "labelValues");
        //requireItemsNonNull(labelValues);
        if (labelValues.length != labelNames.size()) {
            throw new IllegalArgumentException(
                "Incorrect number of label values");
        }
    }

    /**
     * Creates the element.
     */
    protected abstract E newElement();

    /**
     * Wraps an element to customize result serialization.
     */
    public class WrappedElement
        implements MeasureElement<WrappedResult<R>> {

        private final String[] label;
        private final E element;

        private WrappedElement(String[] label,
                               E element) {
            this.label = label;
            this.element = element;
        }

        public String[] getLabel() {
            return label;
        }

        public E get() {
            return element;
        }

        @Override
        public WrappedResult<R> obtain(boolean clear) {
            return new WrappedResult<R>(
                getName(), labelNames, label,
                element.obtain(clear));
        }

        @Override
        public WrappedResult<R> obtain(String watcherName, boolean clear) {
            return new WrappedResult<R>(
                getName(), labelNames, label,
                element.obtain(watcherName, clear));
        }
    }

    /**
     * A wrapped result which serailizes the label name/value pairs and
     * flatterns the result object.
     */
    public static class WrappedResult<R extends JsonSerializable>
        implements JsonSerializable {

        private final String name;
        private final List<String> labelNames;
        private final String[] label;
        private final R result;

        public WrappedResult(String name,
                             List<String> labelNames,
                             String[] label,
                             R result) {
            this.name = name;
            this.labelNames = labelNames;
            this.label = label;
            this.result = result;
        }

        public String getName() {
            return name;
        }

        public List<String> getLabelNames() {
            return labelNames;
        }

        public String[] getLabel() {
            return label;
        }

        public R getResult() {
            return result;
        }

        @Override
        public JsonNode toJson() {
            final ObjectNode object = JsonUtils.createObjectNode();
            writeLabels(object);
            writeElementResult(object, result.toJson());
            return object;
        }

        private void writeLabels(ObjectNode wrappedResult) {
            for (int i = 0; i < labelNames.size(); ++i) {
                final String key =
                    String.format("%s%s%s", name, FLATTERN_DELIMITER,
                                  labelNames.get(i));
                wrappedResult.put(key, label[i]);
            }
        }

        private void writeElementResult(ObjectNode wrappedResult,
                                        JsonNode elementResult) {
            if (!elementResult.isObject()) {
                wrappedResult.put(name, elementResult);
                return;
            }
            /* Flattern the result object into the wrapped */
            final ObjectNode elementObject =
                (ObjectNode) elementResult;
            elementObject.entrySet().forEach(
                (e) -> wrappedResult.put(
                    transformElementKey(e.getKey()),
                    e.getValue()));
        }

        private String transformElementKey(String key) {
            return key.isEmpty() ?
                getName() :
                String.format(
                    "%s%s%s", getName(), FLATTERN_DELIMITER, key);
        }

        @Override
        public boolean isDefault() {
            return result.isDefault();
        }
    }

    protected JsonSerializable toJsonSerializable(String watcherName,
                                                  boolean clear) {

        return new JsonSerializable() {
            @Override
            public ArrayNode toJson() {
                return elementMap.values().stream()
                    .map((e) -> e.obtain(watcherName, clear))
                    .filter((e) -> !e.isDefault())
                    .map((e) -> e.toJson())
                    .collect(JsonSerializationUtils.getArrayCollector());
            }

            @Override
            public boolean isDefault() {
                return elementMap.isEmpty();
            }
        };
    }
}
