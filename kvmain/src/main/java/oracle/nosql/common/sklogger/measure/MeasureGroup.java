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

package oracle.nosql.common.sklogger.measure;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.AbstractJsonSerializable;
import oracle.nosql.common.jss.JsonSerializable;

/**
 * Represents a group of {@linkplain MeasureElement measure elements} which can
 * have a recursive hierarchy.
 *
 * <p>The result of the group is serialized into a json object (recursively)
 * with the key and measure result pair.
 */
public class MeasureGroup<K> implements MeasureElement<JsonSerializable> {

    /* The map for the elements */
    private final Map<K, MeasureElement<? extends JsonSerializable>>
        elementMap = new HashMap<>();

    /**
     * Returns an element associated with the specified key. The semantics is
     * the same with {@link Map#computeIfAbsent}.
     */
    @SuppressWarnings("unchecked")
    public synchronized <E extends MeasureElement<? extends JsonSerializable>>
        E getElement(K key, Supplier<E> supplier)
    {

        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(supplier, "supplier");

        return (E) elementMap.computeIfAbsent(key, (k) -> supplier.get());
    }

    @Override
    public synchronized JsonSerializable obtain(String watcherName,
                                                boolean clear) {
        final Map<String, JsonSerializable> results =
            elementMap.entrySet().stream()
            .collect(Collectors.toMap(
                (e) -> e.getKey().toString(),
                (e) -> e.getValue().obtain(watcherName, clear)));
        return new AbstractJsonSerializable() {
            @Override
            public JsonNode toJson() {
                final ObjectNode json = JsonUtils.createObjectNode();
                results.entrySet().stream()
                    .filter((e) -> !e.getValue().isDefault())
                    .forEach((e) -> json.put(e.getKey(), e.getValue().toJson()));
                return json;
            }

            @Override
            public boolean isDefault() {
                return results.values().stream()
                    .allMatch((v) -> v.isDefault());
            }
        };
    }
}
