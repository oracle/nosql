/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.measurement;

import java.util.Map;
import java.util.stream.Collector;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Some stats utility methods
 *
 * @deprecated since 22.3
 */
@Deprecated
public class StatsUtils {

    /**
     * Returns a json object collector. Use this as the argument to
     * Stream.collect to convert a stream of map entries with strings and
     * JsonElements into a JsonObject with those elements as members.
     */
    public static <T extends Map.Entry<String, JsonElement>>
        Collector<T, JsonObject, JsonObject> getObjectCollector() {

        return Collector.of(
            () -> new JsonObject(),
            (o, e) -> o.add(e.getKey(), e.getValue()),
            (o1, o2) -> {
                o2.entrySet()
                    .forEach(e -> o1.add(e.getKey(), e.getValue()));
                return o1;
            });
    }

    /**
     * Returns a json array collector. Use this as the argument to
     * Stream.collect to convert a stream of JsonElements into a JsonArray.
     */
    public static Collector<JsonElement, JsonArray, JsonArray>
        getArrayCollector() {

        return Collector.of(
            () -> new JsonArray(),
            (a, e) -> a.add(e),
            (a1, a2) -> {
                a1.addAll(a2);
                return a1;
            });
    }

}
