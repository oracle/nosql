/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.jss;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collector;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/**
 * Utility methods for implementing {@link JsonSerializable} classes.
 */
public class JsonSerializationUtils {

    /**
     * Returns a json object node collector. Use this as the argument to
     * Stream.collect to convert a stream of map entries with strings and
     * {@link JsonNode} into a {@link ObjectNode} with those elements as members.
     */
    public static <T extends Map.Entry<String, JsonNode>>
        Collector<T, ObjectNode, ObjectNode> getObjectCollector() {

        return Collector.of(
            () -> JsonUtils.createObjectNode(),
            (o, e) -> o.put(e.getKey(), e.getValue()),
            (o1, o2) -> o1.merge(o2));
    }

    /**
     * Returns a json array collector. Use this as the argument to
     * Stream.collect to convert a stream of JsonNodes into a JsonArray.
     */
    public static Collector<JsonNode, ArrayNode, ArrayNode>
        getArrayCollector() {

        return Collector.of(
            () -> JsonUtils.createArrayNode(),
            (a, e) -> a.add(e),
            (a1, a2) -> {
                a1.addAll(a2);
                return a1;
            });
    }

    /* Read methods. */

    /**
     * Reads the Json node which is a Boolean, {@code defaultValue} if
     * incompatible.
     */
    public static boolean readBoolean(JsonNode jsonNode,
                                      boolean defaultValue) {
        if ((jsonNode == null) || !jsonNode.isBoolean()) {
            return defaultValue;
        }
        return jsonNode.asBoolean();
    }

    /**
     * Reads the Json node which is an integer number, {@code defaultValue} if
     * incompatible.
     */
    public static int readInteger(JsonNode jsonNode, int defaultValue) {
        if ((jsonNode == null) || !jsonNode.isNumber()) {
            return defaultValue;
        }
        try {
            return jsonNode.asInt();
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Reads the Json node which is a long number, {@code defaultValue} if
     * incompatible.
     */
    public static long readLong(JsonNode jsonNode, long defaultValue) {
        if ((jsonNode == null) || !jsonNode.isNumber()) {
            return defaultValue;
        }
        try {
            return jsonNode.asLong();
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Reads the Json node which is a double number, {@code defaultValue} if
     * incompatible.
     */
    public static double readDouble(JsonNode jsonNode, double defaultValue) {
        if ((jsonNode == null) || !jsonNode.isNumber()) {
            return 0;
        }
        try {
            return jsonNode.asDouble();
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Reads the Json node which is a String, {@code defaultValue} if
     * incompatible.
     */
    public static String readString(JsonNode jsonNode, String defaultValue) {
        if ((jsonNode == null) || !jsonNode.isString()) {
            return defaultValue;
        }
        return jsonNode.asText();
    }

    /**
     * Reads the Json node which is an array node and represents a list of
     * JsonSerializable objects, returns {@code defaultValue} if incompatible
     * or the array node is empty.
     */
    public static <T extends JsonSerializable> List<T>
        readList(JsonNode jsonNode,
                 Function<JsonNode, T> reader,
                 List<T> defaultValue)
    {
        Objects.requireNonNull(reader, "reader");
        if ((jsonNode == null) || !jsonNode.isArray()) {
            return defaultValue;
        }
        if (jsonNode.isEmpty()) {
            return defaultValue;
        }
        final List<T> result = new ArrayList<>();
        final ArrayNode array = jsonNode.asArray();
        for (JsonNode e : array) {
            result.add(reader.apply(e));
        }
        return result;
    }

    /**
     * Reads the Json node which is an object node and represents a map of
     * String to JsonSerializable objects, returns {@code defaultValue} if
     * incompatible or the object node is empty.
     */
    public static <T extends JsonSerializable> Map<String, T>
        readMap(JsonNode jsonNode,
                Function<JsonNode, T> reader,
                Map<String, T> defaultValue)
    {
        Objects.requireNonNull(reader, "reader");
        if ((jsonNode == null) || !jsonNode.isObject()) {
            return defaultValue;
        }
        if (jsonNode.isEmpty()) {
            return defaultValue;
        }
        final Map<String, T> result = new HashMap<>();
        final ObjectNode object = jsonNode.asObject();
        for (Map.Entry<String, JsonNode> e : object.entrySet()) {
            result.put(e.getKey(), reader.apply(e.getValue()));
        }
        return result;
    }

    /* Write methods. */

    /**
     * Writes a boolean result to the payload, ignoring if it is the {@code
     * defaultValue}.
     */
    public static void writeBoolean(ObjectNode payload,
                                    String fieldName,
                                    boolean result,
                                    boolean defaultValue) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if (result != defaultValue) {
            payload.put(fieldName, result);
        }
    }

    /**
     * Writes an integer result to the payload, ignoring if it is the {@code
     * defaultValue}.
     */
    public static void writeInteger(ObjectNode payload,
                                    String fieldName,
                                    int result,
                                    int defaultValue) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if (result != defaultValue) {
            payload.put(fieldName, result);
        }
    }

    /**
     * Writes long result to the payload, ignoring if it is the {@code
     * defaultValue}.
     */
    public static void writeLong(ObjectNode payload,
                                 String fieldName,
                                 long result,
                                 long defaultValue) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if (result != defaultValue) {
            payload.put(fieldName, result);
        }
    }

    /**
     * Writes double result to the payload, ignoring if it is the {@code
     * defaultValue}.
     */
    public static void writeDouble(ObjectNode payload,
                                   String fieldName,
                                   double result,
                                   double defaultValue) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if (result != defaultValue) {
            payload.put(fieldName, result);
        }
    }

    /**
     * Writes a string result to the payload, ignoring if it is the {@code
     * defaultValue} or {@code null}.
     */
    public static void writeString(ObjectNode payload,
                                   String fieldName,
                                   String result,
                                   String defaultValue) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if ((result == null) || result.equals(defaultValue)) {
            return;
        }
        payload.put(fieldName, result);
    }

    /**
     * Writes a json node to the payload. Skips writing if it is an array or
     * object node and it is empty, or it is the default value.
     *
     */
    public static void writeJsonNode(ObjectNode payload,
                                     String fieldName,
                                     JsonNode result,
                                     JsonNode defaultValue) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if (result == null) {
            return;
        }
        if (result.isObject() || result.isArray()) {
            if (result.isEmpty()) {
                return;
            }
        } else if (result.equals(defaultValue)) {
            return;
        }
        payload.put(fieldName, result);
    }

    /**
     * Writes a {@link JsonSerializable} to the payload if it is not empty.
     */
    public static void writeJsonSerializable(ObjectNode payload,
                                             String fieldName,
                                             JsonSerializable result) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if (result == null) {
            return;
        }
        if (result.isDefault()) {
            return;
        }
        payload.put(fieldName, result.toJson());
    }

    /**
     * Writes a {@link JsonSerializable} array to the payload. Skips the item
     * if it is empty.
     */
    public static void writeArray(ObjectNode payload,
                                  String fieldName,
                                  JsonSerializable...results) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if (allDefault(results)) {
            return;
        }
        payload.put(
            fieldName,
            Arrays.stream(results)
            .filter((r) -> !r.isDefault())
            .map((r) -> r.toJson())
            .collect(getArrayCollector()));
    }

    /**
     * Returns {@code true} if all the json serializables are default or the
     * array is empty.
     */
    public static boolean allDefault(JsonSerializable...results)
    {
        for (JsonSerializable result : results) {
            if ((result != null) && !result.isDefault()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Writes a {@link JsonSerializable} list to the payload. Skips the item if
     * it is empty.
     */
    public static void writeList(
        ObjectNode payload,
        String fieldName,
        Collection<? extends JsonSerializable> results)
    {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if (isDefault(results)) {
            return;
        }
        final ArrayNode listResult =
            results.stream()
                .filter((r) -> !r.isDefault())
                .map((r) -> r.toJson())
                .collect(getArrayCollector());
        if (!listResult.isEmpty()) {
            payload.put(fieldName, listResult);
        }
    }

    /**
     * Returns {@code true} if all json serializables in the collection are
     * default or the collection is empty.
     */
    public static boolean isDefault(
        Collection<? extends JsonSerializable> collection)
    {
        if (collection == null) {
            return true;
        }
        if (collection.isEmpty()) {
            return true;
        }
        if (collection.stream().anyMatch((p) -> !p.isDefault())) {
            return false;
        }
        return true;
    }

    /**
     * Writes a {@link JsonSerializable} map to the payload. Skips the item if
     * it is empty.
     */
    public static void writeMap(
        ObjectNode payload,
        String fieldName,
        Map<String, ? extends JsonSerializable> results)
    {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(fieldName, "fieldName");
        if (isDefault(results)) {
            return;
        }
        final ObjectNode mapResult =
            results.entrySet().stream()
                .filter((e) -> !e.getValue().isDefault())
                .map((e) ->
                     new AbstractMap.SimpleImmutableEntry
                     <String, JsonNode>(
                         e.getKey(), e.getValue().toJson()))
                .collect(getObjectCollector());
        if (!mapResult.isEmpty()) {
            payload.put(fieldName, mapResult);
        }
    }

    /**
     * Returns {@code true} if all json serializables in the map are default or
     * the map is empty.
     */
    public static <T> boolean isDefault(
        Map<T, ? extends JsonSerializable> map)
    {
        if (map == null) {
            return true;
        }
        if (map.isEmpty()) {
            return true;
        }
        if (map.values().stream().anyMatch((p) -> !p.isDefault())) {
            return false;
        }
        return true;
    }
}
