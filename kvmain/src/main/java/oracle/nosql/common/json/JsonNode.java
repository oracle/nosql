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

package oracle.nosql.common.json;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonNull;
import com.google.gson.JsonPrimitive;

/**
 * A wrapper class around gson JsonElement. This and related classes
 * exist for 2 reasons:
 * 1. compatibility with older, Jackson-based classes to keep code change
 * minimal when Jackson was removed.
 * 2. insulated callers from the actual implementation objects so that if
 * it needs to change again it can be done without modifying callers
 */
public class JsonNode implements Iterable<JsonNode> {

    protected JsonElement element;

    public static JsonNode jsonNodeNull = new JsonNode(JsonNull.INSTANCE);
    public static JsonNode jsonNodeTrue = new JsonNode(new JsonPrimitive(true));
    public static JsonNode jsonNodeFalse = new JsonNode(new JsonPrimitive(false));

    public static JsonNode getJsonNodeNull() {
        return jsonNodeNull;
    }

    public static JsonNode getJsonNodeTrue() {
        return jsonNodeTrue;
    }

    public static JsonNode getJsonNodeFalse() {
        return jsonNodeFalse;
    }

    protected JsonNode(JsonElement element) {
        this.element = element;
    }

    /**
     * Creates the appropriate type of wrapper based on type of
     * the element.
     *
     * This method is deprecated. Please use {@link JsonUtils#createJsonNode}.
     *
     * @deprecated since 1.1.5
     */
    @Deprecated
    public static JsonNode createJsonNode(JsonElement elem) {
        return JsonUtils.createJsonNode(elem);
    }

    /**
     * Creates a json node from a long value.
     *
     * This method is deprecated. Please use {@link JsonUtils#createJsonNode}.
     *
     * @deprecated since 1.1.5
     */
    @Deprecated
    public static JsonNode createJsonNode(long value) {
        return JsonUtils.createJsonNode(value);
    }

    /**
     * Creates a json node from a long value.
     *
     * This method is deprecated. Please use {@link JsonUtils#createObjectNode}.
     *
     * @deprecated since 1.1.5
     */
    @Deprecated
    public static ObjectNode createObjectNode(JsonObject elem) {
        return JsonUtils.createObjectNode(elem);
    }

    @Override
    public Iterator<JsonNode> iterator() {
        return Collections.emptyIterator();
    }

    public JsonElement getElement() {
        return element;
    }

    public boolean isPrimitive() {
        return element.isJsonPrimitive();
    }

    public boolean isObject() {
        return element.isJsonObject();
    }

    public boolean isString() {
        return element.isJsonPrimitive() &&
            element.getAsJsonPrimitive().isString();
    }

    public boolean isBoolean() {
        return element.isJsonPrimitive() &&
            element.getAsJsonPrimitive().isBoolean();
    }

    public boolean isNumber() {
        return element.isJsonPrimitive() &&
            element.getAsJsonPrimitive().isNumber();
    }

    public boolean isArray() {
        return element.isJsonArray();
    }

    public boolean isValueNode() {
        return element.isJsonPrimitive() || element.isJsonNull();
    }

    public boolean isNull() {
        return element.isJsonNull();
    }

    public JsonNode get(String key) {
        /*
         * TODO: It seems returning null is not good, since that would result
         * in a NullPointerException at the upper layer. I think we should
         * throw IllegalStateException since it is the exception thrown by gson
         * for other kind of format error. On the other hand, we should
         * probably wrap IllegalStateException somewhere here or in the upper
         * layer since IllegalStateException will restart the server which we
         * probably do not want.
         */
        return null;
    }

    public JsonNode get(int index) {
        return null;
    }

    public String asText() {
        return element.getAsString();
    }

    public long asLong() {
        return element.getAsLong();
    }

    public int asInt() {
        return element.getAsInt();
    }

    public double asDouble() {
        return element.getAsDouble();
    }

    public boolean asBoolean() {
        return element.getAsBoolean();
    }

    public ObjectNode asObject() {
        return (ObjectNode) this;
    }

    public ArrayNode asArray() {
        return (ArrayNode) this;
    }

    public String toPrettyString() {
        return JsonUtils.toJsonString(this, true);
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this, false);
    }

    /*
     * Mimic Jackson version
     */
    public Set<String> fieldNames() {
        return Collections.emptySet();
    }

    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof JsonNode) {
            return element.equals(((JsonNode)other).element);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return element.hashCode();
    }
}
