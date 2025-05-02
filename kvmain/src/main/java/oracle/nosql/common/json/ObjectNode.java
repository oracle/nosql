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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * A JsonNode implementation where the wrapped object is a JSON
 * object, represented by JsonObject. Map elements are maintained as
 * raw gson JsonElement instances.
 */
public class ObjectNode extends JsonNode {

    protected ObjectNode(JsonObject element) {
        super(element);
    }

    /**
     * Returns the number of key-value mappings in the map of this ObjectNode.
     */
    public int size() {
        return element.getAsJsonObject().size();
    }

    @Override
    public Iterator<JsonNode> iterator() {
        return new ObjectNodeIterator();
    }

    @Override
    public JsonNode get(String key) {
        JsonElement elem = element.getAsJsonObject().get(key);
        if (elem != null) {
            return JsonUtils.createJsonNode(elem);
        }
        return null;
    }

    public Set<Map.Entry<String, JsonNode>> entrySet() {
        final JsonObject elem = element.getAsJsonObject();
        if (elem == null) {
            return null;
        }
        return elem.entrySet().stream()
            .map((e) ->
                 new AbstractMap.SimpleImmutableEntry<String, JsonNode>(
                     e.getKey(),
                     JsonUtils.createJsonNode(e.getValue())))
            .collect(Collectors.toSet());
    }

    public ObjectNode getObject(String key) {
        JsonElement elem = element.getAsJsonObject().get(key);
        if (elem != null && elem.isJsonObject()) {
            return new ObjectNode(elem.getAsJsonObject());
        }
        return null;
    }

    public ObjectNode put(String key, String value) {
        element.getAsJsonObject().addProperty(key, value);
        return this;
    }

    public ObjectNode put(String key, int value) {
        element.getAsJsonObject().addProperty(key, value);
        return this;
    }

    public ObjectNode put(String key, long value) {
        element.getAsJsonObject().addProperty(key, value);
        return this;
    }

    public ObjectNode put(String key, double value) {
        element.getAsJsonObject().addProperty(key, value);
        return this;
    }

    public ObjectNode put(String key, Number value) {
        element.getAsJsonObject().addProperty(key, value);
        return this;
    }

    public ObjectNode put(String key, boolean value) {
        element.getAsJsonObject().addProperty(key, value);
        return this;
    }

    public ObjectNode putNull(String key) {
        element.getAsJsonObject().add(key, JsonNode.jsonNodeNull.getElement());
        return this;
    }

    public ObjectNode put(String key, JsonNode value) {
        element.getAsJsonObject().add(key, value.getElement());
        return this;
    }

    /**
     * Replace vs add.
     */
    public ObjectNode set(String key, JsonNode value) {
        element.getAsJsonObject().add(key, value.getElement());
        return this;
    }

    public ObjectNode setNull(String key) {
        element.getAsJsonObject().add(key, JsonNode.jsonNodeNull.getElement());
        return this;
    }

    public ArrayNode putArray(String key) {
        JsonArray array = new JsonArray();
        element.getAsJsonObject().add(key, array);
        return new ArrayNode(array);
    }

    public ObjectNode putObject(String key) {
        JsonObject object = new JsonObject();
        element.getAsJsonObject().add(key, object);
        return new ObjectNode(object);
    }

    public JsonNode remove(String key) {
        JsonElement removed = element.getAsJsonObject().remove(key);
        if (removed != null) {
            return JsonUtils.createJsonNode(removed);
        }
        return null;
    }

    public boolean has(String key) {
        return element.getAsJsonObject().has(key);
    }

    @Override
    public boolean isEmpty() {
        return element.getAsJsonObject().size() == 0;
    }

    @Override
    public Set<String> fieldNames() {
        return element.getAsJsonObject().keySet();
    }

    public ObjectNode merge(ObjectNode from) {
        JsonObject toObj = element.getAsJsonObject();
        JsonObject fromObj = from.element.getAsJsonObject();
        for (Map.Entry<String, JsonElement> elem : fromObj.entrySet()) {
            toObj.remove(elem.getKey());
            toObj.add(elem.getKey(), elem.getValue());
        }
        return this;
    }

    /**
     * Returns the first occurrence of the named key in the object tree
     */
    public JsonNode findFirst(String key) {
        JsonElement elem = findFirst(element.getAsJsonObject(), key);
        if (elem != null) {
            return JsonUtils.createJsonNode(elem);
        }
        return null;
    }

    private JsonElement findFirst(JsonObject obj, String key) {
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            if (entry.getKey().equals(key)) {
                return entry.getValue();
            }
            if (entry.getValue().isJsonObject()) {
                JsonElement elem =
                    findFirst(entry.getValue().getAsJsonObject(), key);
                if (elem != null) {
                    return elem;
                }
            }
            if (entry.getValue().isJsonArray()) {
                for (JsonElement ent : entry.getValue().getAsJsonArray()) {
                    if (ent.isJsonObject()) {
                        JsonElement elem =
                            findFirst(ent.getAsJsonObject(), key);
                        if (elem != null) {
                            return elem;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Returns all occurrences of the named key in the object tree
     */
    public ArrayList<JsonNode> findAll(String key) {
        ArrayList<JsonNode> nodes = new ArrayList<JsonNode>();
        ArrayList<JsonElement> elems = findAll(element.getAsJsonObject(), key);

        for (JsonElement elem : elems) {
            nodes.add(JsonUtils.createJsonNode(elem));
        }
        return nodes;
    }

    private ArrayList<JsonElement> findAll(JsonObject obj, String key) {
        ArrayList<JsonElement> elements = new ArrayList<JsonElement>();
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            if (entry.getKey().equals(key)) {
                elements.add(entry.getValue());
            }
            if (entry.getValue().isJsonObject()) {
                ArrayList<JsonElement> elems =
                    findAll(entry.getValue().getAsJsonObject(), key);
                elements.addAll(elems);
            }
            if (entry.getValue().isJsonArray()) {
                for (JsonElement ent : entry.getValue().getAsJsonArray()) {
                    if (ent.isJsonObject()) {
                        ArrayList<JsonElement> elems =
                            findAll(ent.getAsJsonObject(), key);
                        elements.addAll(elems);
                    }
                }
            }
        }
        return elements;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ObjectNode) {
            return element.getAsJsonObject().equals(
                ((ObjectNode)other).element.getAsJsonObject());
        }
        return false;
    }

    public void removeAll() {
        element = new JsonObject();
    }

    @Override
    public int hashCode() {
        return element.getAsJsonObject().hashCode();
    }

    @Override
    public String toString() {
        return element.getAsJsonObject().toString();
    }

    private class ObjectNodeIterator implements Iterator<JsonNode> {

        private final Iterator<Map.Entry<String, JsonElement>> entries;

        ObjectNodeIterator() {
            entries = element.getAsJsonObject().entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return entries.hasNext();
        }

        @Override
        public JsonNode next() {
            Map.Entry<String, JsonElement> entry = entries.next();
            return JsonUtils.createJsonNode(entry.getValue());
        }
    }
}
