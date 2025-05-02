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

import java.util.Iterator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * A JsonNode implementation where the wrapped object is an array,
 * represented by JsonArray. Array elements are maintained as
 * raw gson JsonElement instances.
 */
public class ArrayNode extends JsonNode {

    protected ArrayNode(JsonArray element) {
        super(element);
    }

    @Override
    public Iterator<JsonNode> iterator() {
        return new ArrayNodeIterator();
    }

    @Override
    public JsonNode get(int index) {
        JsonElement elem = element.getAsJsonArray().get(index);
        return JsonUtils.createJsonNode(elem);
    }

    public ArrayNode add(String value) {
        element.getAsJsonArray().add(value);
        return this;
    }

    public ArrayNode add(JsonNode value) {
        element.getAsJsonArray().add(value.element);
        return this;
    }

    public ObjectNode addObject() {
        JsonObject object = new JsonObject();
        element.getAsJsonArray().add(object);
        return new ObjectNode(object);
    }

    /**
     * Adds all the element from the other and returns this array node.
     */
    public ArrayNode addAll(ArrayNode other) {
        for (JsonNode n : other) {
            add(n);
        }
        return this;
    }

    @Override
    public boolean isEmpty() {
        return (size() == 0);
    }

    public int size() {
        return element.getAsJsonArray().size();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ArrayNode) {
            return element.getAsJsonArray().equals(
                ((ArrayNode)other).element.getAsJsonArray());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return element.getAsJsonArray().hashCode();
    }

    @Override
    public String toString() {
        return element.getAsJsonArray().toString();
    }

    private class ArrayNodeIterator implements Iterator<JsonNode> {

        private final Iterator<JsonElement> iter;

        ArrayNodeIterator() {
            iter = element.getAsJsonArray().iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public JsonNode next() {
            return JsonUtils.createJsonNode(iter.next());
        }
    }
}
