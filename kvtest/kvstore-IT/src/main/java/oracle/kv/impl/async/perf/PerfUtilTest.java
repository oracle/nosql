/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.async.perf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.TestBase;
import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import org.junit.Test;

/**
 * Tests the perf util methods.
 */
public class PerfUtilTest extends TestBase {

    /**
     * Tests the dfs iteration on a json primitive.
     */
    @Test
    public void testDfsPrimitive() {
        final JsonNode jsonNode = JsonUtils.createJsonNode(42);
        final Iterator<Map.Entry<JsonReference, JsonNode>> iter =
            PerfUtil.depthFirstSearchIterator(jsonNode);
        int count = 0;
        while (iter.hasNext()) {
            final Map.Entry<JsonReference, JsonNode> entry = iter.next();
            assertEquals(JsonReference.ROOT, entry.getKey());
            assertEquals(jsonNode, entry.getValue());
            count++;
        }
        assertEquals(1, count);
    }

    /**
     * Tests the dfs iteration on a json array with primitive elements.
     */
    @Test
    public void testDfsArrayBasic() {
        final ArrayNode arrayNode = JsonUtils.createArrayNode();
        final JsonNode primitiveNode1 = JsonUtils.createJsonNode(42);
        final JsonNode primitiveNode2 = JsonUtils.createJsonNode(43);
        arrayNode.add(primitiveNode1);
        arrayNode.add(primitiveNode2);
        final Iterator<Map.Entry<JsonReference, JsonNode>> iter =
            PerfUtil.depthFirstSearchIterator(arrayNode);
        int count = 0;
        while (iter.hasNext()) {
            final Map.Entry<JsonReference, JsonNode> entry = iter.next();
            if (count == 0) {
                assertEquals(JsonReference.ROOT, entry.getKey());
                assertEquals(arrayNode, entry.getValue());
            } else if (count == 1) {
                assertEquals("/0", entry.getKey().getString("/"));
                assertEquals(primitiveNode1, entry.getValue());
            } else {
                assertEquals("/1", entry.getKey().getString("/"));
                assertEquals(primitiveNode2, entry.getValue());
            }
            count++;
        }
        assertEquals(3, count);
    }

    /**
     * Tests the dfs iteration on a json object with primitive elements.
     */
    @Test
    public void testDfsObjectBasic() {
        final ObjectNode objectNode = JsonUtils.createObjectNode();
        final JsonNode primitiveNode1 = JsonUtils.createJsonNode(42);
        final JsonNode primitiveNode2 = JsonUtils.createJsonNode(43);
        objectNode.put("a", primitiveNode1);
        objectNode.put("b", primitiveNode2);
        final Iterator<Map.Entry<JsonReference, JsonNode>> iter =
            PerfUtil.depthFirstSearchIterator(objectNode);
        int count = 0;
        while (iter.hasNext()) {
            final Map.Entry<JsonReference, JsonNode> entry = iter.next();
            if (count == 0) {
                assertEquals(JsonReference.ROOT, entry.getKey());
                assertEquals(objectNode, entry.getValue());
            } else {
                if (entry.getKey().getString("/").equals("/a")) {
                    assertEquals(primitiveNode1, entry.getValue());
                } else if (entry.getKey().getString("/").equals("/b")) {
                    assertEquals(primitiveNode2, entry.getValue());
                } else {
                    fail(String.format(
                        "Incorrect reference key: %s", entry.getKey()));
                }
            }
            count++;
        }
        assertEquals(3, count);
    }

    /**
     * Tests the dfs iteration on a nested json document.
     */
    @Test
    public void testDfsNested() {
        final ArrayNode node0 = JsonUtils.createArrayNode();
        final ArrayNode node00 = JsonUtils.createArrayNode();
        final ObjectNode node10 = JsonUtils.createObjectNode();
        node0.add(node00);
        node0.add(node10);
        final JsonNode node000 = JsonUtils.createJsonNode(42);
        final JsonNode node100 = JsonUtils.createJsonNode(43);
        node00.add(node000);
        node00.add(node100);
        final JsonNode node110 = JsonUtils.createJsonNode(44);
        node10.put("a", node110);
        final Iterator<Map.Entry<JsonReference, JsonNode>> iter =
            PerfUtil.depthFirstSearchIterator(node0);
        final List<String> keys = new ArrayList<>();
        final List<JsonNode> vals = new ArrayList<>();
        while (iter.hasNext()) {
            final Map.Entry<JsonReference, JsonNode> entry = iter.next();
            keys.add(entry.getKey().getString("/"));
            vals.add(entry.getValue());
        }
        assertEquals(Arrays.asList("", "/0", "/0/0", "/0/1", "/1", "/1/a"),
                     keys);
        assertEquals(Arrays.asList(node0, node00, node000, node100,
                                   node10, node110),
                     vals);
    }

    /**
     * Tests the bfs iteration on a json primitive.
     */
    @Test
    public void testBfsPrimitive() {
        final JsonNode jsonNode = JsonUtils.createJsonNode(42);
        final Iterator<Map.Entry<JsonReference, JsonNode>> iter =
            PerfUtil.breadthFirstSearchIterator(jsonNode);
        int count = 0;
        while (iter.hasNext()) {
            final Map.Entry<JsonReference, JsonNode> entry = iter.next();
            assertEquals(JsonReference.ROOT, entry.getKey());
            assertEquals(jsonNode, entry.getValue());
            count++;
        }
        assertEquals(1, count);
    }

    /**
     * Tests the bfs iteration on a json array with primitive elements.
     */
    @Test
    public void testBfsArrayBasic() {
        final ArrayNode arrayNode = JsonUtils.createArrayNode();
        final JsonNode primitiveNode1 = JsonUtils.createJsonNode(42);
        final JsonNode primitiveNode2 = JsonUtils.createJsonNode(43);
        arrayNode.add(primitiveNode1);
        arrayNode.add(primitiveNode2);
        final Iterator<Map.Entry<JsonReference, JsonNode>> iter =
            PerfUtil.breadthFirstSearchIterator(arrayNode);
        int count = 0;
        while (iter.hasNext()) {
            final Map.Entry<JsonReference, JsonNode> entry = iter.next();
            if (count == 0) {
                assertEquals(JsonReference.ROOT, entry.getKey());
                assertEquals(arrayNode, entry.getValue());
            } else if (count == 1) {
                assertEquals("/0", entry.getKey().getString("/"));
                assertEquals(primitiveNode1, entry.getValue());
            } else {
                assertEquals("/1", entry.getKey().getString("/"));
                assertEquals(primitiveNode2, entry.getValue());
            }
            count++;
        }
        assertEquals(3, count);
    }

    /**
     * Tests the bfs iteration on a json object with primitive elements.
     */
    @Test
    public void testBfsObjectBasic() {
        final ObjectNode objectNode = JsonUtils.createObjectNode();
        final JsonNode primitiveNode1 = JsonUtils.createJsonNode(42);
        final JsonNode primitiveNode2 = JsonUtils.createJsonNode(43);
        objectNode.put("a", primitiveNode1);
        objectNode.put("b", primitiveNode2);
        final Iterator<Map.Entry<JsonReference, JsonNode>> iter =
            PerfUtil.breadthFirstSearchIterator(objectNode);
        int count = 0;
        while (iter.hasNext()) {
            final Map.Entry<JsonReference, JsonNode> entry = iter.next();
            if (count == 0) {
                assertEquals(JsonReference.ROOT, entry.getKey());
                assertEquals(objectNode, entry.getValue());
            } else {
                if (entry.getKey().getString("/").equals("/a")) {
                    assertEquals(primitiveNode1, entry.getValue());
                } else if (entry.getKey().getString("/").equals("/b")) {
                    assertEquals(primitiveNode2, entry.getValue());
                } else {
                    fail(String.format(
                        "Incorrect reference key: %s", entry.getKey()));
                }
            }
            count++;
        }
        assertEquals(3, count);
    }

    /**
     * Tests the bfs iteration on a nested json document.
     */
    @Test
    public void testBfsNested() {
        final ArrayNode node0 = JsonUtils.createArrayNode();
        final ArrayNode node00 = JsonUtils.createArrayNode();
        final ObjectNode node10 = JsonUtils.createObjectNode();
        node0.add(node00);
        node0.add(node10);
        final JsonNode node000 = JsonUtils.createJsonNode(42);
        final JsonNode node100 = JsonUtils.createJsonNode(43);
        node00.add(node000);
        node00.add(node100);
        final JsonNode node110 = JsonUtils.createJsonNode(44);
        node10.put("a", node110);
        final Iterator<Map.Entry<JsonReference, JsonNode>> iter =
            PerfUtil.breadthFirstSearchIterator(node0);
        final List<String> keys = new ArrayList<>();
        final List<JsonNode> vals = new ArrayList<>();
        while (iter.hasNext()) {
            final Map.Entry<JsonReference, JsonNode> entry = iter.next();
            keys.add(entry.getKey().getString("/"));
            vals.add(entry.getValue());
        }
        assertEquals(Arrays.asList("", "/0", "/1", "/0/0", "/0/1", "/1/a"),
                     keys);
        assertEquals(Arrays.asList(node0, node00, node10,
                                   node000, node100, node110),
                     vals);
    }
}
