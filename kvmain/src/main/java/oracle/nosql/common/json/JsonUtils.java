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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Base64;
import java.util.Collections;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * A class of utility methods to encapsulate manipulation of JSON and insulate
 * callers from the specific library that implements the JSON processing.
 *
 * As of this time these methods use the GSON library for the object model and
 * pojo mappings. The JSON object mode is encapsulated in the associated
 * JsonNode, JsonArray and JsonObject classes in this package
 *
 * NoSQL classes should *not* use any JSON libraries directly with the
 * (current) exception of those involved with the Jackson JsonParser, which
 * is part of the core and generally free from CVEs and other obstacles to
 * security and deployment.
 */
public final class JsonUtils {

    /*
     * See *Adapter classes below for what they do
     *
     * consider other options for Gson, see javadoc
     */
    private static final Gson gson = new GsonBuilder()
        .registerTypeAdapter(Double.class,  new DoubleSerializer())
        .registerTypeHierarchyAdapter(JsonNode.class, new JsonNodeSerializer())
        .registerTypeHierarchyAdapter(byte[].class,
                                      new ByteArrayAdapter())
        .serializeNulls() // don't ignore null values in Map<>
        .create();

    private static final Gson gsonPretty =
        new GsonBuilder()
        .registerTypeAdapter(Double.class,  new DoubleSerializer())
        .registerTypeHierarchyAdapter(JsonNode.class, new JsonNodeSerializer())
        .setPrettyPrinting()
        .registerTypeHierarchyAdapter(byte[].class,
                                      new ByteArrayAdapter())
        .serializeNulls() // don't ignore null values in Map<>
        .create();

    /**
     * Returns a new json node representing a boolean.
     *
     * @return the json node
     */
    public static JsonNode createJsonNode(boolean value) {
        if (value) {
            return JsonNode.jsonNodeTrue;
        }
        return JsonNode.jsonNodeFalse;
    }

    /**
     * Returns a new json node representing an integer.
     *
     * @return the json node
     */
    public static JsonNode createJsonNode(int value) {
        return new JsonNode(new JsonPrimitive(value));
    }

    /**
     * Returns a new json node representing a long.
     *
     * @return the json node
     */
    public static JsonNode createJsonNode(long value) {
        return new JsonNode(new JsonPrimitive(value));
    }

    /**
     * Returns a new json node representing a double.
     *
     * @return the json node
     */
    public static JsonNode createJsonNode(double value) {
        return new JsonNode(new JsonPrimitive(value));
    }

    /**
     * Returns a new json node representing a string.
     *
     * @return the json node
     */
    public static JsonNode createJsonNode(String value) {
        return new JsonNode(new JsonPrimitive(value));
    }

    /**
     * Returns a new json node from a gson element. This method is used mostly
     * for backward compatibility.
     *
     * @return the json node
     */
    public static JsonNode createJsonNode(JsonElement elem) {
        if (elem.isJsonPrimitive()) {
            return new JsonNode(elem);
        } else if (elem.isJsonObject()) {
            return new ObjectNode(elem.getAsJsonObject());
        } else if (elem.isJsonArray()) {
            return new ArrayNode(elem.getAsJsonArray());
        } else if (elem.isJsonNull()) {
            return JsonNode.jsonNodeNull;
        } else {
            throw new IllegalArgumentException(
                "Cannot create JsonNode from gson element: " + elem);
        }
    }

    /**
     * Returns a new json node representing a null value.
     *
     * @return the json node
     */
    public static JsonNode createJsonNull() {
        return JsonNode.jsonNodeNull;
    }

    /**
     * Returns a new object node.
     *
     * @return the object node
     */
    public static ObjectNode createObjectNode() {
        return new ObjectNode(new JsonObject());
    }

    /**
     * Returns a new object node from a gson JsonObject.
     *
     * @return the object node
     */
    public static ObjectNode createObjectNode(JsonObject elem) {
        return new ObjectNode(elem);
    }

    /**
     * Returns a new array node.
     *
     * @return the array node
     */
    public static ArrayNode createArrayNode() {
        return new ArrayNode(new JsonArray());
    }

    /**
     * Returns a new array node from a gson JsonArray.
     *
     * @return the object node
     */
    public static ArrayNode createArrayNode(JsonArray elem) {
        return new ArrayNode(elem);
    }

    /**
     * This method is a wrapper around the toJsonTree in the gson library. It
     * serializes the specified object into its equivalent representation as a
     * tree of {@link JsonNode}. Use this method for both generic and
     * non-generic instances unless we need to de-serialize Json instances into
     * Java instances.
     *
     * The gson library cautioned that this method should be used when the
     * specified object is not a generic type.  The issue is that due to Java
     * erausre, gson does not know about the types of the generic objects See
     * https://github.com/google/gson/blob/master/UserGuide.md#TOC-Serializing-and-Deserializing-Generic-Types
     * and
     * https://github.com/google/gson/blob/master/UserGuide.md#TOC-Serializing-and-Deserializing-Collection-with-Objects-of-Arbitrary-Types.
     * for more detail. From my understanding, there is only an issue if the
     * serialized Json instance is then used to de-serialize into the Java
     * instance. Since we are never going to do that, we are not providing the
     * method for the generic instances.
     */
    public static JsonNode toJsonNode(Object obj) {
        return new JsonNode(gson.toJsonTree(obj));
    }

    /**
     * Returns the value of a field in text format if the node is an object
     * node with the specified value field, otherwise null.
     *
     * @param node the node
     * @param field the field name
     * @return the field value as text or null
     */
    public static String getAsText(JsonNode node, String field) {
        return getAsText(node, field, null);
    }

    /**
     * Returns the value of a field in text format if the node is an object
     * node with the specified value field, otherwise returns the default
     * value.
     *
     * @param node the node
     * @param field the field name
     * @param defaultValue the default value
     * @return the field value as text or the default
     */
    public static String getAsText(JsonNode node, String field,
                                   String defaultValue) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isValueNode()) {
            return defaultValue;
        }
        return fieldNode.asText();
    }

    /**
     * Returns the object node for a field if the node is an object node with
     * the specified object field, otherwise null.
     *
     * @param node the node
     * @param field the field name
     * @return the field value or null
     */
    public static ObjectNode getObject(JsonNode node, String field) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isObject()) {
            return null;
        }
        return (ObjectNode) fieldNode;
    }

    /**
     * Returns an iterable object over the elements of an array for a field if
     * the node is an object node with the specified array field, otherwise
     * an empty iterable.
     *
     * @param node the node
     * @param field the field name
     * @return an iterable over the array elements or an empty iterable
     */
    public static Iterable<JsonNode> getArray(JsonNode node, String field) {
        final JsonNode fieldNode = node.get(field);
        if ((fieldNode == null) || !fieldNode.isArray()) {
            return Collections.emptyList();
        }
        return fieldNode;
    }

    /**
     * Compare the contents of two JSON strings without regard
     * to the order in which fields appear in the string.
     *
     * @param a , b Two JSON strings to compare, they must be non-null and
     * they must be JSON Objects (vs primitives or arrays).
     * @return true if the two strings represent equivalent JSON documents
     */
    public static boolean jsonStringsEqual(String a, String b) {
        ObjectNode nodeA = parseJsonObject(a);
        ObjectNode nodeB = parseJsonObject(b);
        return nodeA.equals(nodeB);
    }

    /*
     * Methods to parse directly into JsonNode, ObjectNode
     */

    /**
     * Parses a JSON string into a JsonNode
     *
     * @param s the string to parse, must be non-null
     * @return the JsonNode
     * @throws IllegalArgumentException if there is a problem parsing
     * the string
     */
    public static JsonNode parseJsonNode(final String s) {
        try {
            JsonElement element =
                com.google.gson.JsonParser.parseString(s);
            return createJsonNode(element);
        } catch (JsonParseException jpe) {
            throw new IllegalArgumentException(
                ("Unable to parse JSON: " + s + ":" + jpe.getMessage()), jpe);
        }
    }

    /**
     * Parses a JSON string into an ObjectNode
     *
     * @param s the JSON string, must be non-null
     * @return An ObjectNode representing the JSON document.
     *
     * @throws IllegalArgumentException if the string is not a JSON
     * object
     */
    public static ObjectNode parseJsonObject(final String s) {
        try {
            JsonElement element =
                com.google.gson.JsonParser.parseString(s);
            if (!element.isJsonObject()) {
                throw new IllegalArgumentException(
                    "JSON string is not an object");
            }
            return new ObjectNode(element.getAsJsonObject());
        } catch (JsonParseException jpe) {
            throw new IllegalArgumentException(
                ("Unable to parse JSON: " + s + ":" + jpe.getMessage()), jpe);
        }
    }

    /**
     * Returns the default gson singleton. This should be used sparingly
     * to avoid exposing GSON to users.
     *
     * @returns the Gson singleton
     */
    public static Gson getGson() {
        return gson;
    }

    /**
     * Wrapper around POJO-based methods when using JsonNode.
     * This is for internal API compatibility.
     *
     * @param node the JsonNode
     * @param pretty set to true if pretty-printing is desired
     * @returns the JSON string representing the JsonNode
     */
    public static String toJsonString(JsonNode node, boolean pretty) {
        if (pretty) {
            return gsonPretty.toJson(node.getElement());
        }
        return gson.toJson(node.getElement());
    }

    /*
     * Methods to serialize and deserialize Java objects (POJOs).
     * There are redundant methods to minimize changes to callers
     */

    /**
     * Writes a Java Object to the specified file as JSON
     *
     * @param f the file, must be non-null
     * @param o the object, must be non-null
     * @param pretty set to true for pretty printing
     *
     * @throws IOException if there's a problem with the file
     */
    public static void writeFile(File f, Object o, boolean pretty)
        throws IOException {
        try (FileWriter writer = new FileWriter(f)) {
            if (o instanceof JsonNode) {
                o = ((JsonNode)o).getElement();
            }
            if (pretty) {
                gsonPretty.toJson(o, writer);
            } else {
                gson.toJson(o, writer);
            }
        }
    }

    /**
     * Writes a Java Object as JSON to a string
     *
     * @param o the object, must be non-null
     * @param pretty set to true for pretty printing
     * @returns the JSON string
     */
    public static String writeAsJson(Object o, boolean pretty) {
        if (o instanceof JsonNode) {
            o = ((JsonNode)o).getElement();
        }
        if (pretty) {
            return gsonPretty.toJson(o);
        }
        return gson.toJson(o);
    }

    /**
     * Writes a Java Object as JSON to a string. This is the same as
     * writeAsJson()
     *
     * @param o the object, must be non-null
     * @param pretty set to true for pretty printing
     * @returns the JSON string
     */
    public static String print(Object o, boolean pretty) {
        if (o instanceof JsonNode) {
            o = ((JsonNode)o).getElement();
        }
        if (pretty) {
            return gsonPretty.toJson(o);
        }
        return gson.toJson(o);
    }

    /**
     * Writes a Java Object as JSON to a string, no pretty print.
     * This is the same as writeAsJson(o, false)
     *
     * @param o the object, must be non-null
     * @returns the JSON string
     */
    public static String print(Object o) {
        return print(o, false);
    }

    /**
     * Writes a Java Object as JSON to a string, pretty print.
     * This is the same as writeAsJson(o, true)
     *
     * @param o the object, must be non-null
     * @returns the JSON string
     */
    public static String prettyPrint(Object o) {
        return print(o, true);
    }

    /**
     * Writes a Java Object as JSON to a string, no pretty print.
     * This is the same as writeAsJson(o, false)
     *
     * @param o the object, must be non-null
     * @returns the JSON string
     */
    public static String writeAsJson(Object o) {
        return writeAsJson(o, false);
    }

    /**
     * Writes a Java Object as JSON to a string, no pretty print.
     * This is the same as writeAsJson(o, false)
     *
     * @param o the object, must be non-null
     * @returns the JSON string
     */
    public static String toJson(Object o) {
        return writeAsJson(o, false);
    }

    /**
     * Reads (deserializes) an object of the specified type from a file
     * @param f the file
     * @param type the type of the Java object to read, must be non-null
     * @return an instance of the object
     *
     * @throws IOException if there is a problem reading the file
     */
    public static <T> T fromFile(File f, Type type)
        throws IOException {
        try (FileReader reader = new FileReader(f)) {
            return fromJson(reader, type);
        }
    }

    /**
     * Reads (deserializes) an object of the specified type from a Reader
     * @param reader the Reader to use
     * @param type the type of the Java object to read, must be non-null
     * @return an instance of the object
     *
     * @throws IllegalArgumentException if there is a problem reading the object
     */
    public static <T> T fromJson(Reader reader, Type type) {
        try {
            return gson.fromJson(reader, type);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException(
                "Failed to deserialize into class " + type + ": " + e);
        }
    }

    /**
     * Reads (deserializes) an object of the specified type from an
     * InputStream
     * @param rjson the InputStream, must be non-null
     * @param type the type of the Java object to read, must be non-null
     * @return an instance of the object
     *
     * @throws IllegalArgumentException if there is a problem reading the object
     */
    public static <T> T readValue(InputStream json, Type type) {
        try (InputStreamReader reader = new InputStreamReader(json)) {
            return fromJson(reader, type);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Failed to deserialize into class " + type + ": " + ioe);
        }
    }

    /**
     * Reads (deserializes) an object of the specified type from a string
     * @param json the JSON string to use, must be non-null
     * @param type the type of the Java object to read, must be non-null
     * @return an instance of the object
     *
     * @throws IllegalArgumentException if there is a problem reading the object
     */
    public static <T> T fromJson(String json, Type type) {
        try {
            return gson.fromJson(json, type);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException(
                "Failed to deserialize into class " + type + ": " + e);
        }
    }

    /**
     * Reads (deserializes) an object of the specified type from a byte[]
     * @param bytes the JSON as byte[] to use, must be non-null
     * @param type the type of the Java object to read, must be non-null
     * @return an instance of the object
     *
     * @throws IllegalArgumentException if there is a problem reading the object
     */
    public static <T> T readValue(byte[] bytes, Type type)
        throws IOException {
        try (Reader reader =
             new InputStreamReader(new ByteArrayInputStream(bytes))) {
            return fromJson(reader, type);
        }
    }

    /**
     * Reads (deserializes) an object of the specified type from a string
     * This is the same as fromJson(String, Type)
     *
     * @param json the JSON string to use, must be non-null
     * @param type the type of the Java object to read, must be non-null
     * @return an instance of the object
     *
     * @throws IllegalArgumentException if there is a problem reading the object
     */
    public static <T> T readValue(String json, Type type) {
        return fromJson(json, type);
    }

    /*
     * The encode/decode Base64 methods are here in order to provide
     * consistent binary encode/decode inside JSON
     */

    /**
     * Translates the specified Base64 string into a byte array.
     *
     * @param buf a non-null byte buffer
     *
     * @return a Base64 encoded string that can be decoded using
     * decodeBase64
     */
    public static String encodeBase64(byte[] buf) {
        return Base64.getEncoder().encodeToString(buf);
    }

    /**
     * Decodes the specified Base64 string into a byte array. The string
     * should have been encoded using encodeBase64
     *
     * @param str a non-null encoded string
     *
     * @return a new byte[] containing the decoded byte array.
     */
    public static byte[] decodeBase64(String str) {
        return Base64.getDecoder().decode(str);
    }

    /**
     * This class ensures that when JSON is parsed into a generic Map that
     * integer/long values don't end up as Double
     */
    private static class DoubleSerializer implements JsonSerializer<Double> {

        @Override
        public JsonElement serialize(Double src,
                                     Type typeOfSrc,
                                     JsonSerializationContext context) {
            if(src == src.longValue())
                return new JsonPrimitive(src.longValue());
            return new JsonPrimitive(src);
        }
    }

    /*
     * turn byte[] into Base64 and vice versa
     */
    private static class ByteArrayAdapter implements JsonSerializer<byte[]>,
                                                     JsonDeserializer<byte[]> {

        @Override
        public byte[] deserialize(JsonElement json,
                                  Type typeOfT,
                                  JsonDeserializationContext context)
            throws JsonParseException {
            return decodeBase64(json.getAsString());
        }

        @Override
        public JsonElement serialize(byte[] src,
                                     Type typeOfSrc,
                                     JsonSerializationContext context) {
            return new JsonPrimitive(encodeBase64(src));
        }
    }

    /*
     * This class handles the case where a JsonNode or subclass is a
     * member of a class that is being serialized or deserialized
     * directly. It eliminates what would otherwise appear as an 'element'
     * JSON object that wraps the actual JsonElement (see JsonNode.element)
     */
    private static class JsonNodeSerializer
        implements JsonSerializer<JsonNode>, JsonDeserializer<JsonNode> {

        @Override
        public JsonElement serialize(JsonNode src,
                                     Type typeOfSrc,
                                     JsonSerializationContext context) {
            return src.getElement();
        }

        @Override
        public JsonNode deserialize(JsonElement json,
                                    Type typeOfT,
                                    JsonDeserializationContext context)
            throws JsonParseException {
            return createJsonNode(json);
        }
    }
}
