/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

/**
 * FieldValueEventHandler is an event-driven interface that allows multiple
 * implementations of serializers and deserializers for a {@link FieldValue}.
 * The events correspond to the data model exposed by {@link FieldValue}.
 * <p>
 * Events can be generated from {@link FieldValue} instances themselves,
 * from a wire protocol, or from a generic program. Examples of uses include
 * <ul>
 * <li>Binary serializer for the wire protocol, capturing events generated
 * from a {@link FieldValue} instances</li>
 * <li>Binary de-serializer for the wire protocol, capturing events generated
 * from code that is reading the wire protocol</li>
 * <li>JSON serializer for {@link FieldValue} instances</li>
 * <li>JSON pretty-printfor {@link FieldValue} instances</li>
 * <li>Direct-from/to-object serializer or deserializer that operates on
 * POJOs intead of {@link FieldValue}</li>
 * </ul>
 *
 * In addition to the interface this file includes static methods to generate
 * events from a {@link FieldValue} instance. See {@link #generate}.
 * <p>
 * Example usage
 * <pre>
 *   MapValue map = new MapValue();
 *   map.put(...);
 *   ...
 *   JsonSerializer js = new JsonSerializer(null);
 *   FieldValueEventHandler.generate(map, js);
 *   String json = js.toString();
 * </pre>
 * <p>
 * Functionally here is how the event handler is to be used. Look at the
 * {@link #generate} method for implementation details. There are no
 * specific start/end events, although they could be added if deemed useful.
 * <p>
 * Simple (atomic) field value events are just that, e.g.
 * <pre>
 *   booleanValue(true);
 *   integerValue(6);
 * </pre>
 * Map values have <em>startMap</em> and <em>endMap</em> events that surround
 * them. These methods have a size argument to let the event handler know the
 * size of the map. It's possible that the size is not known but it's best to
 * provide one if possible, and in some cases (protocol serialization) it's
 * required. Map entries require key and value, where the value may be a nested
 * map or array. To account for nesting the events are:
 * <pre>
 *   startMap(1)
 *   startMapField(key) // pass the key String
 *   // an event that creates a value, including a nested map or array
 *   stringValue("a string")
 *   endMapField(key)
 *   endMap(1)
 * </pre>
 * Array values have <em>startArray</em> and <em>endArray</em> events that
 * surround them. These methods have a size argument to let the event handler
 * know the size of the array. It's possible that the size is not known but
 * it's best to provide one if possible, and in some cases (protocol
 * serialization) it's required. Array entry events are followed with
 * endArrayField events. This simplifies implementations that need to know if
 * they are working within an array. Here's an array example.
 * <pre>
 *   startArray(2)
 *   startArrayField(0)
 *   startMap(msize) // nested map
 *   endMap(msize)   // map is empty
 *   endArrayField(0)
 *   startArrayField(1)
 *   stringValue("a string") // add string to the array
 *   endArrayField(1)
 *   endArray(2)
 * </pre>
 * @hide-in-sdk
 */
public interface FieldValueEventHandler {

    /**
     * A no-op event handler for cases where a non-null
     * handler is useful
     */
    public static FieldValueEventHandler nullHandler =
        new FieldValueEventHandler() {};

    /**
     * Start a MapValue. This method accepts the size of the map, but there
     * are situations where the size may not be known. In this case -1
     * should be passed and it is up to the implementing class to decide
     * if it can operate without a size and if not, throw
     * IllegalArgumentException.
     *
     * @param size the number of entries in the map or -1 if not known.
     * @throws IllegalArgumentException if the size is invalid or cannot be
     * handled by the implementation.
     * @throws IOException conditionally, based on implementation
     */
    default void startMap(int size) throws IOException {}

    /**
     * Start an ArrayValue. This method accepts the size of the array, but there
     * are situations where the size may not be known. In this case -1
     * should be passed and it is up to the implementing class to decide
     * if it can operate without a size and if not, throw
     * IllegalArgumentException.
     *
     * @param size the number of entries in the array or -1 if not known.
     * @throws IllegalArgumentException if the size is invalid or cannot be
     * handled by the implementation.
     * @throws IOException conditionally, based on implementation
     */
    default void startArray(int size) throws IOException {}

    /**
     * End a MapValue.
     *
     * @param size the number of entries in the map or -1 if not known.
     * @throws IOException conditionally, based on implementation
     */
    default void endMap(int size) throws IOException {}

    /**
     * End an ArrayValue.
     *
     * @param size the number of entries in the array or -1 if not known.
     * @throws IOException conditionally, based on implementation
     */
    default void endArray(int size) throws IOException {}

    /**
     * Start a field in a map.
     *
     * @param key the key of the field.
     * @return true if the map field should be skipped entirely, true
     * otherwise
     * @throws IOException conditionally, based on implementation
     */
    default boolean startMapField(String key) throws IOException {
        return false;
    }

    /**
     * End a field in a map.
     * @param key the key of the field.
     * @throws IOException conditionally, based on implementation
     */
    default void endMapField(String key) throws IOException {}

    /**
     * Start a field in an array. This can be ignored by most handlers but
     * they can return true to skip generation for the field.
     * @param index the array index
     * @return true if the element should be skipped
     * @throws IOException conditionally, based on implementation
     */
    default boolean startArrayField(int index) throws IOException {
        return false;
    }

    /**
     * End a field in an array. There is no corresponding start for
     * array entries. This allows, for example, JSON serializers to insert
     * array value separators without the complexity of tracking the
     * entire event sequence.
     * @param index the index of the field
     * @throws IOException conditionally, based on implementation
     */
    default void endArrayField(int index) throws IOException {}

    /**
     * A boolean value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    default void booleanValue(boolean value) throws IOException {}

    /**
     * A binary value
     *
     * @param byteArray the byte[] value
     * @throws IOException conditionally, based on implementation
     */
    default void binaryValue(byte[] byteArray) throws IOException {}

    /**
     * A binary value with offset and length
     *
     * @param byteArray the byte[] value
     * @param offset the offset to start at
     * @param length number of bytes total, starting at offset
     * @throws IOException conditionally, based on implementation
     */
    default void binaryValue(byte[] byteArray,
                     int offset,
                     int length) throws IOException {}

    /**
     * A String value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    default void stringValue(String value) throws IOException {}

    /**
     * An integer value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    default void integerValue(int value) throws IOException {}

    /**
     * A long value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    default void longValue(long value) throws IOException {}

    /**
     * A double value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    default void doubleValue(double value) throws IOException {}

    /**
     * A Number value.
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    default void numberValue(BigDecimal value) throws IOException {}

    /**
     * A Timestamp value
     *
     * @param timestamp the value
     * @throws IOException conditionally, based on implementation
     */
    default void timestampValue(TimestampValue timestamp) throws IOException {}

    /**
     * A JsonNullValue
     * @throws IOException conditionally, based on implementation
     */
    default void jsonNullValue() throws IOException {}

    /**
     * A NullValue
     * @throws IOException conditionally, based on implementation
     */
    default void nullValue() throws IOException {}

    /**
     * An EmptyValue
     * @throws IOException conditionally, based on implementation
     */
    default void emptyValue() throws IOException {}

    /**
     * Returns true if event generation should stop. Event generators need
     * to check this between operations that consume input streams so that,
     * for example, navigation through an NSON stream can stop at a specific
     * location. See {@link PathFinder#seek}
     *
     * @return true if the event generation should stop
     */
    default boolean stop() {
        return false;
    }

    /**
     * Generates events from a {@link FieldValue} instance sending them to
     * the {@link FieldValueEventHandler} provided.
     *
     * @param value the FieldValue used to generate events
     * @param handler the handler to use
     * @throws IOException conditionally, based on implementation of handler
     */
    public static void generate(FieldValue value,
                                FieldValueEventHandler handler)
    throws IOException {
        generate(value, handler, false);
    }

    /**
     * Generates events from a {@link FieldValue} instance sending them to
     * the {@link FieldValueEventHandler} provided.
     *
     * @param value the FieldValue used to generate events
     * @param handler the handler to use
     * @param skip if true skip the field
     * @throws IOException conditionally, based on implementation of handler
     */
    public static void generate(FieldValue value,
                                FieldValueEventHandler handler,
                                boolean skip)
    throws IOException {

        FieldValue.Type type = value.getType();
        switch (type) {
            case ARRAY:
                generateForArray(value.asArray(), handler, skip);
                break;
            case BINARY:
                if (!skip) {
                    handler.binaryValue(value.getBinary());
                }
                break;
            case BOOLEAN:
                if (!skip) {
                    handler.booleanValue(value.getBoolean());
                }
                break;
            case DOUBLE:
                if (!skip) {
                    handler.doubleValue(value.getDouble());
                }
                break;
            case INTEGER:
                if (!skip) {
                    handler.integerValue(value.getInt());
                }
                break;
            case LONG:
                if (!skip) {
                    handler.longValue(value.getLong());
                }
                break;
            case MAP:
                generateForMap(value.asMap(), handler, skip);
                break;
            case STRING:
                if (!skip) {
                    handler.stringValue(value.asString().getString());
                }
                break;
            case TIMESTAMP:
                if (!skip) {
                    handler.timestampValue(value.asTimestamp());
                }
                break;
            case NUMBER:
                if (!skip) {
                    handler.numberValue(value.asNumber().getValue());
                }
                break;
            case JSON_NULL:
                if (!skip) {
                    handler.jsonNullValue();
                }
                break;
            case NULL:
                if (!skip) {
                    handler.nullValue();
                }
                break;
            case EMPTY:
                if (!skip) {
                    handler.emptyValue();
                }
                break;
            default:
                throw new IllegalStateException(
                    "FieldValueEventHandler, unknown type " + type);
        }
    }

    /**
     * Generates events for {@link MapValue} sending them to the specified
     * {@link FieldValueEventHandler}.
     *
     * @param map the MapValue to use
     * @param handler the handler to use
     * @param skip if true skip the field
     * @throws IOException conditionally, based on implementation of handler
     */
    public static void generateForMap(MapValue map,
                                      FieldValueEventHandler handler,
                                      boolean skip)
    throws IOException {


        /*
         * This code is generating events from a tree of objects which
         * means that skipping can just navigate past what is being skipped.
         * This is different from skipping fields when reading the
         * serialized format
         */
        if (skip) {
            return;
        }
        handler.startMap(map.size());
        for (Map.Entry<String, FieldValue> entry : map.entrySet()) {
            skip = handler.startMapField(entry.getKey());
            if (handler.stop()) {
                return;
            }
            if (!skip) {
                generate(entry.getValue(), handler, skip);
                if (handler.stop()) {
                    return;
                }
            }
            /* make start/end calls symmetrical */
            handler.endMapField(entry.getKey());
            if (handler.stop()) {
                return;
            }
        }
        handler.endMap(map.size());
    }

    /**
     * Generates events for {@link ArrayValue} sending them to the specified
     * {@link FieldValueEventHandler}.
     *
     * @param array the ArrayValue to use
     * @param handler the handler to use
     * @param skip if true skip the field
     * @throws IOException conditionally, based on implementation of handler
     */
    public static void generateForArray(ArrayValue array,
                                        FieldValueEventHandler handler,
                                        boolean skip)
    throws IOException {

        /*
         * This code is generating events from a tree of objects which
         * means that skipping can just navigate past what is being skipped.
         * This is different from skipping fields when reading the
         * serialized format
         */
        if (skip) {
            return;
        }
        handler.startArray(array.size());
        int index = 0;
        for (FieldValue value : array) {
            skip = handler.startArrayField(index);
            generate(value, handler, skip);
            if (handler.stop()) {
                return;
            }
            handler.endArrayField(index++);
            if (handler.stop()) {
                return;
            }
        }
        handler.endArray(array.size());
    }
}
