/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import static oracle.nosql.nson.util.CheckNull.requireNonNull;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import oracle.nosql.nson.util.SizeOf;

/**
 * MapValue represents a row in a NoSQL Database table. A top-level row is
 * always a MapValue instance that contains {@link FieldValue} objects which
 * may be atomic types or embedded MapValue or {@link ArrayValue} instances,
 * creating a structured row.
 * <p>
 * MapValue is also used to represent key values used in get operations as well
 * as nested maps or records within a row.
 * <p>
 * Field names in a MapValue follow the same rules as Java {@link Map},
 * meaning that they are case-sensitive string values with no duplicates.
 * On input MapValues of any structure can be created, but when put into a
 * table they must conform to the schema of the target table or an exception
 * will be thrown. Note that in the context of a RECORD field in a table
 * schema field names are treated as case-insensitive. If a MapValue
 * represents JSON, field names are case-sensitive.
 * </p>
 * MapValue does not support put of Java null values. If a "null" JSON value is
 * desired it is possible to put a {@link NullValue} instance using
 * "put(fieldName, NullValue.getInstance())"
 * <p>
 * When a MapValue is received on output the value will always conform to
 * the schema of the table from which the value was received or the implied
 * schema of a query projection.
 * </p>
 */
public class MapValue extends FieldValue
    implements Iterable<Map.Entry<String, FieldValue>> {

    private final Map<String, FieldValue> values;

    /**
     * @hidden
     * Internal, test-use only. Only used by hidden scan methods
     *
     * Special case EMPTY_VALUE
     */
    public static final MapValue EMPTY_VALUE = new MapValue();

    /**
     * Creates an empty MapValue instance
     */
    public MapValue() {
        super();
        values = new HashMap<String, FieldValue>();
    }

    /**
     * Creates an empty MapValue instance
     *
     * @param keepInsertionOrder if true a map is created that maintains
     * insertion order. This is the default for get and query results.
     *
     * @param size the initial capacity of the map
     */
    public MapValue(boolean keepInsertionOrder, int size) {
        super();
        if (keepInsertionOrder) {
            values = new LinkedHashMap<String, FieldValue>(size);
        } else {
            values = new HashMap<String, FieldValue>(size);
        }
    }

    /**
     * Creates an empty MapValue instance using the specified initial capacity
     *
     * @param size the initial capacity
     */
    public MapValue(int size) {
        super();
        values = new HashMap<String, FieldValue>(size);
    }

    @Override
    public Type getType() {
        return Type.MAP;
    }

    /**
     * Returns a live {@link Map} of the MapValue state.
     *
     * @return the map
     */
    public Map<String, FieldValue> getMap() {
        return values;
    }

    /**
     * Returns a {@link Set} of entries based on the underlying map
     * that holds the values.
     *
     * @return the set
     */
    public Set<Map.Entry<String, FieldValue>> entrySet() {
        return values.entrySet();
    }

    /**
     * @hidden
     * Returns the set of keys in this map, sorted according to the
     * String.compareTo() method.
     *
     * @return the soerted set of keys
     */
    public SortedSet<String> sortedKeys() {
        return new TreeSet<String>(values.keySet());
    }

    /**
     * Returns an iterator over the entry set.
     * @return the iterator
     * @since 5.4
     */
    @Override
    public Iterator<Map.Entry<String, FieldValue>> iterator() {
        return entrySet().iterator();
    }

    /**
     * Returns the number of entries in the map.
     *
     * @return the size
     */
    public int size() {
        return values.size();
    }

    /**
     * Returns a {@link Collection} of {@link FieldValue} instances contained in
     * this map.
     *
     * @return the values
     */
    public Collection<FieldValue> values() {
        return values.values();
    }

    /**
     * Inserts all of the entries in the specified iterator into the map.
     *
     * @param iter the iterator
     *
     * @return this
     */
    public MapValue addAll(
        Iterator<Map.Entry<String, FieldValue>> iter) {
        requireNonNull(iter, "MapValue.addAll: iter must be non-null");

        while (iter.hasNext()) {
            Map.Entry<String, FieldValue> entry = iter.next();
            values.put(entry.getKey(), entry.getValue());
        }
        return this;
    }

    /**
     * Inserts all of the entries in the specified stream into the map.
     *
     * @param stream the stream
     *
     * @return this
     */
    public MapValue addAll(
        Stream<Map.Entry<String, FieldValue>> stream) {
        requireNonNull(stream, "MapValue.addAll: stream must be non-null");

        stream.forEach(e -> values.put(e.getKey(), e.getValue()));
        return this;
    }

    /**
     * Returns the type of the field with the specified name, or null
     * if the field does not exist.
     *
     * @param name the name of the field
     *
     * @return the type of the field, or null if it does not exist.
     */
    public FieldValue.Type getType(String name) {
        requireNonNull(name, "MapValue.getType: name must be non-null");
        FieldValue val = values.get(name);
        return (val == null ? null : val.getType());
    }

    /**
     * Returns the field the specified name, or null
     * if the field does not exist.
     *
     * @param name the name of the field
     *
     * @return the field, or null if it does not exist.
     */
    public FieldValue get(String name) {
        requireNonNull(name, "MapValue.get: name must be non-null");
        return values.get(name);
    }

    /**
     * Returns true if the specified field exists in the map
     *
     * @param name the name of the field
     *
     * @return true if the field exists, false if not
     * @since 5.4
     */
    public boolean contains(String name) {
        requireNonNull(name, "MapValue.contains: name must be non-null");
        return values.containsKey(name);
    }

    /**
     * Sets the named field. Any existing entry is silently overwritten.
     *
     * @param name the name of the field
     *
     * @param value the value to set
     *
     * @return this
     */
    public MapValue put(String name, FieldValue value) {
        requireNonNull(name, "MapValue.put: name must be non-null");
        requireNonNull(value, "MapValue.put: value must be non-null");
        validateName(name, value);
        values.put(name, value);
        return this;
    }

    /**
     * Sets the named field as an IntegerValue. Any existing entry is silently
     * overwritten.
     *
     * @param name the name of the field
     *
     * @param value the value to set
     *
     * @return this
     */
    public MapValue put(String name, int value) {
        return put(name, new IntegerValue(value));
    }

    /**
     * Sets the named field as a LongValue. Any existing entry is silently
     * overwritten.
     *
     * @param name the name of the field
     *
     * @param value the value to set
     *
     * @return this
     */
    public MapValue put(String name, long value) {
        return put(name, new LongValue(value));
    }

    /**
     * Sets the named field as a DoubleValue. Any existing entry is silently
     * overwritten.
     *
     * @param name the name of the field
     *
     * @param value the value to set
     *
     * @return this
     */
    public MapValue put(String name, double value) {
        return put(name, new DoubleValue(value));
    }

    /**
     * Sets the named field as a NumberValue. Any existing entry is silently
     * overwritten.
     *
     * @param name the name of the field
     *
     * @param value the value to set
     *
     * @return this
     */
    public MapValue put(String name, BigDecimal value) {
        requireNonNull(value, "MapValue.put: value must be non-null");
        return put(name, new NumberValue(value));
    }

    /**
     * Sets the named field as a StringValue. Any existing entry is silently
     * overwritten.
     *
     * @param name the name of the field
     *
     * @param value the value to set
     *
     * @return this
     */
    public MapValue put(String name, String value) {
        requireNonNull(value, "MapValue.put: value must be non-null");
        return put(name, new StringValue(value));
    }

    /**
     * Sets the named field as a BooleanValue. Any existing entry is silently
     * overwritten.
     *
     * @param name the name of the field
     *
     * @param value the value to set
     *
     * @return this
     */
    public MapValue put(String name, boolean value) {
        return put(name, BooleanValue.getInstance(value));
    }

    /**
     * Sets the named field as a BinaryValue. Any existing entry is silently
     * overwritten.
     *
     * @param name the name of the field
     *
     * @param value the value to set
     *
     * @return this
     */
    public MapValue put(String name, byte[] value) {
        requireNonNull(value, "MapValue.put: value must be non-null");
        return put(name, new BinaryValue(value));
    }

    /**
     * Sets the named field as a TimestampValue. Any existing entry is silently
     * overwritten.
     *
     * @param name the name of the field
     *
     * @param value the value to set
     *
     * @return this
     */
    public MapValue put(String name, Timestamp value) {
        requireNonNull(value, "MapValue.put: value must be non-null");
        return put(name, new TimestampValue(value));
    }

    /**
     * Sets the named field based on the JSON string provided. Any existing
     * entry is silently overridden. The type of the field created is inferred
     * from the JSON.
     *
     * @param name the name of the field
     *
     * @param jsonString a JSON formatted String
     *
     * @param options configurable options used to affect the JSON output
     * format of some data types. May be null.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the string is not valid JSON
     */
    public MapValue putFromJson(String name,
                                String jsonString,
                                JsonOptions options) {
        requireNonNull(jsonString,
                       "MapValue.putFromJson: JSON string must be non-null");
        return put(name, JsonUtils.createValueFromJson(jsonString, options));
    }

    /**
     * Removes the named field if it exists.
     *
     * @param name the name of the field
     *
     * @return the previous value if it existed, otherwise null
     */
    public FieldValue remove(String name) {
        return values.remove(name);
    }

    /**
     * Gets the named field as an integer
     *
     * @param name the name of the field
     *
     * @return the integer value
     *
     * @throws IllegalArgumentException if the field does not exist.
     *
     * @throws ClassCastException if the field cannot be cast to the
     * required type
     */
    public int getInt(String name) {
        FieldValue val = values.get(name);
        if (val == null) {
            throw new IllegalArgumentException("Field does not exist: " + name);
        }
        return val.getInt();
    }

    /**
     * Gets the named field as a long
     *
     * @param name the name of the field
     *
     * @return the long value
     *
     * @throws IllegalArgumentException if the field does not exist.
     *
     * @throws ClassCastException if the field cannot be cast to the
     * required type
     */
    public long getLong(String name) {
        FieldValue val = values.get(name);
        if (val == null) {
            throw new IllegalArgumentException("Field does not exist: " + name);
        }
        return val.getLong();
    }

    /**
     * Gets the named field as a double
     *
     * @param name the name of the field
     *
     * @return the double value
     *
     * @throws IllegalArgumentException if the field does not exist.
     *
     * @throws ClassCastException if the field cannot be cast to the
     * required type
     */
    public double getDouble(String name) {
        FieldValue val = values.get(name);
        if (val == null) {
            throw new IllegalArgumentException("Field does not exist: " + name);
        }
        return val.getDouble();
    }

    /**
     * Gets the named field as a BigDecimal
     *
     * @param name the name of the field
     *
     * @return the BigDecimal value
     *
     * @throws IllegalArgumentException if the field does not exist.
     *
     * @throws ClassCastException if the field cannot be cast to the
     * required type
     */
    public BigDecimal getNumber(String name) {
        FieldValue val = values.get(name);
        if (val == null) {
            throw new IllegalArgumentException("Field does not exist: " + name);
        }
        return val.getNumber();
    }

    /**
     * Gets the named field as a String
     *
     * @param name the name of the field
     *
     * @return the String value
     *
     * @throws IllegalArgumentException if the field does not exist.
     * @throws ClassCastException if the field cannot be cast to the
     * required type
     */
    public String getString(String name) {
        FieldValue val = values.get(name);
        if (val == null) {
            throw new IllegalArgumentException("Field does not exist: " + name);
        }
        return val.getString();
    }

    /**
     * Gets the named field as a boolean
     *
     * @param name the name of the field
     *
     * @return the boolean value
     *
     * @throws IllegalArgumentException if the field does not exist.
     *
     * @throws ClassCastException if the field cannot be cast to the
     * required type
     */
    public boolean getBoolean(String name) {
        FieldValue val = values.get(name);
        if (val == null) {
            throw new IllegalArgumentException("Field does not exist: " + name);
        }
        return val.getBoolean();
    }

    /**
     * Gets the named field as a binary value
     *
     * @param name the name of the field
     *
     * @return the binary value
     *
     * @throws IllegalArgumentException if the field does not exist.
     *
     * @throws ClassCastException if the field cannot be cast to the
     * required type
     */
    public byte[] getBinary(String name) {
        FieldValue val = values.get(name);
        if (val == null) {
            throw new IllegalArgumentException("Field does not exist: " + name);
        }
        return val.getBinary();
    }

    /**
     * Gets the named field as a long timestamp representing the milliseconds since
     * January 1, 1970
     *
     * @param name the name of the field
     *
     * @return the timestamp value
     *
     * @throws IllegalArgumentException if the field does not exist.
     *
     * @throws ClassCastException if the field cannot be cast to the
     * required type
     */
    public Timestamp getTimestamp(String name) {
        FieldValue val = values.get(name);
        if (val == null) {
            throw new IllegalArgumentException("Field does not exist: " + name);
        }
        return val.getTimestamp();
    }

    @Override
    public int compareTo(FieldValue other) {

        MapValue otherImpl = other.asMap();

        Iterator<String> keyIter = values.keySet().iterator();

        while (keyIter.hasNext()) {
            String key = keyIter.next();
            FieldValue otherVal = otherImpl.get(key);
            if (otherVal == null) {
                return 1;
            }

            /*
             * Key found, compare values
             */
            FieldValue val = values.get(key);
            try {
                int valCompare = val.compareTo(otherVal);
                if (valCompare != 0) {
                    return valCompare;
                }
            } catch (ClassCastException cce) {
                throw new ClassCastException(cce.getMessage() +
                    ", the key of values: " + key);
            }
        }

        /*
         * The other object with more keys is greater, otherwise they are equal.
         */
        return (size() < otherImpl.size()) ? -1 : 0;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof MapValue) {
            return values.equals(((MapValue)other).values);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        long size = (SizeOf.OBJECT_OVERHEAD +
                     SizeOf.OBJECT_REF_OVERHEAD +
                     SizeOf.HASHMAP_OVERHEAD);
        for (Map.Entry<String, FieldValue> entry : values.entrySet()) {
            size += SizeOf.HASHMAP_ENTRY_OVERHEAD;
            size += SizeOf.stringSize(entry.getKey());
            size += entry.getValue().sizeof();
        }
        return size;
    }

    /**
     * @hidden
     * Called from a sorting ReceiveIter
     */
    public void convertEmptyToNull() {

        for (Map.Entry<String, FieldValue> entry : values.entrySet()) {
            FieldValue val = entry.getValue();
            if (val == EmptyValue.getInstance()) {
                entry.setValue(NullValue.getInstance());
            }
        }
    }
}
