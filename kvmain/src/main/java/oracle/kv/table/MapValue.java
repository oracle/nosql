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

package oracle.kv.table;

import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

/**
 * MapValue extends {@link FieldValue} to define a container object that holds
 * a map of FieldValue objects all of the same type.  The getters and setters
 * use the same semantics as Java Map. Map keys are case-sensitive.
 *
 * @since 3.0
 */
public interface MapValue extends FieldValue {

    /**
     * A constant used as a key for a map value when the value is used as part
     * of an IndexKey when there is an index on the value of a map's element or
     * a nested value within the element if the element is a record.
     */
    static final String ANONYMOUS = "[]";

   /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public MapValue clone();

    /**
     * Returns a String representation of the value.  The value is returned
     * is a JSON string, and is the same as that returned by
     * {@link FieldValue#toJsonString}.
     *
     * @return a String representation of the value
     */
    @Override
    public String toString();

    /**
     * Returns the MapDef that defines the content of this map.
     *
     * @return the MapDef
     */
    @Override
    MapDef getDefinition();

    /**
     * Returns the size of the map.
     *
     * @return the size
     */
    int size();

    /**
     * Returns an unmodifiable view of the MapValue state.  The type of all
     * fields is the same and is defined by the {@link MapDef} returned by
     * {@link #getDefinition}.
     *
     * @return the map
     *
     * @since 3.0.6
     */
    Map<String, FieldValue> getFields();

    /**
     * Remove the named field if it exists.
     *
     * @param fieldName the name of the field to remove
     *
     * @return the FieldValue if it existed, otherwise null
     */
    FieldValue remove(String fieldName);

    /**
     * Returns the FieldValue with the specified name if it
     * appears in the map.
     *
     * @param fieldName the name of the desired field.
     *
     * @return the value for the field or null if the name does not exist in
     * the map.
     */
    FieldValue get(String fieldName);

    /**
     * Puts a JSON null value in the named field, silently overwriting
     * existing values. This operation is only valid for maps of type JSON.
     *
     * @param fieldName name of the desired field
     *
     * @return this
     *
     * @throws IllegalArgumentException if this map is not of type JSON
     *
     * @since 4.3
     */
    MapValue putJsonNull(String fieldName);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue put(String fieldName, int value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue put(String fieldName, long value);

    /**
     * Set the named field.  Any existing entry is silently overwritten. This
     * method is used to put a string into a map of type String. The String
     * value is not parsed or interpreted. The methods {@link #putJson} and
     * {@link #putEnum} exist to put String values of those types.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue put(String fieldName, String value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue put(String fieldName, double value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     *
     * @since 4.4
     */
    MapValue putNumber(String fieldName, int value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     *
     * @since 4.4
     */
    MapValue putNumber(String fieldName, long value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     *
     * @since 4.4
     */
    MapValue putNumber(String fieldName, float value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     *
     * @since 4.4
     */
    MapValue putNumber(String fieldName, double value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     *
     * @since 4.4
     */
    MapValue putNumber(String fieldName, BigDecimal value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue put(String fieldName, float value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue put(String fieldName, boolean value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue put(String fieldName, byte[] value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue putFixed(String fieldName, byte[] value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue putEnum(String fieldName, String value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     *
     * @since 4.3
     */
    MapValue put(String fieldName, Timestamp value);

    /**
     * Set the named field.  Any existing entry is silently overwritten.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the definition of the map type does
     * not match the input type
     */
    MapValue put(String fieldName, FieldValue value);

    /**
     * Puts a Record into the map.  Existing values are silently overwritten.
     *
     * @param fieldName the field to use for the map key
     *
     * @return an uninitialized RecordValue that matches the type
     * definition for the map
     *
     * @throws IllegalArgumentException if the definition of the map type
     * is not a RecordDef
     */
    RecordValue putRecord(String fieldName);

    /**
     * Puts a Map into the map.  Existing values are silently overwritten.
     *
     * @param fieldName the field to use for the map key
     *
     * @return an uninitialized MapValue that matches the type
     * definition for the map
     *
     * @throws IllegalArgumentException if the definition of the map type
     * is not a MapDef
     */
    MapValue putMap(String fieldName);

    /**
     * Puts an Array into the map.  Existing values are silently overwritten.
     *
     * @param fieldName the field to use for the map key
     *
     * @return an uninitialized ArrayValue that matches the type
     * definition for the map
     *
     * @throws IllegalArgumentException if the definition of the map type
     * is not an ArrayDef
     */
    ArrayValue putArray(String fieldName);

    /**
     * Puts arbitrary JSON into this map in the specified field. Existing
     * values are silently overwritten.
     *
     * @param fieldName the field to use for the map key
     *
     * @param jsonInput a JSON string
     *
     * @return this
     *
     * @throws IllegalArgumentException if this map is not of type JSON or
     * the JSON input is invalid.
     *
     * @since 4.2
     */
    MapValue putJson(String fieldName,
                     String jsonInput);

    /**
     * Puts arbitrary JSON into this map in the specified field. Existing
     * values are silently overwritten.
     *
     * @param fieldName the field to use for the map key
     *
     * @param jsonReader a Reader
     *
     * @return this
     *
     * @throws IllegalArgumentException if this map is not of type JSON or
     * the JSON input is invalid.
     *
     * @since 4.2
     */
    MapValue putJson(String fieldName,
                     Reader jsonReader);
}
