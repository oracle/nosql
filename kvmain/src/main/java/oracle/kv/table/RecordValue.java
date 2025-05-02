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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;


/**
 * RecordValue extends {@link FieldValue} to represent a record value. A record
 * value is an ordered collection of key-value pairs called "fields". The keys
 * are called "field names"; they are all strings and must be unique within each
 * record. The associated values, called "field values", do not need to have the
 * same type. Each record is associated with, and conforms to, a record type
 * definition (see {@link RecordDef}). The record type definition specifies the
 * number, names, and ordering of the fields in all conforming records, as well
 * as the type of each field value. The field order is the order in which the
 * fields appear in record type declaration. Based on this order, each field
 * has a unique position, which is a number between 0 and N - 1, where N is
 * the number of fields.
 *
 * Note: during its construction, a record may have fewer fields than what its
 * associated type dictates. Fields can be added to a record via the various
 * put() methods defined by the RecordValue interface.
 *
 * @since 3.0
 */
public interface RecordValue extends FieldValue {

    /**
     * Returns the record type that this record conforms to.
     *
     * @return the RecordDef
     */
    @Override
    RecordDef getDefinition();

    /**
     * Returns the list of all field names that are defined by the associated
     * record type. The list is in field declaration order. Notice that this
     * list does not depend on the actual fields present in this record, and
     * is never empty. Values of the fields, if they are present in this
     * instance, can be obtained using {@link #get}.
     *
     * @return an unmodifiable list of the field names in declaration order
     *
     * @since 3.0.6
     *
     * @deprecated as of 4.2 Please use the equivalent getFieldNames() method.
     */
    @Deprecated
    List<String> getFields();

    /**
     * Returns the list of all field names that are defined by the associated
     * record type. The list is in field declaration order. Notice that this
     * list does not depend on the actual fields present in this record, and
     * is never empty. Values of the fields, if they are present in this
     * instance, can be obtained using {@link #get}.
     * <p>
     * If this RecordValue is an instance of a Row from a JSON collection
     * table this method will return all of the fields in the row, not just
     * the primary keys defined in the schema.
     *
     * @return an unmodifiable list of the field names in declaration order
     *
     * @since 4.2
     */
    List<String> getFieldNames();

    /**
     * Returns the name of the field at the given position
     *
     * @param position the position of the field whose name is to be returned
     *
     * @return the name of the field at the given position
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @since 4.2
     */
    String getFieldName(int position);

    /**
     * Returns the position of the field with the given name
     *
     * @return the position of the field with the given name
     *
     * @throws IllegalArgumentException if the associated record type does
     * not have any field with the given name
     *
     * @since 4.2
     */
    int getFieldPos(String fieldName);

    /**
     * Returns a deep copy of this object.
     *
     * @return a deep copy of this object
     */
    @Override
    public RecordValue clone();


    /**
     * Deletes all the fields from this record.
     *
     * @since 4.1
     */
    public void clear();

    /**
     * Returns a String representation of this record. The value returned
     * is a JSON string, and is the same as that returned by
     * {@link FieldValue#toJsonString}.
     *
     * @return a String representation of the value
     */
    @Override
    public String toString();

    /**
     * Returns the value of the field with the given name.
     *
     * @param fieldName the name of the desired field
     *
     * @return the value of the specified field, if the field exists in this
     * record, or null if it has not been added yet.
     *
     * @throws IllegalArgumentException if the associated record type does
     * not have any field with the given name
     */
    FieldValue get(String fieldName);

    /**
     * Returns the value of the field at the given position.
     *
     * @param position the position of the desired field.
     *
     * @return the field value if it is available, null if it has not
     * been set
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @since 4.0
     */
    FieldValue get(int position);

    /**
     * Returns the number of the fields in this record. This is the number of
     * fields that have actually been set, and is always less or equal to the
     * number of fields in the associated record definition.
     *
     * @return the number of fields that have been set.
     */
    int size();

    /**
     * Returns true if none of the record's fields have been set, false otherwise.
     *
     * @return true if none of the record's fields have been set, false otherwise.
     */
    boolean isEmpty();

    /**
     * Copies the fields from another RecordValue instance, overwriting fields
     * in this object with the same name. If a field exists in this record but
     * not in the other record, the existing field is not removed.
     *
     * @param source the source RecordValue from which to copy
     *
     * @throws IllegalArgumentException if the {@link RecordDef} of source
     * does not match that of this instance.
     */
    void copyFrom(RecordValue source);

    /**
     * Returns true if the record contains the named field.
     *
     * @param fieldName the name of the field
     *
     * @return true if the field exists in the record, otherwise null
     *
     * @throws IllegalArgumentException if the associated record type does
     * not have any field with the given name
     */
    boolean contains(String fieldName);

    /**
     * Returns true if the record contains a field at the given position.
     *
     * @param position the position of the desired field.
     *
     * @return true if the field exists in the record, otherwise null
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @since 4.2
     */
    boolean contains(int position);

    /**
     * Remove the named field if it exists.
     *
     * @param fieldName the name of the field to remove
     *
     * @return the FieldValue if the field existed (was set) in the record,
     * otherwise null
     *
     * @throws IllegalArgumentException if the associated record type does
     * not have any field with the given name
     */
    FieldValue remove(String fieldName);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.1
     */
    RecordValue put(int position, FieldValue value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.2
     */
    RecordValue put(int position, int value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.2
     */
    RecordValue put(int position, long value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.  This method is used to put a string into a field
     * of type String. The String value is not parsed or interpreted. The
     * methods {@link #putJson} and {@link #putEnum} exist to put String values
     * of those types.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.2
     */
    RecordValue put(int position, String value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.2
     */
    RecordValue put(int position, double value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.2
     */
    RecordValue put(int position, float value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.2
     */
    RecordValue put(int position, boolean value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.2
     */
    RecordValue put(int position, byte[] value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.2
     */
    RecordValue putFixed(int position, byte[] value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.2
     */
    RecordValue putEnum(int position, String value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.4
     */
    RecordValue putNumber(int position, int value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.4
     */
    RecordValue putNumber(int position, long value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.4
     */
    RecordValue putNumber(int position, float value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.4
     */
    RecordValue putNumber(int position, double value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.4
     */
    RecordValue putNumber(int position, BigDecimal value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already.
     *
     * @param position the position of the field.
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the type of the specified field does
     * not match that of the value.
     *
     * @since 4.3
     */
    RecordValue put(int position, Timestamp value);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The new value of the field will be the special
     * NULL value.
     *
     * @param position the position of the field.
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not nullable.
     *
     * @since 4.2
     */
    RecordValue putNull(int position);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The new value of the field will be the special
     * JSON NULL value that returns true for {@link FieldValue#isJsonNull}.
     * The field must be of type JSON.
     *
     * @param position the position of the field.
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not of type
     * JSON
     *
     * @since 4.3
     */
    RecordValue putJsonNull(int position);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be a record-valued
     * field, and the value assigned to it by this method is an empty record
     * that conforms to the record type declared for this field.
     *
     * @param position the position of the field.
     *
     * @return the empty record assigned to the field.
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * record-valued.
     *
     * @since 4.2
     */
    RecordValue putRecord(int position);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be a record-valued field,
     * and the value assigned to it by this method is a new record that conforms
     * to the record type declared for this field, and whose content is taken
     * from the given java Map object..
     *
     * @param position the position of the field.
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * record-valued or if the map value doesn't match the record type of the
     * field.
     *
     * @since 4.2
     */
    RecordValue putRecord(int position, Map<String, ?> map);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be a record-valued field,
     * and the value assigned to it by this method is a new record created and
     * populated form a given JSON string. If the {@code exact} parameter is
     * true, the input JSON must contain an exact match to the record type of
     * the specified field, including all fields. It must not have additional
     * data. If false, only matching fields will be added and the input may
     * have additional, unrelated data.
     *
     * @param position the position of the field.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * record-value or if the JSON doesn't match the record type of the
     * field, according to the requirements set by the {@code exact} parameter.
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     *
     * @since 4.2
     */
    RecordValue putRecordAsJson(int position,
                                String jsonInput,
                                boolean exact);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be a record-valued field,
     * and the value assigned to it by this method is a new record created and
     * populated form a given JSON stream. If the {@code exact} parameter is
     * true, the input JSON must contain an exact match to the record type of
     * the specified field, including all fields. It must not have additional
     * data. If false, only matching fields will be added and the input may
     * have additional, unrelated data.
     *
     * @param position the position of the field.
     *
     * @param jsonInput a JSON stream
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * record-value or if the JSON doesn't match the record type of the
     * field, according to the requirements set by the {@code exact} parameter.
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     *
     * @since 4.2
     */
    RecordValue putRecordAsJson(int position,
                                InputStream jsonInput,
                                boolean exact);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be an array-valued
     * field, and the value assigned to it by this method is an empty array
     * that conforms to the array type declared for this field.
     *
     * @param position the position of the field.
     *
     * @return the empty array assigned to the field.
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * array-valued.
     *
     * @since 4.2
     */
    ArrayValue putArray(int position);

    /**
     * Inserts a list of Java objects into the record as an array at the given
     * position, or updates its value if the field exists already. The
     * specified field must be an array-valued field, and the value assigned to
     * it by this method is a new array that conforms to the array type
     * declared for this field, and whose content is taken from the given java
     * Iterable object.
     *
     * @param position the position of the field.
     *
     * @param list the Iterable list of objects to insert.
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * record-valued or if the type of the list doesn't match the array type of
     * the array field.
     *
     * @since 4.2
     */
    RecordValue putArray(int position, Iterable<?> list);

    /**
     * Inserts an array of Java objects into the record as an array at the
     * given given position, or updates its value if the field exists
     * already. The specified field must be an array-valued field, and the
     * value assigned to it by this method is a new array that conforms to the
     * array type declared for this field, and whose content is taken from the
     * array of java objects.
     *
     * @param position the position of the field.
     *
     * @param array the array of objects to insert.
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the field at position is not an array
     * or the type of the objects in the array does not match the type of the
     * array field.
     *
     * @since 4.2
     */
    RecordValue putArray(int position, Object[] array);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be an array-valued field,
     * and the value assigned to it by this method is a new array created and
     * populated form a given JSON string. If the {@code exact} parameter is
     * true, the input JSON must contain an exact match to the array type of
     * the specified field, including all fields. It must not have additional
     * data. If false, only matching fields will be added and the input may
     * have additional, unrelated data.
     *
     * @param position the position of the field.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * array-value or if the JSON doesn't match the array type of the
     * field, according to the requirements set by the {@code exact} parameter.
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     *
     * @since 4.2
     */
    RecordValue putArrayAsJson(int position,
                               String jsonInput,
                               boolean exact);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be an array-valued field,
     * and the value assigned to it by this method is a new array created and
     * populated form a given JSON stream. If the {@code exact} parameter is
     * true, the input JSON must contain an exact match to the array type of
     * the specified field, including all fields. It must not have additional
     * data. If false, only matching fields will be added and the input may
     * have additional, unrelated data.
     *
     * @param position the positionition of the field.
     *
     * @param jsonInput a JSON stream
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * array-value or if the JSON doesn't match the array type of the
     * field, according to the requirements set by the {@code exact} parameter.
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     *
     * @since 4.2
     */
    RecordValue putArrayAsJson(int position,
                               InputStream jsonInput,
                               boolean exact);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be a map-valued
     * field, and the value assigned to it by this method is an empty map
     * that conforms to the array type declared for this field.
     *
     * @param position the position of the field.
     *
     * @return the empty map assigned to the field.
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * map-valued.
     *
     * @since 4.2
     */
    MapValue putMap(int position);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be a map-valued field,
     * and the value assigned to it by this method is a new map that conforms
     * to the map type declared for this field, and whose content is taken
     * from the given java map.
     *
     * @param position the position of the field.
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * map-valued or if the given map doesn't match the array type of the
     * field.
     *
     * @since 4.2
     */
    RecordValue putMap(int position, Map<String, ?> map);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be a map-valued field,
     * and the value assigned to it by this method is a new map created and
     * populated form a given JSON string. If the {@code exact} parameter is
     * true, the input JSON must contain an exact match to the map type of
     * the specified field, including all fields. It must not have additional
     * data. If false, only matching fields will be added and the input may
     * have additional, unrelated data.
     *
     * @param position the position of the field.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * map-value or if the JSON doesn't match the map type of the
     * field, according to the requirements set by the {@code exact} parameter.
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     *
     * @since 4.2
     */
    RecordValue putMapAsJson(int position,
                             String jsonInput,
                             boolean exact);

    /**
     * Inserts the field at the given position, or updates its value if the
     * field exists already. The specified field must be a map-valued field,
     * and the value assigned to it by this method is a new map created and
     * populated form a given JSON stream. If the {@code exact} parameter is
     * true, the input JSON must contain an exact match to the map type of
     * the specified field, including all fields. It must not have additional
     * data. If false, only matching fields will be added and the input may
     * have additional, unrelated data.
     *
     * @param position the position of the field.
     *
     * @param jsonInput a JSON stream
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the specified field is not
     * map-value or if the JSON doesn't match the map type of the
     * field, according to the requirements set by the {@code exact} parameter.
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     *
     * @since 4.2
     */
    RecordValue putMapAsJson(int position,
                             InputStream jsonInput,
                             boolean exact);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, int value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, long value);

    /**
     * Set the named field, silently overwriting existing values. This method is
     * used to put a string into a field of type String. The String value is not
     * parsed or interpreted. The methods {@link #putJson} and {@link #putEnum}
     * exist to put String values of those types.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, String value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, double value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, float value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, boolean value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     *
     * @since 4.4
     */
    RecordValue putNumber(String fieldName, int value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     *
     * @since 4.4
     */
    RecordValue putNumber(String fieldName, long value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     *
     * @since 4.4
     */
    RecordValue putNumber(String fieldName, float value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     *
     * @since 4.4
     */
    RecordValue putNumber(String fieldName, double value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     *
     * @since 4.4
     */
    RecordValue putNumber(String fieldName, BigDecimal value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, byte[] value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue putFixed(String fieldName, byte[] value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type.
     */
    RecordValue putEnum(String fieldName, String value);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the date value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     *
     * @since 4.3
     */
    RecordValue put(String fieldName, Timestamp value);

    /**
     * Put a null value in the named field, silently overwriting
     * existing values.
     *
     * @param fieldName name of the desired field
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue putNull(String fieldName);

    /**
     * Puts a JSON NULL value in the named field, silently overwriting
     * existing values. JSON NULL is a special value that returns
     * true for {@link FieldValue#isJsonNull}.
     * The field must be of type JSON.
     *
     * @param fieldName name of the desired field
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field is not JSON.
     *
     * @since 4.3
     */
    RecordValue putJsonNull(String fieldName);

    /**
     * Set the named field, silently overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param value the value to set
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the type of the field does not match the
     * input type
     */
    RecordValue put(String fieldName, FieldValue value);

    /**
     * Set a RecordValue field, silently overwriting existing values.
     * The returned object is empty of fields and must be further set by the
     * caller.
     *
     * @param fieldName name of the desired field
     *
     * @return an empty instance of RecordValue
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition.
     */
    RecordValue putRecord(String fieldName);

    /**
     * Set a RecordValue field based on map input, silently overwriting
     * existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param map to create value of the desired RecordValue field
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     *
     * @throws IllegalArgumentException if the map value type doesn't match the
     * field definition of the named field.
     */
    RecordValue putRecord(String fieldName, Map<String, ?> map);

    /**
     * Set a RecordValue field, silently overwriting existing values.
     * The created RecordValue is based on JSON string input. If the
     * {@code exact} parameter is true, the input string must contain an exact
     * match to the Record field definition, including all fields. It must not
     * have additional data. If false, only matching fields will be added and
     * the input may have additional, unrelated data.
     *
     * @param fieldName name of the desired field. In this method the field
     * must have a precise type and not be of type JSON.
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     */
    RecordValue putRecordAsJson(String fieldName,
                                String jsonInput,
                                boolean exact);

    /**
     * Set a RecordValue field, silently overwriting existing values.
     * The created RecordValue is based on JSON stream input. If the
     * {@code exact} parameter is true, the input string must contain an exact
     * match to the Record field definition, including all fields. It must not
     * have additional data. If false, only matching fields will be added and
     * the input may have additional, unrelated data.
     *
     * @param fieldName name of the desired field
     *
     * @param jsonInput a JSON input stream
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition, or is of type JSON.
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     */
    RecordValue putRecordAsJson(String fieldName,
                                InputStream jsonInput,
                                boolean exact);

    /**
     * Set a RecordValue field of type JSON using the JSON input.
     * The named field must be of type JSON.
     *
     * @param fieldName name of the desired field. The definition of this
     * field must be of type JSON.
     *
     * @param jsonInput a JSON string
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or its definition is not of type JSON.
     *
     * @throws IllegalArgumentException if the JSON input is invalid or
     * contains an unsupported construct.
     *
     * @since 4.2
     */
    RecordValue putJson(String fieldName,
                        String jsonInput);

    /**
     * Set a RecordValue field of type JSON using the JSON input.
     * The named field must be of type JSON.
     *
     * @param fieldName name of the desired field. The definition of this
     * field must be of type JSON.
     *
     * @param jsonReader a Reader
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or its definition is not of type JSON.
     *
     * @throws IllegalArgumentException if the JSON input is invalid or
     * contains an unsupported construct.
     *
     * @since 4.2
     */
    RecordValue putJson(String fieldName,
                        Reader jsonReader);

    /**
     * Set a RecordValue field of type JSON using the JSON input.
     * The named field must be of type JSON.
     *
     * @param position the position of the field.
     *
     * @param jsonInput a JSON string
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or its definition is not of type JSON.
     *
     * @throws IllegalArgumentException if the JSON input is invalid or
     * contains an unsupported construct.
     *
     * @since 4.2
     */
    RecordValue putJson(int position,
                        String jsonInput);

    /**
     * Set a RecordValue field of type JSON using the JSON input.
     * The named field must be of type JSON.
     *
     * @param position the position of the field.
     *
     * @param jsonReader a Reader
     *
     * @return this
     *
     * @throws IndexOutOfBoundsException if position is negative or greater or
     * equal to the number of fields in the record type definition associated
     * with this record. This situation will happen if this object is a
     * JSON collection row and the position is beyond the range of the
     * primary key fields.
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or its definition is not of type JSON.
     *
     * @throws IllegalArgumentException if the JSON input is invalid or
     * contains an unsupported construct.
     *
     * @since 4.2
     */
    RecordValue putJson(int position,
                        Reader jsonReader);

    /**
     * Set an ArrayValue field, silently overwriting existing values.
     * The returned object is empty of fields and must be further set by the
     * caller.
     *
     * @param fieldName name of the desired field
     *
     * @return an empty instance of ArrayValue
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     */
    ArrayValue putArray(String fieldName);

    /**
     * Set an ArrayValue field based on list input, silently overwriting
     * existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param list to create value of the desired ArrayValue field
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     *
     * @throws IllegalArgumentException if the list type doesn't match the
     * field definition of the named field.
     */
    RecordValue putArray(String fieldName, Iterable<?> list);

    /**
     * Set an ArrayValue field based on an array of Java Objects, silently
     * overwriting existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param array to insert into the ArrayValue field
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field is not an
     * array
     *
     * @throws IllegalArgumentException if the type of the objects in the
     * array does not match the type of the array field.
     */
    RecordValue putArray(String fieldName, Object[] array);

    /**
     * Set a ArrayValue field, silently overwriting existing values.
     * The created ArrayValue is based on JSON string input. If the
     * {@code exact} parameter is true, the input string must contain an exact
     * match to all the nested Record definition in Array field, including all
     * fields. It must not have additional data. If false, only matching fields
     * will be added and the input may have additional, unrelated data.
     *
     * @param fieldName name of the desired field
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     */
    RecordValue putArrayAsJson(String fieldName,
                               String jsonInput,
                               boolean exact);

    /**
     * Set a ArrayValue field, silently overwriting existing values.
     * The created ArrayValue is based on JSON stream input. If the
     * {@code exact} parameter is true, the input string must contain an exact
     * match to all the nested Record definition in Array field, including all
     * fields. It must not have additional data. If false, only matching fields
     * will be added and the input may have additional, unrelated data.
     *
     * @param fieldName name of the desired field
     *
     * @param jsonInput a JSON stream input
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     */
    RecordValue putArrayAsJson(String fieldName,
                               InputStream jsonInput,
                               boolean exact);

    /**
     * Set a MapValue field, silently overwriting existing values.
     * The returned object is empty of fields and must be further set by the
     * caller.
     *
     * @param fieldName name of the desired field
     *
     * @return an empty instance of MapValue
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     */
    MapValue putMap(String fieldName);

    /**
     * Set a MapValue field based on map input, silently overwriting
     * existing values.
     *
     * @param fieldName name of the desired field
     *
     * @param map to create value of the desired MapValue field
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     *
     * @throws IllegalArgumentException if the map value type doesn't match the
     * field definition of the named field.
     */
    RecordValue putMap(String fieldName, Map<String, ?> map);

    /**
     * Set a MapValue field, silently overwriting existing values.
     * The created MapValue is based on JSON string input. If the {@code exact}
     * parameter is true, the input string must contain an exact match to all
     * the nested Record definition in Map field, including all fields. It
     * must not have additional data. If false, only matching fields will be
     * added and the input may have additional, unrelated data.
     *
     * @param fieldName name of the desired field
     *
     * @param jsonInput a JSON string
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     */
    RecordValue putMapAsJson(String fieldName,
                             String jsonInput,
                             boolean exact);

    /**
     * Set a MapValue field, silently overwriting existing values.
     * The created MapValue is based on JSON stream input. If the {@code exact}
     * parameter is true, the input string must contain an exact match to all
     * the nested Record definition in Map field, including all fields. It
     * must not have additional data. If false, only matching fields will be
     * added and the input may have additional, unrelated data.
     *
     * @param fieldName name of the desired field
     *
     * @param jsonInput a JSON stream input
     *
     * @param exact set to true for an exact match.  See above
     *
     * @return this
     *
     * @throws IllegalArgumentException if the named field does not exist in
     * the definition of the object or the definition of the field does not
     * match the input definition
     *
     * @throws IllegalArgumentException if exact is true and a field is
     * missing or extra.  It will also be thrown if a field type or value is
     * not correct
     *
     * @throws IllegalArgumentException if the jsonInput is malformed
     */
    RecordValue putMapAsJson(String fieldName,
                             InputStream jsonInput,
                             boolean exact);
}
