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

/**
 * FieldValue represents an item, that is, a value and its associated type.
 * Values can be atomic or complex. An atomic value is a single, indivisible
 * unit of data. A complex value is a value that contains or consists of other
 * values and provides access to its nested values.
 *<p>
 * FieldValue is the building block of row values in a table.
 *<p>
 * The FieldValue interface defines casting and interrogation methods common to
 * all implementing classes.  Each implementing type has its own interface
 * which is an extension of FieldValue.  In most cases interfaces that extend
 * FieldValue have corresponding classes that extend {@link FieldDef}.  The
 * exceptions are {@link Row}, {@link PrimaryKey} and {@link IndexKey}.  These
 * all extend {@link RecordValue} as they are specialized instances of records.
 *<p>
 * By default, fields can be nullable, which means that a special null value can
 * be assigned to them.  A null value is a FieldValue instance that returns true
 * for {@link #isNull} and will throw exceptions for most other operations that
 * require an actual instance of a type.  This means that callers who might
 * reasonably expect a null value should first check to see if the value is null
 * before using the value.
 *<p>
 * FieldValue instances are not thread safe.
 *
 * @since 3.0
 */
public interface FieldValue extends Comparable<FieldValue> {

    /**
     * Create a deep copy of this object.
     *
     * @return a new copy
     */
    FieldValue clone();

    /**
     * Returns the kind of the type associated with this value. The method will
     * never return any of the "wildcard" types (ANY, ANY_ATOMIC, ANY_RECORD).
     *
     * @return the type of this value
     *
     * @throws UnsupportedOperationException if this is the NullValue
     */
    FieldDef.Type getType();

    /**
     * Returns the type associated with this value.
     *
     * @return the FieldDef
     *
     * @throws UnsupportedOperationException if this is the NullValue
     */
    FieldDef getDefinition();

    /**
     * Returns true if this is a {@link BooleanValue}.
     *
     * @return true if this is a BooleanValue, false otherwise
     */
    boolean isBoolean();

    /**
     * Returns true if this is a {@link BinaryValue}.
     *
     * @return true if this is a BinaryValue, false otherwise
     */
    boolean isBinary();

    /**
     * Returns true if this is a {@link DoubleValue}.
     *
     * @return true if this is a DoubleValue, false otherwise
     */
    boolean isDouble();

    /**
     * Returns true if this is an {@link EnumValue}.
     *
     * @return true if this is an EnumValue, false otherwise
     */
    boolean isEnum();

    /**
     * Returns true if this is a {@link FixedBinaryValue}.
     *
     * @return true if this is a FixedBinaryValue, false otherwise
     */
    boolean isFixedBinary();

    /**
     * Returns true if this is a {@link FloatValue}.
     *
     * @return true if this is a FloatValue, false otherwise
     */
    boolean isFloat();

    /**
     * Returns true if this is an {@link IntegerValue}.
     *
     * @return true if this is an IntegerValue, false otherwise
     */
    boolean isInteger();

    /**
     * Returns true if this is a {@link LongValue}.
     *
     * @return true if this is a LongValue, false otherwise
     */
    boolean isLong();

    /**
     * Returns true if this is a {@link NumberValue}.
     *
     * @return true if this is a NumberValue, false otherwise
     *
     * @since 4.4
     */
    boolean isNumber();

    /**
     * Returns true if this is a {@link StringValue}.
     *
     * @return true if this is a StringValue, false otherwise
     */
    boolean isString();

    /**
     * Returns true if this is a {@link TimestampValue}.
     *
     * @return true if this is a TimestampValue, false otherwise
     *
     * @since 4.3
     */
    boolean isTimestamp();

    /**
     * Returns true if this is an {@link ArrayValue}.
     *
     * @return true if this is an ArrayValue, false otherwise
     */
    boolean isArray();

    /**
     * Returns true if this is a {@link MapValue}.
     *
     * @return true if this is a MapValue, false otherwise
     */
    boolean isMap();

    /**
     * Returns true if this is a {@link RecordValue}.
     *
     * @return true if this is a RecordValue, false otherwise
     */
    boolean isRecord();

    /**
     * Returns true if this is a {@link Row}.  Row also
     * returns true for {@link #isRecord}.
     *
     * @return true if this is a Row}, false otherwise
     */
    boolean isRow();

    /**
     * Returns true if this is a {@link PrimaryKey}.  PrimaryKey also
     * returns true for {@link #isRecord} and {@link #isRow}.
     *
     * @return true if this is a PrimaryKey}, false otherwise
     */
    boolean isPrimaryKey();

    /**
     * Returns true if this is an {@link IndexKey}.  IndexKey also
     * returns true for {@link #isRecord}.
     *
     * @return true if this is an IndexKey}, false otherwise
     */
    boolean isIndexKey();

    /**
     * Returns true if this is a null value in a JSON field. This is
     * different from a null that is assigned in the absence of a value
     * in a nullable field, as indicated by {@link #isNull}.
     *
     * @return true if this is a JSON null value, false otherwise.
     *
     * @since 4.2
     */
    boolean isJsonNull();

    /**
     * Returns true if this is a null value instance.
     *
     * @return true if this is a null value, false otherwise.
     */
    boolean isNull();

    /**
     * Returns true if this is an EMPTY value instance.
     *
     * <p>
     * An EMPTY value is used internally to represent cases where an expression
     * returns an empty result. Applications do not normally have to deal with
     * EMPTY values. The only exception is in IndexKey instances: when a
     * TableIterator is used to scan an index and return index keys, the EMPTY
     * value may appear in the returned IndexKey instances. This method can be
     * used to check if the value of an IndexKey field is EMPTY. Applications
     * may also use the {@link IndexKey#putEMPTY} or
     * {@link IndexKey#putEMPTY(String)} method to search an index for
     * entries containing the EMPTY value in one or more of their fields.
     * </p>
     *
     * @return true if this is an EMPTY value, false otherwise.
     *
     * @since 4.4
     */
    boolean isEMPTY();

    /**
     * Returns true if this is an atomic value.
     *
     * @return true if this is an atomic value, false otherwise
     *
     * @since 4.0
     */
    boolean isAtomic();

    /**
     * Returns true if this is numeric value.
     *
     * @return true if this is a numeric value, false otherwise
     *
     * @since 4.0
     */
    boolean isNumeric();

    /**
     * Returns true if this is a complex value.
     *
     * @return true if this is a complex value, false otherwise
     *
     * @since 4.0
     */
    boolean isComplex();

    /**
     * Casts to BinaryValue.
     *
     * @return a BinaryValue
     *
     * @throws ClassCastException if this is not a BinaryValue
     */
    BinaryValue asBinary();

    /**
     * Casts to NumberValue.
     *
     * @return a NumberValue
     *
     * @throws ClassCastException if this is not a NumberValue
     *
     * @since 4.4
     */
    NumberValue asNumber();

    /**
     * Casts to BooleanValue.
     *
     * @return a BooleanValue
     *
     * @throws ClassCastException if this is not a BooleanValue
     */
    BooleanValue asBoolean();

    /**
     * Casts to DoubleValue.
     *
     * @return a DoubleValue
     *
     * @throws ClassCastException if this is not a DoubleValue
     */
    DoubleValue asDouble();

    /**
     * Casts to EnumValue.
     *
     * @return an EnumValue
     *
     * @throws ClassCastException if this is not an EnumValue
     */
    EnumValue asEnum();

    /**
     * Casts to FixedBinaryValue.
     *
     * @return a FixedBinaryValue
     *
     * @throws ClassCastException if this is not a FixedBinaryValue
     */
    FixedBinaryValue asFixedBinary();

    /**
     * Casts to FloatValue.
     *
     * @return a FloatValue
     *
     * @throws ClassCastException if this is not a FloatValue
     */
    FloatValue asFloat();

    /**
     * Casts to IntegerValue.
     *
     * @return an IntegerValue
     *
     * @throws ClassCastException if this is not an IntegerValue
     */
    IntegerValue asInteger();

    /**
     * Casts to LongValue.
     *
     * @return a LongValue
     *
     * @throws ClassCastException if this is not a LongValue
     */
    LongValue asLong();

    /**
     * Casts to StringValue.
     *
     * @return a StringValue
     *
     * @throws ClassCastException if this is not a StringValue
     */
    StringValue asString();

    /**
     * Casts to TimestampValue.
     *
     * @return a TimestampValue
     *
     * @throws ClassCastException if this is not a TimestampValue
     *
     * @since 4.3
     */
    TimestampValue asTimestamp();

    /**
     * Casts to ArrayValue.
     *
     * @return an ArrayValue
     *
     * @throws ClassCastException if this is not an ArrayValue
     */
    ArrayValue asArray();

    /**
     * Casts to MapValue.
     *
     * @return a MapValue
     *
     * @throws ClassCastException if this is not a MapValue
     */
    MapValue asMap();

    /**
     * Casts to RecordValue.
     *
     * @return a RecordValue
     *
     * @throws ClassCastException if this is not a RecordValue
     */
    RecordValue asRecord();

    /**
     * Casts to Row.
     *
     * @return a Row
     *
     * @throws ClassCastException if this is not a Row.
     */
    Row asRow();

    /**
     * Casts to PrimaryKey.
     *
     * @return a PrimaryKey
     *
     * @throws ClassCastException if this is not a PrimaryKey
     */
    PrimaryKey asPrimaryKey();

    /**
     * Casts to IndexKey.
     *
     * @return an IndexKey
     *
     * @throws ClassCastException if this is not an IndexKey
     */
    IndexKey asIndexKey();

   /**
     * Create a JSON representation of the value.
     *
     * @param prettyPrint set to true for a nicely formatted JSON string,
     * with indentation and carriage returns, otherwise the string will be a
     * single line
     *
     * @return a JSON representation of the value
     */
    String toJsonString(boolean prettyPrint);

}
