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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;

import oracle.kv.impl.util.FastExternalizable;

/**
 * FieldDef represents an immutable metadata object used to represent a single
 * data type. A data type defines a set of values, which are said to belong to
 * or be instances of that data type. Data types are either <i>atomic</i> or
 * <i>complex</i>. Atomic types define sets of atomic values, like integers,
 * doubles, strings, etc. Complex types represent complex values, i.e., values
 * that contain other (nested) values, like records, arrays, and maps.
 * <p>
 * All supported data types are represented as instances of FieldDef.
 * Supported types are defined in the enumeration {@link FieldDef.Type Type}.
 * Instances of FieldDef are created when defining the fields for a new
 * {@link Table}, or during query processing.
 * <p>
 * Each instance has these properties:
 *<ul>
 *<li>type -- the kind of the type (one of the values of the
 *    {@link FieldDef.Type} enum.
 *<li>description -- an optional description used when defining the type.
 *</ul>
 *
 * @since 3.0
 */
public interface FieldDef {

    /**
     * The type of a field definition.
     *
     * @hidden.see {@link #writeFastExternal FastExternalizable format}
     */
    public enum Type implements FastExternalizable {

        /*
         * WARNING: To avoid breaking serialization compatibility, the order of
         * the values must not be changed and new values must be added at the
         * end.
         */

        ARRAY(0),
        BINARY(1),
        BOOLEAN(2),
        DOUBLE(3),
        ENUM(4),
        FIXED_BINARY(5),
        FLOAT(6),
        INTEGER(7),
        LONG(8),
        MAP(9),
        RECORD(10),
        STRING(11),
        ANY(12),
        ANY_RECORD(13),
        ANY_ATOMIC(14),
        EMPTY(15),
        JSON(16),
        TIMESTAMP(17),
        NUMBER(18),
        ANY_JSON_ATOMIC(19),
        GEOMETRY(20), /* This type does not have a corresponding FieldDef class */
        POINT(21),    /* This type does not have a corresponding FieldDef class */
        JSON_INT_MRCOUNTER(22), /* This type is only used for json serialization */
        JSON_LONG_MRCOUNTER(23), /*This type is only used for json serialization */
        JSON_NUM_MRCOUNTER(24); /* This type is only used for json serialization */


        private static final Type[] VALUES = values();

        private Type(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        /**
         * Returns the Type with the specified ordinal.
         *
         * @hidden For internal use only
         * @param ordinal the ordinal
         * @return the type
         * @throws IllegalArgumentException if the type is not found
         */
        public static Type valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unexpected Type ordinal: " + ordinal);
            }
        }

        /**
         * Reads a constant value from an input stream.
         *
         * @hidden For internal use only
         */
        public static Type readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();

            if (ordinal < 0) {
                return null;
            }

            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException("Unexpected Type ordinal: " + ordinal);
            }
        }

        /**
         * Writes this object to an output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i>
         * </ol>
         *
         * @hidden For internal use only
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
            out.writeByte(ordinal());
        }

        /**
         * @return true if this type can be a counter CRDT.
         *
         * @hidden For internal use only
         */
        public boolean isValidCounterCRDTType() {
            if (this == INTEGER || this == LONG || this == NUMBER) {
                return true;
            }
            return false;
        }
    }

    /**
     * Perform a deep copy of this FieldDef instance.
     *
     * @return a new instance equal to this
     */
    FieldDef clone();

    /**
     * Returns the description of the field.
     *
     * @return the description of the field if set, otherwise null
     */
    String getDescription();

    /**
     * Returns the type of the field.
     *
     * @return the type of the field
     */
    Type getType();

    /**
     * Returns true if the type of this field matches the parameter.
     *
     * @return true if the type of this field matches the parameter
     */
    boolean isType(FieldDef.Type type);

    /**
     * @return true if this type can participate in a primary key. Only atomic
     * types can be part of a key. Among the atomic types, Boolean, Binary and
     * FixedBinary are not allowed in keys.
     */
    boolean isValidKeyField();

    /**
     * @return true if this type can participate in an index. Only atomic types
     * can be part of an index key. Among the atomic types, Boolean, Binary and
     * FixedBinary are not allowed in index keys.
     */
    boolean isValidIndexField();

    /**
     * Returns true if this is an {@link AnyDef}.
     *
     * @return true if this is an AnyDef, false otherwise
     *
     * @since 4.0
     */
    boolean isAny();

    /**
     * Returns true if this is an {@link AnyRecordDef}.
     *
     * @return true if this is an AnyRecordDef, false otherwise
     *
     * @since 4.0
     */
    boolean isAnyRecord();

    /**
     * Returns true if this is an {@link AnyAtomicDef}.
     *
     * @return true if this is an AnyAtomicDef, false otherwise
     *
     * @since 4.0
     */
    boolean isAnyAtomic();

    /**
     * Returns true if this is a {@link NumberDef}.
     *
     * @return true if this is a NumberDef, false otherwise
     *
     * @since 4.4
     */
    boolean isNumber();

    /**
     * Returns true if this is an {@link AnyJsonAtomicDef}.
     *
     * @return true if this is an AnyJsonAtomicDef, false otherwise
     *
     * @since 4.3
     */
    boolean isAnyJsonAtomic();

    /**
     * Returns true if this is a {@link BooleanDef}.
     *
     * @return true if this is a BooleanDef, false otherwise
     */
    boolean isBoolean();

    /**
     * Returns true if this is a {@link BinaryDef}.
     *
     * @return true if this is a BinaryDef, false otherwise
     */
    boolean isBinary();

    /**
     * Returns true if this is a {@link DoubleDef}.
     *
     * @return true if this is a DoubleDef, false otherwise
     */
    boolean isDouble();

    /**
     * Returns true if this is an {@link EnumDef}.
     *
     * @return true if this is an EnumDef, false otherwise
     */
    boolean isEnum();

    /**
     * Returns true if this is a {@link FixedBinaryDef}.
     *
     * @return true if this is a FixedBinaryDef, false otherwise
     */
    boolean isFixedBinary();

    /**
     * Returns true if this is a {@link FloatDef}.
     *
     * @return true if this is a FloatDef, false otherwise
     */
    boolean isFloat();

    /**
     * Returns true if this is an {@link IntegerDef}.
     *
     * @return true if this is an IntegerDef, false otherwise
     */
    boolean isInteger();

    /**
     * Returns true if this is a {@link LongDef}.
     *
     * @return true if this is a LongDef, false otherwise
     */
    boolean isLong();

    /**
     * Returns true if this is a {@link StringDef}.
     *
     * @return true if this is a StringDef, false otherwise
     */
    boolean isString();

    /**
     * Returns true if this is a STRING AS UUID type
     *
     * @return true if this is a STRING AS UUID type, false otherwise
     */
    boolean isUUIDString();

    /**
     * Returns true if this is a {@link TimestampDef}.
     *
     * @return true if this is a TimestampDef, false otherwise
     *
     * @since 4.3
     */
    boolean isTimestamp();

    /**
     * Returns true if this is an {@link ArrayDef}.
     *
     * @return true if this is an ArrayDef, false otherwise
     */
    boolean isArray();

    /**
     * Returns true if this is a {@link MapDef}.
     *
     * @return true if this is a MapDef, false otherwise
     */
    boolean isMap();

    /**
     * Returns true if this is a {@link RecordDef}.
     *
     * @return true if this is a RecordDef, false otherwise
     */
    boolean isRecord();

    /**
     * Returns true if this is a JSON type.
     *
     * @return true if this is a JSON type, false otherwise
     *
     * @since 4.2
     */
    boolean isJson();

    /**
     * Returns true if this is an atomic type.
     *
     * @return true if this is an atomic type, false otherwise
     *
     * @since 4.0
     */
    boolean isAtomic();

   /**
     * Returns true if this is a numeric type.
     *
     * @return true if this is a numeric type, false otherwise
     *
     * @since 4.0
     */
    boolean isNumeric();

    /**
     * Returns true if this is a complex type.
     *
     * @return true if this is a complex type, false otherwise
     *
     * @since 4.0
     */
    boolean isComplex();

    /**
     * @since 20.1
     */
    boolean isPrecise();

    /**
     * Returns true if this is a multi-region counter.
     *
     * @return true if this is a multi-region counter, false otherwise.
     *
     * @since 21.1
     */
    default boolean isMRCounter() {
        return false;
    }

    /**
     * Casts to AnyDef.
     *
     * @return an AnyDef
     *
     * @throws ClassCastException if this is not an AnyDef
     *
     * @since 4.0
     */
    AnyDef asAny();

    /**
     * Casts to AnyRecordDef.
     *
     * @return an AnyRecordDef
     *
     * @throws ClassCastException if this is not an AnyRecordDef
     *
     * @since 4.0
     */
    AnyRecordDef asAnyRecord();

    /**
     * Casts to AnyAtomicDef.
     *
     * @return an AnyAtomicDef
     *
     * @throws ClassCastException if this is not an AnyAtomicDef
     *
     * @since 4.0
     */
    AnyAtomicDef asAnyAtomic();

    /**
     * Casts to AnyJsonAtomicDef.
     *
     * @return an AnyJsonAtomicDef
     *
     * @throws ClassCastException if this is not an AnyJsonAtomicDef
     *
     * @since 4.3
     */
    AnyJsonAtomicDef asAnyJsonAtomic();

    /**
     * Casts to BinaryDef.
     *
     * @return a BinaryDef
     *
     * @throws ClassCastException if this is not a BinaryDef
     */
    BinaryDef asBinary();

    /**
     * Casts to NumberDef.
     *
     * @return a NumberDef
     *
     * @throws ClassCastException if this is not a NumberDef
     *
     * @since 4.4
     */
    NumberDef asNumber();

    /**
     * Casts to BooleanDef.
     *
     * @return a BooleanDef
     *
     * @throws ClassCastException if this is not a BooleanDef
     */
    BooleanDef asBoolean();

    /**
     * Casts to DoubleDef.
     *
     * @return a DoubleDef
     *
     * @throws ClassCastException if this is not a DoubleDef
     */
    DoubleDef asDouble();

    /**
     * Casts to EnumDef.
     *
     * @return an EnumDef
     *
     * @throws ClassCastException if this is not an EnumDef
     */
    EnumDef asEnum();

    /**
     * Casts to FixedBinaryDef.
     *
     * @return a FixedBinaryDef
     *
     * @throws ClassCastException if this is not a FixedBinaryDef
     *
     */
    FixedBinaryDef asFixedBinary();

    /**
     * Casts to FloatDef.
     *
     * @return a FloatDef
     *
     * @throws ClassCastException if this is not a FloatDef
     */
    FloatDef asFloat();

    /**
     * Casts to IntegerDef.
     *
     * @return an IntegerDef
     *
     * @throws ClassCastException if this is not an IntegerDef
     */
    IntegerDef asInteger();

    /**
     * Casts to LongDef.
     *
     * @return a LongDef
     *
     * @throws ClassCastException if this is not a LongDef
     */
    LongDef asLong();

    /**
     * Casts to StringDef.
     *
     * @return a StringDef
     *
     * @throws ClassCastException if this is not a StringDef
     */
    StringDef asString();

    /**
     * Casts to TimestampDef.
     *
     * @return a TimestampDef
     *
     * @throws ClassCastException if this is not a TimestampDef
     *
     * @since 4.3
     */
    TimestampDef asTimestamp();

    /**
     * Casts to ArrayDef.
     *
     * @return an ArrayDef
     *
     * @throws ClassCastException if this is not an ArrayDef
     */
    ArrayDef asArray();

    /**
     * Casts to MapDef.
     *
     * @return a MapDef
     *
     * @throws ClassCastException if this is not a MapDef
     */
    MapDef asMap();

    /**
     * Casts to RecordDef.
     *
     * @return a RecordDef
     *
     * @throws ClassCastException if this is not a RecordDef
     */
    RecordDef asRecord();

    /**
     * Casts to JsonDef.
     *
     * @return a JsonDef
     *
     * @throws ClassCastException if this is not a JsonDef
     *
     * @since 4.2
     */
    JsonDef asJson();

    /**
     * Creates an empty ArrayValue.
     *
     * @return an empty ArrayValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * an ArrayValue
     */
    ArrayValue createArray();

    /**
     * Creates a NumberValue instance based on the value
     *
     * @param value the value to use
     *
     * @return a NumberValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     *  a NumberValue
     *
     * @since 4.4
     */
    NumberValue createNumber(int value);

    /**
     * Creates a NumberValue instance based on the value
     *
     * @param value the value to use
     *
     * @return a NumberValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     *  a NumberValue
     *
     * @since 4.4
     */
    NumberValue createNumber(long value);

    /**
     * Creates a NumberValue instance based on the value
     *
     * @param value the value to use
     *
     * @return a NumberValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     *  a NumberValue
     *
     * @since 4.4
     */
    NumberValue createNumber(float value);

    /**
     * Creates a NumberValue instance based on the value
     *
     * @param value the value to use
     *
     * @return a NumberValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     *  a NumberValue
     *
     * @since 4.4
     */
    NumberValue createNumber(double value);

    /**
     * Creates a NumberValue instance based on the value
     *
     * @param value a non-null BigDecimal value
     *
     * @return a NumberValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     *  a NumberValue
     *
     * @since 4.4
     */
    NumberValue createNumber(BigDecimal value);

    /**
     * Creates a BinaryValue instance based on the value.
     *
     * @param value the byte array to use for the new value object.  Must not
     * be null.
     *
     * @return a BinaryValue
     *
     * @throws IllegalArgumentException if this instance is not able to create a
     * BinaryValue or if the value is null
     */
    BinaryValue createBinary(byte[] value);

    /**
     * Creates a BooleanValue instance based on the value.
     *
     * @param value the value to use
     *
     * @return a BooleanValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * a BooleanValue
     */
    BooleanValue createBoolean(boolean value);

    /**
     * Creates a DoubleValue instance based on the value.
     *
     * @param value the value to use
     *
     * @return a DoubleValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     *  a DoubleValue
     */
    DoubleValue createDouble(double value);

    /**
     * Creates an EnumValue instance based on the value.
     *
     * @param value the value to use
     *
     * @return a EnumValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * an EnumValue or if the value is not valid for the definition
     */
    EnumValue createEnum(String value);

    /**
     * Creates a FixedBinaryValue instance based on the value.
     *
     * @param value the value to use.  It must not be null.
     *
     * @return a FixedBinaryValue
     *
     * @throws IllegalArgumentException if this instance is not able to create a
     * FixedBinaryValue or if the value is null or not valid for the definition
     */
    FixedBinaryValue createFixedBinary(byte[] value);

    /**
     * Creates a FloatValue instance based on the value.
     *
     * @param value the value to use
     *
     * @return a FloatValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * a FloatValue
     */
    FloatValue createFloat(float value);

    /**
     * Creates an IntegerValue instance based on the value.
     *
     * @param value the value to use
     *
     * @return a IntegerValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * an IntegerValue or if the value is not valid for the definition
     */
    IntegerValue createInteger(int value);

    /**
     * Creates a LongValue instance based on the value.
     *
     * @param value the value to use
     *
     * @return a LongValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * a LongValue
     */
    LongValue createLong(long value);

    /**
     * Creates a TimestampValue instance based on the value.
     *
     * @param value the value to use
     *
     * @return a TimestampValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * a TimestampValue
     *
     * @since 4.3
     */
    TimestampValue createTimestamp(Timestamp value);

    /**
     * Creates an empty MapValue.
     *
     * @return an empty MapValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * a MapValue
     */
    MapValue createMap();

    /**
     * Creates an empty RecordValue.
     *
     * @return an empty RecordValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * a RecordValue
     */
    RecordValue createRecord();

    /**
     * Creates a StringValue instance based on the value.
     *
     * @param value the value to use
     *
     * @return a StringValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * a StringValue
     */
    StringValue createString(String value);

    /**
     * Creates a special FieldValue instance representing a JSON null.
     * This value returns true for {@link FieldValue#isJsonNull}.
     *
     * @return a FieldValue
     *
     * @throws IllegalArgumentException if this instance is not able to create
     * a JSON null (is not of type JSON)
     *
     * @since 4.3
     */
    FieldValue createJsonNull();
}
