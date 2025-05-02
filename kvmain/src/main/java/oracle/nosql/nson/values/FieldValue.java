/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.nson.values;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Timestamp;

import oracle.nosql.nson.JsonParseException;
import oracle.nosql.nson.Nson;

/**
 * FieldValue is the base class of all data items in the Oracle NoSQL
 * Database Cloud system.  Each data item is an instance of FieldValue
 * allowing access to its type and its value as well as additional
 * utility methods that operate on FieldValue.
 * <p>
 * FieldValue instances are typed, based on the {@link Type} enumeration. The
 * type system is similar to that of JSON with extensions. It is a subset of
 * the database types in Oracle NoSQL Database in that these objects do not
 * inherently conform to a fixed schema and some of the database types, such
 * as RECORD and ENUM, require a schema. The mappings of types is described
 * <a href="package-summary.html#package.description">
 * here
 * </a>
 * and is not reproduced in this documentation.
 * <p>
 * FieldValue instances used for put operations are not validated against the
 * target table schema in the driver. Validation happens where the table schema
 * is available. If an instance does not match the target table an exception is
 * thrown.
 * <p>
 * Returned FieldValue instances always conform to a table schema, or to the
 * shape implied by a query projection.
 * <p>
 * FieldValue instances are created in several ways:
 * <ul>
 * <li>From JSON input. In this path the JSON is parsed and mapped to
 * correspondings types.</li>
 * <li>Construction. Applications can construct instances manually, adding
 * and setting fields and types as needed. This mechanism can be more efficient
 * than parsing from JSON and it gives the application more control over the
 * types used when the mapping from JSON may not be precise.</li>
 * <li>Returned by operations on a table. These instances are created internally
 * by operations that return data and will have the schema implied by the table
 * or query.</li>
 * </ul>
 * <p>
 * There are special cases for handling types that are not in the JSON type
 * system when creating a FieldValue instance from JSON. These are described
 * in the documentation for {@link #createFromJson}.
 * <p>
 * Numeric values are an extension to JSON which has only a single numeric type,
 * Number. For this reason the system is generous in mapping numeric types among
 * one another and will allow any lossless mapping without error. Mappings
 * default to the most efficient valid format.
 * <p>
 * FieldValue has convenience interfaces to return values of atomic types such
 * as number, string, and boolean. These interfaces will allow implicit type
 * coercions (e.g. integer to long) as long as the coercion is lossless;
 * otherwise ClassCastException is thrown. The determination of lossless is
 * based on both type and actual value. For example a long can return an
 * integer value as long as the value of the long is a valid integer value.
 * String coercions always work for atomic types but an atomic type value
 * cannot always be returned from a string. MapValue, ArrayValue, and
 * BinaryValue cannot be coerced. BooleanValue can only be coerced to and
 * from StringValue. If a coercion that can result in loss of information is
 * desired it should be done manually by the application.
 * <p>
 * FieldValue instances are not thread-safe. On input, they should not be reused
 * until the operation that uses them has returned.
 */
public abstract class FieldValue implements Comparable<FieldValue> {

    /**
     * The type of a field.
     * These types correspond to the fundamental Oracle NoSQL Database types.
     */
    public enum Type {
        /** An array of FieldValue instances */
        ARRAY,
        /** A binary value */
        BINARY,
        /** A boolean value */
        BOOLEAN,
        /** A double value */
        DOUBLE,
        /** An integer value */
        INTEGER,
        /** A long value */
        LONG,
        /** A map of FieldValue instances */
        MAP,
        /** A string value */
        STRING,
        /** A timestamp */
        TIMESTAMP,
        /** A number value */
        NUMBER,
        /** A JSON null value */
        JSON_NULL,
        /** A null value, used only by index keys */
        NULL,
        /** @hidden An empty, or missing value, used only by index keys */
        EMPTY;
    }

    /**
     * Returns the type of the object
     *
     * @return the type
     */
    public abstract Type getType();

    /**
     * Returns an integer value for the field if the value can be represented
     * as a valid integer without loss of information. Numbers are coerced
     * using Java rules and strings are parsed according to Java rules.
     *
     * @return an integer value
     *
     * @throws ClassCastException if the coercion cannot be performed based
     * on the type of the value
     * @throws ArithmeticException if a numeric coercion would lose information
     * @throws NumberFormatException if the underlying type is a StringValue
     * and it cannot be coerced
     */
    public int getInt() {
        return asInteger().getValue();
    }

    /**
     * Returns a long value for the field if the value can be represented
     * as a valid long without loss of information. Numbers are coerced
     * using Java rules and strings are parsed according to Java rules.
     *
     * @return a long value
     *
     * @throws ClassCastException if the coercion cannot be performed without
     * loss of information
     * @throws NumberFormatException if the underlying type is a StringValue
     * and it cannot be coerced
     */
    public long getLong() {
        return asLong().getValue();
    }

    /**
     * Returns a double value for the field if the value can be represented
     * as a valid double without loss of information. Numbers are coerced
     * using Java rules and strings are parsed according to Java rules.
     *
     * @return a double value
     *
     * @throws ClassCastException if the coercion cannot be performed without
     * loss of information
     * @throws NumberFormatException if the underlying type is a StringValue
     * and it cannot be coerced
     */
    public double getDouble() {
        return asDouble().getValue();
    }

    /**
     * Returns a BigDecimal value for the field if the value can be represented
     * as a valid BigDecimal without loss of information. Numbers are coerced
     * using Java rules and strings are parsed according to Java rules.
     *
     * @return a BigDecimal value
     *
     * @throws ClassCastException if the coercion cannot be performed without
     * loss of information
     * @throws NumberFormatException if the underlying type is a StringValue
     * and it cannot be coerced
     */
    public BigDecimal getNumber() {
        return asNumber().getValue();
    }

    /**
     * Casts a numeric value to double, possibly with loss of information about
     * magnitude, precision or sign.
     *
     * @return a double value
     *
     * @throws ClassCastException if this value is not numeric
     */
    public double castAsDouble() {
        throw new ClassCastException(
            "Value can not be cast to a double: " + getClass());
    }

    /**
     * Returns a binary byte array value for the field if the value is
     * binary
     *
     * @return a byte array
     *
     * @throws ClassCastException if this is not a BinaryValue
     */
    public byte[] getBinary() {
        return asBinary().getValue();
    }

    /**
     * Returns a boolean value for the field if the value is a boolean or
     * a string. If it is a StringValue the rules used for Java
     * Boolean.parseBoolean() are applied.
     *
     * @return the boolean value
     *
     * @throws ClassCastException if this is not a BooleanValue or
     * StringValue
     */
    public boolean getBoolean() {
        return asBoolean().getValue();
    }

    /**
     * Returns a String value for the field. The String value cannot be
     * created for MapValue, ArrayValue and BinaryValue. String values that
     * are coerced use Java rules for representation.
     *
     * @return a String value
     *
     * @throws ClassCastException if this cannot be represented as a String
     */
    public String getString() {
        return asString().getValue();
    }

    /**
     * Returns a TimestampValue as a {@link java.sql.Timestamp} value.
     *
     * @return a Timestamp value
     *
     * @throws ClassCastException if this is not a TimestampValue
     */
    public Timestamp getTimestamp() {
        return asTimestamp().getValue();
    }

    /**
     * Casts the object to IntegerValue.
     *
     * @return an IntegerValue
     *
     * @throws ClassCastException if this is not an IntegerValue
     */
    public IntegerValue asInteger() {
        return (IntegerValue) this;
    }

    /**
     * Casts to StringValue.
     *
     * @return a StringValue
     *
     * @throws ClassCastException if this is not a StringValue
     */
    public StringValue asString() {
        return (StringValue) this;
    }

    /**
     * Casts the object to LongValue.
     *
     * @return a LongValue
     *
     * @throws ClassCastException if this is not a LongValue
     */
    public LongValue asLong() {
        return (LongValue) this;
    }

    /**
     * Casts the object to NumberValue.
     *
     * @return a NumberValue
     *
     * @throws ClassCastException if this is not a NumberValue
     */
    public NumberValue asNumber() {
        return (NumberValue) this;
    }

    /**
     * Casts the object to TimestampValue. This method accepts objects
     * of type {@link Type#TIMESTAMP}, {@link Type#STRING},
     * {@link Type#INTEGER} and {@link Type#LONG}.
     * In the case of {@link Type#STRING} the value is parsed as an
     * ISO8601 timestamp value and if valid, accepted. In the numeric cases
     * the value is interpreted as milliseconds since the Epoch,
     * "1970-01-01T00:00:00".
     *
     * @return a TimestampValue
     *
     * @throws ClassCastException if this is not a TimestampValue
     */
    public TimestampValue asTimestamp() {
        if (getType() == Type.TIMESTAMP) {
            return (TimestampValue)this;
        } else if (getType() == Type.STRING) {
            return new TimestampValue(getString());
        } else if (getType() == Type.INTEGER || getType() == Type.LONG) {
            return new TimestampValue(getLong());
        }
        throw new ClassCastException("Can't cast to a TimestampValue");
    }

    /**
     * Casts the object to BooleanValue.
     *
     * @return a BooleanValue
     *
     * @throws ClassCastException if this is not a BooleanValue
     */
    public BooleanValue asBoolean() {
        return (BooleanValue) this;
    }

    /**
     * Casts the object to ArrayValue.
     *
     * @return a ArrayValue
     *
     * @throws ClassCastException if this is not a ArrayValue
     */
    public ArrayValue asArray() {
        return (ArrayValue) this;
    }

    /**
     * Casts the object to BinaryValue.
     *
     * @return a BinaryValue
     *
     * @throws ClassCastException if this is not a BinaryValue
     */
    public BinaryValue asBinary() {
        return (BinaryValue) this;
    }

    /**
     * Casts the object to MapValue.
     *
     * @return a MapValue
     *
     * @throws ClassCastException if this is not a MapValue
     */
    public MapValue asMap() {
        return (MapValue) this;
    }

    /**
     * Casts to DoubleValue.
     *
     * @return a DoubleValue
     *
     * @throws ClassCastException if this is not a DoubleValue
     */
    public DoubleValue asDouble() {
        return (DoubleValue) this;
    }

    /**
     * Casts to JsonNullValue.
     *
     * @return a JsonNullValue
     *
     * @throws ClassCastException if this is not a JsonNullValue
     */
    public JsonNullValue asJsonNull() {
        return (JsonNullValue) this;
    }

    /**
     * Casts to NullValue.
     *
     * @return a NullValue
     *
     * @throws ClassCastException if this is not a NullValue
     */
    public NullValue asNull() {
        return (NullValue) this;
    }

    /**
     * @hidden
     *
     * Casts to EmptyValue.
     *
     * @return a EmptyValue
     *
     * @throws ClassCastException if this is not a EmptyValue
     */
    public EmptyValue asEmpty() {
        return (EmptyValue) this;
    }

    /**
     * Returns whether this is an IntegerValue
     *
     * @return true if this FieldValue is of type IntegerValue, false otherwise
     */
    public boolean isInteger() {
        return (getType() == Type.INTEGER);
    }

    /**
     * Returns whether this is an LongValue
     *
     * @return true if this FieldValue is of type LongValue, false otherwise
     */
    public boolean isLong() {
        return (getType() == Type.LONG);
    }

    /**
     * Returns whether this is an DoubleValue
     *
     * @return true if this FieldValue is of type DoubleValue, false otherwise
     */
    public boolean isDouble() {
        return (getType() == Type.DOUBLE);
    }

    /**
     * Returns whether this is an NumberValue
     *
     * @return true if this FieldValue is of type NumberValue, false otherwise
     */
    public boolean isNumber() {
        return (getType() == Type.NUMBER);
    }

    /**
     * Returns whether this is an BinaryValue
     *
     * @return true if this FieldValue is of type BinaryValue, false otherwise
     */
    public boolean isBinary() {
        return (getType() == Type.BINARY);
    }

    /**
     * Returns whether this is an BooleanValue
     *
     * @return true if this FieldValue is of type BooleanValue, false otherwise
     */
    public boolean isBoolean() {
        return (getType() == Type.BOOLEAN);
    }

    /**
     * Returns whether this is an ArrayValue
     *
     * @return true if this FieldValue is of type ArrayValue, false otherwise
     */
    public boolean isArray() {
        return (getType() == Type.ARRAY);
    }

    /**
     * Returns whether this is an MapValue
     *
     * @return true if this FieldValue is of type MapValue, false otherwise
     */
    public boolean isMap() {
        return (getType() == Type.MAP);
    }

    /**
     * Returns whether this is an StringValue
     *
     * @return true if this FieldValue is of type StringValue, false otherwise
     */
    public boolean isString() {
        return (getType() == Type.STRING);
    }

    /**
     * Returns whether this is an TimestampValue
     *
     * @return true if this FieldValue is of type TimestampValue, false
     * otherwise
     */
    public boolean isTimestamp() {
        return (getType() == Type.TIMESTAMP);
    }

    /**
     * Returns whether this is an atomic value, that is, not an array or map
     * value.
     *
     * @return Whether this is an atomic value.
     */
    public boolean isAtomic() {
        Type t = getType();
        return (t != Type.ARRAY && t != Type.MAP);
    }

    /**
     * Returns whether this is a numeric value (integer, long, double,
     * or number)
     * value.
     *
     * @return Whether this is a numeri value.
     */
    public boolean isNumeric() {
        Type t = getType();
        switch(t) {
        case INTEGER:
        case LONG:
        case DOUBLE:
        case NUMBER:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns whether this is an SQL NULL value.
     *
     * @return true if this FieldValue is of type NullValue, false otherwise
     */
    public boolean isNull() {
        return this == NullValue.getInstance();
    }

    /**
     * Returns whether this is a json null value.
     *
     * @return true if this FieldValue is of type JsonNullValue, false otherwise
     */
    public boolean isJsonNull() {
        return this == JsonNullValue.getInstance();
    }

    /**
     * Returns whether this is either a JSON null or a SQL NULL value.
     *
     * @return true if this FieldValue is of type NullValue or JsonNullValue,
     * false otherwise
     */
    public boolean isAnyNull() {
        return isJsonNull() || isNull();
    }


    /**
     * @hidden
     * @return true if is empty
     */
    public boolean isEMPTY() {
        return this == EmptyValue.getInstance();
    }

    /**
     * @hidden
     * @return true if is NULL, json null, or empty
     */
    public boolean isSpecialValue() {
        Type type = getType();
        return (type == Type.NULL ||
                type == Type.JSON_NULL ||
                type == Type.EMPTY);
    }

    /**
     * Returns a JSON representation of the value using a default
     * configuration for output format.
     *
     * @return the JSON representation of this value.
     */
    public String toJson() {
        return toJson(null);
    }

    /**
     * Returns a JSON representation of the value using the options, if
     * specified.
     *
     * @param options configurable options used to affect the JSON output
     * format of some data types. May be null.
     *
     * @return the JSON representation of this value.
     */
    public String toJson(JsonOptions options) {
        FieldValueEventHandler handler =
            (options != null && options.getPrettyPrint() ?
             new JsonPrettySerializer(options) : new JsonSerializer(options));
        try {
            FieldValueEventHandler.generate(this, handler);
            return handler.toString();
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Failed to serialize FieldValue into JSON: " +
                ioe.getMessage());
        }
    }

    /**
     * Returns a String representation of the value, consistent with
     * representation as JSON strings.
     *
     * @return the String value
     */
    @Override
    public String toString() {
        return toJson();
    }

    /**
     * Returns the serialized size of this value. This value can be used to
     * estimate amount of throughput used by a sample value. This size will
     * always be larger than the actual space consumed because the server
     * serializes in a more compact format.
     *
     * @return the size, in bytes, used by the serialized format of this
     * value.
     */
    public int getSerializedSize() {
        return Nson.getSerializedSize(this);
    }

    /**
     * @hidden
     * @return size of value in bytes
     */
    public abstract long sizeof();

    /**
     * Constructs a new FieldValue instance based on the JSON string provided.
     * <p>
     * Two of the types in the driver type system are not part of the JSON data
     * model -- TIMESTAMP and BINARY -- and will never be created using this
     * method. If a table schema includes these types, as well as the ENUM type
     * supported by Oracle NoSQL Database, they should be input as follows:
     * <ul>
     * <li>BINARY should be a Base64-encoded String</li>
     * <li>TIMESTAMP may be either a long value representing milliseconds since
     * January 1, 1970, or a String value that is in a valid ISO 8601 formatted
     * string.
     * </li>
     * <li>ENUM should be a String that matches one of the valid enumeration
     * values</li>
     * </ul>
     * If one of these types is to be used inside a JSON data type, there is no
     * schema in the database server and the type cannot be inferred by the system
     * and interpretation is left to the application.
     *
     * @param jsonInput a JSON formatted String
     *
     * @param options configurable options used to affect the JSON output
     * format of some data types. May be null.
     *
     * @return a new FieldValue instance representing the JSON string
     *
     * @throws JsonParseException if the string is not valid JSON
     */
    public static FieldValue createFromJson(String jsonInput,
                                            JsonOptions options) {
        return JsonUtils.createValueFromJson(jsonInput, options);
    }

    /**
     * Constructs a new FieldValue instance based on JSON read from the
     * Reader provided.
     *
     * @param jsonInput a Reader containing JSON
     *
     * @param options configurable options used to affect the JSON output
     * format of some data types. May be null.
     *
     * @return a new FieldValue instance representing the JSON string
     *
     * @throws JsonParseException if the input is not valid JSON
     */
    public static FieldValue createFromJson(Reader jsonInput,
                                            JsonOptions options) {
        return JsonUtils.createValueFromJson(jsonInput, options);
    }

    /**
     * Constructs a new FieldValue instance based on JSON read from the
     * InputStream provided.
     *
     * @param jsonInput an InputStream containing JSON
     *
     * @param options configurable options used to affect the JSON output
     * format of some data types. May be null.
     *
     * @return a new FieldValue instance representing the JSON string
     *
     * @throws JsonParseException if the input is not valid JSON
     */
    public static FieldValue createFromJson(InputStream jsonInput,
                                            JsonOptions options) {
        return JsonUtils.createValueFromJson(jsonInput, options);
    }

    /*
     * Internal utility methods
     */
    static void validateName(String name, FieldValue value) {
        if (name == null) {
            throw new IllegalArgumentException("Field name is null");
        }
        if (value == null) {
            throw new IllegalArgumentException("FieldValue is null");
        }
    }
}
