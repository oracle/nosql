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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Timestamp;

import oracle.kv.impl.api.table.ArrayDefImpl;
import oracle.kv.impl.api.table.ArrayValueImpl;
import oracle.kv.impl.api.table.BinaryDefImpl;
import oracle.kv.impl.api.table.ComplexValueImpl;
import oracle.kv.impl.api.table.EnumDefImpl;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.FixedBinaryDefImpl;
import oracle.kv.impl.api.table.JsonDefImpl;
import oracle.kv.impl.api.table.MapDefImpl;
import oracle.kv.impl.api.table.MapValueImpl;
import oracle.kv.impl.api.table.RecordDefImpl;
import oracle.kv.impl.api.table.RecordValueImpl;
import oracle.kv.impl.api.table.StringDefImpl;
import oracle.kv.impl.api.table.TableJsonUtils;
import oracle.kv.impl.api.table.TimestampDefImpl;
import oracle.kv.impl.api.table.TimestampUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Factory class to create FieldValue instance objects.
 *
 * @since 4.0
 */
public class FieldValueFactory {

    /**
     * Creates a FieldValue instance from JSON input where the type is not
     * known. This method maps the JSON into a map (object) or array where
     * the element type is JSON or one of the scalar types supporting JSON:
     * INTEGER, LONG, FLOAT, DOUBLE, STRING, and BOOLEAN.
     *
     * @param jsonString the JSON representation
     *
     * @return a FieldValue
     *
     * @throws IllegalArgumentException if the input is not valid JSON or the
     * input cannot be parsed, or it cannot be mapped into supported data types.
     *
     * @since 4.2
     */
    public static FieldValue createValueFromJson(String jsonString) {
        return createValueFromJson(FieldDefImpl.Constants.jsonDef, jsonString);
    }

    /**
     * Creates a FieldValue instance from JSON input where the type is not
     * known. This method maps the JSON into a map (object) or array where
     * the element type is JSON or one of the scalar types supporting JSON:
     * INTEGER, LONG, FLOAT, DOUBLE, STRING, and BOOLEAN.
     *
     * @param jsonReader a Reader over JSON
     *
     * @return a FieldValue
     *
     * @throws IllegalArgumentException if the input is not valid JSON or the
     * input cannot be parsed, or it cannot be mapped into supported data types.
     *
     * @throws IOException if the input is not valid JSON or the input cannot
     * be parse, or it cannot be mapped into supported data types.
     *
     * @since 4.2
     */
    public static FieldValue createValueFromJson(Reader jsonReader)
        throws IOException {

        return createValueFromJson(FieldDefImpl.Constants.jsonDef, jsonReader);
    }


    /**
     * <p>Creates a new value from a JSON doc (which is given as a String).</p>
     *
     * If type is RecordDef then:<br>
     * (a) the JSON doc may have fields that do not appear in the record
     *     schema. Such fields are simply skipped.<br>
     * (b) the JSON doc may be missing fields that appear in the record's
     *     schema. Such fields will remain unset in the record value. This
     *     means {@link RecordValue#get(String)} will return null and {@link
     *     RecordValue#toJsonString(boolean)} will skip unset fields.<br>
     *
     * <p>If type is BINARY then the value must be a base64 encoded value.</p>
     *
     * <p>Note: This methods doesn't handle arbitrary JSON, it has to comply to
     * the given type. Also, top level null is not supported.</p>
     *
     * @param type the type definition of the instance.
     * @param jsonString the JSON representation
     * @return the newly created instance
     * @throws IllegalArgumentException for invalid documents
     */
    public static FieldValue createValueFromJson(
        FieldDef type,
        String jsonString) {

        if (jsonString == null) {
            throw new IllegalArgumentException("Not a valid JSON input.");
        }

        if (type.isMRCounter()) {
            throw new IllegalArgumentException("Cannot create CRDTs from " +
                "json. ");
        }

        int l;
        InputStream jsonInput;

        switch (type.getType()) {
        case ARRAY:
            final ArrayValueImpl arrayValue =
                ((ArrayDefImpl)type).createArray();

            jsonInput =  new ByteArrayInputStream(jsonString.getBytes());
            ComplexValueImpl.createFromJson(arrayValue, jsonInput, false);
            return arrayValue;

        case BINARY:
            l = jsonString.length();

            if (jsonString.charAt(0) != '"' ||
                jsonString.charAt(l - 1) != '"' ||
                l <= 1) {
                throw new IllegalArgumentException("Invalid input for " +
                    "BinaryValue: " + jsonString);
            }
            return ((BinaryDefImpl)type).
                fromString(jsonString.substring(1, l - 1));

        case BOOLEAN:
            if ("true".equals(jsonString.toLowerCase())) {
                return createBoolean(true);
            } else if ("false".equals(jsonString.toLowerCase())) {
                return createBoolean(false);
            }

            throw new IllegalArgumentException("Illegal input for a BooleanValue");

        case DOUBLE:
            try {
                return createDouble(Double.parseDouble(jsonString));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    ("Failed to parse DoubleValue: " + e.getMessage()), e);
            }

        case ENUM:
            l = jsonString.length();

            if (jsonString.charAt(0) != '"' ||
                jsonString.charAt(l - 1) != '"' ||
                l <= 1) {
                throw new IllegalArgumentException("Invalid input for a " +
                    "StringValue: " + jsonString);
            }
            return ((EnumDefImpl)type).
                createEnum(jsonString.substring(1, l - 1));

        case FIXED_BINARY:
            l = jsonString.length();

            if (jsonString.charAt(0) != '"' ||
                jsonString.charAt(l - 1) != '"' ||
                l <= 1) {
                throw new IllegalArgumentException("Invalid input for " +
                    "FixedBinaryValue: " + jsonString);
            }
            return ((FixedBinaryDefImpl)type).
                fromString(jsonString.substring(1, l - 1));

        case FLOAT:
            try {
                return createFloat(Float.parseFloat(jsonString));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    ("Failed to parse FloatValue: " + e.getMessage()), e);
            }

        case INTEGER:
            try {
                return createInteger(Integer.parseInt(jsonString));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    ("Failed to parse IntegerValue: " + e.getMessage()), e);
            }

        case LONG:
            try {
                return createLong(Long.parseLong(jsonString));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    ("Failed to parse LongValue: " + e.getMessage()), e);
            }

        case NUMBER:
            try {
                return createNumber(new BigDecimal(jsonString));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    ("Failed to parse NumberValue: Malformed numeric value: " +
                     jsonString), e);
            }

        case TIMESTAMP:
            l = jsonString.length();

            if (jsonString.charAt(0) != '"' ||
                jsonString.charAt(l - 1) != '"' ||
                l <= 1) {
                throw new IllegalArgumentException("Invalid input for " +
                    "TimestampValue: " + jsonString);
            }
            return ((TimestampDefImpl)type).
                fromString(jsonString.substring(1, l - 1));
        case MAP:
            final MapValueImpl mapValue = ((MapDefImpl)type).createMap();
            jsonInput =  new ByteArrayInputStream(jsonString.getBytes());
            ComplexValueImpl.createFromJson(mapValue, jsonInput, false);
            return mapValue;

        case RECORD:
            final RecordValueImpl recordValue =
                ((RecordDefImpl)type).createRecord();

            jsonInput =  new ByteArrayInputStream(jsonString.getBytes());
            ComplexValueImpl.createFromJson(recordValue, jsonInput, false);
            return recordValue;

        case STRING:
            l = jsonString.length();

            if (jsonString.charAt(0) != '"' ||
                jsonString.charAt(l - 1) != '"' ||
                l <= 1) {
                throw new IllegalArgumentException("Invalid input for a " +
                    "StringValue: " + jsonString);
            }
            return ((StringDefImpl)type).
                createString(jsonString.substring(1, l - 1));
        case JSON:
            Reader reader = new StringReader(jsonString);
            try {
                return createValueFromJson(FieldDefImpl.Constants.jsonDef,
                                           reader);
            } catch (IOException ioe) {
                throw new IllegalArgumentException("Unable to parse JSON " +
                                                   "input: " + jsonString +
                                                   ": " + ioe);
            } finally {
                try { reader.close(); } catch (IOException ioe) {}
            }
        case ANY:
        case ANY_ATOMIC:
        case ANY_RECORD:
        case EMPTY:
        default:
            throw new IllegalArgumentException("Type not supported by " +
                "fromJson: " + type);
        }
    }

    /**
     * Creates a new value from a JSON doc (which is given as a Reader).
     * If type is RecordDef then:
     * (a) the JSON doc may have fields that do not appear in the record
     *     schema. Such fields are simply skipped.
     * (b) the JSON doc may be missing fields that appear in the record's
     *     schema. Such fields will remain unset in the record value.
     * If type is BINARY then the value must be a base64 encoded value.
     *
     * Note: This methods doesn't handle arbitrary JSON, it has to comply to
     * the given type. Also, top level null is not supported.
     *
     * @param type the type definition of the instance.
     * @param jsonReader the JSON representation
     * @return the newly created instance
     * @throws IllegalArgumentException for invalid documents
     * @throws IOException for an invalid Reader
     */
    public static FieldValue createValueFromJson(
        FieldDef type,
        Reader jsonReader)
        throws IOException {

        JsonParser jp = null;
        try {
            jp = TableJsonUtils.createJsonParser(jsonReader);

            return fromJson(type, jp);
        } finally {
            if (jp != null) {
                jp.close();
            }
        }
    }

    /**
     * Creates a new value from a JSON doc (which is given as an InputStream).
     * If type is RecordDef then:
     * (a) the JSON doc may have fields that do not appear in the record
     *     schema. Such fields are simply skipped.
     * (b) the JSON doc may be missing fields that appear in the record's
     *     schema. Such fields will remain unset in the record value.
     * If type is BINARY then the value must be a base64 encoded value.
     *
     * Note: This methods doesn't handle arbitrary JSON, it has to comply to
     * the given type. Also, top level null is not supported.
     *
     * @param type the type definition of the instance.
     * @param jsonStream the JSON representation
     * @return the newly created instance
     * @throws IllegalArgumentException for invalid documents
     * @throws IOException for an invalid InputStream
     */
    public static FieldValue createValueFromJson(
        FieldDef type,
        InputStream jsonStream)
        throws IOException {

        JsonParser jp = null;
        try {
            jp = TableJsonUtils.createJsonParser(jsonStream);

            return fromJson(type, jp);
        } finally {
            if (jp != null) {
                jp.close();
            }
        }
    }

    private static FieldValue fromJson(FieldDef type, JsonParser jp)
        throws IOException {

        JsonToken t;
        switch (type.getType()) {
        case ARRAY:
            final ArrayValueImpl arrayValue = ((ArrayDefImpl)type).createArray();
            jp.nextToken();

            arrayValue.addJsonFields(jp, null, false, true/*addMissingFields*/);

            arrayValue.validate();
            return arrayValue;

        case BINARY:
                t = jp.nextToken();
                if (t == JsonToken.VALUE_STRING) {
                    return ((BinaryDefImpl)type).fromString(jp.getText());
                }

                throw new IllegalArgumentException("Invalid input for " +
                    "BinaryValue.");

        case BOOLEAN:
                t = jp.nextToken();
                return createBoolean(jp.getBooleanValue());

        case DOUBLE:
            t = jp.nextToken();
            return createDouble(jp.getDoubleValue());

        case ENUM:
            t = jp.nextToken();
            if (t == JsonToken.VALUE_STRING) {
                return ((EnumDefImpl)type).createEnum(jp.getText());
            }

            throw new IllegalArgumentException("Invalid input for " +
                "StringValue.");

        case FIXED_BINARY:
            t = jp.nextToken();
            if (t == JsonToken.VALUE_STRING) {
                return ((FixedBinaryDefImpl)type).fromString(jp.getText());
            }

            throw new IllegalArgumentException("Invalid input for " +
                "FixedBinaryValue.");

        case FLOAT:
            t = jp.nextToken();
            return createFloat(jp.getFloatValue());

        case INTEGER:
            t = jp.nextToken();
            return createInteger(jp.getIntValue());

        case LONG:
            t = jp.nextToken();
            return createLong(jp.getLongValue());

        case NUMBER:
            t = jp.nextToken();
            return createNumber(TableJsonUtils.jsonParserGetDecimalValue(jp));

        case TIMESTAMP:
            t = jp.nextToken();
            if (t == JsonToken.VALUE_STRING) {
                return ((TimestampDefImpl)type).fromString(jp.getText());
            }

            throw new IllegalArgumentException("Invalid input for " +
                "TimestampValue.");
        case MAP:
            final MapValueImpl mapValue = ((MapDefImpl)type).createMap();
            jp.nextToken();

            mapValue.addJsonFields(jp, null, false, true /*addMissingFields*/);

            mapValue.validate();
            return mapValue;

        case RECORD:
            final RecordValueImpl recordValue =
                ((RecordDefImpl)type).createRecord();
            jp.nextToken();

            recordValue.addJsonFields(
                jp, null, false, true /*addMissingFields*/);

            recordValue.validate();
            return recordValue;

        case STRING:
                t = jp.nextToken();
                if (t == JsonToken.VALUE_STRING) {
                    return ((StringDefImpl)type).createString(jp.getText());
                }

                throw new IllegalArgumentException("Invalid input for " +
                    "StringValue.");
        case JSON:
            return JsonDefImpl.createFromJson(jp, true);
        case ANY:
        case ANY_ATOMIC:
        case ANY_RECORD:
        case EMPTY:
        default:
            throw new IllegalArgumentException("Type not supported by " +
                "fromJson: " + type);
        }
    }

    /**
     * Creates a BinaryValue instance from its java representation.
     *
     * @param v the java value
     * @return the newly created instance
     */
    public static BinaryValue createBinary(byte[] v) {
        return FieldDefImpl.Constants.binaryDef.createBinary(v);
    }

    /**
     * Creates a BooleanValue instance from its java representation.
     *
     * @param v the java value
     * @return the newly created instance
     */
    public static BooleanValue createBoolean(boolean v) {
        return FieldDefImpl.Constants.booleanDef.createBoolean(v);
    }

    /**
     * Creates a DoubleValue instance from its java representation.
     *
     * @param v the java value
     * @return the newly created instance
     */
    public static DoubleValue createDouble(double v) {
        return FieldDefImpl.Constants.doubleDef.createDouble(v);
    }

    /**
     * Creates a FloatValue instance from its java representation.
     *
     * @param v the java value
     * @return the newly created instance
     */
    public static FloatValue createFloat(float v) {
        return FieldDefImpl.Constants.floatDef.createFloat(v);
    }

    /**
     * Creates a IntegerValue instance from its java representation.
     *
     * @param v the java value
     * @return the newly created instance
     */
    public static IntegerValue createInteger(int v) {
        return FieldDefImpl.Constants.integerDef.createInteger(v);
    }

    /**
     * Creates a LongValue instance from its java representation.
     *
     * @param v the java value
     * @return the newly created instance
     */
    public static LongValue createLong(long v) {
        return FieldDefImpl.Constants.longDef.createLong(v);
    }

    /**
     * Creates a NumberValue instance from a int value.
     *
     * @param v the java value
     * @return the newly created instance
	 *
     * @since 4.4
     */
    public static NumberValue createNumber(int v) {
        return FieldDefImpl.Constants.numberDef.createNumber(v);
    }

    /**
     * Creates a NumberValue instance from a long value.
     *
     * @param v the java value
     * @return the newly created instance
     *
     * @since 4.4
     */
    public static NumberValue createNumber(long v) {
        return FieldDefImpl.Constants.numberDef.createNumber(v);
    }

    /**
     * Creates a NumberValue instance from a float value.
     *
     * @param v the java value
     * @return the newly created instance
     *
     * @since 4.4
     */
    public static NumberValue createNumber(float v) {
        return FieldDefImpl.Constants.numberDef.createNumber(v);
    }

    /**
     * Creates a NumberValue instance from a double value.
     *
     * @param v the java value
     * @return the newly created instance
     *
     * @since 4.4
     */
    public static NumberValue createNumber(double v) {
        return FieldDefImpl.Constants.numberDef.createNumber(v);
    }

    /**
     * Creates a NumberValue instance from a BigDecimal value.
     *
     * @param v the java value
     * @return the newly created instance
     *
     * @since 4.4
     */
    public static NumberValue createNumber(BigDecimal v) {
        return FieldDefImpl.Constants.numberDef.createNumber(v);
    }

    /**
     * Creates a special FieldValue instance representing a JSON null
     * value, which returns true from {@link FieldValue#isJsonNull}.
     *
     * @return the newly created instance
     *
     * @since 4.3
     */
    public static FieldValue createJsonNull() {
        return FieldDefImpl.Constants.jsonDef.createJsonNull();
    }

    /**
     * Creates a StringValue instance from its java representation.
     *
     * @param v the java value
     * @return the newly created instance
     */
    public static StringValue createString(String v) {
        return FieldDefImpl.Constants.stringDef.createString(v);
    }

    /**
     * Creates a TimestampValue instance from its java representation.
     *
     * @param v the java value
     * @param precision the precision of Timestamp value
     * @return the newly created instance
     *
     * @throws IllegalArgumentException if precision is invalid.
     */
    public static TimestampValue createTimestamp(Timestamp v, int precision) {
        if (precision < 0 ||
            precision >= FieldDefImpl.Constants.timestampDefs.length) {
            throw new IllegalArgumentException
                ("Invalid precision, it should be in range from 0 to " +
                (FieldDefImpl.Constants.timestampDefs.length - 1) + ": " +
                 precision);
        }
        return
            FieldDefImpl.Constants.timestampDefs[precision].createTimestamp(v);
    }

    /**
     * Creates a TimestampValue instance from a string in format of
     * {@link TimestampDef#DEFAULT_PATTERN}.
     *
     * @param s the string value
     * @param precision the precision of Timestamp value
     * @return the newly created instance
     *
     * @throws IllegalArgumentException if precision is invalid or the string
     * is not in the format of {@link TimestampDef#DEFAULT_PATTERN}.
     */
    public static TimestampValue createTimestamp(String s, int precision) {
        if (precision < 0 ||
            precision >= FieldDefImpl.Constants.timestampDefs.length) {
            throw new IllegalArgumentException
                ("Invalid precision, it should be in range from 0 to " +
                (FieldDefImpl.Constants.timestampDefs.length - 1) + ": " +
                 precision);
        }
        return
            FieldDefImpl.Constants.timestampDefs[precision].fromString(s);
    }

    /**
     * Creates a TimestampValue instance from date time components.
     *
     * @param year the year, from -6383 to 9999.
     * @param month the month of year, from 1 to 12
     * @param day the day of month, from 1 to 31
     * @param hour the hour of day, from 0 to 23
     * @param minute the minute of hour, from 0 to 59
     * @param second the second of minute, from 0 to 59
     * @param fracSeconds the number of fractional seconds in the specified
     * precision.  e.g. if precision is 3, then fractional seconds can be a
     * value within the range 0 ~ 999.
     * @param precision the precision of Timestamp value
     *
     * @return the newly created instance
     *
     * @throws IllegalArgumentException if precision is invalid or any component
     * is invalid.
     */
    public static TimestampValue createTimestamp(int year, int month, int day,
                                                 int hour, int minute,
                                                 int second, int fracSeconds,
                                                 int precision) {

        if (precision < 0 ||
            precision >= FieldDefImpl.Constants.timestampDefs.length) {
            throw new IllegalArgumentException
                ("Invalid precision, it should be in range from 0 to " +
                (FieldDefImpl.Constants.timestampDefs.length - 1) + ": " +
                 precision);
        }

        TimestampUtils.validateComponent(0, year);
        TimestampUtils.validateComponent(1, month);
        TimestampUtils.validateComponent(2, day);
        TimestampUtils.validateComponent(3, hour);
        TimestampUtils.validateComponent(4, minute);
        TimestampUtils.validateComponent(5, second);
        if (precision == 0) {
            if (fracSeconds != 0) {
                throw new IllegalArgumentException
                    ("Invalid fracSeconds, it should be 0 with precision 0");
            }
        } else {
            int max = (int)Math.pow(10, precision) - 1;
            if (fracSeconds < 0 || fracSeconds > max) {
                throw new IllegalArgumentException
                    ("Invalid fracSeconds, it should be in range 0 ~ " + max);
            }
        }

        byte[] bytes = TimestampUtils.toBytes(year, month, day,
                                              hour, minute, second,
                                              fracSeconds, precision);
        return FieldDefImpl.Constants.timestampDefs[precision].createTimestamp(
            bytes);
    }

    /**
     * @hidden
     *
     * Creates a MapValue instance that can handle fields of any type.
     * This is not useful for schema-based operation but is handy for
     * generic tree of FieldValue manipulation and JSON formatting.
     *
     * @return the newly created instance
     */
    public static MapValue createMap() {
        return FieldDefImpl.Constants.mapAnyDef.createMap();
    }

    /**
     * @hidden
     *
     * Creates an ArrayValue instance that can handle fields of any type.
     * This is not useful for schema-based operation but is handy for
     * generic tree of FieldValue manipulation and JSON formatting.
     *
     * @return the newly created instance
     */
    public static ArrayValue createArray() {
        return FieldDefImpl.Constants.arrayAnyDef.createArray();
    }
}
