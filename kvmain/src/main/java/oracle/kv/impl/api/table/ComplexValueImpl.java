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

package oracle.kv.impl.api.table;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import oracle.kv.impl.util.SizeOf;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.core.JsonToken;

/**
 * ComplexValueImpl is an intermediate abstract implementation class used to
 * factor out common state and code from complex types such as Array, Map,
 * Record, Row, etc.  It introduces a single function to get the field
 * definition ({@link FieldDef}) for the object.
 * <p>
 * The field definition ({@link FieldDef}) is table metadata that defines the
 * types and constraints in a table row.  It is required by ComplexValue
 * instances to define the shape of the values they hold.  It is used to
 * validate type and enforce constraints for values added to a ComplexValue.
 */
public abstract class ComplexValueImpl extends FieldValueImpl {

    private static final long serialVersionUID = 1L;

    protected final FieldDef fieldDef;

    protected ComplexValueImpl(FieldDef fieldDef) {
        this.fieldDef = fieldDef;
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.COMPLEX_VALUE;
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion) {
        /*
         * This class is only used during query return and is not serialized
         * with the table metadata otherwise. So there is no need to support
         * FastExternal.
         */
        fastExternalNotSupported();
    }

    /*
     * Public api methods from Object and FieldValue
     */

    /**
     * Provides a common method for the string value of the complex types.
     */
    @Override
    public String toString() {
        return toJsonString(false);
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    /*
     * Local methods
     */
    @Override
    public long sizeof() {
        return (SizeOf.OBJECT_OVERHEAD + 2 * SizeOf.OBJECT_REF_OVERHEAD);
    }

    /**
     * Validate the value of the object.  By default there is not validation.
     * Subclasses may implement this.
     */
    public void validate() {
    }

    /**
     * Common validation code for map and array enforcing rules as to
     * what types can be inserted.
     */
    protected FieldValue validate(FieldValue value, FieldDefImpl elemDef) {
        if (value.isNull()) {
            throw new IllegalArgumentException(
                "Cannot insert null values into collection: " + getClass());
        }

        FieldDefImpl valDef = (FieldDefImpl)value.getDefinition();

        if (!valDef.isSubtype(elemDef)) {
            throw new IllegalArgumentException(
                "Type mismatch. Cannot insert value of type\n" +
                valDef.getDDLString() +
                "\ninto a collection of type\n" +
                getDefinition().getDDLString());
        }

        /* turn float to double */
        if (value.isFloat() && elemDef.isJson()) {
            value = FieldDefImpl.Constants.doubleDef.createDouble(
                value.asFloat().get());
        }

        if (elemDef.isJson() &&
            value.isComplex() &&
            !valDef.equals(FieldDefImpl.Constants.arrayJsonDef) &&
            !valDef.equals(FieldDefImpl.Constants.mapJsonDef)) {
            throw new IllegalArgumentException(
                "Type mismatch. Cannot insert value of type\n" +
                valDef.getDDLString() +
                "\ninto a collection of type\n" +
                getDefinition().getDDLString());
        }

        value = ((FieldValueImpl)value).castToSuperType(elemDef);
        if (elemDef.hasMin() || elemDef.hasMax()) {
            validateRangeValue(elemDef, value);
        }
        return value;
    }

    void validateRangeValue(FieldDefImpl def, FieldValue value) {
        switch (def.getType()) {
            case INTEGER:
                assert(value.isInteger());
                ((IntegerDefImpl)def).validateValue(value.asInteger().get());
                break;
            case LONG:
                assert(value.isLong());
                ((LongDefImpl)def).validateValue(value.asLong().get());
                break;
            case FLOAT:
                assert(value.isFloat());
                ((FloatDefImpl)def).validateValue(value.asFloat().get());
                break;
            case DOUBLE:
                assert(value.isDouble());
                ((DoubleDefImpl)def).validateValue(value.asDouble().get());
                break;
            case STRING:
                assert(value.isString());
                ((StringDefImpl)def).validateValue(value.asString().get());
                break;
            default:
                break;
        }
    }

    /**
     * Populate the given complex value from a JSON doc (which is given as
     * a reader). If exact is true, the json doc must match exactly to the
     * schema of the complex value. Otherwise, if this is a RecordValue, then
     * (a) the JSON doc may have fields that do not appear in the record
     *     schema. Such fields are simply skipped.
     * (b) the JSON doc may be missing fields that appear in the record's
     *     schema. Such fields will remain unset in the record value.
     */
    public static void createFromJson(
        ComplexValueImpl complexValue,
        Reader jsonInput,
        boolean exact,
        boolean addMissingFields) {

        try (JsonParser jp = TableJsonUtils.createJsonParser(jsonInput)) {
            /*move to START_OBJECT or START_ARRAY*/
            if (jp.nextToken() == null) {
                return;
            }

            complexValue.addJsonFields(jp, null, exact, addMissingFields);
            complexValue.validate();
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                ("Failed to parse JSON input: " + ioe.getMessage()), ioe);
        }
    }

    /**
     * Populate the given complex value from a JSON doc (which is given as an
     * input stream) and add missing fields with default values.
     */
    public static void createFromJson(ComplexValueImpl complexValue,
                                      InputStream jsonInput,
                                      boolean exact) {

        createFromJson(complexValue, jsonInput, exact, true);
    }

    /**
     * Populate the given complex value from a JSON doc (which is given as an
     * input stream). If exact is true, the json doc must match exactly to the
     * schema of the complex value. Otherwise, if this is a RecordValue, then
     * (a) the JSON doc may have fields that do not appear in the record
     *     schema. Such fields are simply skipped.
     * (b) the JSON doc may be missing fields that appear in the record's
     *     schema. Such fields will remain unset in the record value.
     */
    public static void createFromJson(
        ComplexValueImpl complexValue,
        InputStream jsonInput,
        boolean exact,
        boolean addMissingFields) {

        try (JsonParser jp = TableJsonUtils.createJsonParser(jsonInput)) {
            /* move to START_OBJECT or START_ARRAY */
            if (jp.nextToken() == null) {
                return;
            }

            complexValue.addJsonFields(jp, null, exact, addMissingFields);

            complexValue.validate();
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                ("Failed to parse JSON input: " + ioe.getMessage()), ioe);
        }
    }

    /**
     * Add JSON fields from the JsonParser to this object.
     * @param jp the parser
     * This is used to handle situations that are conditional and are not
     * caught by a RecordValueImpl.validate() call.
     * @param currentFieldName the current field name, which is the last
     * field name extracted from the parser.  This is only non-null when
     * addJsonFields is called from RecordValueImpl, which knows field
     * names.
     * @param exact true if the JSON needs have all fields present.
     * @param addMissingFields true if the JSON need to be added missing fields.
     */
    abstract void addJsonFields(
        JsonParser jp,
        String currentFieldName,
        boolean exact,
        boolean addMissingFields);

    /**
     * A utility method for use by subclasses to skip JSON input
     * when an exact match is not required.  This function finds a matching
     * end of array or object token.  It will recurse in the event a
     * nested array or object is detected.
     */
    static void skipToJsonToken(JsonParser jp, JsonToken skipTo) {
        try {
            JsonToken token = jp.nextToken();
            while (token != skipTo) {
                if (token == JsonToken.START_OBJECT) {
                    skipToJsonToken(jp, JsonToken.END_OBJECT);
                } else if (token == JsonToken.START_ARRAY) {
                    skipToJsonToken(jp, JsonToken.END_ARRAY);
                }
                token = jp.nextToken();
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException
                (("Failed to parse JSON input: " + ioe.getMessage()), ioe);
        }
    }

    /*
     * Throw IAE for JSON parse exception, with location information. Location
     * also has raw byte/char offsets if that'd be helpful in the future.
     */
    static void jsonParseException(String msg, JsonLocation location) {
        throw new IllegalArgumentException(msg + " starting at location: line "+
                                           location.getLineNr() + ", column " +
                                           location.getColumnNr());
    }

    static void checkNumberType(
        String fieldName,
        NumberType expected,
        JsonParser jp)
        throws IOException {

        NumberType actual = null;

        if (jp.getCurrentToken().isNumeric()) {

            actual = jp.getNumberType();

            if (actual == expected || expected == NumberType.BIG_DECIMAL) {
                return;
            }

            /* Jackson infers the type.  Many casts are safe, detect these. */
            switch (actual) {
            case INT:
                /* int can cast to long, float, double */
                return;
            case FLOAT:
            case LONG:
                /* float and long can cast to double */
                if (expected == NumberType.DOUBLE) {
                    return;
                }

                /* long may be able to cast to float */
                if (expected != NumberType.FLOAT) {
                    break;
                }
                //$FALL-THROUGH$
            case DOUBLE:
                /*
                 * Jackson parses into DOUBLE and not FLOAT. Allow the cast from
                 * DOUBLE to FLOAT only if the number is within the FLOAT
                 * boundaries .
                 */
                if (expected == NumberType.FLOAT) {
                    Double d = jp.getDoubleValue();
                    if (d.isNaN() || d == 0) {
                        return;
                    }
                    if (d < 0) {
                        d = -d;
                    }
                    float f = d.floatValue();
                    if (f >= Float.MIN_VALUE && f <= Float.MAX_VALUE) {
                        return;
                    }
                }
                break;
            default:
                break;
            }
        }
        throw new IllegalArgumentException(
            "Illegal value for numeric field " +
            (fieldName != null ? fieldName : jp.getText()) +
            ". Expected " + expected + ", is " +
            (jp.getCurrentToken().isNumeric() ?
                 jp.getNumberType() : jp.getCurrentToken()));
    }
}
