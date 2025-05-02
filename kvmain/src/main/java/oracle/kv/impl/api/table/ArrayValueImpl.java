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

import static oracle.kv.impl.api.table.TableJsonUtils.jsonParserGetDecimalValue;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import oracle.kv.impl.api.table.ValueSerializer.ArrayValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.util.SizeOf;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.core.JsonToken;

/**
 * ArrayValueImpl implements the ArrayValue interface to hold an object of
 * type ArrayDef.
 */
public class ArrayValueImpl extends ComplexValueImpl
    implements ArrayValue, ArrayValueSerializer {

    private static final long serialVersionUID = 1L;

    private final ArrayList<FieldValueImpl> array = new ArrayList<>();

    /*
     * Support for homogenous arrays of valid scalars in wildcard arrays,
     * which are arrays of ANY, JSON, ANY_ATOMIC, or ANY_JSON_ATOMIC.
     *
     * This internal feature exists so that storage and serialization can
     * be optimized for homogenous arrays of scalars. Specifically, we don't
     * want to store an extra byte per element to specify the type of the
     * element. We don't track homogeneity for non scalara because the space
     * savings are smaller, and the cpu cost of tracking is higher.
     *
     * Rules:
     *  1. If homogeneousType is non-null, this is an array that may contain
     *     mixed elements, but which currently contains scalars only, all
     *     having homogeneousType as their type. Otherwise, this is either a
     *     "typed" array (it's elementDef is not a wildcard) or a non-
     *     homogeneous wildcard array.
     *  2. Public APIs will always see this as an array of wildcard (e.g. JSON)
     *  3. There are internal APIs to access the homogenous type. These are
     *     used in this class and in other classes that need to know (for now,
     *     FieldValueSerialization only).
     *
     * Empty arrays of wildcards always start with homogeneousType == null.
     * The following are the valid transitions for arrays of wildcard:
     *
     * 1. empty array -> homogeneous array
     *    Empty arrays of wildcards always start with homogeneousType == null.
     *    On first insertion, if the type of inserted element is scalar, it is
     *    stored in this.homogeneousType.
     *
     * 2. homogeneous array -> non-homogeneous array.
     * If an element whose type is not the same as this.homogeneousType is
     * inserted, this.homogeneousType is set to null, making the array a
     * non-homogeneous one.
     *
     * These transitions are handled in trackHomogenousType().
     */

    private FieldDefImpl homogeneousType;

    /* The flag is set when the array is constructed by an ArrayConstrIter,
     * whose theIsConditional flag is true. It is used/needed when a query has
     * array_collect(expr) with generic (not index-based) group-by and the input
     * expr of the array_collect() returns more than 1 value. Generic group-by
     * means that the query execution plan at the RNs will have a GroupIter over
     * an SFWIter. The input expr of the array_collect() is computed by the
     * SFWIter. But if this expr returns more than one items, we have to put
     * these values into a conditionally-contructed array in order to construct
     * the record returned by the SFWIter (because we don't have a class that
     * represents a sequence of values). Then, the GroupIter needs to know if
     * an array it receives as the input value for array_collect() is an
     * "original" array (e.g., an array that existed in an table row), or an
     * "artificial" array that was constructed by the SFWIter. In the former
     * case, the "original" array is included as-is into the array constructed
     * by array_collect(). In later case, the "artificial" array is unboxed and
     * its elements are inserted in the array constructed by array_collect(). */
    private transient boolean isConditionallyConstructed;

    ArrayValueImpl(ArrayDef def) {
        super(def);
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.ARRAY_VALUE;
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

    @Override
    public ArrayValueImpl clone() {
        ArrayValueImpl newArray = new ArrayValueImpl(getDefinition());
        for (FieldValue val : array) {
            newArray.add(val.clone());
        }
        newArray.homogeneousType = homogeneousType;
        return newArray;
    }

    @Override
    public long sizeof() {
        long size = super.sizeof();
        size += (2 * SizeOf.OBJECT_REF_OVERHEAD +
                 SizeOf.ARRAYLIST_OVERHEAD +
                 SizeOf.objectArraySize(array.size()));

        for (FieldValue elem : array) {
            size += ((FieldValueImpl)elem).sizeof();
        }

        return size;
    }

    @Override
    public int hashCode() {
        int code = size();
        for (FieldValue val : array) {
            code += val.hashCode();
        }
        return code;
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof ArrayValueImpl) {
            ArrayValueImpl otherValue = (ArrayValueImpl) other;
            /* maybe avoid some work */
            if (this == otherValue) {
                return true;
            }

            /*
             * detailed comparison
             */
            if (size() == otherValue.size() &&
                getDefinition().equals(otherValue.getDefinition()) &&
                (homogeneousType == null ||
                 homogeneousType.equals(otherValue.getHomogeneousType()))) {

                for (int i = 0; i < size(); i++) {
                    if (!get(i).equals(otherValue.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * FieldDef must match.
     *
     * Compare field values in array order.  Return as soon as there is a
     * difference. If this object has a field the other does not, return &gt;
     * 0.  If this object is missing a field the other has, return &lt; 0.
     */
    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof ArrayValueImpl) {
            ArrayValueImpl otherImpl = (ArrayValueImpl) other;
            if (!getDefinition().equals(otherImpl.getDefinition())) {
                throw new IllegalArgumentException
                    ("Cannot compare ArrayValues with different definitions");
            }

            for (int i = 0; i < size(); i++) {
                FieldValueImpl val = get(i);
                if (otherImpl.size() < i + 1) {
                    return 1;
                }
                int ret = val.compareTo(otherImpl.get(i));
                if (ret != 0) {
                    return ret;
                }
            }
            /* they must be equal */
            return 0;
        }
        throw new ClassCastException("Object is not an ArrayValue");
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.ARRAY;
    }

    @Override
    public boolean isArray() {
        return true;
    }

    @Override
    public ArrayValue asArray() {
        return this;
    }

    /*
     * Public api methods from ArrayValue
     */

    @Override
    public ArrayDefImpl getDefinition() {
        return (ArrayDefImpl)fieldDef;
    }

    @Override
    public FieldValueImpl get(int index) {
        return array.get(index);
    }

    @Override
    public FieldValueImpl getElement(int index) {
        return array.get(index);
    }

    @Override
    public int size() {
        return array.size();
    }

    @Override
    public List<FieldValue> toList() {
        return Collections.unmodifiableList(array);
    }

    @Override
    public ArrayValue add(FieldValue value) {
        value = validate(value, getElementDef());
        array.add((FieldValueImpl)value);
        trackHomogeneousType(value);
        return this;
    }

    @Override
    public ArrayValue add(int index, FieldValue value) {
        value = validate(value, getElementDef());
        array.add(index, (FieldValueImpl)value);
        trackHomogeneousType(value);
        return this;
    }

    public void addAll(ArrayValueImpl arr) {
        array.addAll(arr.getArrayInternal());
    }

    @Override
    public ArrayValue set(int index, FieldValue value) {
        value = validate(value, getElementDef());
        array.set(index, (FieldValueImpl)value);
        trackHomogeneousType(value);
        return this;
    }

    /**
     * Integer
     */
    @Override
    public ArrayValue add(int value) {
        addScalar(getElementDef().createInteger(value));
        return this;
    }

    @Override
    public ArrayValue add(int values[]) {
        FieldDefImpl edef = getElementDef();
        for (int i : values) {
            addScalar(edef.createInteger(i));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, int value) {
        addScalar(index, getElementDef().createInteger(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, int value) {
        setScalar(index, getElementDef().createInteger(value));
        return this;
    }

    /**
     * Long
     */
    @Override
    public ArrayValue add(long value) {
        addScalar(getElementDef().createLong(value));
        return this;
    }

    @Override
    public ArrayValue add(long values[]) {
        FieldDef edef = getElementDef();
        for (long l : values) {
            addScalar(edef.createLong(l));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, long value) {
        addScalar(index, getElementDef().createLong(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, long value) {
        setScalar(index, getElementDef().createLong(value));
        return this;
    }

    /**
     * String
     */
    @Override
    public ArrayValue add(String value) {
        addScalar(getElementDef().createString(value));
        return this;
    }

    @Override
    public ArrayValue add(String values[]) {
        FieldDef edef = getElementDef();
        for (String s : values) {
            addScalar(edef.createString(s));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, String value) {
        addScalar(index, getElementDef().createString(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, String value) {
        setScalar(index, getElementDef().createString(value));
        return this;
    }

    /**
     * Double
     */
    @Override
    public ArrayValue add(double value) {
        addScalar(getElementDef().createDouble(value));
        return this;
    }

    @Override
    public ArrayValue add(double values[]) {
        FieldDef edef = getElementDef();
        for (double d : values) {
            addScalar(edef.createDouble(d));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, double value) {
        addScalar(index, getElementDef().createDouble(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, double value) {
        setScalar(index, getElementDef().createDouble(value));
        return this;
    }

    /**
     * Float
     */
    @Override
    public ArrayValue add(float value) {
        addScalar(getElementDef().createFloat(value));
        return this;
    }

    @Override
    public ArrayValue add(float values[]) {
        FieldDefImpl edef = getElementDef();
        for (float d : values) {
            addScalar(edef.createFloat(d));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, float value) {
        addScalar(index, getElementDef().createFloat(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, float value) {
        setScalar(index, getElementDef().createFloat(value));
        return this;
    }

    /*
     * BigDecimal
     */
    @Override
    public ArrayValue addNumber(int value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(int values[]) {
        FieldDef def = getElementDef();
        for (int val : values) {
            addScalar(def.createNumber(val));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, int value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, int value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(long value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(long values[]) {
        FieldDef def = getElementDef();
        for (long val : values) {
            addScalar(def.createNumber(val));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, long value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, long value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(float value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(float values[]) {
        FieldDef def = getElementDef();
        for (float val : values) {
            addScalar(def.createNumber(val));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, float value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, float value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(double value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(double values[]) {
        FieldDef def = getElementDef();
        for (double val : values) {
            addScalar(def.createNumber(val));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, double value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, double value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(BigDecimal value) {
        addScalar(getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue addNumber(BigDecimal values[]) {
        FieldDef def = getElementDef();
        for (BigDecimal bd : values) {
            addScalar(def.createNumber(bd));
        }
        return this;
    }

    @Override
    public ArrayValue addNumber(int index, BigDecimal value) {
        addScalar(index, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public ArrayValue setNumber(int index, BigDecimal value) {
        setScalar(index, getElementDef().createNumber(value));
        return this;
    }

    /**
     * Boolean
     */
    @Override
    public ArrayValue add(boolean value) {
        addScalar(getElementDef().createBoolean(value));
        return this;
    }

    @Override
    public ArrayValue add(boolean values[]) {
        FieldDef edef = getElementDef();
        for (boolean b : values) {
            addScalar(edef.createBoolean(b));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, boolean value) {
        addScalar(index, getElementDef().createBoolean(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, boolean value) {
        setScalar(index, getElementDef().createBoolean(value));
        return this;
    }

    /**
     * Binary
     */
    @Override
    public ArrayValue add(byte[] value) {
        addScalar(getElementDef().createBinary(value));
        return this;
    }

    @Override
    public ArrayValue add(byte[] values[]) {
        FieldDef edef = getElementDef();
        for (byte[] b : values) {
            addScalar(edef.createBinary(b));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, byte[] value) {
        addScalar(index, getElementDef().createBinary(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, byte[] value) {
        setScalar(index, getElementDef().createBinary(value));
        return this;
    }

    /**
     * FixedBinary
     */
    @Override
    public ArrayValue addFixed(byte[] value) {
        addScalar(getElementDef().createFixedBinary(value));
        return this;
    }

    @Override
    public ArrayValue addFixed(byte[] values[]) {
        FieldDef edef = getElementDef();
        for (byte[] b : values) {
            addScalar(edef.createFixedBinary(b));
        }
        return this;
    }

    @Override
    public ArrayValue addFixed(int index, byte[] value) {
        addScalar(index, getElementDef().createFixedBinary(value));
        return this;
    }

    @Override
    public ArrayValue setFixed(int index, byte[] value) {
        setScalar(index, getElementDef().createFixedBinary(value));
        return this;
    }

    /**
     * Enum
     */
    @Override
    public ArrayValue addEnum(String value) {
        addScalar(getElementDef().createEnum(value));
        return this;
    }

    @Override
    public ArrayValue addEnum(String values[]) {
        FieldDef edef = getElementDef();
        for (String s : values) {
            addScalar(edef.createEnum(s));
        }
        return this;
    }

    @Override
    public ArrayValue addEnum(int index, String value) {
        addScalar(index, getElementDef().createEnum(value));
        return this;
    }

    @Override
    public ArrayValue setEnum(int index, String value) {
        setScalar(index, getElementDef().createEnum(value));
        return this;
    }

    /**
     * Timestamp
     */
    @Override
    public ArrayValue add(Timestamp value) {
        addScalar(getElementDef().createTimestamp(value));
        return this;
    }

    @Override
    public ArrayValue add(Timestamp values[]) {
        FieldDef def = getElementDef();
        for (Timestamp v : values) {
            addScalar(def.createTimestamp(v));
        }
        return this;
    }

    @Override
    public ArrayValue add(int index, Timestamp value) {
        addScalar(index, getElementDef().createTimestamp(value));
        return this;
    }

    @Override
    public ArrayValue set(int index, Timestamp value) {
        addScalar(index, getElementDef().createTimestamp(value));
        return this;
    }

    /**
     * JSON Null
     */
    @Override
    public ArrayValue addJsonNull() {
        addScalar(getElementDef().createJsonNull());
        return this;
    }

    @Override
    public ArrayValue addJsonNull(int index) {
        addScalar(index, getElementDef().createJsonNull());
        return this;
    }

    @Override
    public ArrayValue setJsonNull(int index) {
        setScalar(index, getElementDef().createJsonNull());
        return this;
    }

    /*
     * Record
     */
    @Override
    public RecordValue setRecord(int index) {
        RecordValueImpl val = getElementDef().createRecord();
        array.set(index, val);
        clearHomogeneousType();
        return val;
    }

    @Override
    public RecordValueImpl addRecord() {
        RecordValueImpl val = getElementDef().createRecord();
        array.add(val);
        clearHomogeneousType();
        return val;
    }

    @Override
    public RecordValue addRecord(int index) {
        RecordValueImpl val = getElementDef().createRecord();
        array.add(index, val);
        clearHomogeneousType();
        return val;
    }

    /*
     * Map
     */
    @Override
    public MapValue setMap(int index) {
        MapValueImpl val = getElementDef().createMap();
        array.set(index, val);
        clearHomogeneousType();
        return val;
    }

    @Override
    public MapValueImpl addMap() {
        MapValueImpl val = getElementDef().createMap();
        array.add(val);
        clearHomogeneousType();
        return val;
    }

    @Override
    public MapValue addMap(int index) {
        MapValueImpl val = getElementDef().createMap();
        array.add(index, val);
        clearHomogeneousType();
        return val;
    }

    /*
     * Array
     */
    @Override
    public ArrayValue setArray(int index) {
        ArrayValueImpl val = getElementDef().createArray();
        array.set(index, val);
        clearHomogeneousType();
        return val;
    }

    @Override
    public ArrayValueImpl addArray() {
        ArrayValueImpl val = getElementDef().createArray();
        array.add(val);
        clearHomogeneousType();
        return val;
    }


    @Override
    public ArrayValue addArray(int index) {
        ArrayValueImpl val = getElementDef().createArray();
        array.add(index, val);
        clearHomogeneousType();
        return val;
    }

    /*
     * JSON
     */
    @Override
    public ArrayValueImpl addJson(String jsonInput) {
        Reader reader = new StringReader(jsonInput);
        try {
            return addJson(reader);
        } finally {
            try { reader.close(); } catch (IOException ioe) {}
        }
    }

    @Override
    public ArrayValueImpl addJson(Reader jsonReader) {
        add(JsonDefImpl.createFromReader(jsonReader));
        return this;
    }

    @Override
    public ArrayValueImpl addJson(int index, String jsonInput) {
        Reader reader = new StringReader(jsonInput);
        try {
            return addJson(index, reader);
        } finally {
            try { reader.close(); } catch (IOException ioe) {}
        }
    }

    @Override
    public ArrayValueImpl addJson(int index, Reader jsonReader) {
        add(index, JsonDefImpl.createFromReader(jsonReader));
        return this;
    }

    @Override
    public ArrayValueImpl setJson(int index, String jsonInput) {
        Reader reader = new StringReader(jsonInput);
        try {
            return setJson(index, reader);
        } finally {
            try { reader.close(); } catch (IOException ioe) {}
        }
    }

    @Override
    public ArrayValueImpl setJson(int index, Reader jsonReader) {
        set(index, JsonDefImpl.createFromReader(jsonReader));
        return this;
    }

    /*
     * Methods from ComplexValueImpl
     */

    /**
     * Parse a JSON array and put the extracted values into "this" array.
     */
    @Override
    public void addJsonFields(
        JsonParser jp,
        String fieldName,
        boolean exact,
        boolean addMissingFields) {

        try {
            FieldDef element = getElementDef();

            JsonToken t = jp.currentToken();

            JsonLocation location = jp.currentLocation();

            if (t != JsonToken.START_ARRAY) {
                jsonParseException(("Expected [ token to start array, instead "
                                    + "found " + t), location);
            }

            while ((t = jp.nextToken()) != JsonToken.END_ARRAY) {

                if (t == null || t == JsonToken.END_OBJECT) {
                    jsonParseException("Did not find end of array", location);
                }

                /*
                 * Handle null.
                 */
                if (jp.getCurrentToken() == JsonToken.VALUE_NULL &&
                    !element.isJson()) {
                    throw new IllegalArgumentException
                        ("Invalid null value in JSON input for array");
                }

                switch (element.getType()) {
                case INTEGER:
                    checkNumberType(null, NumberType.INT, jp);
                    add(jp.getIntValue());
                    break;
                case LONG:
                    checkNumberType(null, NumberType.LONG, jp);
                    add(jp.getLongValue());
                    break;
                case DOUBLE:
                    checkNumberType(null, NumberType.DOUBLE, jp);
                    add(jp.getDoubleValue());
                    break;
                case FLOAT:
                    checkNumberType(null, NumberType.FLOAT, jp);
                    add(jp.getFloatValue());
                    break;
                case NUMBER:
                    checkNumberType(null, NumberType.BIG_DECIMAL, jp);
                    addNumber(jsonParserGetDecimalValue(jp));
                    break;
                case STRING:
                    add(jp.getText());
                    break;
                case BINARY:
                    add(jp.getBinaryValue());
                    break;
                case FIXED_BINARY:
                    addFixed(jp.getBinaryValue());
                    break;
                case BOOLEAN:
                    add(jp.getBooleanValue());
                    break;
                case TIMESTAMP:
                    add(element.asTimestamp().fromString(jp.getText()));
                    break;
                case ARRAY:
                    ArrayValueImpl array1 = addArray();
                    array1.addJsonFields(jp, null, exact,
                                         addMissingFields);
                    break;
                case MAP:
                    MapValueImpl map = addMap();
                    map.addJsonFields(jp, null, exact,
                                      addMissingFields);
                    break;
                case RECORD:
                    RecordValueImpl record = addRecord();
                    record.addJsonFields(jp, null, exact,
                                         addMissingFields);
                    break;
                case ENUM:
                    addEnum(jp.getText());
                    break;
                case JSON:
                case ANY_JSON_ATOMIC:
                    array.add((FieldValueImpl)JsonDefImpl.createFromJson(jp, false));
                    break;
                case ANY:
                case ANY_ATOMIC:
                case ANY_RECORD:
                case EMPTY:
                case GEOMETRY:
                case POINT:
                case JSON_INT_MRCOUNTER:
                case JSON_LONG_MRCOUNTER:
                case JSON_NUM_MRCOUNTER:
                    throw new IllegalStateException(
                        "An array type cannot have " + element.getType() +
                        " as its element type");
                }
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException
                (("Failed to parse JSON input: " + ioe.getMessage()), ioe);
        } catch (RuntimeException re) {
            if (re instanceof IllegalArgumentException) {
                throw re;
            }
            throw new IllegalArgumentException
                (("Failed to parse JSON input: " + re.toString()), re);
        }
    }

    /*
     * FieldValueImpl internal api methods
     */

    /**
     * Increment the value of the array element, not the array.  There
     * can only be one element in this array.
     */
    @Override
    public FieldValueImpl getNextValue() {
        if (size() != 1) {
            throw new IllegalArgumentException
                ("Array values used in ranges must contain only one element");
        }
        ArrayValueImpl newArray = new ArrayValueImpl(getDefinition());
        FieldValueImpl fvi = get(0).getNextValue();
        newArray.add(fvi);
        return newArray;
    }

    @Override
    public FieldValueImpl getMinimumValue() {
        if (size() != 1) {
            throw new IllegalArgumentException
                ("Array values used in ranges must contain only one element");
        }
        ArrayValueImpl newArray = new ArrayValueImpl(getDefinition());
        FieldValueImpl fvi = get(0).getMinimumValue();
        newArray.add(fvi);
        return newArray;
    }

    @Override
    public void toStringBuilder(StringBuilder sb, DisplayFormatter formatter) {
        if (formatter == null) {
            throw new IllegalArgumentException(
                "DisplayFormatter must be non-null");
        }
        sb.append('[');
        for (int i = 0; i < array.size(); i++) {
            if (i > 0) {
                formatter.comma(sb);
            }
            FieldValueImpl value = array.get(i);
            value.toStringBuilder(sb, formatter);
        }
        sb.append(']');
    }


    @SuppressWarnings("unchecked")
    static ArrayValueImpl fromJavaObjectValue(FieldDef def, Object o) {

        Iterable<Object> coll = null;

        if (o instanceof Iterable<?>) {
            coll = (Iterable<Object>) o;
        } else {
            coll = Arrays.asList((Object[]) o);
        }

        ArrayValueImpl newArray = (ArrayValueImpl)def.createArray();

        for (Object value : coll) {
            newArray.add(FieldValueImpl.fromJavaObjectValue(
                             newArray.getElementDef(), value));
        }

        return newArray;
    }

    /*
     * Local methods
     */

    public void clear() {
        array.clear();
    }

    public void remove(int pos) {
        array.remove(pos);
    }

    public boolean isConditionallyConstructed() {
        return isConditionallyConstructed;
    }

    public void setConditionallyConstructed(boolean v) {
        isConditionallyConstructed = v;
    }

    /*
     * These next 3 exist to consolidate valid insertions.
     */
    private ArrayValue addScalar(FieldValue value) {
        assert ((FieldDefImpl)value.getDefinition()).isSubtype(getElementDef());
        /* turn float to double */
        if (value.isFloat() && getElementDef().isJson()) {
            value = FieldDefImpl.Constants.doubleDef.createDouble(
                value.asFloat().get());
        }
        trackHomogeneousType(value);
        array.add((FieldValueImpl)value);
        return this;
    }

    private ArrayValue addScalar(int index, FieldValue value) {
        assert ((FieldDefImpl)value.getDefinition()).isSubtype(getElementDef());
        /* turn float to double */
        if (value.isFloat() && getElementDef().isJson()) {
            value = FieldDefImpl.Constants.doubleDef.createDouble(
                value.asFloat().get());
        }
        trackHomogeneousType(value);
        array.add(index, (FieldValueImpl)value);
        return this;
    }

    private ArrayValue setScalar(int index, FieldValue value) {
        assert ((FieldDefImpl)value.getDefinition()).isSubtype(getElementDef());
        /* turn float to double */
        if (value.isFloat() && getElementDef().isJson()) {
            value = FieldDefImpl.Constants.doubleDef.createDouble(
                value.asFloat().get());
        }
        trackHomogeneousType(value);
        array.set(index, (FieldValueImpl)value);
        return this;
    }

    public List<FieldValueImpl> getArrayInternal() {
        return array;
    }

    public FieldDefImpl getElementDef() {
        return ((ArrayDefImpl)fieldDef).getElement();
    }

    FieldDefImpl getHomogeneousType() {
        return homogeneousType;
    }

    void setHomogeneousType(FieldDefImpl def) {
        homogeneousType = def;
    }

    boolean isHomogeneous() {
        return homogeneousType != null;
    }

    public void addInternal(FieldValueImpl value) {
        array.add(value);
    }

    /**
     * This is used by index deserialization.  The format for enums is an
     * integer.
     */
    ArrayValue addEnum(int value) {
        add(((EnumDefImpl)getElementDef()).createEnum(value));
        return this;
    }

    /**
     * This method tracks the type of the elements in the array handling
     * transitions to/from wildcard types.
     */
    private void trackHomogeneousType(FieldValue value) {

        FieldDefImpl elemDef = getElementDef();

        if (!elemDef.isWildcard()) {
            return;
        }

        FieldDefImpl valDef = (FieldDefImpl)value.getDefinition();

        if (size() == 0) {
            /*
             * transition from empty wildcard array to homogenous wildcard
             * array.
             */
            assert(homogeneousType == null);

            if (valDef.isAtomic() && !valDef.isWildcard()) {
                homogeneousType = valDef;
            }

        } else if (homogeneousType != null &&
                   homogeneousType.getType() != valDef.getType()) {
            /* transition from homogenous wildcard to heterogenous wildcard */
            homogeneousType = null;
        }
    }

    private void clearHomogeneousType() {
        homogeneousType = null;
    }

    @Override
    public ArrayValueSerializer asArrayValueSerializer() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<FieldValueSerializer> iterator() {
        final List<?> values = toList();
        return (Iterator<FieldValueSerializer>)values.iterator();
    }
}
