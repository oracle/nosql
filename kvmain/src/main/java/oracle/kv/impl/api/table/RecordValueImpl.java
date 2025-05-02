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

import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.RecordValueSerializer;
import oracle.kv.impl.util.SizeOf;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.core.JsonToken;

/**
 * RecordValueImpl implements RecordValue and is a multi-valued object that
 * contains a map of string names to fields.  The field values may be simple or
 * complex and allowed fields are defined by the FieldDef definition of the
 * record.
 */
public class RecordValueImpl extends ComplexValueImpl
    implements RecordValue, RecordValueSerializer {

    private static final long serialVersionUID = 1L;

    /*
     * valueMap is not used anymore, but it is still here to support
     * java-based deserialization from earlier versions
     */
    @Deprecated
    protected Map<String, FieldValue> valueMap;

    private int size;

    private FieldValue[] values;

    RecordValueImpl(RecordDef def) {
        super(def);
        size = 0;
        valueMap = null;
        values = new FieldValue[getDefinition().getNumFields()];
    }

    public RecordValueImpl(RecordValueImpl other) {
        super(other.getDefinition());
        valueMap = null;
        size = other.size;
        values = new FieldValue[getDefinition().getNumFields()];

        for (int pos = 0; pos < other.values.length; ++pos) {
            values[pos] = other.values[pos];
        }
    }

    public RecordValueImpl() {
        super(null);
        size = 0;
        values = null;
        valueMap = null;
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.RECORD_VALUE;
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

    private synchronized void writeObject(java.io.ObjectOutputStream out)
        throws IOException {
        int numFields = values.length;

        valueMap =
            new TreeMap<String, FieldValue>(FieldComparator.instance);

        for (int i = 0; i < numFields; ++i) {
            if (values[i] != null) {
                valueMap.put(getFieldName(i), values[i]);
            }
        }

        out.defaultWriteObject();
        valueMap = null;
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {

         in.defaultReadObject();
         convertToNewFormat();
    }

    private void convertToNewFormat() {

        int numFields = getDefinition().getNumFields();

        values = new FieldValue[numFields];
        size = 0;

        for (int i = 0; i < numFields; ++i) {
            String fname = getFieldName(i);
            FieldValue fvalue = valueMap.get(fname);
            values[i] = fvalue;
            if (fvalue != null) {
                ++size;
            }
        }

        valueMap = null;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public RecordValueImpl clone() {
        return new RecordValueImpl(this);
    }

    @Override
    public long sizeof() {
        long sz = super.sizeof();
        sz += (2 * SizeOf.OBJECT_REF_OVERHEAD + 4 +
               SizeOf.objectArraySize(values.length));
        for (FieldValue fval : values) {
            sz += ((FieldValueImpl)fval).sizeof();
        }
        return sz;
    }

    @Override
    public int hashCode() {
        int code = size;
        for (int i = 0; i < values.length; ++i) {
            if (values[i] == null) {
                continue;
            }
            code += (values[i].hashCode() + getFieldName(i).hashCode());
        }
        return code;
    }

    @Override
    public boolean equals(Object other) {

        /* maybe avoid some work */
        if (this == other) {
            return true;
        }
        if (!(other instanceof RecordValueImpl)) {
            return false;
        }

        RecordValueImpl otherValue = (RecordValueImpl) other;

        /*
         * field-by-field comparison
         */
        if (size() != otherValue.size() ||
            !getDefinition().equals(otherValue.getDefinition())) {
            return false;
        }

        for (int i = 0; i < values.length; ++i) {

            if (values[i] == null) {
                if (otherValue.values[i] == null) {
                    continue;
                }
                return false;
            }

            if (otherValue.values[i] == null) {
                return false;
            }

            if (!values[i].equals(otherValue.values[i])) {
                return false;
            }
        }

        return true;
    }

    /**
     * FieldDef must match for both objects.
     */
    @Override
    public int compareTo(FieldValue other) {

        if (!(other instanceof RecordValueImpl)) {
            throw new ClassCastException("Object is not an RecordValue");
        }

        RecordValueImpl otherImpl = (RecordValueImpl) other;

        if (!getDefinition().equals(otherImpl.getDefinition())) {
            throw new IllegalArgumentException(
                "Cannot compare RecordValues with different definitions");
        }

        for (int i = 0; i < values.length; ++i) {
            int ret = compareFieldValues(values[i], otherImpl.values[i]);
            if (ret != 0) {
                return ret;
            }
        }

        /* they must be equal */
        return 0;
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.RECORD;
    }

    @Override
    public RecordDefImpl getDefinition() {
        return (RecordDefImpl)fieldDef;
    }

    @Override
    public boolean isRecord() {
        return true;
    }

    @Override
    public RecordValue asRecord() {
        return this;
    }

    /*
     * Public api methods from RecordValue
     */

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Deprecated
    @Override
    public List<String> getFields() {
        return getFieldNames();
    }

    @Override
    public List<String> getFieldNames() {
        return getDefinition().getFieldNames();
    }

    public List<String> getFieldNamesInternal() {
        return getDefinition().getFieldNamesInternal();
    }

    @Override
    public String getFieldName(int pos) {
        return getDefinition().getFieldName(pos);
    }

    @Override
    public int getFieldPos(String fieldName) {
        return getDefinition().getFieldPos(fieldName);
    }

    @Override
    public FieldValueImpl get(String fieldName) {
        int pos = getDefinition().getFieldPos(fieldName);
        return (FieldValueImpl)values[pos];
    }

    @Override
    public FieldValueImpl get(int pos) {
        return (FieldValueImpl)values[pos];
    }

    @Override
    public FieldValueImpl getElement(int pos) {
        return (FieldValueImpl)values[pos];
    }

    @Override
    public boolean contains(String fieldName) {
        int pos = getFieldPos(fieldName);
        return values[pos] != null;
    }

    @Override
    public boolean contains(int pos) {
        return values[pos] != null;
    }

    @Override
    public void clear() {
        for (int i = 0; i < values.length; ++i) {
            values[i] = null;
        }
        size = 0;
    }

    @Override
    public FieldValue remove(String name) {

        int pos = getFieldPos(name);
        FieldValue val = values[pos];
        values[pos] = null;
        if (val != null) {
            --size;
        }
        return val;
    }

    @Override
    public void copyFrom(RecordValue source) {
        copyFrom(source, false/*ignore definition*/);
    }

    /**
     * Put methods.  All of these silently overwrite any existing state by
     * creating/using new FieldValue objects.  These methods return "this"
     * in order to support chaining of operations.
     */

    @Override
    public RecordValue put(int pos, FieldValue value) {

        if (value.isNull()) {
            return putNull(pos);
        }

        if (value.isJsonNull()) {
            return putJsonNull(pos);
        }

        if (((FieldValueImpl)value).isEMPTY()) {
            return putEMPTY(pos);
        }

        value = validateValueType(pos, value);
        putInternal(pos, value);
        return this;
    }

    @Override
    public RecordValue put(String name, FieldValue value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public RecordValue put(String name, int value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public RecordValue put(int pos, int value) {
        putInternal(pos, getFieldDef(pos).createInteger(value));
        return this;
    }

    @Override
    public RecordValue put(String name, long value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public RecordValue put(int pos, long value) {
        putInternal(pos, getFieldDef(pos).createLong(value));
        return this;
    }

    @Override
    public RecordValue put(String name, String value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public RecordValue put(int pos, String value) {
        putInternal(pos, getFieldDef(pos).createString(value));
        return this;
    }

    @Override
    public RecordValue put(String name, double value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public RecordValue put(int pos, double value) {
        putInternal(pos, getFieldDef(pos).createDouble(value));
        return this;
    }

    @Override
    public RecordValue putJson(String name, String jsonInput) {

        int pos = getFieldPos(name);
        return putJson(pos, jsonInput);
    }

    @Override
    public RecordValue putJson(String name, Reader jsonReader) {
        int pos = getFieldPos(name);
        return putJson(pos, jsonReader);
    }

    @Override
    public RecordValue putJson(int pos, String jsonInput) {
        Reader reader = new StringReader(jsonInput);
        try {
            return putJson(pos, reader);
        } finally {
            try { reader.close(); } catch (IOException ioe) {}
        }
    }

    @Override
    public RecordValue putJson(int pos, Reader jsonReader) {

        FieldDefImpl def = getFieldDef(pos);
        if (!def.isJson()) {
            throw new IllegalArgumentException(
                "putJson: field at position " + pos + "is not of type JSON");
        }

        putInternal(pos, JsonDefImpl.createFromReader(jsonReader));
        return this;
    }

    @Override
    public RecordValue put(String name, float value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public RecordValue put(int pos, float value) {
        putInternal(pos, getFieldDef(pos).createFloat(value));
        return this;
    }

    @Override
    public RecordValue put(String name, boolean value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public RecordValue put(int pos, boolean value) {
        putInternal(pos, getFieldDef(pos).createBoolean(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, int value) {
        int pos = getFieldPos(name);
        return putNumber(pos, value);
    }

    @Override
    public RecordValue putNumber(int pos, int value) {
        putInternal(pos, getFieldDef(pos).createNumber(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, long value) {
        int pos = getFieldPos(name);
        return putNumber(pos, value);
    }

    @Override
    public RecordValue putNumber(int pos, long value) {
        putInternal(pos, getFieldDef(pos).createNumber(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, float value) {
        int pos = getFieldPos(name);
        return putNumber(pos, value);
    }

    @Override
    public RecordValue putNumber(int pos, float value) {
        putInternal(pos, getFieldDef(pos).createNumber(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, double value) {
        int pos = getFieldPos(name);
        return putNumber(pos, value);
    }

    @Override
    public RecordValue putNumber(int pos, double value) {
        putInternal(pos, getFieldDef(pos).createNumber(value));
        return this;
    }

    @Override
    public RecordValue putNumber(String name, BigDecimal value) {
        int pos = getFieldPos(name);
        return putNumber(pos, value);
    }

    @Override
    public RecordValue putNumber(int pos, BigDecimal value) {
        putInternal(pos, getFieldDef(pos).createNumber(value));
        return this;
    }

    @Override
    public RecordValue put(String name, byte[] value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public RecordValue put(int pos, byte[] value) {
        putInternal(pos, getFieldDef(pos).createBinary(value));
        return this;
    }

    @Override
    public RecordValue putFixed(String name, byte[] value) {
        int pos = getFieldPos(name);
        return putFixed(pos, value);
    }

    @Override
    public RecordValue putFixed(int pos, byte[] value) {
        putInternal(pos, getFieldDef(pos).createFixedBinary(value));
        return this;
    }

    @Override
    public RecordValue putEnum(String name, String value) {
        int pos = getFieldPos(name);
        return putEnum(pos, value);
    }

    @Override
    public RecordValue putEnum(int pos, String value) {
        EnumDefImpl fdef = (EnumDefImpl)getFieldDef(pos);
        putInternal(pos, fdef.createEnum(value));
        return this;
    }

    @Override
    public RecordValue put(String name, Timestamp value) {
        int pos = getFieldPos(name);
        return put(pos, value);
    }

    @Override
    public RecordValue put(int pos, Timestamp value) {
        TimestampDefImpl fdef = (TimestampDefImpl)getFieldDef(pos);
        putInternal(pos, fdef.createTimestamp(value));
        return this;
    }

    @Override
    public RecordValue putNull(String name) {
        int pos = getFieldPos(name);
        return putNull(pos);
    }

    @Override
    public RecordValue putJsonNull(String name) {
        int pos = getFieldPos(name);
        return putJsonNull(pos);
    }

    @Override
    public RecordValue putNull(int pos) {

        if (!getDefinition().isNullable(pos)) {
            String fname = getFieldName(pos);
            throw new IllegalArgumentException(
                "Field \"" + fname + "\" is not nullable");
        }

        putInternal(pos, NullValueImpl.getInstance());
        return this;
    }

    @Override
    public RecordValue putJsonNull(int pos) {

        FieldDefImpl fdef = getDefinition().getFieldDef(pos);

        if (!fdef.isJson() && !fdef.isJsonAtomic() &&
            !fdef.isAny() && !fdef.isAnyAtomic()) {
            String fname = getFieldName(pos);
            throw new IllegalArgumentException(
                "Field \"" + fname + "\" is not JSON. It's type is:\n" +
                getDefinition().getFieldDef(pos).getDDLString());
        }
        putInternal(pos, NullJsonValueImpl.getInstance());
        return this;
    }

    public RecordValue putEMPTY(int pos) {

        putInternal(pos, EmptyValueImpl.getInstance());
        return this;
    }

    @Override
    public RecordValueImpl putRecord(String name) {
        int pos = getFieldPos(name);
        return putRecord(pos);
    }

    @Override
    public RecordValueImpl putRecord(int pos) {
        RecordValue val = getFieldDef(pos).createRecord();
        putInternal(pos, val);
        return (RecordValueImpl)val;
    }

    @Override
    public RecordValue putRecord(String name, Map<String, ?> map) {
        int pos = getFieldPos(name);
        return putRecord(pos, map);
    }

    @Override
    public RecordValue putRecord(int pos, Map<String, ?> map) {

        validateValueKind(pos, FieldDef.Type.RECORD);
        RecordValue val;
        try {
            val = RecordValueImpl.fromJavaObjectValue(getFieldDef(pos), map);
        } catch (ClassCastException cce) {
            String fname = getFieldName(pos);
            throw new IllegalArgumentException(
                "Cannot set field \"" + fname + "\" to the given java map " +
                "object, because the java map doesn't match the type " +
                "definition of the field.", cce);
        }
        putInternal(pos, val);
        return this;
    }

    @Override
    public RecordValue putRecordAsJson(
        String name,
        String jsonInput,
        boolean exact) {

        return putRecordAsJson(
            name, new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public RecordValue putRecordAsJson(
        String name,
        InputStream jsonInput,
        boolean exact) {

        int pos = getFieldPos(name);
        return putRecordAsJson(pos, jsonInput, exact);
    }

    @Override
    public RecordValue putRecordAsJson(
        int pos,
        String jsonInput,
        boolean exact) {

        return putRecordAsJson(
            pos, new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public RecordValue putRecordAsJson(
        int pos,
        InputStream jsonInput,
        boolean exact) {

        RecordValue record = getFieldDef(pos).createRecord();

        ComplexValueImpl.createFromJson(
            (RecordValueImpl)record, jsonInput, exact);

        putInternal(pos, record);
        return this;
    }

    @Override
    public ArrayValueImpl putArray(String name) {
        int pos = getFieldPos(name);
        return putArray(pos);
    }

    @Override
    public ArrayValueImpl putArray(int pos) {
        ArrayValue val = getFieldDef(pos).createArray();
        putInternal(pos, val);
        return (ArrayValueImpl)val;
    }

    @Override
    public RecordValue putArray(String name, Iterable<?> list) {
        int pos = getFieldPos(name);
        return putArray(pos, list);
    }

    @Override
    public RecordValue putArray(int pos, Iterable<?> list) {

        validateValueKind(pos, FieldDef.Type.ARRAY);
        ArrayValue val;
        try {
            val = ArrayValueImpl.fromJavaObjectValue(getFieldDef(pos), list);
        } catch (ClassCastException cce) {
            String fname = getFieldName(pos);
            throw new IllegalArgumentException(
                "Cannot set field \"" + fname + "\" to the given java list " +
                "object, because the java list doesn't match the type " +
                "definition of the field.", cce);
        }
        putInternal(pos, val);
        return this;
    }

    @Override
    public RecordValue putArray(String name, Object[] array) {
        int pos = getFieldPos(name);
        return putArray(pos, array);
    }

    @Override
    public RecordValue putArray(int pos, Object[] array) {

        validateValueKind(pos, FieldDef.Type.ARRAY);
        ArrayValue val;
        try {
            val = ArrayValueImpl.fromJavaObjectValue(getFieldDef(pos), array);
        } catch (ClassCastException cce) {
            String fname = getFieldName(pos);
            throw new IllegalArgumentException(
                "Cannot set field \"" + fname + "\" to the given java array " +
                "object, because the java array doesn't match the type " +
                "definition of the field.", cce);
        }
        putInternal(pos, val);
        return this;
    }

    @Override
    public RecordValue putArrayAsJson(
        String name,
        String jsonInput,
        boolean exact) {

        return putArrayAsJson(
            name, new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public RecordValue putArrayAsJson(
        String name,
        InputStream jsonInput,
        boolean exact) {

        int pos = getFieldPos(name);
        return putArrayAsJson(pos, jsonInput, exact);
    }

    @Override
    public RecordValue putArrayAsJson(
        int pos,
        String jsonInput,
        boolean exact) {

        return putArrayAsJson(
            pos, new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public RecordValue putArrayAsJson(
        int pos,
        InputStream jsonInput,
        boolean exact) {

        ArrayValue array = getFieldDef(pos).createArray();

        ComplexValueImpl.createFromJson(
            (ArrayValueImpl)array, jsonInput, exact);

        putInternal(pos, array);
        return this;
    }

    @Override
    public MapValueImpl putMap(String name) {
        int pos = getFieldPos(name);
        return putMap(pos);
    }

    @Override
    public MapValueImpl putMap(int pos) {
        MapValue val = getFieldDef(pos).createMap();
        putInternal(pos, val);
        return (MapValueImpl)val;
    }

    @Override
    public RecordValue putMap(String name, Map<String, ?> map) {
        int pos = getFieldPos(name);
        return putMap(pos, map);
    }

    @Override
    public RecordValue putMap(int pos, Map<String, ?> map) {

        validateValueKind(pos, FieldDef.Type.MAP);
        MapValue val;
        try {
            val = MapValueImpl.fromJavaObjectValue(getFieldDef(pos), map);
        } catch (ClassCastException cce) {
            String fname = getFieldName(pos);
            throw new IllegalArgumentException(
                "Cannot set field \"" + fname + "\" to the given java map " +
                "object, because the java map doesn't match the type " +
                "definition of the field.", cce);
        }
        putInternal(pos, val);
        return this;
    }

    @Override
    public RecordValue putMapAsJson(
        String name,
        String jsonInput,
        boolean exact) {

        return putMapAsJson(
            name, new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public RecordValue putMapAsJson(
        String name,
        InputStream jsonInput,
        boolean exact) {

        int pos = getFieldPos(name);
        return putMapAsJson(pos, jsonInput, exact);
    }

    @Override
    public RecordValue putMapAsJson(
        int pos,
        String jsonInput,
        boolean exact) {

        return putMapAsJson(
            pos, new ByteArrayInputStream(jsonInput.getBytes()), exact);
    }

    @Override
    public RecordValue putMapAsJson(
        int pos,
        InputStream jsonInput,
        boolean exact) {

        MapValue map = getFieldDef(pos).createMap();

        ComplexValueImpl.createFromJson((MapValueImpl)map, jsonInput, exact);

        putInternal(pos, map);
        return this;
    }

    /*
     * ComplexValueImpl internal methods
     */


    /**
     * Add matching JSON fields to the record. For a regular table with
     * schema, for each named field do this:
     * 1.  find it in the record definition
     * 2.  if present, add it to the value
     * 3.  if not present ignore, unless exact is true, in which case throw.
     *
     * If exact is true then the input state must match that of the record
     * exactly or IllegalArgumentException is thrown.
     *
     * If exact is false and the JSON input is an empty JSON object (e.g. {})
     * no fields are added and there is no exception thrown.
     *
     * If this is an instance of JsonCollectionRowImpl:
     * 1. add all fields found
     * 2. if exact is true, make sure that all primary key fields are
     * present
     */
    @Override
    public void addJsonFields(
        JsonParser jp,
        String currentFieldName,
        boolean exact,
        boolean addMissingFields) {

        int numFields = 0;

        try {
            JsonToken t = jp.getCurrentToken();

            JsonLocation location = jp.currentLocation();

            if (t != JsonToken.START_OBJECT) {
                jsonParseException(("Expected { token to start object, instead "
                                    + "found " + t), location);
            }

            while ((t = jp.nextToken()) != JsonToken.END_OBJECT) {

                if (t == null || t == JsonToken.END_ARRAY) {
                    jsonParseException("Did not find end of object", location);
                }

                String fieldName = jp.currentName();

                if (fieldName == null) {
                    // Should this be treated as an error ????
                    continue;
                }

                int pos = -1;

                try {
                    pos = getFieldPos(fieldName);
                } catch (IllegalArgumentException e) {

                    if (!(this instanceof JsonCollectionRowImpl)) {

                        if (exact) {
                            throw new IllegalArgumentException(
                                "Unexpected field \"" + fieldName +
                                "\" in JSON input. There is no corresponding " +
                                "field in record type definition");
                        }
                        /*
                         * An exact match is not required.  Consume the token.
                         * If it is a nested JSON Object or Array, skip it entirely.
                         */
                        JsonToken token = jp.nextToken();
                        if (token == JsonToken.START_OBJECT) {
                            skipToJsonToken(jp, JsonToken.END_OBJECT);
                        } else if (token == JsonToken.START_ARRAY) {
                            skipToJsonToken(jp, JsonToken.END_ARRAY);
                        }
                        continue;
                    }
                }

                JsonToken token = jp.nextToken();

                FieldDef fdef = (pos >= 0 ?
                                 getFieldDef(pos) :
                                 FieldDefImpl.Constants.jsonDef);

                /*
                 * Handle null.
                 */
                if (token == JsonToken.VALUE_NULL && !fdef.isJson()) {
                    if (getDefinition().isNullable(pos)) {
                        putNull(pos);
                        ++numFields;
                        continue;
                    }
                    throw new IllegalArgumentException(
                        "Invalid null value in JSON input for field "
                        + fieldName);
                }

                switch (fdef.getType()) {
                case INTEGER:
                    checkNumberType(fieldName, NumberType.INT, jp);
                    int intValue = jp.getIntValue();
                    if (!fdef.isMRCounter()) {
                        put(pos, intValue);
                    } else {
                        /*
                         * Cannot convert the json to a CRDT value.
                         * Put the default value instead. */
                        putInternal(pos,
                                    getFieldMapEntry(fieldName).
                                        getDefaultValue(),
                                    false);
                    }

                    break;
                case LONG:
                    checkNumberType(fieldName, NumberType.LONG, jp);
                    long longValue = jp.getLongValue();
                    if (!fdef.isMRCounter()) {
                        put(pos, longValue);
                    } else {
                        /*
                         * Cannot convert the json to a CRDT value.
                         * Put the default value instead. */
                        putInternal(pos,
                                    getFieldMapEntry(fieldName).
                                        getDefaultValue(),
                                    false);
                    }
                    break;
                case DOUBLE:
                    checkNumberType(fieldName, NumberType.DOUBLE, jp);
                    put(pos, jp.getDoubleValue());
                    break;
                case FLOAT:
                    checkNumberType(fieldName, NumberType.FLOAT, jp);
                    put(pos, jp.getFloatValue());
                    break;
                case NUMBER:
                    checkNumberType(fieldName, NumberType.BIG_DECIMAL, jp);
                    BigDecimal numberValue =  jsonParserGetDecimalValue(jp);
                    if (!fdef.isMRCounter()) {
                        putNumber(fieldName, numberValue);
                    } else {
                        /*
                         * Cannot convert the json to a CRDT value.
                         * Put the default value instead. */
                        putInternal(pos,
                                    getFieldMapEntry(fieldName).
                                        getDefaultValue(),
                                    false);
                    }
                    break;
                case STRING:
                    checkType(fieldName, true, "STRING", jp);
                    put(pos, jp.getText());
                    break;
                case BINARY:
                    checkType(fieldName, true, "BINARY", jp);
                    put(pos, jp.getBinaryValue());
                    break;
                case FIXED_BINARY:
                    checkType(fieldName, true, "BINARY", jp);
                    putFixed(pos, jp.getBinaryValue());
                    break;
                case BOOLEAN:
                    checkType(fieldName, true, "BOOLEAN", jp);
                    put(pos, jp.getBooleanValue());
                    break;
                case TIMESTAMP:
                    checkType(fieldName, true, "TIMESTAMP", jp);
                    put(pos, fdef.asTimestamp().fromString(jp.getText()));
                    break;
                case ARRAY:
                    checkType(fieldName, false, "ARRAY", jp);
                    ArrayValueImpl array = putArray(pos);
                    array.addJsonFields(jp, fieldName, exact, addMissingFields);
                    break;
                case MAP:
                    checkType(fieldName, false, "MAP", jp);
                    MapValueImpl map = putMap(pos);
                    map.addJsonFields(jp, fieldName, exact, addMissingFields);
                    break;
                case RECORD:
                    checkType(fieldName, false, "RECORD", jp);
                    RecordValueImpl record = putRecord(pos);
                    record.addJsonFields(jp, fieldName, exact, addMissingFields);
                    break;
                case ENUM:
                    checkType(fieldName, true, "ENUM", jp);
                    putEnum(pos, jp.getText());
                    break;
                case JSON:
                    if (pos >= 0) {
                        put(pos, JsonDefImpl.createFromJson(jp, false));
                    } else {
                        put(fieldName, JsonDefImpl.createFromJson(jp, false));
                    }
                    break;
                case ANY_JSON_ATOMIC:
                    put(pos, JsonDefImpl.createFromJson(jp, false));
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
                        "A record field cannot have type " + fdef.getType());
                }
                ++numFields;
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                ("Failed to parse JSON input: " + ioe.getMessage()), ioe);
        } catch (RuntimeException re) {
            if (re instanceof IllegalArgumentException) {
                throw re;
            }
            throw new IllegalArgumentException(
                ("Failed to parse JSON input: " + re.toString()), re);
        }

        if (exact && (this instanceof JsonCollectionRowImpl)) {
            ((JsonCollectionRowImpl)this).ensurePrimaryKey();
        } else if (getNumFields() != numFields) {
            if (exact) {
                throw new IllegalArgumentException(
                    "Not enough fields for value in JSON input." +
                    "Found " + numFields + ", expected " + getNumFields());
            }
            if (addMissingFields) {
                addMissingFields();
            }
        }
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public void toStringBuilder(StringBuilder sb, DisplayFormatter formatter) {
        toStringBuilder(sb, formatter, values.length);
    }

    /*
     * Internal method that is abstracted to allow the subclass to limit
     * the number of fields used. This is only used by PrimaryKeyImpl to
     * print the shard key.
     */
    protected void toStringBuilder(StringBuilder sb,
                                   DisplayFormatter formatter,
                                   int numFields) {
        if (formatter == null) {
            throw new IllegalArgumentException(
                "DisplayFormatter must be non-null");
        }

        formatter.startObject();

        boolean wroteFirstField = false;
        sb.append('{');
        for (int i = 0; i < numFields; ++i) {
            String fieldName = getFieldName(i);
            FieldValueImpl val = (FieldValueImpl)values[i];
            if (val != null) {
                formatter.newPair(sb, wroteFirstField);
                sb.append('\"');
                sb.append(fieldName);
                sb.append('\"');
                formatter.separator(sb);
                val.toStringBuilder(sb, formatter);
                wroteFirstField = true;
            }
        }
        formatter.endObject(sb, numFields);
        sb.append('}');
    }

    @SuppressWarnings("unchecked")
    static RecordValueImpl fromJavaObjectValue(
        FieldDef definition,
        Object obj) {

        Map<String, Object> javaMap = (Map<String, Object>) obj;
        RecordValueImpl record = (RecordValueImpl)definition.createRecord();

        for (int i = 0; i < record.values.length; ++i) {
            String fieldName = record.getFieldName(i);
            Object o = javaMap.get(fieldName);
            if (o != null) {
                record.put(i, FieldValueImpl.
                           fromJavaObjectValue(record.getFieldDef(i), o));
            } else {
                record.put(i, record.getDefinition().getDefaultValue(i));
            }
        }
        return record;
    }


    /*
     * local methods
     */

    /**
     * Return the number of fields in this record.
     */
    public int getNumFields() {
        return values.length;
    }

    /**
     * Return the field definition for the named field. Null if the record
     * type does not include such a field.
     */
    public FieldDefImpl getFieldDef(String fieldName) {
        return getDefinition().getFieldDef(fieldName);
    }

    /**
     * Return the field definition for the field at the given position.
     * Null if the record type does not include such a field.
     */
    public FieldDefImpl getFieldDef(int pos) {
        return getDefinition().getFieldDef(pos);
    }

    FieldMapEntry getFieldMapEntry(String fieldName) {
        return getDefinition().getFieldMapEntry(fieldName, false/*mustExist*/);
    }

    /**
     * If any top-level fields of this record are missing, add them with
     * their default value.
     */
    public void addMissingFields() {
        addMissingFields(true);
    }

    void addMissingFields(boolean fromUser) {

        for (int i = 0; i < values.length; ++i) {

            FieldValue fv = values[i];

            if (fv == null) {

                fv = getDefinition().getDefaultValue(i);

                FieldDefImpl fdef = getDefinition().getFieldDef(i);
                if (fdef.hasJsonMRCounter()) {
                    /* Insert default json mr_counters. */
                    TableImpl table = ((RowImpl)this).getTable();
                    List<TablePath> paths = table.getSchemaMRCounterPaths(i);
                    fv = fdef.createMap();
                    putInternal(i, fv, false);
                    insertDefaultJsonMRCounter(paths, this);
                }

                if (fv.isNull() && !getDefinition().isNullable(i)) {
                    throw new IllegalArgumentException(
                        "The field \'" + getFieldName(i) + "\n cannot be NULL");
                }

                putInternal(i, fv, fromUser);
            }
        }
    }

    protected static void insertDefaultJsonMRCounter(List<TablePath> paths,
                                                     FieldValueImpl fieldVal) {
        for (TablePath path : paths) {
            Type cntType = path.getMRCounterType();
            FieldDefImpl cntDef = FieldDefImpl.getCRDTDef(cntType);
            FieldValueImpl cntVal = cntDef.createCRDTValue();
            JsonDefImpl.insertMRCounterField(fieldVal, path, cntVal, true);
        }
    }

    /*
     * Called from a sorting ReceiveIter
     */
    public void convertEmptyToNull() {

        for (int i = 0; i < size; ++i) {
            if (((FieldValueImpl)values[i]).isEMPTY()) {
                values[i] = NullValueImpl.getInstance();
            }
        }
    }

    /*
     * No validation done
     */
    void putInternal(String name, FieldValue value) {
        putInternal(name, value, true);
    }

    public void putInternal(String name, FieldValue value, boolean fromUser) {
        int pos = getFieldPos(name);
        putInternal(pos, value, fromUser);
    }

    /*
     * No validation done
     */
    public void putInternal(int pos, FieldValue value) {
        putInternal(pos, value, true);
    }

    /**
     * Sets the value into the record.
     *
     * @param pos Position of the value to be set into
     * @param value The value to be set
     * @param fromUser True if user sets the value using the API or false in
     *                 the case of identity when the value is set by default
     *                 from the sequence.
     */
    public void putInternal(int pos, FieldValue value, boolean fromUser) {

        assert(value != null);

        FieldDefImpl def = getFieldDef(pos);

        if (def.isMRCounter()) {
            if (fromUser) {
                throw new IllegalArgumentException(
                    "Cannot set value for a CRDT column.");
            }
            if (!def.equals(value.getDefinition())) {
                throw new IllegalArgumentException(
                    "Only allow a CRDT value at a CRDT column.");
            }
        }

        if (values[pos] == null) {
            ++size;
        }

        /* turn float to double */
        if (value.isFloat() && getFieldDef(pos).isJson()) {
            value = FieldDefImpl.Constants.doubleDef.createDouble(
                value.asFloat().get());
        }

        values[pos] = value;

        if (def.hasJsonMRCounter() && fromUser) {
            validateJsonMRCounterPaths(pos, value);
        }
    }

    void validateJsonMRCounterPaths(int pos, FieldValue value) {

        if (!value.isMap()) {
            throw new IllegalArgumentException(
                "Value assigned to top-level json column with MR_COUNTERs " +
                "is not a map");
        }

        TableImpl table = ((RowImpl)this).getTable();
        List<TablePath> paths = table.getSchemaMRCounterPaths(pos);
        FieldValueImpl counter;

        for (TablePath path : paths) {
            try {
                counter = evaluateScalarPath(path.getSteps(), 0);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "MR_COUNTER path " + path + " contains an array");
            }

            if (counter.isNull() || counter.isEMPTY()) {
                throw new IllegalArgumentException(
                    "MR_COUNTER path " + path + " does not exist");
            }

            if (!counter.isMRCounter() && !counter.isAtomic()) {
                throw new IllegalArgumentException(
                    "Found non atomic value at MR_COUNTER path " + path);
            }
        }
    }

    void removeInternal(int pos) {
        if (values[pos] == null) {
            return;
        }
        values[pos] = null;
        --size;
    }

    /**
     * Enum is serialized in indexes as an integer representing the value's
     * index in the enumeration declaration.  Deserialize here.
     */
    RecordValue putEnum(String name, int value) {
        int pos = getFieldPos(name);
        putInternal(pos, ((EnumDefImpl)getFieldDef(pos)).createEnum(value));
        return this;
    }

    /**
     * This is an internal method used by the copy constructor. It's
     * also used by test code. It does no validation on the fields on the
     * assumption that the source has already been validated.
     */
    void copyFields(RecordValueImpl from) {
        for (int pos = 0; pos < from.values.length; ++pos) {
            if (from.values[pos] == null) {
                continue;
            }
            /* this check is needed to support invocations from unit tests */
            if (pos >= values.length) {
                break;
            }

            putInternal(pos, from.values[pos], false);
        }
    }

    /**
     * Use of putField() bypasses unnecessary checking of field
     * name and type which is done implicitly by the definition check.
     */
    public void copyFrom(RecordValue source, boolean ignoreDefinition) {

        RecordValueImpl src = (RecordValueImpl)source;

        if (!ignoreDefinition) {

            if (!getDefinition().equals(src.getDefinition())) {
                throw new IllegalArgumentException(
                    "Types of source and destination RecordValues do not" +
                    "match.\nSource type:\n" + src.getDefinition() +
                    "\nDest type:\n" + getDefinition());
            }

            for (int i = 0; i < values.length; ++i) {
                if (src.values[i] != null) {
                    putInternal(i, src.values[i]);
                }
            }

        } else {
            for (FieldMapEntry fme : getDefinition().getFieldProperties()) {
                FieldValue val = src.get(fme.getFieldName());
                if (val != null) {
                    put(fme.getFieldName(), val);
                }
            }
        }
    }

    private FieldValue validateValueType(int pos, FieldValue value) {

        FieldDefImpl fdef = getFieldDef(pos);
        FieldDefImpl valDef = (FieldDefImpl) value.getDefinition();

        if (!valDef.isSubtype(fdef)) {
            String fname = getFieldName(pos);
            throw new IllegalArgumentException(
                "Type mismatch. Value of type\n" +
                valDef.getDDLString() +
                "\ncannot be used as the value of field " + fname +
                " with type\n" + fdef.getDDLString());
        }

        if (value.isComplex() &&
            (fdef.isJson() ||
             fdef.equals(FieldDefImpl.Constants.arrayJsonDef) ||
             fdef.equals(FieldDefImpl.Constants.mapJsonDef)) &&
            !valDef.equals(FieldDefImpl.Constants.arrayJsonDef) &&
            !(valDef.isMap() && valDef.asMap().getElement().isJson())) {
            String fname = getFieldName(pos);
            throw new IllegalArgumentException(
                "Type mismatch. Value of type\n" +
                valDef.getDDLString() +
                "\ncannot be used as the value of field " + fname +
                " with type\n" + fdef.getDDLString());
        }

        value = ((FieldValueImpl)value).castToSuperType(fdef);
        if (fdef.hasMin() || fdef.hasMax()) {
            validateRangeValue(fdef, value);
        }
        return value;
    }

    void validateValueKind(int pos, FieldDef.Type type) {

       FieldDefImpl fdef = getFieldDef(pos);

        if (fdef.getType() != type) {
            String fname = getFieldName(pos);
            throw new IllegalArgumentException(
                "Type mismatch. Value of kind\n" + type +
                "\ncannot be used as the value of field " + fname +
                " with type\n" + fdef.getDDLString());
        }
    }


    /**
     * This is shared by IndexKeyImpl and PrimaryKeyImpl to ensure that fields
     * present in the record are specified in order. That is, if a field "to the
     * right" in the index definition is set, all fields to its "left" must also
     * be present.
     */
    void validateIndexFields() {
        validIndexFields(this, getClassNameForError());
    }

    static void validIndexFields(RecordValueSerializer record,
                                 String classNameForError) {
        int numFound = 0;

        RecordDef recordDef = record.getDefinition();
        for (int i = 0; i < recordDef.getNumFields(); i++) {

            FieldValueSerializer val = record.get(i);

            if (val != null) {
                if (i != numFound) {
                    throw new IllegalArgumentException
                        (classNameForError +
                         " is missing fields more significant than" +
                         " field: " + recordDef.getFieldName(i));
                }

                if (val.isNull() && !recordDef.isNullable(i)) {
                    throw new IllegalArgumentException
                        ("Field value is null, which is invalid for " +
                         classNameForError + ": " + recordDef.getFieldName(i));
                }
                ++numFound;
            }
        }
    }

    /**
     * This method is called for non-numeric types to do type validation.
     * If the token is numeric or if the token is scalar and shouldn't be,
     * or the token is not scalar and should be, an exception is thrown.
     *
     * @param mustBeScalar indicates whether the type must be a scalar
     * (e.g. string, enum, binary).  If called on a map, array, or record
     * type this will be false, and will catch conditions where a scalar
     * value is incorrectly supplied.
     *
     * Validation of specific type is not done.  In this path mismatched
     * types will be caught later (e.g. enum vs string or map vs record).
     */
    private static void checkType(
        String fieldName,
        boolean mustBeScalar,
        String type,
        JsonParser jp)
        throws IOException {

        JsonToken tok = jp.getCurrentToken();
        /*
         * This method is not called for numeric values.  Prevent cast from
         * a number to another type, e.g. String
         */
        if (tok.isScalarValue() != mustBeScalar || tok.isNumeric()) {
            throw new IllegalArgumentException(
                "Illegal value for field " + fieldName +
                 ": " + jp.getText() + ". Expected " + type);
        }
    }

    protected String getClassNameForError() {
        return "RecordValue";
    }

    @Override
    public RecordValueSerializer asRecordValueSerializer() {
        return this;
    }
}
