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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.MapValueSerializer;
import oracle.kv.impl.util.SizeOf;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.CharTypes;

/**
 * MapValueImpl implements the MapValue interface and is a container object
 * that holds a map of FieldValue objects all of the same type.  The getters
 * and setters use the same semantics as Java Map.
 *
 * TODO: JSON: if duplicate values are to be handled in some cases one option is
 * to do this:
 *  1. detect the duplicates on put by looking at the return value. If not null,
 * save duplicates in a standalone list attached to this object, to be
 * serialized separately.
 *  2. on toString() output, or any output that might want to see all dups,
 * output the dups first (because last-in wins).
 *  3. normal map operations work on the actual copies and will not create dups.
 */
public class MapValueImpl extends ComplexValueImpl
    implements MapValue, MapValueSerializer {

    private static final long serialVersionUID = 1L;

    protected final Map<String, FieldValue> fields = new TreeMap<>();

    MapValueImpl(MapDef def) {
        super(def);
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.MAP_VALUE;
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
    public MapValueImpl clone() {
        MapValueImpl map = new MapValueImpl(getDefinition());
        for (Map.Entry<String, FieldValue> entry : fields.entrySet()) {
            map.put(entry.getKey(), entry.getValue().clone());
        }
        return map;
    }

    @Override
    public long sizeof() {
        long size = super.sizeof();
        size += (SizeOf.OBJECT_REF_OVERHEAD + SizeOf.TREEMAP_OVERHEAD);
        for (Map.Entry<String, FieldValue> entry : fields.entrySet()) {
            size += SizeOf.TREEMAP_ENTRY_OVERHEAD;
            size += SizeOf.stringSize(entry.getKey());
            size += ((FieldValueImpl)entry.getValue()).sizeof();
        }
        return size;
    }

    @Override
    public int hashCode() {
        int code = size();
        for (Map.Entry<String, FieldValue> entry : fields.entrySet()) {
            code += entry.getKey().hashCode() + entry.getValue().hashCode();
        }
        return code;
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof MapValueImpl) {
            MapValueImpl otherValue = (MapValueImpl) other;
            /* maybe avoid some work */
            if (this == otherValue) {
                return true;
            }
            /*
             * detailed comparison
             */
            if (size() == otherValue.size() &&
                getElementDef().equals(otherValue.getElementDef()) &&
                getDefinition().equals(otherValue.getDefinition())) {

                for (Map.Entry<String, FieldValue> entry : fields.entrySet()) {
                    if (!entry.getValue().
                        equals(otherValue.get(entry.getKey()))) {
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
     * Compare field values in order of keys.  The algorithm relies on the fact
     * that fields is a SortedMap (TreeMap).  Return as soon as there is a
     * difference. If this object has a field the other does not, return &gt;
     * 0.  If this object is missing a field the other has, return &lt; 0.
     * Compare both keys and values, keys first.
     */
    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof MapValueImpl) {
            MapValueImpl otherImpl = (MapValueImpl) other;

            if (!getDefinition().equals(otherImpl.getDefinition())) {
                throw new IllegalArgumentException
                    ("Cannot compare MapValues with different definitions");
            }

            /* this relies on the maps being sorted */
            assert fields instanceof TreeMap;
            assert otherImpl.fields instanceof TreeMap;

            Iterator<String> keyIter = fields.keySet().iterator();
            Iterator<String> otherIter = otherImpl.fields.keySet().iterator();

            while (keyIter.hasNext() && otherIter.hasNext()) {
                String key = keyIter.next();
                String otherKey = otherIter.next();
                int keyCompare = key.compareTo(otherKey);
                if (keyCompare != 0) {
                    return keyCompare;
                }
                /*
                 * Keys are equal, values must exist.
                 */
                FieldValue val = fields.get(key);
                FieldValue otherVal = otherImpl.fields.get(key);
                int valCompare = val.compareTo(otherVal);
                if (valCompare != 0) {
                    return valCompare;
                }
            }

            /*
             * The object with more keys is greater, otherwise they are equal.
             */
            if (keyIter.hasNext()) {
                return 1;
            } else if (otherIter.hasNext()) {
                return -1;
            }
            return 0;
        }
        throw new ClassCastException
            ("Object is not a MapValue");
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.MAP;
    }

    @Override
    public boolean isMap() {
        return true;
    }

    @Override
    public MapValue asMap() {
        return this;
    }

    /*
     * Public api methods from MapValue
     */

    @Override
    public MapDefImpl getDefinition() {
        return (MapDefImpl)fieldDef;
    }

    @Override
    public int size() {
        return fields.size();
    }

    @Override
    public Map<String, FieldValue> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    @Override
    public FieldValue remove(String fieldName) {
        return fields.remove(fieldName);
    }

    @Override
    public FieldValueImpl get(String fieldName) {
        return (FieldValueImpl)fields.get(fieldName);
    }

    @Override
    public MapValue put(String name, int value) {
        putScalar(name, getElementDef().createInteger(value));
        return this;
    }

    @Override
    public MapValue put(String name, long value) {
        putScalar(name, getElementDef().createLong(value));
        return this;
    }

    @Override
    public MapValue put(String name, String value) {
        putScalar(name, getElementDef().createString(value));
        return this;
    }

    @Override
    public MapValue put(String name, double value) {
        putScalar(name, getElementDef().createDouble(value));
        return this;
    }

    @Override
    public MapValue put(String name, float value) {
        putScalar(name, getElementDef().createFloat(value));
        return this;
    }

    @Override
    public MapValue putNumber(String name, int value) {
        putScalar(name, getElementDef().createNumber(value));
        return this;
    }


    @Override
    public MapValue putNumber(String name, long value) {
        putScalar(name, getElementDef().createNumber(value));
        return this;
    }


    @Override
    public MapValue putNumber(String name, float value) {
        putScalar(name, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public MapValue putNumber(String name, double value) {
        putScalar(name, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public MapValue putNumber(String name, BigDecimal value) {
        putScalar(name, getElementDef().createNumber(value));
        return this;
    }

    @Override
    public MapValue put(String name, boolean value) {
        putScalar(name, getElementDef().createBoolean(value));
        return this;
    }

    @Override
    public MapValue put(String name, byte[] value) {
        putScalar(name, getElementDef().createBinary(value));
        return this;
    }

    @Override
    public MapValue putJsonNull(String name) {
        if (!getElementDef().isJson()) {
            throw new IllegalArgumentException(
                "Cannot insert a JSON null into a non-JSON map");
        }
        fields.put(name, NullJsonValueImpl.getInstance());
        return this;
    }

    /*
     * This is only used internally for queries involving indexes on map keys
     */
    MapValue putNull(String name) {
        fields.put(name, NullValueImpl.getInstance());
        return this;
    }

    @Override
    public MapValue putFixed(String name, byte[] value) {
        putScalar(name, getElementDef().createFixedBinary(value));
        return this;
    }

    @Override
    public MapValue putEnum(String name, String value) {
        putScalar(name, getElementDef().createEnum(value));
        return this;
    }

    @Override
    public MapValue put(String name, Timestamp value) {
        putScalar(name, getElementDef().createTimestamp(value));
        return this;
    }

    @Override
    public MapValue put(String fieldName, FieldValue value) {
        value = validate(value, getElementDef());
        fields.put(fieldName, value);
        return this;
    }

    @Override
    public RecordValueImpl putRecord(String fieldName) {
        RecordValue val = getElementDef().createRecord();
        fields.put(fieldName, val);
        return (RecordValueImpl) val;
    }

    @Override
    public MapValueImpl putMap(String fieldName) {
        MapValue val = getElementDef().createMap();
        fields.put(fieldName, val);
        return (MapValueImpl) val;
    }

    @Override
    public ArrayValueImpl putArray(String fieldName) {
        ArrayValue val = getElementDef().createArray();
        fields.put(fieldName, val);
        return (ArrayValueImpl) val;
    }

    @Override
    public MapValue putJson(String fieldName,
                            String jsonInput) {
        Reader reader = new StringReader(jsonInput);
        try {
            return putJson(fieldName, reader);
        } finally {
            try { reader.close(); } catch (IOException ioe) {}
        }
    }

    @Override
    public MapValue putJson(String fieldName,
                            Reader jsonReader) {
        put(fieldName, JsonDefImpl.createFromReader(jsonReader));
        return this;
    }

    /*
     * ComplexValueImpl internal api methods
     */

    @Override
    public Map<String, FieldValue> getMap() {
        return fields;
    }

    /**
     * Add JSON fields to the map.
     */
    @Override
    public void addJsonFields(
        JsonParser jp,
        String currentFieldName,
        boolean exact,
        boolean addMissingFields) {

        try {
            FieldDef element = getElementDef();

            JsonToken t = jp.getCurrentToken();

            JsonLocation location = jp.currentLocation();

            if (t != JsonToken.START_OBJECT) {
                jsonParseException(("Expected { token to start map, instead "
                                    + "found " + t), location);
            }

            while ((t = jp.nextToken()) != JsonToken.END_OBJECT) {

                if (t == null || t == JsonToken.END_ARRAY) {
                    jsonParseException("Did not find end of object", location);
                }

                String fieldname = jp.currentName();
                JsonToken token = jp.nextToken();

                /*
                 * A json null is valid only if the element type of
                 * the map is JSON.
                 */
                if (token == JsonToken.VALUE_NULL && !element.isJson()) {
                    throw new IllegalArgumentException
                        ("Invalid null value in JSON input for field "
                         + fieldname);
                }

                switch (element.getType()) {
                case INTEGER:
                    checkNumberType(fieldname, NumberType.INT, jp);
                    put(fieldname, jp.getIntValue());
                    break;
                case LONG:
                    checkNumberType(fieldname, NumberType.LONG, jp);
                    put(fieldname, jp.getLongValue());
                    break;
                case DOUBLE:
                    checkNumberType(fieldname, NumberType.DOUBLE, jp);
                    put(fieldname, jp.getDoubleValue());
                    break;
                case FLOAT:
                    checkNumberType(fieldname, NumberType.FLOAT, jp);
                    put(fieldname, jp.getFloatValue());
                    break;
                case NUMBER:
                    checkNumberType(fieldname, NumberType.BIG_DECIMAL, jp);
                    putNumber(fieldname, jsonParserGetDecimalValue(jp));
                    break;
                case STRING:
                    put(fieldname, jp.getText());
                    break;
                case BINARY:
                    put(fieldname, jp.getBinaryValue());
                    break;
                case FIXED_BINARY:
                    putFixed(fieldname, jp.getBinaryValue());
                    break;
                case BOOLEAN:
                    put(fieldname, jp.getBooleanValue());
                    break;
                case TIMESTAMP:
                    put(fieldname,
                        element.asTimestamp().fromString(jp.getText()));
                    break;
                case ARRAY:
                    /*
                     * current token is '[', then array elements
                     * TODO: need to have a full-on switch for adding
                     * array elements of the right type.
                     */
                    ArrayValueImpl array = putArray(fieldname);
                    array.addJsonFields(jp, null, exact, addMissingFields);
                    break;
                case MAP:
                    MapValueImpl map = putMap(fieldname);
                    map.addJsonFields(jp, null, exact, addMissingFields);
                    break;
                case RECORD:
                    RecordValueImpl record = putRecord(fieldname);
                    record.addJsonFields(jp, null, exact, addMissingFields);
                    break;
                case ENUM:
                    putEnum(fieldname, jp.getText());
                    break;
                case JSON:
                case ANY_JSON_ATOMIC:
                    put(fieldname, JsonDefImpl.createFromJson(jp, false));
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
                        "A map type cannot have " + element.getType() +
                        " as its element type");
                default:
                    break;
                }
            }
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Failed to parse JSON input: " + ioe.getMessage(), ioe);
        } catch (RuntimeException re) {
            if (re instanceof IllegalArgumentException) {
                throw re;
            }
            throw new IllegalArgumentException(
                "Failed to parse JSON input: " + re.toString(), re);
        }
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public void toStringBuilder(StringBuilder sb, DisplayFormatter formatter) {
        toStringBuilder(sb, formatter,
                        Collections.unmodifiableMap(fields), size());
    }

    protected static void toStringBuilder(StringBuilder sb,
                                          DisplayFormatter formatter,
                                          Map<Object, FieldValue> fields,
                                          int size) {
        if (formatter == null) {
            throw new IllegalArgumentException(
                "DisplayFormatter must be non-null");
        }
        formatter.startObject();
        sb.append('{');
        int i = 0;
        for (Map.Entry<Object, FieldValue> entry : fields.entrySet()) {
            String key = entry.getKey().toString();
            FieldValueImpl val = (FieldValueImpl)entry.getValue();
            if (val != null) {
                formatter.newPair(sb, (i > 0));
                sb.append('\"');
                CharTypes.appendQuoted(sb, key);
                sb.append('\"');
                formatter.separator(sb);
                val.toStringBuilder(sb, formatter);
                i++;
            }
        }
        formatter.endObject(sb, size);
        sb.append('}');
    }

    @SuppressWarnings("unchecked")
    static MapValueImpl fromJavaObjectValue(FieldDef def, Object o) {

        Map<String, Object> javaMap = (Map<String, Object>) o;

        MapValue map = def.createMap();

        for (Map.Entry<String, Object> entry : javaMap.entrySet()) {
            String key = entry.getKey().toString();
            map.put(
                key,
                FieldValueImpl.fromJavaObjectValue(
                    map.getDefinition().getElement(),
                    entry.getValue()));
        }
        return (MapValueImpl)map;
    }

    /*
     * local methods
     */

    /**
     * Clears the map.
     */
    void clearMap() {
        fields.clear();
    }

    /*
     * Cheap validation for scalars. If types match, nothing to do; if not,
     * do the more expensive work in validate().
     */
    private MapValueImpl putScalar(String fieldName, FieldValue value) {
        if (getDefinition().getType() != value.getType()) {
            value = validate(value, getElementDef());
        }
        fields.put(fieldName, value);
        return this;
    }

    public FieldDefImpl getElementDef() {
        return getDefinition().getElement();
    }

    public Map<String, FieldValue> getFieldsInternal() {
        return fields;
    }

    public Set<String> getFieldNames() {
        return fields.keySet();
    }

    /**
     * This version is used internally for index deserialization.  Enums are
     * stored as an integer index into the enumeration values in indexes.
     */
    MapValue putEnum(String name, int index) {
        fields.put(name, ((EnumDefImpl)getElementDef()).createEnum(index));
        return this;
    }

    @Override
    public MapValueSerializer asMapValueSerializer() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Entry<String, FieldValueSerializer>> iterator() {
        final Map<String, ?> values = getFields();
        return ((Map<String, FieldValueSerializer>)values).entrySet().iterator();
    }
}
