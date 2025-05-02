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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oracle.kv.impl.api.table.TablePath.StepInfo;
import oracle.kv.impl.api.table.ValueSerializer.ArrayValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.FieldValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.MapValueSerializer;
import oracle.kv.impl.api.table.ValueSerializer.RecordValueSerializer;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.query.types.TypeManager;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.BinaryValue;
import oracle.kv.table.BooleanValue;
import oracle.kv.table.DoubleValue;
import oracle.kv.table.EnumValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FixedBinaryValue;
import oracle.kv.table.FloatValue;
import oracle.kv.table.IndexKey;
import oracle.kv.table.IntegerValue;
import oracle.kv.table.LongValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.NumberValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.StringValue;
import oracle.kv.table.TimestampValue;

import com.sleepycat.bind.tuple.TupleInput;

/**
 * FieldValueImpl represents a value of a single field.  A value may be simple
 * or complex (single-valued vs multi-valued).  FieldValue is the building
 * block of row values in a table.
 *<p>
 * The FieldValueImpl class itself has no state and serves as an abstract base
 * for implementations of FieldValue and its sub-interfaces.
 */
public abstract class FieldValueImpl implements FieldValue, FieldValueSerializer,
                                                Serializable, Cloneable,
                                                FastExternalizable {

    protected enum ValueType implements FastExternalizable {
        ARRAY_VALUE,
        BINARY_VALUE,
        BOOLEAN_VALUE,
        COMPLEX_VALUE,
        DOUBLE_VALUE,
        DOUBLE_RANGE_VALUE,
        EMPTY_VALUE,
        ENUM_VALUE,
        FIXED_BINARY_VALUE,
        FLOAT_RANGE_VALUE,
        FLOAT_VALUE,
        INTEGER_RANGE_VALUE,
        INTEGER_VALUE,
        LONG_RANGE_VALUE,
        LONG_VALUE,
        MAP_VALUE,
        NULL_JSON_VALUE,
        NULL_VALUE,
        NUMBER_VALUE,
        RECORD_VALUE,
        STRING_RANGE_VALUE,
        STRING_VALUE,
        TIMESTAMP_VALUE,
        TUPLE_VALUE;

        private static final ValueType[] VALUES = values();

        private static ValueType valueOf(int ordinal) {
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Unexpected Type ordinal: " + ordinal);
            }
        }

        /**
         * Writes this object to an output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i>
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
            out.writeByte(ordinal());
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Reads a FieldValueImpl from the input stream. May return null.
     */
    static FieldValueImpl readFastExternalOrNull(DataInput in,
                                                 short serialVersion,
                                                 FieldDef def)
            throws IOException {

        final byte ordinal = in.readByte();
        if (ordinal == -1) {
            return null;
        }
        final ValueType type = ValueType.valueOf(ordinal);
        switch (type) {
        case BINARY_VALUE:
            return new BinaryValueImpl(in);
        case BOOLEAN_VALUE:
            return new BooleanValueImpl(in);
        case DOUBLE_VALUE:
            return new DoubleValueImpl(in);
        case DOUBLE_RANGE_VALUE:
            return new DoubleRangeValue(in, (DoubleDefImpl)def);
        case EMPTY_VALUE:
            return EmptyValueImpl.getInstance();
        case ENUM_VALUE:
            return new EnumValueImpl(in, serialVersion, (EnumDefImpl)def);
        case FIXED_BINARY_VALUE:
            return new FixedBinaryValueImpl(in, (FixedBinaryDefImpl)def);
        case FLOAT_RANGE_VALUE:
            return new FloatRangeValue(in, (FloatDefImpl)def);
        case FLOAT_VALUE:
            return new FloatValueImpl(in);
        case INTEGER_RANGE_VALUE:
            return new IntegerRangeValue(in, (IntegerDefImpl)def);
        case INTEGER_VALUE:
            return new IntegerValueImpl(in);
        case LONG_RANGE_VALUE:
            return new LongRangeValue(in, (LongDefImpl)def);
        case LONG_VALUE:
            return new LongValueImpl(in);
        case NUMBER_VALUE:
            return new NumberValueImpl(in);
        case STRING_RANGE_VALUE:
            return new StringRangeValue(in, serialVersion, (StringDefImpl)def);
        case STRING_VALUE:
            return new StringValueImpl(in, serialVersion);
        case TIMESTAMP_VALUE:
            return new TimestampValueImpl(in, serialVersion);

        /*
         * The following types are never used as default values. So are never
         * serialized as part of the FieldMap.
         */
        case ARRAY_VALUE:
        case COMPLEX_VALUE:
        case MAP_VALUE:
        case NULL_JSON_VALUE:
        case NULL_VALUE:
        case RECORD_VALUE:
        case TUPLE_VALUE:
            throw new IllegalStateException("Deserialization is not supported" +
                                            " for field value type " + type);
        default:
            throw new IllegalStateException("Unknown field value type: " + type);
        }
    }

    /**
     * Writes a field value to the output stream. value may be null. Throws
     * IllegalStateException if the value type does not support
     * FastExternalizable. Format:
     * <ol>
     * <li> ({@code byte}) <i>-1 if value is null or </i>
     *                     ({@link FieldValueImpl}) {@code value}
     *                     // if value is not null
     * </ol>
     */
    static void writeFastExternalOrNull(DataOutput out,
                                        short serialVersion,
                                        FieldValueImpl value)
            throws IOException {
        if (value == null) {
            out.writeByte(-1);
            return;
        }
        value.writeFastExternal(out, serialVersion);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@code #ValueType}) {@code getValueType()}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        getValueType().writeFastExternal(out, serialVersion);
    }

    /**
     * Throws an IllegalStateException. Called by subclasses which do not
     * support FastExternal. Some field value classes are not serialized
     * with the table metadata outside of query returns. Those classes do
     * not need to support FastExternal.
     */
    protected void fastExternalNotSupported() {
        throw new IllegalStateException("FastExternal serialization not" +
                                        " supported for the " + getValueType() +
                                        " type");
    }

    /**
     * Gets the value type of this object.
     */
    protected abstract ValueType getValueType();

    /**
     * The serialization version for MRCounter values. This version
     * will be stored at the beginning of the byte arrays for MRCounter
     * columns.
     */
    public enum CounterVersion {

        COUNTER_V1(0);

        private CounterVersion(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalStateException("Wrong ordinal");
            }
        }

    }

    @Override
    public FieldValueImpl clone() {
        try {
            return (FieldValueImpl) super.clone();
        } catch (CloneNotSupportedException ignore) {
        }
        return null;
    }

    @Override
    public int compareTo(FieldValue o) {
        throw new IllegalArgumentException
            ("FieldValueImpl objects must implement compareTo");
    }

    @Override
    public abstract FieldDefImpl getDefinition();

    @Override
    public BinaryValue asBinary() {
        throw new ClassCastException
            ("Field is not a Binary: " + getClass());
    }

    @Override
    public NumberValue asNumber() {
        throw new ClassCastException
            ("Field is not a Number: " + getClass());
    }

    @Override
    public BooleanValue asBoolean() {
        throw new ClassCastException
            ("Field is not a Boolean: " + getClass());
    }

    @Override
    public DoubleValue asDouble() {
        throw new ClassCastException
            ("Field is not a Double: " + getClass());
    }

    @Override
    public FloatValue asFloat() {
        throw new ClassCastException
            ("Field is not a Float: " + getClass());
    }

    @Override
    public IntegerValue asInteger() {
        throw new ClassCastException
            ("Field is not an Integer: " + getClass());
    }

    @Override
    public LongValue asLong() {
        throw new ClassCastException
            ("Field is not a Long: " + getClass());
    }

    @Override
    public StringValue asString() {
        throw new ClassCastException
            ("Field is not a String: " + getClass());
    }

    @Override
    public TimestampValue asTimestamp() {
        throw new ClassCastException
            ("Field is not a Timestamp: " + getClass());
    }

    @Override
    public EnumValue asEnum() {
        throw new ClassCastException
            ("Field is not an Enum: " + getClass());
    }

    @Override
    public FixedBinaryValue asFixedBinary() {
        throw new ClassCastException
            ("Field is not a FixedBinary: " + getClass());
    }

    @Override
    public ArrayValue asArray() {
        throw new ClassCastException
            ("Field is not an Array: " + getClass());
    }

    @Override
    public MapValue asMap() {
        throw new ClassCastException
            ("Field is not a Map: " + getClass());
    }

    @Override
    public RecordValue asRecord() {
        throw new ClassCastException
            ("Field is not a Record: " + getClass());
    }

    @Override
    public Row asRow() {
        throw new ClassCastException
            ("Field is not a Row: " + getClass());
    }

    @Override
    public PrimaryKey asPrimaryKey() {
        throw new ClassCastException
            ("Field is not a PrimaryKey: " + getClass());
    }

    @Override
    public IndexKey asIndexKey() {
        throw new ClassCastException
            ("Field is not an IndexKey: " + getClass());
    }


    @Override
    public boolean isBinary() {
        return false;
    }

    @Override
    public boolean isNumber() {
        return false;
    }

    @Override
    public boolean isBoolean() {
        return false;
    }

    @Override
    public boolean isDouble() {
        return false;
    }

    @Override
    public boolean isFloat() {
        return false;
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public boolean isFixedBinary() {
        return false;
    }

    @Override
    public boolean isLong() {
        return false;
    }

    @Override
    public boolean isString() {
        return false;
    }

    @Override
    public boolean isTimestamp() {
        return false;
    }

    @Override
    public boolean isEnum() {
        return false;
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public boolean isRecord() {
        return false;
    }

    @Override
    public boolean isRow() {
        return false;
    }

    @Override
    public boolean isPrimaryKey() {
        return false;
    }

    @Override
    public boolean isIndexKey() {
        return false;
    }

    @Override
    public boolean isJsonNull() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    /*
     * Check whether this is an EmptyValueImpl.
     * Note: capitals are used for EMPTY to distinguish this method from
     * RecordValueImpl.isEmpty().
     */
    @Override
    public boolean isEMPTY() {
        return false;
    }

    public boolean isSpecialValue() {
        return false;
    }

    @Override
    public boolean isAtomic() {
        return false;
    }

    @Override
    public boolean isNumeric() {
        return false;
    }

    @Override
    public boolean isComplex() {
        return false;
    }

    public boolean isTuple() {
        return false;
    }

    public boolean isMRCounter() {
        return false;
    }

    public boolean isJson() {

        switch (getValueType()) {
        case BOOLEAN_VALUE:
        case DOUBLE_VALUE:
        case FLOAT_VALUE:
        case INTEGER_VALUE:
        case LONG_VALUE:
        case STRING_VALUE:
        case NULL_JSON_VALUE:
            return true;
        case MAP_VALUE:
            MapValueImpl mapv = (MapValueImpl)this;
            Map<String, FieldValue> map = mapv.getMap();
            for (FieldValue val : map.values()) {
                if (!((FieldValueImpl)val).isJson()) {
                    return false;
                }
            }
            return true;
        case ARRAY_VALUE:
            ArrayValueImpl array = (ArrayValueImpl)this;
            for (int i = 0; i < array.size(); ++i) {
                if (!array.getElement(i).isJson()) {
                    return false;
                }
            }
            return true;
        default:
            return false;
        }
    }

    public boolean isJsonRowMap() {
        return false;
    }

    public JsonCollectionRowImpl.JsonRowMap asJsonRowMap() {
        throw new IllegalStateException("Field value is not from a " +
                                        "JSON Collection");
    }

    /**
     * Subclasses can override this but it will do a pretty good job of output
     */
    @Override
    public String toJsonString(boolean pretty) {
        StringBuilder sb = new StringBuilder(128);
        DisplayFormatter fomatter =
            new DisplayFormatter(pretty);
        toStringBuilder(sb, fomatter);
        return sb.toString();
    }

    /**
     * Create a Json representation that includes the internal info for
     * MRCounters.
     *
     * For unit tests only.
     *
     * @return a json string.
     */
    public String mrCounterToJsonString(boolean prettyPrint) {
        StringBuilder sb = new StringBuilder(128);
        DisplayFormatter formatter =
            new DisplayFormatter(prettyPrint);
        Map<Integer, FieldValueImpl> fields = getMRCounterMap();
        MapValueImpl.toStringBuilder(sb, formatter,
                                     Collections.unmodifiableMap(fields),
                                     fields.size());
        return sb.toString();
    }

    public abstract long sizeof();

    public int size() {
        throw new ClassCastException(
            "Value is not complex (array, map, or record): " + getClass());
    }

    public Map<String, FieldValue> getMap() {
        throw new ClassCastException(
            "Value is not a record or map: " + getClass());
    }

    @Override
    public int getInt() {
        throw new ClassCastException(
            "Value is not an integer or subtype: " + getClass());
    }

    public void setInt(@SuppressWarnings("unused")int v) {
        throw new ClassCastException(
            "Value is not an integer or subtype: " + getClass());
    }

    @Override
    public long getLong() {
        throw new ClassCastException(
            "Value is not a long or subtype: " + getClass());
    }

    public void setLong(@SuppressWarnings("unused")long v) {
        throw new ClassCastException(
            "Value is not a long or subtype: " + getClass());
    }

    @Override
    public float getFloat() {
        throw new ClassCastException(
            "Value is not a float or subtype: " + getClass());
    }

    public void setFloat(@SuppressWarnings("unused")float v) {
        throw new ClassCastException(
            "Value is not a float or subtype: " + getClass());
    }

    public void setDecimal(@SuppressWarnings("unused")BigDecimal v) {
        throw new ClassCastException(
            "Value is not a Number or subtype: " + getClass());
    }

    public BigDecimal getDecimal() {
        throw new ClassCastException(
            "Value is not a double or subtype: " + getClass());
    }

    @Override
    public double getDouble() {
        throw new ClassCastException(
            "Value is not a double or subtype: " + getClass());
    }

    public void setDouble(@SuppressWarnings("unused")double v) {
        throw new ClassCastException(
            "Value is not a double or subtype: " + getClass());
    }

    @Override
    public String getString() {
        throw new ClassCastException(
            "Value is not a string or subtype: " + getClass());
    }

    public void setString(@SuppressWarnings("unused")String v) {
        throw new ClassCastException(
            "Value is not a String or subtype: " + getClass());
    }

    @Override
    public String getEnumString() {
        throw new ClassCastException(
            "Value is not an enum or subtype: " + getClass());
    }

    public void setEnum(@SuppressWarnings("unused")String v) {
        throw new ClassCastException(
            "Value is not an enum or subtype: " + getClass());
    }

    @Override
    public boolean getBoolean() {
        throw new ClassCastException(
            "Value is not a boolean: " + getClass());
    }

    public void setBoolean(@SuppressWarnings("unused")boolean v) {
        throw new ClassCastException(
            "Value is not a boolean or subtype: " + getClass());
    }

    @Override
    public byte[] getBytes() {
        throw new ClassCastException(
            "Value is not a binary: " + getClass());
    }

    public void setTimestamp(@SuppressWarnings("unused") Timestamp timestamp) {
        throw new ClassCastException("Value is not a timestamp: " + getClass());
    }

    public Timestamp getTimestamp() {
        throw new ClassCastException("Value is not a timestamp: " + getClass());
    }

    public FieldValueImpl getElement(@SuppressWarnings("unused")int index) {
        throw new ClassCastException(
            "Value is not an array or record: " + getClass());
    }

    public FieldValueImpl getElement(String fieldName) {

        if (isMap()) {
            return ((MapValueImpl)this).get(fieldName);
        }

        if (isRecord()) {
            return ((RecordValueImpl)this).get(fieldName);
        }

        if (isTuple()) {
            return ((TupleValue)this).get(fieldName);
        }

        throw new ClassCastException(
            "Value is not a map or a record: " + getClass());
    }

    /**
     * Returns the internal map of counts for an MRCounter.
     * @return the map of counts.
     */
    public Map<Integer, FieldValueImpl> getMRCounterMap() {
        throw new ClassCastException(
            "Value is not an MR counter : " + getClass());
    }

    /**
     * Update this MRCounter by inserting an entry into its internal map
     * with the region id as the key and the associated count as the value.
     *
     * @param regionID the region id.
     * @param count the associated count.
     */
    public void putMRCounterEntry(Integer regionID, FieldValueImpl count) {
        throw new ClassCastException(
            "Value is not an MR counter : " + getClass());
    }

    /**
     * Increment this MRCounter by the specified value.
     *
     * @param val the increment value
     */
    public void incrementMRCounter(FieldValueImpl val,
                                   @SuppressWarnings("unused") int regionId) {
        throw new ClassCastException(
            "Value is not an MR counter : " + getClass());
    }

    /**
     * Decrement this MRCounter by the specified value.
     *
     * @param val the decrement value
     */
    public void decrementMRCounter(FieldValueImpl val,
                                   @SuppressWarnings("unused") int regionId) {
        throw new ClassCastException(
            "Value is not an MR counter : " + getClass());
    }

    /**
     * Merges another MRCounter into this MRCounter.
     * @param other another MRCounter
     * @return this
     */
    public FieldValue mergeMRCounter(FieldValueImpl other) {

        for (Map.Entry<Integer, FieldValueImpl> entry:
                 other.getMRCounterMap().entrySet()) {
            Integer id = entry.getKey();
            FieldValueImpl curVal = getMRCounterMap().get(id);
            FieldValueImpl otherVal = entry.getValue();

            if (curVal == null || curVal.compareTo(otherVal) < 0) {
                putMRCounterEntry(id, otherVal.clone());
            }
        }
        return this;
    }

    /**
     * Return the "next" legal value for this type in terms of comparison
     * purposes.  That is value.compareTo(value.getNextValue()) is &lt; 0 and
     * there is no legal value such that value &lt; cantHappen &lt;
     * value.getNextValue().
     *
     * This method is only called for indexable fields and is only
     * implemented for types for which FieldDef.isValidIndexField() returns true.
     */
    FieldValueImpl getNextValue() {
        throw new IllegalArgumentException
            ("Type does not implement getNextValue: " +
             getClass().getName());
    }

    /**
     * Return the minimum legal value for this type in terms of comparison
     * purposes such that there is no value V where value.compareTo(V) &gt; 0.
     *
     * This method is only called for indexable fields and is only
     * implemented for types for which FieldDef.isValidIndexField() returns true.
     */
    FieldValueImpl getMinimumValue() {
        throw new IllegalArgumentException
            ("Type does not implement getMinimumValue: " +
             getClass().getName());
    }

    /**
     *
     * @param sb
     *
     * This default method works for most primitives.
     */
    @SuppressWarnings("unused")
    public void toStringBuilder(StringBuilder sb,
                                DisplayFormatter formatter) {
        sb.append(toString());
    }

    /**
     * Default implementation, no pretty printing
     */
    public void toStringBuilder(StringBuilder sb) {
        toStringBuilder(sb, new DisplayFormatter());
    }

    /**
     * Construct a FieldValue from an Java Object.
     */
    static FieldValue fromJavaObjectValue(FieldDef def, Object o) {

        switch (def.getType()) {
        case INTEGER:
            return def.createInteger((Integer)o);
        case LONG:
            return def.createLong((Long)o);
        case DOUBLE:
            return def.createDouble((Double)o);
        case FLOAT:
            return def.createFloat((Float)o);
        case NUMBER:
            return def.createNumber((BigDecimal)o);
        case STRING:
            return def.createString((String)o);
        case BINARY:
            return def.createBinary((byte[])o);
        case FIXED_BINARY:
            return def.createFixedBinary((byte[])o);
        case BOOLEAN:
            return def.createBoolean((Boolean)o);
        case ENUM:
            return def.createEnum((String)o);
        case TIMESTAMP:
            return def.createTimestamp((Timestamp)o);
        case RECORD:
            return RecordValueImpl.fromJavaObjectValue(def, o);
        case ARRAY:
            return ArrayValueImpl.fromJavaObjectValue(def, o);
        case MAP:
            return MapValueImpl.fromJavaObjectValue(def, o);
        default:
            throw new IllegalArgumentException
                ("Complex classes must override fromJavaObjectValue");
        }
    }

    /**
     * Return a String representation of the value suitable for use as part of
     * a primary key.  This method must work for any value that can participate
     * in a primary key.  The key string format may be different than a more
     * "natural" string format and may not be easily human readable.  It is
     * defined so that primary key fields sort and compare correctly and
     * consistently.
     */
    @SuppressWarnings("unused")
    public String formatForKey(FieldDef field, int storageSize) {
        throw new IllegalArgumentException
            ("Key components must be atomic types");
    }

    String formatForKey(FieldDef field) {
        return formatForKey(field, 0);
    }

    /*
     * Evaluate a path expression consisting of field steps only. The expression
     * is supposed to return at most one item. This implies that the starting
     * context item cannot be an array, and no arrays can be crossed by the
     * path.
     */
    public FieldValueImpl evaluateScalarPath(TablePath path, int pathPos) {

        return evaluateScalarPath(path.getSteps(), pathPos);
    }

    public FieldValueImpl evaluateScalarPath(List<StepInfo> path, int pathPos) {

        assert(!path.get(pathPos).isMultiKeyStep());

        if (isNull()) {
            return NullValueImpl.getInstance();
        }

        if (isAtomic()) {
            return EmptyValueImpl.getInstance();
        }

        switch (getType()) {
        case RECORD: {
            RecordValueImpl rec = (RecordValueImpl)this;
            String next = path.get(pathPos).getStep();
            FieldValueImpl fv = rec.get(next);

            if (fv == null) {

                if (this instanceof JsonCollectionRowImpl) {
                    return EmptyValueImpl.getInstance();
                }

                StringBuilder sb = new StringBuilder();
                TablePath.printBranch(sb, path);
                throw new IllegalArgumentException(
                    "Unexpected null field value in atomic path " +
                    sb.toString() + " at step " + next + " in record :\n" +
                    rec);
            }

            ++pathPos;

            if (pathPos >= path.size() || fv.isNull()) {
                return fv;
            }

            if (fv.isAtomic()) {
                return EmptyValueImpl.getInstance();
            }

            return fv.evaluateScalarPath(path, pathPos);
        }
        case MAP: {
            MapValueImpl map = (MapValueImpl)this;
            String next = path.get(pathPos).getStep();
            FieldValueImpl fv = map.get(next);

            if (fv == null) {
                return EmptyValueImpl.getInstance();
            }

            ++pathPos;

            if (pathPos >= path.size()) {
                return fv;
            }

            if (fv.isAtomic()) {
                return EmptyValueImpl.getInstance();
            }

            return fv.evaluateScalarPath(path, pathPos);
        }
        default:
            StringBuilder sb = new StringBuilder();
            TablePath.printBranch(sb, path);
            throw new IllegalArgumentException(
                "Unexpected value in atomic path " + sb.toString() +
                " at step " + path.get(pathPos).getStep() + " value =\n" +
                this);
        }
    }

    /**
     * The type is passed explicitly for the case where it may be JSON.
     * The FieldDef is only needed for Timestamp.
     */
    static Object readTuple(FieldDef.Type type,
                            FieldDef def,
                            TupleInput in) {
        switch (type) {
        case INTEGER:
            return in.readSortedPackedInt();
        case STRING:
            if (def.isUUIDString()) {
                byte[] buf = new byte[16];
                in.read(buf);
                return StringValueImpl.unpackUUID(buf);
            }
            return in.readString();
        case LONG:
            return in.readSortedPackedLong();
        case DOUBLE:
            return in.readSortedDouble();
        case FLOAT:
            return in.readSortedFloat();
        case NUMBER:
            return NumberUtils.readTuple(in);
        case ENUM:
            return in.readSortedPackedInt();
        case BOOLEAN:
            return in.readBoolean();
        case TIMESTAMP: {
            assert def != null;
            byte[] buf = new byte[((TimestampDefImpl)def).getNumBytes()];
            in.read(buf);
            return buf;
        }
        default:
            throw new IllegalStateException
                ("Type not supported in indexes: " + type);
        }
    }

    /**
     * Compares 2 FieldValue instances.
     *
     * For null(java null) or NULL(NullValue) value, they are compared based on
     * "null last" rule:
     *     null &gt; not null
     *     NULL &gt; NOT NULL
     */
    public static int compareFieldValues(FieldValue val1, FieldValue val2) {

        FieldValueImpl v1 = (FieldValueImpl)val1;
        FieldValueImpl v2 = (FieldValueImpl)val2;

        if (v1 != null) {
            if (v2 == null) {
                return -1;
            }
            if (v1.isSpecialValue() || v2.isSpecialValue()) {
                if (!v1.isSpecialValue()) {
                    return -1;
                }
                if (!v2.isSpecialValue()) {
                    return 1;
                }
                return compareSpecialValues(v1, v2);
            }
            return v1.compareTo(v2);
        } else if (v2 != null) {
            return 1;
        }
        return 0;
    }

    /**
     * Checks whether 2 values are "semantically" equal. The
     * FieldValueImpl.equals() method is more strict than this one, because
     * it requires the 2 values to have the same type. So, for example,
     * 3 and 3.0 are not equal according to FieldValueImpl.equals(), but they
     * are equal according to this method.
     */
    public boolean equal(FieldValueImpl o) {

        if (isNull()) {
            return o.isNull();
        }

        if (o.isNull()) {
            return false;
        }

        if (isJsonNull()) {
            return o.isJsonNull();
        }

        if (o.isJsonNull()) {
            return false;
        }

        Type tc1 = getType();
        Type tc2 = o.getType();

        switch (tc1) {
        case ARRAY:
            if (tc2 != Type.ARRAY) {
                return false;
            }

            ArrayValueImpl arr1 = (ArrayValueImpl)this;
            ArrayValueImpl arr2 = (ArrayValueImpl)o;
            if (arr1.size() != arr2.size()) {
                return false;
            }

            for (int i = 0; i < arr1.size(); i++) {
                if (!arr1.get(i).equal(arr2.get(i))) {
                    return false;
                }
            }

            return true;

        case MAP:
            if (tc2 != Type.MAP) {
                return false;
            }

            MapValueImpl map1 = (MapValueImpl)this;
            MapValueImpl map2 = (MapValueImpl)o;
            if (map1.size() != map2.size()) {
                return false;
            }

            Iterator<String> keyIter = map1.getFields().keySet().iterator();

            while (keyIter.hasNext()) {
                String key1 = keyIter.next();
                FieldValueImpl val2 = map2.get(key1);

                if (val2 == null) {
                    return false;
                }

                FieldValueImpl val1 = map1.get(key1);

                if (!val1.equal(val2)) {
                    return false;
                }
            }

            return true;

        case RECORD:
            if (tc2 != Type.RECORD) {
                return false;
            }

            RecordValueImpl rec1 = (RecordValueImpl)this;
            RecordValueImpl rec2 = (RecordValueImpl)o;
            int numFields = rec1.getNumFields();

            if (numFields != rec2.getNumFields()) {
                return false;
            }

            for (int i = 0; i < numFields; ++i) {
                String key1 = rec1.getFieldName(i);
                String key2 = rec2.getFieldName(i);

                if (!key1.equals(key2)) {
                    return false;
                }

                FieldValueImpl fval1 = rec1.get(i);
                FieldValueImpl fval2 = rec2.get(i);

                if (!fval1.equal(fval2)) {
                    return false;
                }
            }

            return true;

        case INTEGER: {
            switch (tc2) {
            case INTEGER:
                return ((IntegerValueImpl)this).get() ==
                       ((IntegerValueImpl)o).get();
            case LONG:
                return ((IntegerValueImpl)this).get() ==
                       ((LongValueImpl)o).get();
            case FLOAT:
                return ((IntegerValueImpl)this).get() ==
                       ((FloatValueImpl)o).get();
            case DOUBLE:
                return ((IntegerValueImpl)this).get() ==
                       ((DoubleValueImpl)o).get();
            case NUMBER:
                BigDecimal bd1 = BigDecimal.
                                 valueOf(((IntegerValueImpl)this).get());
                BigDecimal bd2 = ((NumberValueImpl)o).get();
                return bd1.compareTo(bd2) == 0;
            default:
                return false;
            }
        }
        case LONG: {
            switch (tc2) {
            case INTEGER:
                return ((LongValueImpl)this).get() ==
                       ((IntegerValueImpl)o).get();
            case LONG:
                return ((LongValueImpl)this).get() ==
                       ((LongValueImpl)o).get();
            case FLOAT:
                return ((LongValueImpl)this).get() ==
                       ((FloatValueImpl)o).get();
            case DOUBLE:
                return ((LongValueImpl)this).get() ==
                       ((DoubleValueImpl)o).get();
            case NUMBER:
                BigDecimal bd1 = BigDecimal.
                                 valueOf(((LongValueImpl)this).get());
                BigDecimal bd2 = ((NumberValueImpl)o).get();
                return bd1.compareTo(bd2) == 0;
            default:
                return false;
            }
        }
        case FLOAT: {
            switch (tc2) {
            case INTEGER:
                return ((FloatValueImpl)this).get() ==
                       ((IntegerValueImpl)o).get();
            case LONG:
                return ((FloatValueImpl)this).get() ==
                       ((LongValueImpl)o).get();
            case FLOAT:
                return ((FloatValueImpl)this).get() ==
                       ((FloatValueImpl)o).get();
            case DOUBLE:
                return ((FloatValueImpl)this).get() ==
                       ((DoubleValueImpl)o).get();
            case NUMBER:
                BigDecimal bd1 = BigDecimal.
                                 valueOf(((FloatValueImpl)this).get());
                BigDecimal bd2 = ((NumberValueImpl)o).get();
                return bd1.compareTo(bd2) == 0;
            default:
                return false;
            }
        }
        case DOUBLE: {
            switch (tc2) {
            case INTEGER:
                return ((DoubleValueImpl)this).get() ==
                       ((IntegerValueImpl)o).get();
            case LONG:
                return ((DoubleValueImpl)this).get() ==
                       ((LongValueImpl)o).get();
            case FLOAT:
                return ((DoubleValueImpl)this).get() ==
                       ((FloatValueImpl)o).get();
            case DOUBLE:
                return ((DoubleValueImpl)this).get() ==
                       ((DoubleValueImpl)o).get();
            case NUMBER:
                BigDecimal bd1 = BigDecimal.
                                 valueOf(((DoubleValueImpl)this).get());
                BigDecimal bd2 = ((NumberValueImpl)o).get();
                return bd1.compareTo(bd2) == 0;
            default:
                return false;
            }
        }
        case NUMBER: {
            NumberValue number = (NumberValueImpl)this;
            if (o.isNumeric()) {
                return number.compareTo(o) == 0;
            }
            return false;
        }
        case STRING:
            if (tc2 == Type.STRING) {
                return ((StringValueImpl)this).get().equals(
                       ((StringValueImpl)o).get());
            }
            return false;

        case ENUM:
            return this.equals(o);

        case TIMESTAMP:
            if (tc2 == Type.TIMESTAMP) {
                return ((TimestampValueImpl)this).compareTo(o) == 0;
            }
            return false;

        case BINARY:
            if (tc2 == Type.BINARY) {
                return Arrays.equals(((BinaryValueImpl)this).get(),
                                     ((BinaryValueImpl)o).get());
            } else if (tc2 == Type.FIXED_BINARY) {
                return Arrays.equals(((BinaryValueImpl)this).get(),
                                     ((FixedBinaryValueImpl)o).get());
            }
            return false;

        case FIXED_BINARY:
            if (tc2 == Type.BINARY) {
                return Arrays.equals(((FixedBinaryValueImpl)this).get(),
                                     ((BinaryValueImpl)o).get());
            } else if (tc2 == Type.FIXED_BINARY) {
                return Arrays.equals(((FixedBinaryValueImpl)this).get(),
                                     ((FixedBinaryValueImpl)o).get());
            }
            return false;

        case BOOLEAN:
            if (tc2 == Type.BOOLEAN) {
                return ((BooleanValueImpl)this).get() ==
                       ((BooleanValueImpl)o).get();
            }
            return false;

        case EMPTY:
            return o.isEMPTY();

        default:
            throw new QueryStateException(
                "Unexpected value type in equal method: " + tc1);
        }
    }

    /**
     * A hash method that is consistent with the equal() method above.
     * That is, if 2 values are equal accoring to equal(), they will
     * also have the same hash value according to this method.
     */
    public int hashcode() {

        if (isNull()) {
            return Integer.MAX_VALUE;
        }

        if (isJsonNull()) {
            return Integer.MIN_VALUE;
        }

        switch (getType()) {
        case ARRAY: {
            ArrayValueImpl arr = (ArrayValueImpl)this;
            int code = 1;
            for (int i = 0; i < arr.size(); ++i) {
                code = 31 * code + arr.get(i).hashcode();
            }

            return code;
        }
        case MAP: {
            MapValueImpl map = (MapValueImpl)this;
            int code = 1;
            for (Map.Entry<String, FieldValue> entry :
                 map.getFields().entrySet()) {
                code = (31 * code +
                         entry.getKey().hashCode() +
                         ((FieldValueImpl)entry.getValue()).hashcode());
            }

            return code;
        }
        case RECORD: {
            RecordValueImpl rec = (RecordValueImpl)this;
            int numFields = rec.getNumFields();
            int code = 1;

            for (int i = 0; i < numFields; ++i) {
                code = 31 * code + rec.get(i).hashcode();
            }

            return code;
        }
        case INTEGER: {
            long l = ((IntegerValueImpl)this).get();
            return (int)(l ^ (l >>> 32));
        }
        case LONG: {
            long l = ((LongValueImpl)this).get();
            return (int)(l ^ (l >>> 32));
        }
        case FLOAT: {
            double d = ((FloatValueImpl)this).get();
            if (d != Double.NaN) {
                long l = (long)d;
                if (d == l) {
                    return (int)(l ^ (l >>> 32));
                }
            }
            return Double.hashCode(d);
        }
        case DOUBLE: {
            double d = ((DoubleValueImpl)this).get();
            if (d != Double.NaN) {
                long l = (long)d;
                if (d == l) {
                    return (int)(l ^ (l >>> 32));
                }
            }
            return Double.hashCode(d);
        }
        case NUMBER: {
            long l;
            BigDecimal bd = ((NumberValueImpl)this).get();
            try {
                l = bd.longValueExact();
            } catch (ArithmeticException e) {
                double d = bd.doubleValue();
                if (bd.compareTo(BigDecimal.valueOf(d)) == 0) {
                    return Double.hashCode(d);
                }
                return hashCode();
            }

            return (int)(l ^ (l >>> 32));
        }
        case STRING:
        case ENUM:
        case TIMESTAMP:
        case BINARY:
        case FIXED_BINARY:
        case BOOLEAN:
            return hashCode();
        case EMPTY:
            return 0;
        default:
            throw new QueryStateException(
                "Unexpected value type in hashcode method: " + getType());
        }
    }

    /*
     * Compare 2 atomic values.
     * The method throws an exception if either of the 2 values is non-atomic
     * or the values are not comparable. Otherwise, it retuns 0 if v1 == v2,
     * 1 if v1 > v2, or -1 if v1 < v2.
     */
    public static int compareKeyValues(
        FieldValueImpl v1,
        FieldValueImpl v2) {

        if (v1.isSpecialValue()) {
            if (!v2.isSpecialValue()) {
                return 1;
            }
            return compareSpecialValues(v1, v2);
        }

        if (v2.isSpecialValue()) {
            return -1;
        }

        Type tc1 = v1.getType();
        Type tc2 = v2.getType();

        switch (tc1) {
        case INTEGER: {
            switch (tc2) {
            case INTEGER:
                return IntegerValueImpl.compare(
                               ((IntegerValueImpl)v1).getInt(),
                               ((IntegerValueImpl)v2).getInt());
            case LONG:
                return LongValueImpl.compare(
                               ((IntegerValueImpl)v1).getLong(),
                               ((LongValueImpl)v2).getLong());
            case FLOAT:
                return Float.compare(
                               ((IntegerValueImpl)v1).getInt(),
                               ((FloatValueImpl)v2).getFloat());
            case DOUBLE:
                return Double.compare(
                               ((IntegerValueImpl)v1).getInt(),
                               ((DoubleValueImpl)v2).getDouble());
            case NUMBER:
                return -v2.compareTo(v1);
            default:
                break;
            }
            break;
        }
        case LONG: {
            switch (tc2) {
            case INTEGER:
                return LongValueImpl.compare(
                               ((LongValueImpl)v1).getLong(),
                               ((IntegerValueImpl)v2).getLong());
            case LONG:
                return LongValueImpl.compare(
                               ((LongValueImpl)v1).getLong(),
                               ((LongValueImpl)v2).getLong());
            case FLOAT:
                return Float.compare(
                               ((LongValueImpl)v1).getLong(),
                               ((FloatValueImpl)v2).getFloat());
            case DOUBLE:
                return Double.compare(
                               ((LongValueImpl)v1).getLong(),
                               ((DoubleValueImpl)v2).getDouble());
            case NUMBER:
                return -v2.compareTo(v1);
            default:
                break;
            }
            break;
        }
        case FLOAT: {
            switch (tc2) {
            case INTEGER:
                return Float.compare(
                               ((FloatValueImpl)v1).getFloat(),
                               ((IntegerValueImpl)v2).getInt());
            case LONG:
                return Float.compare(
                               ((FloatValueImpl)v1).getFloat(),
                               ((LongValueImpl)v2).getLong());
            case FLOAT:
                return Float.compare(
                               ((FloatValueImpl)v1).getFloat(),
                               ((FloatValueImpl)v2).getFloat());
            case DOUBLE:
                return Double.compare(
                               ((FloatValueImpl)v1).getDouble(),
                               ((DoubleValueImpl)v2).getDouble());
            case NUMBER:
                return -v2.compareTo(v1);
            default:
                break;
            }
            break;
        }
        case DOUBLE: {
            switch (tc2) {
            case INTEGER:
                return Double.compare(
                               ((DoubleValueImpl)v1).getDouble(),
                               ((IntegerValueImpl)v2).getInt());
            case LONG:
                return Double.compare(
                               ((DoubleValueImpl)v1).getDouble(),
                               ((LongValueImpl)v2).getLong());
            case FLOAT:
                return Double.compare(
                               ((DoubleValueImpl)v1).getDouble(),
                               ((FloatValueImpl)v2).getDouble());
            case DOUBLE:
                return Double.compare(
                               ((DoubleValueImpl)v1).getDouble(),
                               ((DoubleValueImpl)v2).getDouble());
            case NUMBER:
                return -v2.compareTo(v1);
            default:
                break;
            }
            break;
        }
        case NUMBER: {
            NumberValue number = (NumberValue)v1;
            switch (tc2) {
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case NUMBER:
                return number.compareTo(v2);
            default:
                break;
            }
            break;
        }
        case STRING: {
            switch (tc2) {
            case STRING:
                return ((StringValueImpl)v1).getString().compareTo(
                           ((StringValueImpl)v2).getString());
            case ENUM:
                // TODO: optimize this
                FieldValueImpl enumVal = TypeManager.promote(
                    v1, TypeManager.createValueType(v2));

                if (enumVal == null) {
                    break;
                }

                EnumDefImpl def1 = (EnumDefImpl)enumVal.getDefinition();
                EnumDefImpl def2 = (EnumDefImpl)v2.getDefinition();

                if (def1.valuesEqual(def2)) {
                    int idx1 = ((EnumValueImpl)enumVal).getIndex();
                    int idx2 = ((EnumValueImpl)v2).getIndex();
                    return ((Integer)idx1).compareTo(idx2);
                }

                break;
            default:
                break;
            }
            break;
        }
        case ENUM: {
            EnumDefImpl def1 = (EnumDefImpl)v1.getDefinition();
            EnumDefImpl def2;

            switch (tc2) {
            case STRING:
                FieldValueImpl enumVal = TypeManager.promote(
                    v2, TypeManager.createValueType(v1));

                if (enumVal == null) {
                    break;
                }

                def2 = (EnumDefImpl)enumVal.getDefinition();

                if (def1.valuesEqual(def2)) {
                    int idx1 = ((EnumValueImpl)v1).getIndex();
                    int idx2 = ((EnumValueImpl)enumVal).getIndex();
                    return ((Integer)idx1).compareTo(idx2);
                }

                break;
            case ENUM:
                def2 = (EnumDefImpl)v2.getDefinition();

                if (def1.valuesEqual(def2)) {
                    int idx1 = ((EnumValueImpl)v1).getIndex();
                    int idx2 = ((EnumValueImpl)v2).getIndex();
                    return ((Integer)idx1).compareTo(idx2);
                }
                break;
            default:
                break;
            }
            break;
        }
        case BOOLEAN: {
            if (tc2 == Type.BOOLEAN) {
                return ((BooleanValueImpl)v1).compareTo(v2);
            }
            break;
        }
        case TIMESTAMP: {
            switch (tc2) {
            case TIMESTAMP:
                return ((TimestampValueImpl)v1).compareTo(v2);
            default:
                break;
            }
            break;
        }
        default:
            break;
        }

        throw new IllegalArgumentException(
            "Cannot compare value of type " + tc1 + " with value of type " + tc2);
    }

    /*
     * Implements a total ordering among atomic values, retuning 0 if v1 == v2,
     * 1 if v1 > v2, or -1 if v1 < v2.
     *
     * The ordering among values that are not directly (i.e. without casting)
     * comparable is the following:
     *
     * numerics < timestamps < strings < enums < booleans < binaries <
     * empty < json null < null
     *
     * Furthermore, ordering among 2 enum valuies of different types is based
     * on the Object id of their corresponding EnumDefImpl instances.
     */
    public static int compareAtomicsTotalOrder(
        FieldValueImpl v1,
        FieldValueImpl v2) {

        assert(!v1.isComplex() && !v2.isComplex());

        if (v1.isSpecialValue()) {
            if (!v2.isSpecialValue()) {
                return 1;
            }
            return compareSpecialValues(v1, v2);
        }

        if (v2.isSpecialValue()) {
            return -1;
        }

        Type tc1 = v1.getType();
        Type tc2 = v2.getType();

        switch (tc1) {
        case INTEGER: {
            switch (tc2) {
            case INTEGER:
                return IntegerValueImpl.compare(
                               ((IntegerValueImpl)v1).getInt(),
                               ((IntegerValueImpl)v2).getInt());
            case LONG:
                return LongValueImpl.compare(
                               ((IntegerValueImpl)v1).getLong(),
                               ((LongValueImpl)v2).getLong());
            case FLOAT:
                return Float.compare(
                               ((IntegerValueImpl)v1).getInt(),
                               ((FloatValueImpl)v2).getFloat());
            case DOUBLE:
                return Double.compare(
                               ((IntegerValueImpl)v1).getInt(),
                               ((DoubleValueImpl)v2).getDouble());
            case NUMBER:
                return -v2.compareTo(v1);
            case TIMESTAMP:
            case ENUM:
            case STRING:
            case BOOLEAN:
            case BINARY:
            case FIXED_BINARY:
                return -1;
            default:
                break;
            }
            break;
        }
        case LONG: {
            switch (tc2) {
            case INTEGER:
                return LongValueImpl.compare(
                               ((LongValueImpl)v1).getLong(),
                               ((IntegerValueImpl)v2).getLong());
            case LONG:
                return LongValueImpl.compare(
                               ((LongValueImpl)v1).getLong(),
                               ((LongValueImpl)v2).getLong());
            case FLOAT:
                return Float.compare(
                               ((LongValueImpl)v1).getLong(),
                               ((FloatValueImpl)v2).getFloat());
            case DOUBLE:
                return Double.compare(
                               ((LongValueImpl)v1).getLong(),
                               ((DoubleValueImpl)v2).getDouble());
            case NUMBER:
                return -v2.compareTo(v1);
            case TIMESTAMP:
            case ENUM:
            case STRING:
            case BOOLEAN:
            case BINARY:
            case FIXED_BINARY:
                return -1;
            default:
                break;
            }
            break;
        }
        case FLOAT: {
            switch (tc2) {
            case INTEGER:
                return Float.compare(
                               ((FloatValueImpl)v1).getFloat(),
                               ((IntegerValueImpl)v2).getInt());
            case LONG:
                return Float.compare(
                               ((FloatValueImpl)v1).getFloat(),
                               ((LongValueImpl)v2).getLong());
            case FLOAT:
                return Float.compare(
                               ((FloatValueImpl)v1).getFloat(),
                               ((FloatValueImpl)v2).getFloat());
            case DOUBLE:
                return Double.compare(
                               ((FloatValueImpl)v1).getDouble(),
                               ((DoubleValueImpl)v2).getDouble());
            case NUMBER:
                return -v2.compareTo(v1);
            case TIMESTAMP:
            case ENUM:
            case STRING:
            case BOOLEAN:
            case BINARY:
            case FIXED_BINARY:
                return -1;
            default:
                break;
            }
            break;
        }
        case DOUBLE: {
            switch (tc2) {
            case INTEGER:
                return Double.compare(
                               ((DoubleValueImpl)v1).getDouble(),
                               ((IntegerValueImpl)v2).getInt());
            case LONG:
                return Double.compare(
                               ((DoubleValueImpl)v1).getDouble(),
                               ((LongValueImpl)v2).getLong());
            case FLOAT:
                return Double.compare(
                               ((DoubleValueImpl)v1).getDouble(),
                               ((FloatValueImpl)v2).getDouble());
            case DOUBLE:
                return Double.compare(
                               ((DoubleValueImpl)v1).getDouble(),
                               ((DoubleValueImpl)v2).getDouble());
            case NUMBER:
                return -v2.compareTo(v1);
            case TIMESTAMP:
            case ENUM:
            case STRING:
            case BOOLEAN:
            case BINARY:
            case FIXED_BINARY:
                return -1;
            default:
                break;
            }
            break;
        }
        case NUMBER: {
            NumberValue number = (NumberValue)v1;
            switch (tc2) {
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case NUMBER:
                return number.compareTo(v2);
            case TIMESTAMP:
            case ENUM:
            case STRING:
            case BOOLEAN:
            case BINARY:
            case FIXED_BINARY:
                return -1;
            default:
                break;
            }
            break;
        }
        case STRING: {
            switch (tc2) {
            case STRING:
                return ((StringValueImpl)v1).getString().compareTo(
                       ((StringValueImpl)v2).getString());
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case NUMBER:
            case TIMESTAMP:
                return 1;
            case ENUM:
            case BOOLEAN:
            case BINARY:
            case FIXED_BINARY:
                return -1;
            default:
                break;
            }
            break;
        }
        case ENUM: {
            switch (tc2) {
            case ENUM:
                EnumDefImpl def1 = (EnumDefImpl)v1.getDefinition();
                EnumDefImpl def2 = (EnumDefImpl)v2.getDefinition();

                if (def1.valuesEqual(def2)) {
                    int idx1 = ((EnumValueImpl)v1).getIndex();
                    int idx2 = ((EnumValueImpl)v2).getIndex();
                    return ((Integer)idx1).compareTo(idx2);
                }
                int id1 = System.identityHashCode(def1);
                int id2 = System.identityHashCode(def2);
                return ((Integer)id1).compareTo(id2);
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case NUMBER:
            case TIMESTAMP:
            case STRING:
                return 1;
            case BOOLEAN:
            case BINARY:
            case FIXED_BINARY:
                return -1;
            default:
                break;
            }
            break;
        }
        case BOOLEAN: {
            if (v2.isNumeric() ||
                tc2 == Type.TIMESTAMP ||
                tc2 == Type.STRING ||
                tc2 == Type.ENUM) {
                return 1;
            }
            if (tc2 == Type.BOOLEAN) {
                return ((BooleanValueImpl)v1).compareTo(v2);
            }
            if (tc2 == Type.BINARY || tc2 == Type.FIXED_BINARY) {
                return -1;
            }
            break;
        }
        case TIMESTAMP: {
            switch (tc2) {
            case TIMESTAMP:
                return ((TimestampValueImpl)v1).compareTo(v2);
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case NUMBER:
                return 1;
            case ENUM:
            case STRING:
            case BOOLEAN:
            case BINARY:
            case FIXED_BINARY:
                return -1;
            default:
                break;
            }
            break;
        }
        case BINARY:
        case FIXED_BINARY: {
            if (tc2 == Type.BINARY || tc2 == Type.FIXED_BINARY) {
                String str1 = v1.toString();
                String str2 = v2.toString();
                return str1.compareTo(str2);
            }
            return 1;
        }
        default:
            break;
        }

        throw new IllegalArgumentException(
            "Unexpected comparison of value of type " + tc1 +
            " with value of type " + tc2);
    }

    /**
     * The order of 3 kinds of special NULL values is:
     *  Empty < JSON null < SQL null
     */
    private static int compareSpecialValues(FieldValueImpl v1,
                                            FieldValueImpl v2)  {

        if (v1.isEMPTY()) {
            return v2.isEMPTY() ? 0 : -1;
        }
        if (v1.isJsonNull()) {
            return v2.isJsonNull() ? 0 : (v2.isEMPTY() ? 1 : -1);
        }
        return v2.isNull() ? 0 : 1;
    }

    /**
     * Casts the value to int, possibly with loss of information about
     * magnitude, precision or sign.
     */
    public int castAsInt() {
        throw new ClassCastException(
            "Value can not be cast to an integer: " + getClass());
    }

    /**
     * Casts the value to long, possibly with loss of information about
     * magnitude, precision or sign.
     */
    public long castAsLong() {
        throw new ClassCastException(
            "Value can not be cast to a long: " + getClass());
    }

    /**
     * Casts the value to float, possibly with loss of information about
     * magnitude, precision or sign.
     */
    public float castAsFloat() {
        throw new ClassCastException(
            "Value can not be cast to a float: " + getClass());
    }

    /**
     * Casts the value to double, possibly with loss of information about
     * magnitude, precision or sign.
     */
    public double castAsDouble() {
        throw new ClassCastException(
            "Value can not be cast to a double: " + getClass());
    }

    public NumberValueImpl castAsNumber() {
        throw new ClassCastException(
            "Value can not be cast to a Number: " + getClass());
    }

    /**
     * Casts the value to BigDecimal.
     */
    public BigDecimal castAsDecimal() {
        throw new ClassCastException(
            "Value can not be cast to a Number: " + getClass());
    }

    public String castAsString() {
        throw new ClassCastException(
            "Value can not be cast to a String: " + getClass());
    }

    FieldValue castToSuperType(FieldDefImpl targetDef) {

        if (isNull()) {
            return this;
        }

        FieldDefImpl valDef = getDefinition();

        if (targetDef.isWildcard() ||
            targetDef.equals(valDef)) {
            return this;
        }

        if (valDef.isMRCounter()) {
            /* Return a non-CRDT value with the same data type. */
            return convertMRCounterToPlainValue().castToSuperType(targetDef);
        }

        if (targetDef.isMRCounter()) {
            throw new IllegalArgumentException("The target def cannot " +
                "be an MRCounter. ");
        }

        assert(valDef.isSubtype(targetDef));

        switch (getType()) {

        case INTEGER: {
            /* target must be long or number */
            return (targetDef.isLong() ?
                    targetDef.createLong(asInteger().get()) :
                    targetDef.createNumber(asInteger().get()));
        }

        case LONG: {
            /* target must be number */
            assert targetDef.isNumber();
            return targetDef.createNumber(asLong().get());
        }

        case FLOAT: {
            /* target must be double or number */
            return (targetDef.isDouble() ?
                    targetDef.createDouble(asFloat().get()) :
                    targetDef.createNumber(asFloat().get()));
        }

        case DOUBLE: {
            /* target must be number */
            assert targetDef.isNumber();
            return targetDef.createNumber(asDouble().get());
        }

        case TIMESTAMP: {
            /* target must be Timestamp */
            assert targetDef.isTimestamp();
            int toPrec = targetDef.asTimestamp().getPrecision();
            return ((TimestampValueImpl)asTimestamp()).castToPrecision(toPrec);
        }

        case ARRAY: {
            FieldDefImpl elemDef = ((ArrayDefImpl)targetDef).getElement();
            ArrayValueImpl arr = (ArrayValueImpl)this;
            ArrayValueImpl newarr = ((ArrayDefImpl)targetDef).createArray();

            for (FieldValue e : arr.getArrayInternal()) {
                FieldValueImpl elem = (FieldValueImpl)e;
                newarr.addInternal((FieldValueImpl)elem.castToSuperType(elemDef));
            }
            return newarr;
        }

        case MAP: {
            FieldDefImpl targetElemDef = ((MapDefImpl)targetDef).getElement();
            MapValueImpl map = (MapValueImpl)this;
            MapValueImpl newmap = ((MapDefImpl)targetDef).createMap();

            for (Map.Entry<String, FieldValue> entry : map.getMap().entrySet()) {
                String key = entry.getKey();
                FieldValueImpl elem = (FieldValueImpl)entry.getValue();
                newmap.put(key, elem.castToSuperType(targetElemDef));
            }
            return newmap;
        }

        case RECORD: {
            RecordValueImpl rec = (RecordValueImpl)this;
            RecordValueImpl newrec = ((RecordDefImpl)targetDef).createRecord();
            int numFields = rec.getNumFields();
            RecordDefImpl recTargetDef = (RecordDefImpl)targetDef;
            for (int i = 0; i < numFields; ++i) {
                FieldValueImpl fval = rec.get(i);
                if (fval != null) {
                    FieldDefImpl targetFieldDef = recTargetDef.getFieldDef(i);
                    newrec.put(i, fval.castToSuperType(targetFieldDef));
                }
            }
            return newrec;
        }

        /* these have no super types */
        case NUMBER:
        case STRING:
        case ENUM:
        case BOOLEAN:
        case BINARY:
        case FIXED_BINARY: {
            return this;
        }

        default:
            throw new IllegalStateException("Unexpected type: " + getType());
        }
    }

    /**
     * Create a non-CRDT value that has the same data type and
     * returns the same value for get().
     *
     * @return a non-CRDT value.
     */
    public FieldValueImpl convertMRCounterToPlainValue() {
        throw new ClassCastException(
            "Value is not an MRCounter: " + getClass());
    }

    /**
     * Cast this MRCounter to the target MRCounter type.
     *
     * @return an MRCounter with the specified type.
     */
    public FieldValueImpl castToOtherMRCounter(
        @SuppressWarnings("unused")FieldDefImpl targetDef) {
        throw new ClassCastException(
            "Value is not an MRCounter: " + getClass());
    }

    @Override
    public byte[] getFixedBytes() {
        throw new ClassCastException(
            "Value is not a Fixed binary: " + getClass());
    }

    @Override
    public byte[] getNumberBytes() {
        throw new ClassCastException(
            "Value is not a Number: " + getClass());
    }

    @Override
    public byte[] getTimestampBytes() {
        throw new ClassCastException(
            "Value is not a Timestamp: " + getClass());
    }

    @Override
    public RecordValueSerializer asRecordValueSerializer() {
        throw new ClassCastException
        ("Field is not a Record: " + getClass());
    }

    @Override
    public MapValueSerializer asMapValueSerializer() {
        throw new ClassCastException
        ("Field is not a Map: " + getClass());
    }

    @Override
    public ArrayValueSerializer asArrayValueSerializer() {
        throw new ClassCastException
        ("Field is not an Array: " + getClass());
    }
}
