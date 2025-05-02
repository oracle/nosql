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

import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.impl.util.SizeOf;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.NumberValue;

/**
 * This class represents a BigDecimal value.
 */
public class NumberValueImpl extends FieldValueImpl implements NumberValue {

    private static final long serialVersionUID = 1L;

    private byte[] value;

    NumberValueImpl(long value) {
        this.value = NumberUtils.serialize(value);
    }

    NumberValueImpl(BigDecimal value) {
        this.value = NumberUtils.serialize(value);
    }

    public NumberValueImpl(byte[] value) {
        this.value = value;
    }

    /**
     * This constructor creates DecimalValueImpl from the String format used for
     * sorted keys.
     */
    NumberValueImpl(String keyValue) {
        value = SortableString.bytesFromSortable(keyValue);
    }

    /**
     * Constructor for FastExternalizable
     */
    NumberValueImpl(DataInput in) throws IOException {
        value = readByteArray(in);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldValueImpl}) {@code super}
     * <li> ({@link SerializationUtil#writeByteArray byte array}) {@code value}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeByteArray(out, value);
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.NUMBER_VALUE;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public NumberValueImpl clone() {
        return new NumberValueImpl(value);
    }

    @Override
    public long sizeof() {
        return (SizeOf.OBJECT_OVERHEAD +
                SizeOf.OBJECT_REF_OVERHEAD +
                SizeOf.byteArraySize(value.length));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof NumberValueImpl) {
            return Arrays.equals(value, ((NumberValueImpl) other).value);
        }
        return false;
    }

    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof NumberValueImpl) {
            return compareNumber((NumberValueImpl)other);
        } else if (other.isNumeric()){
            BigDecimal otherVal = null;
            switch (other.getType()) {
            case INTEGER:
                otherVal = BigDecimal.valueOf(other.asInteger().get());
                break;
            case LONG:
                otherVal = BigDecimal.valueOf(other.asLong().get());
                break;
            case FLOAT:
                otherVal = BigDecimal.valueOf(other.asFloat().get());
                break;
            case DOUBLE:
                otherVal = BigDecimal.valueOf(other.asDouble().get());
                break;
            default:
                break;
            }
            assert(otherVal != null);
            return get().compareTo(otherVal);
        }
        throw new ClassCastException("Object is not a numeric type");
    }

    protected int compareNumber(NumberValueImpl other) {
        return IndexImpl.compareUnsignedBytes
            (value, other.value);
    }

    @Override
    public String toString() {
        return get().toString();
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.NUMBER;
    }

    @Override
    public NumberDefImpl getDefinition() {
        return FieldDefImpl.Constants.numberDef;
    }

    @Override
    public NumberValue asNumber() {
        return this;
    }

    @Override
    public boolean isNumber() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    @Override
    public boolean isNumeric() {
        return true;
    }

    /*
     * Public api methods from NumberValue
     */

    @Override
    public BigDecimal get() {
        return getDecimal();
    }

    /*
     * FieldValueImpl internal api methods
     */
    @Override
    public BigDecimal getDecimal() {
        Object val = NumberUtils.deserialize(value, true);
        assert(val instanceof BigDecimal);
        return (BigDecimal)val;
    }

    @Override
    public void setDecimal(BigDecimal v) {
        value = NumberUtils.serialize(v);
    }

    @Override
    public int castAsInt() {
        Object val = getNumericValue();
        if (val instanceof Integer) {
            return ((Integer)val).intValue();
        }
        if (val instanceof Long) {
            return ((Long)val).intValue();
        }

        assert(val instanceof BigDecimal);
        return ((BigDecimal)val).intValue();
    }

    @Override
    public long castAsLong() {
        Object val = getNumericValue();
        if (val instanceof Integer) {
            return ((Integer)val).longValue();
        }
        if (val instanceof Long) {
            return ((Long)val).longValue();
        }

        assert(val instanceof BigDecimal);
        return getDecimal().longValue();
    }

    @Override
    public float castAsFloat() {
        Object val = getNumericValue();
        if (val instanceof Integer) {
            return ((Integer)val).floatValue();
        }
        if (val instanceof Long) {
            return ((Long)val).floatValue();
        }

        assert(val instanceof BigDecimal);
        return ((BigDecimal)val).floatValue();
    }

    @Override
    public double castAsDouble() {
        Object val = getNumericValue();
        if (val instanceof Integer) {
            return ((Integer)val).doubleValue();
        }
        if (val instanceof Long) {
            return ((Long)val).doubleValue();
        }

        assert(val instanceof BigDecimal);
        return ((BigDecimal)val).doubleValue();
    }

    @Override
    public NumberValueImpl castAsNumber() {
        return this;
    }

    @Override
    public BigDecimal castAsDecimal() {
        return getDecimal();
    }

    @Override
    public String castAsString() {
        Object val = getNumericValue();
        return val.toString();
    }

    protected Object getNumericValue() {
        return NumberUtils.deserialize(value, false);
    }

    @Override
    public String formatForKey(FieldDef field1, int storageSize) {
        return toKeyString(value);
    }

    @Override
    FieldValueImpl getNextValue() {
        return new NumberValueImpl(NumberUtils.nextUp(value));
    }

    @Override
    FieldValueImpl getMinimumValue() {
        return new NumberValueImpl(NumberUtils.getNegativeInfinity());
    }

    @Override
    public byte[] getBytes() {
        return value;
    }

    static String toKeyString(byte[] bytes) {
        return SortableString.toSortable(bytes);
    }

    @Override
    public byte[] getNumberBytes() {
        return getBytes();
    }
}
