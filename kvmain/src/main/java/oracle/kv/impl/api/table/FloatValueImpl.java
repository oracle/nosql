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
import java.math.BigDecimal;

import oracle.kv.impl.util.SizeOf;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FloatValue;

public class FloatValueImpl extends FieldValueImpl implements FloatValue {

    private static final long serialVersionUID = 1L;

    protected float value;

    FloatValueImpl(float value) {
        this.value = value;
    }

    /**
     * This constructor creates FloatValueImpl from the String format used for
     * sorted keys.
     */
    FloatValueImpl(String keyValue) {
        this.value = SortableString.floatFromSortable(keyValue);
    }

    /**
     * Constructor for FastExternalizable
     */
    FloatValueImpl(DataInput in) throws IOException {
        value = in.readFloat();
    }

    /**
     * Writes this object to the output stream. Format:
     * <ol>
     * <li> ({@link FieldValueImpl}) {@code super}
     * <li> ({@code float}) {@code value}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        out.writeFloat(value);
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.FLOAT_VALUE;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public FloatValueImpl clone() {
        return new FloatValueImpl(value);
    }

    @Override
    public long sizeof() {
        return SizeOf.OBJECT_OVERHEAD + 4;
    }

    @Override
    public int hashCode() {
        return ((Float) value).hashCode();
    }

    @Override
    public boolean equals(Object other) {

        /* == doesn't work for the various Float constants */
        if (other instanceof FloatValueImpl) {
            return Double.compare(value, ((FloatValueImpl)other).get()) == 0;
        }
        return false;
    }

    /**
     * Allow comparison against Double to succeed
     */
    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof FloatValueImpl) {
            return Double.compare(value, ((FloatValueImpl)other).get());
        }
        throw new ClassCastException("Object is not an DoubleValue");
    }

    @Override
    public String toString() {
        return Float.toString(value);
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.FLOAT;
    }

    @Override
    public FloatDefImpl getDefinition() {
        return FieldDefImpl.Constants.floatDef;
    }

    @Override
    public FloatValue asFloat() {
        return this;
    }

    @Override
    public boolean isFloat() {
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
     * Public api methods from FloatValue
     */

    @Override
    public float get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public float getFloat() {
        return value;
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public void setFloat(float v) {
        value = v;
    }

    @Override
    public int castAsInt() {
        return (int)value;
    }

    @Override
    public long castAsLong() {
        return (long)value;
    }

    @Override
    public float castAsFloat() {
        return value;
    }

    @Override
    public double castAsDouble() {
        return value;
    }

    @Override
    public BigDecimal castAsDecimal() {
        return new BigDecimal(value);
    }

    @Override
    public NumberValueImpl castAsNumber() {
        return new NumberValueImpl(new BigDecimal(value));
    }

    @Override
    public String castAsString() {
        return Float.toString(value);
    }

    @Override
    public FieldValueImpl getNextValue() {
        return new FloatValueImpl(Math.nextUp(value));
    }

    @Override
    public FieldValueImpl getMinimumValue() {
        return new FloatValueImpl(Float.MIN_VALUE);
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        return toKeyString(value);
    }

    static String toKeyString(float value) {
        return SortableString.toSortable(value);
    }
}
