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

import static oracle.kv.impl.util.SerializationUtil.readPackedLong;
import static oracle.kv.impl.util.SerializationUtil.writePackedLong;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.impl.util.SizeOf;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.LongValue;

public class LongValueImpl extends FieldValueImpl implements LongValue {

    private static final long serialVersionUID = 1L;

    public static LongValueImpl ZERO = new LongValueImpl(0L);

    protected long value;

    LongValueImpl(long value) {
        this.value = value;
    }

    /**
     * This constructor creates LongValueImpl from the String format used for
     * sorted keys.
     */
    LongValueImpl(String keyValue) {
        this.value = SortableString.longFromSortable(keyValue);
    }

    /**
     * Constructor for FastExternalizable
     */
    LongValueImpl(DataInput in) throws IOException {
        value = readPackedLong(in);
    }


    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldValueImpl}) {@code super}
     * <li>  <i>[Optional]</i>
     *       ({@code boolean}) {@code false}
     * <li> ({@link SerializationUtil#writePackedLong packed long}) {@code value}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writePackedLong(out, value);
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.LONG_VALUE;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public LongValueImpl clone() {
        return new LongValueImpl(value);
    }

    @Override
    public long sizeof() {
        return SizeOf.OBJECT_OVERHEAD + 8;
    }

    @Override
    public int hashCode() {
        return ((Long) value).hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof LongValueImpl) {
            return value == ((LongValueImpl)other).get();
        }
        return false;
    }

    /**
     * Allow comparison to IntegerValue succeed.
     */
    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof LongValueImpl) {
            return compare(get(), ((LongValueImpl)other).get());
        } else if (other instanceof IntegerValueImpl) {
            return compare(get(), ((IntegerValueImpl)other).get());
        }

        throw new ClassCastException("Value is not comparable to LongValue");
    }

    public static int compare(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public String toString() {
        return Long.toString(get());
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.LONG;
    }

    @Override
    public LongDefImpl getDefinition() {
        return FieldDefImpl.Constants.longDef;
    }

    @Override
    public LongValue asLong() {
        return this;
    }

    @Override
    public boolean isLong() {
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
     * Public api methods from LongValue
     */

    @Override
    public long get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public long getLong() {
        return get();
    }

    @Override
    public void setLong(long v) {
        value = v;
    }

    @Override
    public int castAsInt() {
        return (int)get();
    }

    @Override
    public long castAsLong() {
        return get();
    }

    @Override
    public float castAsFloat() {
        return get();
    }

    @Override
    public double castAsDouble() {
        return get();
    }

    @Override
    public NumberValueImpl castAsNumber() {
        return new NumberValueImpl(get());
    }

    @Override
    public BigDecimal castAsDecimal() {
        return new BigDecimal(get());
    }

    @Override
    public String castAsString() {
        return Long.toString(get());
    }

    @Override
    FieldValueImpl getNextValue() {
        if (value == Long.MAX_VALUE) {
            return null;
        }
        return new LongValueImpl(get() + 1L);
    }

    @Override
    FieldValueImpl getMinimumValue() {
        return new LongValueImpl(Long.MIN_VALUE);
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        return toKeyString(value, field);
    }

    static String toKeyString(long value, FieldDef field) {
        int len = (field != null) ? ((LongDefImpl)field).getEncodingLength() : 0;
        return SortableString.toSortable(value, len);
    }
}
