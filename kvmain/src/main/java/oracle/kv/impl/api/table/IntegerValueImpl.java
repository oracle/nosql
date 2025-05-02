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

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.impl.util.SizeOf;
import oracle.kv.impl.util.SortableString;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.IntegerValue;

public class IntegerValueImpl extends FieldValueImpl implements IntegerValue {

    private static final long serialVersionUID = 1L;

    public static IntegerValueImpl zero = new IntegerValueImpl(0);

    protected int value;

    public IntegerValueImpl(int value) {
        this.value = value;
    }

    /**
     * This constructor creates IntegerValueImpl from the String format used for
     * sorted keys.
     */
    IntegerValueImpl(String keyValue) {
        this.value = SortableString.intFromSortable(keyValue);
    }

    /**
     * Constructor for FastExternalizable
     */
    IntegerValueImpl(DataInput in) throws IOException {
        value = readPackedInt(in);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldValueImpl}) {@code super}
     * <li>  <i>[Optional]</i>
     *       ({@code boolean}) {@code false}
     * <li> ({@link SerializationUtil#writePackedInt packed int}) {@code value}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writePackedInt(out, value);
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.INTEGER_VALUE;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public IntegerValueImpl clone() {
        return new IntegerValueImpl(value);
    }

    @Override
    public long sizeof() {
        return SizeOf.OBJECT_OVERHEAD + 4;
    }

    @Override
    public int hashCode() {
        return ((Integer) value).hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof IntegerValueImpl) {
            return value == ((IntegerValueImpl)other).get();
        }
        return false;
    }

    /**
     * Allow comparisons against LongValue to succeed.
     */
    @Override
    public int compareTo(FieldValue other) {

        if (other instanceof IntegerValueImpl) {
            return compare(get(), ((IntegerValueImpl)other).get());
        } else if (other instanceof LongValueImpl) {
            return LongValueImpl.compare(get(), ((LongValueImpl)other).get());
        }

        throw new ClassCastException("Object is not comparable to IntegerValue");
    }

    public static int compare(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    @Override
    public String toString() {
        return Integer.toString(get());
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.INTEGER;
    }

    @Override
    public IntegerDefImpl getDefinition() {
        return FieldDefImpl.Constants.integerDef;
    }

    @Override
    public IntegerValue asInteger() {
        return this;
    }

    @Override
    public boolean isInteger() {
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
     * Public api methods from IntegerValue
     */

    @Override
    public int get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */

    @Override
    public int getInt() {
        return get();
    }

    @Override
    public long getLong() {
        return get();
    }

    @Override
    public void setInt(int v) {
        value = v;
    }

    @Override
    public int castAsInt() {
        return get();
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
        return Integer.toString(get());
    }

    @Override
    FieldValueImpl getNextValue() {
        if (value == Integer.MAX_VALUE) {
            return null;
        }
        return new IntegerValueImpl(value + 1);
    }

    @Override
    FieldValueImpl getMinimumValue() {
        return new IntegerValueImpl(Integer.MIN_VALUE);
    }

    @Override
    public String formatForKey(FieldDef field, int storageSize) {
        return toKeyString(value, field, storageSize);
    }

    void validateStorageSize(int size) {
        int requiredSize = SortableString.encodingLength(value);
        if (requiredSize > size) {
            throw new IllegalArgumentException
                ("Integer value is too large for primary key storage size. " +
                 "It requires " + requiredSize + " bytes, and size must be " +
                 "less than or equal to " + size + " bytes");
        }
    }

    static String toKeyString(int val, FieldDef def, int storageSize) {
        /*
         * Use a schema-defined storage length if available. If not, use
         * the one passed in, which comes from a primary key constraint on
         * this field in the table's primary key.
         */
        int len =
            (def != null ? ((IntegerDefImpl) def).getEncodingLength() : 0);

        /* if len is 0 or the max (5) and storageSize is specified, use it */
        if ((len == 0 || len == 5) && storageSize != 0) {
            len = storageSize;
        }
        return SortableString.toSortable(val, len);
    }

}
