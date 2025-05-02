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
import java.util.Arrays;

import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.impl.util.SizeOf;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.FixedBinaryValue;

import com.fasterxml.jackson.core.Base64Variants;

public class FixedBinaryValueImpl extends FieldValueImpl
    implements FixedBinaryValue {

    private static final long serialVersionUID = 1L;

    private final byte[] value;

    private final FixedBinaryDefImpl def;

    FixedBinaryValueImpl(byte[] value, FixedBinaryDefImpl def) {
        this.value = value;
        this.def = def;
    }

    /**
     * Constructor for FastExternalizable
     */
    FixedBinaryValueImpl(DataInput in, FixedBinaryDefImpl def)
            throws IOException {
        value = readByteArray(in);
        this.def = def;
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
        /* def is not written */
    }

    @Override
    protected ValueType getValueType() {
        return ValueType.FIXED_BINARY_VALUE;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public FixedBinaryValueImpl clone() {
        return new FixedBinaryValueImpl(value, def);
    }

    @Override
    public long sizeof() {
        return (SizeOf.OBJECT_OVERHEAD +
                2 * SizeOf.OBJECT_REF_OVERHEAD +
                SizeOf.byteArraySize(value.length));
    }

   @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof FixedBinaryValueImpl) {
            FixedBinaryValueImpl otherImpl = (FixedBinaryValueImpl)other;
            return (def.equals(otherImpl.def) &&
                    Arrays.equals(value, otherImpl.get()));
        }
        return false;
    }

    /**
     * TODO: maybe use JE comparator algorithm.
     * For now, all binary is equal
     */
    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof FixedBinaryValueImpl) {
            return 0;
        }
        throw new ClassCastException
            ("Object is not an FixedBinaryValue");
    }

    @Override
    public String toString() {
        return (value != null) ?
            Base64Variants.getDefaultVariant().encode(value, false) :
            "null";
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.FIXED_BINARY;
    }

    @Override
    public FixedBinaryDefImpl getDefinition() {
        return def;
    }

    @Override
    public FixedBinaryValue asFixedBinary() {
        return this;
    }

    @Override
    public boolean isFixedBinary() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    /*
     * Public api methods from FixedBinaryValue
     */

    @Override
    public byte[] get() {
        return value;
    }

    /*
     * FieldValueImpl internal api methods
     */
    @Override
    public byte[] getBytes() {
        return value;
    }

    @Override
    public void toStringBuilder(StringBuilder sb, DisplayFormatter formatter) {
        sb.append(Base64Variants.getDefaultVariant().encode(value, true));
    }

    @Override
    public byte[] getFixedBytes() {
        return getBytes();
    }
}
