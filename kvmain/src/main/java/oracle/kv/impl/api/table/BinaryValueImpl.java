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
import oracle.kv.table.BinaryValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;

public class BinaryValueImpl extends FieldValueImpl implements BinaryValue {

    private static final long serialVersionUID = 1L;

    final private byte[] value;

    BinaryValueImpl(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException
                ("Binary values cannot be null");
        }
        this.value = value;
    }

    /**
     * Constructor for FastExternalizable
     */
    BinaryValueImpl(DataInput in) throws IOException {
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
        return ValueType.BINARY_VALUE;
    }

    /*
     * Public api methods from Object and FieldValue
     */

    @Override
    public BinaryValueImpl clone() {
        return new BinaryValueImpl(value);
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
        if (other instanceof BinaryValueImpl) {
            return Arrays.equals(value, ((BinaryValueImpl)other).get());
        }
        return false;
    }

    /**
     * Returns 0 if the two values are equal in terms of length and byte
     * content, otherwise it returns -1.
     */
    @Override
    public int compareTo(FieldValue otherValue) {
        return (equals(otherValue) ? 0 : -1);
    }

   @Override
    public String toString() {
        return TableJsonUtils.encodeBase64(value);
    }

    @Override
    public FieldDef.Type getType() {
        return FieldDef.Type.BINARY;
    }

    @Override
    public BinaryDefImpl getDefinition() {
        return FieldDefImpl.Constants.binaryDef;
    }

    @Override
    public BinaryValue asBinary() {
        return this;
    }

    @Override
    public boolean isBinary() {
        return true;
    }

    @Override
    public boolean isAtomic() {
        return true;
    }

    /*
     * Public api methods from BinaryValue
     */

    @Override
    public byte[] get() {
        return value;
    }

    @Override
    public void toStringBuilder(StringBuilder sb,
                                DisplayFormatter formatter) {
        sb.append("\"");
        sb.append(TableJsonUtils.encodeBase64(value));
        sb.append("\"");
    }

    /*
     * Methods from FieldValueImpl
     */

    @Override
    public byte[] getBytes() {
        return value;
    }

    /*
     * local methods
     */
    public static BinaryValueImpl create(byte[] value) {
        return new BinaryValueImpl(value);
    }
}
