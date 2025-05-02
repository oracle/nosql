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
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerializationUtil;   /* for Javadoc */
import oracle.kv.table.FixedBinaryDef;

/**
 * FixedBinaryDefImpl implements the FixedBinaryDef interface.
 */
public class FixedBinaryDefImpl extends FieldDefImpl implements FixedBinaryDef {

    private static final long serialVersionUID = 1L;

    /* requires names for records. */
    private String name;

    private final int size;

    FixedBinaryDefImpl(int size, String description) {

        super(Type.FIXED_BINARY, description);
        this.size = size;

        validate();
    }

    FixedBinaryDefImpl(String name, int size, String description) {

        this(size, description);

        if (name == null) {
            throw new IllegalArgumentException
                ("FixedBinaryDef requires a name");
        }

        this.name = name;
    }

    FixedBinaryDefImpl(String name, int size) {
        this(name, size, null);
    }

    private FixedBinaryDefImpl(FixedBinaryDefImpl impl) {
        super(impl);
        this.name = impl.name;
        this.size = impl.size;
    }

    /**
     * Constructor for FastExternalizable
     */
    FixedBinaryDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.FIXED_BINARY);
        name = readString(in, serialVersion);
        size = readPackedInt(in);
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link FieldDefImpl}) {@code super}
     * <li> ({@link SerializationUtil#writeString String}) {@code name}
     * <li> ({@link SerializationUtil#writePackedInt packed int}) {@code size}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, name);
        writePackedInt(out, size);
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public FixedBinaryDefImpl clone() {
        return new FixedBinaryDefImpl(this);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + size + name.hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (other instanceof FixedBinaryDefImpl) {
            FixedBinaryDefImpl otherDef = (FixedBinaryDefImpl) other;
            return (size == otherDef.size);
        }
        return false;
    }

    @Override
    public FixedBinaryDef asFixedBinary() {
        return this;
    }

    @Override
    public FixedBinaryValueImpl createFixedBinary(byte[] value) {
        validateValue(value);
        return new FixedBinaryValueImpl(value, this);
    }

    /*
     * Public api methods from FixedBinaryDef
     */

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getFieldName() {
        return name;
    }

    @Override
    public FixedBinaryValueImpl fromString(String base64) {
        return createFixedBinary(TableJsonUtils.decodeBase64(base64));
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isFixedBinary()) {
            return this.equals(superType);
        }

        if (superType.isBinary() ||
            superType.isAny() ||
            superType.isAnyAtomic()) {
            return true;
        }

        return false;
    }

    /*
     * local methods
     */

    public void setName(String n) {
        name = n;

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                "Fixed binary types require a name");
        }
    }

    private void validate() {
        if (size <= 0) {
            throw new IllegalArgumentException
                ("FixedBinaryDef size limit must be a positive integer");
        }
    }

    public void validateValue(byte[] value) {
        if (value.length != size) {
            throw new IllegalArgumentException
                ("Invalid length for FixedBinary array, it must be " + size +
                 ", and it is " + value.length);
        }
    }
}
