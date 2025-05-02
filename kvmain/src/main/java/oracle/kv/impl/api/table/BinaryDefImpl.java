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

import oracle.kv.table.BinaryDef;

/**
 * BinaryDefImpl implements the BinaryDef interface.
 */
public class BinaryDefImpl extends FieldDefImpl implements BinaryDef {

    private static final long serialVersionUID = 1L;

    BinaryDefImpl(String description) {
        super(Type.BINARY, description);
    }

    BinaryDefImpl() {
        super(Type.BINARY);
    }

    /**
     * Constructor for FastExternalizable
     */
    BinaryDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.BINARY);
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        super.writeFastExternal(out, serialVersion);
    }

    /*
     * Public api methods from Object and FieldDef
     */

    @Override
    public BinaryDefImpl clone() {
        return FieldDefImpl.Constants.binaryDef;
    }

    @Override
    public boolean equals(Object other) {

        return (other instanceof BinaryDefImpl);
    }

    @Override
    public BinaryDef asBinary() {
        return this;
    }

    @Override
    public BinaryValueImpl createBinary(byte[] value) {
        return new BinaryValueImpl(value);
    }

    /*
     * Public api methods from BinaryDef
     */

    @Override
    public BinaryValueImpl fromString(String base64) {
        return createBinary(TableJsonUtils.decodeBase64(base64));
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isBinary() ||
            superType.isAny() ||
            superType.isAnyAtomic()) {
            return true;
        }
        return false;
    }
}
