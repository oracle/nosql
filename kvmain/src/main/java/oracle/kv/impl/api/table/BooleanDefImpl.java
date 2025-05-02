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

import oracle.kv.table.BooleanDef;

/**
 * BooleanDefImpl implements the BooleanDef interface.
 */
public class BooleanDefImpl extends FieldDefImpl implements BooleanDef {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor requiring all fields.
     */
    BooleanDefImpl(final String description) {
        super(Type.BOOLEAN, description);
    }

    /**
     * This constructor defaults most fields.
     */
    BooleanDefImpl() {
        super(Type.BOOLEAN);
    }
    
    /**
     * Constructor for FastExternalizable
     */
    BooleanDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.BOOLEAN);
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
    public BooleanDefImpl clone() {
        return FieldDefImpl.Constants.booleanDef;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof BooleanDefImpl;
    }

    @Override
    public boolean isValidKeyField() {
        return true;
    }

    @Override
    public boolean isValidIndexField() {
        return true;
    }

    @Override
    public BooleanDef asBoolean() {
        return this;
    }

    @Override
    public BooleanValueImpl createBoolean(boolean value) {
        return BooleanValueImpl.create(value);
    }

    @Override
    BooleanValueImpl createBoolean(String value) {
        return BooleanValueImpl.create(value);
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isSubtype(FieldDefImpl superType) {

        if (superType.isBoolean() ||
            superType.isAny() ||
            superType.isAnyAtomic() ||
            superType.isAnyJsonAtomic() ||
            superType.isJson()) {
            return true;
        }
        return false;
    }
}
