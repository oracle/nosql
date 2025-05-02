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

import oracle.kv.table.AnyAtomicDef;

public class AnyAtomicDefImpl extends FieldDefImpl implements AnyAtomicDef {

    private static final long serialVersionUID = 1L;

    AnyAtomicDefImpl() {
        super(Type.ANY_ATOMIC, "");
    }

    /**
     * Constructor for FastExternalizable
     */
    AnyAtomicDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.ANY_ATOMIC);
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
    public AnyAtomicDefImpl clone() {
        return FieldDefImpl.Constants.anyAtomicDef;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof AnyAtomicDefImpl;
    }

    @Override
    public AnyAtomicDef asAnyAtomic() {
        return this;
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isPrecise() {
        return false;
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {
        return (superType.getType() == Type.ANY ||
                superType.getType() == Type.ANY_ATOMIC);
    }
}
