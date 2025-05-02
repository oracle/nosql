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

import oracle.kv.table.AnyJsonAtomicDef;

public class AnyJsonAtomicDefImpl
    extends FieldDefImpl
    implements AnyJsonAtomicDef {

    private static final long serialVersionUID = 1L;

    AnyJsonAtomicDefImpl() {
        super(Type.ANY_JSON_ATOMIC, "");
    }

    /**
     * Constructor for FastExternalizable
     */
    AnyJsonAtomicDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.ANY_JSON_ATOMIC);
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
    public AnyJsonAtomicDefImpl clone() {
        return FieldDefImpl.Constants.anyJsonAtomicDef;
    }

    @Override
    public AnyJsonAtomicDef asAnyJsonAtomic() {
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
    public boolean equals(Object other) {
        return other instanceof AnyJsonAtomicDefImpl;
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {
        Type st = superType.getType();
        return (st == Type.ANY ||
                st == Type.JSON ||
                st == Type.ANY_ATOMIC ||
                st == Type.ANY_JSON_ATOMIC);
    }
}
