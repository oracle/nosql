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

/**
 * EmptyDefImpl represents the "empty" data type; the type that contains no
 * values. EmptyDefImpl cannot be used when defining the schema of a table
 * (i.e., in the CREATE TABLE statement). Instead, it is used to describe
 * the result of a query expression, when the query processor can infer that
 * the result will always be empty.
 */
public class EmptyDefImpl extends FieldDefImpl {

    private static final long serialVersionUID = 1L;

    EmptyDefImpl() {
        super(Type.EMPTY, "");
    }

    /**
     * Constructor for FastExternalizable
     */
    EmptyDefImpl(DataInput in, short serialVersion) throws IOException {
        super(in, serialVersion, Type.EMPTY);
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
    public EmptyDefImpl clone() {
        return FieldDefImpl.Constants.emptyDef;
    }

    @Override
    public boolean equals(Object other) {
        return (other instanceof EmptyDefImpl);
    }

    /*
     * FieldDefImpl internal api methods
     */

    @Override
    public boolean isPrecise() {
        return true;
    }

    @Override
    public boolean isSubtype(FieldDefImpl superType) {
        return superType.getType() == Type.EMPTY;
    }
}
