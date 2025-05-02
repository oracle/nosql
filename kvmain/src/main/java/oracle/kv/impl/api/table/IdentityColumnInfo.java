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
import java.io.Serializable;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Container object for identity column information.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class IdentityColumnInfo implements Serializable, FastExternalizable {
    private static final long serialVersionUID = 1L;

    private final int identityColumn;
    private final boolean identityGeneratedAlways;
    private final boolean identityOnNull;

    public IdentityColumnInfo(int identityColumn,
                              boolean identityGeneratedAlways,
                              boolean identityOnNull) {
        this.identityColumn = identityColumn;
        this.identityGeneratedAlways = identityGeneratedAlways;
        this.identityOnNull = identityOnNull;
    }

    /**
     * Constructor for FastExternalizable
     */
    IdentityColumnInfo(DataInput in,
                       @SuppressWarnings("unused") short serialVersion)
        throws IOException
    {
        identityColumn = SerializationUtil.readPackedInt(in);
        identityGeneratedAlways = in.readBoolean();
        identityOnNull = in.readBoolean();
    }

    /**
     * Writes this object to the output stream. Format:
     *
     * <ol>
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@code identityColumn}
     * <li> ({@code boolean}) {@code identityGeneratedAlways}
     * <li> ({@code boolean}) {@code isIdentityOnNull}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {
        SerializationUtil.writePackedInt(out, identityColumn);
        out.writeBoolean(identityGeneratedAlways);
        out.writeBoolean(identityOnNull);
    }

    public int getIdentityColumn() {
        return identityColumn;
    }

    public boolean isIdentityGeneratedAlways() {
        return identityGeneratedAlways;
    }

    public boolean isIdentityOnNull() {
        return identityOnNull;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof IdentityColumnInfo)) {
            return false;
        }

        IdentityColumnInfo that = (IdentityColumnInfo)o;
        if (this.identityColumn != that.identityColumn) {
            return false;
        }

        if (this.identityGeneratedAlways != that.identityGeneratedAlways) {
            return false;
        }

        if (this.identityOnNull != that.identityOnNull) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int value = 31;
        value = value * 31 + Integer.hashCode(identityColumn);
        value = value * 31 + Boolean.hashCode(identityGeneratedAlways);
        value = value * 31 + Boolean.hashCode(identityOnNull);
        return value;
    }

}
