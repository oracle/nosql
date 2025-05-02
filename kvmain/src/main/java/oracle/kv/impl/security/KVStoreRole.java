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
package oracle.kv.impl.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.FastExternalizable;

/**
 * KVStore role defined in R3. Reserved to maintain compatibility during
 * online upgrade.
 */
public enum KVStoreRole implements FastExternalizable {
    /*
     * Any authenticated user, or internal. Only reserved for upgrade,
     * unused since R3.Q3.
     */
    AUTHENTICATED(0),

    /* KVStore infrastructure. */
    INTERNAL(1),

    /* KVStore admin user. Only reserved for upgrade, unused since R3.Q3. */
    ADMIN(2);

    private static final KVStoreRole[] VALUES = values();

    KVStoreRole(int ordinal) {
        if (ordinal != ordinal()) {
            throw new IllegalArgumentException("Wrong ordinal");
        }
    }

    /** Returns a KVStoreRole read from the input stream. */
    static KVStoreRole readFastExternal(DataInput in,
                                        @SuppressWarnings("unused") short sv)
        throws IOException
    {
        final int ordinal = in.readByte();
        try {
            return VALUES[ordinal];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                "Wrong ordinal for KVStoreRole: " + ordinal, e);
        }
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException
    {
        out.writeByte(ordinal());
    }
}
