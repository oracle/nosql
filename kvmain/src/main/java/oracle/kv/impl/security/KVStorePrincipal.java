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
import java.security.Principal;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.ReadFastExternal;

/**
 * A marker interface for principals used by UserLoginAPI and TrustedLoginAPI.
 *
 * @since 21.2
 */
public interface KVStorePrincipal extends Principal, FastExternalizable {

    /** Identifies all classes that implement KVStorePrincipal. */
    enum PrincipalType implements FastExternalizable {
        USER(0, KVStoreUserPrincipal::new),
        ROLE(1, KVStoreRolePrincipal::new);

        private static final PrincipalType[] VALUES = values();

        private final ReadFastExternal<KVStorePrincipal> reader;

        PrincipalType(final int ordinal,
                      final ReadFastExternal<KVStorePrincipal> reader) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
            this.reader = reader;
        }

        static
        PrincipalType readFastExternal(DataInput in,
                                       @SuppressWarnings("unused") short sv)
            throws IOException
        {
            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "Wrong ordinal for PrincipalType: " + ordinal, e);
            }
        }

        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException
        {
            out.writeByte(ordinal());
        }

        KVStorePrincipal readPrincipal(final DataInput in,
                                       final short serialVersion)
            throws IOException
        {
            return reader.readFastExternal(in, serialVersion);
        }

        @Override
        public String toString() {
            return name() + '(' + ordinal() + ')';
        }
    }

    /** Returns the principal type of this principal. */
    PrincipalType getPrincipalType();

    /** Reads a KVStorePrincipal from the input stream. */
    static KVStorePrincipal readPrincipal(DataInput in, short sv)
        throws IOException
    {
        return PrincipalType.readFastExternal(in, sv).readPrincipal(in, sv);
    }

    /** Writes this object as a KVStorePrincipal to the output stream. */
    default void writePrincipal(DataOutput out, short serialVersion)
        throws IOException
    {
        getPrincipalType().writeFastExternal(out, serialVersion);
        writeFastExternal(out, serialVersion);
    }
}
