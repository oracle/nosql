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

package oracle.kv.impl.topo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.FastExternalizable;

/**
 * The type of a datacenter.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public enum DatacenterType implements FastExternalizable {

    /** Contains electable nodes. */
    PRIMARY(0) {
        @Override
        public boolean isPrimary() {
            return true;
        }
    },

    /** Contains secondary nodes. */
    SECONDARY(1) {
        @Override
        public boolean isSecondary() {
            return true;
        }
    };

    private static final DatacenterType[] VALUES = values();

    public static final int VALUES_COUNT = VALUES.length;

    private DatacenterType(int ordinal) {
        if (ordinal != ordinal()) {
            throw new IllegalArgumentException("Wrong ordinal");
        }
    }

    /**
     * Returns whether this is the {@link #PRIMARY} type.
     *
     * @return whether this is {@code PRIMARY}
     */
    public boolean isPrimary() {
        return false;
    }

    /**
     * Returns whether this is the {@link #SECONDARY} type.
     *
     * @return whether this is {@code SECONDARY}
     */
    public boolean isSecondary() {
        return false;
    }

    public static DatacenterType readFastExternal(
        DataInput in,
        @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        final int ordinal = in.readByte();
        try {
            return VALUES[ordinal];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException(
                "Wrong ordinal for DatacenterType: " + ordinal, e);
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@code byte}) <i>value</i> // {@link #PRIMARY}=0, {@link
     *      #SECONDARY}=1
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        out.writeByte(ordinal());
    }
}
