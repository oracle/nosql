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

package oracle.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.FastExternalizable;

/**
 * Used with multiple-key and iterator operations to specify whether to select
 * (return or operate on) the key-value pair for the parent key, and the
 * key-value pairs for only immediate children or all descendants.
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public enum Depth implements FastExternalizable {

    /*
     * WARNING: To avoid breaking serialization compatibility, the order of the
     * values must not be changed and new values must be added at the end.
     */

    /**
     * Select only immediate children, do not select the parent.
     */
    CHILDREN_ONLY(0),

    /**
     * Select immediate children and the parent.
     */
    PARENT_AND_CHILDREN(1),

    /**
     * Select all descendants, do not select the parent.
     */
    DESCENDANTS_ONLY(2),

    /**
     * Select all descendants and the parent.
     */
    PARENT_AND_DESCENDANTS(3);

    private static final Depth[] VALUES = values();

    private Depth(int ordinal) {
        if (ordinal != ordinal()) {
            throw new IllegalArgumentException("Wrong ordinal");
        }
    }

    /**
     * Returns the {@link Depth} with the specified ordinal.
     *
     * @hidden For internal use only
     *
     * @param ordinal the ordinal
     * @return the {@code Depth}
     * @throws IllegalArgumentException if the value is not found
     */
    public static Depth valueOf(int ordinal) {
        try {
            return VALUES[ordinal];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("unknown Depth: " + ordinal);
        }
    }

    /**
     * @hidden For internal use only
     */
    public static Depth readFastExternal(
        DataInput in, @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        return valueOf(in.readByte());
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@code byte}) <i>ordinal</i> // {@link #CHILDREN_ONLY}=0, {@link
     *      #PARENT_AND_CHILDREN}=1, {@link #DESCENDANTS_ONLY}=2, {@link
     *      #PARENT_AND_DESCENDANTS}=3
     * </ol>
     *
     * @hidden For Internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        out.writeByte(ordinal());
    }
}
