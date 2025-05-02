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
 * Used with iterator operations to specify the order that keys are returned.
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public enum Direction implements FastExternalizable {

    /*
     * WARNING: To avoid breaking serialization compatibility, the order of the
     * values must not be changed and new values must be added at the end.
     */

    /**
     * Iterate in ascending key order.
     */
    FORWARD(0),

    /**
     * Iterate in descending key order.
     */
    REVERSE(1),

    /**
     * Iterate in no particular key order.
     */
    UNORDERED(2);

    private static final Direction[] VALUES = values();
    public static final int VALUES_COUNT = VALUES.length;

    private Direction(int ordinal) {
        if (ordinal != ordinal()) {
            throw new IllegalArgumentException("Wrong ordinal");
        }
    }

    /**
     * Returns the {@link Direction} with the specified ordinal.
     *
     * @hidden For internal use only
     *
     * @param ordinal the ordinal
     * @return the {@code Direction}
     * @throws IllegalArgumentException if the value is not found
     */
    public static Direction valueOf(int ordinal) {
        try {
            return VALUES[ordinal];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                "unknown Direction: " + ordinal);
        }
    }

    /**
     * Read a Direction from the input stream.
     *
     * @hidden For internal use only
     */
    public static Direction readFastExternal(
        DataInput in, @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        return valueOf(in.readUnsignedByte());
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@code byte}) <i>ordinal</i> // {@link #FORWARD}=0, {@link
     *      #REVERSE}=1, {@link #UNORDERED}=2
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        out.writeByte(ordinal());
    }
}
