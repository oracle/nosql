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

package oracle.kv.impl.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.sleepycat.je.rep.ReplicatedEnvironment.State;

/**
 * Utility methods for serializing and deserializing JE data structures.
 */
public class JESerializationUtil {

    /** The possible values of the State enum */
    private static final State[] STATE_VALUES = State.values();

    /* Verify enum ordinal values for this JE enum. */
    static {
        if ((State.DETACHED.ordinal() != 0) ||
            (State.UNKNOWN.ordinal() != 1) ||
            (State.MASTER.ordinal() != 2) ||
            (State.REPLICA.ordinal() != 3)) {
            throw new IllegalStateException(
                "State ordinal values check failed");
        }
    }

    /**
     * Deserializes a State enum constant from the input stream.
     */
    public static State readState(DataInput in,
                                  @SuppressWarnings("unused")
                                  short serialVersion)
        throws IOException
    {
        final int value = in.readByte();
        if (value == -1) {
            return null;
        }
        try {
            return STATE_VALUES[value];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IOException(
                "Wrong value for ReplicatedEnvironment.State: " + value);
        }
    }

    /**
     * Serializes a State enum constant to the output stream.
     */
    public static void writeState(DataOutput out,
                                  @SuppressWarnings("unused")
                                  short serialVersion,
                                  State state)
        throws IOException
    {
        out.writeByte((state != null) ? state.ordinal() : -1);
    }
}
