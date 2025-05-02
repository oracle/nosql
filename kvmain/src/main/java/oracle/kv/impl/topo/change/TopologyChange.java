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

package oracle.kv.impl.topo.change;

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.Topology.Component;
import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * The Base class for all topology changes. A sequence of changes can be
 * applied to a Topology instance, via {@link Topology#apply} to make it more
 * current.
 * <p>
 * Each TopologyChange represents a logical change entry in a logical log with
 * changes being applied in sequence via {@link Topology#apply} to modify the
 * topology and bring it up to date.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class TopologyChange
        implements FastExternalizable, Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    /**
     * The type of the topology change.
     *
     * @see #writeFastExternal FastExternalizable format
     */
    public enum Type implements FastExternalizable {
        ADD(0),
        UPDATE(1),
        REMOVE(2);

        private static final Type[] VALUES = values();

        private Type(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        public static Type readFastExternal(
            DataInput in,
            @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException("Wrong ordinal for Type: " + ordinal, e);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #ADD}=0,
         *      {@link #UPDATE}=1, {@link #REMOVE}=2
         * </ol>
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    protected final int sequenceNumber;

    TopologyChange(int sequenceNumber) {
        super();
        this.sequenceNumber = sequenceNumber;
    }

    TopologyChange(DataInput in,
                   @SuppressWarnings("unused") short serialVersion)
        throws IOException {

        sequenceNumber = readPackedInt(in);
    }

    public static TopologyChange readFastExternal(DataInput in,
                                                  short serialVersion)
        throws IOException {

        final Type type = Type.readFastExternal(in, serialVersion);
        switch (type) {
        case ADD:
            return new Add(in, serialVersion);
        case UPDATE:
            return new Update(in, serialVersion);
        case REMOVE:
            return new Remove(in, serialVersion);
        default:
            throw new AssertionError();
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link Type}) {@linkplain #getType <i>type</i>}
     * <li> ({@link SerializationUtil#writePackedInt packedInt}) {@link
     *      #getSequenceNumber sequenceNumber}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        getType().writeFastExternal(out, serialVersion);
        writePackedInt(out, sequenceNumber);
    }

    public abstract Type getType();

    /**
     * Identifies the resource being changed
     */
    public abstract ResourceId getResourceId();

    /**
     * Returns the impacted component, or null if one is not available.
     */
    public abstract Component<?> getComponent();

    @Override
    public abstract TopologyChange clone();

    /**
     * Returns The sequence number associated with this change.
     */
    public int getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "seq=" + sequenceNumber + "/" + getType() + " " + 
            getResourceId() + "/" + getComponent();
    }
}
