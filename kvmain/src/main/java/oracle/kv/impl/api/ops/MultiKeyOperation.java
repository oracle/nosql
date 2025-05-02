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

package oracle.kv.impl.api.ops;

import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeFastExternalOrNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.Depth;
import oracle.kv.KeyRange;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.UserDataControl;

/**
 * A multi-key operation has a parent key, optional KeyRange and depth.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class MultiKeyOperation extends InternalOperation {

    /**
     * The parent key, or null.
     */
    private final byte[] parentKey;

    /**
     * Sub-key range of traversal, or null.
     */
    private final KeyRange subRange;

    /**
     * Depth of traversal or null.
     */
    private final Depth depth;

    /**
     * Constructs a multi-key operation.
     *
     * For subclasses, allows passing OpCode.
     */
    MultiKeyOperation(OpCode opCode,
                      byte[] parentKey,
                      KeyRange subRange,
                      Depth depth) {
        super(opCode);
        this.parentKey = parentKey;
        this.subRange = subRange;
        this.depth = depth;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     *
     * For subclasses, allows passing OpCode.
     */
    MultiKeyOperation(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);

        parentKey = readByteArray(in);

        if (!in.readBoolean()) {
            subRange = null;
        } else {
            subRange = new KeyRange(in, serialVersion);
        }

        final int depthOrdinal = in.readByte();
        if (depthOrdinal == -1) {
            depth = null;
        } else {
            depth = Depth.valueOf(depthOrdinal);
        }
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link InternalOperation}) {@code super}
     * <li> ({@link SerializationUtil#writeByteArray byte array}) {@link
     *      #getParentKey parentKey}
     * <li> ({@link SerializationUtil#writeFastExternalOrNull KeyRange or null}
     *      {@link #getSubRange subRange}
     * <li> ({@code byte}) {@link #getDepth depth} // Depth ordinal or -1 if
     *      not present
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        writeByteArray(out, parentKey);

        writeFastExternalOrNull(out, serialVersion, subRange);

        if (depth == null) {
            out.writeByte(-1);
        } else {
            out.writeByte(depth.ordinal());
        }
    }

    byte[] getParentKey() {
        return parentKey;
    }

    KeyRange getSubRange() {
        return subRange;
    }

    Depth getDepth() {
        return depth;
    }

    @Override
    public String toString() {
        return super.toString() +
            " parentKey: " + UserDataControl.displayKey(parentKey) +
            " subRange: " + UserDataControl.displayKeyRange(subRange) +
            " depth: " + depth;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof MultiKeyOperation)) {
            return false;
        }
        final MultiKeyOperation other = (MultiKeyOperation) obj;
        return Arrays.equals(parentKey, other.parentKey) &&
            Objects.equals(subRange, other.subRange) &&
            (depth == other.depth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parentKey, subRange, depth);
    }
}
