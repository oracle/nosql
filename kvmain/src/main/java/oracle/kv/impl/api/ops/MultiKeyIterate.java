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

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.KeyRange;
import oracle.kv.impl.util.SerializationUtil;

/**
 * A multi-key iterate operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
abstract public class MultiKeyIterate extends MultiKeyOperation {

    private final Direction direction;
    private final int batchSize;
    private final byte[] resumeKey;
    private final boolean excludeTombstones;

    /**
     * Construct a multi-key iterate operation.
     */
    MultiKeyIterate(OpCode opCode,
                    byte[] parentKey,
                    KeyRange subRange,
                    Depth depth,
                    Direction direction,
                    int batchSize,
                    byte[] resumeKey,
                    boolean excludeTombstones) {
        super(opCode, parentKey, subRange, depth);
        checkNull("direction", direction);
        this.direction = direction;
        this.batchSize = batchSize;
        this.resumeKey = resumeKey;
        this.excludeTombstones = excludeTombstones;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiKeyIterate(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);

        direction = Direction.readFastExternal(in, serialVersion);
        batchSize = in.readInt();

        resumeKey = readByteArray(in);
        excludeTombstones = in.readBoolean();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link MultiKeyOperation}) {@code super}
     * <li> ({@link Direction}) {@link #getDirection direction}
     * <li> ({@link DataOutput#writeInt int}) {@link #getBatchSize batchSize}
     * <li> ({@link SerializationUtil#writeByteArray byte array}) {@link
     *      #getResumeKey resultKey}
     * <li> ({@link DataOutput#writeBoolean boolean})
     *      {@link #getExcludeTombstones excludeTombstones}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        direction.writeFastExternal(out, serialVersion);
        out.writeInt(batchSize);

        writeByteArray(out, resumeKey);
        out.writeBoolean(excludeTombstones);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof MultiKeyIterate)) {
            return false;
        }
        final MultiKeyIterate other = (MultiKeyIterate) obj;
        return direction.equals(other.direction) &&
            (batchSize == other.batchSize) &&
            Arrays.equals(resumeKey, other.resumeKey) &&
            (excludeTombstones == other.excludeTombstones);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), direction, batchSize, resumeKey,
                            excludeTombstones);
    }

    boolean getExcludeTombstones() {
        return excludeTombstones;
    }

    Direction getDirection() {
        return direction;
    }

    int getBatchSize() {
        return batchSize;
    }

    byte[] getResumeKey() {
        return resumeKey;
    }
}
