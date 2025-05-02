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
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.readSequenceLength;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeCollectionLength;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import oracle.kv.Depth;
import oracle.kv.KeyRange;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.UserDataControl;

/**
 * This is an intermediate class for multi-get-batch iterate operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
abstract class MultiGetBatchIterateOperation extends MultiKeyOperation {

    private final List<byte[]> parentKeys;
    private final int batchSize;
    private final byte[] resumeKey;
    private final boolean excludeTombstones;

    public MultiGetBatchIterateOperation(OpCode opCode,
                                         List<byte[]> parentKeys,
                                         byte[] resumekey,
                                         KeyRange subRange,
                                         Depth depth,
                                         int batchSize,
                                         boolean excludeTombstones) {

        super(opCode, parentKeys.get(0), subRange, depth);

        this.parentKeys = parentKeys;
        this.resumeKey = resumekey;
        this.batchSize = batchSize;
        this.excludeTombstones = excludeTombstones;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    protected MultiGetBatchIterateOperation(OpCode opCode,
                                            DataInput in,
                                            short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);
        final int nkeys = readSequenceLength(in);
        if (nkeys >= 0) {
            parentKeys = new ArrayList<byte[]>(nkeys);
            for (int i = 0; i < nkeys; i++) {
                parentKeys.add(readNonNullByteArray(in));
            }
        } else {
            parentKeys = null;
        }
        resumeKey = readByteArray(in);
        batchSize = in.readInt();
        excludeTombstones = in.readBoolean();
    }

    List<byte[]> getParentKeys() {
        return parentKeys;
    }

    int getBatchSize() {
        return batchSize;
    }

    byte[] getResumeKey() {
        return resumeKey;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link MultiKeyOperation}) {@code super}
     * <li> ({@link SerializationUtil#writeCollectionLength sequence length})
     *      <i>number of parentKeys</i>
     * <li> <i>[Optional]</i> <i>Repeat for each parent key</i>
     *    <ol type="a">
     *    <li> ({@link SerializationUtil#writeNonNullByteArray non-null
     *         byte array}) <i>key</i>
     *    </ol>
     * <li> ({@link SerializationUtil#writeByteArray byte array})
     *      {@link #getResumeKey resumeKey}
     * <li> ({@link DataOutput#writeInt int}) {@link #getBatchSize batchSize}
     * <li> ({@link DataOutput#writeBoolean boolean})
     *      {@link #getExcludeTombstones excludeTombstones}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeCollectionLength(out, parentKeys);

        if (parentKeys != null) {
            for (byte[] key: parentKeys) {
                writeNonNullByteArray(out, key);
            }
        }
        writeByteArray(out, resumeKey);
        out.writeInt(batchSize);
        out.writeBoolean(excludeTombstones);
    }

    boolean getExcludeTombstones() {
        return excludeTombstones;
    }

    @Override
    public String toString() {
        return super.toString() +
            " parentKeys: " + (parentKeys != null ? parentKeys.size() : 0) +
            " resumeKey: " + UserDataControl.displayKey(resumeKey) +
            " batchSize: " + batchSize +
            " excludeTombstones: " + excludeTombstones;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof MultiGetBatchIterateOperation)) {
            return false;
        }
        final MultiGetBatchIterateOperation other =
            (MultiGetBatchIterateOperation) obj;
        return equalLists(parentKeys, other.parentKeys) &&
            (batchSize == other.batchSize) &&
            Arrays.equals(resumeKey, other.resumeKey) &&
            (excludeTombstones == other.excludeTombstones);
    }

    /**
     * Check if two possibly null lists with byte array elements are equal.
     */
    private static boolean equalLists(List<byte[]> x, List<byte[]> y) {
        if (x == y) {
            return true;
        }
        if ((x == null) || (y == null)) {
            return false;
        }
        if (x.size() != y.size()) {
            return false;
        }
        for (int i = 0; i < x.size(); i++) {
            if (!Arrays.equals(x.get(i), y.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            (parentKeys != null ? parentKeys.size() : 0),
                            batchSize, resumeKey, excludeTombstones);
    }
}
