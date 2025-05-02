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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import oracle.kv.Depth;
import oracle.kv.KeyRange;

/**
 * A multi-get-keys operation.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class MultiGetKeys extends MultiKeyOperation {
    private final boolean excludeTombstones;

    /**
     * Construct a multi-get operation.
     */
    public MultiGetKeys(byte[] parentKey,
                        KeyRange subRange,
                        Depth depth,
                        boolean excludeTombstones) {
        super(OpCode.MULTI_GET_KEYS, parentKey, subRange, depth);
        this.excludeTombstones = excludeTombstones;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiGetKeys(DataInput in, short serialVersion)
        throws IOException {

        super(OpCode.MULTI_GET_KEYS, in, serialVersion);
        this.excludeTombstones = in.readBoolean();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link MultiKeyOperation}) {@code super}
     * <li> ({@link DataOutput#writeBoolean boolean})
     *      {@link #getExcludeTombstones excludeTombstone}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        out.writeBoolean(excludeTombstones);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof MultiGetKeys)) {
            return false;
        }
        final MultiGetKeys other = (MultiGetKeys) obj;
        return (excludeTombstones == other.excludeTombstones);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), excludeTombstones);
    }

    boolean getExcludeTombstones() {
        return excludeTombstones;
    }
}
