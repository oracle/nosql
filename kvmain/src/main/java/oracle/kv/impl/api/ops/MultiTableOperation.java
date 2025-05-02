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

import static oracle.kv.impl.api.ops.InternalOperationHandler.MIN_READ;
import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.Depth;
import oracle.kv.KeyRange;
import oracle.kv.impl.api.table.TargetTables;

/**
 * This is an intermediate class that encapsulates a get or iteration
 * operation on multiple tables. It can be directly constructed by
 * server-side iteration code, but is usually used in the context of
 * a table iteration client request.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class MultiTableOperation extends MultiKeyOperation {

    /*
     * Encapsulates the target table, child tables, ancestor tables.
     */
    private final TargetTables targetTables;

    private final int emptyReadFactor;

    /**
     * Construct a multi-get operation.
     */
    protected MultiTableOperation(OpCode opCode,
                                  byte[] parentKey,
                                  TargetTables targetTables,
                                  KeyRange subRange) {
        this(opCode, parentKey, targetTables, subRange, 1 /* emptyReadFactor */);
    }

    protected MultiTableOperation(OpCode opCode,
                                  byte[] parentKey,
                                  TargetTables targetTables,
                                  KeyRange subRange,
                                  int emptyReadFactor) {
        super(opCode, parentKey, subRange, Depth.PARENT_AND_DESCENDANTS);
        checkNull("targetTables", targetTables);
        this.targetTables = targetTables;

        /* emptyReadFactor is serialized as a byte */
        assert emptyReadFactor <= Byte.MAX_VALUE;
        this.emptyReadFactor = emptyReadFactor;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    MultiTableOperation(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);

        targetTables = new TargetTables(in, serialVersion);

        emptyReadFactor = in.readByte();
    }

    TargetTables getTargetTables() {
        return targetTables;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link MultiKeyOperation}) {@code super}
     * <li> ({@link TargetTables}) {@link #getTargetTables targetTables}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        targetTables.writeFastExternal(out, serialVersion);

        out.writeByte(emptyReadFactor);
    }

    @Override
    public long getTableId() {
        return targetTables.getTargetTableId();
    }

    @Override
    public void addEmptyReadCharge() {
        /*
         * Override to factor in the emptyReadFactor, only count the empty
         * read if current readKB is 0
         */
        if (getReadKB() == 0) {
            addReadBytes(emptyReadFactor * MIN_READ);
        }
    }
}
