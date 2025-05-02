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

/**
 * Operation that applies a read throughput charge to a specified table.
 */
public class TableCharge extends InternalOperation {

    private final long tableId;
    private final int readUnits;

    public TableCharge(long tableId, int readUnits) {
        super(OpCode.TABLE_CHARGE);
        assert tableId > 0L;
        this.tableId = tableId;
        this.readUnits = readUnits;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     */
    TableCharge(DataInput in, short serialVersion)
            throws IOException {
        super(OpCode.TABLE_CHARGE, in, serialVersion);
        tableId = in.readLong();
        readUnits = in.readInt();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SingleKeyOperation}) {@code super}
     * <li> ({@link DataOutput#writeLong long}) {@link #getTableId tableId}
     * <li> ({@link DataOutput#writeInt int})  {@link #getReadUnits readUnits}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

        super.writeFastExternal(out, serialVersion);
        out.writeLong(tableId);
        out.writeInt(readUnits);
    }

    @Override
    public long getTableId() {
        return tableId;
    }

    int getReadUnits() {
        return readUnits;
    }
}
