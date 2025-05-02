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

import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerializationUtil;

/**
 * Thrown when write operations cannot be completed because a partition size
 * limit has been exceeded. When this exception is thrown, the caller can
 * assume that the associated operation was not performed and that there were
 * no side effects.
 *
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class TablePartitionSizeLimitException extends TableSizeLimitException {
    private static final long serialVersionUID = 1L;

    private final int partitionSize;
    private final int partitionSizeLimit;

    /**
     * Constructs a new instance of
     * <code>TablePartitionSizeLimitException</code> with the specified table
     * name and detail message.
     *
     * @param tableName the table name
     * @param tableSize the table size
     * @param tableSizeLimit the table size limit
     * @param partitionSize the partition size
     * @param partitionSizeLimit the partition size limit
     * @param msg the detail message
     *
     * @hidden For internal use only
     */
    public TablePartitionSizeLimitException(String tableName,
                                            int tableSize,
                                            int tableSizeLimit,
                                            int partitionSize,
                                            int partitionSizeLimit,
                                            String msg) {
        super(tableName, tableSize, tableSizeLimit, msg);
        assert tableName != null;
        this.partitionSize = partitionSize;
        this.partitionSizeLimit = partitionSizeLimit;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public TablePartitionSizeLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        partitionSize = readPackedInt(in);
        partitionSizeLimit = readPackedInt(in);
    }

    /**
     * Gets the partition size at the time of the exception.
     *
     * @return the partition size
     *
     * @hidden For internal use only
     */
    public int getPartitionSize() {
        return partitionSize;
    }

    /**
     * Gets the partition size limit at the time of the exception.
     *
     * @return the partition size limit
     *
     * @hidden For internal use only
     */
    public int getPartitionSizeLimit() {
        return partitionSizeLimit;
    }
    
    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link TableSizeLimitException}) {@code super}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getTableSize shardSize}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getTableSizeLimit shardSizeLimit}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        writePackedInt(out, partitionSize);
        writePackedInt(out, partitionSizeLimit);
    }
}
