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
 * Thrown when write operations cannot be completed because a table size
 * limit has been exceeded. When this exception is thrown, the caller can
 * assume that the associated operation was not performed and that there were
 * no side effects.
 * 
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class TableSizeLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    private final int tableSize;
    private final int tableSizeLimit;

    /**
     * Constructs an instance of <code>TableSizeLimitException</code> with
     * the specified table name and detail message.
     *
     * @param tableName the table name
     * @param tableSize the table size
     * @param tableSizeLimit the table size limit
     * @param msg the detail message
     * 
     * @hidden For internal use only
     */
    public TableSizeLimitException(String tableName,
                                   int tableSize,
                                   int tableSizeLimit,
                                   String msg) {
        super(tableName, msg);
        assert tableName != null;
        this.tableSize = tableSize;
        this.tableSizeLimit = tableSizeLimit;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public TableSizeLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        tableSize = readPackedInt(in);
        tableSizeLimit = readPackedInt(in);
    }

    /**
     * Gets the table size at the time of the exception.
     *
     * @return the table size
     * 
     * @hidden For internal use only
     */
    public int getTableSize() {
        return tableSize;
    }

    /**
     * Gets the table size limit at the time of the exception.
     *
     * @return the table size limit
     * 
     * @hidden For internal use only
     */
    public int getTableSizeLimit() {
        return tableSizeLimit;
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceLimitException}) {@code super}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getTableSize tableSize}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getTableSizeLimit tableSizeLimit}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        writePackedInt(out, tableSize);
        writePackedInt(out, tableSizeLimit);
    }
}
