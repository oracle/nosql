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
 * Thrown when read or write operations cannot be completed because a
 * throughput rate limit has been exceeded. The exception may be due to
 * limits on read or write throughout, or both. If the throughput was associated
 * with a table the table name returned by getTableName will be non-null.
 * When this exception is thrown, the caller can assume that the associated
 * operation was not performed and that there were no side effects.
 * 
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class ThroughputLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    private final int readRate;
    private final int readRateLimit;
    private final int writeRate;
    private final int writeRateLimit;

    /**
     * Constructs an instance of <code>ThroughputLimitException</code> with
     * the specified detail message. The read/write rates and limits may be 0.
     *
     * @param tableName the table name
     * @param readRate read rate
     * @param readRateLimit read rate limit
     * @param writeRate write rate
     * @param writeRateLimit write rate limit
     * @param msg the detail message
     * 
     * @hidden For internal use only
     */
    public ThroughputLimitException(String tableName,
                                    int readRate,
                                    int readRateLimit,
                                    int writeRate,
                                    int writeRateLimit,
                                    String msg) {
        super(tableName, msg);
        this.readRate = readRate;
        this.readRateLimit = readRateLimit;
        this.writeRate = writeRate;
        this.writeRateLimit = writeRateLimit;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public ThroughputLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        readRate = readPackedInt(in);
        readRateLimit = readPackedInt(in);
        writeRate = readPackedInt(in);
        writeRateLimit = readPackedInt(in);
    }

    /**
     * Gets the read rate at the time of the exception. The returned
     * value will be 0 if this exception was due to a write rate overage.
     *
     * @return the read rate or 0
     * 
     * @hidden For internal use only
     */
    public int getReadRate() {
        return readRate;
    }

    /**
     * Gets the read rate limit at the time of the exception.  The returned
     * value will be 0 if this exception was due to a write rate overage.
     *
     * @return the read rate limit or 0
     * 
     * @hidden For internal use only
     */
    public int getReadRateLimit() {
        return readRateLimit;
    }

    /**
     * Gets the write rate at the time of the exception. The returned
     * value will be 0 if this exception was due to a read rate overage.
     *
     * @return the write rate or 0
     * 
     * @hidden For internal use only
     */
    public int getWriteRate() {
        return writeRate;
    }

    /**
     * Gets the write rate limit at the time of the exception. The returned
     * value will be 0 if this exception was due to a read rate overage.
     *
     * @return the write rate limit or 0
     * 
     * @hidden For internal use only
     */
    public int getWriteRateLimit() {
        return writeRateLimit;
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceLimitException}) {@code super}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getReadRate readRate}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getReadRateLimit readRateLimit}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getWriteRate writeRate}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getWriteRateLimit writeRateLimit}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        writePackedInt(out, readRate);
        writePackedInt(out, readRateLimit);
        writePackedInt(out, writeRate);
        writePackedInt(out, writeRateLimit);
    }
}
