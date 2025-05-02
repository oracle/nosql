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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Thrown when a write operation cannot be completed because the write
 * throughput rate limit has been exceeded. If the throughput was associated
 * with a table the table name returned by getTableName will be non-null.
 * When this exception is thrown, the caller can assume that the associated
 * operation was not performed and that there were no side effects.
 * 
 * @since 19.3
 * @hidden.see {@link #writeFastExternal FastExternalizable format}
 */
public class WriteThroughputException extends ThroughputLimitException {
    
    private static final long serialVersionUID = 1L;

    /**
     * Constructs an instance of <code>WriteThroughputException</code> with
     * the specified detail message. The write rates and limits may be 0.
     *
     * @param tableName the table name
     * @param writeRate write rate
     * @param writeRateLimit write rate limit
     * @param msg the detail message
     * 
     * @hidden For internal use only
     */
    public WriteThroughputException(String tableName,
                                    int writeRate,
                                    int writeRateLimit,
                                    String msg) {
        super(tableName, 0, 0, writeRate, writeRateLimit, msg);
    }
    
    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public WriteThroughputException(DataInput in, short serialVersion)
        throws IOException {
        
        super(in, serialVersion);
    }
    
    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ThroughputLimitException}) {@code super}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
    }
}
