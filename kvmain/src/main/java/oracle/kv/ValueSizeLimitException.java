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
 * Thrown when table value exceeds the size limit. When this exception is
 * thrown, the caller can assume that the associated operation was not
 * performed and that there were no side effects.
 *
 * @hidden For HTTP Proxy use only.
 */
public class ValueSizeLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    private final int valueSizeLimit;

    /**
     * Constructs an instance of <code>ValueSizeLimitException</code>
     * with the specified table and value size limit, and detail message.
     *
     * @param tableName the table name
     * @param valueSizeLimit the value size limit
     * @param msg the detail message
     *
     * @hidden For internal use only
     */
    public ValueSizeLimitException(String tableName,
                                   int valueSizeLimit,
                                   String msg) {
        super(tableName, msg);
        assert tableName != null;
        this.valueSizeLimit = valueSizeLimit;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public ValueSizeLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        valueSizeLimit = readPackedInt(in);
    }

    /**
     * Gets the value size limit at the time of the exception.
     *
     * @return the value size table limit
     *
     * @hidden For internal use only
     */
    public int getValueSizeLimit() {
        return valueSizeLimit;
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceLimitException}) {@code super}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getValueSizeLimit valueSizeLimit}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        writePackedInt(out, valueSizeLimit);
    }
}
