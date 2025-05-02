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
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.util.SerializationUtil;

/**
 * Thrown when an index key exceeds the size limit. When this exception is
 * thrown, the caller can assume that the associated operation was not
 * performed and that there were no side effects.
 *
 * @deprecated as of 21.3.5, replaced by {@link KeySizeLimitException}
 *
 * @hidden For HTTP Proxy use only.
 */
@Deprecated
public class IndexKeySizeLimitException extends ResourceLimitException {

    private static final long serialVersionUID = 1L;

    private final String indexName;
    private final int indexKeySizeLimit;

    /**
     * Constructs an instance of <code>IndexKeySizeLimitException</code>
     * with the specified table, index key size limit, and detail message.
     *
     * @param tableName the table name
     * @param indexKeySizeLimit the index key size limit
     * @param msg the detail message
     *
     * @hidden For internal use only
     */
    public IndexKeySizeLimitException(String tableName,
                                      String indexName,
                                      int indexKeySizeLimit,
                                      String msg) {
        super(tableName, msg);
        assert tableName != null;
        assert indexName != null;
        this.indexName = indexName;
        this.indexKeySizeLimit = indexKeySizeLimit;
    }

    /**
     * Creates an instance from the input stream.
     *
     * @hidden For internal use only
     */
    public IndexKeySizeLimitException(DataInput in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        indexName = readString(in, serialVersion);
        indexKeySizeLimit = readPackedInt(in);
    }

    /**
     * Gets the name of the index who's key exceeded the limit.
     *
     * @return a index name
     *
     * @hidden For internal use only
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Gets the index key size limit at the time of the exception.
     *
     * @return the index key size table limit
     *
     * @hidden For internal use only
     */
    public int getIndexKeySizeLimit() {
        return indexKeySizeLimit;
    }

    /**
     * Writes the fields of this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResourceLimitException}) {@code super}
     * <li> ({@link SerializationUtil#writePackedInt packedInt})
     *      {@link #getIndexKeySizeLimit indexKeySizeLimit}
     * </ol>
     *
     * @hidden For internal use only
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, indexName);
        writePackedInt(out, indexKeySizeLimit);
    }
}
