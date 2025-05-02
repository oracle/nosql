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

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.impl.util.FastExternalizable;
import oracle.kv.impl.util.SerializationUtil;

/**
 * Holds results of an index key iteration over a table.  This result includes
 * primary key and index key byte arrays.  This is all of the information that
 * is available in a single secondary scan without doing an additional database
 * read of the primary data.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public class ResultIndexKeys implements FastExternalizable {

    private final byte[] primaryKeyBytes;
    private final byte[] indexKeyBytes;
    private final long expirationTime;

    public ResultIndexKeys(byte[] primaryKeyBytes,
                           byte[] indexKeyBytes,
                           long expirationTime) {
        checkNull("primaryKeyBytes", primaryKeyBytes);
        checkNull("indexKeyBytes", indexKeyBytes);
        this.primaryKeyBytes = primaryKeyBytes;
        this.indexKeyBytes = indexKeyBytes;
        this.expirationTime = expirationTime;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor
     * first to read common elements.
     */
    public ResultIndexKeys(DataInput in, short serialVersion)
        throws IOException {

        primaryKeyBytes = readNonNullByteArray(in);
        indexKeyBytes = readNonNullByteArray(in);
        expirationTime = Result.readTimestamp(in, serialVersion);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@link #getPrimaryKeyBytes primaryKeyBytes}
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@link #getIndexKeyBytes indexKeyBytes}
     * <li> ({@link DataOutput#writeBoolean boolean}) <i>expirationTime
     *      non-zero</i>
     * <li> <i>[Optional]</i> ({@link DataOutput#writeLong long}) {@link
     *      #getExpirationTime expirationTime} // if non-zero
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        writeNonNullByteArray(out, primaryKeyBytes);
        writeNonNullByteArray(out, indexKeyBytes);
        Result.writeTimestamp(out, expirationTime, serialVersion);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ResultIndexKeys)) {
            return false;
        }
        final ResultIndexKeys other = (ResultIndexKeys) obj;
        return Arrays.equals(primaryKeyBytes, other.primaryKeyBytes) &&
            Arrays.equals(indexKeyBytes, other.indexKeyBytes) &&
            (expirationTime == other.expirationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKeyBytes, indexKeyBytes, expirationTime);
    }

    public byte[] getPrimaryKeyBytes() {
        return primaryKeyBytes;
    }

    public byte[] getIndexKeyBytes() {
        return indexKeyBytes;
    }

    public long getExpirationTime() {
        return expirationTime;
    }
}
