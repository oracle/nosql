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

import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.Version;
import oracle.kv.impl.util.SerializationUtil;

/**
 * This class holds results of an index row iteration over a table.  It extends
 * ResultKeyValueVersion to do this, adding the index key in addition to the
 * row itself.  The index key is needed by the client in order to accurately do
 * chunked iteration using a resume key.  The resume key cannot be constructed
 * from the Row alone in all cases.
 *
 * This class, which includes the index key bytes, was introduced in release
 * 3.2, so it only includes/expected index key bytes if the serialVersion of
 * the "other" side of the connection is at least the version associated with
 * 3.2 (V6).
 *
 * @since 3.2
 * @see #writeFastExternal FastExternalizable format
 */
public class ResultIndexRows extends ResultKeyValueVersion {

    /* this class adds the index key to ResultKeyValueVersion */
    private final byte[] indexKeyBytes;

    public ResultIndexRows(byte[] indexKeyBytes,
                           byte[] primaryKeyBytes,
                           byte[] valueBytes,
                           Version version,
                           long expirationTime,
                           long creationTime,
                           long modificationTime) {
        super(primaryKeyBytes, valueBytes, version, expirationTime,
            creationTime, modificationTime, false/* isTombstone */);
        this.indexKeyBytes = indexKeyBytes;
    }

    /** Constructor to implement deserializedForm */
    private ResultIndexRows(ResultIndexRows other, short serialVersion) {
        super(other, serialVersion);
        indexKeyBytes = other.indexKeyBytes;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor
     * first to read common elements.
     */
    public ResultIndexRows(DataInput in, short serialVersion)
        throws IOException {
        super(in, serialVersion);
        indexKeyBytes = readNonNullByteArray(in);
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link ResultKeyValueVersion}) {@code super}
     * <li> ({@link SerializationUtil#writeNonNullByteArray non-null byte
     *      array}) {@link #getIndexKeyBytes indexKeyBytes}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {
        super.writeFastExternal(out, serialVersion);
        writeNonNullByteArray(out, indexKeyBytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof ResultIndexRows)) {
            return false;
        }
        final ResultIndexRows other = (ResultIndexRows) obj;
        return Arrays.equals(indexKeyBytes, other.indexKeyBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), indexKeyBytes);
    }

    public byte[] getIndexKeyBytes() {
        return indexKeyBytes;
    }

    @Override
    public ResultIndexRows deserializedForm(short serialVersion) {
        return new ResultIndexRows(this, serialVersion);
    }
}
