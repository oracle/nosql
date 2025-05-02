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
import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.readNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.readNonNullString;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeNonNullString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import oracle.kv.impl.api.table.IndexRange;
import oracle.kv.impl.api.table.TargetTables;
import oracle.kv.impl.util.SerializationUtil;

/**
 * An index operation identifies the index by name and table and includes
 * start, end, and resume keys.  Rules for keys:
 * <ul>
 * <li>the resume key overrides the start key</li>
 * <li>a null start key is used to operate over the entire index</li>
 * <li>the boolean dontIterate is used to do exact match operations.  If true,
 * then only matching entries are returned.  There may be more than one.</li>
 * </ul>
 *
 * Resume key is tricky because secondary databases may have duplicates so in
 * order to resume properly it's necessary to have both the resume secondary
 * key and the resume primary key.
 * <p>
 * This class is public to make it available to tests.
 *
 * @see #writeFastExternal FastExternalizable format
 */
public abstract class IndexOperation extends InternalOperation {

    /*
     * These members represent the serialized state of the class in the
     * protocol.  These are in order.
     */
    private final String indexName;
    protected final TargetTables targetTables;
    protected final IndexRange range;
    private final byte[] resumeSecondaryKey;
    private final byte[] resumePrimaryKey;
    private final int batchSize;
    private final int maxReadKB;
    private final int emptyReadFactor;

    /**
     * Constructs an index operation.
     *
     * For subclasses, allows passing OpCode.
     */
    protected IndexOperation(OpCode opCode,
                             String indexName,
                             TargetTables targetTables,
                             IndexRange range,
                             byte[] resumeSecondaryKey,
                             byte[] resumePrimaryKey,
                             int batchSize,
                             int maxReadKB,
                             int emptyReadFactor) {
        super(opCode);
        checkNull("indexName", indexName);
        checkNull("targetTables", targetTables);
        checkNull("range", range);
        if (resumeSecondaryKey != null) {
            checkNull("resumePrimaryKey", resumePrimaryKey);
        }
        this.indexName = indexName;
        this.targetTables = targetTables;
        this.range = range;
        this.resumeSecondaryKey = resumeSecondaryKey;
        this.resumePrimaryKey = resumePrimaryKey;
        this.batchSize = batchSize;
        this.maxReadKB = maxReadKB;
        /* emptyReadFactor is serialized as a byte */
        assert emptyReadFactor <= Byte.MAX_VALUE;
        this.emptyReadFactor = emptyReadFactor;
    }

    /**
     * FastExternalizable constructor.  Must call superclass constructor first
     * to read common elements.
     *
     * For subclasses, allows passing OpCode.
     */
    IndexOperation(OpCode opCode, DataInput in, short serialVersion)
        throws IOException {

        super(opCode, in, serialVersion);

        /* index name */
        indexName = readNonNullString(in, serialVersion);

        targetTables = new TargetTables(in, serialVersion);

        /* index range */
        range = new IndexRange(in, serialVersion);

        /* resume key */
        resumeSecondaryKey = readByteArray(in);
        resumePrimaryKey = (resumeSecondaryKey == null) ?
            null :
            readNonNullByteArray(in);

        /* batch size */
        batchSize = in.readInt();

        /* max read limit in KB */
        maxReadKB = in.readInt();

        emptyReadFactor = in.readByte();
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link InternalOperation}) {@code super}
     * <li> ({@link SerializationUtil#writeNonNullString non-null String})
     *      {@link #getIndexName indexName}
     * <li> ({@link TargetTables}) {@link #getTargetTables targetTables}
     * <li> ({@link IndexRange}) {@link #getIndexRange range}
     * <li> ({@link SerializationUtil#writeByteArray byte array})
     *      {@link #getResumeSecondaryKey resumeSecondaryKey}
     * <li> <i>[Optional]</i> ({@link
     *      SerializationUtil#writeNonNullByteArray non-null byte array})
     *      {@link #getResumePrimaryKey resumePrimaryKey} // if
     *      secondaryResumeKey was non-null
     * <li> ({@link DataOutput#writeInt int}) {@link #getBatchSize batchSize}
     * </ol>
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);

        writeNonNullString(out, serialVersion, indexName);

        targetTables.writeFastExternal(out, serialVersion);

        range.writeFastExternal(out, serialVersion);

        writeByteArray(out, resumeSecondaryKey);
        if (resumeSecondaryKey != null) {
            writeNonNullByteArray(out, resumePrimaryKey);
        }
        out.writeInt(batchSize);
        out.writeInt(maxReadKB);
        out.writeByte(emptyReadFactor);
    }

    IndexRange getIndexRange() {
        return range;
    }

    byte[] getResumeSecondaryKey() {
        return resumeSecondaryKey;
    }

    byte[] getResumePrimaryKey() {
        return resumePrimaryKey;
    }

    int getBatchSize() {
        return batchSize;
    }

    int getMaxReadKB() {
        return maxReadKB;
    }

    @Override
    void addEmptyReadCharge() {
        /*
         * Override to factor in the emptyReadFactor, only count the empty
         * read if current readKB is 0
         */
        if (getReadKB() == 0) {
            addReadBytes(MIN_READ * emptyReadFactor);
        }
    }

    String getIndexName() {
        return indexName;
    }

    TargetTables getTargetTables() {
        return targetTables;
    }

    @Override
    public String toString() {
        return super.toString(); //TODO
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj) ||
            !(obj instanceof IndexOperation)) {
            return false;
        }
        final IndexOperation other = (IndexOperation) obj;
        return indexName.equals(other.indexName) &&
            targetTables.equals(other.targetTables) &&
            range.equals(other.range) &&
            Arrays.equals(resumeSecondaryKey, other.resumeSecondaryKey) &&
            Arrays.equals(resumePrimaryKey, other.resumePrimaryKey) &&
            (batchSize == other.batchSize) &&
            (maxReadKB == other.maxReadKB) &&
            (emptyReadFactor == other.emptyReadFactor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), indexName, targetTables, range,
                            resumeSecondaryKey, resumePrimaryKey, batchSize,
                            maxReadKB, emptyReadFactor);
    }

    @Override
    public long getTableId(){
        return targetTables.getTargetTableId();
    }
}
